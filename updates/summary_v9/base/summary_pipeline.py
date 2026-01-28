"""
Summary Pipeline v9 - Complete Implementation
==============================================

Enhanced version of v9 with ALL 3 cases:
- Case I:   New accounts (no existing summary)
- Case II:  Forward entries (month > max existing)
- Case III: Backfill (month <= max existing)

Key Advantages over v8:
1. SQL-based array updates (faster than UDF loops)
2. Broadcast optimization for small tables
3. Better cache management (MEMORY_AND_DISK)
4. Single-pass array updates
5. Production-ready validation

Performance Improvements:
- Reduced table scans from 4 to 3
- Better join strategies with broadcast hints
- SQL transform() instead of Python UDFs
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, ArrayType, DoubleType, DecimalType
from pyspark import StorageLevel
import logging
import json
import argparse
import sys
import time
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple, Any, Optional


# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SummaryPipeline.v9")


# ============================================================================
# CONSTANTS
# ============================================================================

HISTORY_LENGTH = 36
TARGET_RECORDS_PER_PARTITION = 5_000_000
MIN_PARTITIONS = 32
MAX_PARTITIONS = 16384


# ============================================================================
# CONFIGURATION
# ============================================================================

class PipelineConfig:
    """Configuration for summary pipeline"""
    
    def __init__(self, config_path: str = None):
        # Default table names
        self.accounts_table = "spark_catalog.edf_gold.ivaps_consumer_accounts_all"
        self.latest_summary_table = "primary_catalog.edf_gold.latest_summary"
        self.summary_table = "primary_catalog.edf_gold.summary"
        
        # Keys
        self.primary_key = "cons_acct_key"
        self.partition_key = "rpt_as_of_mo"
        self.timestamp_key = "base_ts"
        
        # Performance tuning
        self.cache_level = StorageLevel.MEMORY_AND_DISK
        self.broadcast_threshold = 10_000_000  # 10M rows = broadcast
        
        # History arrays to update (array_name, source_column)
        self.history_arrays = [
            ("payment_history_grid", "payment_history_grid"),
            ("asset_class_cd_4in_history", "asset_class_cd_4in"),
            ("days_past_due_history", "days_past_due_cd"),
            ("payment_rating_cd_history", "payment_rating_cd"),
            ("past_due_am_history", "past_due_am"),
            ("credit_limit_am_history", "acct_credit_ext_am"),
            ("balance_am_history", "acct_high_credit_am"),
            ("actual_payment_am_history", "actual_payment_am")
        ]
        
        # Grid columns (generated from history arrays)
        self.grid_columns = [
            {
                "name": "payment_history_grid_str",
                "source_history": "payment_rating_cd_history",
                "placeholder": "?",
                "separator": ""
            }
        ]
        
        # Load from JSON if provided
        if config_path:
            self._load_from_json(config_path)
    
    def _load_from_json(self, config_path: str):
        """Load configuration from JSON file"""
        with open(config_path, 'r') as f:
            cfg = json.load(f)
        
        # Tables
        if 'tables' in cfg:
            self.accounts_table = cfg['tables'].get('source', self.accounts_table)
            self.summary_table = cfg['tables'].get('summary', self.summary_table)
            self.latest_summary_table = cfg['tables'].get('latest_summary', self.latest_summary_table)
        
        # Keys
        if 'keys' in cfg:
            self.primary_key = cfg['keys'].get('primary', self.primary_key)
            self.partition_key = cfg['keys'].get('partition', self.partition_key)
            self.timestamp_key = cfg['keys'].get('timestamp', self.timestamp_key)
        
        # Rolling columns -> history arrays
        if 'rolling_columns' in cfg:
            self.history_arrays = [
                (f"{rc['name']}_history", rc['source_column'])
                for rc in cfg['rolling_columns']
            ]
        
        # Grid columns
        if 'grid_columns' in cfg:
            self.grid_columns = cfg['grid_columns']
    
    def validate(self) -> bool:
        """Validate configuration"""
        if not self.primary_key or not self.partition_key:
            logger.error("Primary key and partition key are required")
            return False
        if not self.history_arrays:
            logger.error("At least one history array is required")
            return False
        return True


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM to integer"""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def calculate_partitions(record_count: int) -> int:
    """Calculate optimal partition count"""
    if record_count == 0:
        return MIN_PARTITIONS
    partitions = record_count / TARGET_RECORDS_PER_PARTITION
    import math
    power_of_2 = 2 ** round(math.log2(max(1, partitions)))
    return int(max(MIN_PARTITIONS, min(MAX_PARTITIONS, power_of_2)))


def configure_spark(spark: SparkSession, num_partitions: int):
    """Configure Spark for optimal partitioning"""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))


# ============================================================================
# STEP 1: LOAD AND CLASSIFY RECORDS
# ============================================================================

def load_and_classify_accounts(spark: SparkSession, config: PipelineConfig, 
                                filter_expr: str = None):
    """
    Load accounts and classify into Case I/II/III
    
    Case I:   New accounts (not in summary)
    Case II:  Forward entries (month > max existing)
    Case III: Backfill (month <= max existing)
    
    Returns: DataFrame with case_type column
    """
    logger.info("=" * 80)
    logger.info("STEP 1: Load and Classify Accounts")
    logger.info("=" * 80)
    
    pk = config.primary_key
    prt = config.partition_key
    ts = config.timestamp_key
    
    # Load accounts
    logger.info(f"Loading accounts from {config.accounts_table}")
    if filter_expr:
        accounts_df = spark.sql(f"""
            SELECT * FROM {config.accounts_table}
            WHERE {filter_expr}
        """)
    else:
        accounts_df = spark.read.table(config.accounts_table)
    
    # Deduplicate by primary key + partition, keeping latest timestamp
    logger.info("Deduplicating accounts")
    window_spec = Window.partitionBy(pk, prt).orderBy(
        F.col(ts).desc(),
        F.col("insert_ts").desc() if "insert_ts" in accounts_df.columns else F.lit(1),
        F.col("update_ts").desc() if "update_ts" in accounts_df.columns else F.lit(1)
    )
    
    accounts_deduped = (
        accounts_df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    # Add month integer for comparison
    accounts_deduped = accounts_deduped.withColumn(
        "month_int",
        F.expr(month_to_int_expr(prt))
    )
    
    # Load summary metadata (small - can broadcast)
    logger.info(f"Loading summary metadata from {config.latest_summary_table}")
    try:
        summary_meta = spark.sql(f"""
            SELECT 
                {pk},
                {prt} as max_existing_month,
                {ts} as max_existing_ts,
                {month_to_int_expr(prt)} as max_month_int
            FROM {config.latest_summary_table}
        """)
        meta_count = summary_meta.count()
        logger.info(f"Loaded metadata for {meta_count:,} existing accounts")
    except Exception as e:
        logger.warning(f"Could not load summary metadata: {e}")
        # No existing summary - all are Case I
        return accounts_deduped.withColumn("case_type", F.lit("CASE_I"))
    
    # Classify records
    logger.info("Classifying accounts into Case I/II/III")
    classified = (
        accounts_deduped.alias("n")
        .join(
            F.broadcast(summary_meta.alias("s")),
            F.col(f"n.{pk}") == F.col(f"s.{pk}"),
            "left"
        )
        .select(
            F.col("n.*"),
            F.col("s.max_existing_month"),
            F.col("s.max_existing_ts"),
            F.col("s.max_month_int")
        )
        .withColumn(
            "case_type",
            F.when(F.col("max_existing_month").isNull(), F.lit("CASE_I"))
            .when(F.col("month_int") > F.col("max_month_int"), F.lit("CASE_II"))
            .otherwise(F.lit("CASE_III"))
        )
        .withColumn(
            "MONTH_DIFF",
            F.when(
                F.col("case_type") == "CASE_II",
                F.col("month_int") - F.col("max_month_int")
            ).otherwise(F.lit(1))
        )
    )
    
    # Cache for multiple filters
    classified.persist(config.cache_level)
    
    # Log case distribution
    case_stats = classified.groupBy("case_type").count().collect()
    logger.info("-" * 60)
    logger.info("CLASSIFICATION RESULTS:")
    for row in case_stats:
        logger.info(f"  {row['case_type']}: {row['count']:,} records")
    logger.info("-" * 60)
    
    return classified


# ============================================================================
# STEP 2: PROCESS CASE I (NEW ACCOUNTS)
# ============================================================================

def process_case_i(case_i_df, config: PipelineConfig):
    """
    Process Case I - create initial arrays for new accounts
    
    Arrays are initialized as: [current_value, NULL, NULL, ..., NULL] (36 elements)
    """
    logger.info("=" * 80)
    logger.info("STEP 2a: Process Case I (New Accounts)")
    logger.info("=" * 80)
    
    count = case_i_df.count()
    if count == 0:
        logger.info("No Case I records to process")
        return None
    
    logger.info(f"Processing {count:,} new accounts")
    
    result = case_i_df
    
    # Create initial arrays for each history column
    for array_name, source_col in config.history_arrays:
        # Array: [current_value, NULL, NULL, ..., NULL]
        null_array = ", ".join(["NULL" for _ in range(HISTORY_LENGTH - 1)])
        result = result.withColumn(
            array_name,
            F.expr(f"array({source_col}, {null_array})")
        )
    
    # Generate grid columns
    for gc in config.grid_columns:
        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                gc.get('separator', ''),
                F.transform(
                    F.col(gc['source_history']),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(gc.get('placeholder', '?')))
                )
            )
        )
    
    # Drop classification columns
    drop_cols = ["month_int", "max_existing_month", "max_existing_ts",
                 "max_month_int", "case_type", "MONTH_DIFF"]
    result = result.drop(*[c for c in drop_cols if c in result.columns])
    
    logger.info(f"Case I complete: {count:,} new accounts with initial arrays")
    return result


# ============================================================================
# STEP 3: PROCESS CASE II (FORWARD ENTRIES)
# ============================================================================

def process_case_ii(spark: SparkSession, case_ii_df, config: PipelineConfig):
    """
    Process Case II - shift existing arrays for forward entries
    
    Logic:
    - MONTH_DIFF == 1: [new_value, prev[0:35]]
    - MONTH_DIFF > 1:  [new_value, NULLs_for_gap, prev[0:35-gap]]
    """
    logger.info("=" * 80)
    logger.info("STEP 2b: Process Case II (Forward Entries)")
    logger.info("=" * 80)
    
    count = case_ii_df.count()
    if count == 0:
        logger.info("No Case II records to process")
        return None
    
    logger.info(f"Processing {count:,} forward entries")
    
    pk = config.primary_key
    
    # Get affected accounts
    affected_keys = case_ii_df.select(pk).distinct()
    affected_count = affected_keys.count()
    
    # Load latest summary for affected accounts
    logger.info(f"Loading latest summary for {affected_count:,} accounts")
    
    # Select only needed columns from latest summary
    history_cols = [arr_name for arr_name, _ in config.history_arrays]
    latest_summary = spark.read.table(config.latest_summary_table).select(
        pk, config.partition_key, *history_cols
    )
    
    # Use broadcast for account filtering if small enough
    if affected_count < config.broadcast_threshold:
        latest_for_affected = latest_summary.join(
            F.broadcast(affected_keys),
            pk,
            "inner"
        )
    else:
        latest_for_affected = latest_summary.join(
            affected_keys,
            pk,
            "inner"
        )
    
    # Create temp views for SQL
    case_ii_df.createOrReplaceTempView("case_ii_records")
    latest_for_affected.createOrReplaceTempView("latest_summary_affected")
    
    # Build SQL for array shifting
    shift_exprs = []
    for array_name, source_col in config.history_arrays:
        shift_expr = f"""
            CASE
                WHEN c.MONTH_DIFF = 1 THEN
                    slice(concat(array(c.{source_col}), p.{array_name}), 1, {HISTORY_LENGTH})
                WHEN c.MONTH_DIFF > 1 THEN
                    slice(
                        concat(
                            array(c.{source_col}),
                            array_repeat(NULL, c.MONTH_DIFF - 1),
                            p.{array_name}
                        ),
                        1, {HISTORY_LENGTH}
                    )
                ELSE
                    concat(array(c.{source_col}), array_repeat(NULL, {HISTORY_LENGTH - 1}))
            END as {array_name}
        """
        shift_exprs.append(shift_expr)
    
    # Get non-array columns from current record
    exclude_cols = {"month_int", "max_existing_month", "max_existing_ts",
                   "max_month_int", "case_type", "MONTH_DIFF"}
    exclude_cols.update(history_cols)
    current_cols = [c for c in case_ii_df.columns if c not in exclude_cols]
    current_select = ", ".join([f"c.{col}" for col in current_cols])
    
    sql = f"""
        SELECT 
            {current_select},
            {', '.join(shift_exprs)}
        FROM case_ii_records c
        JOIN latest_summary_affected p ON c.{pk} = p.{pk}
    """
    
    result = spark.sql(sql)
    
    # Generate grid columns
    for gc in config.grid_columns:
        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                gc.get('separator', ''),
                F.transform(
                    F.col(gc['source_history']),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(gc.get('placeholder', '?')))
                )
            )
        )
    
    logger.info(f"Case II complete: {count:,} forward entries with shifted arrays")
    return result


# ============================================================================
# STEP 4: PROCESS CASE III (BACKFILL)
# ============================================================================

def process_case_iii(spark: SparkSession, case_iii_df, config: PipelineConfig):
    """
    Process Case III - rebuild history arrays for backfill
    
    Uses SQL transform() for efficient array updates:
    1. Join backfill records with existing summary
    2. Calculate position in array (month_diff)
    3. Update array at position using transform()
    """
    logger.info("=" * 80)
    logger.info("STEP 2c: Process Case III (Backfill)")
    logger.info("=" * 80)
    
    count = case_iii_df.count()
    if count == 0:
        logger.info("No Case III records to process")
        return None
    
    logger.info(f"Processing {count:,} backfill records")
    
    pk = config.primary_key
    prt = config.partition_key
    ts = config.timestamp_key
    
    # Get affected accounts
    affected_accounts = case_iii_df.select(pk).distinct()
    affected_count = affected_accounts.count()
    logger.info(f"Affected accounts: {affected_count:,}")
    
    # Load summary for affected accounts
    history_cols = [arr_name for arr_name, _ in config.history_arrays]
    summary_df = spark.read.table(config.summary_table).select(
        pk, prt, ts, *history_cols
    )
    
    # Filter to affected accounts (use broadcast if small)
    if affected_count < config.broadcast_threshold:
        summary_filtered = summary_df.join(
            F.broadcast(affected_accounts),
            pk,
            "left_semi"
        )
    else:
        summary_filtered = summary_df.join(
            affected_accounts,
            pk,
            "left_semi"
        )
    
    # Join backfill with summary
    case_iii_df.createOrReplaceTempView("backfill_records")
    summary_filtered.createOrReplaceTempView("summary_affected")
    
    # Calculate month difference (position in array)
    joined = spark.sql(f"""
        SELECT 
            b.*,
            s.{prt} as summary_month,
            s.{ts} as summary_ts,
            {', '.join([f's.{arr} as existing_{arr}' for arr in history_cols])},
            (
                {month_to_int_expr(f"s.{prt}")} - {month_to_int_expr(f"b.{prt}")}
            ) as backfill_position
        FROM backfill_records b
        JOIN summary_affected s ON b.{pk} = s.{pk}
        WHERE {month_to_int_expr(f"s.{prt}")} > {month_to_int_expr(f"b.{prt}")}
          AND ({month_to_int_expr(f"s.{prt}")} - {month_to_int_expr(f"b.{prt}")}) < {HISTORY_LENGTH}
    """)
    
    joined.persist(config.cache_level)
    joined_count = joined.count()
    logger.info(f"Valid backfill joins: {joined_count:,}")
    
    if joined_count == 0:
        logger.info("No valid backfill records (all outside 36-month window)")
        return None
    
    joined.createOrReplaceTempView("backfill_joined")
    
    # Build array update expressions using transform()
    update_exprs = []
    for array_name, source_col in config.history_arrays:
        update_expr = f"""
            transform(
                existing_{array_name},
                (x, i) -> IF(i = backfill_position, {source_col}, x)
            ) as {array_name}
        """
        update_exprs.append(update_expr)
    
    # Get columns to preserve
    exclude_cols = {"month_int", "max_existing_month", "max_existing_ts",
                   "max_month_int", "case_type", "MONTH_DIFF", "summary_month",
                   "summary_ts", "backfill_position"}
    exclude_cols.update([f"existing_{arr}" for arr in history_cols])
    preserve_cols = [c for c in joined.columns if c not in exclude_cols]
    preserve_cols.remove(pk)  # Will add explicitly
    
    # Final SQL with updated arrays
    result = spark.sql(f"""
        SELECT 
            {pk},
            summary_month as {prt},
            {', '.join(update_exprs)},
            summary_ts as {ts}
        FROM backfill_joined
    """)
    
    # Group by account+month and keep latest (in case of multiple backfills)
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
    result = (
        result
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    # Generate grid columns
    for gc in config.grid_columns:
        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                gc.get('separator', ''),
                F.transform(
                    F.col(gc['source_history']),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(gc.get('placeholder', '?')))
                )
            )
        )
    
    joined.unpersist()
    
    logger.info(f"Case III complete: rebuilt arrays for {result.count():,} summary records")
    return result


# ============================================================================
# STEP 5: WRITE RESULTS
# ============================================================================

def write_results(spark: SparkSession, result_df, config: PipelineConfig, 
                  mode: str = "merge"):
    """
    Write results to summary and latest_summary tables
    
    Args:
        mode: "merge" (update existing) or "overwrite" (replace all)
    """
    if result_df is None or result_df.count() == 0:
        logger.info("No results to write")
        return 0
    
    logger.info("=" * 80)
    logger.info("STEP 3: Write Results")
    logger.info("=" * 80)
    
    pk = config.primary_key
    prt = config.partition_key
    
    result_df.createOrReplaceTempView("pipeline_result")
    count = result_df.count()
    
    if mode == "merge":
        # MERGE to summary table
        logger.info(f"Merging {count:,} records to {config.summary_table}")
        spark.sql(f"""
            MERGE INTO {config.summary_table} t
            USING pipeline_result s
            ON t.{pk} = s.{pk} AND t.{prt} = s.{prt}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # MERGE to latest_summary (keep only latest per account)
        logger.info(f"Updating {config.latest_summary_table}")
        spark.sql(f"""
            MERGE INTO {config.latest_summary_table} t
            USING (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY {prt} DESC) as rn
                    FROM pipeline_result
                ) WHERE rn = 1
            ) s
            ON t.{pk} = s.{pk}
            WHEN MATCHED AND s.{prt} >= t.{prt} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        # Overwrite mode
        logger.info(f"Writing {count:,} records to {config.summary_table}")
        result_df.writeTo(config.summary_table).using("iceberg").createOrReplace()
    
    logger.info(f"Write complete: {count:,} records")
    return count


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_pipeline(spark: SparkSession, config: PipelineConfig, 
                 filter_expr: str = None) -> Dict[str, Any]:
    """
    Main pipeline execution
    
    Processing Order (CRITICAL for correctness):
    1. Backfill FIRST (rebuilds history with corrections)
    2. New accounts SECOND (no dependencies)
    3. Forward LAST (uses corrected history from backfill)
    
    Args:
        spark: SparkSession
        config: PipelineConfig
        filter_expr: Optional SQL filter for accounts table
    
    Returns:
        Dict with processing statistics
    """
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE V9 - START")
    logger.info("=" * 80)
    
    stats = {
        'total_records': 0,
        'case_i_records': 0,
        'case_ii_records': 0,
        'case_iii_records': 0,
        'records_written': 0
    }
    
    try:
        # Step 1: Load and classify
        classified = load_and_classify_accounts(spark, config, filter_expr)
        
        # Get counts
        case_counts = classified.groupBy("case_type").count().collect()
        case_dict = {row["case_type"]: row["count"] for row in case_counts}
        stats['case_i_records'] = case_dict.get('CASE_I', 0)
        stats['case_ii_records'] = case_dict.get('CASE_II', 0)
        stats['case_iii_records'] = case_dict.get('CASE_III', 0)
        stats['total_records'] = sum(case_dict.values())
        
        # Configure partitions based on total records
        num_partitions = calculate_partitions(stats['total_records'])
        configure_spark(spark, num_partitions)
        
        # Process in CORRECT order: Backfill -> New -> Forward
        
        # 1. Process Case III (Backfill) - HIGHEST PRIORITY
        case_iii_df = classified.filter(F.col("case_type") == "CASE_III")
        if stats['case_iii_records'] > 0:
            logger.info(f"\n>>> PROCESSING BACKFILL ({stats['case_iii_records']:,} records)")
            result = process_case_iii(spark, case_iii_df, config)
            if result is not None:
                stats['records_written'] += write_results(spark, result, config)
        
        # 2. Process Case I (New Accounts)
        case_i_df = classified.filter(F.col("case_type") == "CASE_I")
        if stats['case_i_records'] > 0:
            logger.info(f"\n>>> PROCESSING NEW ACCOUNTS ({stats['case_i_records']:,} records)")
            result = process_case_i(case_i_df, config)
            if result is not None:
                stats['records_written'] += write_results(spark, result, config)
        
        # 3. Process Case II (Forward Entries) - LOWEST PRIORITY
        case_ii_df = classified.filter(F.col("case_type") == "CASE_II")
        if stats['case_ii_records'] > 0:
            logger.info(f"\n>>> PROCESSING FORWARD ENTRIES ({stats['case_ii_records']:,} records)")
            result = process_case_ii(spark, case_ii_df, config)
            if result is not None:
                stats['records_written'] += write_results(spark, result, config)
        
        # Cleanup
        classified.unpersist()
        
        end_time = time.time()
        duration = (end_time - start_time) / 60
        
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE V9 - COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Total records:      {stats['total_records']:,}")
        logger.info(f"  Case I (new):     {stats['case_i_records']:,}")
        logger.info(f"  Case II (fwd):    {stats['case_ii_records']:,}")
        logger.info(f"  Case III (bkfl):  {stats['case_iii_records']:,}")
        logger.info(f"Records written:    {stats['records_written']:,}")
        logger.info(f"Duration:           {duration:.2f} minutes")
        logger.info("=" * 80)
        
        stats['duration_minutes'] = duration
        return stats
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    
    finally:
        spark.catalog.clearCache()


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Summary Pipeline v9")
    parser.add_argument('--config', help='Path to pipeline config JSON')
    parser.add_argument('--filter', help='SQL filter expression for accounts')
    
    args = parser.parse_args()
    
    # Create config
    config = PipelineConfig(args.config)
    if not config.validate():
        sys.exit(1)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SummaryPipeline_v9") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "100m") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        stats = run_pipeline(spark, config, args.filter)
        logger.info(f"Pipeline completed: {stats}")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
