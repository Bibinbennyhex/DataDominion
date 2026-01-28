"""
Summary Pipeline v8.0 - Simplified Single-File Implementation
==============================================================

A streamlined, function-only implementation of the DPD Summary Pipeline.
Combines all processing logic into a single optimized file without classes.

Features:
- Single file, no classes - just pure functions
- Config-driven with JSON configuration
- Handles all 3 cases: New accounts, Forward entries, Backfill
- Optimized Spark configurations
- Dynamic partitioning
- Iceberg table optimization

Usage:
    python summary_pipeline.py --config pipeline_config.json --mode incremental
    python summary_pipeline.py --config pipeline_config.json --mode initial --start 2024-01 --end 2025-12
"""

import json
import math
import logging
import argparse
import sys
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType, DecimalType, DateType

# =============================================================================
# LOGGING SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SummaryPipeline.v8")

# =============================================================================
# CONSTANTS
# =============================================================================

TARGET_RECORDS_PER_PARTITION = 5_000_000
MIN_PARTITIONS = 32
MAX_PARTITIONS = 16384
AVG_RECORD_SIZE_BYTES = 200
MAX_FILE_SIZE_MB = 256
OPTIMIZATION_INTERVAL = 12
HISTORY_LENGTH = 36

TYPE_MAP = {
    "Integer": IntegerType(),
    "String": StringType(),
    "Decimal": DecimalType(18, 2),
    "Date": DateType()
}

# =============================================================================
# CONFIGURATION FUNCTIONS
# =============================================================================

def load_config(config_path: str) -> Dict[str, Any]:
    """Load pipeline configuration from JSON file."""
    logger.info(f"Loading configuration from: {config_path}")
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required sections
    required = ['tables', 'keys', 'columns', 'rolling_columns']
    for section in required:
        if section not in config:
            raise ValueError(f"Missing required config section: {section}")
    
    return config


def get_spark_type(data_type: str):
    """Get PySpark type from string type name."""
    return TYPE_MAP.get(data_type, StringType())


# =============================================================================
# SPARK SESSION FUNCTIONS
# =============================================================================

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create and configure Spark session with optimal settings."""
    app_name = config.get('spark', {}).get('app_name', 'SummaryPipeline_v8')
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.sql.autoBroadcastJoinThreshold", "100m") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "8") \
        .config("spark.dynamicAllocation.maxExecutors", "200") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
        .config("spark.sql.sources.bucketing.enabled", "true") \
        .enableHiveSupport()
    
    # Add catalog config if present
    if 'catalog' in config:
        cat = config['catalog']
        builder = builder \
            .config("spark.sql.defaultCatalog", cat.get('name', 'primary_catalog')) \
            .config(f"spark.sql.catalog.{cat.get('name', 'primary_catalog')}", 
                   "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{cat.get('name', 'primary_catalog')}.type", 
                   cat.get('type', 'glue')) \
            .config(f"spark.sql.catalog.{cat.get('name', 'primary_catalog')}.io-impl", 
                   "org.apache.iceberg.aws.s3.S3FileIO")
    
    return builder.getOrCreate()


def configure_partitions(spark: SparkSession, num_partitions: int):
    """Configure Spark for optimal partition count."""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))
    logger.info(f"Configured Spark for {num_partitions} partitions")


def calculate_optimal_partitions(record_count: int, avg_size: int = AVG_RECORD_SIZE_BYTES) -> int:
    """Calculate optimal partition count based on data size."""
    if record_count == 0:
        return MIN_PARTITIONS
    
    partitions_by_count = record_count / TARGET_RECORDS_PER_PARTITION
    data_size_mb = (record_count * avg_size) / (1024 * 1024)
    partitions_by_size = data_size_mb / MAX_FILE_SIZE_MB
    
    optimal = max(partitions_by_count, partitions_by_size)
    power_of_2 = 2 ** round(math.log2(max(1, optimal)))
    
    return int(max(MIN_PARTITIONS, min(MAX_PARTITIONS, power_of_2)))


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def month_to_int(month_str: str) -> int:
    """Convert YYYY-MM string to integer for comparison."""
    parts = month_str.split('-')
    return int(parts[0]) * 12 + int(parts[1])


def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM column to integer."""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def replace_invalid_dates(df: DataFrame, date_columns: List[str]) -> DataFrame:
    """Replace dates with invalid years (< 1000) with NULL."""
    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                F.when(F.year(F.col(col_name)) < 1000, None)
                 .otherwise(F.to_date(F.col(col_name)))
            )
    return df


# =============================================================================
# ARRAY BUILDING FUNCTIONS
# =============================================================================

def create_initial_array(value_column: str, data_type: str, num_cols: int = HISTORY_LENGTH) -> F.Column:
    """
    Create initial history array: [value, NULL, NULL, ...].
    Used for Case I (new accounts).
    """
    spark_type = get_spark_type(data_type)
    return F.array(
        F.col(value_column).cast(spark_type),
        *[F.lit(None).cast(spark_type) for _ in range(num_cols - 1)]
    )


def create_shifted_array(
    history_col: str,
    value_column: str,
    data_type: str,
    num_cols: int = HISTORY_LENGTH,
    current_alias: str = "c",
    previous_alias: str = "p"
) -> F.Column:
    """
    Create shifted history array with gap handling.
    Used for Case II (forward entries).
    
    Logic:
    - MONTH_DIFF == 1: Normal shift [new, prev[0:n-1]]
    - MONTH_DIFF > 1:  Gap handling [new, NULLs for gap, prev[0:n-gap-1]]
    - Otherwise:       New account [new, NULL, NULL, ...]
    """
    spark_type = get_spark_type(data_type)
    
    # Current value with NULL fallback
    current_value = F.coalesce(
        F.col(f"{current_alias}.{value_column}").cast(spark_type),
        F.lit(None).cast(spark_type)
    )
    
    # Previous history with empty array fallback
    prev_history = F.coalesce(
        F.col(f"{previous_alias}.{history_col}"),
        F.array(*[F.lit(None).cast(spark_type) for _ in range(num_cols)])
    )
    
    # Null array for gap filling
    null_array = F.array_repeat(
        F.lit(None).cast(spark_type),
        F.col("MONTH_DIFF") - F.lit(1)
    )
    
    # New account array (no previous history)
    new_account_array = F.array(
        current_value,
        *[F.lit(None).cast(spark_type) for _ in range(num_cols - 1)]
    )
    
    return (
        F.when(
            F.col("MONTH_DIFF") == 1,
            # Normal: prepend current, keep n-1 from previous
            F.slice(
                F.concat(F.array(current_value), prev_history),
                1, num_cols
            )
        )
        .when(
            F.col("MONTH_DIFF") > 1,
            # Gap: prepend current, add null gaps, then previous
            F.slice(
                F.concat(
                    F.array(current_value),
                    null_array,
                    prev_history
                ),
                1, num_cols
            )
        )
        .otherwise(new_account_array)
    )


def generate_grid_column(
    df: DataFrame,
    grid_name: str,
    source_history_col: str,
    null_placeholder: str = "?",
    separator: str = ""
) -> DataFrame:
    """
    Generate grid column from rolling history array.
    Transforms array like ["0", "1", NULL, "2"] into string like "01?2".
    """
    if source_history_col not in df.columns:
        logger.warning(f"History column {source_history_col} not found, skipping grid generation")
        return df
    
    return df.withColumn(
        grid_name,
        F.concat_ws(
            separator,
            F.transform(
                F.col(source_history_col),
                lambda x: F.coalesce(x.cast(StringType()), F.lit(null_placeholder))
            )
        )
    )


# =============================================================================
# DATA PREPARATION FUNCTIONS
# =============================================================================

def prepare_source_data(
    df: DataFrame,
    column_mappings: Dict[str, str],
    primary_key: str,
    partition_key: str,
    timestamp_key: str,
    transformations: List[Dict[str, str]] = None
) -> DataFrame:
    """
    Prepare source data with column mappings, deduplication, and transformations.
    """
    # Apply column mappings
    select_exprs = [F.col(src).alias(dst) for src, dst in column_mappings.items()]
    result = df.select(*select_exprs)
    
    # Deduplicate by primary key + partition, keeping latest timestamp
    window_spec = Window.partitionBy(primary_key, partition_key) \
                        .orderBy(F.col(timestamp_key).desc())
    
    result = result \
        .withColumn("_rn", F.row_number().over(window_spec)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")
    
    # Apply transformations
    if transformations:
        for transform in transformations:
            result = result.withColumn(transform['name'], F.expr(transform['expr']))
    
    return result


def apply_rolling_mappers(
    df: DataFrame,
    rolling_columns: List[Dict[str, Any]]
) -> DataFrame:
    """Apply mapper expressions to prepare rolling column values."""
    result = df
    for rc in rolling_columns:
        result = result.withColumn(
            rc['source_column'],
            F.expr(rc['mapper_expr'])
        )
    return result


# =============================================================================
# CLASSIFICATION FUNCTIONS
# =============================================================================

def classify_records(
    spark: SparkSession,
    batch_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Classify records into Case I, II, III.
    
    Case I:   New accounts (no existing data in summary)
    Case II:  Forward entries (month > max existing month)
    Case III: Backfill (month <= max existing month, with newer timestamp)
    
    Returns DataFrame with case_type, month_gap, MONTH_DIFF columns added.
    """
    logger.info("Classifying records into Case I/II/III...")
    
    primary = config['keys']['primary']
    partition = config['keys']['partition']
    timestamp = config['keys']['timestamp']
    summary_table = config['tables']['summary']
    
    # Add month integer for comparison
    batch_with_month = batch_df.withColumn(
        "month_int",
        F.expr(month_to_int_expr(partition))
    )
    
    # Get summary metadata per account (max month, max timestamp)
    try:
        summary_meta = spark.sql(f"""
            SELECT 
                {primary},
                MAX({partition}) as max_existing_month,
                MAX({timestamp}) as max_existing_ts,
                {month_to_int_expr(f"MAX({partition})")} as max_month_int
            FROM {summary_table}
            GROUP BY {primary}
        """)
        meta_count = summary_meta.count()
        logger.info(f"Loaded metadata for {meta_count:,} existing accounts")
    except Exception as e:
        logger.warning(f"Could not load summary metadata (table may not exist): {e}")
        # No existing summary - all records are Case I
        return batch_with_month \
            .withColumn("case_type", F.lit("CASE_I")) \
            .withColumn("month_gap", F.lit(0)) \
            .withColumn("MONTH_DIFF", F.lit(1)) \
            .withColumn("max_existing_month", F.lit(None)) \
            .withColumn("max_existing_ts", F.lit(None)) \
            .withColumn("max_month_int", F.lit(None))
    
    # Join and classify
    classified = batch_with_month.alias("n").join(
        F.broadcast(summary_meta.alias("s")),
        F.col(f"n.{primary}") == F.col(f"s.{primary}"),
        "left"
    ).select(
        F.col("n.*"),
        F.col("s.max_existing_month"),
        F.col("s.max_existing_ts"),
        F.col("s.max_month_int")
    )
    
    # Apply classification logic
    classified = classified.withColumn(
        "case_type",
        F.when(
            F.col("max_existing_month").isNull(),
            F.lit("CASE_I")
        ).when(
            F.col("month_int") > F.col("max_month_int"),
            F.lit("CASE_II")
        ).otherwise(
            F.lit("CASE_III")
        )
    )
    
    # Calculate month gap for Case II
    classified = classified.withColumn(
        "month_gap",
        F.when(
            F.col("case_type") == "CASE_II",
            F.col("month_int") - F.col("max_month_int") - 1
        ).otherwise(F.lit(0))
    )
    
    # MONTH_DIFF for array builder (gap + 1)
    classified = classified.withColumn(
        "MONTH_DIFF",
        F.when(
            F.col("case_type") == "CASE_II",
            F.col("month_gap") + 1
        ).otherwise(F.lit(1))
    )
    
    # Persist for multiple filters
    classified = classified.persist()
    
    # Log classification counts
    case_counts = classified.groupBy("case_type").count().collect()
    case_dict = {row["case_type"]: row["count"] for row in case_counts}
    
    logger.info("-" * 60)
    logger.info("CLASSIFICATION RESULTS:")
    logger.info(f"  Case I   (New accounts):    {case_dict.get('CASE_I', 0):>12,}")
    logger.info(f"  Case II  (Forward entries): {case_dict.get('CASE_II', 0):>12,}")
    logger.info(f"  Case III (Backfill):        {case_dict.get('CASE_III', 0):>12,}")
    logger.info(f"  TOTAL:                      {sum(case_dict.values()):>12,}")
    logger.info("-" * 60)
    
    return classified


# =============================================================================
# CASE I PROCESSOR (NEW ACCOUNTS)
# =============================================================================

def process_case_i(
    case_i_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Process Case I records - create initial arrays for new accounts.
    """
    record_count = case_i_df.count()
    logger.info(f"Processing Case I (new accounts): {record_count:,} records")
    
    rolling_columns = config['rolling_columns']
    grid_columns = config.get('grid_columns', [])
    history_len = config.get('history', {}).get('length', HISTORY_LENGTH)
    
    result = case_i_df
    
    # Apply rolling column mapper expressions
    result = apply_rolling_mappers(result, rolling_columns)
    
    # Create initial history arrays
    for rc in rolling_columns:
        history_col = f"{rc['name']}_history"
        result = result.withColumn(
            history_col,
            create_initial_array(rc['source_column'], rc['data_type'], history_len)
        )
    
    # Generate grid columns
    for gc in grid_columns:
        source_history = f"{gc['source_rolling']}_history"
        result = generate_grid_column(
            result, gc['name'], source_history,
            gc.get('null_placeholder', '?'),
            gc.get('separator', '')
        )
    
    # Drop classification columns
    drop_cols = ["month_int", "max_existing_month", "max_existing_ts",
                 "max_month_int", "case_type", "month_gap", "MONTH_DIFF"]
    result = result.drop(*[c for c in drop_cols if c in result.columns])
    
    logger.info("Case I processing complete - initial arrays created")
    return result


# =============================================================================
# CASE II PROCESSOR (FORWARD ENTRIES)
# =============================================================================

def process_case_ii(
    spark: SparkSession,
    case_ii_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Process Case II records - shift existing arrays for forward entries.
    """
    record_count = case_ii_df.count()
    logger.info(f"Processing Case II (forward entries): {record_count:,} records")
    
    primary = config['keys']['primary']
    rolling_columns = config['rolling_columns']
    grid_columns = config.get('grid_columns', [])
    latest_summary_table = config['tables']['latest_summary']
    history_len = config.get('history', {}).get('length', HISTORY_LENGTH)
    
    # Get affected accounts
    affected_keys = case_ii_df.select(primary).distinct()
    
    # Get latest summary for affected accounts
    latest_summary = spark.read.table(latest_summary_table).alias("ls")
    latest_for_affected = latest_summary.join(
        affected_keys.alias("ak"),
        F.col(f"ls.{primary}") == F.col(f"ak.{primary}"),
        "inner"
    ).select("ls.*")
    
    logger.info(f"Loaded latest summary for {latest_for_affected.count():,} accounts")
    
    result = case_ii_df
    
    # Apply rolling column mapper expressions
    result = apply_rolling_mappers(result, rolling_columns)
    
    # Join with latest summary
    joined = result.alias("c").join(
        latest_for_affected.alias("p"),
        F.col(f"c.{primary}") == F.col(f"p.{primary}"),
        "inner"
    )
    
    # Build select expressions
    classification_cols = ["month_int", "max_existing_month", "max_existing_ts",
                          "max_month_int", "case_type", "month_gap"]
    
    select_exprs = []
    for col in case_ii_df.columns:
        if col not in classification_cols:
            select_exprs.append(F.col(f"c.{col}"))
    
    # Add shifted history arrays
    for rc in rolling_columns:
        history_col = f"{rc['name']}_history"
        select_exprs.append(
            create_shifted_array(
                history_col,
                rc['source_column'],
                rc['data_type'],
                history_len,
                "c", "p"
            ).alias(history_col)
        )
    
    result = joined.select(*select_exprs)
    
    # Drop MONTH_DIFF if present
    if "MONTH_DIFF" in result.columns:
        result = result.drop("MONTH_DIFF")
    
    # Generate grid columns
    for gc in grid_columns:
        source_history = f"{gc['source_rolling']}_history"
        result = generate_grid_column(
            result, gc['name'], source_history,
            gc.get('null_placeholder', '?'),
            gc.get('separator', '')
        )
    
    logger.info("Case II processing complete - arrays shifted")
    return result


# =============================================================================
# CASE III PROCESSOR (BACKFILL)
# =============================================================================

def process_case_iii(
    spark: SparkSession,
    case_iii_df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """
    Process Case III records - rebuild history arrays for backfill.
    
    Uses a 7-CTE approach to:
    1. Validate backfill records (newer timestamp wins)
    2. Get all existing data for affected accounts
    3. Combine backfill + existing (backfill has priority)
    4. Deduplicate keeping winner
    5. Collect all months per account
    6. Explode back to one row per month
    7. Rebuild history arrays for each month
    """
    record_count = case_iii_df.count()
    logger.info(f"Processing Case III (backfill): {record_count:,} records")
    
    primary = config['keys']['primary']
    partition = config['keys']['partition']
    timestamp = config['keys']['timestamp']
    summary_table = config['tables']['summary']
    rolling_columns = config['rolling_columns']
    grid_columns = config.get('grid_columns', [])
    history_len = config.get('history', {}).get('length', HISTORY_LENGTH)
    
    # Get affected accounts
    affected_accounts = case_iii_df.select(primary).distinct()
    affected_count = affected_accounts.count()
    affected_months = case_iii_df.select(partition).distinct().count()
    
    logger.info(f"Affected: {affected_count:,} accounts, {affected_months} unique months")
    logger.info("Rebuilding history arrays with corrected values...")
    
    # Apply rolling column mapper expressions
    result = apply_rolling_mappers(case_iii_df, rolling_columns)
    
    # Create temp views
    result.createOrReplaceTempView("backfill_records")
    affected_accounts.createOrReplaceTempView("affected_accounts")
    
    # Identify columns to preserve
    exclude_cols = [primary, partition, timestamp, "month_int", "case_type",
                   "month_gap", "max_existing_month", "max_existing_ts", 
                   "max_month_int", "MONTH_DIFF"]
    preserve_cols = [c for c in result.columns if c not in exclude_cols]
    
    # Build rolling column SQL for history array rebuild
    rolling_col_history = []
    for rc in rolling_columns:
        sql_type = "INT" if rc['data_type'] == "Integer" else "STRING"
        rolling_col_history.append(f"""
            TRANSFORM(
                SEQUENCE(0, {history_len - 1}),
                offset -> (
                    AGGREGATE(
                        FILTER(e.month_values, mv -> mv.month_int = e.month_int - offset),
                        CAST(NULL AS {sql_type}),
                        (acc, x) -> x.{rc['source_column']}
                    )
                )
            ) as {rc['name']}_history
        """)
    
    # Build scalar extraction SQL for non-rolling columns
    scalar_extract = []
    rolling_sources = [rc['source_column'] for rc in rolling_columns]
    for col in preserve_cols:
        if col not in rolling_sources:
            scalar_extract.append(
                f"element_at(FILTER(e.month_values, mv -> mv.month_int = e.month_int), 1).{col} as {col}"
            )
    
    struct_cols = ", ".join(preserve_cols)
    
    # Build existing data column selection
    existing_cols = []
    for col in preserve_cols:
        if col in rolling_sources:
            rc = next(r for r in rolling_columns if r['source_column'] == col)
            existing_cols.append(f"s.{rc['name']}_history[0] as {col}")
        else:
            existing_cols.append(f"s.{col}")
    
    # Build the combined SQL query with 7 CTEs
    combined_sql = f"""
    WITH 
    -- CTE 1: Validate backfill records (only keep newer timestamps)
    backfill_validated AS (
        SELECT b.*
        FROM backfill_records b
        LEFT JOIN (
            SELECT {primary}, {partition}, {timestamp}
            FROM {summary_table}
            WHERE {primary} IN (SELECT {primary} FROM affected_accounts)
        ) e ON b.{primary} = e.{primary} AND b.{partition} = e.{partition}
        WHERE e.{timestamp} IS NULL OR b.{timestamp} > e.{timestamp}
    ),
    
    -- CTE 2: Get all existing data for affected accounts
    existing_data AS (
        SELECT 
            s.{primary},
            s.{partition},
            {month_to_int_expr(f"s.{partition}")} as month_int,
            {', '.join(existing_cols)},
            s.{timestamp},
            2 as priority
        FROM {summary_table} s
        WHERE s.{primary} IN (SELECT {primary} FROM affected_accounts)
    ),
    
    -- CTE 3: Combine backfill + existing (backfill has higher priority)
    combined_data AS (
        SELECT 
            {primary}, {partition}, month_int,
            {struct_cols},
            {timestamp},
            1 as priority
        FROM backfill_validated
        
        UNION ALL
        
        SELECT * FROM existing_data
    ),
    
    -- CTE 4: Deduplicate (keep backfill if exists, else existing)
    deduped AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY {primary}, {partition}
                ORDER BY priority, {timestamp} DESC
            ) as rn
        FROM combined_data
    ),
    
    -- CTE 5: Keep only winning rows
    final_data AS (
        SELECT * FROM deduped WHERE rn = 1
    ),
    
    -- CTE 6: Collect all months per account into arrays
    account_months AS (
        SELECT 
            {primary},
            COLLECT_LIST(STRUCT(month_int, {struct_cols})) as month_values,
            COLLECT_LIST(STRUCT({partition}, month_int, {timestamp})) as month_metadata
        FROM final_data
        GROUP BY {primary}
    ),
    
    -- CTE 7: Explode back to one row per month (with full history available)
    exploded AS (
        SELECT 
            {primary},
            month_values,
            meta.{partition},
            meta.month_int,
            meta.{timestamp}
        FROM account_months
        LATERAL VIEW EXPLODE(month_metadata) t AS meta
    )
    
    -- Final: Rebuild history arrays for each month
    SELECT
        e.{primary},
        e.{partition},
        {', '.join(rolling_col_history)},
        {', '.join(scalar_extract) if scalar_extract else f'e.{timestamp}'},
        e.{timestamp}
    FROM exploded e
    """
    
    result = spark.sql(combined_sql)
    
    # Generate grid columns
    for gc in grid_columns:
        source_history = f"{gc['source_rolling']}_history"
        result = generate_grid_column(
            result, gc['name'], source_history,
            gc.get('null_placeholder', '?'),
            gc.get('separator', '')
        )
    
    logger.info("Case III processing complete - history arrays rebuilt")
    return result


# =============================================================================
# WRITE FUNCTIONS
# =============================================================================

def write_results(
    spark: SparkSession,
    result: DataFrame,
    config: Dict[str, Any]
):
    """Write results to summary and latest_summary tables using MERGE."""
    primary = config['keys']['primary']
    partition = config['keys']['partition']
    summary_table = config['tables']['summary']
    latest_summary_table = config['tables']['latest_summary']
    
    result.createOrReplaceTempView("pipeline_result")
    
    # MERGE to summary table
    logger.info(f"Merging to {summary_table}...")
    spark.sql(f"""
        MERGE INTO {summary_table} t
        USING pipeline_result s
        ON t.{primary} = s.{primary} AND t.{partition} = s.{partition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    # MERGE to latest_summary (only latest month per account)
    logger.info(f"Merging to {latest_summary_table}...")
    
    latest_per_account = spark.sql(f"""
        SELECT * FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {primary}
                    ORDER BY {partition} DESC
                ) as rn
            FROM pipeline_result
        ) WHERE rn = 1
    """).drop("rn")
    
    latest_per_account.createOrReplaceTempView("latest_result")
    
    spark.sql(f"""
        MERGE INTO {latest_summary_table} t
        USING latest_result s
        ON t.{primary} = s.{primary}
        WHEN MATCHED AND s.{partition} >= t.{partition} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    
    count = result.count()
    logger.info(f"Written {count:,} records")
    return count


# =============================================================================
# TABLE OPTIMIZATION FUNCTIONS
# =============================================================================

def optimize_iceberg_table(spark: SparkSession, table_name: str):
    """Optimize Iceberg table by rewriting files and expiring snapshots."""
    logger.info(f"Optimizing table: {table_name}")
    
    parts = table_name.split('.')
    if len(parts) >= 3:
        catalog = parts[0]
        db = parts[1]
        table = parts[2]
    else:
        catalog = "primary_catalog"
        db = parts[0] if len(parts) > 1 else "default"
        table = parts[-1]
    
    try:
        logger.info("Rewriting data files...")
        spark.sql(f"CALL {catalog}.system.rewrite_data_files('{db}.{table}')").show()
        
        logger.info("Expiring old snapshots...")
        spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{db}.{table}', retain_last => 1)").show()
        
        logger.info(f"Table optimization complete: {table_name}")
    except Exception as e:
        logger.warning(f"Table optimization failed (non-fatal): {e}")


# =============================================================================
# INCREMENTAL MODE
# =============================================================================

def run_incremental(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run incremental processing - the production default.
    
    Automatically handles all scenarios:
    - Case I:   New accounts
    - Case II:  Forward entries
    - Case III: Backfill
    
    Processing Order (CRITICAL):
    1. Backfill FIRST (rebuilds history with corrections)
    2. New accounts SECOND (no dependencies)
    3. Forward LAST (uses corrected history from backfill)
    """
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE v8.0 - INCREMENTAL MODE")
    logger.info("=" * 80)
    
    stats = {
        'mode': 'incremental',
        'total_records': 0,
        'case_i_records': 0,
        'case_ii_records': 0,
        'case_iii_records': 0,
        'records_written': 0
    }
    
    timestamp_key = config['keys']['timestamp']
    summary_table = config['tables']['summary']
    source_table = config['tables']['source']
    
    # Get latest timestamp from summary
    try:
        max_ts_row = spark.sql(f"""
            SELECT MAX({timestamp_key}) as max_ts FROM {summary_table}
        """).collect()
        max_ts = max_ts_row[0]["max_ts"] if max_ts_row else None
    except Exception:
        max_ts = None
    
    if max_ts is None:
        logger.warning("No existing summary found. Use 'initial' mode for first load.")
        return stats
    
    logger.info(f"Latest timestamp in summary: {max_ts}")
    
    # Read new data
    logger.info("Reading new data from source...")
    batch_df = spark.sql(f"""
        SELECT * FROM {source_table}
        WHERE {timestamp_key} > TIMESTAMP '{max_ts}'
    """)
    
    record_count = batch_df.count()
    if record_count == 0:
        logger.info("No new records to process. Summary is up to date.")
        return stats
    
    stats['total_records'] = record_count
    logger.info(f"New records found: {record_count:,}")
    
    # Configure partitioning
    num_partitions = calculate_optimal_partitions(record_count)
    configure_partitions(spark, num_partitions)
    
    # Prepare and classify
    logger.info("Preparing and classifying records...")
    batch_df = prepare_source_data(
        batch_df,
        config['columns'],
        config['keys']['primary'],
        config['keys']['partition'],
        config['keys']['timestamp'],
        config.get('transformations', [])
    )
    
    classified = classify_records(spark, batch_df, config)
    
    # Get counts
    case_counts = classified.groupBy("case_type").count().collect()
    case_dict = {row["case_type"]: row["count"] for row in case_counts}
    stats['case_i_records'] = case_dict.get('CASE_I', 0)
    stats['case_ii_records'] = case_dict.get('CASE_II', 0)
    stats['case_iii_records'] = case_dict.get('CASE_III', 0)
    
    # Process in CORRECT order: Backfill -> New -> Forward
    
    # 1. Process Case III (Backfill) - HIGHEST PRIORITY
    case_iii_df = classified.filter(F.col("case_type") == "CASE_III")
    if case_iii_df.count() > 0:
        logger.info("=" * 80)
        logger.info(f"STEP 1/3: Processing {stats['case_iii_records']:,} backfill records (Case III)")
        logger.info("=" * 80)
        result = process_case_iii(spark, case_iii_df, config)
        stats['records_written'] += write_results(spark, result, config)
    
    # 2. Process Case I (New Accounts)
    case_i_df = classified.filter(F.col("case_type") == "CASE_I")
    if case_i_df.count() > 0:
        logger.info("=" * 80)
        logger.info(f"STEP 2/3: Processing {stats['case_i_records']:,} new accounts (Case I)")
        logger.info("=" * 80)
        result = process_case_i(case_i_df, config)
        stats['records_written'] += write_results(spark, result, config)
    
    # 3. Process Case II (Forward Entries) - LOWEST PRIORITY
    case_ii_df = classified.filter(F.col("case_type") == "CASE_II")
    if case_ii_df.count() > 0:
        logger.info("=" * 80)
        logger.info(f"STEP 3/3: Processing {stats['case_ii_records']:,} forward entries (Case II)")
        logger.info("=" * 80)
        result = process_case_ii(spark, case_ii_df, config)
        stats['records_written'] += write_results(spark, result, config)
    
    # Cleanup
    classified.unpersist()
    
    # Optimize tables
    logger.info("=" * 80)
    logger.info("FINALIZING - Optimizing tables")
    logger.info("=" * 80)
    optimize_iceberg_table(spark, config['tables']['latest_summary'])
    
    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60
    
    # Log summary
    logger.info("=" * 80)
    logger.info("INCREMENTAL PROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total records processed: {stats['total_records']:,}")
    logger.info(f"  Case I (new):      {stats['case_i_records']:,}")
    logger.info(f"  Case II (forward): {stats['case_ii_records']:,}")
    logger.info(f"  Case III (backfill): {stats['case_iii_records']:,}")
    logger.info(f"Records written: {stats['records_written']:,}")
    logger.info(f"Duration: {duration_minutes:.2f} minutes")
    logger.info("=" * 80)
    
    return stats


# =============================================================================
# INITIAL/SEQUENTIAL MODE
# =============================================================================

def run_initial(
    spark: SparkSession,
    config: Dict[str, Any],
    start_month: date,
    end_month: date
) -> Dict[str, Any]:
    """
    Run initial/sequential processing - month by month.
    
    Used for first-time load or sequential processing of historical data.
    """
    start_time = time.time()
    
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE v8.0 - INITIAL/SEQUENTIAL MODE")
    logger.info(f"Processing from {start_month} to {end_month}")
    logger.info("=" * 80)
    
    stats = {
        'mode': 'initial',
        'months_processed': 0,
        'total_records': 0,
        'records_written': 0
    }
    
    source_table = config['tables']['source']
    summary_table = config['tables']['summary']
    latest_summary_table = config['tables']['latest_summary']
    primary = config['keys']['primary']
    partition = config['keys']['partition']
    timestamp = config['keys']['timestamp']
    rolling_columns = config['rolling_columns']
    grid_columns = config.get('grid_columns', [])
    history_len = config.get('history', {}).get('length', HISTORY_LENGTH)
    date_columns = config.get('date_col_list', [])
    
    optimization_counter = 0
    is_initial = not spark.catalog.tableExists(summary_table)
    
    month = start_month
    while month <= end_month:
        month_start_time = time.time()
        month_str = month.strftime('%Y-%m')
        
        logger.info(f"\nProcessing month: {month_str}")
        
        # Optimize periodically
        if optimization_counter >= OPTIMIZATION_INTERVAL:
            optimize_iceberg_table(spark, latest_summary_table)
            optimization_counter = 0
        
        # Check if summary exists and validate month
        if spark.catalog.tableExists(summary_table):
            max_month = spark.table(summary_table) \
                .agg(F.max(partition).alias("max_month")) \
                .first()["max_month"]
            
            if max_month is not None:
                is_initial = False
                if max_month > month_str:
                    logger.error(f"Processing month {month_str} < max summary month {max_month}")
                    raise ValueError("Processing month less than max entry in Summary")
        
        # Read source data for this month
        accounts_df = spark.read.table(source_table) \
            .filter(F.col(partition) == month_str)
        
        # Apply column mappings
        accounts_df = prepare_source_data(
            accounts_df,
            config['columns'],
            primary,
            partition,
            timestamp,
            config.get('transformations', [])
        )
        
        current_count = accounts_df.count()
        if current_count == 0:
            logger.info(f"No records for {month_str}, skipping...")
            month = (month.replace(day=28) + relativedelta(days=4)).replace(day=1)
            continue
        
        logger.info(f"Records for {month_str}: {current_count:,}")
        stats['total_records'] += current_count
        
        # Configure partitions
        num_partitions = calculate_optimal_partitions(current_count)
        configure_partitions(spark, num_partitions)
        
        accounts_df = accounts_df.repartition(num_partitions, F.col(primary))
        accounts_df.cache()
        
        # Apply rolling mappers
        accounts_df = apply_rolling_mappers(accounts_df, rolling_columns)
        
        if is_initial:
            # First month - create initial arrays
            result = accounts_df
            for rc in rolling_columns:
                history_col = f"{rc['name']}_history"
                result = result.withColumn(
                    history_col,
                    create_initial_array(rc['source_column'], rc['data_type'], history_len)
                )
        else:
            # Subsequent months - shift arrays
            rolling_history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
            
            latest_prev = spark.read.table(latest_summary_table).select(
                primary, partition, timestamp, *rolling_history_cols
            )
            
            latest_prev = latest_prev.repartition(
                calculate_optimal_partitions(latest_prev.count()),
                F.col(primary)
            )
            
            joined = accounts_df.alias("c").join(
                latest_prev.alias("p"),
                primary,
                "left"
            )
            
            # Calculate month difference
            joined = joined.withColumn(
                "MONTH_DIFF",
                F.when(
                    F.col(f"p.{partition}").isNotNull(),
                    F.months_between(
                        F.to_date(F.lit(month_str), "yyyy-MM"),
                        F.to_date(F.col(f"p.{partition}"), "yyyy-MM")
                    ).cast(IntegerType())
                ).otherwise(1)
            )
            
            # Build select expressions
            select_exprs = [F.col(f"c.{c}") for c in accounts_df.columns]
            
            for rc in rolling_columns:
                history_col = f"{rc['name']}_history"
                select_exprs.append(
                    create_shifted_array(
                        history_col,
                        rc['source_column'],
                        rc['data_type'],
                        history_len,
                        "c", "p"
                    ).alias(history_col)
                )
            
            result = joined.select(*select_exprs)
        
        # Generate grid columns
        for gc in grid_columns:
            source_history = f"{gc['source_rolling']}_history"
            result = generate_grid_column(
                result, gc['name'], source_history,
                gc.get('null_placeholder', '?'),
                gc.get('separator', '')
            )
        
        # Replace invalid dates
        result = replace_invalid_dates(result, date_columns)
        result.cache()
        
        # Write results
        if result.count() > 0:
            stats['records_written'] += write_results(spark, result, config)
            is_initial = False
        
        # Cleanup
        accounts_df.unpersist()
        result.unpersist()
        
        month_end_time = time.time()
        month_duration = (month_end_time - month_start_time) / 60
        logger.info(f"Completed {month_str} in {month_duration:.2f} minutes")
        
        stats['months_processed'] += 1
        optimization_counter += 1
        month = (month.replace(day=28) + relativedelta(days=4)).replace(day=1)
    
    # Final optimization
    optimize_iceberg_table(spark, latest_summary_table)
    
    end_time = time.time()
    total_duration = (end_time - start_time) / 60
    
    logger.info("=" * 80)
    logger.info("INITIAL/SEQUENTIAL PROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Months processed: {stats['months_processed']}")
    logger.info(f"Total records: {stats['total_records']:,}")
    logger.info(f"Records written: {stats['records_written']:,}")
    logger.info(f"Total duration: {total_duration:.2f} minutes")
    logger.info("=" * 80)
    
    return stats


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="Summary Pipeline v8.0")
    parser.add_argument('--config', required=True, help='Path to pipeline config JSON')
    parser.add_argument('--mode', choices=['incremental', 'initial'], default='incremental',
                       help='Processing mode (default: incremental)')
    parser.add_argument('--start', help='Start month for initial mode (YYYY-MM)')
    parser.add_argument('--end', help='End month for initial mode (YYYY-MM)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Create Spark session
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        if args.mode == 'incremental':
            stats = run_incremental(spark, config)
        else:
            if not args.start or not args.end:
                logger.error("Initial mode requires --start and --end parameters")
                sys.exit(1)
            
            start_month = datetime.strptime(args.start, '%Y-%m').date().replace(day=1)
            end_month = datetime.strptime(args.end, '%Y-%m').date().replace(day=1)
            
            stats = run_initial(spark, config, start_month, end_month)
        
        logger.info(f"Pipeline completed successfully: {stats}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
