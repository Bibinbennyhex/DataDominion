"""
Summary Pipeline v9.1 FIXED - Complete Production Implementation
=================================================================

FIXED VERSION: Corrects the backfill logic to also create summary rows for 
backfill months, not just update future summaries.

BUG FIXED:
- Original v9 only updated future summaries when backfill data arrived
- This version ALSO creates a new summary row for the backfill month itself,
  inheriting historical data from the closest prior summary

FEATURES:
- Full column mapping support (source -> destination)
- Column transformations (sentinel value handling)
- Inferred/derived columns
- Date column validation
- Separate latest_summary and summary column lists
- Rolling history arrays (36-month)
- Grid column generation
- Case I/II/III handling with correct backfill logic

Enhanced version of v9 with ALL 3 cases:
- Case I:   New accounts (no existing summary)
- Case II:  Forward entries (month > max existing)
- Case III: Backfill (month <= max existing) - NOW CREATES BACKFILL MONTH ROW

Performance Improvements:
- SQL-based array updates (faster than UDF loops)
- Broadcast optimization for small tables
- Better cache management (MEMORY_AND_DISK)
- Single-pass array updates
- Configurable Spark settings
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, ArrayType, DoubleType, DecimalType, DateType
from pyspark import StorageLevel
import logging
import json
import argparse
import sys
import time
import math
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
logger = logging.getLogger("SummaryPipeline.v9.1.fixed")


# ============================================================================
# CONSTANTS
# ============================================================================

HISTORY_LENGTH = 36
TARGET_RECORDS_PER_PARTITION = 5_000_000
MIN_PARTITIONS = 32
MAX_PARTITIONS = 16384

TYPE_MAP = {
    "Integer": IntegerType(),
    "String": StringType(),
    "Decimal": DecimalType(18, 2),
    "Date": DateType()
}


# ============================================================================
# CONFIGURATION
# ============================================================================

class PipelineConfig:
    """Configuration for summary pipeline - Full production support"""
    
    def __init__(self, config_path: str = None):
        # Default table names
        self.accounts_table = "spark_catalog.edf_gold.ivaps_consumer_accounts_all"
        self.latest_summary_table = "ascend_iceberg.ascenddb.latest_summary"
        self.summary_table = "ascend_iceberg.ascenddb.summary"
        
        # Keys
        self.primary_key = "cons_acct_key"
        self.partition_key = "rpt_as_of_mo"
        self.timestamp_key = "base_ts"
        self.primary_date_key = "acct_dt"
        
        # Performance tuning
        self.cache_level = StorageLevel.MEMORY_AND_DISK
        self.broadcast_threshold = 10_000_000
        self.target_per_partition = TARGET_RECORDS_PER_PARTITION
        self.min_partitions = MIN_PARTITIONS
        self.max_partitions = MAX_PARTITIONS
        
        # Column mappings (source -> destination)
        self.column_mappings = {}
        
        # Column transformations
        self.column_transformations = []
        
        # Inferred/derived columns
        self.inferred_columns = []
        
        # Date columns (need special handling for invalid dates)
        self.date_columns = []
        
        # Coalesce exclusion columns
        self.coalesce_exclusion_columns = []
        
        # History arrays to update (array_name, source_column, data_type, mapper_expr)
        self.rolling_columns = []
        
        # Grid columns (generated from history arrays)
        self.grid_columns = []
        
        # Column lists for output tables
        self.summary_columns = []
        self.latest_summary_columns = []
        
        # History length
        self.history_length = HISTORY_LENGTH
        
        # Catalog config
        self.catalog = {}
        
        # Spark config
        self.spark_config = {}
        
        # Load from JSON if provided
        if config_path:
            self._load_from_json(config_path)
    
    def _load_from_json(self, config_path: str):
        """Load configuration from JSON file"""
        logger.info(f"Loading configuration from: {config_path}")
        with open(config_path, 'r') as f:
            cfg = json.load(f)
        
        # Catalog
        if 'catalog' in cfg:
            self.catalog = cfg['catalog']
        
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
            self.primary_date_key = cfg['keys'].get('primary_date', self.primary_date_key)
        
        # History length
        if 'history' in cfg:
            self.history_length = cfg['history'].get('length', HISTORY_LENGTH)
        
        # Column mappings
        if 'columns' in cfg:
            self.column_mappings = cfg['columns']
        
        # Column transformations
        if 'column_transformations' in cfg:
            self.column_transformations = cfg['column_transformations']
        
        # Inferred columns
        if 'inferred_columns' in cfg:
            self.inferred_columns = cfg['inferred_columns']
        
        # Date columns
        if 'date_columns' in cfg:
            self.date_columns = cfg['date_columns']
        
        # Coalesce exclusion
        if 'coalesce_exclusion_columns' in cfg:
            self.coalesce_exclusion_columns = cfg['coalesce_exclusion_columns']
        
        # Rolling columns
        if 'rolling_columns' in cfg:
            self.rolling_columns = cfg['rolling_columns']
        
        # Grid columns
        if 'grid_columns' in cfg:
            self.grid_columns = cfg['grid_columns']
        
        # Output column lists
        if 'summary_columns' in cfg:
            self.summary_columns = cfg['summary_columns']
        if 'latest_summary_columns' in cfg:
            self.latest_summary_columns = cfg['latest_summary_columns']
        
        # Performance settings
        if 'performance' in cfg:
            perf = cfg['performance']
            self.broadcast_threshold = perf.get('broadcast_threshold', self.broadcast_threshold)
            self.target_per_partition = perf.get('target_records_per_partition', self.target_per_partition)
            self.min_partitions = perf.get('min_partitions', self.min_partitions)
            self.max_partitions = perf.get('max_partitions', self.max_partitions)
            cache_level = perf.get('cache_level', 'MEMORY_AND_DISK')
            if cache_level == 'MEMORY_AND_DISK':
                self.cache_level = StorageLevel.MEMORY_AND_DISK
            elif cache_level == 'MEMORY_ONLY':
                self.cache_level = StorageLevel.MEMORY_ONLY
            elif cache_level == 'DISK_ONLY':
                self.cache_level = StorageLevel.DISK_ONLY
        
        # Spark config
        if 'spark' in cfg:
            self.spark_config = cfg['spark']
        
        logger.info(f"Configuration loaded: {len(self.column_mappings)} column mappings, "
                   f"{len(self.rolling_columns)} rolling columns, "
                   f"{len(self.column_transformations)} transformations")
    
    def validate(self) -> bool:
        """Validate configuration"""
        if not self.primary_key or not self.partition_key:
            logger.error("Primary key and partition key are required")
            return False
        if not self.rolling_columns:
            logger.error("At least one rolling column is required")
            return False
        return True
    
    def get_history_col_name(self, rolling_col_name: str) -> str:
        """Get history array column name for a rolling column"""
        return f"{rolling_col_name}_history"


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM to integer"""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def calculate_partitions(record_count: int, config: PipelineConfig) -> int:
    """Calculate optimal partition count"""
    if record_count == 0:
        return config.min_partitions
    partitions = record_count / config.target_per_partition
    power_of_2 = 2 ** round(math.log2(max(1, partitions)))
    return int(max(config.min_partitions, min(config.max_partitions, power_of_2)))


def configure_spark(spark: SparkSession, num_partitions: int, config: PipelineConfig):
    """Configure Spark for optimal partitioning"""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))
    
    # Apply additional spark config from config file
    if config.spark_config:
        if config.spark_config.get('adaptive_enabled', True):
            spark.conf.set("spark.sql.adaptive.enabled", "true")
        if config.spark_config.get('adaptive_coalesce', True):
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        if config.spark_config.get('adaptive_skew_join', True):
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        if 'broadcast_timeout' in config.spark_config:
            spark.conf.set("spark.sql.broadcastTimeout", str(config.spark_config['broadcast_timeout']))
        if 'auto_broadcast_threshold' in config.spark_config:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", config.spark_config['auto_broadcast_threshold'])


def get_spark_type(data_type: str):
    """Get PySpark type from string type name"""
    return TYPE_MAP.get(data_type, StringType())


# ============================================================================
# DATA PREPARATION FUNCTIONS
# ============================================================================

def prepare_source_data(df, config: PipelineConfig):
    """
    Prepare source data with:
    1. Column mappings (rename source -> destination)
    2. Column transformations (sentinel value handling)
    3. Inferred/derived columns
    4. Date column validation
    5. Deduplication
    """
    logger.info("Preparing source data...")
    
    pk = config.primary_key
    prt = config.partition_key
    ts = config.timestamp_key
    
    result = df
    
    # Step 1: Apply column mappings
    if config.column_mappings:
        select_exprs = []
        for src_col, dst_col in config.column_mappings.items():
            if src_col in result.columns:
                select_exprs.append(F.col(src_col).alias(dst_col))
            else:
                logger.warning(f"Source column '{src_col}' not found, skipping")
        
        # Add any columns not in mapping (like partition columns already present)
        mapped_sources = set(config.column_mappings.keys())
        for col in result.columns:
            if col not in mapped_sources:
                # Check if it's a destination column name
                if col in config.column_mappings.values():
                    select_exprs.append(F.col(col))
        
        if select_exprs:
            result = result.select(*select_exprs)
            logger.info(f"Applied {len(config.column_mappings)} column mappings")
    
    # Step 2: Apply column transformations
    for transform in config.column_transformations:
        col_name = transform['name']
        expr = transform['expr']
        if col_name in result.columns or any(c in expr for c in result.columns):
            result = result.withColumn(col_name, F.expr(expr))
    
    if config.column_transformations:
        logger.info(f"Applied {len(config.column_transformations)} column transformations")
    
    # Step 3: Apply inferred/derived columns
    for inferred in config.inferred_columns:
        col_name = inferred['name']
        expr = inferred['expr']
        result = result.withColumn(col_name, F.expr(expr))
    
    if config.inferred_columns:
        logger.info(f"Created {len(config.inferred_columns)} inferred columns")
    
    # Step 4: Apply rolling column mappers (prepare values for history arrays)
    for rc in config.rolling_columns:
        src_col = rc['source_column']
        mapper_expr = rc.get('mapper_expr', src_col)
        # Create a prepared column for the rolling value
        if mapper_expr != src_col:
            result = result.withColumn(f"_prepared_{src_col}", F.expr(mapper_expr))
    
    # Step 5: Validate date columns (replace invalid years with NULL)
    for date_col in config.date_columns:
        if date_col in result.columns:
            result = result.withColumn(
                date_col,
                F.when(F.year(F.col(date_col)) < 1000, None)
                 .otherwise(F.col(date_col))
            )
    
    if config.date_columns:
        logger.info(f"Validated {len(config.date_columns)} date columns")
    
    # Step 6: Deduplicate by primary key + partition, keeping latest timestamp
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
    
    result = (
        result
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    logger.info("Source data preparation complete")
    return result


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
    
    # Prepare source data (mappings, transformations, deduplication)
    accounts_prepared = prepare_source_data(accounts_df, config)
    
    # Add month integer for comparison
    accounts_prepared = accounts_prepared.withColumn(
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
        return accounts_prepared.withColumn("case_type", F.lit("CASE_I"))
    
    # Classify records
    logger.info("Classifying accounts into Case I/II/III")
    classified = (
        accounts_prepared.alias("n")
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
    history_len = config.history_length
    
    # Create initial arrays for each rolling column
    for rc in config.rolling_columns:
        array_name = config.get_history_col_name(rc['name'])
        src_col = rc['source_column']
        mapper_expr = rc.get('mapper_expr', src_col)
        
        # Use prepared column if mapper was applied, else use source
        value_col = f"_prepared_{src_col}" if mapper_expr != src_col and f"_prepared_{src_col}" in result.columns else src_col
        
        # Array: [current_value, NULL, NULL, ..., NULL]
        null_array = ", ".join(["NULL" for _ in range(history_len - 1)])
        result = result.withColumn(
            array_name,
            F.expr(f"array({value_col}, {null_array})")
        )
    
    # Generate grid columns
    for gc in config.grid_columns:
        source_history = gc['source_history']
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('separator', '')
        
        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )
    
    # Drop classification and temporary columns
    drop_cols = ["month_int", "max_existing_month", "max_existing_ts",
                 "max_month_int", "case_type", "MONTH_DIFF"]
    # Also drop prepared columns
    drop_cols.extend([c for c in result.columns if c.startswith("_prepared_")])
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
    history_len = config.history_length
    
    # Get affected accounts
    affected_keys = case_ii_df.select(pk).distinct()
    affected_count = affected_keys.count()
    
    # Load latest summary for affected accounts
    logger.info(f"Loading latest summary for {affected_count:,} accounts")
    
    # Select needed columns from latest summary
    history_cols = [config.get_history_col_name(rc['name']) for rc in config.rolling_columns]
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
    for rc in config.rolling_columns:
        array_name = config.get_history_col_name(rc['name'])
        src_col = rc['source_column']
        mapper_expr = rc.get('mapper_expr', src_col)
        
        # Use prepared column if available
        value_col = f"_prepared_{src_col}" if f"_prepared_{src_col}" in case_ii_df.columns else src_col
        
        shift_expr = f"""
            CASE
                WHEN c.MONTH_DIFF = 1 THEN
                    slice(concat(array(c.{value_col}), p.{array_name}), 1, {history_len})
                WHEN c.MONTH_DIFF > 1 THEN
                    slice(
                        concat(
                            array(c.{value_col}),
                            array_repeat(NULL, c.MONTH_DIFF - 1),
                            p.{array_name}
                        ),
                        1, {history_len}
                    )
                ELSE
                    concat(array(c.{value_col}), array_repeat(NULL, {history_len - 1}))
            END as {array_name}
        """
        shift_exprs.append(shift_expr)
    
    # Get non-array columns from current record
    exclude_cols = {"month_int", "max_existing_month", "max_existing_ts",
                   "max_month_int", "case_type", "MONTH_DIFF"}
    exclude_cols.update(history_cols)
    exclude_cols.update([c for c in case_ii_df.columns if c.startswith("_prepared_")])
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
        source_history = gc['source_history']
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('separator', '')
        
        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )
    
    logger.info(f"Case II complete: {count:,} forward entries with shifted arrays")
    return result


# ============================================================================
# STEP 4: PROCESS CASE III (BACKFILL) - FIXED VERSION
# ============================================================================

def process_case_iii(spark: SparkSession, case_iii_df, config: PipelineConfig):
    """
    Process Case III - rebuild history arrays for backfill
    
    FIXED: This version now does TWO things:
    1. Creates NEW summary rows for backfill months (with inherited history)
    2. Updates ALL future summary rows with backfill data
    
    Logic:
    A. For each backfill record, find the closest PRIOR summary
    B. Create a new summary row for the backfill month with:
       - Position 0: backfill data
       - Position 1-N: shifted data from prior summary (based on month gap)
    C. Update all FUTURE summaries with backfill data at correct position
    """
    logger.info("=" * 80)
    logger.info("STEP 2c: Process Case III (Backfill) - FIXED")
    logger.info("=" * 80)
    
    count = case_iii_df.count()
    if count == 0:
        logger.info("No Case III records to process")
        return None
    
    logger.info(f"Processing {count:,} backfill records")
    
    pk = config.primary_key
    prt = config.partition_key
    ts = config.timestamp_key
    history_len = config.history_length
    
    # Get affected accounts
    affected_accounts = case_iii_df.select(pk).distinct()
    affected_count = affected_accounts.count()
    logger.info(f"Affected accounts: {affected_count:,}")
    
    # Load ALL summary rows for affected accounts
    history_cols = [config.get_history_col_name(rc['name']) for rc in config.rolling_columns]
    
    # Build select for summary - include all columns we need
    summary_select_cols = [pk, prt, ts] + history_cols
    
    # Also include scalar columns from summary that we need to preserve
    # These are columns that are NOT history arrays
    summary_df = spark.read.table(config.summary_table)
    available_summary_cols = summary_df.columns
    
    # Add any scalar columns from summary_columns config that exist
    scalar_cols = []
    for col in config.summary_columns:
        if col in available_summary_cols and col not in summary_select_cols and not col.endswith('_history'):
            scalar_cols.append(col)
            summary_select_cols.append(col)
    
    summary_df = summary_df.select(*[c for c in summary_select_cols if c in available_summary_cols])
    
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
    
    # Create temp views
    case_iii_df.createOrReplaceTempView("backfill_records")
    summary_filtered.createOrReplaceTempView("summary_affected")
    
    # =========================================================================
    # PART A: Create NEW summary rows for backfill months
    # =========================================================================
    logger.info("Part A: Creating new summary rows for backfill months...")
    
    # Find the closest PRIOR summary for each backfill record
    prior_summary_joined = spark.sql(f"""
        SELECT 
            b.*,
            s.{prt} as prior_month,
            s.{ts} as prior_ts,
            {', '.join([f's.{arr} as prior_{arr}' for arr in history_cols])},
            (
                {month_to_int_expr(f"b.{prt}")} - {month_to_int_expr(f"s.{prt}")}
            ) as months_since_prior,
            ROW_NUMBER() OVER (
                PARTITION BY b.{pk}, b.{prt} 
                ORDER BY {month_to_int_expr(f"s.{prt}")} DESC
            ) as rn
        FROM backfill_records b
        LEFT JOIN summary_affected s 
            ON b.{pk} = s.{pk} 
            AND {month_to_int_expr(f"s.{prt}")} < {month_to_int_expr(f"b.{prt}")}
    """)
    
    # Keep only the closest prior summary (rn = 1)
    prior_summary = prior_summary_joined.filter(F.col("rn") == 1).drop("rn")
    prior_summary.createOrReplaceTempView("backfill_with_prior")
    
    # Build expressions for creating new summary row
    new_row_exprs = []
    for rc in config.rolling_columns:
        array_name = config.get_history_col_name(rc['name'])
        src_col = rc['source_column']
        mapper_expr = rc.get('mapper_expr', src_col)
        value_col = f"_prepared_{src_col}" if f"_prepared_{src_col}" in case_iii_df.columns else src_col
        
        new_row_expr = f"""
            CASE
                WHEN prior_month IS NOT NULL AND months_since_prior > 0 THEN
                    slice(
                        concat(
                            array({value_col}),
                            CASE 
                                WHEN months_since_prior > 1 THEN array_repeat(NULL, months_since_prior - 1)
                                ELSE array()
                            END,
                            prior_{array_name}
                        ),
                        1, {history_len}
                    )
                ELSE
                    concat(array({value_col}), array_repeat(NULL, {history_len - 1}))
            END as {array_name}
        """
        new_row_exprs.append(new_row_expr)
    
    # Get columns from backfill record (excluding temp columns)
    exclude_backfill = {"month_int", "max_existing_month", "max_existing_ts", 
                       "max_month_int", "case_type", "MONTH_DIFF",
                       "prior_month", "prior_ts", "months_since_prior"}
    exclude_backfill.update([f"prior_{arr}" for arr in history_cols])
    exclude_backfill.update([c for c in case_iii_df.columns if c.startswith("_prepared_")])
    
    backfill_cols = [c for c in case_iii_df.columns if c not in exclude_backfill]
    backfill_select = ", ".join(backfill_cols)
    
    new_rows_sql = f"""
        SELECT 
            {backfill_select},
            {', '.join(new_row_exprs)}
        FROM backfill_with_prior
    """
    
    new_backfill_rows = spark.sql(new_rows_sql)
    new_rows_count = new_backfill_rows.count()
    logger.info(f"Created {new_rows_count:,} new summary rows for backfill months")
    
    # =========================================================================
    # PART B: Update FUTURE summary rows with backfill data
    # =========================================================================
    logger.info("Part B: Updating future summary rows...")
    
    # Build value column references for backfill
    backfill_value_cols = []
    for rc in config.rolling_columns:
        src_col = rc['source_column']
        value_col = f"_prepared_{src_col}" if f"_prepared_{src_col}" in case_iii_df.columns else src_col
        backfill_value_cols.append(f'b.{value_col} as backfill_{src_col}')
    
    # Join backfill with future summaries
    future_joined = spark.sql(f"""
        SELECT 
            b.{pk},
            b.{ts} as backfill_ts,
            {', '.join(backfill_value_cols)},
            s.{prt} as summary_month,
            s.{ts} as summary_ts,
            {', '.join([f's.{arr} as existing_{arr}' for arr in history_cols])},
            {', '.join([f's.{col}' for col in scalar_cols]) + ',' if scalar_cols else ''}
            (
                {month_to_int_expr(f"s.{prt}")} - {month_to_int_expr(f"b.{prt}")}
            ) as backfill_position
        FROM backfill_records b
        JOIN summary_affected s ON b.{pk} = s.{pk}
        WHERE {month_to_int_expr(f"s.{prt}")} > {month_to_int_expr(f"b.{prt}")}
          AND ({month_to_int_expr(f"s.{prt}")} - {month_to_int_expr(f"b.{prt}")}) < {history_len}
    """)
    
    future_joined.persist(config.cache_level)
    future_count = future_joined.count()
    logger.info(f"Valid future summary updates: {future_count:,}")
    
    if future_count > 0:
        future_joined.createOrReplaceTempView("future_backfill_joined")
        
        # Build array update expressions using transform()
        update_exprs = []
        for rc in config.rolling_columns:
            array_name = config.get_history_col_name(rc['name'])
            src_col = rc['source_column']
            update_expr = f"""
                transform(
                    existing_{array_name},
                    (x, i) -> IF(i = backfill_position, backfill_{src_col}, x)
                ) as {array_name}
            """
            update_exprs.append(update_expr)
        
        # Include scalar columns in output
        scalar_select = ', '.join(scalar_cols) + ',' if scalar_cols else ''
        
        # Final SQL for updated future rows
        updated_futures = spark.sql(f"""
            SELECT 
                {pk},
                summary_month as {prt},
                {scalar_select}
                {', '.join(update_exprs)},
                summary_ts as {ts}
            FROM future_backfill_joined
        """)
        
        # Group by account+month and keep latest (in case of multiple backfills)
        window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
        updated_futures = (
            updated_futures
            .withColumn("_rn", F.row_number().over(window_spec))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        
        updated_count = updated_futures.count()
        logger.info(f"Updated {updated_count:,} future summary rows")
    else:
        updated_futures = None
    
    future_joined.unpersist()
    
    # =========================================================================
    # PART C: Combine new rows and updated futures
    # =========================================================================
    logger.info("Part C: Combining new and updated summary rows...")
    
    # Generate grid columns for new rows
    for gc in config.grid_columns:
        source_history = gc['source_history']
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('separator', '')
        
        new_backfill_rows = new_backfill_rows.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )
    
    if updated_futures is not None:
        # Generate grid columns for updated futures
        for gc in config.grid_columns:
            source_history = gc['source_history']
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('separator', '')
            
            updated_futures = updated_futures.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.col(source_history),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )
        
        # Union new rows and updated futures
        result = new_backfill_rows.unionByName(updated_futures, allowMissingColumns=True)
    else:
        result = new_backfill_rows
    
    # Deduplicate - keep only one row per account+month (prefer the one with latest ts)
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
    result = (
        result
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    total_count = result.count()
    logger.info(f"Case III complete: {total_count:,} total summary records (new + updated)")
    
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
    
    # Select only columns that should go to summary table
    if config.summary_columns:
        available_cols = result_df.columns
        summary_cols = [c for c in config.summary_columns if c in available_cols]
        result_for_summary = result_df.select(*summary_cols)
    else:
        result_for_summary = result_df
    
    result_for_summary.createOrReplaceTempView("pipeline_result")
    count = result_for_summary.count()
    
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
        
        # Prepare data for latest_summary
        if config.latest_summary_columns:
            available_cols = result_df.columns
            latest_cols = [c for c in config.latest_summary_columns if c in available_cols]
            result_for_latest = result_df.select(*latest_cols)
        else:
            result_for_latest = result_df
        
        result_for_latest.createOrReplaceTempView("pipeline_result_latest")
        
        # MERGE to latest_summary (keep only latest per account)
        logger.info(f"Updating {config.latest_summary_table}")
        spark.sql(f"""
            MERGE INTO {config.latest_summary_table} t
            USING (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY {prt} DESC) as rn
                    FROM pipeline_result_latest
                ) WHERE rn = 1
            ) s
            ON t.{pk} = s.{pk}
            WHEN MATCHED AND s.{prt} >= t.{prt} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        # Overwrite mode
        logger.info(f"Writing {count:,} records to {config.summary_table}")
        result_for_summary.writeTo(config.summary_table).using("iceberg").createOrReplace()
    
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
    logger.info("SUMMARY PIPELINE V9.1 FIXED - START")
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
        num_partitions = calculate_partitions(stats['total_records'], config)
        configure_spark(spark, num_partitions, config)
        logger.info(f"Configured {num_partitions} partitions for {stats['total_records']:,} records")
        
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
        logger.info("SUMMARY PIPELINE V9.1 FIXED - COMPLETED")
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
# SPARK SESSION CREATION
# ============================================================================

def create_spark_session(config: PipelineConfig) -> SparkSession:
    """Create and configure Spark session with optimal settings"""
    
    app_name = config.spark_config.get('app_name', 'SummaryPipeline_v9_fixed')
    
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
        .config("spark.sql.broadcastTimeout", "1200") \
        .config("spark.sql.autoBroadcastJoinThreshold", "500m") \
        .enableHiveSupport()
    
    # Add catalog config
    if config.catalog:
        cat = config.catalog
        cat_name = cat.get('name', 'primary_catalog')
        builder = builder \
            .config("spark.sql.defaultCatalog", cat_name) \
            .config(f"spark.sql.catalog.{cat_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{cat_name}.catalog-impl", 
                   "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{cat_name}.warehouse", cat.get('warehouse', '')) \
            .config(f"spark.sql.catalog.{cat_name}.io-impl", 
                   cat.get('io_impl', 'org.apache.iceberg.aws.s3.S3FileIO'))
    
    # Add dynamic allocation if configured
    if config.spark_config.get('dynamic_allocation', {}).get('enabled', False):
        dyn = config.spark_config['dynamic_allocation']
        builder = builder \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", str(dyn.get('min_executors', 50))) \
            .config("spark.dynamicAllocation.maxExecutors", str(dyn.get('max_executors', 250))) \
            .config("spark.dynamicAllocation.initialExecutors", str(dyn.get('initial_executors', 100)))
    
    return builder.getOrCreate()


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Summary Pipeline v9.1 FIXED")
    parser.add_argument('--config', required=True, help='Path to pipeline config JSON')
    parser.add_argument('--filter', help='SQL filter expression for accounts')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental',
                       help='Processing mode')
    
    args = parser.parse_args()
    
    # Create config
    config = PipelineConfig(args.config)
    if not config.validate():
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session(config)
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
