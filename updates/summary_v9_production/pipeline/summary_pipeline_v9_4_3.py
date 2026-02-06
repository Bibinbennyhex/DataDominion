"""
Summary Pipeline v9.4.3 - Temp Table Optimized Production Implementation
=================================================================

VERSION 9.4.3 FIXES (Critical):
- Fixed Case III gap filling logic: Now attempts peer_map lookup even when no prior existing
  summary is found. This ensures backfill chains work correctly for initial historical loads.
- Added Double/Float/Decimal support for array updates (previously defaulted to BIGINT).
- Improved date handling robustness in backfill range calculation.

VERSION 9.4.2 FIXES:
- Chained Backfill Support (Case III): Windowed Map-Lookup Optimization
- Multi-Month Forward Chaining (Case II)

VERSION 9.4 OPTIMIZATIONS:
- Temp table approach: Breaks Spark lineage for better performance at scale
- Each case writes to temp table, releasing memory immediately
- Implicit checkpointing per case
- Easier debugging (can query temp table mid-run)
- Fault tolerance (partial progress preserved on failure)
- Automatic cleanup with try/finally
- Orphan cleanup utility for failed runs

VERSION 9.3 OPTIMIZATIONS:
- Partition-aware reads: Case III now uses partition filters to avoid full table scan
- Single MERGE: Consolidates all results and writes once instead of 4 separate writes
- Case II filter pushdown: Uses SQL subquery to enable Iceberg data file pruning
- Partition-aligned processing: Leverages rpt_as_of_mo partitioning throughout

VERSION 9.2 CHANGES:
- Added Case IV: Bulk Historical Load for new accounts with multiple months
- Fixed classification to detect when a new account uploads many months at once
- Uses SQL window functions to build rolling arrays efficiently

BUG FIXED (v9.1):
- Original v9 only updated future summaries when backfill data arrived
- This version ALSO creates a new summary row for the backfill month itself,
  inheriting historical data from the closest prior summary

NEW IN v9.2:
- Case IV: When a brand new account uploads 72 months of data at once, the pipeline
  now correctly builds rolling 36-month arrays for each month, not just position 0.

FEATURES:
- Full column mapping support (source -> destination)
- Column transformations (sentinel value handling)
- Inferred/derived columns
- Date column validation
- Separate latest_summary and summary column lists
- Rolling history arrays (36-month)
- Grid column generation
- Case I/II/III/IV handling with correct logic

Enhanced version with ALL 4 cases:
- Case I:   New accounts (no existing summary) - single month only
- Case II:  Forward entries (month > max existing)
- Case III: Backfill (month <= max existing) - NOW CREATES BACKFILL MONTH ROW
- Case IV:  Bulk historical load (new account with multiple months in batch)

Performance Improvements:
- TEMP TABLE: Breaks complex DAG lineage (prevents driver OOM)
- PARTITION PRUNING: Case III reads only necessary partitions (50-90% I/O reduction)
- SINGLE MERGE: 1 MERGE per table instead of 4 (4x less write amplification)
- SQL-based array updates (faster than UDF loops)
- Broadcast optimization for small tables
- Better cache management (MEMORY_AND_DISK)
- Single-pass array updates
- Configurable Spark settings

CONFIG FORMAT:
- Uses flat JSON structure matching original summary.json format
- No PipelineConfig class - config is a plain Python dict

USAGE:
  # Run with temp table (default, recommended)
  python summary_pipeline.py --config config.json

  # Run legacy (in-memory union)
  python summary_pipeline.py --config config.json --legacy

  # Cleanup orphaned temp tables
  python summary_pipeline.py --config config.json --cleanup-orphans
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
import uuid
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple, Any, Optional


# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SummaryPipeline.v9.4")


# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_HISTORY_LENGTH = 36
TARGET_RECORDS_PER_PARTITION = 5_000_000
MIN_PARTITIONS = 32
MAX_PARTITIONS = 16384

TYPE_MAP = {
    "Integer": IntegerType(),
    "String": StringType(),
    "Decimal": DecimalType(18, 2),
    "Date": DateType()
}

# Storage level mapping
CACHE_LEVELS = {
    "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
    "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
    "DISK_ONLY": StorageLevel.DISK_ONLY
}


# ============================================================================
# CONFIG HELPER FUNCTIONS
# ============================================================================

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from JSON file and return as dict"""
    logger.info(f"Loading configuration from: {config_path}")
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Log config summary
    rolling_count = len(config.get('rolling_columns', []))
    columns_count = len(config.get('columns', {}))
    transform_count = len(config.get('column_transformations', []))
    logger.info(f"Configuration loaded: {columns_count} column mappings, "
               f"{rolling_count} rolling columns, "
               f"{transform_count} transformations")
    
    return config


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration"""
    required_fields = ['primary_column', 'partition_column', 'source_table', 
                       'destination_table', 'rolling_columns']
    
    for field in required_fields:
        if field not in config or not config[field]:
            logger.error(f"Required config field '{field}' is missing or empty")
            return False
    
    return True


def get_history_col_name(rolling_col_name: str) -> str:
    """Get history array column name for a rolling column"""
    return f"{rolling_col_name}_history"


def get_history_length(config: Dict[str, Any]) -> int:
    """Get history length from config or default"""
    return config.get('history_length', DEFAULT_HISTORY_LENGTH)


def get_cache_level(config: Dict[str, Any]) -> StorageLevel:
    """Get cache level from config or default"""
    perf = config.get('performance', {})
    cache_str = perf.get('cache_level', 'MEMORY_AND_DISK')
    return CACHE_LEVELS.get(cache_str, StorageLevel.MEMORY_AND_DISK)


def get_broadcast_threshold(config: Dict[str, Any]) -> int:
    """Get broadcast threshold from config or default"""
    perf = config.get('performance', {})
    return perf.get('broadcast_threshold', 10_000_000)


def get_target_per_partition(config: Dict[str, Any]) -> int:
    """Get target records per partition from config or default"""
    perf = config.get('performance', {})
    return perf.get('target_records_per_partition', TARGET_RECORDS_PER_PARTITION)


def get_min_partitions(config: Dict[str, Any]) -> int:
    """Get min partitions from config or default"""
    perf = config.get('performance', {})
    return perf.get('min_partitions', MIN_PARTITIONS)


def get_max_partitions(config: Dict[str, Any]) -> int:
    """Get max partitions from config or default"""
    perf = config.get('performance', {})
    return perf.get('max_partitions', MAX_PARTITIONS)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM to integer"""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def calculate_partitions(record_count: int, config: Dict[str, Any]) -> int:
    """Calculate optimal partition count"""
    if record_count == 0:
        return get_min_partitions(config)
    target = get_target_per_partition(config)
    min_parts = get_min_partitions(config)
    max_parts = get_max_partitions(config)
    partitions = record_count / target
    power_of_2 = 2 ** round(math.log2(max(1, partitions)))
    return int(max(min_parts, min(max_parts, power_of_2)))


# ============================================================================
# TEMP TABLE MANAGEMENT
# ============================================================================

TEMP_TABLE_PREFIX = "pipeline_batch_"
TEMP_TABLE_SCHEMA = "temp"  # Schema/namespace for temp tables


def generate_temp_table_name(config: Dict[str, Any]) -> str:
    """
    Generate a unique temp table name for this batch.
    
    Format: {catalog}.{temp_schema}.pipeline_batch_{timestamp}_{uuid}
    
    Example: demo.temp.pipeline_batch_20260130_143052_a1b2c3d4
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = uuid.uuid4().hex[:8]
    
    # Extract catalog from destination table (e.g., "demo.schema.table" -> "demo")
    dest_table = config['destination_table']
    parts = dest_table.split('.')
    
    if len(parts) >= 2:
        catalog = parts[0]
    else:
        catalog = "spark_catalog"
    
    # Use configured temp schema or default
    temp_schema = config.get('temp_table_schema', TEMP_TABLE_SCHEMA)
    
    table_name = f"{catalog}.{temp_schema}.{TEMP_TABLE_PREFIX}{timestamp}_{unique_id}"
    return table_name


def create_temp_table(spark, temp_table_name: str, config: Dict[str, Any]) -> bool:
    """
    Create a temp table with the same schema as the summary table plus _case_type column.
    
    Uses CREATE TABLE ... AS SELECT to clone the schema, then adds _case_type column.
    
    Args:
        spark: SparkSession
        temp_table_name: Fully qualified temp table name
        config: Pipeline config
        
    Returns:
        True if created successfully, False otherwise
    """
    summary_table = config['destination_table']
    
    try:
        # Ensure temp schema exists
        parts = temp_table_name.split('.')
        if len(parts) >= 2:
            catalog_schema = '.'.join(parts[:-1])
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_schema}")
        
        # Create empty table with same schema as summary PLUS _case_type column
        spark.sql(f"""
            CREATE TABLE {temp_table_name}
            USING iceberg
            AS SELECT *, CAST(NULL AS STRING) as _case_type 
            FROM {summary_table} WHERE 1=0
        """)
        
        logger.info(f"Created temp table: {temp_table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create temp table {temp_table_name}: {e}")
        raise


def write_to_temp_table(spark, df, temp_table_name: str, case_type: str) -> int:
    """
    Write a DataFrame to the temp table.
    
    Filters out internal columns that aren't in the target table schema.
    
    Args:
        spark: SparkSession
        df: DataFrame to write
        temp_table_name: Fully qualified temp table name
        case_type: Case type for logging (CASE_I, CASE_II, etc.)
        
    Returns:
        Number of records written
    """
    if df is None:
        return 0
    
    count = df.count()
    if count == 0:
        logger.info(f"{case_type}: No records to write")
        return 0
    
    # Get target table columns
    target_columns = [field.name for field in spark.table(temp_table_name).schema.fields]
    
    # Filter DataFrame to only include target columns
    # Internal columns to exclude (they're used for processing but shouldn't be persisted)
    internal_cols = {'min_month_for_new_account', 'count_months_for_new_account', 
                     'month_int', 'max_existing_month', 'max_existing_ts', 
                     'max_month_int', 'case_type', 'MONTH_DIFF', '_rn', '_case_priority'}
    
    # Select only columns that exist in target table
    select_cols = [c for c in target_columns if c in df.columns or c == '_case_type']
    
    # Build the final DataFrame
    df_to_write = df
    
    # Ensure _case_type exists
    if '_case_type' not in df.columns:
        df_to_write = df_to_write.withColumn('_case_type', F.lit(case_type))
    
    # Add any missing columns as NULL
    for col in target_columns:
        if col not in df_to_write.columns:
            df_to_write = df_to_write.withColumn(col, F.lit(None))
    
    # Select in target table column order
    df_to_write = df_to_write.select(*target_columns)
    
    # Write to temp table
    df_to_write.writeTo(temp_table_name).append()
    logger.info(f"{case_type}: Wrote {count:,} records to temp table")
    
    return count


def drop_temp_table(spark, temp_table_name: str, purge: bool = True) -> bool:
    """
    Drop the temp table.
    
    Args:
        spark: SparkSession
        temp_table_name: Fully qualified temp table name
        purge: If True, also delete the underlying data files
        
    Returns:
        True if dropped successfully
    """
    try:
        purge_clause = "PURGE" if purge else ""
        spark.sql(f"DROP TABLE IF EXISTS {temp_table_name} {purge_clause}")
        logger.info(f"Dropped temp table: {temp_table_name}")
        return True
    except Exception as e:
        logger.warning(f"Failed to drop temp table {temp_table_name}: {e}")
        return False


def cleanup_orphaned_temp_tables(spark, config: Dict[str, Any], 
                                  max_age_hours: int = 24) -> int:
    """
    Clean up orphaned temp tables from failed pipeline runs.
    
    This function should be called periodically (e.g., daily) to clean up
    temp tables that were not properly dropped due to job failures.
    
    Args:
        spark: SparkSession
        config: Pipeline config
        max_age_hours: Maximum age in hours before a temp table is considered orphaned
        
    Returns:
        Number of tables cleaned up
    """
    dest_table = config['destination_table']
    parts = dest_table.split('.')
    
    if len(parts) >= 2:
        catalog = parts[0]
    else:
        catalog = "spark_catalog"
    
    temp_schema = config.get('temp_table_schema', TEMP_TABLE_SCHEMA)
    
    cleaned = 0
    cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
    cutoff_str = cutoff_time.strftime('%Y%m%d_%H%M%S')
    
    try:
        # List all temp tables
        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{temp_schema}")
        tables = tables_df.filter(
            tables_df.tableName.startswith(TEMP_TABLE_PREFIX)
        ).collect()
        
        for row in tables:
            table_name = row['tableName']
            
            # Extract timestamp from table name
            # Format: pipeline_batch_20260130_143052_a1b2c3d4
            try:
                parts = table_name.replace(TEMP_TABLE_PREFIX, '').split('_')
                if len(parts) >= 2:
                    table_timestamp = f"{parts[0]}_{parts[1]}"
                    
                    # If table is older than cutoff, drop it
                    if table_timestamp < cutoff_str:
                        full_name = f"{catalog}.{temp_schema}.{table_name}"
                        drop_temp_table(spark, full_name, purge=True)
                        cleaned += 1
                        logger.info(f"Cleaned up orphaned temp table: {full_name}")
            except Exception as e:
                logger.warning(f"Could not parse temp table name {table_name}: {e}")
                continue
        
        logger.info(f"Cleanup complete: removed {cleaned} orphaned temp tables")
        return cleaned
        
    except Exception as e:
        logger.warning(f"Error during temp table cleanup: {e}")
        return cleaned


def configure_spark(spark: SparkSession, num_partitions: int, config: Dict[str, Any]):
    """Configure Spark for optimal partitioning"""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))
    
    # Apply additional spark config from config file
    spark_config = config.get('spark', {})
    if spark_config:
        if spark_config.get('adaptive_enabled', True):
            spark.conf.set("spark.sql.adaptive.enabled", "true")
        if spark_config.get('adaptive_coalesce', True):
            spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        if spark_config.get('adaptive_skew_join', True):
            spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        if 'broadcast_timeout' in spark_config:
            spark.conf.set("spark.sql.broadcastTimeout", str(spark_config['broadcast_timeout']))
        if 'auto_broadcast_threshold' in spark_config:
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", spark_config['auto_broadcast_threshold'])


def get_spark_type(data_type: str):
    """Get PySpark type from string type name"""
    return TYPE_MAP.get(data_type, StringType())


# ============================================================================
# DATA PREPARATION FUNCTIONS
# ============================================================================

def prepare_source_data(df, config: Dict[str, Any]):
    """
    Prepare source data with:
    1. Column mappings (rename source -> destination)
    2. Column transformations (sentinel value handling)
    3. Inferred/derived columns
    4. Date column validation
    5. Deduplication
    """
    logger.info("Preparing source data...")
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    
    result = df
    
    # Step 1: Apply column mappings
    column_mappings = config.get('columns', {})
    if column_mappings:
        select_exprs = []
        for src_col, dst_col in column_mappings.items():
            if src_col in result.columns:
                select_exprs.append(F.col(src_col).alias(dst_col))
            else:
                logger.warning(f"Source column '{src_col}' not found, skipping")
        
        # Add any columns not in mapping (like partition columns already present)
        mapped_sources = set(column_mappings.keys())
        for col in result.columns:
            if col not in mapped_sources:
                # Check if it's a destination column name
                if col in column_mappings.values():
                    select_exprs.append(F.col(col))
        
        if select_exprs:
            result = result.select(*select_exprs)
            logger.info(f"Applied {len(column_mappings)} column mappings")
    
    # Step 2: Apply column transformations
    column_transformations = config.get('column_transformations', [])
    for transform in column_transformations:
        col_name = transform['name']
        # Use 'mapper_expr' to match original format
        expr = transform.get('mapper_expr', transform.get('expr', col_name))
        if col_name in result.columns or any(c in expr for c in result.columns):
            result = result.withColumn(col_name, F.expr(expr))
    
    if column_transformations:
        logger.info(f"Applied {len(column_transformations)} column transformations")
    
    # Step 3: Apply inferred/derived columns
    inferred_columns = config.get('inferred_columns', [])
    for inferred in inferred_columns:
        col_name = inferred['name']
        # Use 'mapper_expr' to match original format
        expr = inferred.get('mapper_expr', inferred.get('expr', ''))
        if expr:
            result = result.withColumn(col_name, F.expr(expr))
    
    if inferred_columns:
        logger.info(f"Created {len(inferred_columns)} inferred columns")
    
    # Step 4: Apply rolling column mappers (prepare values for history arrays)
    rolling_columns = config.get('rolling_columns', [])
    for rc in rolling_columns:
        # Use 'mapper_column' to match original format (source column for the value)
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        mapper_expr = rc.get('mapper_expr', src_col)
        # Create a prepared column for the rolling value
        if mapper_expr != src_col:
            result = result.withColumn(f"_prepared_{rc['name']}", F.expr(mapper_expr))
    
    # Step 5: Validate date columns (replace invalid years with NULL)
    date_columns = config.get('date_col_list', config.get('date_columns', []))
    for date_col in date_columns:
        if date_col in result.columns:
            result = result.withColumn(
                date_col,
                F.when(F.year(F.col(date_col)) < 1000, None)
                 .otherwise(F.col(date_col))
            )
    
    if date_columns:
        logger.info(f"Validated {len(date_columns)} date columns")
    
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

def load_and_classify_accounts(spark: SparkSession, config: Dict[str, Any], 
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
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    source_table = config['source_table']
    latest_summary_table = config['latest_history_table']
    
    # Load accounts
    logger.info(f"Loading accounts from {source_table}")
    if filter_expr:
        accounts_df = spark.sql(f"""
            SELECT * FROM {source_table}
            WHERE {filter_expr}
        """)
    else:
        accounts_df = spark.read.table(source_table)
    
    # Prepare source data (mappings, transformations, deduplication)
    accounts_prepared = prepare_source_data(accounts_df, config)
    
    # Add month integer for comparison
    accounts_prepared = accounts_prepared.withColumn(
        "month_int",
        F.expr(month_to_int_expr(prt))
    )
    
    # Load summary metadata (small - can broadcast)
    logger.info(f"Loading summary metadata from {latest_summary_table}")
    try:
        summary_meta = spark.sql(f"""
            SELECT 
                {pk},
                {prt} as max_existing_month,
                {ts} as max_existing_ts,
                {month_to_int_expr(prt)} as max_month_int
            FROM {latest_summary_table}
        """)
        meta_count = summary_meta.count()
        logger.info(f"Loaded metadata for {meta_count:,} existing accounts")
    except Exception as e:
        logger.warning(f"Could not load summary metadata: {e}")
        # No existing summary - all are Case I
        return accounts_prepared.withColumn("case_type", F.lit("CASE_I"))
    
    # Classify records
    logger.info("Classifying accounts into Case I/II/III/IV")
    
    # Initial classification (before detecting bulk historical)
    initial_classified = (
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
            "initial_case_type",
            F.when(F.col("max_existing_month").isNull(), F.lit("CASE_I"))
            .when(F.col("month_int") > F.col("max_month_int"), F.lit("CASE_II"))
            .otherwise(F.lit("CASE_III"))
        )
    )
    
    # Detect bulk historical: new accounts with multiple months in batch
    # For Case I accounts, find the earliest month per account
    window_spec = Window.partitionBy(pk)
    classified = (
        initial_classified
        .withColumn(
            "min_month_for_new_account",
            F.when(
                F.col("initial_case_type") == "CASE_I",
                F.min("month_int").over(window_spec)
            )
        )
        .withColumn(
            "count_months_for_new_account",
            F.when(
                F.col("initial_case_type") == "CASE_I",
                F.count("*").over(window_spec)
            ).otherwise(F.lit(1))
        )
        .withColumn(
            "case_type",
            F.when(
                # Case I: New account with ONLY single month in batch
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") == 1),
                F.lit("CASE_I")
            ).when(
                # Case IV: New account with MULTIPLE months - this is the first/earliest month
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") > 1) &
                (F.col("month_int") == F.col("min_month_for_new_account")),
                F.lit("CASE_I")  # First month treated as Case I
            ).when(
                # Case IV: New account with MULTIPLE months - subsequent months
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") > 1) &
                (F.col("month_int") > F.col("min_month_for_new_account")),
                F.lit("CASE_IV")  # Bulk historical - will build on earlier months in batch
            ).otherwise(
                # Keep existing classification for Case II and III
                F.col("initial_case_type")
            )
        )
        .withColumn(
            "MONTH_DIFF",
            F.when(
                F.col("case_type") == "CASE_II",
                F.col("month_int") - F.col("max_month_int")
            ).when(
                # For Case IV, calculate diff from earliest month in batch
                F.col("case_type") == "CASE_IV",
                F.col("month_int") - F.col("min_month_for_new_account")
            ).otherwise(F.lit(1))
        )
        .drop("initial_case_type")
    )
    
    # Cache for multiple filters
    cache_level = get_cache_level(config)
    classified.persist(cache_level)
    
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

def process_case_i(case_i_df, config: Dict[str, Any]):
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
    history_len = get_history_length(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    
    # Create initial arrays for each rolling column
    for rc in rolling_columns:
        array_name = get_history_col_name(rc['name'])
        # Use 'mapper_column' to match original format
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        mapper_expr = rc.get('mapper_expr', src_col)
        
        # Use prepared column if mapper was applied, else use source
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in result.columns else src_col
        
        # Array: [current_value, NULL, NULL, ..., NULL]
        null_array = ", ".join(["NULL" for _ in range(history_len - 1)])
        result = result.withColumn(
            array_name,
            F.expr(f"array({value_col}, {null_array})")
        )
    
    # Generate grid columns
    for gc in grid_columns:
        # Use 'mapper_rolling_column' to match original format
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = get_history_col_name(source_rolling)
        placeholder = gc.get('placeholder', '?')
        # Note: original uses 'seperator' (typo preserved)
        separator = gc.get('seperator', gc.get('separator', ''))
        
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

def process_case_ii(spark: SparkSession, case_ii_df, config: Dict[str, Any]):
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
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    latest_summary_table = config['latest_history_table']
    history_len = get_history_length(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    
    # Get affected accounts
    affected_keys = case_ii_df.select(pk).distinct()
    affected_count = affected_keys.count()
    
    # Load latest summary for affected accounts
    logger.info(f"Loading latest summary for {affected_count:,} accounts")
    
    # Select needed columns from latest summary
    history_cols = [get_history_col_name(rc['name']) for rc in rolling_columns]
    
    # =========================================================================
    # OPTIMIZATION v9.3: Push filter down for Iceberg data file pruning
    # Instead of reading entire table then joining, we use SQL subquery
    # which allows Iceberg to prune data files based on the filter.
    # =========================================================================
    
    # Create temp view for affected keys
    affected_keys.createOrReplaceTempView("case_ii_affected_keys")
    
    # Build column list
    cols_select = ", ".join([pk, prt] + history_cols)
    
    # Use SQL with subquery - allows Iceberg to apply filter during scan
    latest_for_affected_sql = f"""
        SELECT {cols_select}
        FROM {latest_summary_table} s
        WHERE s.{pk} IN (SELECT {pk} FROM case_ii_affected_keys)
    """
    
    logger.info(f"  Using SQL subquery for filter pushdown (Iceberg optimization)")
    latest_for_affected = spark.sql(latest_for_affected_sql)
    
    # FIX v9.4.5: Collect peer forward entries into MAP for gap filling (Chaining)
    # Map Key: month_int, Map Value: Struct(val_col1, val_col2...)
    val_struct_fields = []
    for rc in rolling_columns:
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in case_ii_df.columns else src_col
        val_struct_fields.append(F.col(value_col).alias(f"val_{rc['name']}"))
        
    peer_window = Window.partitionBy(pk)
    case_ii_df = case_ii_df.withColumn(
        "peer_map", 
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("month_int"), 
                    F.struct(*val_struct_fields)
                )
            ).over(peer_window)
        )
    )
    
    # Create temp views for SQL
    case_ii_df.createOrReplaceTempView("case_ii_records")
    latest_for_affected.createOrReplaceTempView("latest_summary_affected")
    
    # Build SQL for array shifting
    shift_exprs = []
    for rc in rolling_columns:
        array_name = get_history_col_name(rc['name'])
        # Use 'mapper_column' to match original format
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        mapper_expr = rc.get('mapper_expr', src_col)
        
        # Use prepared column if available
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in case_ii_df.columns else src_col
        val_col_name = f"val_{rc['name']}"
        
        # Determine SQL type for CAST(NULL AS type) if needed, but transform handles it via map lookup
        # transform(sequence(1, gap), i -> peer_map[month - i].val)
        
        shift_expr = f"""
            CASE
                WHEN c.MONTH_DIFF = 1 THEN
                    slice(concat(array(c.{value_col}), p.{array_name}), 1, {history_len})
                WHEN c.MONTH_DIFF > 1 THEN
                    slice(
                        concat(
                            array(c.{value_col}),
                            transform(
                                sequence(1, c.MONTH_DIFF - 1),
                                i -> c.peer_map[CAST(c.month_int - i AS INT)].{val_col_name}
                            ),
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
    for gc in grid_columns:
        # Use 'mapper_rolling_column' to match original format
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = get_history_col_name(source_rolling)
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        
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

def process_case_iii(spark: SparkSession, case_iii_df, config: Dict[str, Any]):
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
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = config['destination_table']
    history_len = get_history_length(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    broadcast_threshold = get_broadcast_threshold(config)
    cache_level = get_cache_level(config)
    summary_columns = config.get('summary_columns', [])
    
    # Get affected accounts
    affected_accounts = case_iii_df.select(pk).distinct()
    affected_count = affected_accounts.count()
    logger.info(f"Affected accounts: {affected_count:,}")
    
    # =========================================================================
    # OPTIMIZATION v9.3: PARTITION PRUNING
    # Instead of reading the ENTIRE summary table, we use partition filters
    # to only read partitions that could be affected by the backfill.
    # 
    # For backfill at month M, we need:
    # - Prior months (for inheriting history) - partitions < M
    # - Future months (for updating arrays) - partitions > M, within 36 months
    #
    # So we filter: partition >= (min_backfill_month - 36) AND partition <= (max_backfill_month + 36)
    # =========================================================================
    
    # Get the min/max backfill months to calculate partition range
    backfill_range = case_iii_df.agg(
        F.min(prt).alias("min_backfill_month"),
        F.max(prt).alias("max_backfill_month")
    ).first()
    
    min_backfill = str(backfill_range['min_backfill_month'])
    max_backfill = str(backfill_range['max_backfill_month'])
    
    logger.info(f"Backfill month range: {min_backfill} to {max_backfill}")
    
    # Calculate the partition range we need to read
    # We need prior months (up to 36 months before min backfill) for history inheritance
    # We need future months (up to 36 months after max backfill) for updating
    # Convert to month_int for calculation
    min_backfill_int = int(min_backfill[:4]) * 12 + int(min_backfill[5:7])
    max_backfill_int = int(max_backfill[:4]) * 12 + int(max_backfill[5:7])
    
    # Calculate boundary months
    earliest_needed_int = min_backfill_int - history_len
    latest_needed_int = max_backfill_int + history_len
    
    earliest_year = earliest_needed_int // 12
    earliest_month = earliest_needed_int % 12
    if earliest_month == 0:
        earliest_month = 12
        earliest_year -= 1
    earliest_partition = f"{earliest_year}-{earliest_month:02d}"
    
    latest_year = latest_needed_int // 12
    latest_month = latest_needed_int % 12
    if latest_month == 0:
        latest_month = 12
        latest_year -= 1
    latest_partition = f"{latest_year}-{latest_month:02d}"
    
    logger.info(f"Reading summary partitions from {earliest_partition} to {latest_partition}")
    
    # Load history columns list
    history_cols = [get_history_col_name(rc['name']) for rc in rolling_columns]
    
    # Build select for summary - include all columns we need
    summary_select_cols = [pk, prt, ts] + history_cols
    
    # Get available columns from summary table schema (without reading data)
    summary_schema = spark.read.table(summary_table).schema
    available_summary_cols = [f.name for f in summary_schema.fields]
    
    # Add any scalar columns from summary_columns config that exist
    scalar_cols = []
    for col in summary_columns:
        if col in available_summary_cols and col not in summary_select_cols and not col.endswith('_history'):
            scalar_cols.append(col)
            summary_select_cols.append(col)
    
    # PARTITION-PRUNED READ: Only read the partitions we need
    # This is the key optimization - instead of spark.read.table() which reads everything,
    # we use SQL with partition filter to leverage Iceberg's partition pruning
    partition_filter_sql = f"""
        SELECT {', '.join([c for c in summary_select_cols if c in available_summary_cols])}
        FROM {summary_table}
        WHERE {prt} >= '{earliest_partition}' AND {prt} <= '{latest_partition}'
    """
    summary_df = spark.sql(partition_filter_sql)
    
    logger.info(f"Applied partition filter: {prt} BETWEEN '{earliest_partition}' AND '{latest_partition}'")
    
    # Filter to affected accounts (use broadcast if small)
    if affected_count < broadcast_threshold:
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
    
    # =========================================================================
    # OPTIMIZATION: Pre-calculate peer backfills for gap filling (A1+A2 Combined)
    # =========================================================================
    # Collect all backfill values for the account into a MAP to fill gaps immediately
    # Map Key: month_int, Map Value: Struct(val_col1, val_col2...)
    val_struct_fields = []
    for rc in rolling_columns:
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in case_iii_df.columns else src_col
        val_struct_fields.append(F.col(value_col).alias(f"val_{rc['name']}"))
        
    peer_window = Window.partitionBy(pk)
    case_iii_with_peers = case_iii_df.withColumn(
        "peer_map", 
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("month_int"), 
                    F.struct(*val_struct_fields)
                )
            ).over(peer_window)
        )
    )
    
    # Create temp views
    case_iii_with_peers.createOrReplaceTempView("backfill_records")
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
            {month_to_int_expr(f"s.{prt}")} as prior_month_int,
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
    for rc in rolling_columns:
        array_name = get_history_col_name(rc['name'])
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in case_iii_df.columns else src_col
        val_col_name = f"val_{rc['name']}"
        
        # Logic: 
        # 1. Start with current value
        # 2. Fill Gap: Use transform on sequence to lookup peer_map
        #    Map lookup returns NULL if key not found (perfect for gaps)
        # 3. Append Prior History - AND PATCH IT with peer_map to resolve conflicts
        new_row_expr = f"""
            CASE
                WHEN prior_month IS NOT NULL AND months_since_prior > 0 THEN
                    slice(
                        concat(
                            array({value_col}),
                            CASE 
                                WHEN months_since_prior > 1 THEN 
                                    transform(
                                        sequence(1, months_since_prior - 1),
                                        i -> peer_map[CAST(month_int - i AS INT)].{val_col_name}
                                    )
                                ELSE array()
                            END,
                            transform(
                                prior_{array_name},
                                (val, i) -> COALESCE(
                                    peer_map[CAST(prior_month_int - i AS INT)].{val_col_name},
                                    val
                                )
                            )
                        ),
                        1, {history_len}
                    )
                ELSE
                    concat(
                        array({value_col}),
                        transform(
                            sequence(1, {history_len} - 1),
                            i -> peer_map[CAST(month_int - i AS INT)].{val_col_name}
                        )
                    )
            END as {array_name}
        """
        new_row_exprs.append(new_row_expr)
    
    # Get columns from backfill record (excluding temp columns)
    exclude_backfill = {"month_int", "max_existing_month", "max_existing_ts", 
                       "max_month_int", "case_type", "MONTH_DIFF",
                       "prior_month", "prior_month_int", "prior_ts", "months_since_prior"}
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
    for rc in rolling_columns:
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in case_iii_df.columns else src_col
        backfill_value_cols.append(f'b.{value_col} as backfill_{rc["name"]}')
    
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
    
    future_joined.persist(cache_level)
    future_count = future_joined.count()
    logger.info(f"Valid future summary updates: {future_count:,}")
    
    # Store future_joined data for Part B (direct UPDATE later)
    # We'll return this separately for the pipeline to handle
    backfill_updates_df = None
    
    if future_count > 0:
        future_joined.createOrReplaceTempView("future_backfill_joined")
        
        # =====================================================================
        # MULTIPLE BACKFILL FIX v9.4.1:
        # When multiple backfills arrive for the same account in the same batch,
        # we need to MERGE all their updates into the future summary rows.
        # 
        # Strategy:
        # 1. Collect all (position, value) pairs for each account+month+array
        # 2. Create a map from positions to values
        # 3. Apply all updates in one pass using transform()
        # =====================================================================
        
        # First, collect backfill positions and values per account+month
        # We need to aggregate: for each (pk, summary_month), collect all backfills
        backfill_collect_cols = [f"backfill_{rc['name']}" for rc in rolling_columns]
        existing_array_cols = [f"existing_{get_history_col_name(rc['name'])}" for rc in rolling_columns]
        
        # Create a struct with position and all backfill values, then collect as array
        struct_fields = ["backfill_position"] + backfill_collect_cols
        
        # For each account+month, collect all backfill structs and the existing arrays
        # We use first() for existing arrays since they're the same for all rows of same account+month
        agg_exprs = [
            F.collect_list(F.struct(*[F.col(c) for c in struct_fields])).alias("backfill_list")
        ]
        for rc in rolling_columns:
            array_name = get_history_col_name(rc['name'])
            agg_exprs.append(F.first(f"existing_{array_name}").alias(f"existing_{array_name}"))
        
        aggregated_df = future_joined.groupBy(pk, "summary_month").agg(*agg_exprs)
        aggregated_df.createOrReplaceTempView("aggregated_backfills")
        
        # Build array update expressions that apply ALL collected backfills
        # For each position in the array, check if any backfill targets that position
        update_exprs = []
        for rc in rolling_columns:
            array_name = get_history_col_name(rc['name'])
            backfill_col = f"backfill_{rc['name']}"
            
            # Get the data type for this column (Integer or String)
            data_type = rc.get('type', rc.get('data_type', 'Integer'))
            if data_type.lower() == 'string':
                sql_type = 'STRING'
            elif data_type.lower() in ('double', 'float', 'decimal'):
                sql_type = 'DOUBLE'
            else:
                sql_type = 'BIGINT'
            
            # Use transform with aggregate to check all backfills for this position
            # For each array element at index i:
            #   - Check if any backfill has position == i
            #   - If yes, use that backfill's value; if no, keep original
            update_expr = f"""
                transform(
                    existing_{array_name},
                    (x, i) -> COALESCE(
                        aggregate(
                            backfill_list,
                            CAST(NULL AS {sql_type}),
                            (acc, b) -> CASE WHEN b.backfill_position = i THEN b.{backfill_col} ELSE acc END
                        ),
                        x
                    )
                ) as {array_name}
            """
            update_exprs.append(update_expr)
        
        # Create DataFrame for backfill updates with all backfills merged
        backfill_updates_df = spark.sql(f"""
            SELECT 
                {pk},
                summary_month as {prt},
                {', '.join(update_exprs)}
            FROM aggregated_backfills
        """)
        
        updates_after_merge = backfill_updates_df.count()
        logger.info(f"Prepared {updates_after_merge:,} future summary rows for direct UPDATE (merged from {future_count:,} individual updates)")
    
    future_joined.unpersist()
    
    # =========================================================================
    # PART C: Return new backfill rows only (Part B handled separately)
    # =========================================================================
    logger.info("Part C: Preparing new backfill rows for temp table...")
    
    # Generate grid columns for new rows
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = get_history_col_name(source_rolling)
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        
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
    
    # Generate grid columns for backfill_updates_df (for direct UPDATE)
    if backfill_updates_df is not None:
        for gc in grid_columns:
            source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
            source_history = get_history_col_name(source_rolling)
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('seperator', gc.get('separator', ''))
            
            backfill_updates_df = backfill_updates_df.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.col(source_history),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )
    
    # Deduplicate new backfill rows - keep only one row per account+month
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
    new_backfill_rows = (
        new_backfill_rows
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    new_rows_count = new_backfill_rows.count()
    updates_count = backfill_updates_df.count() if backfill_updates_df is not None else 0
    
    logger.info(f"Case III Part A complete: {new_rows_count:,} new backfill rows for temp table")
    logger.info(f"Case III Part B prepared: {updates_count:,} future rows for direct UPDATE")
    
    # Return tuple: (new_rows for temp table, updates for direct UPDATE)
    return (new_backfill_rows, backfill_updates_df)


def apply_backfill_updates(spark: SparkSession, backfill_updates_df, config: Dict[str, Any]):
    """
    Apply Case III Part B updates directly to the summary table.
    
    This function performs a direct UPDATE to existing summary rows,
    only modifying the rolling arrays + grid + updated_base_ts columns.
    All other columns remain unchanged.
    
    Args:
        spark: SparkSession
        backfill_updates_df: DataFrame with updated arrays for future rows
                            Columns: pk, prt, 7 array columns, 1 grid column
        config: Config dict
    
    Returns:
        Number of rows updated
    """
    if backfill_updates_df is None:
        logger.info("No backfill updates to apply")
        return 0
    
    update_count = backfill_updates_df.count()
    if update_count == 0:
        logger.info("No backfill updates to apply")
        return 0
    
    logger.info("=" * 80)
    logger.info("STEP: Apply Case III Part B - Direct UPDATE to Summary")
    logger.info("=" * 80)
    logger.info(f"Updating {update_count:,} existing summary rows with backfill data")
    
    pk = config['primary_column']
    prt = config['partition_column']
    summary_table = config['destination_table']
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    
    # Create temp view for the updates
    backfill_updates_df.createOrReplaceTempView("backfill_updates")
    
    # Build the column list for UPDATE SET
    # Only update: 7 arrays + 1 grid + updated_base_ts
    update_columns = []
    
    # Add rolling array columns
    for rc in rolling_columns:
        array_name = get_history_col_name(rc['name'])
        update_columns.append(f"t.{array_name} = s.{array_name}")
    
    # Add grid columns
    for gc in grid_columns:
        update_columns.append(f"t.{gc['name']} = s.{gc['name']}")
    
    # Add updated_base_ts
    update_columns.append("t.updated_base_ts = current_timestamp()")
    
    update_set_clause = ", ".join(update_columns)
    
    # Execute the MERGE (UPDATE only - no INSERT)
    merge_sql = f"""
        MERGE INTO {summary_table} t
        USING backfill_updates s
        ON t.{pk} = s.{pk} AND t.{prt} = s.{prt}
        WHEN MATCHED THEN UPDATE SET {update_set_clause}
    """
    
    spark.sql(merge_sql)
    
    logger.info(f"Applied backfill updates to {update_count:,} summary rows")
    logger.info(f"Updated columns: 7 arrays + 1 grid + updated_base_ts")
    
    return update_count


# ============================================================================
# STEP 4b: PROCESS CASE IV (BULK HISTORICAL LOAD)
# ============================================================================

def process_case_iv(spark: SparkSession, case_iv_df, case_i_results, config: Dict[str, Any]):
    """
    Process Case IV - Bulk historical load for new accounts with multiple months
    
    Uses SQL window functions to build rolling arrays efficiently in a single pass.
    
    Logic:
    - For new accounts with multiple months uploaded at once
    - Build rolling history arrays using window functions
    - Each month's array contains up to 36 months of prior data from the batch
    
    Args:
        spark: SparkSession
        case_iv_df: DataFrame with Case IV records (subsequent months for new accounts)
        case_i_results: DataFrame with Case I results (first month for each new account)
        config: Config dict
    
    Returns:
        DataFrame with correctly built summary records
    """
    logger.info("=" * 80)
    logger.info("STEP 2d: Process Case IV (Bulk Historical Load)")
    logger.info("=" * 80)
    
    count = case_iv_df.count()
    if count == 0:
        logger.info("No Case IV records to process")
        return None
    
    logger.info(f"Processing {count:,} bulk historical records")
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    history_len = get_history_length(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    
    # Combine Case I results with Case IV records to build complete history
    # Case I records are the "first month" for each new account
    # Case IV records are "subsequent months" that need rolling history
    
    # Get unique accounts in Case IV
    case_iv_accounts = case_iv_df.select(pk).distinct()
    
    # Get Case I records for these accounts (the first month data)
    if case_i_results is not None:
        first_month_data = case_i_results.join(case_iv_accounts, pk, "inner")
    else:
        # If no Case I results yet, we need to get them from the classified data
        first_month_data = None
    
    # Prepare columns needed for rolling history
    # We need to collect all source values by account, ordered by month
    
    # Build the complete history for all months using window functions
    case_iv_df.createOrReplaceTempView("case_iv_records")
    
    # For each rolling column, we need to build an array using:
    # COLLECT_LIST with window ORDER BY month_int ROWS BETWEEN 35 PRECEDING AND CURRENT ROW
    # But we need to REVERSE and pad with NULLs
    
    # Get all months for bulk historical accounts (including the first month)
    # We need to include Case I records to build complete history
    logger.info("Building complete history using window functions")
    
    all_new_account_months = spark.sql(f"""
        SELECT * FROM case_iv_records
    """)
    
    # Add Case I first months if available
    if case_i_results is not None and case_i_results.count() > 0:
        # Get Case I records that belong to accounts with Case IV records
        case_i_for_iv = case_i_results.join(case_iv_accounts, pk, "inner")
        
        # We need to ensure the columns match
        common_cols = list(set(case_iv_df.columns) & set(case_i_for_iv.columns))
        if common_cols:
            # Add month_int if not present in case_i_results
            if "month_int" not in case_i_for_iv.columns:
                case_i_for_iv = case_i_for_iv.withColumn(
                    "month_int",
                    F.expr(month_to_int_expr(prt))
                )
            
            # Select matching columns and union
            case_iv_cols = case_iv_df.columns
            
            # Add missing columns to case_i_for_iv with NULL values
            for col in case_iv_cols:
                if col not in case_i_for_iv.columns:
                    case_i_for_iv = case_i_for_iv.withColumn(col, F.lit(None))
            
            case_i_for_iv = case_i_for_iv.select(*case_iv_cols)
            all_new_account_months = case_iv_df.unionByName(case_i_for_iv, allowMissingColumns=True)
    
    all_new_account_months.createOrReplaceTempView("all_bulk_records")
    
    # =========================================================================
    # FIX FOR GAP HANDLING (v9.2.1)
    # =========================================================================
    # Previous approach using COLLECT_LIST doesn't handle gaps correctly.
    # COLLECT_LIST only collects existing values, ignoring month gaps.
    # 
    # Example problem:
    #   - Jan 2025: 6000, Mar 2025: 5400 (Feb missing)
    #   - COLLECT_LIST returns: [6000, 5400]
    #   - After reverse: [5400, 6000, NULL, NULL, ...]
    #   - WRONG: bal[1]=6000 (should be NULL for Feb gap!)
    #
    # Solution: Use MAP_FROM_ENTRIES + TRANSFORM to look up values by month_int
    #   - Create MAP of month_int -> value for each account
    #   - For each row, generate positions 0-35
    #   - Look up value for (current_month_int - position)
    #   - Missing months return NULL automatically
    # =========================================================================
    
    # Step 1: Build MAP for each column by account
    # MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, value)))
    map_build_exprs = []
    for rc in rolling_columns:
        col_name = rc['name']
        src_col = rc.get('mapper_column', rc.get('source_column', col_name))
        mapper_expr = rc.get('mapper_expr', src_col)
        
        prepared_col = f"_prepared_{col_name}"
        value_col = prepared_col if prepared_col in all_new_account_months.columns else src_col
        
        map_build_exprs.append(f"""
            MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, {value_col}))) as value_map_{col_name}
        """)
    
    # Create account-level maps
    map_sql = f"""
        SELECT 
            {pk},
            {', '.join(map_build_exprs)}
        FROM all_bulk_records
        GROUP BY {pk}
    """
    
    account_maps = spark.sql(map_sql)
    account_maps.createOrReplaceTempView("account_value_maps")
    
    # Step 2: Join maps back to records and build arrays using TRANSFORM
    # For each position 0-35, look up value from map using (month_int - position)
    
    array_build_exprs = []
    for rc in rolling_columns:
        col_name = rc['name']
        # Use 'type' to match original format (instead of 'data_type')
        data_type = rc.get('type', rc.get('data_type', 'String'))
        array_name = get_history_col_name(col_name)
        
        # TRANSFORM generates array by looking up each position
        # Position 0 = current month, Position N = N months ago
        # If month doesn't exist in map, returns NULL (correct for gaps!)
        array_build_exprs.append(f"""
            TRANSFORM(
                SEQUENCE(0, {history_len - 1}),
                pos -> CAST(m.value_map_{col_name}[r.month_int - pos] AS {data_type.upper()})
            ) as {array_name}
        """)
    
    # Get base columns from the records table
    exclude_cols = set(["month_int", "max_existing_month", "max_existing_ts", "max_month_int",
                        "case_type", "MONTH_DIFF", "min_month_for_new_account", 
                        "count_months_for_new_account"])
    exclude_cols.update([c for c in all_new_account_months.columns if c.startswith("_prepared_")])
    
    base_cols = [f"r.{c}" for c in all_new_account_months.columns if c not in exclude_cols]
    
    # Build final SQL joining records with maps
    final_sql = f"""
        SELECT 
            {', '.join(base_cols)},
            {', '.join(array_build_exprs)}
        FROM all_bulk_records r
        JOIN account_value_maps m ON r.{pk} = m.{pk}
    """
    
    result = spark.sql(final_sql)
    
    # Generate grid columns
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = get_history_col_name(source_rolling)
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        
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
    
    # Filter to only Case IV records (exclude the first month which is already processed as Case I)
    # Join back with original case_iv_df to get only the subsequent months
    result = result.join(
        case_iv_df.select(pk, prt).distinct(),
        [pk, prt],
        "inner"
    )
    
    result_count = result.count()
    logger.info(f"Case IV complete: {result_count:,} bulk historical records with rolling arrays")
    
    return result


# ============================================================================
# STEP 5: WRITE RESULTS
# ============================================================================

def build_merge_clauses(source_cols: list, pk: str, prt: str = None) -> tuple:
    """
    Build explicit UPDATE SET and INSERT clauses for MERGE statements.
    
    This avoids using 'UPDATE SET *' and 'INSERT *' which require ALL target
    table columns to exist in the source DataFrame.
    
    Args:
        source_cols: List of columns in the source DataFrame
        pk: Primary key column name
        prt: Partition column name (optional)
        
    Returns:
        Tuple of (update_clause, insert_cols, insert_vals)
    """
    # Exclude 'rn' if present (used for row_number windowing)
    cols_for_merge = [c for c in source_cols if c != 'rn']
    
    # Build UPDATE SET clause - exclude primary key (and partition for latest_summary)
    exclude_from_update = {pk}
    if prt:
        exclude_from_update.add(prt)
    
    update_cols = [c for c in cols_for_merge if c not in exclude_from_update]
    update_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
    
    # Build INSERT clause
    insert_cols = ", ".join(cols_for_merge)
    insert_vals = ", ".join([f"s.{c}" for c in cols_for_merge])
    
    return update_clause, insert_cols, insert_vals


def write_results(spark: SparkSession, result_df, config: Dict[str, Any], 
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
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = config['destination_table']
    latest_summary_table = config['latest_history_table']
    summary_columns = config.get('summary_columns', [])
    latest_summary_columns = config.get('latest_summary_columns', 
                                        config.get('latest_history_addon_cols', []))
    
    # Select only columns that should go to summary table
    if summary_columns:
        available_cols = result_df.columns
        summary_cols = [c for c in summary_columns if c in available_cols]
        result_for_summary = result_df.select(*summary_cols)
    else:
        result_for_summary = result_df
    
    result_for_summary.createOrReplaceTempView("pipeline_result")
    count = result_for_summary.count()
    
    if mode == "merge":
        # MERGE to summary table using explicit column lists
        # This avoids errors when source doesn't have all target table columns
        logger.info(f"Merging {count:,} records to {summary_table}")
        
        summary_source_cols = result_for_summary.columns
        summary_update, summary_insert_cols, summary_insert_vals = build_merge_clauses(
            summary_source_cols, pk, prt
        )
        
        spark.sql(f"""
            MERGE INTO {summary_table} t
            USING pipeline_result s
            ON t.{pk} = s.{pk} AND t.{prt} = s.{prt}
            WHEN MATCHED THEN UPDATE SET {summary_update}
            WHEN NOT MATCHED THEN INSERT ({summary_insert_cols}) VALUES ({summary_insert_vals})
        """)
        
        # Prepare data for latest_summary
        if latest_summary_columns:
            available_cols = result_df.columns
            latest_cols = [c for c in latest_summary_columns if c in available_cols]
            # Always include primary key and partition
            if pk not in latest_cols:
                latest_cols.insert(0, pk)
            if prt not in latest_cols:
                latest_cols.insert(1, prt)
            result_for_latest = result_df.select(*latest_cols)
        else:
            result_for_latest = result_df
        
        result_for_latest.createOrReplaceTempView("pipeline_result_latest")
        
        # MERGE to latest_summary (keep only latest per account)
        # Uses explicit column lists to avoid column resolution errors
        logger.info(f"Updating {latest_summary_table}")
        
        # Get columns from the result_for_latest DataFrame
        latest_source_cols = result_for_latest.columns
        # For latest_summary, we only use pk in ON clause (no prt in match condition)
        # IMPORTANT: Pass None for prt so that rpt_as_of_mo IS included in UPDATE clause
        # (latest_summary needs to update the month when a newer entry arrives)
        latest_update, latest_insert_cols, latest_insert_vals = build_merge_clauses(
            latest_source_cols, pk, None  # Don't exclude prt - it must be updated
        )
        
        spark.sql(f"""
            MERGE INTO {latest_summary_table} t
            USING (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk} ORDER BY {prt} DESC) as rn
                    FROM pipeline_result_latest
                ) WHERE rn = 1
            ) s
            ON t.{pk} = s.{pk}
            WHEN MATCHED AND s.{prt} >= t.{prt} THEN UPDATE SET {latest_update}
            WHEN NOT MATCHED THEN INSERT ({latest_insert_cols}) VALUES ({latest_insert_vals})
        """)
    else:
        # Overwrite mode
        logger.info(f"Writing {count:,} records to {summary_table}")
        result_for_summary.writeTo(summary_table).using("iceberg").createOrReplace()
    
    logger.info(f"Write complete: {count:,} records")
    return count


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_pipeline(spark: SparkSession, config: Dict[str, Any], 
                 filter_expr: str = None) -> Dict[str, Any]:
    """
    Main pipeline execution with TEMP TABLE approach.
    
    VERSION 9.4: Uses temp table to break Spark lineage and improve performance.
    
    Benefits of temp table approach:
    - Breaks complex DAG lineage (prevents driver OOM)
    - Implicit checkpointing per case
    - Memory released after each case write
    - Easier debugging (can query temp table)
    - Fault tolerance (partial progress preserved)
    
    Processing Order (CRITICAL for correctness):
    1. Backfill FIRST (rebuilds history with corrections)
    2. New accounts SECOND (no dependencies)
    3. Bulk Historical THIRD (builds arrays for new multi-month accounts)
    4. Forward LAST (uses corrected history from backfill)
    
    Args:
        spark: SparkSession
        config: Config dict
        filter_expr: Optional SQL filter for accounts table
    
    Returns:
        Dict with processing statistics
    """
    start_time = time.time()
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE V9.4 - TEMP TABLE OPTIMIZED - START")
    logger.info("=" * 80)
    
    stats = {
        'total_records': 0,
        'case_i_records': 0,
        'case_ii_records': 0,
        'case_iii_records': 0,
        'case_iv_records': 0,
        'records_written': 0,
        'temp_table': None
    }
    
    # Generate unique temp table name
    temp_table_name = generate_temp_table_name(config)
    stats['temp_table'] = temp_table_name
    logger.info(f"Using temp table: {temp_table_name}")
    
    try:
        # Step 1: Load and classify
        classified = load_and_classify_accounts(spark, config, filter_expr)
        
        # Get counts
        case_counts = classified.groupBy("case_type").count().collect()
        case_dict = {row["case_type"]: row["count"] for row in case_counts}
        stats['case_i_records'] = case_dict.get('CASE_I', 0)
        stats['case_ii_records'] = case_dict.get('CASE_II', 0)
        stats['case_iii_records'] = case_dict.get('CASE_III', 0)
        stats['case_iv_records'] = case_dict.get('CASE_IV', 0)
        stats['total_records'] = sum(case_dict.values())
        
        if stats['total_records'] == 0:
            logger.info("No records to process")
            return stats
        
        # Configure partitions based on total records
        num_partitions = calculate_partitions(stats['total_records'], config)
        configure_spark(spark, num_partitions, config)
        logger.info(f"Configured {num_partitions} partitions for {stats['total_records']:,} records")
        
        # =========================================================================
        # STEP 2: CREATE TEMP TABLE
        # =========================================================================
        logger.info("=" * 80)
        logger.info("STEP 2: Create Temp Table")
        logger.info("=" * 80)
        create_temp_table(spark, temp_table_name, config)
        
        # =========================================================================
        # STEP 3: PROCESS EACH CASE AND WRITE TO TEMP TABLE
        # Each case writes to temp table, breaking lineage automatically
        # =========================================================================
        
        # Track records written to temp table
        temp_records = 0
        
        # Store backfill updates for Part B (direct UPDATE after main MERGE)
        backfill_updates_df = None
        
        # Process in CORRECT order: Backfill -> New -> Bulk Historical -> Forward
        
        # 3a. Process Case III (Backfill) - HIGHEST PRIORITY
        # Returns tuple: (new_rows for temp table, updates for direct UPDATE)
        case_iii_df = classified.filter(F.col("case_type") == "CASE_III")
        case_iii_result = None
        if stats['case_iii_records'] > 0:
            logger.info(f"\n>>> PROCESSING BACKFILL ({stats['case_iii_records']:,} records)")
            case_iii_output = process_case_iii(spark, case_iii_df, config)
            if case_iii_output is not None:
                case_iii_result, backfill_updates_df = case_iii_output
                if case_iii_result is not None:
                    case_iii_result = case_iii_result.withColumn("_case_type", F.lit("CASE_III"))
                    temp_records += write_to_temp_table(spark, case_iii_result, temp_table_name, "CASE_III")
        
        # 3b. Process Case I (New Accounts - first month only)
        case_i_df = classified.filter(F.col("case_type") == "CASE_I")
        case_i_result = None
        if stats['case_i_records'] > 0:
            logger.info(f"\n>>> PROCESSING NEW ACCOUNTS ({stats['case_i_records']:,} records)")
            case_i_result = process_case_i(case_i_df, config)
            if case_i_result is not None:
                case_i_result = case_i_result.withColumn("_case_type", F.lit("CASE_I"))
                temp_records += write_to_temp_table(spark, case_i_result, temp_table_name, "CASE_I")
        
        # 3c. Process Case IV (Bulk Historical) - after Case I so we have first months
        case_iv_df = classified.filter(F.col("case_type") == "CASE_IV")
        if stats['case_iv_records'] > 0:
            logger.info(f"\n>>> PROCESSING BULK HISTORICAL ({stats['case_iv_records']:,} records)")
            case_iv_result = process_case_iv(spark, case_iv_df, case_i_result, config)
            if case_iv_result is not None:
                case_iv_result = case_iv_result.withColumn("_case_type", F.lit("CASE_IV"))
                temp_records += write_to_temp_table(spark, case_iv_result, temp_table_name, "CASE_IV")
        
        # 3d. Process Case II (Forward Entries) - LOWEST PRIORITY
        case_ii_df = classified.filter(F.col("case_type") == "CASE_II")
        if stats['case_ii_records'] > 0:
            logger.info(f"\n>>> PROCESSING FORWARD ENTRIES ({stats['case_ii_records']:,} records)")
            case_ii_result = process_case_ii(spark, case_ii_df, config)
            if case_ii_result is not None:
                case_ii_result = case_ii_result.withColumn("_case_type", F.lit("CASE_II"))
                temp_records += write_to_temp_table(spark, case_ii_result, temp_table_name, "CASE_II")
        
        logger.info(f"\nTotal records in temp table: {temp_records:,}")
        
        # =========================================================================
        # STEP 4: READ FROM TEMP TABLE, DEDUPLICATE, AND MERGE
        # This is now a CLEAN operation with no complex lineage
        # =========================================================================
        if temp_records > 0:
            logger.info("=" * 80)
            logger.info("STEP 4: Read Temp Table and Merge to Summary")
            logger.info("=" * 80)
            
            # Read from temp table (clean lineage!)
            combined_result = spark.table(temp_table_name)
            
            # Deduplicate - if same account+month appears multiple times, keep the one
            # from the highest priority case type (III > I > IV > II)
            # This handles edge cases where backfill might create a row that's also in Case I
            
            # Define case priority (lower = higher priority)
            combined_result = combined_result.withColumn(
                "_case_priority",
                F.when(F.col("_case_type") == "CASE_III", F.lit(1))
                .when(F.col("_case_type") == "CASE_I", F.lit(2))
                .when(F.col("_case_type") == "CASE_IV", F.lit(3))
                .when(F.col("_case_type") == "CASE_II", F.lit(4))
                .otherwise(F.lit(99))
            )
            
            # Deduplicate by account+month, keeping highest priority
            window_spec = Window.partitionBy(pk, prt).orderBy(F.col("_case_priority").asc())
            combined_result = (
                combined_result
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn", "_case_type", "_case_priority")
            )
            
            stats['records_written'] = write_results(spark, combined_result, config)
            
            # =========================================================================
            # STEP 5: Apply Case III Part B - Direct UPDATE to existing summary rows
            # This must happen AFTER the main MERGE to avoid conflicts
            # =========================================================================
            if backfill_updates_df is not None:
                stats['backfill_updates'] = apply_backfill_updates(spark, backfill_updates_df, config)
            else:
                stats['backfill_updates'] = 0
        
        # Cleanup classified DataFrame
        classified.unpersist()
        
        end_time = time.time()
        duration = (end_time - start_time) / 60
        
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE V9.4 - COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total records:      {stats['total_records']:,}")
        logger.info(f"  Case I (new):     {stats['case_i_records']:,}")
        logger.info(f"  Case II (fwd):    {stats['case_ii_records']:,}")
        logger.info(f"  Case III (bkfl):  {stats['case_iii_records']:,}")
        logger.info(f"  Case IV (bulk):   {stats['case_iv_records']:,}")
        logger.info(f"Records written:    {stats['records_written']:,}")
        logger.info(f"Backfill updates:   {stats.get('backfill_updates', 0):,}")
        logger.info(f"Duration:           {duration:.2f} minutes")
        logger.info("=" * 80)
        
        stats['duration_minutes'] = duration
        return stats
    
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    
    finally:
        # ALWAYS cleanup temp table
        logger.info("-" * 40)
        logger.info("Cleaning up...")
        drop_temp_table(spark, temp_table_name, purge=True)
        spark.catalog.clearCache()


def run_pipeline_legacy(spark: SparkSession, config: Dict[str, Any], 
                        filter_expr: str = None) -> Dict[str, Any]:
    """
    Legacy pipeline execution (v9.3) - uses in-memory union instead of temp table.
    
    Kept for backwards compatibility and comparison testing.
    Use run_pipeline() for production.
    
    Processing Order (CRITICAL for correctness):
    1. Backfill FIRST (rebuilds history with corrections)
    2. New accounts SECOND (no dependencies)
    3. Forward LAST (uses corrected history from backfill)
    
    Args:
        spark: SparkSession
        config: Config dict
        filter_expr: Optional SQL filter for accounts table
    
    Returns:
        Dict with processing statistics
    """
    start_time = time.time()
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    
    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE V9.3 LEGACY - START")
    logger.info("=" * 80)
    
    stats = {
        'total_records': 0,
        'case_i_records': 0,
        'case_ii_records': 0,
        'case_iii_records': 0,
        'case_iv_records': 0,
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
        stats['case_iv_records'] = case_dict.get('CASE_IV', 0)
        stats['total_records'] = sum(case_dict.values())
        
        # Configure partitions based on total records
        num_partitions = calculate_partitions(stats['total_records'], config)
        configure_spark(spark, num_partitions, config)
        logger.info(f"Configured {num_partitions} partitions for {stats['total_records']:,} records")
        
        # =========================================================================
        # OPTIMIZATION v9.3: COLLECT ALL RESULTS, SINGLE MERGE
        # Instead of 4 separate MERGE operations (one per case type),
        # we collect all results and do a single MERGE at the end.
        # This reduces write amplification by 4x.
        # =========================================================================
        all_results = []
        
        # Store backfill updates for Part B (direct UPDATE after main MERGE)
        backfill_updates_df = None
        
        # Process in CORRECT order: Backfill -> New -> Bulk Historical -> Forward
        
        # 1. Process Case III (Backfill) - HIGHEST PRIORITY
        # Returns tuple: (new_rows, updates for direct UPDATE)
        case_iii_df = classified.filter(F.col("case_type") == "CASE_III")
        case_iii_result = None
        if stats['case_iii_records'] > 0:
            logger.info(f"\n>>> PROCESSING BACKFILL ({stats['case_iii_records']:,} records)")
            case_iii_output = process_case_iii(spark, case_iii_df, config)
            if case_iii_output is not None:
                case_iii_result, backfill_updates_df = case_iii_output
                if case_iii_result is not None:
                    case_iii_result = case_iii_result.withColumn("_case_type", F.lit("CASE_III"))
                    all_results.append(case_iii_result)
        
        # 2. Process Case I (New Accounts - first month only)
        case_i_df = classified.filter(F.col("case_type") == "CASE_I")
        case_i_result = None
        if stats['case_i_records'] > 0:
            logger.info(f"\n>>> PROCESSING NEW ACCOUNTS ({stats['case_i_records']:,} records)")
            case_i_result = process_case_i(case_i_df, config)
            if case_i_result is not None:
                case_i_result = case_i_result.withColumn("_case_type", F.lit("CASE_I"))
                all_results.append(case_i_result)
        
        # 3. Process Case IV (Bulk Historical) - after Case I so we have first months
        case_iv_df = classified.filter(F.col("case_type") == "CASE_IV")
        if stats['case_iv_records'] > 0:
            logger.info(f"\n>>> PROCESSING BULK HISTORICAL ({stats['case_iv_records']:,} records)")
            case_iv_result = process_case_iv(spark, case_iv_df, case_i_result, config)
            if case_iv_result is not None:
                case_iv_result = case_iv_result.withColumn("_case_type", F.lit("CASE_IV"))
                all_results.append(case_iv_result)
        
        # 4. Process Case II (Forward Entries) - LOWEST PRIORITY
        case_ii_df = classified.filter(F.col("case_type") == "CASE_II")
        if stats['case_ii_records'] > 0:
            logger.info(f"\n>>> PROCESSING FORWARD ENTRIES ({stats['case_ii_records']:,} records)")
            case_ii_result = process_case_ii(spark, case_ii_df, config)
            if case_ii_result is not None:
                case_ii_result = case_ii_result.withColumn("_case_type", F.lit("CASE_II"))
                all_results.append(case_ii_result)
        
        # =========================================================================
        # SINGLE CONSOLIDATED MERGE
        # =========================================================================
        if all_results:
            logger.info(f"\n>>> WRITING ALL RESULTS (single MERGE operation)")
            
            # Union all results
            from functools import reduce
            combined_result = reduce(
                lambda a, b: a.unionByName(b, allowMissingColumns=True),
                all_results
            )
            
            # Deduplicate - if same account+month appears multiple times, keep the one
            # from the highest priority case type (III > I > IV > II)
            # This handles edge cases where backfill might create a row that's also in Case I
            
            # Define case priority (lower = higher priority)
            combined_result = combined_result.withColumn(
                "_case_priority",
                F.when(F.col("_case_type") == "CASE_III", F.lit(1))
                .when(F.col("_case_type") == "CASE_I", F.lit(2))
                .when(F.col("_case_type") == "CASE_IV", F.lit(3))
                .when(F.col("_case_type") == "CASE_II", F.lit(4))
                .otherwise(F.lit(99))
            )
            
            # Deduplicate by account+month, keeping highest priority
            window_spec = Window.partitionBy(pk, prt).orderBy(F.col("_case_priority").asc())
            combined_result = (
                combined_result
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn", "_case_type", "_case_priority")
            )
            
            stats['records_written'] = write_results(spark, combined_result, config)
            
            # =========================================================================
            # Apply Case III Part B - Direct UPDATE to existing summary rows
            # This must happen AFTER the main MERGE to avoid conflicts
            # =========================================================================
            if backfill_updates_df is not None:
                stats['backfill_updates'] = apply_backfill_updates(spark, backfill_updates_df, config)
            else:
                stats['backfill_updates'] = 0
        
        # Cleanup
        classified.unpersist()
        
        end_time = time.time()
        duration = (end_time - start_time) / 60
        
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE V9.3 LEGACY - COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Total records:      {stats['total_records']:,}")
        logger.info(f"  Case I (new):     {stats['case_i_records']:,}")
        logger.info(f"  Case II (fwd):    {stats['case_ii_records']:,}")
        logger.info(f"  Case III (bkfl):  {stats['case_iii_records']:,}")
        logger.info(f"  Case IV (bulk):   {stats['case_iv_records']:,}")
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

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create and configure Spark session with optimal settings"""
    
    spark_config = config.get('spark', {})
    app_name = spark_config.get('app_name', 'SummaryPipeline_v9.3')
    
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
    
    # Add catalog config if present
    catalog = config.get('catalog', {})
    if catalog:
        cat_name = catalog.get('name', 'primary_catalog')
        builder = builder \
            .config("spark.sql.defaultCatalog", cat_name) \
            .config(f"spark.sql.catalog.{cat_name}", "org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{cat_name}.catalog-impl", 
                   "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{cat_name}.warehouse", catalog.get('warehouse', '')) \
            .config(f"spark.sql.catalog.{cat_name}.io-impl", 
                   catalog.get('io_impl', 'org.apache.iceberg.aws.s3.S3FileIO'))
    
    # Add dynamic allocation if configured
    dyn_alloc = spark_config.get('dynamic_allocation', {})
    if dyn_alloc.get('enabled', False):
        builder = builder \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", str(dyn_alloc.get('min_executors', 50))) \
            .config("spark.dynamicAllocation.maxExecutors", str(dyn_alloc.get('max_executors', 250))) \
            .config("spark.dynamicAllocation.initialExecutors", str(dyn_alloc.get('initial_executors', 100)))
    
    return builder.getOrCreate()


# ============================================================================
# ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Summary Pipeline v9.4 - Temp Table Optimized",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run pipeline with temp table (default, recommended)
  python summary_pipeline.py --config config.json

  # Run legacy pipeline (in-memory union)
  python summary_pipeline.py --config config.json --legacy

  # Cleanup orphaned temp tables from failed runs
  python summary_pipeline.py --config config.json --cleanup-orphans

  # Cleanup with custom max age
  python summary_pipeline.py --config config.json --cleanup-orphans --max-age-hours 48
        """
    )
    parser.add_argument('--config', required=True, help='Path to pipeline config JSON')
    parser.add_argument('--filter', help='SQL filter expression for accounts')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental',
                       help='Processing mode')
    parser.add_argument('--legacy', action='store_true',
                       help='Use legacy v9.3 pipeline (in-memory union instead of temp table)')
    parser.add_argument('--cleanup-orphans', action='store_true',
                       help='Cleanup orphaned temp tables from failed runs and exit')
    parser.add_argument('--max-age-hours', type=int, default=24,
                       help='Max age in hours for orphaned temp tables (default: 24)')
    
    args = parser.parse_args()
    
    # Load config as plain dict
    config = load_config(args.config)
    if not validate_config(config):
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Cleanup orphans mode
        if args.cleanup_orphans:
            logger.info(f"Cleaning up orphaned temp tables older than {args.max_age_hours} hours...")
            cleaned = cleanup_orphaned_temp_tables(spark, config, args.max_age_hours)
            logger.info(f"Cleanup complete: removed {cleaned} orphaned temp tables")
            return
        
        # Run pipeline
        if args.legacy:
            logger.info("Using LEGACY pipeline (v9.3 in-memory union)")
            stats = run_pipeline_legacy(spark, config, args.filter)
        else:
            logger.info("Using TEMP TABLE pipeline (v9.4)")
            stats = run_pipeline(spark, config, args.filter)
        
        logger.info(f"Pipeline completed: {stats}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
