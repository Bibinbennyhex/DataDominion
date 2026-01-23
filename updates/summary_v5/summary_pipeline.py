#!/usr/bin/env python3
"""
Summary Pipeline v5.0 - Unified Production Pipeline
====================================================

A production-grade, config-driven pipeline for maintaining account summary 
tables with 36-month rolling history arrays.

Key Features:
1. Config-driven - all columns, transformations defined in JSON
2. Pure Spark SQL + DataFrame API - no UDFs for scale
3. Four processing modes:
   - Incremental: Auto-detects and handles all scenarios (PRODUCTION DEFAULT)
   - Initial: First-time load from scratch
   - Forward: Month-by-month sequential processing  
   - Backfill: Late-arriving data with history rebuild
4. Two-table architecture: summary + latest_summary
5. Grid column generation for payment history

Scale Target: 500B+ summary records

Usage:
    # Production mode (auto-handles backfill + forward + new accounts)
    spark-submit summary_pipeline.py --config summary_config.json
    
    # Or explicitly:
    spark-submit summary_pipeline.py --config summary_config.json --mode incremental
    
    # Month-by-month processing
    spark-submit summary_pipeline.py --config summary_config.json --mode forward
    
    # Initial load
    spark-submit summary_pipeline.py --config summary_config.json --mode initial
    
    # Manual backfill
    spark-submit summary_pipeline.py --config summary_config.json --mode backfill

Author: DataDominion Team
Version: 5.0.0
"""

import json
import logging
import argparse
import sys
import time
import math
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType, ArrayType
import pyspark.sql.functions as F


# ============================================================
# LOGGING SETUP
# ============================================================

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


# ============================================================
# CONFIGURATION CLASS
# ============================================================

class SummaryConfig:
    """
    Configuration loaded from JSON file.
    
    All processing logic is defined in the config - no hardcoded values.
    """
    
    def __init__(self, config_path: str):
        """Load configuration from JSON file."""
        with open(config_path, 'r') as f:
            self._config = json.load(f)
        
        # Core tables
        self.source_table = self._config['source_table']
        self.destination_table = self._config['destination_table']
        self.latest_history_table = self._config['latest_history_table']
        
        # Key columns
        self.partition_column = self._config['partition_column']
        self.primary_column = self._config['primary_column']
        self.primary_date_column = self._config['primary_date_column']
        self.max_identifier_column = self._config['max_identifier_column']
        self.history_length = self._config.get('history_length', 36)
        
        # Column mappings
        self.columns = self._config['columns']
        self.column_transformations = self._config.get('column_transformations', [])
        self.inferred_columns = self._config.get('inferred_columns', [])
        self.coalesce_exclusion_cols = self._config.get('coalesce_exclusion_cols', [])
        self.date_col_list = self._config.get('date_col_list', [])
        self.latest_history_addon_cols = self._config.get('latest_history_addon_cols', [])
        
        # Rolling columns
        self.rolling_columns = self._config.get('rolling_columns', [])
        self.grid_columns = self._config.get('grid_columns', [])
        
        # Performance settings
        perf = self._config.get('performance', {})
        self.target_records_per_partition = perf.get('target_records_per_partition', 10_000_000)
        self.min_partitions = perf.get('min_partitions', 16)
        self.max_partitions = perf.get('max_partitions', 8192)
        self.avg_record_size_bytes = perf.get('avg_record_size_bytes', 200)
        self.snapshot_interval = perf.get('snapshot_interval', 12)
        self.max_file_size_mb = perf.get('max_file_size_mb', 256)
        
        # Spark config
        spark_cfg = self._config.get('spark_config', {})
        self.adaptive_enabled = spark_cfg.get('adaptive_enabled', True)
        self.dynamic_partition_pruning = spark_cfg.get('dynamic_partition_pruning', True)
        self.skew_join_enabled = spark_cfg.get('skew_join_enabled', True)
        self.broadcast_threshold_mb = spark_cfg.get('broadcast_threshold_mb', 50)
        
        # Backfill settings
        backfill = self._config.get('backfill_settings', {})
        self.backfill_enabled = backfill.get('enabled', True)
        self.backfill_validate_timestamp = backfill.get('validate_timestamp', True)
        self.backfill_rebuild_future_rows = backfill.get('rebuild_future_rows', True)
        
        # Derived values
        self.rolling_mapper_list = [col['mapper_column'] for col in self.rolling_columns]
        self.rolling_history_cols = [f"{col['name']}_history" for col in self.rolling_columns]
    
    def get_select_expressions(self) -> List[Column]:
        """Get column select expressions from source to destination names."""
        return [F.col(src).alias(dst) for src, dst in self.columns.items()]
    
    def get_rolling_column_by_name(self, name: str) -> Optional[Dict]:
        """Get rolling column config by name."""
        for col in self.rolling_columns:
            if col['name'] == name:
                return col
        return None


# ============================================================
# SPARK SESSION FACTORY
# ============================================================

def create_spark_session(config: SummaryConfig, app_name: str = "SummaryPipeline_v5") -> SparkSession:
    """Create production-optimized Spark session with Iceberg support."""
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # Adaptive Query Execution (Spark 3.x)
    if config.adaptive_enabled:
        builder = builder \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", str(config.skew_join_enabled).lower())
    
    # Dynamic partition pruning
    if config.dynamic_partition_pruning:
        builder = builder \
            .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
    
    # Network and timeout settings for production scale
    builder = builder \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.autoBroadcastJoinThreshold", f"{config.broadcast_threshold_mb}m") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    
    return builder.getOrCreate()


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def calculate_optimal_partitions(record_count: int, config: SummaryConfig) -> int:
    """
    Calculate optimal partition count based on data size.
    
    Uses both record count and data size to determine the best partition count,
    then rounds to nearest power of 2 for even distribution.
    """
    partitions_by_count = record_count / config.target_records_per_partition
    data_size_mb = (record_count * config.avg_record_size_bytes) / (1024 * 1024)
    partitions_by_size = data_size_mb / config.max_file_size_mb
    
    optimal = max(partitions_by_count, partitions_by_size)
    power_of_2 = 2 ** round(math.log2(max(optimal, 1)))
    return int(max(config.min_partitions, min(config.max_partitions, power_of_2)))


def configure_spark_partitions(spark: SparkSession, num_partitions: int):
    """Dynamically configure Spark shuffle partitions."""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))
    logger.info(f"Configured Spark for {num_partitions} partitions")


def replace_invalid_years(df: DataFrame, columns: List[str]) -> DataFrame:
    """Replace invalid year values (< 1000) with NULL."""
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name, 
                F.when(F.year(F.col(col_name)) < 1000, None)
                .otherwise(F.to_date(F.col(col_name)))
            )
    return df


def month_to_int(month_str: str) -> int:
    """Convert YYYY-MM string to integer for arithmetic."""
    parts = month_str.split('-')
    return int(parts[0]) * 12 + int(parts[1])


def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM column to integer."""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


# ============================================================
# ROLLING COLUMN TRANSFORMATIONS
# ============================================================

class RollingColumnBuilder:
    """
    Build rolling column transformations from config.
    
    Handles three cases:
    - Initial: [value, NULL, NULL, ...]
    - Normal shift: [new, prev[0:n-1]]
    - Gap handling: [new, NULLs, prev[0:n-gap-1]]
    """
    
    TYPE_MAP = {
        "Integer": IntegerType(),
        "String": StringType()
    }
    
    @staticmethod
    def get_spark_type(type_name: str):
        """Get Spark type from config type name."""
        return RollingColumnBuilder.TYPE_MAP.get(type_name, StringType())
    
    @staticmethod
    def initial_history_array(
        mapper_column: str, 
        data_type: str, 
        num_cols: int
    ) -> Column:
        """Create initial history array: [value, NULL, NULL, ...]."""
        spark_type = RollingColumnBuilder.get_spark_type(data_type)
        
        return F.array(
            F.col(mapper_column).cast(spark_type),
            *[F.lit(None).cast(spark_type) for _ in range(num_cols - 1)]
        )
    
    @staticmethod
    def shifted_history_array(
        rolling_col: str,
        mapper_column: str,
        data_type: str,
        num_cols: int,
        current_alias: str = "c",
        previous_alias: str = "p"
    ) -> Column:
        """
        Create shifted history array with gap handling.
        
        Logic:
        - MONTH_DIFF == 1: Normal shift [new, prev[0:n-1]]
        - MONTH_DIFF > 1:  Gap handling [new, NULLs for gap, prev[0:n-gap-1]]
        - Otherwise:       New account [new, NULL, NULL, ...]
        """
        spark_type = RollingColumnBuilder.get_spark_type(data_type)
        
        # Current value with NULL fallback
        current_value = F.coalesce(
            F.col(f"{current_alias}.{mapper_column}").cast(spark_type),
            F.lit(None).cast(spark_type)
        )
        
        # Previous history with empty array fallback
        prev_history = F.coalesce(
            F.col(f"{previous_alias}.{rolling_col}_history"),
            F.array(*[F.lit(None).cast(spark_type) for _ in range(num_cols)])
        )
        
        # Null array generator for gap filling
        null_array = lambda count: F.array_repeat(
            F.lit(None).cast(spark_type), 
            count
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
                        null_array(F.col("MONTH_DIFF") - F.lit(1)),
                        prev_history
                    ),
                    1, num_cols
                )
            )
            .otherwise(new_account_array)
            .alias(f"{rolling_col}_history")
        )


# ============================================================
# GRID COLUMN GENERATOR
# ============================================================

def generate_grid_column(
    df: DataFrame,
    grid_config: Dict,
    rolling_columns: List[Dict]
) -> DataFrame:
    """
    Generate grid column from rolling history array.
    
    Transforms array like ["0", "1", NULL, "2", ...] 
    into string like "01?2..." with placeholder for NULLs.
    """
    
    grid_name = grid_config['name']
    mapper_col = grid_config['mapper_rolling_column']
    placeholder = grid_config.get('placeholder', '?')
    separator = grid_config.get('separator', '')
    
    history_col = f"{mapper_col}_history"
    
    if history_col not in df.columns:
        logger.warning(f"History column {history_col} not found, skipping grid generation")
        return df
    
    # Transform array to string grid
    df = df.withColumn(
        grid_name,
        F.concat_ws(
            separator,
            F.transform(
                F.col(history_col),
                lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
            )
        )
    )
    
    return df


# ============================================================
# SUMMARY PIPELINE - MAIN CLASS
# ============================================================

class SummaryPipeline:
    """
    Main pipeline for summary generation.
    
    Supports four modes:
    1. incremental: Auto-detects all scenarios, processes in correct order (PRODUCTION DEFAULT)
    2. initial: First-time load from scratch
    3. forward: Month-by-month sequential processing
    4. backfill: Late-arriving data with history rebuild
    """
    
    def __init__(self, spark: SparkSession, config: SummaryConfig):
        self.spark = spark
        self.config = config
        self.start_time = time.time()
        self.optimization_counter = 0
    
    # ==========================================================
    # INCREMENTAL MODE - PRODUCTION DEFAULT
    # ==========================================================
    
    def run_incremental(self):
        """
        Run incremental processing for production.
        
        This mode automatically handles all scenarios in a single run:
        - Case I:   New accounts (first appearance)
        - Case II:  Forward entries (month > max existing)
        - Case III: Backfill (month <= max existing, newer timestamp)
        
        Processing Order (CRITICAL):
        1. Backfill FIRST (rebuilds history for affected months)
        2. New accounts SECOND (no dependencies)
        3. Forward LAST (uses corrected history from backfill)
        
        Usage:
            spark-submit summary_pipeline.py --config config.json
            spark-submit summary_pipeline.py --config config.json --mode incremental
        """
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE v5.0 - INCREMENTAL MODE (PRODUCTION)")
        logger.info("=" * 80)
        
        # Step 1: Get latest timestamp from summary table
        max_ts = self._get_max_summary_timestamp()
        
        if max_ts is None:
            logger.warning("No existing summary found. Use 'initial' mode for first load.")
            logger.warning("Example: --mode initial --start-month 2016-01 --end-month 2025-12")
            return
        
        logger.info(f"Latest timestamp in summary: {max_ts}")
        logger.info(f"Processing all new data since: {max_ts}")
        
        # Step 2: Read all new data (base_ts > max existing timestamp)
        logger.info("Reading new data from source...")
        
        batch_df = self.spark.sql(f"""
            SELECT * FROM {self.config.source_table}
            WHERE {self.config.max_identifier_column} > TIMESTAMP '{max_ts}'
        """)
        
        # Check if we have any new data
        record_count = batch_df.count()
        if record_count == 0:
            logger.info("No new records to process. Summary is up to date.")
            return
        
        logger.info(f"New records found: {record_count:,}")
        
        # Step 3: Prepare and classify ALL records
        logger.info("Preparing and classifying records...")
        
        # Apply column mappings, transformations, deduplication
        batch_df = self._prepare_source_data(batch_df)
        
        # Classify into Case I, II, III
        classified = self._classify_backfill_records(batch_df)
        classified.persist()  # Cache for multiple filters
        
        # Count each case type
        logger.info("Classifying records into cases...")
        case_counts = classified.groupBy("case_type").count().collect()
        case_dict = {row["case_type"]: row["count"] for row in case_counts}
        
        case_i_count = case_dict.get('CASE_I', 0)
        case_ii_count = case_dict.get('CASE_II', 0)
        case_iii_count = case_dict.get('CASE_III', 0)
        
        logger.info("-" * 80)
        logger.info("CLASSIFICATION RESULTS:")
        logger.info(f"  Case I   (New accounts):    {case_i_count:>10,} records")
        logger.info(f"  Case II  (Forward entries): {case_ii_count:>10,} records")
        logger.info(f"  Case III (Backfill):        {case_iii_count:>10,} records")
        logger.info(f"  TOTAL:                      {record_count:>10,} records")
        logger.info("-" * 80)
        
        # Step 4: Process in CORRECT order
        # CRITICAL: Order matters!
        # - Backfill FIRST: Rebuilds history arrays with corrections
        # - New accounts SECOND: No dependencies, can go anytime
        # - Forward LAST: Must use corrected history from backfill
        
        # 4a. Process Case III: Backfill (HIGHEST PRIORITY)
        case_iii = classified.filter(F.col("case_type") == "CASE_III")
        if case_iii.limit(1).count() > 0:
            logger.info("=" * 80)
            logger.info(f"STEP 1/3: Processing {case_iii_count:,} backfill records (Case III)")
            logger.info("=" * 80)
            
            step_start = time.time()
            
            # Get affected stats for logging
            affected_months = case_iii.select(self.config.partition_column).distinct().count()
            affected_accounts = case_iii.select(self.config.primary_column).distinct().count()
            
            logger.info(f"Affected: {affected_accounts:,} accounts, {affected_months} unique months")
            logger.info("Rebuilding history arrays with corrected values...")
            
            self._process_case_iii(case_iii)
            
            elapsed = (time.time() - step_start) / 60
            logger.info(f"Backfill processing completed in {elapsed:.2f} minutes")
            logger.info("-" * 80)
        else:
            logger.info("No backfill records to process (Case III)")
        
        # 4b. Process Case I: New Accounts (MEDIUM PRIORITY)
        case_i = classified.filter(F.col("case_type") == "CASE_I")
        if case_i.limit(1).count() > 0:
            logger.info("=" * 80)
            logger.info(f"STEP 2/3: Processing {case_i_count:,} new accounts (Case I)")
            logger.info("=" * 80)
            
            step_start = time.time()
            
            logger.info("Creating initial history arrays for new accounts...")
            
            self._process_case_i(case_i)
            
            elapsed = (time.time() - step_start) / 60
            logger.info(f"New account processing completed in {elapsed:.2f} minutes")
            logger.info("-" * 80)
        else:
            logger.info("No new accounts to process (Case I)")
        
        # 4c. Process Case II: Forward Entries (LOWEST PRIORITY - uses corrected history)
        case_ii = classified.filter(F.col("case_type") == "CASE_II")
        if case_ii.limit(1).count() > 0:
            logger.info("=" * 80)
            logger.info(f"STEP 3/3: Processing {case_ii_count:,} forward entries (Case II)")
            logger.info("=" * 80)
            
            step_start = time.time()
            
            # Get unique months for logging
            forward_months = case_ii.select(self.config.partition_column).distinct() \
                .orderBy(self.config.partition_column).collect()
            month_list = [row[self.config.partition_column] for row in forward_months]
            
            logger.info(f"Processing months: {', '.join(month_list)}")
            logger.info("Shifting history arrays with new data (using corrected history from backfill)...")
            
            self._process_case_ii(case_ii)
            
            elapsed = (time.time() - step_start) / 60
            logger.info(f"Forward processing completed in {elapsed:.2f} minutes")
            logger.info("-" * 80)
        else:
            logger.info("No forward entries to process (Case II)")
        
        # Cleanup cached classified DataFrame
        classified.unpersist()
        
        # Step 5: Final optimization and summary
        logger.info("=" * 80)
        logger.info("FINALIZING")
        logger.info("=" * 80)
        
        logger.info("Optimizing tables (compacting files, expiring snapshots)...")
        self._optimize_tables()
        
        # Get final statistics
        logger.info("Gathering final statistics...")
        final_stats = self.spark.sql(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT {self.config.primary_column}) as unique_accounts,
                MAX({self.config.partition_column}) as latest_month,
                MAX({self.config.max_identifier_column}) as latest_timestamp
            FROM {self.config.destination_table}
        """).first()
        
        logger.info("=" * 80)
        logger.info("INCREMENTAL PROCESSING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Total records in summary:    {final_stats['total_records']:,}")
        logger.info(f"Unique accounts:             {final_stats['unique_accounts']:,}")
        logger.info(f"Latest month:                {final_stats['latest_month']}")
        logger.info(f"Latest timestamp:            {final_stats['latest_timestamp']}")
        
        total_time = (time.time() - self.start_time) / 60
        logger.info(f"Total processing time:       {total_time:.2f} minutes")
        logger.info("=" * 80)
    
    # ==========================================================
    # FORWARD MODE - Month-by-Month Processing
    # ==========================================================
    
    def run_forward(
        self, 
        start_month: date, 
        end_month: date,
        is_initial: bool = False
    ):
        """
        Run forward (month-by-month) processing.
        
        This processes months sequentially, building history arrays
        by shifting forward each month.
        
        Use for:
        - Initial load: --mode initial --start-month 2016-01 --end-month 2025-12
        - Sequential processing: --mode forward --start-month 2026-01 --end-month 2026-03
        """
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE v5.0 - FORWARD MODE")
        logger.info(f"Processing: {start_month} to {end_month}")
        logger.info("=" * 80)
        
        month = start_month
        while month <= end_month:
            month_start = time.time()
            
            # Optimize tables periodically
            if self.optimization_counter >= self.config.snapshot_interval:
                self._optimize_tables()
                self.optimization_counter = 0
            
            # Check/sync with existing data
            is_initial = self._check_initial_state(is_initial, month)
            
            # Process month
            success = self._process_single_month(month, is_initial)
            
            if success:
                is_initial = False
                self.optimization_counter += 1
            
            # Log timing
            elapsed = (time.time() - month_start) / 60
            logger.info(f"Month {month.strftime('%Y-%m')} completed in {elapsed:.2f} minutes")
            logger.info("-" * 80)
            
            # Next month (safe increment)
            month = (month.replace(day=28) + relativedelta(days=4)).replace(day=1)
        
        # Final optimization
        self._optimize_tables()
        
        total_time = (time.time() - self.start_time) / 60
        logger.info(f"Pipeline completed in {total_time:.2f} minutes")
    
    # ==========================================================
    # BACKFILL MODE - Manual Late-Arriving Data
    # ==========================================================
    
    def run_backfill(
        self,
        batch_df: Optional[DataFrame] = None,
        source_filter: Optional[str] = None
    ):
        """
        Run backfill processing for late-arriving data.
        
        This handles Case III - data arriving for historical months
        that requires rebuilding history arrays.
        
        Args:
            batch_df: Optional DataFrame with backfill data
            source_filter: Optional SQL filter for source table
        
        Use for manual corrections:
            --mode backfill --backfill-filter "cons_acct_key = 123 AND rpt_as_of_mo = '2025-06'"
        """
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE v5.0 - BACKFILL MODE")
        logger.info("=" * 80)
        
        # Get backfill batch
        if batch_df is None:
            if source_filter:
                batch_df = self.spark.sql(f"""
                    SELECT * FROM {self.config.source_table}
                    WHERE {source_filter}
                """)
            else:
                # Get records newer than latest summary
                max_ts = self._get_max_summary_timestamp()
                if max_ts:
                    batch_df = self.spark.sql(f"""
                        SELECT * FROM {self.config.source_table}
                        WHERE {self.config.max_identifier_column} > TIMESTAMP '{max_ts}'
                    """)
                else:
                    logger.warning("No existing summary found. Use forward mode for initial load.")
                    return
        
        # Prepare batch with column mappings
        batch_df = self._prepare_source_data(batch_df)
        
        # Classify records
        classified = self._classify_backfill_records(batch_df)
        
        # Process all cases (backfill mode processes in natural order)
        case_i = classified.filter(F.col("case_type") == "CASE_I")
        if case_i.limit(1).count() > 0:
            self._process_case_i(case_i)
        
        case_ii = classified.filter(F.col("case_type") == "CASE_II")
        if case_ii.limit(1).count() > 0:
            self._process_case_ii(case_ii)
        
        case_iii = classified.filter(F.col("case_type") == "CASE_III")
        if case_iii.limit(1).count() > 0:
            self._process_case_iii(case_iii)
        
        total_time = (time.time() - self.start_time) / 60
        logger.info(f"Backfill completed in {total_time:.2f} minutes")
    
    # ==========================================================
    # FORWARD MODE HELPERS
    # ==========================================================
    
    def _check_initial_state(self, is_initial: bool, current_month: date) -> bool:
        """Check if this should be initial load or continuation."""
        month_str = current_month.strftime('%Y-%m')
        
        if self.spark.catalog.tableExists(self.config.destination_table):
            max_month = self.spark.table(self.config.destination_table) \
                .agg(F.max(self.config.partition_column).alias("max_month")) \
                .first()["max_month"]
            
            if max_month is not None:
                is_initial = False
                if max_month > month_str:
                    raise ValueError(f"Processing month {month_str} < max existing {max_month}. Use backfill mode.")
        
        return is_initial
    
    def _process_single_month(self, month: date, is_initial: bool) -> bool:
        """Process a single month's data."""
        month_str = month.strftime('%Y-%m')
        logger.info(f"Processing month: {month_str}")
        
        # Read source data for this month
        source_df = self.spark.read.table(self.config.source_table) \
            .select(*self.config.get_select_expressions()) \
            .filter(F.col(self.config.partition_column) == month_str)
        
        # Deduplicate by primary key, keeping latest
        window_spec = Window.partitionBy(self.config.primary_column) \
            .orderBy(F.col(self.config.max_identifier_column).desc())
        
        source_df = source_df \
            .withColumn("_rn", F.row_number().over(window_spec)) \
            .filter(F.col("_rn") == 1) \
            .drop("_rn")
        
        # Apply column transformations
        for transform in self.config.column_transformations:
            source_df = source_df.withColumn(
                transform['name'],
                F.expr(transform['mapper_expr'])
            )
        
        # Apply inferred columns
        for inferred in self.config.inferred_columns:
            source_df = source_df.withColumn(
                inferred['name'],
                F.expr(inferred['mapper_expr'])
            )
        
        # Check record count
        record_count = source_df.count()
        if record_count == 0:
            logger.info(f"No records for {month_str}")
            return False
        
        logger.info(f"Records to process: {record_count:,}")
        
        # Configure partitioning
        num_partitions = calculate_optimal_partitions(record_count, self.config)
        configure_spark_partitions(self.spark, num_partitions)
        
        source_df = source_df.repartition(num_partitions, F.col(self.config.primary_column))
        source_df.cache()
        
        # Build result
        if is_initial:
            result = self._build_initial_summary(source_df)
        else:
            result = self._build_incremental_summary(source_df, month_str)
        
        # Generate grid columns
        for grid_config in self.config.grid_columns:
            result = generate_grid_column(result, grid_config, self.config.rolling_columns)
        
        # Clean date columns
        result = replace_invalid_years(result, self.config.date_col_list)
        result.cache()
        
        # Write results
        self._write_results(result, is_initial)
        
        # Cleanup
        source_df.unpersist()
        result.unpersist()
        
        return True
    
    def _build_initial_summary(self, source_df: DataFrame) -> DataFrame:
        """Build initial summary with fresh history arrays."""
        logger.info("Building initial summary...")
        
        result = source_df
        
        # Apply rolling column mapper expressions
        for rolling_col in self.config.rolling_columns:
            result = result.withColumn(
                rolling_col['mapper_column'],
                F.expr(rolling_col['mapper_expr'])
            )
        
        # Create history arrays
        for rolling_col in self.config.rolling_columns:
            result = result.withColumn(
                f"{rolling_col['name']}_history",
                RollingColumnBuilder.initial_history_array(
                    rolling_col['mapper_column'],
                    rolling_col['type'],
                    rolling_col['num_cols']
                )
            )
        
        return result
    
    def _build_incremental_summary(self, source_df: DataFrame, month_str: str) -> DataFrame:
        """Build incremental summary by shifting history arrays."""
        logger.info("Building incremental summary...")
        
        # Get previous state from latest_summary
        latest_prev = self.spark.read.table(self.config.latest_history_table).select(
            self.config.primary_column,
            self.config.primary_date_column,
            self.config.partition_column,
            self.config.max_identifier_column,
            *self.config.rolling_history_cols
        )
        
        prev_count = latest_prev.count()
        logger.info(f"Previous records: {prev_count:,}")
        
        num_partitions = calculate_optimal_partitions(prev_count, self.config)
        configure_spark_partitions(self.spark, num_partitions)
        
        latest_prev = latest_prev.repartition(num_partitions, self.config.primary_column)
        
        # Apply mapper expressions to source
        for rolling_col in self.config.rolling_columns:
            source_df = source_df.withColumn(
                rolling_col['mapper_column'],
                F.expr(rolling_col['mapper_expr'])
            )
        
        # Join current with previous
        joined = source_df.alias("c").join(
            latest_prev.alias("p"),
            self.config.primary_column,
            "left"
        )
        
        # Calculate month difference
        joined = joined.withColumn(
            "MONTH_DIFF",
            F.when(
                F.col(f"p.{self.config.partition_column}").isNotNull(),
                F.months_between(
                    F.to_date(F.lit(month_str), "yyyy-MM"),
                    F.to_date(F.col(f"p.{self.config.partition_column}"), "yyyy-MM")
                ).cast(IntegerType())
            ).otherwise(1)
        )
        
        # Build select expressions
        select_exprs = []
        
        # Non-rolling columns (from current)
        for src, dst in self.config.columns.items():
            if dst not in self.config.coalesce_exclusion_cols and dst not in self.config.rolling_mapper_list:
                select_exprs.append(F.col(f"c.{dst}").alias(dst))
        
        # Inferred columns
        for inferred in self.config.inferred_columns:
            select_exprs.append(F.col(f"c.{inferred['name']}").alias(inferred['name']))
        
        # Rolling columns (mapper values + shifted histories)
        for rolling_col in self.config.rolling_columns:
            mapper_col = rolling_col['mapper_column']
            data_type = rolling_col['type']
            
            # Mapper column value
            if data_type == "Integer":
                select_exprs.append(
                    F.coalesce(
                        F.col(f"c.{mapper_col}"),
                        F.lit(None).cast(IntegerType())
                    ).alias(mapper_col)
                )
            else:
                select_exprs.append(
                    F.coalesce(
                        F.col(f"c.{mapper_col}"),
                        F.lit(None).cast(StringType())
                    ).alias(mapper_col)
                )
            
            # Shifted history array
            select_exprs.append(
                RollingColumnBuilder.shifted_history_array(
                    rolling_col['name'],
                    mapper_col,
                    data_type,
                    rolling_col['num_cols']
                )
            )
        
        # Date columns with coalesce
        select_exprs.append(
            F.coalesce(
                F.col(f"c.{self.config.primary_date_column}"),
                F.to_date(F.lit(month_str))
            ).alias(self.config.primary_date_column)
        )
        select_exprs.append(
            F.coalesce(
                F.col(f"c.{self.config.partition_column}"),
                F.lit(month_str)
            ).alias(self.config.partition_column)
        )
        
        result = joined.select(*select_exprs)
        
        return result
    
    def _write_results(self, result: DataFrame, is_initial: bool):
        """Write results to summary and latest_summary tables."""
        
        if result.count() == 0:
            logger.info("No records to write")
            return
        
        # Get target table columns to match schema
        target_cols = [f.name for f in self.spark.table(self.config.destination_table).schema.fields]
        
        # Select only columns that exist in both DataFrame and target table
        select_cols = [c for c in target_cols if c in result.columns]
        result_matched = result.select(*select_cols)
        
        if is_initial:
            # Initial append
            logger.info("Writing initial data to summary...")
            result_matched.writeTo(self.config.destination_table).append()
            
            # Write to latest_summary
            latest_target_cols = [f.name for f in self.spark.table(self.config.latest_history_table).schema.fields]
            latest_select = [c for c in latest_target_cols if c in result.columns]
            
            result.select(*latest_select) \
                .writeTo(self.config.latest_history_table).append()
        else:
            # MERGE for updates
            logger.info("Merging to summary...")
            result_matched.createOrReplaceTempView("result")
            
            self.spark.sql(f"""
                MERGE INTO {self.config.destination_table} d
                USING result r
                ON d.{self.config.primary_column} = r.{self.config.primary_column} 
                   AND d.{self.config.partition_column} = r.{self.config.partition_column}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            
            # MERGE to latest_summary
            logger.info("Merging to latest_summary...")
            latest_target_cols = [f.name for f in self.spark.table(self.config.latest_history_table).schema.fields]
            latest_select_cols = [c for c in latest_target_cols if c in result.columns]
            
            result.select(*latest_select_cols) \
                .createOrReplaceTempView("result_latest")
            
            self.spark.sql(f"""
                MERGE INTO {self.config.latest_history_table} d
                USING result_latest r
                ON d.{self.config.primary_column} = r.{self.config.primary_column}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
    
    # ==========================================================
    # BACKFILL/INCREMENTAL MODE HELPERS
    # ==========================================================
    
    def _get_max_summary_timestamp(self):
        """Get max base_ts from summary table."""
        try:
            return self.spark.sql(f"""
                SELECT MAX({self.config.max_identifier_column}) as max_ts 
                FROM {self.config.destination_table}
            """).collect()[0]["max_ts"]
        except Exception:
            return None
    
    def _prepare_source_data(self, df: DataFrame) -> DataFrame:
        """Apply column mappings and transformations to source data."""
        
        # Select with column mapping
        result = df.select(*self.config.get_select_expressions())
        
        # Deduplicate by primary key + partition, keeping latest
        window_spec = Window.partitionBy(
            self.config.primary_column, 
            self.config.partition_column
        ).orderBy(F.col(self.config.max_identifier_column).desc())
        
        result = result \
            .withColumn("_rn", F.row_number().over(window_spec)) \
            .filter(F.col("_rn") == 1) \
            .drop("_rn")
        
        # Apply transformations
        for transform in self.config.column_transformations:
            result = result.withColumn(
                transform['name'],
                F.expr(transform['mapper_expr'])
            )
        
        for inferred in self.config.inferred_columns:
            result = result.withColumn(
                inferred['name'],
                F.expr(inferred['mapper_expr'])
            )
        
        # Apply rolling column mappers (needed for all cases)
        for rolling_col in self.config.rolling_columns:
            result = result.withColumn(
                rolling_col['mapper_column'],
                F.expr(rolling_col['mapper_expr'])
            )
        
        # Add month integer for classification
        result = result.withColumn(
            "month_int",
            F.expr(month_to_int_expr(self.config.partition_column))
        )
        
        return result
    
    def _classify_backfill_records(self, batch_df: DataFrame) -> DataFrame:
        """
        Classify records into Case I, II, III.
        
        Case I:   New account (no existing data)
        Case II:  Forward entry (month > max existing month)
        Case III: Backfill (month <= max existing month, newer timestamp)
        """
        logger.info("Classifying records...")
        
        # Get summary metadata per account
        summary_meta = self.spark.sql(f"""
            SELECT 
                {self.config.primary_column},
                MAX({self.config.partition_column}) as max_existing_month,
                MAX({self.config.max_identifier_column}) as max_existing_ts,
                {month_to_int_expr(f"MAX({self.config.partition_column})")} as max_month_int
            FROM {self.config.destination_table}
            GROUP BY {self.config.primary_column}
        """)
        
        # Join and classify
        classified = batch_df.alias("n").join(
            F.broadcast(summary_meta.alias("s")),
            F.col(f"n.{self.config.primary_column}") == F.col(f"s.{self.config.primary_column}"),
            "left"
        ).select(
            F.col("n.*"),
            F.col("s.max_existing_month"),
            F.col("s.max_existing_ts"),
            F.col("s.max_month_int")
        )
        
        classified = classified.withColumn(
            "case_type",
            F.when(F.col("max_existing_month").isNull(), F.lit("CASE_I"))
            .when(F.col("month_int") > F.col("max_month_int"), F.lit("CASE_II"))
            .otherwise(F.lit("CASE_III"))
        ).withColumn(
            "month_gap",
            F.when(
                F.col("case_type") == "CASE_II",
                F.col("month_int") - F.col("max_month_int") - 1
            ).otherwise(F.lit(0))
        )
        
        return classified.persist()
    
    def _process_case_i(self, case_i_df: DataFrame):
        """Process new accounts (Case I) - Create initial arrays."""
        logger.info("Processing Case I (new accounts)...")
        
        result = case_i_df
        
        # Create initial history arrays
        for rolling_col in self.config.rolling_columns:
            result = result.withColumn(
                f"{rolling_col['name']}_history",
                RollingColumnBuilder.initial_history_array(
                    rolling_col['mapper_column'],
                    rolling_col['type'],
                    rolling_col['num_cols']
                )
            )
        
        # Generate grid columns
        for grid_config in self.config.grid_columns:
            result = generate_grid_column(result, grid_config, self.config.rolling_columns)
        
        # Drop classification columns
        result = result.drop("month_int", "max_existing_month", "max_existing_ts", 
                            "max_month_int", "case_type", "month_gap")
        
        # Write
        result = replace_invalid_years(result, self.config.date_col_list)
        self._write_backfill_results(result)
    
    def _process_case_ii(self, case_ii_df: DataFrame):
        """Process forward entries (Case II) - Shift existing history."""
        logger.info("Processing Case II (forward entries)...")
        
        # Get latest summary for affected accounts
        affected_keys = case_ii_df.select(self.config.primary_column).distinct()
        
        latest_summary = self.spark.read.table(self.config.latest_history_table).alias("ls")
        latest_for_affected = latest_summary.join(
            affected_keys.alias("ak"),
            F.col(f"ls.{self.config.primary_column}") == F.col(f"ak.{self.config.primary_column}"),
            "inner"
        ).select("ls.*")
        
        result = case_ii_df
        
        # Rename month_gap to MONTH_DIFF for RollingColumnBuilder
        result = result.withColumnRenamed("month_gap", "MONTH_DIFF") \
            .withColumn("MONTH_DIFF", F.col("MONTH_DIFF") + 1)  # gap + 1 = diff
        
        # Join with latest summary
        joined = result.alias("c").join(
            latest_for_affected.alias("p"),
            F.col(f"c.{self.config.primary_column}") == F.col(f"p.{self.config.primary_column}"),
            "inner"
        )
        
        # Build shifted history arrays
        select_exprs = [F.col(f"c.{c}") for c in case_ii_df.columns 
                       if c not in ["month_int", "max_existing_month", "max_existing_ts", 
                                   "max_month_int", "case_type", "MONTH_DIFF"]]
        
        for rolling_col in self.config.rolling_columns:
            select_exprs.append(
                RollingColumnBuilder.shifted_history_array(
                    rolling_col['name'],
                    rolling_col['mapper_column'],
                    rolling_col['type'],
                    rolling_col['num_cols']
                )
            )
        
        result = joined.select(*select_exprs)
        
        # Generate grid columns
        for grid_config in self.config.grid_columns:
            result = generate_grid_column(result, grid_config, self.config.rolling_columns)
        
        result = replace_invalid_years(result, self.config.date_col_list)
        self._write_backfill_results(result)
    
    def _process_case_iii(self, case_iii_df: DataFrame):
        """
        Process backfill entries (Case III) - Rebuild entire history.
        
        This rebuilds history arrays for ALL affected months of affected accounts
        using the corrected values from the backfill.
        """
        logger.info("Processing Case III (backfill with history rebuild)...")
        
        hl = self.config.history_length
        primary_col = self.config.primary_column
        partition_col = self.config.partition_column
        max_id_col = self.config.max_identifier_column
        
        # Get affected accounts
        affected_accounts = case_iii_df.select(primary_col).distinct()
        case_iii_df.createOrReplaceTempView("backfill_records")
        affected_accounts.createOrReplaceTempView("affected_accounts")
        
        # Identify columns to preserve
        exclude_cols = [primary_col, partition_col, max_id_col, "month_int", 
                       "case_type", "month_gap", "max_existing_month", 
                       "max_existing_ts", "max_month_int"]
        preserve_cols = [c for c in case_iii_df.columns if c not in exclude_cols]
        
        # Build rolling column SQL parts for history array rebuild
        rolling_col_history = []
        for rolling_col in self.config.rolling_columns:
            name = rolling_col['name']
            mapper = rolling_col['mapper_column']
            dtype = rolling_col['type']
            sql_type = "INT" if dtype == "Integer" else "STRING"
            
            rolling_col_history.append(f"""
                TRANSFORM(
                    SEQUENCE(0, {hl - 1}),
                    offset -> (
                        AGGREGATE(
                            FILTER(e.month_values, mv -> mv.month_int = e.month_int - offset),
                            CAST(NULL AS {sql_type}),
                            (acc, x) -> x.{mapper}
                        )
                    )
                ) as {name}_history
            """)
        
        # Build scalar extraction SQL
        scalar_extract_simple = []
        for col in preserve_cols:
            scalar_extract_simple.append(
                f"element_at(FILTER(e.month_values, mv -> mv.month_int = e.month_int), 1).{col} as {col}"
            )
        
        struct_cols = ", ".join(preserve_cols)
        
        # Build existing data column selection
        existing_cols = []
        rolling_mappers = [rc['mapper_column'] for rc in self.config.rolling_columns]
        
        for col in preserve_cols:
            if col in rolling_mappers:
                rc_name = next(rc['name'] for rc in self.config.rolling_columns if rc['mapper_column'] == col)
                existing_cols.append(f"s.{rc_name}_history[0] as {col}")
            else:
                existing_cols.append(f"s.{col}")
        
        # Build combined SQL query with 7 CTEs
        combined_sql = f"""
        WITH 
        -- CTE 1: Validate backfill records (only keep newer timestamps)
        backfill_validated AS (
            SELECT b.*
            FROM backfill_records b
            LEFT JOIN (
                SELECT {primary_col}, {partition_col}, {max_id_col}
                FROM {self.config.destination_table}
                WHERE {primary_col} IN (SELECT {primary_col} FROM affected_accounts)
            ) e ON b.{primary_col} = e.{primary_col} 
                AND b.{partition_col} = e.{partition_col}
            WHERE e.{max_id_col} IS NULL OR b.{max_id_col} > e.{max_id_col}
        ),
        
        -- CTE 2: Get all existing data for affected accounts
        existing_data AS (
            SELECT 
                s.{primary_col},
                s.{partition_col},
                {month_to_int_expr(f"s.{partition_col}")} as month_int,
                {', '.join(existing_cols)},
                s.{max_id_col},
                2 as priority
            FROM {self.config.destination_table} s
            WHERE s.{primary_col} IN (SELECT {primary_col} FROM affected_accounts)
        ),
        
        -- CTE 3: Combine backfill + existing (backfill has higher priority)
        combined_data AS (
            SELECT 
                {primary_col}, {partition_col}, month_int,
                {struct_cols},
                {max_id_col},
                1 as priority
            FROM backfill_validated
            
            UNION ALL
            
            SELECT * FROM existing_data
        ),
        
        -- CTE 4: Deduplicate (keep backfill if exists, else existing)
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {primary_col}, {partition_col}
                    ORDER BY priority, {max_id_col} DESC
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
                {primary_col},
                COLLECT_LIST(STRUCT(month_int, {struct_cols})) as month_values,
                COLLECT_LIST(STRUCT({partition_col}, month_int, {max_id_col})) as month_metadata
            FROM final_data
            GROUP BY {primary_col}
        ),
        
        -- CTE 7: Explode back to one row per month (but with full history available)
        exploded AS (
            SELECT 
                {primary_col},
                month_values,
                meta.{partition_col},
                meta.month_int,
                meta.{max_id_col}
            FROM account_months
            LATERAL VIEW EXPLODE(month_metadata) t AS meta
        )
        
        -- Final: Rebuild history arrays for each month
        SELECT
            e.{primary_col},
            e.{partition_col},
            {', '.join(rolling_col_history)},
            {', '.join(scalar_extract_simple)},
            e.{max_id_col}
        FROM exploded e
        """
        
        result = self.spark.sql(combined_sql)
        
        # Generate grid columns
        for grid_config in self.config.grid_columns:
            result = generate_grid_column(result, grid_config, self.config.rolling_columns)
        
        self._write_backfill_results(result)
    
    def _write_backfill_results(self, result: DataFrame):
        """Write backfill/incremental results via MERGE."""
        result.createOrReplaceTempView("backfill_result")
        
        # MERGE to summary (upsert by account + month)
        self.spark.sql(f"""
            MERGE INTO {self.config.destination_table} t
            USING backfill_result s
            ON t.{self.config.primary_column} = s.{self.config.primary_column}
               AND t.{self.config.partition_column} = s.{self.config.partition_column}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # MERGE to latest_summary (only for latest month per account)
        latest_per_account = self.spark.sql(f"""
            SELECT * FROM (
                SELECT *, 
                    ROW_NUMBER() OVER (
                        PARTITION BY {self.config.primary_column} 
                        ORDER BY {self.config.partition_column} DESC
                    ) as rn
                FROM backfill_result
            ) WHERE rn = 1
        """).drop("rn")
        
        # Get target columns from latest_summary
        latest_target_cols = [f.name for f in self.spark.table(self.config.latest_history_table).schema.fields]
        latest_select = [c for c in latest_target_cols if c in latest_per_account.columns]
        
        latest_per_account.select(*latest_select) \
            .createOrReplaceTempView("backfill_latest")
        
        self.spark.sql(f"""
            MERGE INTO {self.config.latest_history_table} t
            USING backfill_latest s
            ON t.{self.config.primary_column} = s.{self.config.primary_column}
            WHEN MATCHED AND s.{self.config.partition_column} >= t.{self.config.partition_column} 
                THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    # ==========================================================
    # TABLE OPTIMIZATION
    # ==========================================================
    
    def _optimize_tables(self):
        """Optimize Iceberg tables - compact files and expire snapshots."""
        logger.info("Optimizing tables...")
        
        for table in [self.config.latest_history_table]:
            try:
                parts = table.split('.')
                if len(parts) >= 2:
                    catalog = parts[0] if len(parts) == 3 else "spark_catalog"
                    db = parts[-2]
                    tbl = parts[-1]
                    
                    logger.info(f"Rewriting data files for {table}...")
                    self.spark.sql(f"CALL {catalog}.system.rewrite_data_files('{db}.{tbl}')")
                    
                    logger.info(f"Expiring snapshots for {table}...")
                    self.spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{db}.{tbl}', retain_last => 1)")
            except Exception as e:
                logger.warning(f"Could not optimize {table}: {e}")


# ============================================================
# CLI ENTRY POINT
# ============================================================

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Summary Pipeline v5.0 - Unified Production Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Production run (auto-handles all scenarios)
  spark-submit summary_pipeline.py --config config.json
  
  # Initial load from historical data
  spark-submit summary_pipeline.py --config config.json --mode initial --start-month 2016-01 --end-month 2025-12
  
  # Process specific month range
  spark-submit summary_pipeline.py --config config.json --mode forward --start-month 2026-01 --end-month 2026-03
  
  # Manual backfill with filter
  spark-submit summary_pipeline.py --config config.json --mode backfill --backfill-filter "cons_acct_key = 123"
        """
    )
    
    parser.add_argument('--config', type=str, required=True,
                       help='Path to config JSON file')
    parser.add_argument('--mode', type=str, 
                       choices=['incremental', 'initial', 'forward', 'backfill'],
                       default='incremental',
                       help='Processing mode (default: incremental)')
    parser.add_argument('--start-month', type=str,
                       help='Start month (YYYY-MM) for forward/initial mode')
    parser.add_argument('--end-month', type=str,
                       help='End month (YYYY-MM) for forward/initial mode')
    parser.add_argument('--backfill-filter', type=str,
                       help='SQL filter for backfill source data')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without writing (not yet implemented)')
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    # Log startup
    logger.info("=" * 80)
    logger.info("Summary Pipeline v5.0")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Config: {args.config}")
    logger.info("=" * 80)
    
    # Load config
    config = SummaryConfig(args.config)
    
    # Create Spark session
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("INFO")
    
    try:
        pipeline = SummaryPipeline(spark, config)
        
        if args.mode == 'incremental':
            # Production default: auto-handles all scenarios
            pipeline.run_incremental()
            
        elif args.mode == 'forward' or args.mode == 'initial':
            # Parse dates
            if args.start_month:
                start_month = datetime.strptime(args.start_month, '%Y-%m').date().replace(day=1)
            else:
                start_month = date.today().replace(day=1)
            
            if args.end_month:
                end_month = datetime.strptime(args.end_month, '%Y-%m').date().replace(day=1)
            else:
                end_month = start_month
            
            is_initial = args.mode == 'initial'
            pipeline.run_forward(start_month, end_month, is_initial)
            
        elif args.mode == 'backfill':
            pipeline.run_backfill(source_filter=args.backfill_filter)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
