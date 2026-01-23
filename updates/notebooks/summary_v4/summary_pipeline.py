#!/usr/bin/env python3
"""
Summary Pipeline v4.0 - Config-Driven Production Scale
=======================================================

A production-grade, config-driven pipeline for maintaining account summary 
tables with 36-month rolling history arrays.

Key Features:
1. Config-driven - all columns, transformations defined in JSON
2. Pure Spark SQL + DataFrame API - no UDFs for scale
3. Three processing modes:
   - Initial: First-time load
   - Forward: Month-by-month sequential processing  
   - Backfill: Late-arriving data with history rebuild
4. Two-table architecture: summary + latest_summary
5. Grid column generation for payment history

Scale Target: 500B+ summary records

Usage:
    # Normal month-by-month processing
    spark-submit summary_pipeline.py --config summary_config.json --mode forward
    
    # Backfill late-arriving data  
    spark-submit summary_pipeline.py --config summary_config.json --mode backfill
    
    # Initial load
    spark-submit summary_pipeline.py --config summary_config.json --mode initial

Author: DataDominion Team
Version: 4.0.0
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
# Logging Setup
# ============================================================

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False

# ============================================================
# Configuration Classes
# ============================================================

class SummaryConfig:
    """Configuration loaded from JSON file."""
    
    def __init__(self, config_path: str):
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
# Spark Session Factory
# ============================================================

def create_spark_session(config: SummaryConfig, app_name: str = "SummaryPipeline_v4") -> SparkSession:
    """Create production-optimized Spark session."""
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # Adaptive Query Execution
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
    
    # Network and timeout settings
    builder = builder \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.autoBroadcastJoinThreshold", f"{config.broadcast_threshold_mb}m") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    
    return builder.getOrCreate()


# ============================================================
# Utility Functions
# ============================================================

def calculate_optimal_partitions(record_count: int, config: SummaryConfig) -> int:
    """Calculate optimal partition count based on data size."""
    partitions_by_count = record_count / config.target_records_per_partition
    data_size_mb = (record_count * config.avg_record_size_bytes) / (1024 * 1024)
    partitions_by_size = data_size_mb / config.max_file_size_mb
    
    optimal = max(partitions_by_count, partitions_by_size)
    power_of_2 = 2 ** round(math.log2(max(optimal, 1)))
    return int(max(config.min_partitions, min(config.max_partitions, power_of_2)))


def configure_spark_partitions(spark: SparkSession, num_partitions: int):
    """Dynamically configure Spark for partition count."""
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
    """Convert YYYY-MM string to integer (for sorting/math)."""
    parts = month_str.split('-')
    return int(parts[0]) * 12 + int(parts[1])


def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM column to integer."""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


# ============================================================
# Rolling Column Transformations
# ============================================================

class RollingColumnBuilder:
    """Build rolling column transformations from config."""
    
    TYPE_MAP = {
        "Integer": IntegerType(),
        "String": StringType()
    }
    
    @staticmethod
    def get_spark_type(type_name: str):
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
        Create shifted history array handling gaps.
        
        Logic:
        - MONTH_DIFF == 1: Normal shift [new, prev[0:n-1]]
        - MONTH_DIFF > 1:  Gap handling [new, NULLs for gap, prev[0:n-gap-1]]
        - Otherwise:       New account [new, NULL, NULL, ...]
        """
        spark_type = RollingColumnBuilder.get_spark_type(data_type)
        
        # Current value
        current_value = F.coalesce(
            F.col(f"{current_alias}.{mapper_column}").cast(spark_type),
            F.lit(None).cast(spark_type)
        )
        
        # Previous history with fallback
        prev_history = F.coalesce(
            F.col(f"{previous_alias}.{rolling_col}_history"),
            F.array(*[F.lit(None).cast(spark_type) for _ in range(num_cols)])
        )
        
        # Empty array for gap filling
        null_array = lambda count: F.array_repeat(
            F.lit(None).cast(spark_type), 
            count
        )
        
        # New account array
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
# Grid Column Generator
# ============================================================

def generate_grid_column(
    df: DataFrame,
    grid_config: Dict,
    rolling_columns: List[Dict]
) -> DataFrame:
    """Generate grid column from rolling history array."""
    
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
# Summary Pipeline Core
# ============================================================

class SummaryPipeline:
    """
    Main pipeline for summary generation.
    
    Supports three modes:
    1. initial: First-time load from scratch
    2. forward: Month-by-month sequential processing (production default)
    3. backfill: Late-arriving data with history rebuild
    """
    
    def __init__(self, spark: SparkSession, config: SummaryConfig):
        self.spark = spark
        self.config = config
        self.start_time = time.time()
        self.optimization_counter = 0
    
    # ----------------------------------------------------------
    # Entry Points
    # ----------------------------------------------------------
    
    def run_forward(
        self, 
        start_month: date, 
        end_month: date,
        is_initial: bool = False
    ):
        """
        Run forward (month-by-month) processing.
        
        This matches production behavior - processes months sequentially,
        building history arrays by shifting forward.
        """
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE v4.0 - FORWARD MODE")
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
            
            # Next month
            month = (month.replace(day=28) + relativedelta(days=4)).replace(day=1)
        
        # Final optimization
        self._optimize_tables()
        
        total_time = (time.time() - self.start_time) / 60
        logger.info(f"Pipeline completed in {total_time:.2f} minutes")
    
    def run_backfill(
        self,
        batch_df: Optional[DataFrame] = None,
        source_filter: Optional[str] = None
    ):
        """
        Run backfill processing for late-arriving data.
        
        This handles Case III from v3 - data arriving for historical months
        that requires rebuilding history arrays.
        
        Args:
            batch_df: Optional DataFrame with backfill data
            source_filter: Optional SQL filter for source table
        """
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE v4.0 - BACKFILL MODE")
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
        
        # Process Case I (new accounts) - simple, no history to merge
        case_i = classified.filter(F.col("case_type") == "CASE_I")
        if case_i.limit(1).count() > 0:
            self._process_case_i(case_i)
        
        # Process Case II (forward entries) - shift existing history
        case_ii = classified.filter(F.col("case_type") == "CASE_II")
        if case_ii.limit(1).count() > 0:
            self._process_case_ii(case_ii)
        
        # Process Case III (backfill) - rebuild history
        case_iii = classified.filter(F.col("case_type") == "CASE_III")
        if case_iii.limit(1).count() > 0:
            self._process_case_iii(case_iii)
        
        total_time = (time.time() - self.start_time) / 60
        logger.info(f"Backfill completed in {total_time:.2f} minutes")
    
    # ----------------------------------------------------------
    # Forward Mode Helpers
    # ----------------------------------------------------------
    
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
                    raise ValueError(f"Processing month {month_str} < max existing {max_month}")
        
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
        
        logger.info(f"Records to process: {record_count}")
        
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
        logger.info(f"Previous records: {prev_count}")
        
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
        
        if is_initial:
            # Initial append
            logger.info("Writing initial data to summary...")
            result.writeTo(self.config.destination_table).append()
            
            # Write to latest_summary
            latest_cols = [
                self.config.primary_column,
                self.config.primary_date_column,
                self.config.partition_column,
                self.config.max_identifier_column,
                *[c for c in self.config.latest_history_addon_cols if c in result.columns],
                *self.config.rolling_history_cols
            ]
            
            result.select(*[c for c in latest_cols if c in result.columns]) \
                .writeTo(self.config.latest_history_table).append()
        else:
            # MERGE for updates
            logger.info("Merging to summary...")
            result.createOrReplaceTempView("result")
            
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
            latest_cols = [
                self.config.primary_column,
                self.config.primary_date_column,
                self.config.partition_column,
                self.config.max_identifier_column,
                *[c for c in self.config.latest_history_addon_cols if c in result.columns],
                *self.config.rolling_history_cols
            ]
            
            result.select(*[c for c in latest_cols if c in result.columns]) \
                .createOrReplaceTempView("result_latest")
            
            self.spark.sql(f"""
                MERGE INTO {self.config.latest_history_table} d
                USING result_latest r
                ON d.{self.config.primary_column} = r.{self.config.primary_column}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
    
    # ----------------------------------------------------------
    # Backfill Mode Helpers
    # ----------------------------------------------------------
    
    def _get_max_summary_timestamp(self):
        """Get max base_ts from summary table."""
        return self.spark.sql(f"""
            SELECT MAX({self.config.max_identifier_column}) as max_ts 
            FROM {self.config.destination_table}
        """).collect()[0]["max_ts"]
    
    def _prepare_source_data(self, df: DataFrame) -> DataFrame:
        """Apply column mappings and transformations to source data."""
        
        # Select with column mapping
        result = df.select(*self.config.get_select_expressions())
        
        # Deduplicate
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
        
        # Add month integer
        result = result.withColumn(
            "month_int",
            F.expr(month_to_int_expr(self.config.partition_column))
        )
        
        return result
    
    def _classify_backfill_records(self, batch_df: DataFrame) -> DataFrame:
        """Classify records into Case I, II, III."""
        logger.info("Classifying backfill records...")
        
        # Get summary metadata
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
        """Process new accounts (Case I)."""
        logger.info("Processing Case I (new accounts)...")
        
        result = case_i_df
        
        # Apply rolling column mappers
        for rolling_col in self.config.rolling_columns:
            result = result.withColumn(
                rolling_col['mapper_column'],
                F.expr(rolling_col['mapper_expr'])
            )
        
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
        """Process forward entries (Case II)."""
        logger.info("Processing Case II (forward entries)...")
        
        # Get latest summary for affected accounts
        affected_keys = case_ii_df.select(self.config.primary_column).distinct()
        
        latest_summary = self.spark.read.table(self.config.latest_history_table).alias("ls")
        latest_for_affected = latest_summary.join(
            affected_keys.alias("ak"),
            F.col(f"ls.{self.config.primary_column}") == F.col(f"ak.{self.config.primary_column}"),
            "inner"
        ).select("ls.*")
        
        # Apply rolling column mappers
        result = case_ii_df
        for rolling_col in self.config.rolling_columns:
            result = result.withColumn(
                rolling_col['mapper_column'],
                F.expr(rolling_col['mapper_expr'])
            )
        
        # Rename month_gap to MONTH_DIFF for RollingColumnBuilder
        result = result.withColumnRenamed("month_gap", "MONTH_DIFF") \
            .withColumn("MONTH_DIFF", F.col("MONTH_DIFF") + 1)  # Add 1 because gap + 1 = diff
        
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
        Process backfill entries (Case III).
        
        This rebuilds history arrays for affected accounts using pure SQL.
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
        
        # Build rolling column SQL parts
        rolling_col_agg = []
        rolling_col_history = []
        
        for rolling_col in self.config.rolling_columns:
            name = rolling_col['name']
            mapper = rolling_col['mapper_column']
            dtype = rolling_col['type']
            
            # Cast type
            sql_type = "INT" if dtype == "Integer" else "STRING"
            
            # Aggregation to get value at each month
            rolling_col_agg.append(f"{mapper}")
            
            # History array rebuild
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
        
        # Build combined query
        combined_sql = f"""
        WITH 
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
        
        existing_data AS (
            SELECT 
                s.{primary_col},
                s.{partition_col},
                {month_to_int_expr(f"s.{partition_col}")} as month_int,
                {', '.join([f"s.{c}_history[0] as {c['mapper_column']}" for c in self.config.rolling_columns])},
                s.{max_id_col},
                2 as priority
            FROM {self.config.destination_table} s
            WHERE s.{primary_col} IN (SELECT {primary_col} FROM affected_accounts)
        ),
        
        combined_data AS (
            SELECT 
                {primary_col}, {partition_col}, month_int,
                {', '.join(rolling_col_agg)},
                {max_id_col},
                1 as priority
            FROM backfill_validated
            
            UNION ALL
            
            SELECT * FROM existing_data
        ),
        
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {primary_col}, {partition_col}
                    ORDER BY priority, {max_id_col} DESC
                ) as rn
            FROM combined_data
        ),
        
        final_data AS (
            SELECT * FROM deduped WHERE rn = 1
        ),
        
        account_months AS (
            SELECT 
                {primary_col},
                COLLECT_LIST(STRUCT(month_int, {', '.join(rolling_col_agg)})) as month_values,
                COLLECT_LIST(STRUCT({partition_col}, month_int, {max_id_col})) as month_metadata
            FROM final_data
            GROUP BY {primary_col}
        ),
        
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
        
        SELECT
            e.{primary_col},
            e.{partition_col},
            {', '.join(rolling_col_history)},
            e.{max_id_col}
        FROM exploded e
        """
        
        result = self.spark.sql(combined_sql)
        
        # Generate grid columns
        for grid_config in self.config.grid_columns:
            result = generate_grid_column(result, grid_config, self.config.rolling_columns)
        
        self._write_backfill_results(result)
    
    def _write_backfill_results(self, result: DataFrame):
        """Write backfill results via MERGE."""
        result.createOrReplaceTempView("backfill_result")
        
        # MERGE to summary
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
        
        latest_cols = [
            self.config.primary_column,
            self.config.partition_column,
            self.config.max_identifier_column,
            *self.config.rolling_history_cols
        ]
        
        latest_per_account.select(*[c for c in latest_cols if c in latest_per_account.columns]) \
            .createOrReplaceTempView("backfill_latest")
        
        self.spark.sql(f"""
            MERGE INTO {self.config.latest_history_table} t
            USING backfill_latest s
            ON t.{self.config.primary_column} = s.{self.config.primary_column}
            WHEN MATCHED AND s.{self.config.partition_column} >= t.{self.config.partition_column} 
                THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    
    # ----------------------------------------------------------
    # Optimization
    # ----------------------------------------------------------
    
    def _optimize_tables(self):
        """Optimize Iceberg tables."""
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
# CLI Entry Point
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Summary Pipeline v4.0 - Config-driven production scale'
    )
    
    parser.add_argument('--config', type=str, required=True,
                       help='Path to config JSON file')
    parser.add_argument('--mode', type=str, choices=['initial', 'forward', 'backfill'],
                       default='forward', help='Processing mode')
    parser.add_argument('--start-month', type=str,
                       help='Start month (YYYY-MM) for forward mode')
    parser.add_argument('--end-month', type=str,
                       help='End month (YYYY-MM) for forward mode')
    parser.add_argument('--backfill-filter', type=str,
                       help='SQL filter for backfill source data')
    parser.add_argument('--dry-run', action='store_true',
                       help='Preview without writing')
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Load config
    config = SummaryConfig(args.config)
    
    # Create Spark session
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("INFO")
    
    try:
        pipeline = SummaryPipeline(spark, config)
        
        if args.mode == 'forward' or args.mode == 'initial':
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
