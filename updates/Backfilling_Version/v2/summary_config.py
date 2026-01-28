"""
Summary Pipeline v2.0 - Configuration Module

Loads configuration from JSON (compatible with Summary v5) with enhanced
validation and runtime optimization hints.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger("SummaryConfig")


@dataclass
class RollingColumn:
    """Definition of a rolling history column"""
    name: str
    mapper_expr: str
    mapper_column: str
    num_cols: int = 36
    data_type: str = "Integer"  # Integer, String, Decimal
    
    @property
    def history_col(self) -> str:
        return f"{self.name}_history"


@dataclass
class GridColumn:
    """Definition of a grid column (array -> string)"""
    name: str
    mapper_rolling_column: str
    placeholder: str = "?"
    separator: str = ""


@dataclass
class PerformanceConfig:
    """Performance tuning parameters"""
    # Partition sizing
    target_records_per_partition: int = 10_000_000
    min_partitions: int = 16
    max_partitions: int = 8192
    
    # Memory management
    avg_record_size_bytes: int = 500
    max_file_size_mb: int = 256
    broadcast_threshold_mb: int = 50
    
    # Processing thresholds
    small_dataset_threshold: int = 10_000      # Use full rebuild
    medium_dataset_threshold: int = 1_000_000  # Use partition-wise
    # Above medium = use account-batched + partition-wise
    
    # Batch sizes
    account_batch_size: int = 100_000
    partition_batch_size: int = 5
    
    # Parallelism
    max_parallel_partitions: int = 8
    max_parallel_account_batches: int = 4
    
    # Checkpointing
    checkpoint_interval_partitions: int = 5
    checkpoint_interval_accounts: int = 50_000
    
    # Retry
    max_retries: int = 3
    retry_backoff_base: int = 2
    
    # Optimization
    snapshot_interval: int = 12
    compaction_threshold_files: int = 100
    
    # Spark settings
    adaptive_enabled: bool = True
    skew_join_enabled: bool = True
    dynamic_partition_pruning: bool = True


@dataclass
class TableConfig:
    """Table configuration"""
    source_table: str
    destination_table: str
    latest_history_table: str
    staging_table: str = "summary_staging"
    audit_table: str = "summary_audit"
    checkpoint_table: str = "summary_checkpoint"
    
    partition_column: str = "rpt_as_of_mo"
    primary_column: str = "cons_acct_key"
    primary_date_column: str = "acct_dt"
    max_identifier_column: str = "base_ts"
    
    bucket_count: int = 64


@dataclass 
class SummaryConfig:
    """
    Main configuration class - loads from JSON and provides runtime hints.
    Compatible with Summary v5 JSON format.
    """
    # Core settings
    tables: TableConfig
    performance: PerformanceConfig
    
    # Column definitions
    columns: Dict[str, str] = field(default_factory=dict)
    column_transformations: List[Dict] = field(default_factory=list)
    inferred_columns: List[Dict] = field(default_factory=list)
    coalesce_exclusion_cols: List[str] = field(default_factory=list)
    date_col_list: List[str] = field(default_factory=list)
    latest_history_addon_cols: List[str] = field(default_factory=list)
    
    # Rolling and grid columns
    rolling_columns: List[RollingColumn] = field(default_factory=list)
    grid_columns: List[GridColumn] = field(default_factory=list)
    
    # Derived helpers (computed on load)
    history_length: int = 36
    
    @classmethod
    def from_json(cls, config_path: str) -> 'SummaryConfig':
        """Load configuration from JSON file (Summary v5 compatible)"""
        logger.info(f"Loading configuration from: {config_path}")
        
        with open(config_path, 'r') as f:
            raw = json.load(f)
        
        # Parse table config
        tables = TableConfig(
            source_table=raw['source_table'],
            destination_table=raw['destination_table'],
            latest_history_table=raw['latest_history_table'],
            staging_table=raw.get('staging_table', 'summary_staging'),
            audit_table=raw.get('audit_table', 'summary_audit'),
            checkpoint_table=raw.get('checkpoint_table', 'summary_checkpoint'),
            partition_column=raw['partition_column'],
            primary_column=raw['primary_column'],
            primary_date_column=raw['primary_date_column'],
            max_identifier_column=raw['max_identifier_column'],
            bucket_count=raw.get('bucket_count', 64)
        )
        
        # Parse performance config
        perf_raw = raw.get('performance', {})
        performance = PerformanceConfig(
            target_records_per_partition=perf_raw.get('target_records_per_partition', 10_000_000),
            min_partitions=perf_raw.get('min_partitions', 16),
            max_partitions=perf_raw.get('max_partitions', 8192),
            avg_record_size_bytes=perf_raw.get('avg_record_size_bytes', 500),
            max_file_size_mb=perf_raw.get('max_file_size_mb', 256),
            broadcast_threshold_mb=perf_raw.get('broadcast_threshold_mb', 50),
            small_dataset_threshold=perf_raw.get('small_dataset_threshold', 10_000),
            medium_dataset_threshold=perf_raw.get('medium_dataset_threshold', 1_000_000),
            account_batch_size=perf_raw.get('account_batch_size', 100_000),
            partition_batch_size=perf_raw.get('partition_batch_size', 5),
            max_parallel_partitions=perf_raw.get('max_parallel_partitions', 8),
            max_parallel_account_batches=perf_raw.get('max_parallel_account_batches', 4),
            checkpoint_interval_partitions=perf_raw.get('checkpoint_interval_partitions', 5),
            checkpoint_interval_accounts=perf_raw.get('checkpoint_interval_accounts', 50_000),
            max_retries=perf_raw.get('max_retries', 3),
            retry_backoff_base=perf_raw.get('retry_backoff_base', 2),
            snapshot_interval=perf_raw.get('snapshot_interval', 12),
            compaction_threshold_files=perf_raw.get('compaction_threshold_files', 100),
            adaptive_enabled=perf_raw.get('adaptive_enabled', True),
            skew_join_enabled=perf_raw.get('skew_join_enabled', True),
            dynamic_partition_pruning=perf_raw.get('dynamic_partition_pruning', True)
        )
        
        # Parse rolling columns
        rolling_columns = [
            RollingColumn(
                name=rc['name'],
                mapper_expr=rc['mapper_expr'],
                mapper_column=rc['mapper_column'],
                num_cols=rc.get('num_cols', 36),
                data_type=rc.get('type', 'Integer')
            )
            for rc in raw.get('rolling_columns', [])
        ]
        
        # Parse grid columns
        grid_columns = [
            GridColumn(
                name=gc['name'],
                mapper_rolling_column=gc['mapper_rolling_column'],
                placeholder=gc.get('placeholder', '?'),
                separator=gc.get('separator', '')
            )
            for gc in raw.get('grid_columns', [])
        ]
        
        config = cls(
            tables=tables,
            performance=performance,
            columns=raw.get('columns', {}),
            column_transformations=raw.get('column_transformations', []),
            inferred_columns=raw.get('inferred_columns', []),
            coalesce_exclusion_cols=raw.get('coalesce_exclusion_cols', []),
            date_col_list=raw.get('date_col_list', []),
            latest_history_addon_cols=raw.get('latest_history_addon_cols', []),
            rolling_columns=rolling_columns,
            grid_columns=grid_columns,
            history_length=raw.get('history_length', 36)
        )
        
        config._validate()
        logger.info(f"Configuration loaded: {len(rolling_columns)} rolling columns, {len(grid_columns)} grid columns")
        
        return config
    
    def _validate(self):
        """Validate configuration"""
        errors = []
        
        if not self.tables.source_table:
            errors.append("source_table is required")
        if not self.tables.destination_table:
            errors.append("destination_table is required")
        if not self.rolling_columns:
            errors.append("At least one rolling_column is required")
        if self.history_length < 1 or self.history_length > 120:
            errors.append(f"history_length must be 1-120, got {self.history_length}")
        
        if errors:
            raise ValueError(f"Configuration validation failed: {errors}")
    
    @property
    def rolling_mapper_list(self) -> List[str]:
        """List of mapper column names"""
        return [col.mapper_column for col in self.rolling_columns]
    
    @property
    def rolling_history_cols(self) -> List[str]:
        """List of history column names"""
        return [col.history_col for col in self.rolling_columns]
    
    def get_processing_strategy(self, record_count: int) -> str:
        """
        Determine optimal processing strategy based on data size.
        
        Returns:
            'full_rebuild': Single query approach (small datasets)
            'partition_wise': Process partition by partition (medium)
            'batched_partition': Account batches + partition (large)
        """
        if record_count <= self.performance.small_dataset_threshold:
            return 'full_rebuild'
        elif record_count <= self.performance.medium_dataset_threshold:
            return 'partition_wise'
        else:
            return 'batched_partition'
    
    def estimate_resources(self, record_count: int) -> Dict[str, Any]:
        """
        Estimate required resources for processing.
        
        Returns dict with:
            - estimated_time_minutes
            - recommended_executors
            - recommended_memory_gb
            - estimated_cost_usd (approximate)
        """
        strategy = self.get_processing_strategy(record_count)
        
        # Base estimates (tune based on your cluster)
        if strategy == 'full_rebuild':
            time_minutes = max(5, record_count / 50_000)  # ~50K/min
            executors = 5
            memory_gb = 64
        elif strategy == 'partition_wise':
            time_minutes = max(10, (record_count / 100_000) * self.history_length / 10)
            executors = 10
            memory_gb = 32
        else:  # batched_partition
            batches = (record_count + self.performance.account_batch_size - 1) // self.performance.account_batch_size
            time_minutes = batches * 15  # ~15 min per 100K batch
            executors = 10
            memory_gb = 32
        
        # Cost estimate (EMR r5.2xlarge ~$0.50/hr per instance)
        hours = time_minutes / 60
        cost_usd = hours * executors * 0.50
        
        return {
            'strategy': strategy,
            'estimated_time_minutes': round(time_minutes, 1),
            'recommended_executors': executors,
            'recommended_memory_gb': memory_gb,
            'estimated_cost_usd': round(cost_usd, 2),
            'record_count': record_count
        }
    
    def to_spark_configs(self) -> Dict[str, str]:
        """Generate Spark configuration settings"""
        configs = {
            # Iceberg
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.iceberg.planning.preserve-data-grouping": "true",
            "spark.sql.iceberg.vectorization.enabled": "true",
            
            # Adaptive Query Execution
            "spark.sql.adaptive.enabled": str(self.performance.adaptive_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": str(self.performance.skew_join_enabled).lower(),
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            
            # Partition pruning
            "spark.sql.optimizer.dynamicPartitionPruning.enabled": str(self.performance.dynamic_partition_pruning).lower(),
            
            # File sizes
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": f"{self.performance.max_file_size_mb}MB",
            "spark.sql.files.maxPartitionBytes": f"{self.performance.max_file_size_mb}MB",
            
            # Joins
            "spark.sql.autoBroadcastJoinThreshold": f"{self.performance.broadcast_threshold_mb}m",
            "spark.sql.join.preferSortMergeJoin": "true",
            
            # Timeouts
            "spark.network.timeout": "800s",
            "spark.executor.heartbeatInterval": "60s",
            
            # Parquet
            "spark.sql.parquet.int96RebaseModeInWrite": "LEGACY",
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
        }
        
        return configs
