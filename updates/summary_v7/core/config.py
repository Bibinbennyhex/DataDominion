"""
Summary Pipeline v7.0 - Enhanced Configuration
===============================================

Configuration with all performance optimizations.
"""

import json
import os
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

from .types import RollingColumn, GridColumn, DataType


@dataclass
class BloomFilterConfig:
    """Bloom filter optimization settings."""
    enabled: bool = True
    expected_items: int = 10_000_000
    false_positive_rate: float = 0.01


@dataclass
class BucketingConfig:
    """Bucketed join optimization settings."""
    enabled: bool = True
    num_buckets: int = 64
    bucket_column: str = "cons_acct_key"


@dataclass
class ZOrderConfig:
    """Z-ordering optimization settings."""
    enabled: bool = True
    columns: List[str] = field(default_factory=lambda: ["cons_acct_key", "rpt_as_of_mo"])


@dataclass
class ParallelCasesConfig:
    """Parallel case processing settings."""
    enabled: bool = True
    max_parallel: int = 2


@dataclass
class OptimizationConfig:
    """All optimization settings."""
    bloom_filter: BloomFilterConfig = field(default_factory=BloomFilterConfig)
    bucketing: BucketingConfig = field(default_factory=BucketingConfig)
    z_ordering: ZOrderConfig = field(default_factory=ZOrderConfig)
    columnar_projection: bool = True
    parallel_cases: ParallelCasesConfig = field(default_factory=ParallelCasesConfig)


@dataclass
class StreamingConfig:
    """Streaming micro-batch settings."""
    enabled: bool = True
    micro_batch_size: int = 50_000_000
    checkpoint_after_batch: bool = True
    max_concurrent_batches: int = 3


@dataclass
class GPUConfig:
    """GPU acceleration settings."""
    enabled: bool = False
    rapids_enabled: bool = False
    photon_enabled: bool = False


@dataclass
class PerformanceConfig:
    """Performance tuning settings."""
    target_records_per_partition: int = 5_000_000
    min_partitions: int = 64
    max_partitions: int = 16384
    avg_record_size_bytes: int = 200
    max_file_size_mb: int = 256
    broadcast_threshold_mb: int = 100
    optimization_interval: int = 6


@dataclass
class ResilienceConfig:
    """Resilience settings."""
    retry_count: int = 3
    retry_backoff_base: int = 2
    retry_max_delay_seconds: int = 300
    checkpoint_enabled: bool = True
    checkpoint_granularity: str = "batch"


@dataclass
class SparkConfig:
    """Spark session settings."""
    app_name: str = "SummaryPipeline_v7"
    adaptive_enabled: bool = True
    adaptive_coalesce: bool = True
    adaptive_skew_join: bool = True
    dynamic_partition_pruning: bool = True
    broadcast_timeout: int = 600
    network_timeout: str = "800s"
    shuffle_service_enabled: bool = True
    speculation_enabled: bool = True


@dataclass
class MetricsConfig:
    """Metrics collection settings."""
    enabled: bool = True
    collect_detailed: bool = True
    push_to_table: bool = True


class PipelineConfig:
    """
    Main configuration class for v7.0.
    
    Enhanced with all optimization settings.
    """
    
    def __init__(self, config_path: str):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config not found: {config_path}")
        
        with open(config_path, 'r') as f:
            self._raw = json.load(f)
        
        self._parse()
    
    def _parse(self):
        """Parse configuration into typed objects."""
        self.version = self._raw.get('version', '7.0')
        self.environment = self._raw.get('environment', 'production')
        
        # Tables
        tables = self._raw['tables']
        self.source_table = tables['source']
        self.summary_table = tables['summary']
        self.latest_summary_table = tables['latest_summary']
        self.account_metadata_table = tables.get('account_metadata', 'default.account_metadata')
        self.audit_table = tables.get('audit', 'default.pipeline_audit')
        self.checkpoint_table = tables.get('checkpoint', 'default.pipeline_checkpoint')
        self.metrics_table = tables.get('metrics', 'default.pipeline_metrics')
        
        # Keys
        keys = self._raw['keys']
        self.primary_key = keys['primary']
        self.partition_key = keys['partition']
        self.timestamp_key = keys['timestamp']
        self.primary_date_key = keys.get('primary_date', 'acct_dt')
        
        # History
        history = self._raw.get('history', {})
        self.history_length = history.get('length', 36)
        
        # Column mappings
        self.column_mappings: Dict[str, str] = self._raw.get('columns', {})
        self.transformations: List[Dict] = self._raw.get('transformations', [])
        
        # Rolling columns
        self.rolling_columns = [
            RollingColumn.from_dict(rc) for rc in self._raw.get('rolling_columns', [])
        ]
        
        # Grid columns
        self.grid_columns = [
            GridColumn.from_dict(gc) for gc in self._raw.get('grid_columns', [])
        ]
        
        # Optimization config
        opt = self._raw.get('optimization', {})
        bf = opt.get('bloom_filter', {})
        buck = opt.get('bucketing', {})
        zo = opt.get('z_ordering', {})
        pc = opt.get('parallel_cases', {})
        
        self.optimization = OptimizationConfig(
            bloom_filter=BloomFilterConfig(
                enabled=bf.get('enabled', True),
                expected_items=bf.get('expected_items', 10_000_000),
                false_positive_rate=bf.get('false_positive_rate', 0.01)
            ),
            bucketing=BucketingConfig(
                enabled=buck.get('enabled', True),
                num_buckets=buck.get('num_buckets', 64),
                bucket_column=buck.get('bucket_column', self.primary_key)
            ),
            z_ordering=ZOrderConfig(
                enabled=zo.get('enabled', True),
                columns=zo.get('columns', [self.primary_key, self.partition_key])
            ),
            columnar_projection=opt.get('columnar_projection', {}).get('enabled', True),
            parallel_cases=ParallelCasesConfig(
                enabled=pc.get('enabled', True),
                max_parallel=pc.get('max_parallel', 2)
            )
        )
        
        # Streaming config
        stream = self._raw.get('streaming', {})
        self.streaming = StreamingConfig(
            enabled=stream.get('enabled', True),
            micro_batch_size=stream.get('micro_batch_size', 50_000_000),
            checkpoint_after_batch=stream.get('checkpoint_after_batch', True),
            max_concurrent_batches=stream.get('max_concurrent_batches', 3)
        )
        
        # GPU config
        gpu = self._raw.get('gpu', {})
        self.gpu = GPUConfig(
            enabled=gpu.get('enabled', False),
            rapids_enabled=gpu.get('rapids_enabled', False),
            photon_enabled=gpu.get('photon_enabled', False)
        )
        
        # Performance config
        perf = self._raw.get('performance', {})
        self.performance = PerformanceConfig(
            target_records_per_partition=perf.get('target_records_per_partition', 5_000_000),
            min_partitions=perf.get('min_partitions', 64),
            max_partitions=perf.get('max_partitions', 16384),
            avg_record_size_bytes=perf.get('avg_record_size_bytes', 200),
            max_file_size_mb=perf.get('max_file_size_mb', 256),
            broadcast_threshold_mb=perf.get('broadcast_threshold_mb', 100),
            optimization_interval=perf.get('optimization_interval', 6)
        )
        
        # Resilience config
        res = self._raw.get('resilience', {})
        self.resilience = ResilienceConfig(
            retry_count=res.get('retry_count', 3),
            retry_backoff_base=res.get('retry_backoff_base', 2),
            retry_max_delay_seconds=res.get('retry_max_delay_seconds', 300),
            checkpoint_enabled=res.get('checkpoint_enabled', True),
            checkpoint_granularity=res.get('checkpoint_granularity', 'batch')
        )
        
        # Spark config
        spark = self._raw.get('spark', {})
        self.spark = SparkConfig(
            app_name=spark.get('app_name', 'SummaryPipeline_v7'),
            adaptive_enabled=spark.get('adaptive_enabled', True),
            adaptive_coalesce=spark.get('adaptive_coalesce', True),
            adaptive_skew_join=spark.get('adaptive_skew_join', True),
            dynamic_partition_pruning=spark.get('dynamic_partition_pruning', True),
            broadcast_timeout=spark.get('broadcast_timeout', 600),
            network_timeout=spark.get('network_timeout', '800s'),
            shuffle_service_enabled=spark.get('shuffle_service_enabled', True),
            speculation_enabled=spark.get('speculation_enabled', True)
        )
        
        # Metrics config
        metrics = self._raw.get('metrics', {})
        self.metrics = MetricsConfig(
            enabled=metrics.get('enabled', True),
            collect_detailed=metrics.get('collect_detailed', True),
            push_to_table=metrics.get('push_to_table', True)
        )
    
    @property
    def history_column_names(self) -> List[str]:
        return [rc.history_column_name for rc in self.rolling_columns]
    
    @property
    def source_columns_for_rolling(self) -> List[str]:
        return [rc.source_column for rc in self.rolling_columns]
    
    def get_minimal_columns_for_case_ii(self) -> List[str]:
        """Get minimal columns needed for Case II processing."""
        return [
            self.primary_key,
            self.partition_key,
            self.timestamp_key,
            *self.history_column_names
        ]
    
    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary."""
        configs = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.network.timeout": self.spark.network_timeout,
            "spark.executor.heartbeatInterval": "60s",
            "spark.sql.broadcastTimeout": str(self.spark.broadcast_timeout),
            "spark.sql.autoBroadcastJoinThreshold": f"{self.performance.broadcast_threshold_mb}m",
        }
        
        if self.spark.adaptive_enabled:
            configs.update({
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": str(self.spark.adaptive_coalesce).lower(),
                "spark.sql.adaptive.skewJoin.enabled": str(self.spark.adaptive_skew_join).lower(),
                "spark.sql.adaptive.localShuffleReader.enabled": "true",
            })
        
        if self.spark.dynamic_partition_pruning:
            configs["spark.sql.optimizer.dynamicPartitionPruning.enabled"] = "true"
        
        if self.spark.shuffle_service_enabled:
            configs["spark.shuffle.service.enabled"] = "true"
        
        if self.spark.speculation_enabled:
            configs["spark.speculation"] = "true"
            configs["spark.speculation.multiplier"] = "1.5"
        
        if self.gpu.rapids_enabled:
            configs["spark.rapids.sql.enabled"] = "true"
        
        if self.gpu.photon_enabled:
            configs["spark.databricks.photon.enabled"] = "true"
        
        return configs
