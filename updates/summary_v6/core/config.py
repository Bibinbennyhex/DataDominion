"""
Summary Pipeline v6.0 - Configuration Loader
=============================================

Loads and validates configuration from JSON file.
"""

import json
import os
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

from .types import RollingColumn, GridColumn, DataType


@dataclass
class PerformanceConfig:
    """Performance tuning configuration."""
    target_records_per_partition: int = 5_000_000
    min_partitions: int = 32
    max_partitions: int = 16384
    avg_record_size_bytes: int = 200
    max_file_size_mb: int = 256
    broadcast_threshold_mb: int = 100
    optimization_interval: int = 12


@dataclass
class ResilienceConfig:
    """Resilience and retry configuration."""
    retry_count: int = 3
    retry_backoff_base: int = 2
    retry_max_delay_seconds: int = 300
    batch_size: int = 50_000_000
    checkpoint_enabled: bool = True
    checkpoint_interval: int = 1


@dataclass
class SparkConfig:
    """Spark session configuration."""
    app_name: str = "SummaryPipeline_v6"
    adaptive_enabled: bool = True
    adaptive_coalesce: bool = True
    adaptive_skew_join: bool = True
    dynamic_partition_pruning: bool = True
    broadcast_timeout: int = 600
    network_timeout: str = "800s"
    heartbeat_interval: str = "60s"


@dataclass
class AuditConfig:
    """Audit and logging configuration."""
    enabled: bool = True
    log_level: str = "INFO"
    retain_days: int = 90
    include_sample_data: bool = False


class PipelineConfig:
    """
    Main configuration class.
    
    Loads configuration from JSON file and provides typed access to all settings.
    """
    
    def __init__(self, config_path: str):
        """
        Load configuration from JSON file.
        
        Args:
            config_path: Path to the configuration JSON file
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            self._raw = json.load(f)
        
        self._validate()
        self._parse()
    
    def _validate(self):
        """Validate required configuration sections."""
        required_sections = ['tables', 'keys', 'columns', 'rolling_columns']
        for section in required_sections:
            if section not in self._raw:
                raise ValueError(f"Missing required configuration section: {section}")
    
    def _parse(self):
        """Parse configuration into typed objects."""
        # Environment
        self.environment = self._raw.get('environment', 'production')
        
        # Tables
        tables = self._raw['tables']
        self.source_table = tables['source']
        self.summary_table = tables['summary']
        self.latest_summary_table = tables['latest_summary']
        self.audit_table = tables.get('audit', 'default.pipeline_audit')
        self.checkpoint_table = tables.get('checkpoint', 'default.pipeline_checkpoint')
        
        # Keys
        keys = self._raw['keys']
        self.primary_key = keys['primary']
        self.partition_key = keys['partition']
        self.timestamp_key = keys['timestamp']
        self.primary_date_key = keys.get('primary_date', 'acct_dt')
        
        # History settings
        history = self._raw.get('history', {})
        self.history_length = history.get('length', 36)
        
        # Column mappings
        self.column_mappings: Dict[str, str] = self._raw['columns']
        
        # Transformations
        self.transformations: List[Dict[str, str]] = self._raw.get('transformations', [])
        
        # Rolling columns
        self.rolling_columns: List[RollingColumn] = [
            RollingColumn.from_dict(rc) for rc in self._raw['rolling_columns']
        ]
        
        # Grid columns
        self.grid_columns: List[GridColumn] = [
            GridColumn.from_dict(gc) for gc in self._raw.get('grid_columns', [])
        ]
        
        # Performance config
        perf = self._raw.get('performance', {})
        self.performance = PerformanceConfig(
            target_records_per_partition=perf.get('target_records_per_partition', 5_000_000),
            min_partitions=perf.get('min_partitions', 32),
            max_partitions=perf.get('max_partitions', 16384),
            avg_record_size_bytes=perf.get('avg_record_size_bytes', 200),
            max_file_size_mb=perf.get('max_file_size_mb', 256),
            broadcast_threshold_mb=perf.get('broadcast_threshold_mb', 100),
            optimization_interval=perf.get('optimization_interval', 12)
        )
        
        # Resilience config
        res = self._raw.get('resilience', {})
        self.resilience = ResilienceConfig(
            retry_count=res.get('retry_count', 3),
            retry_backoff_base=res.get('retry_backoff_base', 2),
            retry_max_delay_seconds=res.get('retry_max_delay_seconds', 300),
            batch_size=res.get('batch_size', 50_000_000),
            checkpoint_enabled=res.get('checkpoint_enabled', True),
            checkpoint_interval=res.get('checkpoint_interval', 1)
        )
        
        # Spark config
        spark = self._raw.get('spark', {})
        self.spark = SparkConfig(
            app_name=spark.get('app_name', 'SummaryPipeline_v6'),
            adaptive_enabled=spark.get('adaptive_enabled', True),
            adaptive_coalesce=spark.get('adaptive_coalesce', True),
            adaptive_skew_join=spark.get('adaptive_skew_join', True),
            dynamic_partition_pruning=spark.get('dynamic_partition_pruning', True),
            broadcast_timeout=spark.get('broadcast_timeout', 600),
            network_timeout=spark.get('network_timeout', '800s'),
            heartbeat_interval=spark.get('heartbeat_interval', '60s')
        )
        
        # Audit config
        audit = self._raw.get('audit', {})
        self.audit = AuditConfig(
            enabled=audit.get('enabled', True),
            log_level=audit.get('log_level', 'INFO'),
            retain_days=audit.get('retain_days', 90),
            include_sample_data=audit.get('include_sample_data', False)
        )
    
    @property
    def rolling_column_names(self) -> List[str]:
        """Get list of rolling column names."""
        return [rc.name for rc in self.rolling_columns]
    
    @property
    def history_column_names(self) -> List[str]:
        """Get list of history array column names."""
        return [rc.history_column_name for rc in self.rolling_columns]
    
    @property
    def source_columns(self) -> List[str]:
        """Get list of source column names for rolling columns."""
        return [rc.source_column for rc in self.rolling_columns]
    
    def get_rolling_column(self, name: str) -> Optional[RollingColumn]:
        """Get rolling column by name."""
        for rc in self.rolling_columns:
            if rc.name == name:
                return rc
        return None
    
    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration dictionary."""
        configs = {
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.network.timeout": self.spark.network_timeout,
            "spark.executor.heartbeatInterval": self.spark.heartbeat_interval,
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
        
        return configs
