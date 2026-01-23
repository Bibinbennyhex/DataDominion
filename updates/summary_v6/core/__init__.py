"""
Summary Pipeline v6.0 - Core Module
====================================
"""

from .types import (
    CaseType,
    ProcessingMode,
    BatchStatus,
    DataType,
    RollingColumn,
    GridColumn,
    BatchInfo,
    ProcessingStats,
    CheckpointState
)
from .config import PipelineConfig, PerformanceConfig, ResilienceConfig, SparkConfig, AuditConfig
from .session import create_spark_session, configure_partitions

__all__ = [
    # Types
    'CaseType',
    'ProcessingMode',
    'BatchStatus',
    'DataType',
    'RollingColumn',
    'GridColumn',
    'BatchInfo',
    'ProcessingStats',
    'CheckpointState',
    # Config
    'PipelineConfig',
    'PerformanceConfig',
    'ResilienceConfig',
    'SparkConfig',
    'AuditConfig',
    # Session
    'create_spark_session',
    'configure_partitions',
]
