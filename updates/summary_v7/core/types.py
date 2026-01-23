"""
Summary Pipeline v7.0 - Types
=============================

Type definitions for v7.0 with enhanced tracking.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional, Any
from datetime import datetime


class CaseType(Enum):
    CASE_I = "CASE_I"
    CASE_II = "CASE_II"
    CASE_III = "CASE_III"


class ProcessingMode(Enum):
    INCREMENTAL = "incremental"
    INITIAL = "initial"
    FORWARD = "forward"
    BACKFILL = "backfill"
    STREAMING = "streaming"


class BatchStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    SKIPPED = "skipped"


class DataType(Enum):
    INTEGER = "Integer"
    STRING = "String"
    DECIMAL = "Decimal"
    DATE = "Date"


@dataclass
class RollingColumn:
    name: str
    source_column: str
    mapper_expr: str
    data_type: DataType
    history_length: int = 36
    
    @property
    def history_column_name(self) -> str:
        return f"{self.name}_history"
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'RollingColumn':
        return cls(
            name=d['name'],
            source_column=d['source_column'],
            mapper_expr=d['mapper_expr'],
            data_type=DataType(d['data_type']),
            history_length=d.get('history_length', 36)
        )


@dataclass
class GridColumn:
    name: str
    source_rolling: str
    null_placeholder: str = "?"
    separator: str = ""
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'GridColumn':
        return cls(
            name=d['name'],
            source_rolling=d['source_rolling'],
            null_placeholder=d.get('null_placeholder', '?'),
            separator=d.get('separator', '')
        )


@dataclass
class MicroBatch:
    """Represents a streaming micro-batch."""
    batch_id: str
    batch_number: int
    record_count: int
    start_offset: int
    end_offset: int
    status: BatchStatus = BatchStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class CaseMetrics:
    """Metrics for a single case type."""
    case_type: CaseType
    record_count: int = 0
    batches_total: int = 0
    batches_completed: int = 0
    batches_failed: int = 0
    rows_written: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def throughput(self) -> Optional[float]:
        """Records per second."""
        if self.duration_seconds and self.duration_seconds > 0:
            return self.record_count / self.duration_seconds
        return None


@dataclass
class ProcessingMetrics:
    """Comprehensive metrics for a processing run."""
    run_id: str
    mode: ProcessingMode
    start_time: datetime
    end_time: Optional[datetime] = None
    
    total_records: int = 0
    records_written: int = 0
    
    case_metrics: Dict[CaseType, CaseMetrics] = field(default_factory=dict)
    
    # Optimization metrics
    bloom_filter_hits: int = 0
    bloom_filter_misses: int = 0
    bucketed_join_used: bool = False
    parallel_cases_used: bool = False
    
    # Resource metrics
    peak_memory_gb: float = 0.0
    total_shuffle_bytes: int = 0
    total_spill_bytes: int = 0
    
    @property
    def duration_minutes(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() / 60
        return None
    
    @property
    def overall_throughput(self) -> Optional[float]:
        """Overall records per second."""
        if self.duration_minutes and self.duration_minutes > 0:
            return self.total_records / (self.duration_minutes * 60)
        return None
    
    def get_case_count(self, case_type: CaseType) -> int:
        if case_type in self.case_metrics:
            return self.case_metrics[case_type].record_count
        return 0


@dataclass
class CheckpointState:
    """Enhanced checkpoint state for v7.0."""
    run_id: str
    mode: ProcessingMode
    current_case: Optional[CaseType] = None
    completed_cases: List[CaseType] = field(default_factory=list)
    current_batch: Optional[MicroBatch] = None
    completed_batches: List[str] = field(default_factory=list)
    last_updated: Optional[datetime] = None
    metrics: Optional[ProcessingMetrics] = None
    
    def mark_case_complete(self, case_type: CaseType):
        if case_type not in self.completed_cases:
            self.completed_cases.append(case_type)
        self.current_case = None
        self.last_updated = datetime.now()
    
    def mark_batch_complete(self, batch_id: str):
        if batch_id not in self.completed_batches:
            self.completed_batches.append(batch_id)
        self.current_batch = None
        self.last_updated = datetime.now()
