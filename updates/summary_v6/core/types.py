"""
Summary Pipeline v6.0 - Core Types
==================================

Type definitions and enums for the pipeline.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional, Any
from datetime import datetime


class CaseType(Enum):
    """Classification of record processing cases."""
    CASE_I = "CASE_I"      # New accounts (no existing data)
    CASE_II = "CASE_II"    # Forward entries (month > max existing)
    CASE_III = "CASE_III"  # Backfill (month <= max existing, newer timestamp)


class ProcessingMode(Enum):
    """Pipeline processing modes."""
    INCREMENTAL = "incremental"  # Auto-detect and handle all cases
    INITIAL = "initial"          # First-time load from scratch
    FORWARD = "forward"          # Sequential month processing
    BACKFILL = "backfill"        # Manual backfill with filter


class BatchStatus(Enum):
    """Status of a processing batch."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


class DataType(Enum):
    """Supported data types for rolling columns."""
    INTEGER = "Integer"
    STRING = "String"
    DECIMAL = "Decimal"
    DATE = "Date"


@dataclass
class RollingColumn:
    """Definition of a rolling history column."""
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
    """Definition of a grid column derived from rolling history."""
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
class BatchInfo:
    """Information about a processing batch."""
    batch_id: str
    case_type: CaseType
    status: BatchStatus
    record_count: int
    partition_key: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass 
class ProcessingStats:
    """Statistics for a processing run."""
    run_id: str
    mode: ProcessingMode
    start_time: datetime
    end_time: Optional[datetime] = None
    
    total_records: int = 0
    case_i_records: int = 0
    case_ii_records: int = 0
    case_iii_records: int = 0
    
    batches_total: int = 0
    batches_completed: int = 0
    batches_failed: int = 0
    
    records_written: int = 0
    partitions_updated: int = 0
    
    @property
    def duration_minutes(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() / 60
        return None
    
    @property
    def success_rate(self) -> float:
        if self.batches_total == 0:
            return 0.0
        return 100.0 * self.batches_completed / self.batches_total


@dataclass
class CheckpointState:
    """State for checkpoint/resume capability."""
    run_id: str
    mode: ProcessingMode
    current_case: Optional[CaseType] = None
    completed_cases: List[CaseType] = field(default_factory=list)
    current_batch_id: Optional[str] = None
    completed_batch_ids: List[str] = field(default_factory=list)
    last_updated: Optional[datetime] = None
    
    def mark_case_complete(self, case_type: CaseType):
        if case_type not in self.completed_cases:
            self.completed_cases.append(case_type)
        self.current_case = None
        self.last_updated = datetime.now()
    
    def mark_batch_complete(self, batch_id: str):
        if batch_id not in self.completed_batch_ids:
            self.completed_batch_ids.append(batch_id)
        self.current_batch_id = None
        self.last_updated = datetime.now()
