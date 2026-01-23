"""
Summary Pipeline v7.0 - Partitioning Utilities
"""

import math
import logging
from core.config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.Partitioning")


def calculate_optimal_partitions(record_count: int, config: PipelineConfig) -> int:
    """Calculate optimal partition count with power-of-2 rounding."""
    perf = config.performance
    
    partitions_by_count = record_count / perf.target_records_per_partition
    data_size_mb = (record_count * perf.avg_record_size_bytes) / (1024 * 1024)
    partitions_by_size = data_size_mb / perf.max_file_size_mb
    
    optimal = max(partitions_by_count, partitions_by_size, 1)
    power_of_2 = 2 ** round(math.log2(optimal)) if optimal > 0 else perf.min_partitions
    
    result = int(max(perf.min_partitions, min(perf.max_partitions, power_of_2)))
    
    logger.info(f"Optimal partitions: {result} (from {record_count:,} records)")
    
    return result


def estimate_processing_time(record_count: int, rows_per_second: int = 100000) -> dict:
    """Estimate processing time."""
    total_seconds = record_count / rows_per_second * 1.5  # 1.5x for overhead
    
    return {
        "record_count": record_count,
        "estimated_minutes": total_seconds / 60,
        "estimated_hours": total_seconds / 3600,
    }
