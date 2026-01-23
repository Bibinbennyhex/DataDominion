"""
Summary Pipeline v6.0 - Dynamic Partitioning
=============================================

Calculate optimal partition counts based on data size.
"""

import math
import logging

from core.config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.Partitioning")


def calculate_optimal_partitions(record_count: int, config: PipelineConfig) -> int:
    """
    Calculate optimal partition count based on data size.
    
    Uses both record count and estimated data size to determine the best
    partition count, then rounds to nearest power of 2 for even distribution.
    
    Args:
        record_count: Number of records to process
        config: Pipeline configuration
        
    Returns:
        Optimal partition count
    """
    perf = config.performance
    
    # Calculate partitions needed by record count
    partitions_by_count = record_count / perf.target_records_per_partition
    
    # Calculate partitions needed by data size
    data_size_mb = (record_count * perf.avg_record_size_bytes) / (1024 * 1024)
    partitions_by_size = data_size_mb / perf.max_file_size_mb
    
    # Take the larger of the two
    optimal = max(partitions_by_count, partitions_by_size)
    
    # Round to nearest power of 2 for even distribution
    if optimal > 0:
        power_of_2 = 2 ** round(math.log2(optimal))
    else:
        power_of_2 = perf.min_partitions
    
    # Clamp to min/max bounds
    result = int(max(perf.min_partitions, min(perf.max_partitions, power_of_2)))
    
    logger.info(f"Calculated optimal partitions: {result}")
    logger.info(f"  Record count: {record_count:,}")
    logger.info(f"  By count: {partitions_by_count:.0f}, By size: {partitions_by_size:.0f}")
    logger.info(f"  Power of 2: {power_of_2}, Bounds: [{perf.min_partitions}, {perf.max_partitions}]")
    
    return result


def estimate_processing_time(
    record_count: int,
    config: PipelineConfig,
    rows_per_second: int = 50000
) -> dict:
    """
    Estimate processing time for a given record count.
    
    Args:
        record_count: Number of records to process
        config: Pipeline configuration
        rows_per_second: Estimated processing rate
        
    Returns:
        Dictionary with time estimates
    """
    # Base processing time
    base_seconds = record_count / rows_per_second
    
    # Add overhead for MERGE operations (roughly 50% overhead)
    merge_overhead = base_seconds * 0.5
    
    # Add overhead for classification
    classification_overhead = record_count / 200000  # ~200K records/sec for classification
    
    total_seconds = base_seconds + merge_overhead + classification_overhead
    
    return {
        "record_count": record_count,
        "base_seconds": base_seconds,
        "total_seconds": total_seconds,
        "total_minutes": total_seconds / 60,
        "total_hours": total_seconds / 3600,
        "rows_per_second": rows_per_second
    }
