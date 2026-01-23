"""
Summary Pipeline v7.0 - Optimizations Module
=============================================

Iceberg-native optimizations for maximum performance.
"""

from .bloom_filter import BloomFilterOptimizer, create_bloom_filter_column
from .iceberg_optimizer import (
    IcebergOptimizer,
    repartition_for_iceberg_join,
    coalesce_for_write
)
from .columnar_projection import ColumnarProjectionOptimizer

__all__ = [
    'BloomFilterOptimizer',
    'create_bloom_filter_column',
    'IcebergOptimizer',
    'repartition_for_iceberg_join',
    'coalesce_for_write',
    'ColumnarProjectionOptimizer',
]
