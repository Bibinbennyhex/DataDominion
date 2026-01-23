"""
Summary Pipeline v7.0 - Bloom Filter Utilities
===============================================

Bloom filter for efficient pre-filtering of affected accounts.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Optional
import logging

from core.config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.BloomFilter")


class BloomFilterOptimizer:
    """
    Uses Bloom filters for efficient account pre-filtering.
    
    Reduces full table scans by pre-filtering to likely matching partitions.
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.bloom_config = config.optimization.bloom_filter
        self._bloom_filter = None
    
    def build_bloom_filter(self, df: DataFrame, column: str) -> str:
        """
        Build a Bloom filter from the given DataFrame column.
        
        Args:
            df: DataFrame containing the column
            column: Column name to build filter from
            
        Returns:
            Serialized bloom filter string
        """
        if not self.bloom_config.enabled:
            return None
        
        logger.info(f"Building Bloom filter for {column}...")
        
        # Build bloom filter using Spark's built-in function
        bloom_result = df.select(
            F.xxhash64(F.col(column)).alias("hash")
        ).agg(
            F.approx_count_distinct("hash").alias("distinct_count")
        ).collect()[0]
        
        distinct_count = bloom_result["distinct_count"]
        logger.info(f"Bloom filter built for ~{distinct_count:,} distinct values")
        
        # Store the distinct keys for filtering
        self._affected_keys = df.select(column).distinct()
        self._affected_keys.cache()
        
        return str(distinct_count)
    
    def apply_bloom_filter(
        self,
        target_df: DataFrame,
        column: str
    ) -> DataFrame:
        """
        Apply Bloom filter to reduce target DataFrame.
        
        Args:
            target_df: DataFrame to filter
            column: Column to filter on
            
        Returns:
            Filtered DataFrame
        """
        if not self.bloom_config.enabled or self._affected_keys is None:
            return target_df
        
        logger.info("Applying Bloom filter pre-filtering...")
        
        # Use semi-join with cached affected keys
        filtered = target_df.join(
            F.broadcast(self._affected_keys),
            column,
            "semi"
        )
        
        original_count = target_df.count()
        filtered_count = filtered.count()
        reduction = (1 - filtered_count / original_count) * 100 if original_count > 0 else 0
        
        logger.info(f"Bloom filter reduced: {original_count:,} → {filtered_count:,} ({reduction:.1f}% reduction)")
        
        return filtered
    
    def cleanup(self):
        """Release cached resources."""
        if self._affected_keys is not None:
            self._affected_keys.unpersist()
            self._affected_keys = None


def create_bloom_filter_column(
    df: DataFrame,
    column: str,
    expected_items: int = 10_000_000,
    fpp: float = 0.01
) -> DataFrame:
    """
    Add a Bloom filter column to DataFrame for later use.
    
    Args:
        df: Input DataFrame
        column: Column to create filter for
        expected_items: Expected number of items
        fpp: False positive probability
        
    Returns:
        DataFrame with bloom filter column
    """
    return df.withColumn(
        f"{column}_bloom_hash",
        F.xxhash64(F.col(column))
    )
