Here's the clean final function without printing:

```python
from pyspark.sql import DataFrame

def dynamic_coalesce(
    df: DataFrame,
    target_file_mb: int = 256,
    min_partitions: int = 50,
    max_partitions: int = 2000,
    compression_ratio: float = 5.0,
    safety_factor: float = 1.2
) -> DataFrame:
    """
    Dynamically coalesce partitions based on estimated data size.
    
    Args:
        df: Input DataFrame
        target_file_mb: Target size per output file in MB
        min_partitions: Minimum number of partitions
        max_partitions: Maximum number of partitions
        compression_ratio: Expected compression ratio (e.g., 5.0 for Parquet/Snappy)
        safety_factor: Buffer multiplier for estimation errors
    
    Returns:
        Coalesced DataFrame (or original if optimization not possible)
    """
    try:
        # Get estimated data size
        size_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
        
        if size_bytes <= 0:
            return df
        
        # Calculate optimal partition count
        adjusted_size = size_bytes / compression_ratio * safety_factor
        target_bytes = target_file_mb * 1024 * 1024
        num_partitions = max(1, int(adjusted_size / target_bytes))
        num_partitions = min(max(num_partitions, min_partitions), max_partitions)
        
        # Get current partitions
        current_parts = df.rdd.getNumPartitions()
        
        # Only coalesce if reducing partitions
        if num_partitions < current_parts:
            return df.coalesce(num_partitions)
        
        return df
        
    except Exception:
        # If anything fails, return original DataFrame
        return df
```

## Usage:

```python
# Simple usage
df_optimized = dynamic_coalesce(df, target_file_mb=256)

# With custom parameters
df_optimized = dynamic_coalesce(
    df,
    target_file_mb=128,
    min_partitions=20,
    max_partitions=1000,
    compression_ratio=4.0
)

# Write to storage
df_optimized.write.parquet("output_path", mode="overwrite")
```

Clean, simple, and production-ready! 🚀
