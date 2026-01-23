"""
Summary Pipeline v7.0 - Iceberg-Native Optimizations
=====================================================

Iceberg-specific optimizations instead of legacy bucketing.

Key Iceberg Features Used:
- Sort orders (replaces bucketing)
- Hidden partitioning
- Partition pruning
- Row-level deletes (copy-on-write vs merge-on-read)
- Data file compaction
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

from core.config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.IcebergOptimizer")


class IcebergOptimizer:
    """
    Iceberg-native optimizations.
    
    Iceberg doesn't use bucketing like Hive - instead it uses:
    - Sort orders for data locality
    - Hidden partitioning for automatic partition pruning
    - Distributed sort for efficient joins
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def configure_sort_order(self, table: str, columns: list = None):
        """
        Configure Iceberg write sort order for better data locality.
        
        This replaces traditional bucketing - Iceberg will sort data
        files by these columns, enabling more efficient joins and scans.
        
        Args:
            table: Full table name
            columns: Columns to sort by (defaults to primary + partition key)
        """
        if columns is None:
            columns = [self.config.primary_key, self.config.partition_key]
        
        sort_cols = ", ".join(columns)
        
        try:
            self.spark.sql(f"""
                ALTER TABLE {table} 
                WRITE ORDERED BY ({sort_cols})
            """)
            logger.info(f"Configured sort order for {table}: {sort_cols}")
        except Exception as e:
            logger.warning(f"Could not set sort order for {table}: {e}")
    
    def configure_distribution(self, table: str, column: str = None):
        """
        Configure write distribution for better parallelism.
        
        Args:
            table: Full table name
            column: Distribution column (defaults to primary key)
        """
        dist_col = column or self.config.primary_key
        
        try:
            self.spark.sql(f"""
                ALTER TABLE {table}
                WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY ({dist_col})
            """)
            logger.info(f"Configured distribution for {table} by {dist_col}")
        except Exception as e:
            logger.warning(f"Could not set distribution for {table}: {e}")
    
    def set_merge_mode(self, table: str, mode: str = "merge-on-read"):
        """
        Set Iceberg's merge mode for MERGE operations.
        
        Args:
            table: Full table name
            mode: 'copy-on-write' or 'merge-on-read'
                  - copy-on-write: Rewrites files on MERGE (slower writes, faster reads)
                  - merge-on-read: Writes delete files (faster writes, slower reads)
        """
        write_mode = "merge-on-read" if mode == "merge-on-read" else "copy-on-write"
        
        try:
            self.spark.sql(f"""
                ALTER TABLE {table} SET TBLPROPERTIES (
                    'write.merge.mode' = '{write_mode}',
                    'write.update.mode' = '{write_mode}',
                    'write.delete.mode' = '{write_mode}'
                )
            """)
            logger.info(f"Set MERGE mode for {table}: {write_mode}")
        except Exception as e:
            logger.warning(f"Could not set merge mode for {table}: {e}")
    
    def optimize_table_properties(self, table: str):
        """
        Set optimal table properties for performance.
        
        Args:
            table: Full table name
        """
        try:
            self.spark.sql(f"""
                ALTER TABLE {table} SET TBLPROPERTIES (
                    'write.parquet.compression-codec' = 'zstd',
                    'write.parquet.row-group-size-bytes' = '134217728',
                    'write.target-file-size-bytes' = '268435456',
                    'read.split.target-size' = '134217728',
                    'write.metadata.delete-after-commit.enabled' = 'true',
                    'write.metadata.previous-versions-max' = '50'
                )
            """)
            logger.info(f"Optimized table properties for {table}")
        except Exception as e:
            logger.warning(f"Could not optimize properties for {table}: {e}")
    
    def enable_position_deletes(self, table: str):
        """
        Enable position-based deletes for efficient MERGE operations.
        
        Args:
            table: Full table name
        """
        try:
            self.spark.sql(f"""
                ALTER TABLE {table} SET TBLPROPERTIES (
                    'format-version' = '2',
                    'write.delete.mode' = 'merge-on-read'
                )
            """)
            logger.info(f"Enabled position deletes for {table}")
        except Exception as e:
            logger.warning(f"Could not enable position deletes for {table}: {e}")
    
    def rewrite_with_sort(self, table: str, filter_expr: str = None):
        """
        Rewrite data files with proper sorting for better performance.
        
        This is like compaction but also re-sorts the data.
        
        Args:
            table: Full table name
            filter_expr: Optional filter to rewrite only specific partitions
        """
        parts = table.split('.')
        catalog = parts[0] if len(parts) == 3 else "spark_catalog"
        db = parts[-2]
        tbl = parts[-1]
        
        try:
            # Rewrite data files with sort order
            if filter_expr:
                self.spark.sql(f"""
                    CALL {catalog}.system.rewrite_data_files(
                        table => '{db}.{tbl}',
                        strategy => 'sort',
                        sort_order => '{self.config.primary_key}, {self.config.partition_key}',
                        where => '{filter_expr}'
                    )
                """)
            else:
                self.spark.sql(f"""
                    CALL {catalog}.system.rewrite_data_files(
                        table => '{db}.{tbl}',
                        strategy => 'sort',
                        sort_order => '{self.config.primary_key}, {self.config.partition_key}'
                    )
                """)
            
            logger.info(f"Rewrote data files with sort for {table}")
        except Exception as e:
            logger.warning(f"Could not rewrite with sort for {table}: {e}")
    
    def compact_and_expire(self, table: str, retain_snapshots: int = 3):
        """
        Compact data files and expire old snapshots.
        
        Args:
            table: Full table name
            retain_snapshots: Number of snapshots to retain
        """
        parts = table.split('.')
        catalog = parts[0] if len(parts) == 3 else "spark_catalog"
        db = parts[-2]
        tbl = parts[-1]
        
        try:
            # Compact small files
            self.spark.sql(f"""
                CALL {catalog}.system.rewrite_data_files(
                    table => '{db}.{tbl}',
                    options => map('min-input-files', '5')
                )
            """)
            
            # Rewrite manifests
            self.spark.sql(f"""
                CALL {catalog}.system.rewrite_manifests(
                    table => '{db}.{tbl}'
                )
            """)
            
            # Expire old snapshots
            self.spark.sql(f"""
                CALL {catalog}.system.expire_snapshots(
                    table => '{db}.{tbl}',
                    retain_last => {retain_snapshots}
                )
            """)
            
            # Remove orphan files
            self.spark.sql(f"""
                CALL {catalog}.system.remove_orphan_files(
                    table => '{db}.{tbl}'
                )
            """)
            
            logger.info(f"Compacted and cleaned up {table}")
        except Exception as e:
            logger.warning(f"Could not compact {table}: {e}")
    
    def get_table_metadata(self, table: str) -> dict:
        """
        Get Iceberg table metadata for analysis.
        
        Args:
            table: Full table name
            
        Returns:
            Dictionary with table metadata
        """
        try:
            # Get snapshot info
            snapshots = self.spark.sql(f"""
                SELECT * FROM {table}.snapshots
                ORDER BY committed_at DESC
                LIMIT 5
            """).collect()
            
            # Get file info
            files = self.spark.sql(f"""
                SELECT COUNT(*) as file_count,
                       SUM(file_size_in_bytes) as total_bytes,
                       AVG(file_size_in_bytes) as avg_file_size
                FROM {table}.files
            """).collect()[0]
            
            # Get partition info
            partitions = self.spark.sql(f"""
                SELECT COUNT(DISTINCT {self.config.partition_key}) as partition_count
                FROM {table}
            """).collect()[0]
            
            return {
                "table": table,
                "snapshot_count": len(snapshots),
                "latest_snapshot": snapshots[0] if snapshots else None,
                "file_count": files["file_count"],
                "total_size_gb": files["total_bytes"] / (1024**3) if files["total_bytes"] else 0,
                "avg_file_size_mb": files["avg_file_size"] / (1024**2) if files["avg_file_size"] else 0,
                "partition_count": partitions["partition_count"]
            }
        except Exception as e:
            logger.warning(f"Could not get metadata for {table}: {e}")
            return {"table": table, "error": str(e)}


def repartition_for_iceberg_join(
    df: DataFrame,
    column: str,
    num_partitions: int = 200
) -> DataFrame:
    """
    Repartition DataFrame optimally for Iceberg MERGE operations.
    
    Uses hash partitioning on the join key for co-located processing.
    
    Args:
        df: Input DataFrame
        column: Column to partition by
        num_partitions: Target number of partitions
        
    Returns:
        Repartitioned DataFrame
    """
    return df.repartition(num_partitions, F.col(column))


def coalesce_for_write(df: DataFrame, target_file_size_mb: int = 256) -> DataFrame:
    """
    Coalesce DataFrame for optimal Iceberg file sizes.
    
    Args:
        df: Input DataFrame
        target_file_size_mb: Target file size
        
    Returns:
        Coalesced DataFrame
    """
    # Estimate output partitions based on data size
    # AQE usually handles this, but explicit control can help
    return df.coalesce(max(1, df.rdd.getNumPartitions() // 2))
