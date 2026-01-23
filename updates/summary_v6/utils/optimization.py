"""
Summary Pipeline v6.0 - Table Optimization
==========================================

Utilities for Iceberg table optimization.
"""

import logging

from pyspark.sql import SparkSession

from core.config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.Optimization")


class TableOptimizer:
    """
    Handles Iceberg table optimization operations.
    
    Features:
    - File compaction (rewrite_data_files)
    - Snapshot expiration
    - Orphan file cleanup
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.optimization_counter = 0
    
    def optimize_tables(self, force: bool = False):
        """
        Optimize summary and latest_summary tables.
        
        Args:
            force: Force optimization even if interval not reached
        """
        self.optimization_counter += 1
        
        if not force and self.optimization_counter < self.config.performance.optimization_interval:
            logger.debug(f"Skipping optimization (counter: {self.optimization_counter})")
            return
        
        logger.info("Optimizing tables...")
        
        # Optimize latest_summary (most frequently accessed)
        self._optimize_table(self.config.latest_summary_table)
        
        # Optionally optimize summary (less frequently)
        if force:
            self._optimize_table(self.config.summary_table)
        
        self.optimization_counter = 0
    
    def _optimize_table(self, table: str):
        """
        Optimize a single table.
        
        Args:
            table: Full table name
        """
        try:
            # Parse table name
            parts = table.split('.')
            if len(parts) >= 2:
                catalog = parts[0] if len(parts) == 3 else "spark_catalog"
                db = parts[-2]
                tbl = parts[-1]
                
                # Rewrite data files (compaction)
                logger.info(f"Rewriting data files for {table}...")
                self.spark.sql(f"""
                    CALL {catalog}.system.rewrite_data_files(
                        table => '{db}.{tbl}'
                    )
                """)
                
                # Expire old snapshots
                logger.info(f"Expiring snapshots for {table}...")
                self.spark.sql(f"""
                    CALL {catalog}.system.expire_snapshots(
                        table => '{db}.{tbl}',
                        retain_last => 3
                    )
                """)
                
                logger.info(f"Optimization complete for {table}")
                
        except Exception as e:
            logger.warning(f"Could not optimize {table}: {e}")
    
    def cleanup_orphan_files(self, table: str):
        """
        Remove orphan files that are no longer referenced.
        
        Args:
            table: Full table name
        """
        try:
            parts = table.split('.')
            if len(parts) >= 2:
                catalog = parts[0] if len(parts) == 3 else "spark_catalog"
                db = parts[-2]
                tbl = parts[-1]
                
                logger.info(f"Removing orphan files from {table}...")
                self.spark.sql(f"""
                    CALL {catalog}.system.remove_orphan_files(
                        table => '{db}.{tbl}'
                    )
                """)
                
        except Exception as e:
            logger.warning(f"Could not cleanup orphan files for {table}: {e}")
    
    def get_table_stats(self, table: str) -> dict:
        """
        Get statistics for a table.
        
        Args:
            table: Full table name
            
        Returns:
            Dictionary with table statistics
        """
        try:
            # Get basic counts
            count_df = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table}")
            record_count = count_df.collect()[0]["cnt"]
            
            # Get partition info if available
            try:
                partitions_df = self.spark.sql(f"""
                    SELECT COUNT(DISTINCT {self.config.partition_key}) as partition_count
                    FROM {table}
                """)
                partition_count = partitions_df.collect()[0]["partition_count"]
            except:
                partition_count = None
            
            return {
                "table": table,
                "record_count": record_count,
                "partition_count": partition_count
            }
            
        except Exception as e:
            logger.warning(f"Could not get stats for {table}: {e}")
            return {"table": table, "error": str(e)}
