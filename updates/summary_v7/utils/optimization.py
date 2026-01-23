"""
Summary Pipeline v7.0 - Enhanced Table Optimization (Iceberg-Native)
=====================================================================

Iceberg-specific table optimization utilities.
"""

import logging
from pyspark.sql import SparkSession
from core.config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.Optimization")


class TableOptimizer:
    """
    Iceberg-native table optimization.
    
    Uses Iceberg maintenance procedures:
    - rewrite_data_files (with sort strategy)
    - rewrite_manifests (for faster planning)
    - expire_snapshots (cleanup old versions)
    - remove_orphan_files (cleanup leftover files)
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.counter = 0
    
    def optimize_tables(self, force: bool = False):
        """Run optimization on summary tables."""
        self.counter += 1
        if not force and self.counter < self.config.performance.optimization_interval:
            return
        
        logger.info("Running Iceberg table optimization...")
        
        # Optimize latest_summary (most frequently accessed)
        self._optimize_iceberg_table(self.config.latest_summary_table)
        
        # Optionally optimize full summary
        if force:
            self._optimize_iceberg_table(self.config.summary_table)
        
        self.counter = 0
    
    def _optimize_iceberg_table(self, table: str):
        """
        Run full Iceberg optimization suite on a table.
        
        Args:
            table: Full table name (catalog.db.table or db.table)
        """
        parts = table.split('.')
        catalog = parts[0] if len(parts) == 3 else "spark_catalog"
        db = parts[-2]
        tbl = parts[-1]
        
        try:
            logger.info(f"Optimizing Iceberg table: {table}")
            
            # 1. Rewrite data files with sort (uses configured sort order)
            logger.info(f"  - Rewriting data files (sort strategy)...")
            self.spark.sql(f"""
                CALL {catalog}.system.rewrite_data_files(
                    table => '{db}.{tbl}',
                    strategy => 'sort',
                    options => map(
                        'min-input-files', '3',
                        'target-file-size-bytes', '268435456',
                        'partial-progress.enabled', 'true'
                    )
                )
            """)
            
            # 2. Rewrite manifests for faster query planning
            logger.info(f"  - Rewriting manifests...")
            self.spark.sql(f"""
                CALL {catalog}.system.rewrite_manifests(
                    table => '{db}.{tbl}'
                )
            """)
            
            # 3. Expire old snapshots (keep last 3)
            logger.info(f"  - Expiring old snapshots...")
            self.spark.sql(f"""
                CALL {catalog}.system.expire_snapshots(
                    table => '{db}.{tbl}',
                    retain_last => 3
                )
            """)
            
            # 4. Remove orphan files
            logger.info(f"  - Removing orphan files...")
            self.spark.sql(f"""
                CALL {catalog}.system.remove_orphan_files(
                    table => '{db}.{tbl}',
                    dry_run => false
                )
            """)
            
            logger.info(f"Optimization complete for {table}")
            
        except Exception as e:
            logger.warning(f"Optimization issue for {table}: {e}")
    
    def set_iceberg_properties(self, table: str):
        """
        Set optimal Iceberg table properties.
        
        Args:
            table: Full table name
        """
        try:
            # Configure for optimal performance
            self.spark.sql(f"""
                ALTER TABLE {table} SET TBLPROPERTIES (
                    -- Compression
                    'write.parquet.compression-codec' = 'zstd',
                    
                    -- File sizing
                    'write.target-file-size-bytes' = '268435456',
                    'read.split.target-size' = '134217728',
                    
                    -- Merge optimization (merge-on-read for faster writes)
                    'write.merge.mode' = 'merge-on-read',
                    'write.update.mode' = 'merge-on-read',
                    'write.delete.mode' = 'merge-on-read',
                    
                    -- Metadata cleanup
                    'write.metadata.delete-after-commit.enabled' = 'true',
                    'write.metadata.previous-versions-max' = '50'
                )
            """)
            
            # Configure sort order for better data locality
            self.spark.sql(f"""
                ALTER TABLE {table}
                WRITE ORDERED BY ({self.config.primary_key}, {self.config.partition_key})
            """)
            
            logger.info(f"Set Iceberg properties for {table}")
            
        except Exception as e:
            logger.warning(f"Could not set properties for {table}: {e}")
    
    def analyze_table(self, table: str) -> dict:
        """
        Get table statistics for performance tuning.
        
        Args:
            table: Full table name
            
        Returns:
            Dictionary with table statistics
        """
        try:
            # Record count
            count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
            
            # File statistics
            files = self.spark.sql(f"""
                SELECT 
                    COUNT(*) as file_count,
                    SUM(file_size_in_bytes) as total_bytes,
                    AVG(file_size_in_bytes) as avg_bytes,
                    MIN(file_size_in_bytes) as min_bytes,
                    MAX(file_size_in_bytes) as max_bytes
                FROM {table}.files
            """).collect()[0]
            
            # Snapshot count
            snapshots = self.spark.sql(f"""
                SELECT COUNT(*) as cnt FROM {table}.snapshots
            """).collect()[0]["cnt"]
            
            return {
                "table": table,
                "record_count": count,
                "file_count": files["file_count"],
                "total_size_gb": round(files["total_bytes"] / (1024**3), 2) if files["total_bytes"] else 0,
                "avg_file_size_mb": round(files["avg_bytes"] / (1024**2), 2) if files["avg_bytes"] else 0,
                "min_file_size_mb": round(files["min_bytes"] / (1024**2), 2) if files["min_bytes"] else 0,
                "max_file_size_mb": round(files["max_bytes"] / (1024**2), 2) if files["max_bytes"] else 0,
                "snapshot_count": snapshots
            }
        except Exception as e:
            return {"table": table, "error": str(e)}
    
    def get_optimization_recommendations(self, table: str) -> list:
        """
        Get optimization recommendations for a table.
        
        Args:
            table: Full table name
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        try:
            stats = self.analyze_table(table)
            
            if "error" in stats:
                return [f"Could not analyze: {stats['error']}"]
            
            # Check file sizes
            if stats["avg_file_size_mb"] < 100:
                recommendations.append(
                    f"Small files detected (avg {stats['avg_file_size_mb']:.1f}MB). "
                    "Run rewrite_data_files to compact."
                )
            
            if stats["max_file_size_mb"] > 500:
                recommendations.append(
                    f"Large files detected (max {stats['max_file_size_mb']:.1f}MB). "
                    "Consider reducing target-file-size-bytes."
                )
            
            # Check file count
            files_per_gb = stats["file_count"] / max(stats["total_size_gb"], 0.1)
            if files_per_gb > 10:
                recommendations.append(
                    f"High file count ({stats['file_count']} files, {files_per_gb:.1f}/GB). "
                    "Run compaction."
                )
            
            # Check snapshot count
            if stats["snapshot_count"] > 50:
                recommendations.append(
                    f"Many snapshots ({stats['snapshot_count']}). "
                    "Run expire_snapshots."
                )
            
            if not recommendations:
                recommendations.append("Table appears well-optimized.")
            
        except Exception as e:
            recommendations.append(f"Analysis error: {e}")
        
        return recommendations
