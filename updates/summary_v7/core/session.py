"""
Summary Pipeline v7.0 - Spark Session
=====================================

Enhanced session factory with GPU and optimization support.
"""

from pyspark.sql import SparkSession
import logging

from .config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.Session")


def create_spark_session(config: PipelineConfig) -> SparkSession:
    """Create optimized Spark session for v7.0."""
    logger.info(f"Creating Spark session: {config.spark.app_name}")
    
    builder = SparkSession.builder.appName(config.spark.app_name)
    
    # Apply all configurations
    for key, value in config.get_spark_configs().items():
        builder = builder.config(key, value)
    
    # Additional production settings
    builder = builder \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    
    spark = builder.getOrCreate()
    
    logger.info(f"Spark {spark.version} initialized")
    
    return spark


def configure_partitions(spark: SparkSession, num_partitions: int):
    """Configure shuffle partitions."""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))


def enable_gpu_acceleration(spark: SparkSession, config: PipelineConfig):
    """Enable GPU acceleration if configured."""
    if config.gpu.rapids_enabled:
        spark.conf.set("spark.rapids.sql.enabled", "true")
        spark.conf.set("spark.rapids.memory.pinnedPool.size", "2G")
        logger.info("RAPIDS GPU acceleration enabled")
    
    if config.gpu.photon_enabled:
        spark.conf.set("spark.databricks.photon.enabled", "true")
        logger.info("Photon acceleration enabled")
