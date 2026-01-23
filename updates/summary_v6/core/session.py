"""
Summary Pipeline v6.0 - Spark Session Factory
==============================================

Creates production-optimized Spark sessions with Iceberg support.
"""

from pyspark.sql import SparkSession
import logging

from .config import PipelineConfig

logger = logging.getLogger("SummaryPipeline.Session")


def create_spark_session(config: PipelineConfig) -> SparkSession:
    """
    Create production-optimized Spark session with Iceberg support.
    
    Args:
        config: Pipeline configuration
        
    Returns:
        Configured SparkSession
    """
    logger.info(f"Creating Spark session: {config.spark.app_name}")
    
    builder = SparkSession.builder.appName(config.spark.app_name)
    
    # Apply all Spark configurations
    for key, value in config.get_spark_configs().items():
        builder = builder.config(key, value)
    
    # Additional production settings
    builder = builder \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    
    spark = builder.getOrCreate()
    
    logger.info(f"Spark session created: {spark.version}")
    logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    
    return spark


def configure_partitions(spark: SparkSession, num_partitions: int):
    """
    Dynamically configure Spark shuffle partitions.
    
    Args:
        spark: SparkSession
        num_partitions: Number of partitions to use
    """
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))
    logger.debug(f"Configured {num_partitions} shuffle partitions")
