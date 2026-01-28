"""
Local Spark Configuration for v9 Testing
=========================================

Creates a local Spark session configured for testing
with Iceberg support using a local file-based catalog.
"""

from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import tempfile


def create_local_spark_session(app_name: str = "SummaryPipeline_v9_Test") -> SparkSession:
    """
    Create a local Spark session for testing.
    
    Features:
    - Local mode with 4 cores
    - Iceberg support with local catalog
    - Adaptive query execution enabled
    - Memory optimized for testing
    """
    
    # Use temp directory for warehouse
    warehouse_path = os.environ.get(
        "ICEBERG_WAREHOUSE", 
        os.path.join(tempfile.gettempdir(), "iceberg-warehouse")
    )
    
    os.makedirs(warehouse_path, exist_ok=True)
    
    conf = SparkConf()
    
    # Core settings
    conf.set("spark.master", "local[4]")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.sql.shuffle.partitions", "8")
    
    # Iceberg configuration
    conf.set("spark.sql.extensions", 
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.local.type", "hadoop")
    conf.set("spark.sql.catalog.local.warehouse", warehouse_path)
    conf.set("spark.sql.defaultCatalog", "local")
    
    # Performance settings
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "50m")
    
    # Testing optimizations
    conf.set("spark.ui.enabled", "true")
    conf.set("spark.ui.port", "4040")
    conf.set("spark.driver.host", "localhost")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config(conf=conf) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def create_test_tables(spark: SparkSession, database: str = "test_db"):
    """
    Create test tables in local Iceberg catalog.
    """
    
    # Create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS local.{database}")
    
    # Create accounts table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.{database}.accounts (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am INT,
            actual_payment_am INT,
            credit_limit_am INT,
            past_due_am INT,
            days_past_due INT,
            asset_class_cd STRING,
            insert_ts TIMESTAMP,
            update_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo)
    """)
    
    # Create summary table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.{database}.summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_history ARRAY<INT>,
            payment_history ARRAY<INT>,
            credit_limit_history ARRAY<INT>,
            past_due_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            payment_rating_history ARRAY<STRING>,
            asset_class_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo)
    """)
    
    # Create latest_summary table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.{database}.latest_summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_history ARRAY<INT>,
            payment_history ARRAY<INT>,
            credit_limit_history ARRAY<INT>,
            past_due_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            payment_rating_history ARRAY<STRING>,
            asset_class_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    print(f"Created test tables in local.{database}")


def cleanup_test_tables(spark: SparkSession, database: str = "test_db"):
    """
    Clean up test tables after testing.
    """
    spark.sql(f"DROP TABLE IF EXISTS local.{database}.accounts")
    spark.sql(f"DROP TABLE IF EXISTS local.{database}.summary")
    spark.sql(f"DROP TABLE IF EXISTS local.{database}.latest_summary")
    spark.sql(f"DROP DATABASE IF EXISTS local.{database}")
    print(f"Cleaned up local.{database}")


class LocalTestConfig:
    """
    Configuration for local testing that mimics production config.
    """
    
    def __init__(self, database: str = "test_db"):
        self.database = database
        
        # Table names for local testing
        self.accounts_table = f"local.{database}.accounts"
        self.summary_table = f"local.{database}.summary"
        self.latest_summary_table = f"local.{database}.latest_summary"
        
        # Keys
        self.primary_key = "cons_acct_key"
        self.partition_key = "rpt_as_of_mo"
        self.timestamp_key = "base_ts"
        
        # History arrays
        self.history_arrays = [
            ("balance_history", "balance_am"),
            ("payment_history", "actual_payment_am"),
            ("credit_limit_history", "credit_limit_am"),
            ("past_due_history", "past_due_am"),
            ("days_past_due_history", "days_past_due"),
            ("payment_rating_history", "payment_rating_cd"),
            ("asset_class_history", "asset_class_cd")
        ]
        
        # Grid columns
        self.grid_columns = [
            {
                "name": "payment_history_grid",
                "source_history": "payment_rating_history",
                "placeholder": "?",
                "separator": ""
            }
        ]
        
        # Performance settings (local)
        self.broadcast_threshold = 1000
        self.cache_level = None  # Use default for local
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization."""
        return {
            "tables": {
                "source": self.accounts_table,
                "summary": self.summary_table,
                "latest_summary": self.latest_summary_table
            },
            "keys": {
                "primary": self.primary_key,
                "partition": self.partition_key,
                "timestamp": self.timestamp_key
            },
            "history_arrays": self.history_arrays,
            "grid_columns": self.grid_columns,
            "performance": {
                "broadcast_threshold": self.broadcast_threshold
            }
        }


if __name__ == "__main__":
    # Quick test
    print("Creating local Spark session...")
    spark = create_local_spark_session()
    print(f"Spark version: {spark.version}")
    
    print("\nCreating test tables...")
    create_test_tables(spark)
    
    print("\nTest tables created. Listing...")
    spark.sql("SHOW TABLES IN local.test_db").show()
    
    print("\nCleaning up...")
    cleanup_test_tables(spark)
    
    spark.stop()
    print("Done!")
