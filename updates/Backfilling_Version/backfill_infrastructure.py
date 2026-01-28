"""
Infrastructure setup for Iceberg History Backfill System.
Creates staging and audit tables.
"""

from pyspark.sql import SparkSession
import logging

logger = logging.getLogger("BackfillInfrastructure")


def create_backfill_staging_table(spark: SparkSession, table_name: str = "backfill_staging"):
    """
    Create the staging table that holds backfill data.
    This is your source of truth for what needs to be backfilled.
    """
    logger.info(f"Creating backfill staging table: {table_name}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            backfill_id STRING,
            acct BIGINT,
            backfill_month STRING,  -- The month being backfilled (e.g., '2023-12')
            
            -- Values to backfill for each history column
            balance DECIMAL(18,2),
            payment DECIMAL(18,2),
            credit_limit DECIMAL(18,2),
            utilization DECIMAL(5,4),
            delinquency INT,
            available_credit DECIMAL(18,2),
            min_payment DECIMAL(18,2),
            interest_charged DECIMAL(18,2),
            fees_charged DECIMAL(18,2),
            purchases DECIMAL(18,2),
            cash_advances DECIMAL(18,2),
            balance_transfers DECIMAL(18,2),
            credit_score INT,
            payment_status STRING,
            overlimit INT,
            days_past_due INT,
            promotional_rate DECIMAL(5,4),
            reward_points BIGINT,
            transaction_count INT,
            dispute_count INT,
            
            -- Metadata
            created_at TIMESTAMP,
            status STRING,  -- 'pending', 'processing', 'completed', 'failed'
            processed_at TIMESTAMP,
            error_message STRING
        )
        USING iceberg
        PARTITIONED BY (backfill_month)
    """)
    
    logger.info(f"Backfill staging table created: {table_name}")


def create_backfill_audit_table(spark: SparkSession, table_name: str = "backfill_audit"):
    """
    Create audit/checkpoint table for tracking backfill progress.
    """
    logger.info(f"Creating backfill audit table: {table_name}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            backfill_id STRING,
            target_partition STRING,
            accounts_updated BIGINT,
            rows_updated BIGINT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            duration_seconds BIGINT,
            status STRING,
            error_message STRING
        )
        USING iceberg
        PARTITIONED BY (backfill_id)
    """)
    
    logger.info(f"Backfill audit table created: {table_name}")


def setup_infrastructure(spark: SparkSession):
    """
    Complete infrastructure setup.
    """
    create_backfill_staging_table(spark)
    create_backfill_audit_table(spark)
    logger.info("Backfill infrastructure setup complete")


def insert_sample_backfill_data(spark: SparkSession, backfill_id: str = "sample-001"):
    """
    Insert sample backfill data for testing.
    """
    from pyspark.sql import functions as F
    
    sample_data = [
        ("sample-001", 12345, "2023-12", 1500.00, 200.00, 5000.00, 0.30, 0, 3500.00, 50.00, 25.00, 10.00, 100.00, 0.00, 0.00, 720, "current", 0, 0, 0.0000, 5000, 15, 0),
        ("sample-001", 67890, "2023-12", 2300.00, 150.00, 8000.00, 0.29, 0, 5700.00, 75.00, 30.00, 15.00, 150.00, 0.00, 0.00, 685, "current", 0, 0, 0.0000, 8500, 22, 0),
        ("sample-001", 11111, "2023-12", 500.00, 500.00, 3000.00, 0.17, 0, 2500.00, 25.00, 8.00, 0.00, 50.00, 0.00, 0.00, 750, "current", 0, 0, 0.0000, 1200, 8, 0),
    ]
    
    columns = [
        "backfill_id", "acct", "backfill_month", "balance", "payment", "credit_limit", 
        "utilization", "delinquency", "available_credit", "min_payment", "interest_charged",
        "fees_charged", "purchases", "cash_advances", "balance_transfers", "credit_score",
        "payment_status", "overlimit", "days_past_due", "promotional_rate", "reward_points",
        "transaction_count", "dispute_count"
    ]
    
    backfill_df = spark.createDataFrame(sample_data, columns) \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("status", F.lit("pending")) \
        .withColumn("processed_at", F.lit(None).cast("timestamp")) \
        .withColumn("error_message", F.lit(None).cast("string"))
    
    backfill_df.writeTo("backfill_staging").append()
    logger.info(f"Inserted {len(sample_data)} sample backfill records")


if __name__ == "__main__":
    # For standalone execution
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    spark = SparkSession.builder \
        .appName("BackfillInfrastructureSetup") \
        .getOrCreate()
    
    setup_infrastructure(spark)
    
    # Optionally insert sample data
    if "--sample-data" in sys.argv:
        insert_sample_backfill_data(spark)
    
    spark.stop()
