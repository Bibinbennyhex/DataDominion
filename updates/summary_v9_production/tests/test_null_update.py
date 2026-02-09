
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

# Add pipeline directory to path
sys.path.append("/home/iceberg/summary_v9_production/pipeline")
from summary_pipeline import run_pipeline

def get_spark():
    return SparkSession.builder \
        .appName("TestNullUpdate") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.defaultCatalog", "demo") \
        .getOrCreate()

def setup_data(spark):
    print("Setting up Null Update Scenario...")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.null_test")
    
    # 1. Create Tables
    spark.sql("DROP TABLE IF EXISTS demo.null_test.accounts")
    spark.sql("""
        CREATE TABLE demo.null_test.accounts (
            cons_acct_key STRING,
            rpt_as_of_mo DATE,
            balance_am DOUBLE,
            base_ts TIMESTAMP
        ) USING iceberg
    """)
    
    # Summary (Destination)
    summary_schema = """
        cons_acct_key STRING,
        rpt_as_of_mo DATE,
        month_int INT,
        base_ts TIMESTAMP,
        balance_am DOUBLE,
        balance_am_history ARRAY<DOUBLE>,
        updated_base_ts TIMESTAMP
    """
    spark.sql("DROP TABLE IF EXISTS demo.null_test.summary")
    spark.sql(f"CREATE TABLE demo.null_test.summary ({summary_schema}) USING iceberg PARTITIONED BY (rpt_as_of_mo)")
    
    spark.sql("DROP TABLE IF EXISTS demo.null_test.latest_summary")
    spark.sql(f"CREATE TABLE demo.null_test.latest_summary ({summary_schema}) USING iceberg")

    # 2. Insert Existing Records
    # 2025-09 (Val=49), 2025-11 (Val=0)
    print("Inserting existing records...")
    spark.sql("""
        INSERT INTO demo.null_test.summary VALUES
        ('9999', DATE '2025-09-30', 24309, timestamp '2025-09-01 00:00:00', 49.0, array(49.0, null, null), timestamp '2025-09-01 00:00:00'),
        ('9999', DATE '2025-11-30', 24311, timestamp '2025-11-01 00:00:00', 0.0, array(0.0, null, 49.0), timestamp '2025-11-01 00:00:00')
    """)
    spark.sql("""
        INSERT INTO demo.null_test.latest_summary VALUES
        ('9999', DATE '2025-11-30', 24311, timestamp '2025-11-01 00:00:00', 0.0, array(0.0, null, 49.0), timestamp '2025-11-01 00:00:00')
    """)

    # 3. Create Input Batch (Updates/Backfills with NULLs)
    print("Generating input batch with NULLs...")
    spark.sql("""
        INSERT INTO demo.null_test.accounts VALUES
        ('9999', DATE '2025-09-30', NULL, timestamp '2026-01-01 00:00:00'),
        ('9999', DATE '2025-10-31', NULL, timestamp '2026-01-01 00:00:00'),
        ('9999', DATE '2025-11-30', NULL, timestamp '2026-01-01 00:00:00')
    """)

def verify_results(spark):
    print("\n==================================================")
    print("VERIFYING NULL UPDATES")
    print("==================================================")
    
    df = spark.table("demo.null_test.summary").filter("cons_acct_key = '9999'").collect()
    res = {row['rpt_as_of_mo'].strftime("%Y-%m"): row for row in df}
    
    # 1. Verify 2025-09 (Update Existing)
    if "2025-09" in res:
        row = res["2025-09"]
        print(f"2025-09 Balance: {row['balance_am']}")
        print(f"2025-09 History: {row['balance_am_history']}")
        if row['balance_am'] is None:
            print("[PASS] 2025-09 Scalar updated to NULL")
        else:
            print(f"[FAIL] 2025-09 Scalar is {row['balance_am']} (Expected NULL)")
            
        if row['balance_am_history'][0] is None:
             print("[PASS] 2025-09 History[0] updated to NULL")
        else:
             print(f"[FAIL] 2025-09 History[0] is {row['balance_am_history'][0]}")

    # 2. Verify 2025-10 (New Backfill)
    if "2025-10" in res:
        row = res["2025-10"]
        print(f"2025-10 Balance: {row['balance_am']}")
        print(f"2025-10 History: {row['balance_am_history']}")
        if row['balance_am'] is None:
            print("[PASS] 2025-10 Scalar created as NULL")
        else:
            print(f"[FAIL] 2025-10 Scalar is {row['balance_am']}")
            
        if row['balance_am_history'][1] is None:
             print("[PASS] 2025-10 History[1] (Sept) is NULL (chained correctly)")
        else:
             print(f"[FAIL] 2025-10 History[1] is {row['balance_am_history'][1]}")

    # 3. Verify 2025-11 (Update Existing)
    if "2025-11" in res:
        row = res["2025-11"]
        print(f"2025-11 Balance: {row['balance_am']}")
        print(f"2025-11 History: {row['balance_am_history']}")
        if row['balance_am'] is None:
            print("[PASS] 2025-11 Scalar updated to NULL")
        else:
            print(f"[FAIL] 2025-11 Scalar is {row['balance_am']}")
            
        if (row['balance_am_history'][0] is None and 
            row['balance_am_history'][1] is None and 
            row['balance_am_history'][2] is None):
             print("[PASS] 2025-11 History[0,1,2] are all NULL")
        else:
             print(f"[FAIL] 2025-11 History mismatch")

def run():
    spark = get_spark()
    setup_data(spark)
    
    config = {
        "source_table": "demo.null_test.accounts",
        "destination_table": "demo.null_test.summary",
        "latest_history_table": "demo.null_test.latest_summary",
        "primary_column": "cons_acct_key",
        "partition_column": "rpt_as_of_mo",
        "max_identifier_column": "base_ts",
        "rolling_columns": [
            {"name": "balance_am", "type": "Double"}
        ],
        "history_length": 36,
        "temp_database": "demo.temp"
    }
    
    run_pipeline(spark, config)
    verify_results(spark)

if __name__ == "__main__":
    run()
