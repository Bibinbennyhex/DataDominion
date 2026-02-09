
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
        .appName("TestNullUpdateOtherCases") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.defaultCatalog", "demo") \
        .getOrCreate()

def setup_data(spark):
    print("Setting up Null Update Scenario (Case I & IV)...")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.null_test_other")
    
    # 1. Create Tables
    spark.sql("DROP TABLE IF EXISTS demo.null_test_other.accounts")
    spark.sql("""
        CREATE TABLE demo.null_test_other.accounts (
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
    spark.sql("DROP TABLE IF EXISTS demo.null_test_other.summary")
    spark.sql(f"CREATE TABLE demo.null_test_other.summary ({summary_schema}) USING iceberg PARTITIONED BY (rpt_as_of_mo)")
    
    spark.sql("DROP TABLE IF EXISTS demo.null_test_other.latest_summary")
    spark.sql(f"CREATE TABLE demo.null_test_other.latest_summary ({summary_schema}) USING iceberg")

    # 2. NO EXISTING RECORDS FOR THESE ACCOUNTS (Case I / IV logic)
    print("No existing records for 8001/7001 (Simulating New Accounts)")

    # 3. Create Input Batch (Case I & IV with NULLs)
    print("Generating input batch with NULLs...")
    
    # Case I: New Account 8001 (Jan 2026) with NULL balance
    spark.sql("""
        INSERT INTO demo.null_test_other.accounts VALUES
        ('8001', DATE '2026-01-31', NULL, timestamp '2026-02-01 00:00:00')
    """)
    
    # Case IV: Bulk Account 7001 (Jan, Feb, Mar 2025)
    # Jan=100, Feb=NULL, Mar=300
    spark.sql("""
        INSERT INTO demo.null_test_other.accounts VALUES
        ('7001', DATE '2025-01-31', 100.0, timestamp '2026-01-01 00:00:00'),
        ('7001', DATE '2025-02-28', NULL, timestamp '2026-01-01 00:00:00'),
        ('7001', DATE '2025-03-31', 300.0, timestamp '2026-01-01 00:00:00')
    """)

def verify_results(spark):
    print("\n==================================================")
    print("VERIFYING NULL UPDATES (CASE I & IV)")
    print("==================================================")
    
    df = spark.table("demo.null_test_other.summary").collect()
    data = {}
    for row in df:
        acct = row['cons_acct_key']
        mo = row['rpt_as_of_mo'].strftime("%Y-%m")
        if acct not in data: data[acct] = {}
        data[acct][mo] = row

    # --- Case I Verification (Account 8001) ---
    res8 = data.get("8001", {})
    if "2026-01" in res8:
        row = res8["2026-01"]
        print(f"Acct 8001 (Case I) 2026-01 Balance: {row['balance_am']}")
        if row['balance_am'] is None:
            if row['balance_am_history'][0] is None:
                print("[PASS] Case I: Created new account with NULL value correctly")
            else:
                print(f"[FAIL] Case I: Array index 0 is not NULL: {row['balance_am_history'][0]}")
        else:
            print(f"[FAIL] Case I: Failed. Scalar={row['balance_am']}")
    else:
        print("[FAIL] Case I: 8001 record missing")

    # --- Case IV Verification (Account 7001) ---
    res7 = data.get("7001", {})
    
    # Check Feb 2025 (The NULL month)
    if "2025-02" in res7:
        row = res7["2025-02"]
        print(f"Acct 7001 (Case IV) 2025-02 Balance: {row['balance_am']}")
        if row['balance_am'] is None:
            print("[PASS] Case IV: Feb 2025 created with NULL scalar")
        else:
            print(f"[FAIL] Case IV: Feb 2025 scalar is {row['balance_am']}")
            
        if row['balance_am_history'][0] is None and row['balance_am_history'][1] == 100.0:
            print("[PASS] Case IV: Feb 2025 history correct [NULL, 100...]")
        else:
            print(f"[FAIL] Case IV: Feb 2025 history mismatch: {row['balance_am_history'][:3]}")
            
    # Check Mar 2025 (The 300 month)
    if "2025-03" in res7:
        row = res7["2025-03"]
        hist = row['balance_am_history']
        print(f"Acct 7001 (Case IV) 2025-03 History: {hist[:3]}")
        if hist[0] == 300.0 and hist[1] is None and hist[2] == 100.0:
            print("[PASS] Case IV: Mar 2025 history correct [300, NULL, 100]")
        else:
            print(f"[FAIL] Case IV: Mar 2025 history mismatch")

def run():
    spark = get_spark()
    setup_data(spark)
    
    config = {
        "source_table": "demo.null_test_other.accounts",
        "destination_table": "demo.null_test_other.summary",
        "latest_history_table": "demo.null_test_other.latest_summary",
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
