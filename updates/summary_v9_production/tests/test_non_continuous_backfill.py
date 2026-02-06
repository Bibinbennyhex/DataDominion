#!/usr/bin/env python3
"""
Test: Non-Continuous Backfill (Gaps in Batch)

Scenario:
Existing: Jan 2025 (bal=1000).
Backfill Batch: Feb, Apr, Jun, Jul 2025.
Missing/Gaps: Mar, May.

Expected Behavior:
July 2025 row should be created with history:
[Jul, Jun, NULL(May), Apr, NULL(Mar), Feb, Jan...]
"""

import sys
from pyspark.sql import SparkSession

# Use production pipeline (v9.4.3)
sys.path.insert(0, '/home/iceberg/summary_v9_production')
from pipeline.summary_pipeline import run_pipeline

def create_spark_session():
    return (SparkSession.builder
            .appName("NonContinuousBackfillTest")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.demo.type", "rest")
            .config("spark.sql.catalog.demo.uri", "http://rest:8181")
            .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/")
            .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000")
            .config("spark.sql.catalog.demo.s3.path-style-access", "true")
            .config("spark.sql.defaultCatalog", "demo")
            .getOrCreate())

def setup_data(spark):
    spark.sql("DROP TABLE IF EXISTS demo.nc_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.nc_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.nc_test.latest_summary")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.nc_test")
    
    spark.sql("""
        CREATE TABLE demo.nc_test.accounts (
            cons_acct_key BIGINT, rpt_as_of_mo STRING, base_ts TIMESTAMP, 
            balance_am BIGINT, payment_rating_cd STRING
        ) USING iceberg
    """)
    
    spark.sql("""
        CREATE TABLE demo.nc_test.summary (
            cons_acct_key BIGINT, rpt_as_of_mo STRING, base_ts TIMESTAMP,
            balance_am BIGINT, payment_rating_cd STRING,
            balance_am_history ARRAY<BIGINT>, payment_rating_cd_history ARRAY<STRING>,
            updated_base_ts TIMESTAMP
        ) USING iceberg PARTITIONED BY (rpt_as_of_mo)
    """)
    
    spark.sql("""
        CREATE TABLE demo.nc_test.latest_summary (
            cons_acct_key BIGINT, rpt_as_of_mo STRING, base_ts TIMESTAMP,
            balance_am BIGINT, payment_rating_cd STRING,
            balance_am_history ARRAY<BIGINT>, payment_rating_cd_history ARRAY<STRING>,
            updated_base_ts TIMESTAMP
        ) USING iceberg
    """)

    # 1. Existing History: Jan 2025 AND Dec 2025 (to force Case III)
    print("Inserting Jan 2025 & Dec 2025 (Existing)...")
    
    # Jan 2025 (The past baseline)
    spark.sql("""
        INSERT INTO demo.nc_test.summary 
        VALUES (
            9901, '2025-01', TIMESTAMP '2025-01-01 00:00:00', 1000, '0',
            array(1000, NULL, NULL), array('0', '?', '?'), NULL
        )
    """)
    
    # Dec 2025 (The future anchor - makes Feb-Jul Backfills)
    # History: [Dec, NULL... Jan]
    # Pos 0 = Dec. Pos 11 = Jan.
    dec_hist = [10000] + [None]*10 + [1000] 
    dec_hist_sql = f"array({', '.join(map(str, [x if x is not None else 'NULL' for x in dec_hist]))})"
    
    spark.sql(f"""
        INSERT INTO demo.nc_test.summary 
        VALUES (
            9901, '2025-12', TIMESTAMP '2025-12-01 00:00:00', 10000, '0',
            {dec_hist_sql}, array('0'), NULL
        )
    """)
    
    # Latest summary points to Dec
    spark.sql(f"""
        INSERT INTO demo.nc_test.latest_summary 
        VALUES (
            9901, '2025-12', TIMESTAMP '2025-12-01 00:00:00', 10000, '0',
            {dec_hist_sql}, array('0'), NULL
        )
    """)

    # 2. Backfill Batch: Feb, Apr, Jun, Jul (All < Dec 2025, so Case III)
    print("Inserting Backfill Batch (Feb, Apr, Jun, Jul)...")
    spark.sql("""
        INSERT INTO demo.nc_test.accounts VALUES
        (9901, '2025-02', TIMESTAMP '2025-02-01 00:00:00', 2000, '0'),
        (9901, '2025-04', TIMESTAMP '2025-04-01 00:00:00', 4000, '0'),
        (9901, '2025-06', TIMESTAMP '2025-06-01 00:00:00', 6000, '0'),
        (9901, '2025-07', TIMESTAMP '2025-07-01 00:00:00', 7000, '0')
    """)

def verify(spark):
    print("\n" + "="*50)
    print("VERIFYING NON-CONTINUOUS BACKFILL")
    print("="*50)
    
    # Check July 2025 (The latest new row)
    # Expected: [7000(Jul), 6000(Jun), NULL(May), 4000(Apr), NULL(Mar), 2000(Feb), 1000(Jan)]
    
    row = spark.sql("""
        SELECT balance_am_history 
        FROM demo.nc_test.summary 
        WHERE cons_acct_key = 9901 AND rpt_as_of_mo = '2025-07'
    """).first()
    
    if not row:
        print("[FAIL] July 2025 row not found")
        return 1
        
    hist = row['balance_am_history']
    print(f"July History: {hist[:7]}")
    
    expected = [7000, 6000, None, 4000, None, 2000, 1000]
    
    passed = True
    for i, exp in enumerate(expected):
        val = hist[i]
        if val != exp:
            print(f"[FAIL] Pos {i}: Expected {exp}, Got {val}")
            passed = False
            
    if passed:
        print("[PASS] History matches expected non-continuous pattern.")
        return 0
    else:
        return 1

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    setup_data(spark)
    
    config = {
        "source_table": "demo.nc_test.accounts",
        "destination_table": "demo.nc_test.summary",
        "latest_history_table": "demo.nc_test.latest_summary",
        "primary_column": "cons_acct_key", "partition_column": "rpt_as_of_mo", "max_identifier_column": "base_ts",
        "history_length": 36, "temp_table_schema": "demo.temp",
        "rolling_columns": [{"name": "balance_am", "type": "Integer"}, {"name": "payment_rating_cd", "type": "String"}],
        "performance": {"broadcast_threshold": 10000000}, "spark": {"app_name": "NCTest"}
    }
    
    run_pipeline(spark, config)
    sys.exit(verify(spark))

if __name__ == "__main__":
    main()
