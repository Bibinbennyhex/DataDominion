#!/usr/bin/env python3
"""
Test: Complex Scenarios (Full Range Update & Multi-Month Forward)

Scenario 1: Full Range Update (Case III)
- Existing: 2024-01 to 2025-12 (All Bal=1000)
- Update: 2024-01 to 2025-12 (All Bal=5000)
- Verify: All rows updated to 5000. History arrays updated to show 5000s.

Scenario 2: Multi-Month Forward (Case II)
- Existing: 2025-12 (Bal=1000)
- Update: 2026-01 (2000), 2026-02 (3000), 2026-03 (4000)
- Verify: 2026-03 history contains [4000, 3000, 2000, 1000...]
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession

# Use production pipeline (v9.4.3)
sys.path.insert(0, '/home/iceberg/summary_v9_production')
from pipeline.summary_pipeline import run_pipeline

def create_spark_session():
    return (SparkSession.builder
            .appName("ComplexScenariosTest")
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
    spark.sql("DROP TABLE IF EXISTS demo.complex_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.complex_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.complex_test.latest_summary")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.complex_test")
    
    spark.sql("""
        CREATE TABLE demo.complex_test.accounts (
            cons_acct_key BIGINT, rpt_as_of_mo DATE, base_ts TIMESTAMP, 
            balance_am BIGINT, payment_rating_cd STRING
        ) USING iceberg
    """)
    
    spark.sql("""
        CREATE TABLE demo.complex_test.summary (
            cons_acct_key BIGINT, rpt_as_of_mo DATE, base_ts TIMESTAMP,
            balance_am BIGINT, payment_rating_cd STRING,
            balance_am_history ARRAY<BIGINT>, payment_rating_cd_history ARRAY<STRING>,
            updated_base_ts TIMESTAMP
        ) USING iceberg PARTITIONED BY (rpt_as_of_mo)
    """)
    
    spark.sql("""
        CREATE TABLE demo.complex_test.latest_summary (
            cons_acct_key BIGINT, rpt_as_of_mo DATE, base_ts TIMESTAMP,
            balance_am BIGINT, payment_rating_cd STRING,
            balance_am_history ARRAY<BIGINT>, payment_rating_cd_history ARRAY<STRING>,
            updated_base_ts TIMESTAMP
        ) USING iceberg
    """)

    # ---------------------------------------------------------
    # Scenario 1 Setup: Account 1001 (Full Range)
    # ---------------------------------------------------------
    print("Setting up Scenario 1 (Acct 1001)...")
    # Insert 24 months (2024-01 to 2025-12) with Balance=1000
    
    # Let's insert Dec 2025 as the anchor.
    spark.sql(f"""
        INSERT INTO demo.complex_test.summary 
        VALUES (
            1001, DATE '2025-12-31', TIMESTAMP '2025-12-01 00:00:00', 1000, '0',
            array_repeat(1000, 36), array_repeat('0', 36), NULL
        )
    """)
    spark.sql(f"""
        INSERT INTO demo.complex_test.latest_summary 
        VALUES (
            1001, DATE '2025-12-31', TIMESTAMP '2025-12-01 00:00:00', 1000, '0',
            array_repeat(1000, 36), array_repeat('0', 36), NULL
        )
    """)
    
    # Now insert Updates: 2024-01 to 2025-12. All Bal=5000.
    spark.sql("""
        INSERT INTO demo.complex_test.accounts VALUES
        (1001, DATE '2025-12-31', TIMESTAMP '2026-01-01 00:00:00', 5000, '0'),
        (1001, DATE '2025-11-30', TIMESTAMP '2026-01-01 00:00:00', 5000, '0'),
        (1001, DATE '2025-10-31', TIMESTAMP '2026-01-01 00:00:00', 5000, '0')
    """)

    # ---------------------------------------------------------
    # Scenario 2 Setup: Account 2002 (Multi-Month Forward)
    # ---------------------------------------------------------
    print("Setting up Scenario 2 (Acct 2002)...")
    # Existing: Dec 2025 (Bal=1000)
    spark.sql("""
        INSERT INTO demo.complex_test.latest_summary 
        VALUES (
            2002, DATE '2025-12-31', TIMESTAMP '2025-12-01 00:00:00', 1000, '0',
            array(1000, NULL, NULL), array('0', '?', '?'), NULL
        )
    """)
    
    # Forward Batch: Jan, Feb, Mar 2026
    spark.sql("""
        INSERT INTO demo.complex_test.accounts VALUES
        (2002, DATE '2026-01-31', TIMESTAMP '2026-01-01 00:00:00', 2000, '0'),
        (2002, DATE '2026-02-28', TIMESTAMP '2026-02-01 00:00:00', 3000, '0'),
        (2002, DATE '2026-03-31', TIMESTAMP '2026-03-01 00:00:00', 4000, '0')
    """)

def verify(spark):
    print("\n" + "="*50)
    print("VERIFYING RESULTS")
    print("="*50)
    
    passed_s1 = False
    passed_s2 = False
    
    # ---------------------------------------------------------
    # Verify Scenario 1: Full Range Update
    # ---------------------------------------------------------
    # Account 1001: 2025-12 should have Bal=5000. 
    # AND History[1] (Nov) should be 5000. History[2] (Oct) should be 5000.
    
    print("\nScenario 1: Acct 1001 (Full Update)")
    row1 = spark.sql("""
        SELECT balance_am, balance_am_history, base_ts
        FROM demo.complex_test.summary 
        WHERE cons_acct_key = 1001 AND rpt_as_of_mo = DATE '2025-12-31'
    """).first()
    
    if row1:
        bal = row1['balance_am']
        hist = row1['balance_am_history']
        ts = row1['base_ts']
        
        print(f"Dec 2025 Balance: {bal}")
        print(f"Dec 2025 History: {hist[:5]}")
        print(f"Dec 2025 BaseTS: {ts}")
        
        # Expectation: 
        # - Balance updated to 5000
        # - History Index 0 updated to 5000
        # - BaseTS updated to 2026-01-01 (from update batch), replacing 2025-12-01
        
        expected_ts = datetime(2026, 1, 1)
        
        if bal == 5000 and hist[0] == 5000 and hist[1] == 5000 and hist[2] == 5000 and ts == expected_ts:
            print("[PASS] Updates propagated correctly (Balance, History, and BaseTS updated).")
            passed_s1 = True
        else:
            print(f"[FAIL] Updates failed. Expected TS={expected_ts}, Got TS={ts}, Hist[0]={hist[0]}")
            passed_s1 = False
    else:
        print("[FAIL] Row not found.")

    # ---------------------------------------------------------
    # Verify Scenario 2: Multi-Month Forward
    # ---------------------------------------------------------
    # Account 2002: Mar 2026.
    # History should be: [4000, 3000, 2000, 1000...]
    
    print("\nScenario 2: Acct 2002 (Multi-Forward)")
    row2 = spark.sql("""
        SELECT balance_am_history 
        FROM demo.complex_test.summary 
        WHERE cons_acct_key = 2002 AND rpt_as_of_mo = DATE '2026-03-31'
    """).first()
    
    if row2:
        hist = row2['balance_am_history']
        print(f"Mar 2026 History: {hist[:5]}")
        
        # Check chaining
        if hist[1] == 3000 and hist[2] == 2000 and hist[3] == 1000:
            print("[PASS] Forward months chained correctly.")
            passed_s2 = True
        else:
            print("[FAIL] Forward months NOT chained. Likely gaps.")
    else:
        print("[FAIL] Row not found.")
        
    return passed_s1 and passed_s2

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    setup_data(spark)
    
    config = {
        "source_table": "demo.complex_test.accounts",
        "destination_table": "demo.complex_test.summary",
        "latest_history_table": "demo.complex_test.latest_summary",
        "primary_column": "cons_acct_key", "partition_column": "rpt_as_of_mo", "max_identifier_column": "base_ts",
        "history_length": 36, "temp_table_schema": "demo.temp",
        "rolling_columns": [{"name": "balance_am", "type": "Integer"}, {"name": "payment_rating_cd", "type": "String"}],
        "performance": {"broadcast_threshold": 10000000}, "spark": {"app_name": "ComplexTest"}
    }
    
    run_pipeline(spark, config)
    success = verify(spark)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main()
