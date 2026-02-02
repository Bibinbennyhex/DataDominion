#!/usr/bin/env python3
"""
Test: Consecutive Missing Months Backfill (Chained Backfill)

Scenario:
Account has data up to Dec 2025.
Missing months: April, May, June 2025.
Batch contains: April, May, June 2025 (all arriving at once).

Expected Behavior:
1. June 2025 row created. Array should contain May and April values (not NULLs).
2. May 2025 row created. Array should contain April value.
3. July 2025 (existing) updated. Array should contain June, May, April.

This tests "Case III Part A Chaining" - where backfills need to update each other.
"""

import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Add pipeline to path
sys.path.insert(0, '/home/iceberg/summary_v9_production')
# IMPORT THE NEW v9.4.2 PIPELINE
from pipeline.summary_pipeline_v9_4_2 import run_pipeline


def create_spark_session():
    return (SparkSession.builder
            .appName("ConsecutiveBackfillTest")
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


def setup_test_data(spark):
    print("\n" + "=" * 70)
    print("SETTING UP CONSECUTIVE BACKFILL TEST")
    print("=" * 70)
    
    spark.sql("DROP TABLE IF EXISTS demo.chain_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.chain_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.chain_test.latest_summary")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.chain_test")
    
    # Create tables
    spark.sql("""
        CREATE TABLE demo.chain_test.accounts (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am BIGINT,
            payment_rating_cd STRING
        ) USING iceberg
    """)
    
    spark.sql("""
        CREATE TABLE demo.chain_test.summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am BIGINT,
            payment_rating_cd STRING,
            balance_am_history ARRAY<BIGINT>,
            payment_rating_cd_history ARRAY<STRING>,
            updated_base_ts TIMESTAMP
        ) USING iceberg PARTITIONED BY (rpt_as_of_mo)
    """)
    
    spark.sql("""
        CREATE TABLE demo.chain_test.latest_summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am BIGINT,
            payment_rating_cd STRING,
            balance_am_history ARRAY<BIGINT>,
            payment_rating_cd_history ARRAY<STRING>,
            updated_base_ts TIMESTAMP
        ) USING iceberg
    """)

    # ---------------------------------------------------------
    # 1. Insert Existing History (Jan, Feb, Mar ... July ... Dec)
    # Gaps: April, May, June are MISSING
    # ---------------------------------------------------------
    print("Inserting existing summary (Jan-Mar, Jul-Dec)...")
    
    # We cheat a bit and just insert the relevant rows to establish the "Before" state
    # March 2025: bal=3000
    spark.sql("""
        INSERT INTO demo.chain_test.summary 
        VALUES (
            9900, '2025-03', TIMESTAMP '2025-03-01 00:00:00', 3000, '0',
            array(3000, 2000, 1000), array('0', '0', '0'), NULL
        )
    """)
    
    # July 2025: bal=7000. 
    # History: [7000, NULL(Jun), NULL(May), NULL(Apr), 3000(Mar), ...]
    spark.sql("""
        INSERT INTO demo.chain_test.summary 
        VALUES (
            9900, '2025-07', TIMESTAMP '2025-07-01 00:00:00', 7000, '0',
            array(7000, NULL, NULL, NULL, 3000, 2000), array('0', '?', '?', '?', '0', '0'), NULL
        )
    """)
    
    # Latest summary points to July
    spark.sql("""
        INSERT INTO demo.chain_test.latest_summary 
        VALUES (
            9900, '2025-07', TIMESTAMP '2025-07-01 00:00:00', 7000, '0',
            array(7000, NULL, NULL, NULL, 3000, 2000), array('0', '?', '?', '?', '0', '0'), NULL
        )
    """)

    # ---------------------------------------------------------
    # 2. Insert The "Batch" (Backfills for Apr, May, Jun)
    # ---------------------------------------------------------
    print("Inserting backfill batch (Apr, May, Jun)...")
    spark.sql("""
        INSERT INTO demo.chain_test.accounts VALUES
        (9900, '2025-04', TIMESTAMP '2025-04-01 00:00:00', 4000, '0'),
        (9900, '2025-05', TIMESTAMP '2025-05-01 00:00:00', 5000, '0'),
        (9900, '2025-06', TIMESTAMP '2025-06-01 00:00:00', 6000, '0')
    """)

def get_config():
    return {
        "source_table": "demo.chain_test.accounts",
        "destination_table": "demo.chain_test.summary",
        "latest_history_table": "demo.chain_test.latest_summary",
        "primary_column": "cons_acct_key",
        "partition_column": "rpt_as_of_mo",
        "max_identifier_column": "base_ts",
        "history_length": 36,
        "temp_table_schema": "demo.temp",
        
        "rolling_columns": [
            {"name": "balance_am", "type": "Integer"},
            {"name": "payment_rating_cd", "type": "String"}
        ],
        
        "performance": {"broadcast_threshold": 10000000, "min_partitions": 1},
        "spark": {"app_name": "ChainBackfillTest"}
    }

def verify(spark):
    print("\n" + "=" * 70)
    print("VERIFYING RESULTS")
    print("=" * 70)
    
    passed = 0
    failed = 0
    
    # 1. Verify July 2025 (Existing Future Row) - Should have 6000, 5000, 4000
    print("\nChecking July 2025 (Existing)...")
    row_jul = spark.sql("""
        SELECT balance_am_history 
        FROM demo.chain_test.summary 
        WHERE rpt_as_of_mo = '2025-07' AND cons_acct_key = 9900
    """).first()
    
    if row_jul:
        hist = row_jul['balance_am_history']
        # hist[0]=7000(Jul), hist[1]=6000(Jun), hist[2]=5000(May), hist[3]=4000(Apr)
        print(f"July History: {hist}")
        if hist[1] == 6000 and hist[2] == 5000 and hist[3] == 4000:
            print("[PASS] July updated with chained backfills.")
            passed += 1
        else:
            print(f"[FAIL] July missing backfills. Expected [..., 6000, 5000, 4000...], Got {hist}")
            failed += 1
    else:
        print("[FAIL] July row missing!")
        failed += 1

    # 2. Verify June 2025 (New Row) - Should have 5000, 4000
    print("\nChecking June 2025 (New Row)...")
    row_jun = spark.sql("""
        SELECT balance_am_history 
        FROM demo.chain_test.summary 
        WHERE rpt_as_of_mo = '2025-06' AND cons_acct_key = 9900
    """).first()
    
    if row_jun:
        hist = row_jun['balance_am_history']
        # hist[0]=6000(Jun), hist[1]=5000(May), hist[2]=4000(Apr)
        print(f"June History: {hist}")
        if hist[1] == 5000 and hist[2] == 4000:
            print("[PASS] June created with chained backfills (May, Apr present).")
            passed += 1
        else:
            print(f"[FAIL] June missing chained backfills. Expected [6000, 5000, 4000...], Got {hist}")
            failed += 1
    else:
        print("[FAIL] June row missing!")
        failed += 1

    # 3. Verify May 2025 (New Row) - Should have 4000
    print("\nChecking May 2025 (New Row)...")
    row_may = spark.sql("""
        SELECT balance_am_history 
        FROM demo.chain_test.summary 
        WHERE rpt_as_of_mo = '2025-05' AND cons_acct_key = 9900
    """).first()
    
    if row_may:
        hist = row_may['balance_am_history']
        # hist[0]=5000(May), hist[1]=4000(Apr)
        print(f"May History: {hist}")
        if hist[1] == 4000:
            print("[PASS] May created with chained backfill (Apr present).")
            passed += 1
        else:
            print(f"[FAIL] May missing chained backfill. Expected [5000, 4000...], Got {hist}")
            failed += 1
    else:
        print("[FAIL] May row missing!")
        failed += 1

    return passed, failed

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    setup_test_data(spark)
    
    # Run v9.4.2 Pipeline
    print("Running Pipeline v9.4.2...")
    stats = run_pipeline(spark, get_config())
    print(stats)
    
    passed, failed = verify(spark)
    
    print("\n" + "="*30)
    print(f"RESULTS: {passed} PASSED, {failed} FAILED")
    print("="*30)
    
    if failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
