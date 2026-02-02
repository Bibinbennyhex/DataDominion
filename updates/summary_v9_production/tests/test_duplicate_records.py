#!/usr/bin/env python3
"""
Test: Duplicate Record Handling

This test verifies that when the same (cons_acct_key, rpt_as_of_mo) appears
multiple times in the input with different values, the pipeline correctly
uses the record with the LATEST base_ts.

Test Cases:
- 12001: Same month, two records with different base_ts -> latest wins
- 12002: Same month, three records -> latest wins
- 12003: Multiple months, some with duplicates
"""

import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession

# Add pipeline to path
sys.path.insert(0, '/home/iceberg/summary_v9_production')
from pipeline.summary_pipeline import run_pipeline


def create_spark_session():
    """Create SparkSession for testing"""
    return (SparkSession.builder
            .appName("DuplicateRecordTest")
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


def setup_test_tables(spark):
    """Create fresh test tables"""
    print("\n" + "=" * 70)
    print("SETTING UP DUPLICATE RECORD TEST TABLES")
    print("=" * 70)
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.dup_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.dup_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.dup_test.latest_summary")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.dup_test")
    
    # Create accounts table
    spark.sql("""
        CREATE TABLE demo.dup_test.accounts (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am BIGINT,
            actual_payment_am BIGINT,
            credit_limit_am BIGINT,
            past_due_am BIGINT,
            days_past_due BIGINT,
            asset_class_cd STRING,
            payment_rating_cd STRING
        )
        USING iceberg
    """)
    
    # Create summary table
    spark.sql("""
        CREATE TABLE demo.dup_test.summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am BIGINT,
            actual_payment_am BIGINT,
            credit_limit_am BIGINT,
            past_due_am BIGINT,
            days_past_due BIGINT,
            asset_class_cd STRING,
            payment_rating_cd STRING,
            actual_payment_am_history ARRAY<BIGINT>,
            balance_am_history ARRAY<BIGINT>,
            credit_limit_am_history ARRAY<BIGINT>,
            past_due_am_history ARRAY<BIGINT>,
            payment_rating_cd_history ARRAY<STRING>,
            days_past_due_history ARRAY<BIGINT>,
            asset_class_cd_4in_history ARRAY<STRING>,
            payment_history_grid STRING,
            updated_base_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo)
    """)
    
    # Create latest_summary table
    spark.sql("""
        CREATE TABLE demo.dup_test.latest_summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am BIGINT,
            actual_payment_am BIGINT,
            credit_limit_am BIGINT,
            past_due_am BIGINT,
            days_past_due BIGINT,
            asset_class_cd STRING,
            payment_rating_cd STRING,
            actual_payment_am_history ARRAY<BIGINT>,
            balance_am_history ARRAY<BIGINT>,
            credit_limit_am_history ARRAY<BIGINT>,
            past_due_am_history ARRAY<BIGINT>,
            payment_rating_cd_history ARRAY<STRING>,
            days_past_due_history ARRAY<BIGINT>,
            asset_class_cd_4in_history ARRAY<STRING>,
            payment_history_grid STRING,
            updated_base_ts TIMESTAMP
        )
        USING iceberg
    """)
    
    print("Tables created successfully!")


def insert_test_data(spark):
    """Insert test data with duplicates"""
    print("\n" + "=" * 70)
    print("INSERTING TEST DATA WITH DUPLICATES")
    print("=" * 70)
    
    # 12001: Two records for same month, different base_ts
    # Earlier record: balance = 5000
    # Later record: balance = 5500 (should win)
    print("\n  12001: Two records for Jan 2026")
    print("    - Earlier (10:00:00): balance = 5000")
    print("    - Later   (11:00:00): balance = 5500 <- should win")
    
    spark.sql("""
        INSERT INTO demo.dup_test.accounts VALUES
        (12001, '2026-01', TIMESTAMP '2026-01-15 10:00:00', 5000, 100, 10000, 0, 0, 'A', '0'),
        (12001, '2026-01', TIMESTAMP '2026-01-15 11:00:00', 5500, 150, 10000, 0, 0, 'A', '0')
    """)
    
    # 12002: Three records for same month
    # First: balance = 6000
    # Second: balance = 6500
    # Third (latest): balance = 7000 (should win)
    print("\n  12002: Three records for Jan 2026")
    print("    - First  (09:00:00): balance = 6000")
    print("    - Second (10:00:00): balance = 6500")
    print("    - Third  (11:00:00): balance = 7000 <- should win")
    
    spark.sql("""
        INSERT INTO demo.dup_test.accounts VALUES
        (12002, '2026-01', TIMESTAMP '2026-01-15 09:00:00', 6000, 200, 15000, 0, 0, 'A', '0'),
        (12002, '2026-01', TIMESTAMP '2026-01-15 10:00:00', 6500, 250, 15000, 0, 0, 'A', '0'),
        (12002, '2026-01', TIMESTAMP '2026-01-15 11:00:00', 7000, 300, 15000, 0, 0, 'A', '0')
    """)
    
    # 12003: Multiple months, some with duplicates
    # Jan 2026: one record (8000)
    # Feb 2026: two records - 8500 (earlier), 9000 (later - should win)
    # Mar 2026: one record (9500)
    print("\n  12003: Multiple months with Feb duplicate")
    print("    - Jan 2026: balance = 8000 (single)")
    print("    - Feb 2026: 8500 (earlier), 9000 (later) <- 9000 should win")
    print("    - Mar 2026: balance = 9500 (single)")
    
    spark.sql("""
        INSERT INTO demo.dup_test.accounts VALUES
        (12003, '2026-01', TIMESTAMP '2026-01-15 10:00:00', 8000, 400, 20000, 0, 0, 'A', '0'),
        (12003, '2026-02', TIMESTAMP '2026-02-15 09:00:00', 8500, 450, 20000, 0, 0, 'A', '0'),
        (12003, '2026-02', TIMESTAMP '2026-02-15 11:00:00', 9000, 500, 20000, 0, 0, 'A', '0'),
        (12003, '2026-03', TIMESTAMP '2026-03-15 10:00:00', 9500, 550, 20000, 0, 0, 'A', '0')
    """)
    
    # Show what we inserted
    print("\n  All records in accounts table:")
    spark.sql("""
        SELECT cons_acct_key, rpt_as_of_mo, base_ts, balance_am
        FROM demo.dup_test.accounts
        ORDER BY cons_acct_key, rpt_as_of_mo, base_ts
    """).show(truncate=False)


def get_config():
    """Get pipeline configuration"""
    return {
        "source_table": "demo.dup_test.accounts",
        "destination_table": "demo.dup_test.summary",
        "latest_history_table": "demo.dup_test.latest_summary",
        "primary_column": "cons_acct_key",
        "partition_column": "rpt_as_of_mo",
        "max_identifier_column": "base_ts",
        "history_length": 36,
        
        "temp_table_schema": "demo.temp",
        
        # NOTE: Don't specify summary_columns - let all columns be written including arrays
        
        "rolling_columns": [
            {"name": "actual_payment_am", "mapper_expr": "actual_payment_am", 
             "mapper_column": "actual_payment_am", "num_cols": 36, "type": "Integer"},
            {"name": "balance_am", "mapper_expr": "balance_am", 
             "mapper_column": "balance_am", "num_cols": 36, "type": "Integer"},
            {"name": "credit_limit_am", "mapper_expr": "credit_limit_am", 
             "mapper_column": "credit_limit_am", "num_cols": 36, "type": "Integer"},
            {"name": "past_due_am", "mapper_expr": "past_due_am", 
             "mapper_column": "past_due_am", "num_cols": 36, "type": "Integer"},
            {"name": "payment_rating_cd", "mapper_expr": "payment_rating_cd", 
             "mapper_column": "payment_rating_cd", "num_cols": 36, "type": "String"},
            {"name": "days_past_due", "mapper_expr": "days_past_due", 
             "mapper_column": "days_past_due", "num_cols": 36, "type": "Integer"},
            {"name": "asset_class_cd_4in", "mapper_expr": "asset_class_cd", 
             "mapper_column": "asset_class_cd", "num_cols": 36, "type": "String"}
        ],
        
        "grid_columns": [
            {"name": "payment_history_grid", "mapper_rolling_column": "payment_rating_cd",
             "placeholder": "?", "seperator": ""}
        ],
        
        "performance": {
            "broadcast_threshold": 10000000,
            "min_partitions": 4,
            "max_partitions": 32,
            "cache_level": "MEMORY_AND_DISK"
        },
        
        "spark": {
            "app_name": "DuplicateRecordTest",
            "adaptive_enabled": True,
            "adaptive_coalesce": True,
            "adaptive_skew_join": True,
            "broadcast_timeout": 300
        }
    }


def verify_results(spark):
    """Verify that latest base_ts records won"""
    print("\n" + "=" * 70)
    print("VERIFYING DUPLICATE HANDLING RESULTS")
    print("=" * 70)
    
    passed = 0
    failed = 0
    
    # Test 12001: Should have balance = 5500 (from 11:00:00 record)
    print("\n  Account 12001 (two duplicates):")
    row = spark.sql("""
        SELECT balance_am, base_ts, balance_am_history[0] as bal_0
        FROM demo.dup_test.summary
        WHERE cons_acct_key = 12001 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if row:
        if row['balance_am'] == 5500:
            print(f"    [PASS] balance_am = 5500 (latest base_ts won)")
            passed += 1
        else:
            print(f"    [FAIL] balance_am = {row['balance_am']} (expected 5500)")
            failed += 1
        
        if row['bal_0'] == 5500:
            print(f"    [PASS] balance_am_history[0] = 5500")
            passed += 1
        else:
            print(f"    [FAIL] balance_am_history[0] = {row['bal_0']} (expected 5500)")
            failed += 1
        
        # Check base_ts is the later one
        expected_ts = "2026-01-15 11:00:00"
        if "11:00:00" in str(row['base_ts']):
            print(f"    [PASS] base_ts = {row['base_ts']} (correct)")
            passed += 1
        else:
            print(f"    [FAIL] base_ts = {row['base_ts']} (expected 11:00:00)")
            failed += 1
    else:
        print("    [FAIL] No row found for 12001 Jan 2026")
        failed += 3
    
    # Test 12002: Should have balance = 7000 (from 11:00:00 record)
    print("\n  Account 12002 (three duplicates):")
    row = spark.sql("""
        SELECT balance_am, actual_payment_am, base_ts
        FROM demo.dup_test.summary
        WHERE cons_acct_key = 12002 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if row:
        if row['balance_am'] == 7000:
            print(f"    [PASS] balance_am = 7000 (latest base_ts won)")
            passed += 1
        else:
            print(f"    [FAIL] balance_am = {row['balance_am']} (expected 7000)")
            failed += 1
        
        if row['actual_payment_am'] == 300:
            print(f"    [PASS] actual_payment_am = 300 (from same record)")
            passed += 1
        else:
            print(f"    [FAIL] actual_payment_am = {row['actual_payment_am']} (expected 300)")
            failed += 1
    else:
        print("    [FAIL] No row found for 12002 Jan 2026")
        failed += 2
    
    # Test 12003: Multiple months with Feb duplicate
    print("\n  Account 12003 (multiple months, Feb duplicate):")
    
    # Check Jan (single record)
    jan = spark.sql("""
        SELECT balance_am FROM demo.dup_test.summary
        WHERE cons_acct_key = 12003 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if jan and jan['balance_am'] == 8000:
        print(f"    [PASS] Jan balance_am = 8000 (single record)")
        passed += 1
    else:
        print(f"    [FAIL] Jan balance_am = {jan['balance_am'] if jan else 'NULL'} (expected 8000)")
        failed += 1
    
    # Check Feb (should be 9000 from later record)
    feb = spark.sql("""
        SELECT balance_am, balance_am_history[0] as bal_0, balance_am_history[1] as bal_1
        FROM demo.dup_test.summary
        WHERE cons_acct_key = 12003 AND rpt_as_of_mo = '2026-02'
    """).first()
    
    if feb:
        if feb['balance_am'] == 9000:
            print(f"    [PASS] Feb balance_am = 9000 (latest base_ts won)")
            passed += 1
        else:
            print(f"    [FAIL] Feb balance_am = {feb['balance_am']} (expected 9000)")
            failed += 1
        
        if feb['bal_1'] == 8000:
            print(f"    [PASS] Feb bal_history[1] = 8000 (Jan correctly shifted)")
            passed += 1
        else:
            print(f"    [FAIL] Feb bal_history[1] = {feb['bal_1']} (expected 8000)")
            failed += 1
    else:
        print("    [FAIL] No row found for 12003 Feb 2026")
        failed += 2
    
    # Check Mar
    mar = spark.sql("""
        SELECT balance_am, balance_am_history[0] as bal_0, 
               balance_am_history[1] as bal_1, balance_am_history[2] as bal_2
        FROM demo.dup_test.summary
        WHERE cons_acct_key = 12003 AND rpt_as_of_mo = '2026-03'
    """).first()
    
    if mar:
        if mar['balance_am'] == 9500:
            print(f"    [PASS] Mar balance_am = 9500")
            passed += 1
        else:
            print(f"    [FAIL] Mar balance_am = {mar['balance_am']} (expected 9500)")
            failed += 1
        
        if mar['bal_1'] == 9000:
            print(f"    [PASS] Mar bal_history[1] = 9000 (Feb with correct value)")
            passed += 1
        else:
            print(f"    [FAIL] Mar bal_history[1] = {mar['bal_1']} (expected 9000)")
            failed += 1
        
        if mar['bal_2'] == 8000:
            print(f"    [PASS] Mar bal_history[2] = 8000 (Jan shifted correctly)")
            passed += 1
        else:
            print(f"    [FAIL] Mar bal_history[2] = {mar['bal_2']} (expected 8000)")
            failed += 1
    else:
        print("    [FAIL] No row found for 12003 Mar 2026")
        failed += 3
    
    # Check latest_summary has correct values
    print("\n  Checking latest_summary table:")
    latest = spark.sql("""
        SELECT cons_acct_key, rpt_as_of_mo, balance_am
        FROM demo.dup_test.latest_summary
        ORDER BY cons_acct_key
    """).collect()
    
    expected = {12001: 5500, 12002: 7000, 12003: 9500}
    for row in latest:
        acct = row['cons_acct_key']
        if acct in expected:
            if row['balance_am'] == expected[acct]:
                print(f"    [PASS] Account {acct} latest balance = {row['balance_am']}")
                passed += 1
            else:
                print(f"    [FAIL] Account {acct} latest balance = {row['balance_am']} (expected {expected[acct]})")
                failed += 1
    
    return passed, failed


def main():
    print("=" * 70)
    print("DUPLICATE RECORD HANDLING TEST")
    print("=" * 70)
    print(f"Start time: {datetime.now()}")
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Setup
        setup_test_tables(spark)
        insert_test_data(spark)
        
        # Run pipeline
        print("\n" + "=" * 70)
        print("RUNNING PIPELINE")
        print("=" * 70)
        
        config = get_config()
        stats = run_pipeline(spark, config)
        
        print("\n" + "-" * 40)
        print("PIPELINE RESULTS")
        print("-" * 40)
        print(f"Total records:  {stats.get('total_records', 0)}")
        print(f"Records written: {stats.get('records_written', 0)}")
        
        # Verify
        passed, failed = verify_results(spark)
        
        # Summary
        print("\n" + "=" * 70)
        print(f"FINAL RESULTS: {passed} passed, {failed} failed")
        print("=" * 70)
        
        if failed == 0:
            print("\n*** ALL TESTS PASSED ***")
        else:
            print(f"\n*** {failed} TESTS FAILED ***")
        
        print(f"\nEnd time: {datetime.now()}")
        
        return 0 if failed == 0 else 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
