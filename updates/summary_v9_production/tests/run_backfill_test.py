"""
CASCADE BACKFILL TEST for Summary Pipeline v9.1 Fixed
======================================================

Tests the FIXED pipeline that correctly:
1. Creates new summary rows for backfill months
2. Updates all future summary rows with backfill data

This test verifies the bug fix where original v9 only updated future summaries
but did NOT create a summary row for the backfill month itself.

Usage:
    # In Docker container
    docker exec spark-iceberg python3 /home/iceberg/tests/run_backfill_test.py
    
    # Or with spark-submit
    spark-submit tests/run_backfill_test.py
"""

import sys
import os

# Add the pipeline directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_dir = os.path.join(os.path.dirname(script_dir), 'pipeline')
sys.path.insert(0, pipeline_dir)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("v9.1_fixed_backfill_test") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
        .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.defaultCatalog", "demo") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def setup_tables(spark):
    """Create test tables"""
    print("Creating test database and tables...")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.backfill_test")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.backfill_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.backfill_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.backfill_test.latest_summary")
    
    # Create accounts table (source)
    spark.sql("""
        CREATE TABLE demo.backfill_test.accounts (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am INT,
            actual_payment_am INT,
            credit_limit_am INT,
            past_due_am INT,
            days_past_due INT,
            asset_class_cd STRING,
            payment_rating_cd STRING
        )
        USING iceberg
    """)
    
    # Create summary table
    spark.sql("""
        CREATE TABLE demo.backfill_test.summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am INT,
            actual_payment_am INT,
            credit_limit_am INT,
            past_due_am INT,
            days_past_due INT,
            asset_class_cd STRING,
            payment_rating_cd STRING,
            balance_am_history ARRAY<INT>,
            actual_payment_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_history ARRAY<STRING>,
            payment_rating_cd_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    # Create latest_summary table
    spark.sql("""
        CREATE TABLE demo.backfill_test.latest_summary (
            cons_acct_key BIGINT,
            rpt_as_of_mo STRING,
            base_ts TIMESTAMP,
            balance_am INT,
            actual_payment_am INT,
            credit_limit_am INT,
            past_due_am INT,
            days_past_due INT,
            asset_class_cd STRING,
            payment_rating_cd STRING,
            balance_am_history ARRAY<INT>,
            actual_payment_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_history ARRAY<STRING>,
            payment_rating_cd_history ARRAY<STRING>,
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    print("Tables created successfully!")


def build_int_array(positions_values, length=36):
    """Build SQL array with specific integer values at positions"""
    arr = ["NULL"] * length
    for pos, val in positions_values.items():
        arr[pos] = str(val)
    return "array(" + ", ".join(arr) + ")"


def build_str_array(positions_values, length=36):
    """Build SQL array of strings with specific values at positions"""
    arr = ["NULL"] * length
    for pos, val in positions_values.items():
        arr[pos] = f"'{val}'"
    return "array(" + ", ".join(arr) + ")"


def insert_test_data(spark):
    """
    Insert test data for cascade backfill test.
    
    Scenario:
    - Account 8001 has Nov 2024 summary row (original)
    - Account 8001 has Jan/Feb/Mar 2026 summary rows
    - Backfill: Jan 2025 data arrives late (in April 2026)
    
    Expected after pipeline:
    - Jan 2025 summary row CREATED (with bal[0]=3000, bal[2]=4000 from Nov 2024)
    - Jan 2026 updated (Jan 2025 at position 12)
    - Feb 2026 updated (Jan 2025 at position 13)
    - Mar 2026 updated (Jan 2025 at position 14)
    """
    print("\n" + "=" * 80)
    print("INSERTING TEST DATA")
    print("=" * 80)
    
    # =========================================================================
    # Nov 2024 SUMMARY ROW (the original/oldest summary)
    # Position 0 has the Nov 2024 data
    # =========================================================================
    print("\n1. Nov 2024 Summary Row (original)")
    
    spark.sql(f"""
        INSERT INTO demo.backfill_test.summary VALUES
        (8001, '2024-11', TIMESTAMP '2024-11-15 10:00:00', 
         4000, 400, 12000, 0, 0, 'A', '0',
         {build_int_array({0: 4000})},
         {build_int_array({0: 400})},
         {build_int_array({0: 12000})},
         {build_int_array({0: 0})},
         {build_int_array({0: 0})},
         {build_str_array({0: 'A'})},
         {build_str_array({0: '0'})},
         '0???????????????????????????????????')
    """)
    print("  - Inserted Nov 2024 summary: bal[0]=4000")
    
    # =========================================================================
    # Jan 2026 Summary Row (Nov 2024 is 14 months back = position 14)
    # =========================================================================
    print("\n2. Jan 2026 Summary Row (Nov 2024 at position 14)")
    
    spark.sql(f"""
        INSERT INTO demo.backfill_test.summary VALUES
        (8001, '2026-01', TIMESTAMP '2026-01-15 10:00:00', 
         6000, 600, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 6000, 14: 4000})},
         {build_int_array({0: 600, 14: 400})},
         {build_int_array({0: 15000, 14: 12000})},
         {build_int_array({0: 0, 14: 0})},
         {build_int_array({0: 0, 14: 0})},
         {build_str_array({0: 'A', 14: 'A'})},
         {build_str_array({0: '0', 14: '0'})},
         '0?????????????0?????????????????????')
    """)
    
    # =========================================================================
    # Feb 2026 Summary Row (Nov 2024 at position 15)
    # =========================================================================
    print("\n3. Feb 2026 Summary Row (Nov 2024 at position 15)")
    
    spark.sql(f"""
        INSERT INTO demo.backfill_test.summary VALUES
        (8001, '2026-02', TIMESTAMP '2026-02-15 10:00:00', 
         6500, 650, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 6500, 15: 4000})},
         {build_int_array({0: 650, 15: 400})},
         {build_int_array({0: 15000, 15: 12000})},
         {build_int_array({0: 0, 15: 0})},
         {build_int_array({0: 0, 15: 0})},
         {build_str_array({0: 'A', 15: 'A'})},
         {build_str_array({0: '0', 15: '0'})},
         '0??????????????0????????????????????')
    """)
    
    # =========================================================================
    # Mar 2026 Summary Row (LATEST - Nov 2024 at position 16)
    # =========================================================================
    print("\n4. Mar 2026 Summary Row (LATEST - Nov 2024 at position 16)")
    
    spark.sql(f"""
        INSERT INTO demo.backfill_test.summary VALUES
        (8001, '2026-03', TIMESTAMP '2026-03-15 10:00:00', 
         7000, 700, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 7000, 16: 4000})},
         {build_int_array({0: 700, 16: 400})},
         {build_int_array({0: 15000, 16: 12000})},
         {build_int_array({0: 0, 16: 0})},
         {build_int_array({0: 0, 16: 0})},
         {build_str_array({0: 'A', 16: 'A'})},
         {build_str_array({0: '0', 16: '0'})},
         '0???????????????0???????????????????')
    """)
    
    # Insert into latest_summary (Mar 2026 is latest)
    spark.sql(f"""
        INSERT INTO demo.backfill_test.latest_summary VALUES
        (8001, '2026-03', TIMESTAMP '2026-03-15 10:00:00', 
         7000, 700, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 7000, 16: 4000})},
         {build_int_array({0: 700, 16: 400})},
         {build_int_array({0: 15000, 16: 12000})},
         {build_int_array({0: 0, 16: 0})},
         {build_int_array({0: 0, 16: 0})},
         {build_str_array({0: 'A', 16: 'A'})},
         {build_str_array({0: '0', 16: '0'})},
         '0???????????????0???????????????????')
    """)
    
    # =========================================================================
    # BACKFILL DATA: Jan 2025 arriving late (in April 2026)
    # =========================================================================
    print("\n5. Backfill Data: Jan 2025 (arriving late in April 2026)")
    spark.sql("""
        INSERT INTO demo.backfill_test.accounts VALUES
        (8001, '2025-01', TIMESTAMP '2026-04-01 10:00:00', 
         3000, 300, 10000, 0, 0, 'A', '0')
    """)
    print("  - Inserted accounts record: Jan 2025, balance=3000")
    
    print("\n" + "-" * 60)
    print("Data counts:")
    print(f"  Accounts: {spark.sql('SELECT COUNT(*) FROM demo.backfill_test.accounts').first()[0]}")
    print(f"  Summary: {spark.sql('SELECT COUNT(*) FROM demo.backfill_test.summary').first()[0]}")
    print(f"  Latest Summary: {spark.sql('SELECT COUNT(*) FROM demo.backfill_test.latest_summary').first()[0]}")


def show_before_state(spark):
    """Show state before pipeline runs"""
    print("\n" + "=" * 80)
    print("STATE BEFORE PIPELINE")
    print("=" * 80)
    
    print("\nSummary table (4 rows expected):")
    spark.sql("""
        SELECT 
            cons_acct_key, rpt_as_of_mo,
            balance_am_history[0] as bal_0,
            balance_am_history[2] as bal_2,
            balance_am_history[12] as bal_12,
            balance_am_history[14] as bal_14,
            balance_am_history[16] as bal_16
        FROM demo.backfill_test.summary
        ORDER BY rpt_as_of_mo
    """).show(10, False)


def run_pipeline_test(spark):
    """Run the FIXED pipeline"""
    print("\n" + "=" * 80)
    print("RUNNING FIXED PIPELINE v9.1")
    print("=" * 80)
    
    try:
        from summary_pipeline import run_pipeline, PipelineConfig
    except ImportError as e:
        print(f"Could not import pipeline: {e}")
        print("Make sure summary_pipeline.py is in the path")
        return None
    
    # Create config for test tables
    config = PipelineConfig()
    config.accounts_table = "demo.backfill_test.accounts"
    config.summary_table = "demo.backfill_test.summary"
    config.latest_summary_table = "demo.backfill_test.latest_summary"
    config.primary_key = "cons_acct_key"
    config.partition_key = "rpt_as_of_mo"
    config.timestamp_key = "base_ts"
    config.history_length = 36
    
    # Configure rolling columns (matching table schema)
    config.rolling_columns = [
        {"name": "balance_am", "source_column": "balance_am", "mapper_expr": "balance_am", "data_type": "Integer"},
        {"name": "actual_payment_am", "source_column": "actual_payment_am", "mapper_expr": "actual_payment_am", "data_type": "Integer"},
        {"name": "credit_limit_am", "source_column": "credit_limit_am", "mapper_expr": "credit_limit_am", "data_type": "Integer"},
        {"name": "past_due_am", "source_column": "past_due_am", "mapper_expr": "past_due_am", "data_type": "Integer"},
        {"name": "days_past_due", "source_column": "days_past_due", "mapper_expr": "days_past_due", "data_type": "Integer"},
        {"name": "asset_class_cd", "source_column": "asset_class_cd", "mapper_expr": "asset_class_cd", "data_type": "String"},
        {"name": "payment_rating_cd", "source_column": "payment_rating_cd", "mapper_expr": "payment_rating_cd", "data_type": "String"}
    ]
    
    config.grid_columns = [
        {
            "name": "payment_history_grid",
            "source_history": "payment_rating_cd_history",
            "placeholder": "?",
            "separator": ""
        }
    ]
    
    # Empty column mappings (source columns already have correct names)
    config.column_mappings = {}
    config.column_transformations = []
    config.inferred_columns = []
    config.date_columns = []
    config.summary_columns = []
    config.latest_summary_columns = []
    
    try:
        stats = run_pipeline(spark, config)
        print("\n" + "-" * 60)
        print("PIPELINE RESULTS")
        print("-" * 60)
        print(f"Total records:       {stats.get('total_records', 0)}")
        print(f"Case I (new):        {stats.get('case_i_records', 0)}")
        print(f"Case II (forward):   {stats.get('case_ii_records', 0)}")
        print(f"Case III (backfill): {stats.get('case_iii_records', 0)}")
        print(f"Records written:     {stats.get('records_written', 0)}")
        return stats
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def verify_results(spark):
    """Verify the fix worked correctly"""
    print("\n" + "=" * 80)
    print("VERIFYING FIXED PIPELINE RESULTS")
    print("=" * 80)
    
    print("\nSummary table after pipeline (5 rows expected):")
    spark.sql("""
        SELECT 
            cons_acct_key, rpt_as_of_mo,
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[2] as bal_2,
            balance_am_history[12] as bal_12,
            balance_am_history[13] as bal_13,
            balance_am_history[14] as bal_14,
            balance_am_history[15] as bal_15,
            balance_am_history[16] as bal_16
        FROM demo.backfill_test.summary
        ORDER BY rpt_as_of_mo
    """).show(10, False)
    
    summary_count = spark.sql("""
        SELECT COUNT(*) FROM demo.backfill_test.summary WHERE cons_acct_key = 8001
    """).first()[0]
    print(f"Total summary rows: {summary_count}")
    
    passed = 0
    failed = 0
    
    # =========================================================================
    # TEST 0: Verify 5 summary rows exist (was 4 before fix)
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 0: Summary Row Count")
    print("-" * 60)
    
    if summary_count == 5:
        print(f"  [PASS] Expected 5 rows, got {summary_count}")
        passed += 1
    else:
        print(f"  [FAIL] Expected 5 rows, got {summary_count}")
        failed += 1
    
    # =========================================================================
    # TEST 1: Nov 2024 unchanged
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 1: Nov 2024 Summary (should be unchanged)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT balance_am_history[0] as bal_0
        FROM demo.backfill_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2024-11'
    """).first()
    
    if result and result['bal_0'] == 4000:
        print(f"  [PASS] Nov 2024 bal[0]: {result['bal_0']} (expected 4000)")
        passed += 1
    else:
        print(f"  [FAIL] Nov 2024 bal[0]: {result} (expected 4000)")
        failed += 1
    
    # =========================================================================
    # TEST 2: Jan 2025 summary row CREATED (THE KEY TEST!)
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 2: Jan 2025 Summary (should be CREATED by fix)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[2] as bal_2
        FROM demo.backfill_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2025-01'
    """).first()
    
    if result:
        print("  [PASS] Jan 2025 summary row EXISTS!")
        passed += 1
        
        # Check bal[0] = 3000 (backfill data)
        if result['bal_0'] == 3000:
            print(f"  [PASS] Jan 2025 bal[0]: {result['bal_0']} (expected 3000 - backfill data)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 bal[0]: {result['bal_0']} (expected 3000)")
            failed += 1
        
        # Check bal[1] = NULL (Dec 2024 gap - no data)
        if result['bal_1'] is None:
            print(f"  [PASS] Jan 2025 bal[1]: NULL (expected NULL - Dec 2024 gap)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 bal[1]: {result['bal_1']} (expected NULL)")
            failed += 1
        
        # Check bal[2] = 4000 (Nov 2024 inherited, 2 months back)
        if result['bal_2'] == 4000:
            print(f"  [PASS] Jan 2025 bal[2]: {result['bal_2']} (expected 4000 - Nov 2024 inherited)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 bal[2]: {result['bal_2']} (expected 4000)")
            failed += 1
    else:
        print("  [FAIL] Jan 2025 summary row NOT FOUND!")
        print("  This is the bug that was fixed - row should now be created")
        failed += 4
    
    # =========================================================================
    # TEST 3: Jan 2026 updated with Jan 2025 at position 12
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 3: Jan 2026 Summary (Jan 2025 at position 12)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[0] as bal_0,
            balance_am_history[12] as bal_12,
            balance_am_history[14] as bal_14
        FROM demo.backfill_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if result:
        if result['bal_12'] == 3000:
            print(f"  [PASS] Jan 2025 at pos 12: {result['bal_12']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 at pos 12: {result['bal_12']} (expected 3000)")
            failed += 1
        
        if result['bal_14'] == 4000:
            print(f"  [PASS] Nov 2024 at pos 14: {result['bal_14']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2024 at pos 14: {result['bal_14']} (expected 4000)")
            failed += 1
    
    # =========================================================================
    # TEST 4: Mar 2026 updated with Jan 2025 at position 14
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 4: Mar 2026 Summary (Jan 2025 at position 14)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[14] as bal_14,
            balance_am_history[16] as bal_16
        FROM demo.backfill_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2026-03'
    """).first()
    
    if result:
        if result['bal_14'] == 3000:
            print(f"  [PASS] Jan 2025 at pos 14: {result['bal_14']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 at pos 14: {result['bal_14']} (expected 3000)")
            failed += 1
        
        if result['bal_16'] == 4000:
            print(f"  [PASS] Nov 2024 at pos 16: {result['bal_16']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2024 at pos 16: {result['bal_16']} (expected 4000)")
            failed += 1
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("=" * 80)
    
    if failed == 0:
        print("\nFIX VERIFIED SUCCESSFULLY:")
        print("  1. Jan 2025 summary row CREATED (was missing in buggy v9)")
        print("  2. Jan 2025 has bal[0]=3000 (backfill data)")
        print("  3. Jan 2025 has bal[1]=NULL (Dec 2024 gap - correct)")
        print("  4. Jan 2025 has bal[2]=4000 (Nov 2024 inherited)")
        print("  5. All future summaries UPDATED with Jan 2025 at correct positions")
        print("  6. Nov 2024 original data PRESERVED in all summaries")
    else:
        print("\nSome tests failed - please check the output above")
    
    return passed, failed


def main():
    print("=" * 80)
    print("SUMMARY PIPELINE v9.1 FIXED - BACKFILL TEST")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    
    spark = create_spark_session()
    print(f"\nSpark version: {spark.version}")
    
    try:
        setup_tables(spark)
        insert_test_data(spark)
        show_before_state(spark)
        
        stats = run_pipeline_test(spark)
        
        if stats:
            passed, failed = verify_results(spark)
            
            if failed == 0:
                print("\n" + "=" * 80)
                print("*** ALL TESTS PASSED - FIX VERIFIED ***")
                print("=" * 80)
                return 0
            else:
                print(f"\n*** {failed} TESTS FAILED ***")
                return 1
        else:
            print("\n*** PIPELINE FAILED TO RUN ***")
            return 1
    
    except Exception as e:
        print(f"\nTest run failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        print(f"\nEnd time: {datetime.now()}")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
