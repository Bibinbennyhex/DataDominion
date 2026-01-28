"""
CASCADE BACKFILL TEST for FIXED Summary Pipeline v9
====================================================

Tests the FIXED pipeline that correctly:
1. Creates new summary rows for backfill months
2. Updates all future summary rows with backfill data
"""

import sys
sys.path.insert(0, '/home/iceberg/pipeline_fixed')
sys.path.insert(0, '/home/iceberg/test')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("v9_fixed_cascade_test") \
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
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.fixed_test")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.fixed_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.fixed_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.fixed_test.latest_summary")
    
    # Create accounts table
    spark.sql("""
        CREATE TABLE demo.fixed_test.accounts (
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
        CREATE TABLE demo.fixed_test.summary (
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
    
    # Create latest_summary table
    spark.sql("""
        CREATE TABLE demo.fixed_test.latest_summary (
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
    
    print("Tables created successfully!")


def build_array_with_values(positions_values, length=36, default="NULL"):
    """Build SQL array with specific values at positions"""
    arr = [default] * length
    for pos, val in positions_values.items():
        arr[pos] = str(val)
    return "array(" + ", ".join(arr) + ")"


def build_str_array_with_values(positions_values, length=36, default="NULL"):
    """Build SQL array of strings with specific values at positions"""
    arr = [default] * length
    for pos, val in positions_values.items():
        arr[pos] = f"'{val}'"
    return "array(" + ", ".join(arr) + ")"


def insert_test_data(spark):
    """Insert test data for cascade backfill test"""
    print("\n" + "=" * 80)
    print("INSERTING TEST DATA")
    print("=" * 80)
    
    # =========================================================================
    # Nov 2024 SUMMARY ROW (the original/oldest summary)
    # =========================================================================
    print("\n1. Nov 2024 Summary Row (original)")
    
    nov_2024_balance = build_array_with_values({0: 4000})
    nov_2024_payment = build_array_with_values({0: 400})
    nov_2024_credit = build_array_with_values({0: 12000})
    nov_2024_past_due = build_array_with_values({0: 0})
    nov_2024_dpd = build_array_with_values({0: 0})
    nov_2024_rating = build_str_array_with_values({0: '0'})
    nov_2024_asset = build_str_array_with_values({0: 'A'})
    
    spark.sql(f"""
        INSERT INTO demo.fixed_test.summary VALUES
        (8001, '2024-11', TIMESTAMP '2024-11-15 10:00:00', 
         {nov_2024_balance}, {nov_2024_payment}, {nov_2024_credit},
         {nov_2024_past_due}, {nov_2024_dpd}, {nov_2024_rating},
         {nov_2024_asset}, '0???????????????????????????????????')
    """)
    print("  - Inserted Nov 2024 summary: bal[0]=4000")
    
    # =========================================================================
    # Jan 2026, Feb 2026, Mar 2026 Summary Rows
    # =========================================================================
    print("\n2. Jan 2026 Summary Row (Nov 2024 at position 14)")
    jan_2026_balance = build_array_with_values({0: 6000, 14: 4000})
    jan_2026_payment = build_array_with_values({0: 600, 14: 400})
    jan_2026_credit = build_array_with_values({0: 15000, 14: 12000})
    jan_2026_past_due = build_array_with_values({0: 0, 14: 0})
    jan_2026_dpd = build_array_with_values({0: 0, 14: 0})
    jan_2026_rating = build_str_array_with_values({0: '0', 14: '0'})
    jan_2026_asset = build_str_array_with_values({0: 'A', 14: 'A'})
    
    spark.sql(f"""
        INSERT INTO demo.fixed_test.summary VALUES
        (8001, '2026-01', TIMESTAMP '2026-01-15 10:00:00', 
         {jan_2026_balance}, {jan_2026_payment}, {jan_2026_credit},
         {jan_2026_past_due}, {jan_2026_dpd}, {jan_2026_rating},
         {jan_2026_asset}, '0?????????????0?????????????????????')
    """)
    
    print("\n3. Feb 2026 Summary Row (Nov 2024 at position 15)")
    feb_2026_balance = build_array_with_values({0: 6500, 15: 4000})
    feb_2026_payment = build_array_with_values({0: 650, 15: 400})
    feb_2026_credit = build_array_with_values({0: 15000, 15: 12000})
    feb_2026_past_due = build_array_with_values({0: 0, 15: 0})
    feb_2026_dpd = build_array_with_values({0: 0, 15: 0})
    feb_2026_rating = build_str_array_with_values({0: '0', 15: '0'})
    feb_2026_asset = build_str_array_with_values({0: 'A', 15: 'A'})
    
    spark.sql(f"""
        INSERT INTO demo.fixed_test.summary VALUES
        (8001, '2026-02', TIMESTAMP '2026-02-15 10:00:00', 
         {feb_2026_balance}, {feb_2026_payment}, {feb_2026_credit},
         {feb_2026_past_due}, {feb_2026_dpd}, {feb_2026_rating},
         {feb_2026_asset}, '0??????????????0????????????????????')
    """)
    
    print("\n4. Mar 2026 Summary Row (LATEST - Nov 2024 at position 16)")
    mar_2026_balance = build_array_with_values({0: 7000, 16: 4000})
    mar_2026_payment = build_array_with_values({0: 700, 16: 400})
    mar_2026_credit = build_array_with_values({0: 15000, 16: 12000})
    mar_2026_past_due = build_array_with_values({0: 0, 16: 0})
    mar_2026_dpd = build_array_with_values({0: 0, 16: 0})
    mar_2026_rating = build_str_array_with_values({0: '0', 16: '0'})
    mar_2026_asset = build_str_array_with_values({0: 'A', 16: 'A'})
    
    spark.sql(f"""
        INSERT INTO demo.fixed_test.summary VALUES
        (8001, '2026-03', TIMESTAMP '2026-03-15 10:00:00', 
         {mar_2026_balance}, {mar_2026_payment}, {mar_2026_credit},
         {mar_2026_past_due}, {mar_2026_dpd}, {mar_2026_rating},
         {mar_2026_asset}, '0???????????????0???????????????????')
    """)
    
    # Insert latest_summary (Mar 2026 is latest)
    spark.sql(f"""
        INSERT INTO demo.fixed_test.latest_summary VALUES
        (8001, '2026-03', TIMESTAMP '2026-03-15 10:00:00', 
         {mar_2026_balance}, {mar_2026_payment}, {mar_2026_credit},
         {mar_2026_past_due}, {mar_2026_dpd}, {mar_2026_rating},
         {mar_2026_asset}, '0???????????????0???????????????????')
    """)
    
    # =========================================================================
    # BACKFILL DATA: Jan 2025 arriving late
    # =========================================================================
    print("\n5. Backfill Data: Jan 2025 (arriving late)")
    spark.sql("""
        INSERT INTO demo.fixed_test.accounts VALUES
        (8001, '2025-01', TIMESTAMP '2026-04-01 10:00:00', 3000, 300, 10000, 0, 0, 'A', '0')
    """)
    print("  - Inserted accounts record: Jan 2025, balance=3000")
    
    print("\n" + "-" * 60)
    print("Data counts:")
    print(f"  Accounts: {spark.sql('SELECT COUNT(*) FROM demo.fixed_test.accounts').first()[0]}")
    print(f"  Summary: {spark.sql('SELECT COUNT(*) FROM demo.fixed_test.summary').first()[0]}")
    print(f"  Latest Summary: {spark.sql('SELECT COUNT(*) FROM demo.fixed_test.latest_summary').first()[0]}")


def show_before_state(spark):
    """Show state before pipeline runs"""
    print("\n" + "=" * 80)
    print("STATE BEFORE PIPELINE")
    print("=" * 80)
    
    print("\nSummary table (4 rows):")
    spark.sql("""
        SELECT 
            cons_acct_key, rpt_as_of_mo,
            balance_history[0] as bal_0,
            balance_history[2] as bal_2,
            balance_history[12] as bal_12,
            balance_history[14] as bal_14,
            balance_history[16] as bal_16
        FROM demo.fixed_test.summary
        ORDER BY rpt_as_of_mo
    """).show(10, False)


def run_pipeline_test(spark):
    """Run the FIXED pipeline"""
    print("\n" + "=" * 80)
    print("RUNNING FIXED PIPELINE")
    print("=" * 80)
    
    from summary_pipeline import run_pipeline, PipelineConfig
    
    config = PipelineConfig()
    config.accounts_table = "demo.fixed_test.accounts"
    config.summary_table = "demo.fixed_test.summary"
    config.latest_summary_table = "demo.fixed_test.latest_summary"
    config.primary_key = "cons_acct_key"
    config.partition_key = "rpt_as_of_mo"
    config.timestamp_key = "base_ts"
    
    config.history_arrays = [
        ("balance_history", "balance_am"),
        ("payment_history", "actual_payment_am"),
        ("credit_limit_history", "credit_limit_am"),
        ("past_due_history", "past_due_am"),
        ("days_past_due_history", "days_past_due"),
        ("payment_rating_history", "payment_rating_cd"),
        ("asset_class_history", "asset_class_cd")
    ]
    
    config.grid_columns = [
        {
            "name": "payment_history_grid",
            "source_history": "payment_rating_history",
            "placeholder": "?",
            "separator": ""
        }
    ]
    
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
    
    print("\nSummary table after pipeline:")
    spark.sql("""
        SELECT 
            cons_acct_key, rpt_as_of_mo,
            balance_history[0] as bal_0,
            balance_history[2] as bal_2,
            balance_history[12] as bal_12,
            balance_history[13] as bal_13,
            balance_history[14] as bal_14,
            balance_history[15] as bal_15,
            balance_history[16] as bal_16
        FROM demo.fixed_test.summary
        ORDER BY rpt_as_of_mo
    """).show(10, False)
    
    summary_count = spark.sql("""
        SELECT COUNT(*) FROM demo.fixed_test.summary WHERE cons_acct_key = 8001
    """).first()[0]
    print(f"Total summary rows: {summary_count}")
    
    passed = 0
    failed = 0
    
    # =========================================================================
    # TEST 0: Verify 5 summary rows exist
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
    print("TEST 1: Nov 2024 Summary (unchanged)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT balance_history[0] as bal_0
        FROM demo.fixed_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2024-11'
    """).first()
    
    if result and result['bal_0'] == 4000:
        print(f"  [PASS] Nov 2024 bal[0]: {result['bal_0']} (expected 4000)")
        passed += 1
    else:
        print(f"  [FAIL] Nov 2024 bal[0]: {result} (expected 4000)")
        failed += 1
    
    # =========================================================================
    # TEST 2: Jan 2025 summary row CREATED
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 2: Jan 2025 Summary (should be CREATED)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_history[0] as bal_0,
            balance_history[2] as bal_2,
            payment_history[0] as pay_0
        FROM demo.fixed_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2025-01'
    """).first()
    
    if result:
        print("  [PASS] Jan 2025 summary row EXISTS")
        passed += 1
        
        if result['bal_0'] == 3000:
            print(f"  [PASS] Jan 2025 bal[0]: {result['bal_0']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 bal[0]: {result['bal_0']} (expected 3000)")
            failed += 1
        
        if result['bal_2'] == 4000:
            print(f"  [PASS] Jan 2025 bal[2] (Nov 2024): {result['bal_2']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 bal[2]: {result['bal_2']} (expected 4000)")
            failed += 1
        
        if result['pay_0'] == 300:
            print(f"  [PASS] Jan 2025 pay[0]: {result['pay_0']} (expected 300)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 pay[0]: {result['pay_0']} (expected 300)")
            failed += 1
    else:
        print("  [FAIL] Jan 2025 summary row NOT FOUND!")
        failed += 4
    
    # =========================================================================
    # TEST 3: Jan 2026 updated
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 3: Jan 2026 Summary (updated with Jan 2025 at pos 12)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT balance_history[12] as bal_12, balance_history[14] as bal_14
        FROM demo.fixed_test.summary 
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
    # TEST 4: Feb 2026 updated
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 4: Feb 2026 Summary (updated with Jan 2025 at pos 13)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT balance_history[13] as bal_13, balance_history[15] as bal_15
        FROM demo.fixed_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2026-02'
    """).first()
    
    if result:
        if result['bal_13'] == 3000:
            print(f"  [PASS] Jan 2025 at pos 13: {result['bal_13']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 at pos 13: {result['bal_13']} (expected 3000)")
            failed += 1
        
        if result['bal_15'] == 4000:
            print(f"  [PASS] Nov 2024 at pos 15: {result['bal_15']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2024 at pos 15: {result['bal_15']} (expected 4000)")
            failed += 1
    
    # =========================================================================
    # TEST 5: Mar 2026 updated
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 5: Mar 2026 Summary (updated with Jan 2025 at pos 14)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT balance_history[14] as bal_14, balance_history[16] as bal_16
        FROM demo.fixed_test.summary 
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
    print(f"FIXED PIPELINE TEST: {passed} passed, {failed} failed")
    print("=" * 80)
    
    if failed == 0:
        print("\nFIX VERIFIED:")
        print("  - Jan 2025 summary row CREATED with correct data")
        print("  - Jan 2025 inherited Nov 2024 data at position 2")
        print("  - All future summaries UPDATED correctly")
        print("  - Nov 2024 data PRESERVED in all summaries")
    
    return passed, failed


def main():
    print("=" * 80)
    print("FIXED PIPELINE v9 - CASCADE BACKFILL TEST")
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
                print("\n*** ALL TESTS PASSED - FIX VERIFIED ***")
                return 0
            else:
                print(f"\n*** {failed} TESTS FAILED ***")
                return 1
        else:
            print("\n*** PIPELINE FAILED ***")
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
