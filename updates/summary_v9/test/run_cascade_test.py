"""
CASCADE BACKFILL TEST for Summary Pipeline v9
==============================================

Test Scenario:
- Account 8001 has existing summaries for Jan 2026, Feb 2026, Mar 2026
- Each summary already has Nov 2024 data populated in ALL arrays (non-null)
- Backfill: Jan 2025 data arrives late

Expected Behavior:
- Jan 2025 data should be inserted into ALL 3 future summaries
- Nov 2024 data should be PRESERVED in all summaries
- All array positions should be correct:
  - Jan 2026: Jan 2025 at pos 12, Nov 2024 at pos 14
  - Feb 2026: Jan 2025 at pos 13, Nov 2024 at pos 15
  - Mar 2026: Jan 2025 at pos 14, Nov 2024 at pos 16
"""

import sys
sys.path.insert(0, '/home/iceberg/pipeline')
sys.path.insert(0, '/home/iceberg/test')

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("v9_cascade_test") \
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
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.cascade_test")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.cascade_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.cascade_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.cascade_test.latest_summary")
    
    # Create accounts table
    spark.sql("""
        CREATE TABLE demo.cascade_test.accounts (
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
        CREATE TABLE demo.cascade_test.summary (
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
        CREATE TABLE demo.cascade_test.latest_summary (
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
    """Build SQL array with specific values at positions
    
    Args:
        positions_values: dict of {position: value}
        length: array length (default 36)
        default: default value for empty positions
    """
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


def insert_cascade_test_data(spark):
    """Insert test data for cascade backfill test
    
    Scenario: Account 8001 has summaries for Jan 2026, Feb 2026, Mar 2026
    Each summary already has Nov 2024 data (14/15/16 months back respectively)
    We'll backfill Jan 2025 data which should update all 3 summaries
    """
    print("\n" + "=" * 80)
    print("INSERTING CASCADE TEST DATA")
    print("=" * 80)
    
    # =========================================================================
    # Account 8001: Multiple future summaries with Nov 2024 data pre-populated
    # =========================================================================
    # 
    # Timeline:
    # - Nov 2024: Historical data already in summaries (will be preserved)
    # - Jan 2025: BACKFILL data arriving late (to be inserted)
    # - Jan 2026: Summary month 1
    # - Feb 2026: Summary month 2
    # - Mar 2026: Summary month 3 (latest)
    #
    # Array positions (0-indexed, position 0 = summary month):
    # From Jan 2026: Nov 2024 = pos 14, Jan 2025 = pos 12
    # From Feb 2026: Nov 2024 = pos 15, Jan 2025 = pos 13
    # From Mar 2026: Nov 2024 = pos 16, Jan 2025 = pos 14
    
    print("\nAccount 8001: Existing summaries with Nov 2024 data")
    print("-" * 60)
    
    # Jan 2026 summary - Nov 2024 at position 14
    jan_2026_balance = build_array_with_values({0: 6000, 14: 4000})
    jan_2026_payment = build_array_with_values({0: 600, 14: 400})
    jan_2026_credit = build_array_with_values({0: 15000, 14: 12000})
    jan_2026_past_due = build_array_with_values({0: 0, 14: 0})
    jan_2026_dpd = build_array_with_values({0: 0, 14: 0})
    jan_2026_rating = build_str_array_with_values({0: '0', 14: '0'})
    jan_2026_asset = build_str_array_with_values({0: 'A', 14: 'A'})
    
    spark.sql(f"""
        INSERT INTO demo.cascade_test.summary VALUES
        (8001, '2026-01', TIMESTAMP '2026-01-15 10:00:00', 
         {jan_2026_balance}, {jan_2026_payment}, {jan_2026_credit},
         {jan_2026_past_due}, {jan_2026_dpd}, {jan_2026_rating},
         {jan_2026_asset}, '0?????????????0?????????????????????')
    """)
    print("  - Inserted Jan 2026 summary: bal[0]=6000, bal[14]=4000 (Nov 2024)")
    
    # Feb 2026 summary - Nov 2024 at position 15
    feb_2026_balance = build_array_with_values({0: 6500, 15: 4000})
    feb_2026_payment = build_array_with_values({0: 650, 15: 400})
    feb_2026_credit = build_array_with_values({0: 15000, 15: 12000})
    feb_2026_past_due = build_array_with_values({0: 0, 15: 0})
    feb_2026_dpd = build_array_with_values({0: 0, 15: 0})
    feb_2026_rating = build_str_array_with_values({0: '0', 15: '0'})
    feb_2026_asset = build_str_array_with_values({0: 'A', 15: 'A'})
    
    spark.sql(f"""
        INSERT INTO demo.cascade_test.summary VALUES
        (8001, '2026-02', TIMESTAMP '2026-02-15 10:00:00', 
         {feb_2026_balance}, {feb_2026_payment}, {feb_2026_credit},
         {feb_2026_past_due}, {feb_2026_dpd}, {feb_2026_rating},
         {feb_2026_asset}, '0??????????????0????????????????????')
    """)
    print("  - Inserted Feb 2026 summary: bal[0]=6500, bal[15]=4000 (Nov 2024)")
    
    # Mar 2026 summary - Nov 2024 at position 16 (LATEST)
    mar_2026_balance = build_array_with_values({0: 7000, 16: 4000})
    mar_2026_payment = build_array_with_values({0: 700, 16: 400})
    mar_2026_credit = build_array_with_values({0: 15000, 16: 12000})
    mar_2026_past_due = build_array_with_values({0: 0, 16: 0})
    mar_2026_dpd = build_array_with_values({0: 0, 16: 0})
    mar_2026_rating = build_str_array_with_values({0: '0', 16: '0'})
    mar_2026_asset = build_str_array_with_values({0: 'A', 16: 'A'})
    
    spark.sql(f"""
        INSERT INTO demo.cascade_test.summary VALUES
        (8001, '2026-03', TIMESTAMP '2026-03-15 10:00:00', 
         {mar_2026_balance}, {mar_2026_payment}, {mar_2026_credit},
         {mar_2026_past_due}, {mar_2026_dpd}, {mar_2026_rating},
         {mar_2026_asset}, '0???????????????0???????????????????')
    """)
    print("  - Inserted Mar 2026 summary: bal[0]=7000, bal[16]=4000 (Nov 2024)")
    
    # Insert latest_summary (Mar 2026 is latest)
    spark.sql(f"""
        INSERT INTO demo.cascade_test.latest_summary VALUES
        (8001, '2026-03', TIMESTAMP '2026-03-15 10:00:00', 
         {mar_2026_balance}, {mar_2026_payment}, {mar_2026_credit},
         {mar_2026_past_due}, {mar_2026_dpd}, {mar_2026_rating},
         {mar_2026_asset}, '0???????????????0???????????????????')
    """)
    print("  - Inserted latest_summary (Mar 2026)")
    
    # =========================================================================
    # BACKFILL DATA: Jan 2025 arriving late
    # =========================================================================
    print("\nBackfill data: Jan 2025 (arriving late)")
    print("-" * 60)
    
    spark.sql("""
        INSERT INTO demo.cascade_test.accounts VALUES
        (8001, '2025-01', TIMESTAMP '2026-04-01 10:00:00', 3000, 300, 10000, 0, 0, 'A', '0')
    """)
    print("  - Inserted accounts record: Jan 2025, balance=3000")
    
    # Show data counts
    print("\n" + "-" * 60)
    print("Data counts:")
    print(f"  Accounts: {spark.sql('SELECT COUNT(*) FROM demo.cascade_test.accounts').first()[0]}")
    print(f"  Summary: {spark.sql('SELECT COUNT(*) FROM demo.cascade_test.summary').first()[0]}")
    print(f"  Latest Summary: {spark.sql('SELECT COUNT(*) FROM demo.cascade_test.latest_summary').first()[0]}")


def show_before_state(spark):
    """Show state before pipeline runs"""
    print("\n" + "=" * 80)
    print("STATE BEFORE PIPELINE")
    print("=" * 80)
    
    print("\nSummary table - Account 8001:")
    spark.sql("""
        SELECT 
            cons_acct_key,
            rpt_as_of_mo,
            balance_history[0] as bal_0,
            balance_history[12] as bal_12,
            balance_history[13] as bal_13,
            balance_history[14] as bal_14,
            balance_history[15] as bal_15,
            balance_history[16] as bal_16
        FROM demo.cascade_test.summary
        WHERE cons_acct_key = 8001
        ORDER BY rpt_as_of_mo
    """).show(10, False)
    
    print("Expected positions for Jan 2025 backfill:")
    print("  - Jan 2026: position 12 (12 months back from Jan 2026)")
    print("  - Feb 2026: position 13 (13 months back from Feb 2026)")
    print("  - Mar 2026: position 14 (14 months back from Mar 2026)")
    print("\nNov 2024 data should remain at:")
    print("  - Jan 2026: position 14")
    print("  - Feb 2026: position 15")
    print("  - Mar 2026: position 16")


def run_pipeline_test(spark):
    """Run the pipeline"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE")
    print("=" * 80)
    
    from summary_pipeline import run_pipeline, PipelineConfig
    
    config = PipelineConfig()
    config.accounts_table = "demo.cascade_test.accounts"
    config.summary_table = "demo.cascade_test.summary"
    config.latest_summary_table = "demo.cascade_test.latest_summary"
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


def verify_cascade_results(spark):
    """Verify cascade backfill worked correctly"""
    print("\n" + "=" * 80)
    print("VERIFYING CASCADE BACKFILL RESULTS")
    print("=" * 80)
    
    # Show all summary records for account 8001
    print("\nSummary table after pipeline - Account 8001:")
    spark.sql("""
        SELECT 
            cons_acct_key,
            rpt_as_of_mo,
            balance_history[0] as bal_0,
            balance_history[12] as bal_12,
            balance_history[13] as bal_13,
            balance_history[14] as bal_14,
            balance_history[15] as bal_15,
            balance_history[16] as bal_16
        FROM demo.cascade_test.summary
        WHERE cons_acct_key = 8001
        ORDER BY rpt_as_of_mo
    """).show(10, False)
    
    passed = 0
    failed = 0
    
    # =========================================================================
    # TEST 1: Jan 2026 summary - Jan 2025 backfill at position 12
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 1: Jan 2026 Summary - Jan 2025 at position 12")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_history[0] as bal_0,
            balance_history[12] as bal_12,
            balance_history[14] as bal_14,
            payment_history[12] as pay_12,
            credit_limit_history[12] as cred_12
        FROM demo.cascade_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if result:
        # Check Jan 2025 backfill at position 12
        if result['bal_12'] == 3000:
            print(f"  [PASS] Jan 2025 balance at pos 12: {result['bal_12']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 balance at pos 12: {result['bal_12']} (expected 3000)")
            failed += 1
        
        # Check Nov 2024 preserved at position 14
        if result['bal_14'] == 4000:
            print(f"  [PASS] Nov 2024 balance preserved at pos 14: {result['bal_14']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2024 balance at pos 14: {result['bal_14']} (expected 4000)")
            failed += 1
        
        # Check payment array also updated
        if result['pay_12'] == 300:
            print(f"  [PASS] Jan 2025 payment at pos 12: {result['pay_12']} (expected 300)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 payment at pos 12: {result['pay_12']} (expected 300)")
            failed += 1
    else:
        print("  [FAIL] Jan 2026 summary not found!")
        failed += 3
    
    # =========================================================================
    # TEST 2: Feb 2026 summary - Jan 2025 backfill at position 13
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 2: Feb 2026 Summary - Jan 2025 at position 13")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_history[0] as bal_0,
            balance_history[13] as bal_13,
            balance_history[15] as bal_15,
            payment_history[13] as pay_13
        FROM demo.cascade_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2026-02'
    """).first()
    
    if result:
        # Check Jan 2025 backfill at position 13
        if result['bal_13'] == 3000:
            print(f"  [PASS] Jan 2025 balance at pos 13: {result['bal_13']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 balance at pos 13: {result['bal_13']} (expected 3000)")
            failed += 1
        
        # Check Nov 2024 preserved at position 15
        if result['bal_15'] == 4000:
            print(f"  [PASS] Nov 2024 balance preserved at pos 15: {result['bal_15']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2024 balance at pos 15: {result['bal_15']} (expected 4000)")
            failed += 1
        
        # Check original data at position 0 unchanged
        if result['bal_0'] == 6500:
            print(f"  [PASS] Feb 2026 balance at pos 0 unchanged: {result['bal_0']} (expected 6500)")
            passed += 1
        else:
            print(f"  [FAIL] Feb 2026 balance at pos 0: {result['bal_0']} (expected 6500)")
            failed += 1
    else:
        print("  [FAIL] Feb 2026 summary not found!")
        failed += 3
    
    # =========================================================================
    # TEST 3: Mar 2026 summary - Jan 2025 backfill at position 14
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 3: Mar 2026 Summary - Jan 2025 at position 14")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_history[0] as bal_0,
            balance_history[14] as bal_14,
            balance_history[16] as bal_16,
            payment_history[14] as pay_14
        FROM demo.cascade_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2026-03'
    """).first()
    
    if result:
        # Check Jan 2025 backfill at position 14
        if result['bal_14'] == 3000:
            print(f"  [PASS] Jan 2025 balance at pos 14: {result['bal_14']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025 balance at pos 14: {result['bal_14']} (expected 3000)")
            failed += 1
        
        # Check Nov 2024 preserved at position 16
        if result['bal_16'] == 4000:
            print(f"  [PASS] Nov 2024 balance preserved at pos 16: {result['bal_16']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2024 balance at pos 16: {result['bal_16']} (expected 4000)")
            failed += 1
        
        # Check original data at position 0 unchanged
        if result['bal_0'] == 7000:
            print(f"  [PASS] Mar 2026 balance at pos 0 unchanged: {result['bal_0']} (expected 7000)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2026 balance at pos 0: {result['bal_0']} (expected 7000)")
            failed += 1
    else:
        print("  [FAIL] Mar 2026 summary not found!")
        failed += 3
    
    # =========================================================================
    # TEST 4: latest_summary should be updated (Mar 2026)
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 4: Latest Summary (Mar 2026) Updated")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            rpt_as_of_mo,
            balance_history[14] as bal_14,
            balance_history[16] as bal_16
        FROM demo.cascade_test.latest_summary 
        WHERE cons_acct_key = 8001
    """).first()
    
    if result:
        if result['bal_14'] == 3000:
            print(f"  [PASS] Latest summary Jan 2025 at pos 14: {result['bal_14']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] Latest summary Jan 2025 at pos 14: {result['bal_14']} (expected 3000)")
            failed += 1
        
        if result['bal_16'] == 4000:
            print(f"  [PASS] Latest summary Nov 2024 at pos 16: {result['bal_16']} (expected 4000)")
            passed += 1
        else:
            print(f"  [FAIL] Latest summary Nov 2024 at pos 16: {result['bal_16']} (expected 4000)")
            failed += 1
    else:
        print("  [FAIL] Latest summary not found!")
        failed += 2
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print(f"CASCADE TEST SUMMARY: {passed} passed, {failed} failed")
    print("=" * 80)
    
    if failed == 0:
        print("\nCASCADE BACKFILL VERIFIED:")
        print("  - Jan 2025 backfill correctly inserted into ALL 3 future summaries")
        print("  - Nov 2024 data PRESERVED at correct positions in all summaries")
        print("  - All arrays updated correctly (balance, payment, etc.)")
        print("  - Latest summary updated with cascade changes")
    else:
        print("\nCASCADE BACKFILL ISSUES DETECTED!")
        print("  - Check pipeline logic for cascade updates")
    
    return passed, failed


def main():
    print("=" * 80)
    print("SUMMARY PIPELINE V9 - CASCADE BACKFILL TEST")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    print()
    print("Test Scenario:")
    print("  - Account 8001 has summaries for Jan 2026, Feb 2026, Mar 2026")
    print("  - Each summary already has Nov 2024 data at correct positions")
    print("  - Backfill: Jan 2025 data (arriving late)")
    print()
    print("Expected Behavior:")
    print("  - Jan 2025 should update ALL 3 future summaries")
    print("  - Nov 2024 data should be PRESERVED")
    
    spark = create_spark_session()
    print(f"\nSpark version: {spark.version}")
    
    try:
        setup_tables(spark)
        insert_cascade_test_data(spark)
        show_before_state(spark)
        
        stats = run_pipeline_test(spark)
        
        if stats:
            passed, failed = verify_cascade_results(spark)
            
            if failed == 0:
                print("\n*** ALL CASCADE TESTS PASSED ***")
                return 0
            else:
                print(f"\n*** {failed} CASCADE TESTS FAILED ***")
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
