"""
BULK HISTORICAL LOAD TEST - Case IV Scenario
==============================================

Tests the scenario where a NEW account (no existing summary) uploads
72 months of historical data in a single batch.

Expected Behavior:
------------------
1. 72 summary rows should be created (one per month)
2. Each row should have a 36-element rolling history array
3. Arrays should be correctly populated based on available historical data:
   
   Example for account uploading Jan 2020 - Dec 2025 (72 months):
   
   | Month    | bal[0]   | bal[1]   | bal[2]   | ... | bal[35]  |
   |----------|----------|----------|----------|-----|----------|
   | 2020-01  | Jan 2020 | NULL     | NULL     | ... | NULL     |
   | 2020-02  | Feb 2020 | Jan 2020 | NULL     | ... | NULL     |
   | 2020-03  | Mar 2020 | Feb 2020 | Jan 2020 | ... | NULL     |
   | ...      | ...      | ...      | ...      | ... | ...      |
   | 2022-12  | Dec 2022 | Nov 2022 | Oct 2022 | ... | Jan 2020 | (36th month - full array)
   | 2023-01  | Jan 2023 | Dec 2022 | Nov 2022 | ... | Feb 2020 | (Jan 2020 falls off)
   | ...      | ...      | ...      | ...      | ... | ...      |
   | 2025-12  | Dec 2025 | Nov 2025 | Oct 2025 | ... | Jan 2023 | (72nd month)

Current Pipeline Concern:
-------------------------
The current pipeline classifies records as:
- Case I: New accounts (no summary exists for this account_key)
- Case II: Forward entries (summary exists, new month > max existing month)
- Case III: Backfill (summary exists, new month < max existing month)

For a brand new account with 72 months of data uploaded at once:
- ALL records might be classified as Case I (since no summary exists initially)
- Case I only creates arrays with position 0 filled
- This would result in INCORRECT arrays - no rolling history!

The solution might require:
- Case IV: "Bulk Historical Load" - process multiple months for new account
- OR: Process within the batch (first month = Case I, rest = Case II relative to batch)

Usage:
    docker exec spark-iceberg python3 /home/iceberg/tests/test_bulk_historical_load.py
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
from dateutil.relativedelta import relativedelta


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("bulk_historical_load_test") \
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
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.bulk_test")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.bulk_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.bulk_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.bulk_test.latest_summary")
    
    # Create accounts table (source)
    spark.sql("""
        CREATE TABLE demo.bulk_test.accounts (
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
        CREATE TABLE demo.bulk_test.summary (
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
        CREATE TABLE demo.bulk_test.latest_summary (
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


def insert_bulk_historical_data(spark, num_months=72):
    """
    Insert 72 months of historical data for a NEW account.
    
    Account 9001 - brand new, never seen before
    Data spans: Jan 2020 - Dec 2025 (72 months)
    All uploaded today (same base_ts)
    """
    print("\n" + "=" * 80)
    print(f"INSERTING {num_months} MONTHS OF HISTORICAL DATA FOR NEW ACCOUNT")
    print("=" * 80)
    
    # Start date: Jan 2020
    start_date = datetime(2020, 1, 1)
    upload_timestamp = datetime(2026, 1, 30, 10, 0, 0)  # Today's upload time
    
    account_key = 9001
    base_balance = 10000
    
    print(f"\nAccount: {account_key}")
    print(f"Data range: {start_date.strftime('%Y-%m')} to {(start_date + relativedelta(months=num_months-1)).strftime('%Y-%m')}")
    print(f"Upload timestamp: {upload_timestamp}")
    
    # Generate INSERT statements for all 72 months
    values = []
    for i in range(num_months):
        month_date = start_date + relativedelta(months=i)
        rpt_as_of_mo = month_date.strftime('%Y-%m')
        
        # Simulate varying balances (decreasing over time - paying off loan)
        balance = base_balance - (i * 100)  # Decreases by 100 each month
        payment = 200 + (i % 12) * 10  # Varies by month of year
        credit_limit = 15000
        past_due = 0 if i % 6 != 5 else 50  # Occasional late payment every 6 months
        dpd = 0 if past_due == 0 else 15
        asset_class = 'A' if dpd == 0 else 'B'
        payment_rating = '0' if dpd == 0 else '1'
        
        values.append(f"""
            ({account_key}, '{rpt_as_of_mo}', TIMESTAMP '{upload_timestamp}',
             {balance}, {payment}, {credit_limit}, {past_due}, {dpd}, 
             '{asset_class}', '{payment_rating}')
        """)
    
    # Insert all records
    insert_sql = f"INSERT INTO demo.bulk_test.accounts VALUES {','.join(values)}"
    spark.sql(insert_sql)
    
    # Show sample of inserted data
    print("\nSample of inserted data (first 5 and last 5 months):")
    spark.sql("""
        SELECT cons_acct_key, rpt_as_of_mo, balance_am, actual_payment_am, days_past_due
        FROM demo.bulk_test.accounts
        ORDER BY rpt_as_of_mo
    """).show(5, False)
    
    print("... (middle months omitted) ...")
    
    spark.sql("""
        SELECT cons_acct_key, rpt_as_of_mo, balance_am, actual_payment_am, days_past_due
        FROM demo.bulk_test.accounts
        ORDER BY rpt_as_of_mo DESC
    """).show(5, False)
    
    count = spark.sql("SELECT COUNT(*) FROM demo.bulk_test.accounts").first()[0]
    print(f"\nTotal records inserted: {count}")
    
    # Verify summary is empty (new account)
    summary_count = spark.sql("SELECT COUNT(*) FROM demo.bulk_test.summary").first()[0]
    print(f"Existing summary rows: {summary_count} (should be 0 - new account)")


def run_pipeline_test(spark):
    """Run the pipeline and capture results"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE v9.1")
    print("=" * 80)
    
    try:
        from summary_pipeline import run_pipeline, PipelineConfig
    except ImportError as e:
        print(f"Could not import pipeline: {e}")
        print("Make sure summary_pipeline.py is in the path")
        return None
    
    # Create config for test tables
    config = PipelineConfig()
    config.accounts_table = "demo.bulk_test.accounts"
    config.summary_table = "demo.bulk_test.summary"
    config.latest_summary_table = "demo.bulk_test.latest_summary"
    config.primary_key = "cons_acct_key"
    config.partition_key = "rpt_as_of_mo"
    config.timestamp_key = "base_ts"
    config.history_length = 36
    
    # Configure rolling columns
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


def verify_results(spark, num_months=72):
    """Verify the bulk historical load was processed correctly"""
    print("\n" + "=" * 80)
    print("VERIFYING BULK HISTORICAL LOAD RESULTS")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # =========================================================================
    # TEST 1: Verify correct number of summary rows created
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 1: Summary Row Count")
    print("-" * 60)
    
    summary_count = spark.sql("""
        SELECT COUNT(*) FROM demo.bulk_test.summary WHERE cons_acct_key = 9001
    """).first()[0]
    
    if summary_count == num_months:
        print(f"  [PASS] Expected {num_months} rows, got {summary_count}")
        passed += 1
    else:
        print(f"  [FAIL] Expected {num_months} rows, got {summary_count}")
        failed += 1
    
    # =========================================================================
    # TEST 2: Verify first month (Jan 2020) - should have only position 0 filled
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 2: First Month (2020-01) - Only position 0 should have data")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[35] as bal_35
        FROM demo.bulk_test.summary 
        WHERE cons_acct_key = 9001 AND rpt_as_of_mo = '2020-01'
    """).first()
    
    if result:
        # Position 0 should have Jan 2020 balance (10000)
        if result['bal_0'] == 10000:
            print(f"  [PASS] 2020-01 bal[0]: {result['bal_0']} (expected 10000)")
            passed += 1
        else:
            print(f"  [FAIL] 2020-01 bal[0]: {result['bal_0']} (expected 10000)")
            failed += 1
        
        # Position 1 should be NULL (no prior data)
        if result['bal_1'] is None:
            print(f"  [PASS] 2020-01 bal[1]: NULL (expected NULL - no prior data)")
            passed += 1
        else:
            print(f"  [FAIL] 2020-01 bal[1]: {result['bal_1']} (expected NULL)")
            failed += 1
    else:
        print("  [FAIL] 2020-01 summary row NOT FOUND!")
        failed += 3
    
    # =========================================================================
    # TEST 3: Verify second month (Feb 2020) - should have positions 0 and 1 filled
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 3: Second Month (2020-02) - Positions 0,1 should have data")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[2] as bal_2
        FROM demo.bulk_test.summary 
        WHERE cons_acct_key = 9001 AND rpt_as_of_mo = '2020-02'
    """).first()
    
    if result:
        # Position 0 should have Feb 2020 balance (9900)
        if result['bal_0'] == 9900:
            print(f"  [PASS] 2020-02 bal[0]: {result['bal_0']} (expected 9900 - Feb 2020)")
            passed += 1
        else:
            print(f"  [FAIL] 2020-02 bal[0]: {result['bal_0']} (expected 9900)")
            failed += 1
        
        # Position 1 should have Jan 2020 balance (10000)
        if result['bal_1'] == 10000:
            print(f"  [PASS] 2020-02 bal[1]: {result['bal_1']} (expected 10000 - Jan 2020)")
            passed += 1
        else:
            print(f"  [FAIL] 2020-02 bal[1]: {result['bal_1']} (expected 10000)")
            failed += 1
        
        # Position 2 should be NULL
        if result['bal_2'] is None:
            print(f"  [PASS] 2020-02 bal[2]: NULL (expected NULL)")
            passed += 1
        else:
            print(f"  [FAIL] 2020-02 bal[2]: {result['bal_2']} (expected NULL)")
            failed += 1
    else:
        print("  [FAIL] 2020-02 summary row NOT FOUND!")
        failed += 3
    
    # =========================================================================
    # TEST 4: Verify 36th month (Dec 2022) - should have full array
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 4: 36th Month (2022-12) - Full array should be populated")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[35] as bal_35
        FROM demo.bulk_test.summary 
        WHERE cons_acct_key = 9001 AND rpt_as_of_mo = '2022-12'
    """).first()
    
    if result:
        # Position 0: Dec 2022 (month 36, balance = 10000 - 35*100 = 6500)
        expected_bal_0 = 10000 - 35 * 100  # 6500
        if result['bal_0'] == expected_bal_0:
            print(f"  [PASS] 2022-12 bal[0]: {result['bal_0']} (expected {expected_bal_0} - Dec 2022)")
            passed += 1
        else:
            print(f"  [FAIL] 2022-12 bal[0]: {result['bal_0']} (expected {expected_bal_0})")
            failed += 1
        
        # Position 1: Nov 2022 (month 35, balance = 10000 - 34*100 = 6600)
        expected_bal_1 = 10000 - 34 * 100  # 6600
        if result['bal_1'] == expected_bal_1:
            print(f"  [PASS] 2022-12 bal[1]: {result['bal_1']} (expected {expected_bal_1} - Nov 2022)")
            passed += 1
        else:
            print(f"  [FAIL] 2022-12 bal[1]: {result['bal_1']} (expected {expected_bal_1})")
            failed += 1
        
        # Position 35: Jan 2020 (month 1, balance = 10000)
        if result['bal_35'] == 10000:
            print(f"  [PASS] 2022-12 bal[35]: {result['bal_35']} (expected 10000 - Jan 2020)")
            passed += 1
        else:
            print(f"  [FAIL] 2022-12 bal[35]: {result['bal_35']} (expected 10000)")
            failed += 1
    else:
        print("  [FAIL] 2022-12 summary row NOT FOUND!")
        failed += 3
    
    # =========================================================================
    # TEST 5: Verify 37th month (Jan 2023) - Jan 2020 should fall off
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 5: 37th Month (2023-01) - Jan 2020 should fall off (rolling window)")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[0] as bal_0,
            balance_am_history[35] as bal_35
        FROM demo.bulk_test.summary 
        WHERE cons_acct_key = 9001 AND rpt_as_of_mo = '2023-01'
    """).first()
    
    if result:
        # Position 0: Jan 2023 (month 37, balance = 10000 - 36*100 = 6400)
        expected_bal_0 = 10000 - 36 * 100  # 6400
        if result['bal_0'] == expected_bal_0:
            print(f"  [PASS] 2023-01 bal[0]: {result['bal_0']} (expected {expected_bal_0} - Jan 2023)")
            passed += 1
        else:
            print(f"  [FAIL] 2023-01 bal[0]: {result['bal_0']} (expected {expected_bal_0})")
            failed += 1
        
        # Position 35: Feb 2020 (month 2, balance = 9900) - Jan 2020 fell off!
        if result['bal_35'] == 9900:
            print(f"  [PASS] 2023-01 bal[35]: {result['bal_35']} (expected 9900 - Feb 2020, Jan 2020 fell off)")
            passed += 1
        else:
            print(f"  [FAIL] 2023-01 bal[35]: {result['bal_35']} (expected 9900 - Feb 2020)")
            failed += 1
    else:
        print("  [FAIL] 2023-01 summary row NOT FOUND!")
        failed += 2
    
    # =========================================================================
    # TEST 6: Verify last month (Dec 2025) - should have correct rolling window
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 6: Last Month (2025-12) - Correct rolling window")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT 
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[35] as bal_35
        FROM demo.bulk_test.summary 
        WHERE cons_acct_key = 9001 AND rpt_as_of_mo = '2025-12'
    """).first()
    
    if result:
        # Position 0: Dec 2025 (month 72, balance = 10000 - 71*100 = 2900)
        expected_bal_0 = 10000 - 71 * 100  # 2900
        if result['bal_0'] == expected_bal_0:
            print(f"  [PASS] 2025-12 bal[0]: {result['bal_0']} (expected {expected_bal_0} - Dec 2025)")
            passed += 1
        else:
            print(f"  [FAIL] 2025-12 bal[0]: {result['bal_0']} (expected {expected_bal_0})")
            failed += 1
        
        # Position 1: Nov 2025 (month 71, balance = 10000 - 70*100 = 3000)
        expected_bal_1 = 10000 - 70 * 100  # 3000
        if result['bal_1'] == expected_bal_1:
            print(f"  [PASS] 2025-12 bal[1]: {result['bal_1']} (expected {expected_bal_1} - Nov 2025)")
            passed += 1
        else:
            print(f"  [FAIL] 2025-12 bal[1]: {result['bal_1']} (expected {expected_bal_1})")
            failed += 1
        
        # Position 35: Jan 2023 (month 37, balance = 10000 - 36*100 = 6400)
        expected_bal_35 = 10000 - 36 * 100  # 6400
        if result['bal_35'] == expected_bal_35:
            print(f"  [PASS] 2025-12 bal[35]: {result['bal_35']} (expected {expected_bal_35} - Jan 2023)")
            passed += 1
        else:
            print(f"  [FAIL] 2025-12 bal[35]: {result['bal_35']} (expected {expected_bal_35})")
            failed += 1
    else:
        print("  [FAIL] 2025-12 summary row NOT FOUND!")
        failed += 3
    
    # =========================================================================
    # TEST 7: Verify latest_summary has the most recent month
    # =========================================================================
    print("\n" + "-" * 60)
    print("TEST 7: Latest Summary Table")
    print("-" * 60)
    
    result = spark.sql("""
        SELECT rpt_as_of_mo, balance_am_history[0] as bal_0
        FROM demo.bulk_test.latest_summary 
        WHERE cons_acct_key = 9001
    """).first()
    
    if result and result['rpt_as_of_mo'] == '2025-12':
        print(f"  [PASS] Latest summary is 2025-12 with bal[0]={result['bal_0']}")
        passed += 1
    else:
        print(f"  [FAIL] Latest summary: {result}")
        failed += 1
    
    # =========================================================================
    # Show sample of summary data
    # =========================================================================
    print("\n" + "-" * 60)
    print("SAMPLE SUMMARY DATA")
    print("-" * 60)
    
    print("\nFirst 5 months:")
    spark.sql("""
        SELECT 
            rpt_as_of_mo,
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[2] as bal_2,
            balance_am_history[35] as bal_35
        FROM demo.bulk_test.summary
        WHERE cons_acct_key = 9001
        ORDER BY rpt_as_of_mo
        LIMIT 5
    """).show(5, False)
    
    print("Months around the 36th (when array fills up):")
    spark.sql("""
        SELECT 
            rpt_as_of_mo,
            balance_am_history[0] as bal_0,
            balance_am_history[34] as bal_34,
            balance_am_history[35] as bal_35
        FROM demo.bulk_test.summary
        WHERE cons_acct_key = 9001
        AND rpt_as_of_mo IN ('2022-11', '2022-12', '2023-01', '2023-02')
        ORDER BY rpt_as_of_mo
    """).show(10, False)
    
    print("Last 5 months:")
    spark.sql("""
        SELECT 
            rpt_as_of_mo,
            balance_am_history[0] as bal_0,
            balance_am_history[1] as bal_1,
            balance_am_history[35] as bal_35
        FROM demo.bulk_test.summary
        WHERE cons_acct_key = 9001
        ORDER BY rpt_as_of_mo DESC
        LIMIT 5
    """).show(5, False)
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 80)
    print(f"TEST RESULTS: {passed} passed, {failed} failed")
    print("=" * 80)
    
    if failed == 0:
        print("\nBULK HISTORICAL LOAD WORKS CORRECTLY:")
        print("  1. All 72 summary rows created")
        print("  2. Rolling arrays correctly populated")
        print("  3. Data correctly falls off after 36 months")
        print("  4. Latest summary correctly set")
    else:
        print("\nBULK HISTORICAL LOAD HAS ISSUES:")
        print("  The pipeline may need a 'Case IV' to handle this scenario")
        print("  See test output above for details")
    
    return passed, failed


def main():
    print("=" * 80)
    print("BULK HISTORICAL LOAD TEST - Case IV Scenario")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    print("\nScenario: New account uploads 72 months of historical data at once")
    
    spark = create_spark_session()
    print(f"\nSpark version: {spark.version}")
    
    try:
        setup_tables(spark)
        insert_bulk_historical_data(spark, num_months=72)
        
        stats = run_pipeline_test(spark)
        
        if stats:
            passed, failed = verify_results(spark, num_months=72)
            
            if failed == 0:
                print("\n" + "=" * 80)
                print("*** ALL TESTS PASSED - BULK HISTORICAL LOAD WORKS ***")
                print("=" * 80)
                return 0
            else:
                print(f"\n*** {failed} TESTS FAILED - CASE IV MAY BE NEEDED ***")
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
