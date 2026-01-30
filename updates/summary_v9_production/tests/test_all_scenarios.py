"""
COMPREHENSIVE TEST - All 4 Cases + Gap Handling
================================================

Tests ALL scenarios in a single run:
- Case I:   New account with single month
- Case II:  Forward entry (existing account, new month)
- Case III: Backfill (existing account, late-arriving historical data)
- Case IV:  Bulk historical load (new account with multiple months)
- GAPS:     Non-reporting months in historical data

Accounts:
- 1001: Case I   - Brand new account, single month (Jan 2026)
- 2001: Case II  - Existing account, forward entry (Feb 2026 -> Mar 2026)
- 3001: Case III - Existing account, backfill (has Jan 2026, receives Nov 2025 late)
- 4001: Case IV  - New account, 12 months bulk upload (Jan 2025 - Dec 2025)
- 5001: Case IV + GAPS - New account, 12 months with gaps (missing Feb, May, Aug 2025)

Usage:
    docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_all_scenarios.py
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
        .appName("all_scenarios_test") \
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
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.all_cases_test")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.all_cases_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.all_cases_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.all_cases_test.latest_summary")
    
    # Create accounts table (source)
    spark.sql("""
        CREATE TABLE demo.all_cases_test.accounts (
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
        CREATE TABLE demo.all_cases_test.summary (
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
        CREATE TABLE demo.all_cases_test.latest_summary (
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
    arr = ["CAST(NULL AS INT)"] * length
    for pos, val in positions_values.items():
        arr[pos] = str(val)
    return "array(" + ", ".join(arr) + ")"


def build_str_array(positions_values, length=36):
    """Build SQL array of strings with specific values at positions"""
    arr = ["CAST(NULL AS STRING)"] * length
    for pos, val in positions_values.items():
        arr[pos] = f"'{val}'"
    return "array(" + ", ".join(arr) + ")"


def setup_existing_data(spark):
    """
    Set up EXISTING summary data for Case II and Case III tests.
    This simulates data that was processed in previous pipeline runs.
    """
    print("\n" + "=" * 80)
    print("SETTING UP EXISTING SUMMARY DATA")
    print("=" * 80)
    
    # =========================================================================
    # Account 2001 - For Case II (Forward Entry)
    # Has Feb 2026 summary, will receive Mar 2026 forward entry
    # =========================================================================
    print("\n1. Account 2001 (Case II) - Existing Feb 2026 summary")
    
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.summary VALUES
        (2001, '2026-02', TIMESTAMP '2026-02-15 10:00:00',
         5000, 500, 10000, 0, 0, 'A', '0',
         {build_int_array({0: 5000, 1: 4500})},
         {build_int_array({0: 500, 1: 450})},
         {build_int_array({0: 10000, 1: 10000})},
         {build_int_array({0: 0, 1: 0})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         {build_str_array({0: '0', 1: '0'})},
         '00??????????????????????????????????')
    """)
    
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.latest_summary VALUES
        (2001, '2026-02', TIMESTAMP '2026-02-15 10:00:00',
         5000, 500, 10000, 0, 0, 'A', '0',
         {build_int_array({0: 5000, 1: 4500})},
         {build_int_array({0: 500, 1: 450})},
         {build_int_array({0: 10000, 1: 10000})},
         {build_int_array({0: 0, 1: 0})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         {build_str_array({0: '0', 1: '0'})},
         '00??????????????????????????????????')
    """)
    print("  - Inserted Feb 2026 summary with bal[0]=5000, bal[1]=4500")
    
    # =========================================================================
    # Account 3001 - For Case III (Backfill)
    # Has Dec 2025 and Jan 2026 summaries, will receive Nov 2025 backfill
    # =========================================================================
    print("\n2. Account 3001 (Case III) - Existing Dec 2025 + Jan 2026 summaries")
    
    # Dec 2025 summary
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.summary VALUES
        (3001, '2025-12', TIMESTAMP '2025-12-15 10:00:00',
         8000, 800, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 8000})},
         {build_int_array({0: 800})},
         {build_int_array({0: 15000})},
         {build_int_array({0: 0})},
         {build_int_array({0: 0})},
         {build_str_array({0: 'A'})},
         {build_str_array({0: '0'})},
         '0???????????????????????????????????')
    """)
    print("  - Inserted Dec 2025 summary with bal[0]=8000")
    
    # Jan 2026 summary (Dec 2025 at position 1)
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.summary VALUES
        (3001, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         7500, 750, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 7500, 1: 8000})},
         {build_int_array({0: 750, 1: 800})},
         {build_int_array({0: 15000, 1: 15000})},
         {build_int_array({0: 0, 1: 0})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         {build_str_array({0: '0', 1: '0'})},
         '00??????????????????????????????????')
    """)
    print("  - Inserted Jan 2026 summary with bal[0]=7500, bal[1]=8000 (Dec 2025)")
    
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.latest_summary VALUES
        (3001, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         7500, 750, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 7500, 1: 8000})},
         {build_int_array({0: 750, 1: 800})},
         {build_int_array({0: 15000, 1: 15000})},
         {build_int_array({0: 0, 1: 0})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         {build_str_array({0: '0', 1: '0'})},
         '00??????????????????????????????????')
    """)
    
    print("\nExisting data setup complete:")
    print(f"  Summary rows: {spark.sql('SELECT COUNT(*) FROM demo.all_cases_test.summary').first()[0]}")
    print(f"  Latest summary rows: {spark.sql('SELECT COUNT(*) FROM demo.all_cases_test.latest_summary').first()[0]}")


def insert_test_accounts(spark):
    """
    Insert NEW account records to be processed by the pipeline.
    These simulate incoming data from the source system.
    """
    print("\n" + "=" * 80)
    print("INSERTING NEW ACCOUNT RECORDS (to be processed)")
    print("=" * 80)
    
    upload_ts = "2026-01-30 10:00:00"
    
    # =========================================================================
    # Account 1001 - Case I: Brand new account, single month
    # =========================================================================
    print("\n1. Account 1001 (Case I) - New account, single month Jan 2026")
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.accounts VALUES
        (1001, '2026-01', TIMESTAMP '{upload_ts}',
         3000, 300, 8000, 0, 0, 'A', '0')
    """)
    
    # =========================================================================
    # Account 2001 - Case II: Forward entry (has Feb 2026, now Mar 2026)
    # =========================================================================
    print("2. Account 2001 (Case II) - Forward entry Mar 2026")
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.accounts VALUES
        (2001, '2026-03', TIMESTAMP '{upload_ts}',
         5500, 550, 10000, 0, 0, 'A', '0')
    """)
    
    # =========================================================================
    # Account 3001 - Case III: Backfill (has Jan 2026, Nov 2025 arrives late)
    # =========================================================================
    print("3. Account 3001 (Case III) - Backfill Nov 2025 arriving late")
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.accounts VALUES
        (3001, '2025-11', TIMESTAMP '{upload_ts}',
         8500, 850, 15000, 100, 30, 'B', '1')
    """)
    
    # =========================================================================
    # Account 4001 - Case IV: Bulk historical, 12 months (Jan-Dec 2025)
    # =========================================================================
    print("4. Account 4001 (Case IV) - Bulk historical 12 months (Jan-Dec 2025)")
    for i in range(12):
        month = f"2025-{i+1:02d}"
        balance = 10000 - (i * 500)  # Decreasing balance
        payment = 200 + (i * 10)
        spark.sql(f"""
            INSERT INTO demo.all_cases_test.accounts VALUES
            (4001, '{month}', TIMESTAMP '{upload_ts}',
             {balance}, {payment}, 12000, 0, 0, 'A', '0')
        """)
    
    # =========================================================================
    # Account 5001 - Case IV with GAPS: 12 months but missing Feb, May, Aug
    # =========================================================================
    print("5. Account 5001 (Case IV + GAPS) - 9 months with 3 gaps")
    gap_months = {2, 5, 8}  # Feb, May, Aug are missing
    for i in range(12):
        if (i + 1) in gap_months:
            continue  # Skip gap months
        month = f"2025-{i+1:02d}"
        balance = 6000 - (i * 300)
        payment = 100 + (i * 5)
        spark.sql(f"""
            INSERT INTO demo.all_cases_test.accounts VALUES
            (5001, '{month}', TIMESTAMP '{upload_ts}',
             {balance}, {payment}, 8000, 0, 0, 'A', '0')
        """)
    
    print("\nAccounts inserted:")
    spark.sql("""
        SELECT cons_acct_key, COUNT(*) as months, 
               MIN(rpt_as_of_mo) as min_month, MAX(rpt_as_of_mo) as max_month
        FROM demo.all_cases_test.accounts
        GROUP BY cons_acct_key
        ORDER BY cons_acct_key
    """).show(10, False)


def run_pipeline(spark):
    """Run the pipeline"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE v9.2")
    print("=" * 80)
    
    try:
        from summary_pipeline import run_pipeline as pipeline_run, PipelineConfig
    except ImportError as e:
        print(f"Could not import pipeline: {e}")
        return None
    
    config = PipelineConfig()
    config.accounts_table = "demo.all_cases_test.accounts"
    config.summary_table = "demo.all_cases_test.summary"
    config.latest_summary_table = "demo.all_cases_test.latest_summary"
    config.primary_key = "cons_acct_key"
    config.partition_key = "rpt_as_of_mo"
    config.timestamp_key = "base_ts"
    config.history_length = 36
    
    config.rolling_columns = [
        {"name": "balance_am", "source_column": "balance_am", "mapper_expr": "balance_am", "data_type": "INT"},
        {"name": "actual_payment_am", "source_column": "actual_payment_am", "mapper_expr": "actual_payment_am", "data_type": "INT"},
        {"name": "credit_limit_am", "source_column": "credit_limit_am", "mapper_expr": "credit_limit_am", "data_type": "INT"},
        {"name": "past_due_am", "source_column": "past_due_am", "mapper_expr": "past_due_am", "data_type": "INT"},
        {"name": "days_past_due", "source_column": "days_past_due", "mapper_expr": "days_past_due", "data_type": "INT"},
        {"name": "asset_class_cd", "source_column": "asset_class_cd", "mapper_expr": "asset_class_cd", "data_type": "STRING"},
        {"name": "payment_rating_cd", "source_column": "payment_rating_cd", "mapper_expr": "payment_rating_cd", "data_type": "STRING"}
    ]
    
    config.grid_columns = [
        {"name": "payment_history_grid", "source_history": "payment_rating_cd_history", "placeholder": "?", "separator": ""}
    ]
    
    config.column_mappings = {}
    config.column_transformations = []
    config.inferred_columns = []
    config.date_columns = []
    config.summary_columns = []
    config.latest_summary_columns = []
    
    try:
        stats = pipeline_run(spark, config)
        print("\n" + "-" * 60)
        print("PIPELINE RESULTS")
        print("-" * 60)
        print(f"Total records:       {stats.get('total_records', 0)}")
        print(f"Case I (new):        {stats.get('case_i_records', 0)}")
        print(f"Case II (forward):   {stats.get('case_ii_records', 0)}")
        print(f"Case III (backfill): {stats.get('case_iii_records', 0)}")
        print(f"Case IV (bulk):      {stats.get('case_iv_records', 0)}")
        print(f"Records written:     {stats.get('records_written', 0)}")
        return stats
    except Exception as e:
        print(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def verify_case_i(spark):
    """Verify Case I: New account with single month"""
    print("\n" + "=" * 80)
    print("CASE I VERIFICATION: Account 1001 (New Single Month)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    result = spark.sql("""
        SELECT rpt_as_of_mo, 
               balance_am_history[0] as bal_0,
               balance_am_history[1] as bal_1,
               balance_am_history[35] as bal_35
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 1001
    """).first()
    
    if result:
        print(f"  Found summary row for 2026-01")
        
        if result['bal_0'] == 3000:
            print(f"  [PASS] bal[0] = {result['bal_0']} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] bal[0] = {result['bal_0']} (expected 3000)")
            failed += 1
        
        if result['bal_1'] is None:
            print(f"  [PASS] bal[1] = NULL (expected NULL - no prior data)")
            passed += 1
        else:
            print(f"  [FAIL] bal[1] = {result['bal_1']} (expected NULL)")
            failed += 1
    else:
        print("  [FAIL] No summary row found for account 1001")
        failed += 2
    
    return passed, failed


def verify_case_ii(spark):
    """Verify Case II: Forward entry"""
    print("\n" + "=" * 80)
    print("CASE II VERIFICATION: Account 2001 (Forward Entry)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # Should now have Mar 2026 row with Feb at pos 1, Jan at pos 2
    result = spark.sql("""
        SELECT rpt_as_of_mo, 
               balance_am_history[0] as bal_0,
               balance_am_history[1] as bal_1,
               balance_am_history[2] as bal_2
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 2001 AND rpt_as_of_mo = '2026-03'
    """).first()
    
    if result:
        print(f"  Found Mar 2026 summary row")
        
        # bal[0] should be Mar 2026 = 5500
        if result['bal_0'] == 5500:
            print(f"  [PASS] bal[0] = {result['bal_0']} (expected 5500 - Mar 2026)")
            passed += 1
        else:
            print(f"  [FAIL] bal[0] = {result['bal_0']} (expected 5500)")
            failed += 1
        
        # bal[1] should be Feb 2026 = 5000 (shifted from previous summary)
        if result['bal_1'] == 5000:
            print(f"  [PASS] bal[1] = {result['bal_1']} (expected 5000 - Feb 2026 shifted)")
            passed += 1
        else:
            print(f"  [FAIL] bal[1] = {result['bal_1']} (expected 5000)")
            failed += 1
        
        # bal[2] should be Jan 2026 = 4500 (shifted from previous summary pos 1)
        if result['bal_2'] == 4500:
            print(f"  [PASS] bal[2] = {result['bal_2']} (expected 4500 - Jan 2026 shifted)")
            passed += 1
        else:
            print(f"  [FAIL] bal[2] = {result['bal_2']} (expected 4500)")
            failed += 1
    else:
        print("  [FAIL] No Mar 2026 summary row found")
        failed += 3
    
    # Verify latest_summary updated
    latest = spark.sql("""
        SELECT rpt_as_of_mo FROM demo.all_cases_test.latest_summary
        WHERE cons_acct_key = 2001
    """).first()
    
    if latest and latest['rpt_as_of_mo'] == '2026-03':
        print(f"  [PASS] latest_summary updated to 2026-03")
        passed += 1
    else:
        print(f"  [FAIL] latest_summary not updated (got {latest})")
        failed += 1
    
    return passed, failed


def verify_case_iii(spark):
    """Verify Case III: Backfill"""
    print("\n" + "=" * 80)
    print("CASE III VERIFICATION: Account 3001 (Backfill)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # Count total rows - should be 3 (Nov 2025, Dec 2025, Jan 2026)
    count = spark.sql("""
        SELECT COUNT(*) FROM demo.all_cases_test.summary WHERE cons_acct_key = 3001
    """).first()[0]
    
    if count == 3:
        print(f"  [PASS] Total rows = {count} (expected 3: Nov, Dec, Jan)")
        passed += 1
    else:
        print(f"  [FAIL] Total rows = {count} (expected 3)")
        failed += 1
    
    # Nov 2025 should be CREATED with bal[0]=8500
    nov = spark.sql("""
        SELECT balance_am_history[0] as bal_0, balance_am_history[1] as bal_1
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 3001 AND rpt_as_of_mo = '2025-11'
    """).first()
    
    if nov:
        print(f"  [PASS] Nov 2025 row CREATED (was missing before)")
        passed += 1
        
        if nov['bal_0'] == 8500:
            print(f"  [PASS] Nov 2025 bal[0] = {nov['bal_0']} (expected 8500)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2025 bal[0] = {nov['bal_0']} (expected 8500)")
            failed += 1
    else:
        print("  [FAIL] Nov 2025 row NOT CREATED")
        failed += 2
    
    # Jan 2026 should be UPDATED with Nov at position 2
    jan = spark.sql("""
        SELECT balance_am_history[0] as bal_0, 
               balance_am_history[1] as bal_1,
               balance_am_history[2] as bal_2
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 3001 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if jan:
        # bal[2] should now have Nov 2025 = 8500
        if jan['bal_2'] == 8500:
            print(f"  [PASS] Jan 2026 bal[2] = {jan['bal_2']} (expected 8500 - Nov 2025 backfilled)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2026 bal[2] = {jan['bal_2']} (expected 8500)")
            failed += 1
    
    return passed, failed


def verify_case_iv(spark):
    """Verify Case IV: Bulk historical load"""
    print("\n" + "=" * 80)
    print("CASE IV VERIFICATION: Account 4001 (Bulk Historical 12 months)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # Should have 12 rows
    count = spark.sql("""
        SELECT COUNT(*) FROM demo.all_cases_test.summary WHERE cons_acct_key = 4001
    """).first()[0]
    
    if count == 12:
        print(f"  [PASS] Total rows = {count} (expected 12)")
        passed += 1
    else:
        print(f"  [FAIL] Total rows = {count} (expected 12)")
        failed += 1
    
    # First month (Jan 2025) - only position 0
    jan = spark.sql("""
        SELECT balance_am_history[0] as bal_0, balance_am_history[1] as bal_1
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 4001 AND rpt_as_of_mo = '2025-01'
    """).first()
    
    if jan and jan['bal_0'] == 10000 and jan['bal_1'] is None:
        print(f"  [PASS] Jan 2025: bal[0]={jan['bal_0']}, bal[1]=NULL")
        passed += 1
    else:
        print(f"  [FAIL] Jan 2025: {jan}")
        failed += 1
    
    # Third month (Mar 2025) - should have Feb at pos 1, Jan at pos 2
    mar = spark.sql("""
        SELECT balance_am_history[0] as bal_0, 
               balance_am_history[1] as bal_1,
               balance_am_history[2] as bal_2,
               balance_am_history[3] as bal_3
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 4001 AND rpt_as_of_mo = '2025-03'
    """).first()
    
    if mar:
        # Mar = 10000 - 2*500 = 9000
        # Feb = 10000 - 1*500 = 9500
        # Jan = 10000
        if mar['bal_0'] == 9000:
            print(f"  [PASS] Mar 2025 bal[0] = {mar['bal_0']} (expected 9000)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[0] = {mar['bal_0']} (expected 9000)")
            failed += 1
        
        if mar['bal_1'] == 9500:
            print(f"  [PASS] Mar 2025 bal[1] = {mar['bal_1']} (expected 9500 - Feb)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[1] = {mar['bal_1']} (expected 9500)")
            failed += 1
        
        if mar['bal_2'] == 10000:
            print(f"  [PASS] Mar 2025 bal[2] = {mar['bal_2']} (expected 10000 - Jan)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[2] = {mar['bal_2']} (expected 10000)")
            failed += 1
    
    # Last month (Dec 2025) - should have full 12-month history
    dec = spark.sql("""
        SELECT balance_am_history[0] as bal_0, 
               balance_am_history[11] as bal_11
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 4001 AND rpt_as_of_mo = '2025-12'
    """).first()
    
    if dec:
        # Dec = 10000 - 11*500 = 4500
        # Jan (11 months back) = 10000
        if dec['bal_0'] == 4500:
            print(f"  [PASS] Dec 2025 bal[0] = {dec['bal_0']} (expected 4500)")
            passed += 1
        else:
            print(f"  [FAIL] Dec 2025 bal[0] = {dec['bal_0']} (expected 4500)")
            failed += 1
        
        if dec['bal_11'] == 10000:
            print(f"  [PASS] Dec 2025 bal[11] = {dec['bal_11']} (expected 10000 - Jan)")
            passed += 1
        else:
            print(f"  [FAIL] Dec 2025 bal[11] = {dec['bal_11']} (expected 10000)")
            failed += 1
    
    return passed, failed


def verify_case_iv_with_gaps(spark):
    """Verify Case IV with gaps: Non-reporting months"""
    print("\n" + "=" * 80)
    print("CASE IV + GAPS VERIFICATION: Account 5001 (Missing Feb, May, Aug)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # Should have 9 rows (12 - 3 gaps)
    count = spark.sql("""
        SELECT COUNT(*) FROM demo.all_cases_test.summary WHERE cons_acct_key = 5001
    """).first()[0]
    
    if count == 9:
        print(f"  [PASS] Total rows = {count} (expected 9 - gaps not reported)")
        passed += 1
    else:
        print(f"  [FAIL] Total rows = {count} (expected 9)")
        failed += 1
    
    # Show the data
    print("\n  Summary rows for account 5001:")
    spark.sql("""
        SELECT rpt_as_of_mo, 
               balance_am_history[0] as bal_0, 
               balance_am_history[1] as bal_1,
               balance_am_history[2] as bal_2,
               balance_am_history[3] as bal_3
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 5001
        ORDER BY rpt_as_of_mo
    """).show(12, False)
    
    # Mar 2025 - Feb is MISSING, so bal[1] should be NULL, bal[2] should have Jan
    mar = spark.sql("""
        SELECT balance_am_history[0] as bal_0, 
               balance_am_history[1] as bal_1,
               balance_am_history[2] as bal_2
        FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 5001 AND rpt_as_of_mo = '2025-03'
    """).first()
    
    if mar:
        # Mar = 6000 - 2*300 = 5400
        # Feb = GAP (NULL)
        # Jan = 6000
        if mar['bal_0'] == 5400:
            print(f"  [PASS] Mar 2025 bal[0] = {mar['bal_0']} (expected 5400)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[0] = {mar['bal_0']} (expected 5400)")
            failed += 1
        
        # NOTE: This is the critical gap test
        # bal[1] represents "1 month ago" which is Feb - should be NULL
        if mar['bal_1'] is None:
            print(f"  [PASS] Mar 2025 bal[1] = NULL (expected NULL - Feb gap)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[1] = {mar['bal_1']} (expected NULL - Feb gap)")
            failed += 1
        
        # bal[2] represents "2 months ago" which is Jan = 6000
        if mar['bal_2'] == 6000:
            print(f"  [PASS] Mar 2025 bal[2] = {mar['bal_2']} (expected 6000 - Jan)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[2] = {mar['bal_2']} (expected 6000)")
            failed += 1
    else:
        print("  [FAIL] Mar 2025 not found")
        failed += 3
    
    return passed, failed


def main():
    print("=" * 80)
    print("COMPREHENSIVE TEST - ALL 4 CASES + GAP HANDLING")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    
    spark = create_spark_session()
    print(f"\nSpark version: {spark.version}")
    
    total_passed = 0
    total_failed = 0
    
    try:
        # Setup
        setup_tables(spark)
        setup_existing_data(spark)  # For Case II and III
        insert_test_accounts(spark)
        
        # Run pipeline
        stats = run_pipeline(spark)
        
        if stats:
            # Verify each case
            p, f = verify_case_i(spark)
            total_passed += p
            total_failed += f
            
            p, f = verify_case_ii(spark)
            total_passed += p
            total_failed += f
            
            p, f = verify_case_iii(spark)
            total_passed += p
            total_failed += f
            
            p, f = verify_case_iv(spark)
            total_passed += p
            total_failed += f
            
            p, f = verify_case_iv_with_gaps(spark)
            total_passed += p
            total_failed += f
            
            # Final Summary
            print("\n" + "=" * 80)
            print(f"FINAL RESULTS: {total_passed} passed, {total_failed} failed")
            print("=" * 80)
            
            if total_failed == 0:
                print("\n*** ALL TESTS PASSED ***")
                return 0
            else:
                print(f"\n*** {total_failed} TESTS FAILED ***")
                return 1
        else:
            print("\n*** PIPELINE FAILED ***")
            return 1
    
    except Exception as e:
        print(f"\nTest failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        print(f"\nEnd time: {datetime.now()}")
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
