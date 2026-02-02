"""
COMPREHENSIVE TEST - All 4 Cases + Gap Handling + Production Config Format
============================================================================

Tests ALL scenarios matching production pipeline_config.json format:
- Case I:   New account with single month
- Case II:  Forward entry (existing account, new month)
- Case III: Backfill (existing account, late-arriving historical data)
- Case IV:  Bulk historical load (new account with multiple months)
- GAPS:     Non-reporting months in historical data

This test uses the production config FORMAT with all 7 rolling columns:
- actual_payment_am_history
- balance_am_history
- credit_limit_am_history
- past_due_am_history
- payment_rating_cd_history
- days_past_due_history
- asset_class_cd_4in_history

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
        .appName("all_scenarios_test_production_format") \
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
    """Create test tables with all 7 rolling columns"""
    print("Creating test database and tables with 7 rolling columns...")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.all_cases_test")
    
    # Create temp schema for temp tables (v9.4)
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.temp")
    
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
    
    # Create summary table with all 7 rolling arrays
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
            actual_payment_am_history ARRAY<INT>,
            balance_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            payment_rating_cd_history ARRAY<STRING>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_4in_history ARRAY<STRING>,
            payment_history_grid STRING,
            updated_base_ts TIMESTAMP
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
            actual_payment_am_history ARRAY<INT>,
            balance_am_history ARRAY<INT>,
            credit_limit_am_history ARRAY<INT>,
            past_due_am_history ARRAY<INT>,
            payment_rating_cd_history ARRAY<STRING>,
            days_past_due_history ARRAY<INT>,
            asset_class_cd_4in_history ARRAY<STRING>,
            payment_history_grid STRING,
            updated_base_ts TIMESTAMP
        )
        USING iceberg
    """)
    
    print("Tables created successfully with 7 rolling columns!")


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
         {build_int_array({0: 500, 1: 450})},
         {build_int_array({0: 5000, 1: 4500})},
         {build_int_array({0: 10000, 1: 10000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
    """)
    
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.latest_summary VALUES
        (2001, '2026-02', TIMESTAMP '2026-02-15 10:00:00',
         5000, 500, 10000, 0, 0, 'A', '0',
         {build_int_array({0: 500, 1: 450})},
         {build_int_array({0: 5000, 1: 4500})},
         {build_int_array({0: 10000, 1: 10000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
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
         {build_int_array({0: 800})},
         {build_int_array({0: 8000})},
         {build_int_array({0: 15000})},
         {build_int_array({0: 0})},
         {build_str_array({0: '0'})},
         {build_int_array({0: 0})},
         {build_str_array({0: 'A'})},
         '0???????????????????????????????????',
         NULL)
    """)
    print("  - Inserted Dec 2025 summary with bal[0]=8000")
    
    # Jan 2026 summary (Dec 2025 at position 1)
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.summary VALUES
        (3001, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         7500, 750, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 750, 1: 800})},
         {build_int_array({0: 7500, 1: 8000})},
         {build_int_array({0: 15000, 1: 15000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
    """)
    print("  - Inserted Jan 2026 summary with bal[0]=7500, bal[1]=8000 (Dec 2025)")
    
    spark.sql(f"""
        INSERT INTO demo.all_cases_test.latest_summary VALUES
        (3001, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         7500, 750, 15000, 0, 0, 'A', '0',
         {build_int_array({0: 750, 1: 800})},
         {build_int_array({0: 7500, 1: 8000})},
         {build_int_array({0: 15000, 1: 15000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
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
    # Note: Nov 2025 has 30 days_past_due (payment_rating_cd = '1')
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
        dpd = 0  # All on-time
        spark.sql(f"""
            INSERT INTO demo.all_cases_test.accounts VALUES
            (4001, '{month}', TIMESTAMP '{upload_ts}',
             {balance}, {payment}, 12000, 0, {dpd}, 'A', '0')
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
    """Run the pipeline with production config format"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE v9.3 with PRODUCTION CONFIG FORMAT")
    print("=" * 80)
    
    try:
        # Import v9.4.2 pipeline for regression testing
        sys.path.insert(0, '/home/iceberg/summary_v9_production')
        from pipeline.summary_pipeline_v9_4_2 import run_pipeline as pipeline_run
    except ImportError as e:
        print(f"Could not import pipeline: {e}")
        return None
    
    # Config matches production pipeline_config.json FORMAT with all 7 rolling columns
    config = {
        # Table names (flat structure matching original)
        "source_table": "demo.all_cases_test.accounts",
        "destination_table": "demo.all_cases_test.summary",
        "latest_history_table": "demo.all_cases_test.latest_summary",
        
        # Temp table schema (v9.4)
        "temp_table_schema": "temp",
        
        # Keys (flat structure matching original)
        "primary_column": "cons_acct_key",
        "partition_column": "rpt_as_of_mo",
        "max_identifier_column": "base_ts",
        
        # History length
        "history_length": 36,
        
        # Column mappings - empty for test since we use destination names directly
        "columns": {},
        
        # Column transformations - empty for test
        "column_transformations": [],
        
        # Inferred columns - empty for test
        "inferred_columns": [],
        
        # Date columns - empty for test
        "date_col_list": [],
        
        # Latest summary addon columns
        "latest_history_addon_cols": [],
        
        # Rolling columns - ALL 7 from production (using original field names)
        "rolling_columns": [
            {
                "name": "actual_payment_am",
                "mapper_expr": "actual_payment_am",
                "mapper_column": "actual_payment_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "balance_am",
                "mapper_expr": "balance_am",
                "mapper_column": "balance_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "credit_limit_am",
                "mapper_expr": "credit_limit_am",
                "mapper_column": "credit_limit_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "past_due_am",
                "mapper_expr": "past_due_am",
                "mapper_column": "past_due_am",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "payment_rating_cd",
                "mapper_expr": "payment_rating_cd",
                "mapper_column": "payment_rating_cd",
                "num_cols": 36,
                "type": "String"
            },
            {
                "name": "days_past_due",
                "mapper_expr": "days_past_due",
                "mapper_column": "days_past_due",
                "num_cols": 36,
                "type": "Integer"
            },
            {
                "name": "asset_class_cd_4in",
                "mapper_expr": "asset_class_cd",
                "mapper_column": "asset_class_cd",
                "num_cols": 36,
                "type": "String"
            }
        ],
        
        # Grid columns (using original field names: mapper_rolling_column, seperator)
        "grid_columns": [
            {
                "name": "payment_history_grid",
                "mapper_rolling_column": "payment_rating_cd",
                "placeholder": "?",
                "seperator": ""
            }
        ],
        
        # Performance settings
        "performance": {
            "broadcast_threshold": 10000000,
            "min_partitions": 4,
            "max_partitions": 32,
            "cache_level": "MEMORY_AND_DISK"
        },
        
        # Spark settings
        "spark": {
            "app_name": "SummaryPipeline_v9.3_test",
            "adaptive_enabled": True,
            "adaptive_coalesce": True,
            "adaptive_skew_join": True,
            "broadcast_timeout": 300
        }
    }
    
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
        SELECT * FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 1001
    """)
    
    if result.count() == 1:
        row = result.first()
        print(f"  Found summary row for {row['rpt_as_of_mo']}")
        
        # Check balance_am_history
        bal_hist = row['balance_am_history']
        if bal_hist[0] == 3000:
            print(f"  [PASS] bal[0] = {bal_hist[0]} (expected 3000)")
            passed += 1
        else:
            print(f"  [FAIL] bal[0] = {bal_hist[0]} (expected 3000)")
            failed += 1
        
        if bal_hist[1] is None:
            print(f"  [PASS] bal[1] = NULL (expected NULL - no prior data)")
            passed += 1
        else:
            print(f"  [FAIL] bal[1] = {bal_hist[1]} (expected NULL)")
            failed += 1
        
        # Check payment_rating_cd_history
        prc_hist = row['payment_rating_cd_history']
        if prc_hist[0] == '0':
            print(f"  [PASS] payment_rating_cd[0] = '{prc_hist[0]}' (expected '0')")
            passed += 1
        else:
            print(f"  [FAIL] payment_rating_cd[0] = '{prc_hist[0]}' (expected '0')")
            failed += 1
        
        # Check asset_class_cd_4in_history
        asc_hist = row['asset_class_cd_4in_history']
        if asc_hist[0] == 'A':
            print(f"  [PASS] asset_class_cd_4in[0] = '{asc_hist[0]}' (expected 'A')")
            passed += 1
        else:
            print(f"  [FAIL] asset_class_cd_4in[0] = '{asc_hist[0]}' (expected 'A')")
            failed += 1
        
        # Check payment_history_grid
        grid = row['payment_history_grid']
        if grid and grid.startswith('0'):
            print(f"  [PASS] payment_history_grid starts with '0'")
            passed += 1
        else:
            print(f"  [FAIL] payment_history_grid = '{grid}' (expected to start with '0')")
            failed += 1
    else:
        print(f"  [FAIL] Expected 1 row, found {result.count()}")
        failed += 1
    
    return passed, failed


def verify_case_ii(spark):
    """Verify Case II: Forward entry"""
    print("\n" + "=" * 80)
    print("CASE II VERIFICATION: Account 2001 (Forward Entry)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    result = spark.sql("""
        SELECT * FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 2001 AND rpt_as_of_mo = '2026-03'
    """)
    
    if result.count() == 1:
        row = result.first()
        print(f"  Found Mar 2026 summary row")
        
        bal_hist = row['balance_am_history']
        
        # bal[0] should be Mar 2026 value = 5500
        if bal_hist[0] == 5500:
            print(f"  [PASS] bal[0] = {bal_hist[0]} (expected 5500 - Mar 2026)")
            passed += 1
        else:
            print(f"  [FAIL] bal[0] = {bal_hist[0]} (expected 5500)")
            failed += 1
        
        # bal[1] should be Feb 2026 value = 5000 (shifted)
        if bal_hist[1] == 5000:
            print(f"  [PASS] bal[1] = {bal_hist[1]} (expected 5000 - Feb 2026 shifted)")
            passed += 1
        else:
            print(f"  [FAIL] bal[1] = {bal_hist[1]} (expected 5000)")
            failed += 1
        
        # bal[2] should be Jan 2026 value = 4500 (shifted)
        if bal_hist[2] == 4500:
            print(f"  [PASS] bal[2] = {bal_hist[2]} (expected 4500 - Jan 2026 shifted)")
            passed += 1
        else:
            print(f"  [FAIL] bal[2] = {bal_hist[2]} (expected 4500)")
            failed += 1
        
        # Check latest_summary updated
        latest = spark.sql("""
            SELECT rpt_as_of_mo FROM demo.all_cases_test.latest_summary
            WHERE cons_acct_key = 2001
        """).first()
        
        if latest['rpt_as_of_mo'] == '2026-03':
            print(f"  [PASS] latest_summary updated to 2026-03")
            passed += 1
        else:
            print(f"  [FAIL] latest_summary = {latest['rpt_as_of_mo']} (expected 2026-03)")
            failed += 1
    else:
        print(f"  [FAIL] Expected 1 row for Mar 2026, found {result.count()}")
        failed += 1
    
    return passed, failed


def verify_case_iii(spark):
    """Verify Case III: Backfill"""
    print("\n" + "=" * 80)
    print("CASE III VERIFICATION: Account 3001 (Backfill)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # Should have 3 rows: Nov 2025 (NEW), Dec 2025, Jan 2026
    total = spark.sql("""
        SELECT COUNT(*) as cnt FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 3001
    """).first()['cnt']
    
    if total == 3:
        print(f"  [PASS] Total rows = {total} (expected 3: Nov, Dec, Jan)")
        passed += 1
    else:
        print(f"  [FAIL] Total rows = {total} (expected 3)")
        failed += 1
    
    # Check Nov 2025 row was CREATED (new row for backfill)
    nov = spark.sql("""
        SELECT * FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 3001 AND rpt_as_of_mo = '2025-11'
    """)
    
    if nov.count() == 1:
        print(f"  [PASS] Nov 2025 row CREATED (was missing before)")
        passed += 1
        
        nov_row = nov.first()
        bal_hist = nov_row['balance_am_history']
        prc_hist = nov_row['payment_rating_cd_history']
        
        # bal[0] should be Nov 2025 value = 8500
        if bal_hist[0] == 8500:
            print(f"  [PASS] Nov 2025 bal[0] = {bal_hist[0]} (expected 8500)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2025 bal[0] = {bal_hist[0]} (expected 8500)")
            failed += 1
        
        # payment_rating_cd[0] should be '1' (30 days past due)
        if prc_hist[0] == '1':
            print(f"  [PASS] Nov 2025 payment_rating_cd[0] = '{prc_hist[0]}' (expected '1' - 30 DPD)")
            passed += 1
        else:
            print(f"  [FAIL] Nov 2025 payment_rating_cd[0] = '{prc_hist[0]}' (expected '1')")
            failed += 1
    else:
        print(f"  [FAIL] Nov 2025 row not found")
        failed += 1
    
    # Check Dec 2025 was UPDATED with backfill at position 1
    dec = spark.sql("""
        SELECT * FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 3001 AND rpt_as_of_mo = '2025-12'
    """)
    
    if dec.count() == 1:
        dec_row = dec.first()
        bal_hist = dec_row['balance_am_history']
        
        # bal[1] should now have Nov 2025 backfill = 8500
        if bal_hist[1] == 8500:
            print(f"  [PASS] Dec 2025 bal[1] = {bal_hist[1]} (expected 8500 - Nov 2025 backfilled)")
            passed += 1
        else:
            print(f"  [FAIL] Dec 2025 bal[1] = {bal_hist[1]} (expected 8500)")
            failed += 1
    else:
        print(f"  [FAIL] Dec 2025 row not found")
        failed += 1
    
    # Check Jan 2026 was UPDATED with backfill at position 2
    jan = spark.sql("""
        SELECT * FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 3001 AND rpt_as_of_mo = '2026-01'
    """)
    
    if jan.count() == 1:
        jan_row = jan.first()
        bal_hist = jan_row['balance_am_history']
        
        # bal[2] should now have Nov 2025 backfill = 8500
        if bal_hist[2] == 8500:
            print(f"  [PASS] Jan 2026 bal[2] = {bal_hist[2]} (expected 8500 - Nov 2025 backfilled)")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2026 bal[2] = {bal_hist[2]} (expected 8500)")
            failed += 1
    else:
        print(f"  [FAIL] Jan 2026 row not found")
        failed += 1
    
    return passed, failed


def verify_case_iv(spark):
    """Verify Case IV: Bulk historical load"""
    print("\n" + "=" * 80)
    print("CASE IV VERIFICATION: Account 4001 (Bulk Historical 12 months)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # Should have 12 rows (Jan-Dec 2025)
    total = spark.sql("""
        SELECT COUNT(*) as cnt FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 4001
    """).first()['cnt']
    
    if total == 12:
        print(f"  [PASS] Total rows = {total} (expected 12)")
        passed += 1
    else:
        print(f"  [FAIL] Total rows = {total} (expected 12)")
        failed += 1
    
    # Check Jan 2025 (first month)
    jan = spark.sql("""
        SELECT balance_am_history FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 4001 AND rpt_as_of_mo = '2025-01'
    """)
    
    if jan.count() == 1:
        bal_hist = jan.first()['balance_am_history']
        if bal_hist[0] == 10000 and bal_hist[1] is None:
            print(f"  [PASS] Jan 2025: bal[0]=10000, bal[1]=NULL")
            passed += 1
        else:
            print(f"  [FAIL] Jan 2025: bal[0]={bal_hist[0]}, bal[1]={bal_hist[1]}")
            failed += 1
    
    # Check Mar 2025 (should have rolling history)
    mar = spark.sql("""
        SELECT balance_am_history FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 4001 AND rpt_as_of_mo = '2025-03'
    """)
    
    if mar.count() == 1:
        bal_hist = mar.first()['balance_am_history']
        
        # Mar 2025 balance = 10000 - 2*500 = 9000
        if bal_hist[0] == 9000:
            print(f"  [PASS] Mar 2025 bal[0] = {bal_hist[0]} (expected 9000)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[0] = {bal_hist[0]} (expected 9000)")
            failed += 1
        
        # Feb 2025 balance = 10000 - 1*500 = 9500
        if bal_hist[1] == 9500:
            print(f"  [PASS] Mar 2025 bal[1] = {bal_hist[1]} (expected 9500 - Feb)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[1] = {bal_hist[1]} (expected 9500)")
            failed += 1
        
        # Jan 2025 balance = 10000
        if bal_hist[2] == 10000:
            print(f"  [PASS] Mar 2025 bal[2] = {bal_hist[2]} (expected 10000 - Jan)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[2] = {bal_hist[2]} (expected 10000)")
            failed += 1
    
    # Check Dec 2025 (last month - should have full 12-month history)
    dec = spark.sql("""
        SELECT balance_am_history FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 4001 AND rpt_as_of_mo = '2025-12'
    """)
    
    if dec.count() == 1:
        bal_hist = dec.first()['balance_am_history']
        
        # Dec 2025 balance = 10000 - 11*500 = 4500
        if bal_hist[0] == 4500:
            print(f"  [PASS] Dec 2025 bal[0] = {bal_hist[0]} (expected 4500)")
            passed += 1
        else:
            print(f"  [FAIL] Dec 2025 bal[0] = {bal_hist[0]} (expected 4500)")
            failed += 1
        
        # Position 11 should have Jan 2025 = 10000
        if bal_hist[11] == 10000:
            print(f"  [PASS] Dec 2025 bal[11] = {bal_hist[11]} (expected 10000 - Jan)")
            passed += 1
        else:
            print(f"  [FAIL] Dec 2025 bal[11] = {bal_hist[11]} (expected 10000)")
            failed += 1
    
    return passed, failed


def verify_gaps(spark):
    """Verify Case IV with gaps: Non-reporting months correctly handled"""
    print("\n" + "=" * 80)
    print("CASE IV + GAPS VERIFICATION: Account 5001 (Missing Feb, May, Aug)")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    # Should have 9 rows (12 - 3 gaps)
    total = spark.sql("""
        SELECT COUNT(*) as cnt FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 5001
    """).first()['cnt']
    
    if total == 9:
        print(f"  [PASS] Total rows = {total} (expected 9 - gaps not reported)")
        passed += 1
    else:
        print(f"  [FAIL] Total rows = {total} (expected 9)")
        failed += 1
    
    # Show all rows for debugging
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
    """).show(15, False)
    
    # Check Mar 2025 (Feb is gap)
    mar = spark.sql("""
        SELECT balance_am_history FROM demo.all_cases_test.summary
        WHERE cons_acct_key = 5001 AND rpt_as_of_mo = '2025-03'
    """)
    
    if mar.count() == 1:
        bal_hist = mar.first()['balance_am_history']
        
        # Mar 2025 balance = 6000 - 2*300 = 5400
        if bal_hist[0] == 5400:
            print(f"  [PASS] Mar 2025 bal[0] = {bal_hist[0]} (expected 5400)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[0] = {bal_hist[0]} (expected 5400)")
            failed += 1
        
        # Position 1 should be NULL (Feb gap!)
        if bal_hist[1] is None:
            print(f"  [PASS] Mar 2025 bal[1] = NULL (expected NULL - Feb gap)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[1] = {bal_hist[1]} (expected NULL - Feb gap)")
            failed += 1
        
        # Position 2 should have Jan 2025 = 6000
        if bal_hist[2] == 6000:
            print(f"  [PASS] Mar 2025 bal[2] = {bal_hist[2]} (expected 6000 - Jan)")
            passed += 1
        else:
            print(f"  [FAIL] Mar 2025 bal[2] = {bal_hist[2]} (expected 6000)")
            failed += 1
    
    return passed, failed


def main():
    print("=" * 80)
    print("COMPREHENSIVE TEST WITH 7 ROLLING COLUMNS (PRODUCTION FORMAT)")
    print("Summary Pipeline v9.3")
    print("=" * 80)
    print(f"\nStart time: {datetime.now()}")
    
    spark = create_spark_session()
    
    try:
        # Setup
        setup_tables(spark)
        setup_existing_data(spark)
        insert_test_accounts(spark)
        
        # Run pipeline
        stats = run_pipeline(spark)
        
        if stats is None:
            print("\n[FATAL] Pipeline failed to run")
            return
        
        # Verify all cases
        total_passed = 0
        total_failed = 0
        
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
        
        p, f = verify_gaps(spark)
        total_passed += p
        total_failed += f
        
        # Summary
        print("\n" + "=" * 80)
        print(f"FINAL RESULTS: {total_passed} passed, {total_failed} failed")
        print("=" * 80)
        
        if total_failed == 0:
            print("\n*** ALL TESTS PASSED ***")
        else:
            print(f"\n*** {total_failed} TESTS FAILED ***")
        
        print(f"\nEnd time: {datetime.now()}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
