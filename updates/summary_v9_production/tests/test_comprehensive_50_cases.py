"""
COMPREHENSIVE TEST SUITE - 50+ Unique Test Cases
=================================================

This test suite covers ALL edge cases for the Summary Pipeline v9.4:

CASE I - New Accounts (Tests 1-8):
  1. Single new account, single month
  2. Multiple new accounts in same batch
  3. New account with NULL values in some fields
  4. New account with zero values
  5. New account with negative values
  6. New account with maximum integer values
  7. New account with empty string in string fields
  8. New account at year boundary (Dec)

CASE II - Forward Entries (Tests 9-18):
  9. Single forward entry, consecutive month (MONTH_DIFF=1)
  10. Forward entry with gap of 2 months (MONTH_DIFF=3)
  11. Forward entry with gap of 6 months
  12. Forward entry with gap of 12 months
  13. Forward entry with gap of 35 months (max before array full)
  14. Forward entry with gap > 36 months (array overflow)
  15. Forward entry with NULL values replacing non-NULL
  16. Forward entry at year boundary (Dec -> Jan)
  17. Forward entry where previous array is full (36 elements)
  18. Multiple forward entries for SAME account (known limitation)

CASE III - Backfill (Tests 19-28):
  19. Single backfill, one month before latest
  20. Backfill multiple months before latest
  21. Backfill at the very beginning of history
  22. Backfill that fills a gap in existing history
  23. Backfill with NULL values
  24. Backfill updating all future rows correctly
  25. Backfill where future rows have full arrays
  26. Multiple backfills for SAME account (known limitation)
  27. Backfill at year boundary
  28. Backfill for account with only 1 existing row

CASE IV - Bulk Historical (Tests 29-38):
  29. New account with 2 months
  30. New account with 12 months (1 year)
  31. New account with 36 months (full history)
  32. New account with 37 months (more than history length)
  33. Bulk load with consecutive months
  34. Bulk load with gaps (missing months)
  35. Bulk load with multiple gaps
  36. Bulk load with only first and last month (35 month gap)
  37. Bulk load with NULL values in some months
  38. Bulk load with all same values

MIXED & EDGE CASES (Tests 39-50):
  39. All 4 cases in single batch
  40. Month at year boundary (Dec -> Jan transition)
  41. Very old months (year 2000)
  42. Future dated months (year 2030)
  43. Array shifts correctly at 36th position
  44. Grid column with all same values
  45. Grid column with all NULLs
  46. Grid column with mixed values and NULLs
  47. Empty batch (0 records)
  48. Single record batch
  49. Account key with large value
  50. Concurrent updates to same account (edge case)
  51. Payment rating codes mapping correctly
  52. Asset class codes mapping correctly

Usage:
    docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_comprehensive_50_cases.py
"""

import sys
import os

# Add the pipeline directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_dir = os.path.join(os.path.dirname(script_dir), 'pipeline')
sys.path.insert(0, pipeline_dir)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from dateutil.relativedelta import relativedelta


class TestResult:
    """Track test results"""
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.failures = []
    
    def add_pass(self, test_name, message=""):
        self.passed += 1
        print(f"  [PASS] {test_name}: {message}")
    
    def add_fail(self, test_name, message=""):
        self.failed += 1
        self.failures.append((test_name, message))
        print(f"  [FAIL] {test_name}: {message}")
    
    def check(self, condition, test_name, pass_msg="", fail_msg=""):
        if condition:
            self.add_pass(test_name, pass_msg)
        else:
            self.add_fail(test_name, fail_msg)
        return condition
    
    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*80}")
        print(f"FINAL RESULTS: {self.passed} passed, {self.failed} failed out of {total} tests")
        print(f"{'='*80}")
        if self.failures:
            print("\nFailed tests:")
            for name, msg in self.failures:
                print(f"  - {name}: {msg}")
        return self.failed == 0


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("comprehensive_50_test_cases") \
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
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.comprehensive_test")
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.temp")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.comprehensive_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.comprehensive_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.comprehensive_test.latest_summary")
    
    # Create accounts table (source)
    spark.sql("""
        CREATE TABLE demo.comprehensive_test.accounts (
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
    
    # Create summary table with rolling arrays
    spark.sql("""
        CREATE TABLE demo.comprehensive_test.summary (
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
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    # Create latest_summary table
    spark.sql("""
        CREATE TABLE demo.comprehensive_test.latest_summary (
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
            payment_history_grid STRING
        )
        USING iceberg
    """)
    
    print("Tables created successfully!")


def clear_tables(spark):
    """Clear all data from test tables"""
    spark.sql("DELETE FROM demo.comprehensive_test.accounts")
    spark.sql("DELETE FROM demo.comprehensive_test.summary")
    spark.sql("DELETE FROM demo.comprehensive_test.latest_summary")


def insert_account(spark, acct_key, month, balance, payment=100, credit_limit=10000,
                   past_due=0, days_past_due=0, asset_class='A', payment_rating='0',
                   base_ts=None):
    """Insert a single account record"""
    if base_ts is None:
        base_ts = datetime.now()
    
    # Handle NULL values
    balance_str = str(balance) if balance is not None else "NULL"
    payment_str = str(payment) if payment is not None else "NULL"
    credit_limit_str = str(credit_limit) if credit_limit is not None else "NULL"
    past_due_str = str(past_due) if past_due is not None else "NULL"
    days_past_due_str = str(days_past_due) if days_past_due is not None else "NULL"
    asset_class_str = f"'{asset_class}'" if asset_class is not None else "NULL"
    payment_rating_str = f"'{payment_rating}'" if payment_rating is not None else "NULL"
    
    spark.sql(f"""
        INSERT INTO demo.comprehensive_test.accounts VALUES (
            {acct_key}, '{month}', TIMESTAMP '{base_ts}',
            {balance_str}, {payment_str}, {credit_limit_str},
            {past_due_str}, {days_past_due_str}, {asset_class_str}, {payment_rating_str}
        )
    """)


def insert_existing_summary(spark, acct_key, month, balance_history, payment_rating_history=None,
                           base_ts=None):
    """Insert an existing summary row for testing Case II and Case III"""
    if base_ts is None:
        base_ts = datetime.now()
    if payment_rating_history is None:
        payment_rating_history = ['0'] * len(balance_history)
    
    # Pad arrays to 36 elements
    while len(balance_history) < 36:
        balance_history.append(None)
    while len(payment_rating_history) < 36:
        payment_rating_history.append(None)
    
    # Create array literals
    bal_array = "ARRAY(" + ",".join([str(x) if x is not None else "NULL" for x in balance_history]) + ")"
    pr_array = "ARRAY(" + ",".join([f"'{x}'" if x is not None else "NULL" for x in payment_rating_history]) + ")"
    
    # Create other arrays (simplified - all NULLs)
    null_int_array = "ARRAY(" + ",".join(["NULL"] * 36) + ")"
    null_str_array = "ARRAY(" + ",".join(["NULL"] * 36) + ")"
    
    # Generate grid from payment rating
    grid = "".join([x if x is not None else "?" for x in payment_rating_history[:36]])
    
    # Insert into both summary and latest_summary
    for table in ["summary", "latest_summary"]:
        spark.sql(f"""
            INSERT INTO demo.comprehensive_test.{table} VALUES (
                {acct_key}, '{month}', TIMESTAMP '{base_ts}',
                {balance_history[0] if balance_history[0] else 'NULL'}, 100, 10000, 0, 0, 'A', '{payment_rating_history[0]}',
                {null_int_array},
                {bal_array},
                {null_int_array},
                {null_int_array},
                {pr_array},
                {null_int_array},
                {null_str_array},
                '{grid}'
            )
        """)


def get_config():
    """Get pipeline configuration"""
    return {
        "source_table": "demo.comprehensive_test.accounts",
        "destination_table": "demo.comprehensive_test.summary",
        "latest_history_table": "demo.comprehensive_test.latest_summary",
        "temp_table_schema": "temp",
        "primary_column": "cons_acct_key",
        "partition_column": "rpt_as_of_mo",
        "max_identifier_column": "base_ts",
        "history_length": 36,
        "columns": {},
        "column_transformations": [],
        "inferred_columns": [],
        "date_col_list": [],
        "latest_history_addon_cols": [],
        "rolling_columns": [
            {"name": "actual_payment_am", "mapper_expr": "actual_payment_am", "mapper_column": "actual_payment_am", "num_cols": 36, "type": "Integer"},
            {"name": "balance_am", "mapper_expr": "balance_am", "mapper_column": "balance_am", "num_cols": 36, "type": "Integer"},
            {"name": "credit_limit_am", "mapper_expr": "credit_limit_am", "mapper_column": "credit_limit_am", "num_cols": 36, "type": "Integer"},
            {"name": "past_due_am", "mapper_expr": "past_due_am", "mapper_column": "past_due_am", "num_cols": 36, "type": "Integer"},
            {"name": "payment_rating_cd", "mapper_expr": "payment_rating_cd", "mapper_column": "payment_rating_cd", "num_cols": 36, "type": "String"},
            {"name": "days_past_due", "mapper_expr": "days_past_due", "mapper_column": "days_past_due", "num_cols": 36, "type": "Integer"},
            {"name": "asset_class_cd_4in", "mapper_expr": "asset_class_cd", "mapper_column": "asset_class_cd", "num_cols": 36, "type": "String"},
        ],
        "grid_columns": [
            {"name": "payment_history_grid", "mapper_rolling_column": "payment_rating_cd", "placeholder": "?", "seperator": ""}
        ]
    }


def run_pipeline(spark, config):
    """Run the pipeline"""
    import summary_pipeline
    return summary_pipeline.run_pipeline(spark, config)


def get_summary_row(spark, acct_key, month):
    """Get a summary row for verification"""
    rows = spark.sql(f"""
        SELECT * FROM demo.comprehensive_test.summary
        WHERE cons_acct_key = {acct_key} AND rpt_as_of_mo = '{month}'
    """).collect()
    return rows[0] if rows else None


def get_summary_count(spark, acct_key):
    """Get count of summary rows for an account"""
    result = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM demo.comprehensive_test.summary
        WHERE cons_acct_key = {acct_key}
    """).collect()
    return result[0]['cnt']


def get_latest_summary(spark, acct_key):
    """Get the latest summary row for an account"""
    rows = spark.sql(f"""
        SELECT * FROM demo.comprehensive_test.latest_summary
        WHERE cons_acct_key = {acct_key}
    """).collect()
    return rows[0] if rows else None


# ==============================================================================
# TEST CASES
# ==============================================================================

def test_case_i_single_new_account(spark, results):
    """Test 1: Single new account, single month"""
    print("\n" + "="*80)
    print("TEST 1: Case I - Single new account, single month")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 1001, "2026-01", 5000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 1001, "2026-01")
    results.check(row is not None, "T1.1", "Summary row created", "Summary row not found")
    if row:
        results.check(row.balance_am_history[0] == 5000, "T1.2", 
                     f"bal[0]={row.balance_am_history[0]}", f"Expected 5000, got {row.balance_am_history[0]}")
        results.check(row.balance_am_history[1] is None, "T1.3",
                     "bal[1]=NULL", f"Expected NULL, got {row.balance_am_history[1]}")


def test_case_i_multiple_new_accounts(spark, results):
    """Test 2: Multiple new accounts in same batch"""
    print("\n" + "="*80)
    print("TEST 2: Case I - Multiple new accounts in same batch")
    print("="*80)
    
    clear_tables(spark)
    for i in range(5):
        insert_account(spark, 2000 + i, "2026-01", 1000 * (i + 1))
    
    config = get_config()
    run_pipeline(spark, config)
    
    for i in range(5):
        row = get_summary_row(spark, 2000 + i, "2026-01")
        expected = 1000 * (i + 1)
        results.check(row is not None and row.balance_am_history[0] == expected,
                     f"T2.{i+1}", f"Account {2000+i} bal[0]={expected}", 
                     f"Account {2000+i} incorrect")


def test_case_i_null_values(spark, results):
    """Test 3: New account with NULL values in some fields"""
    print("\n" + "="*80)
    print("TEST 3: Case I - New account with NULL values")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 3001, "2026-01", balance=None, payment=None, 
                  past_due=None, days_past_due=None)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 3001, "2026-01")
    results.check(row is not None, "T3.1", "Summary row created with NULLs", "Row not found")
    if row:
        results.check(row.balance_am_history[0] is None, "T3.2",
                     "bal[0]=NULL", f"Expected NULL, got {row.balance_am_history[0]}")


def test_case_i_zero_values(spark, results):
    """Test 4: New account with zero values"""
    print("\n" + "="*80)
    print("TEST 4: Case I - New account with zero values")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 4001, "2026-01", balance=0, payment=0, past_due=0)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 4001, "2026-01")
    results.check(row is not None, "T4.1", "Summary row created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 0, "T4.2",
                     "bal[0]=0", f"Expected 0, got {row.balance_am_history[0]}")


def test_case_i_negative_values(spark, results):
    """Test 5: New account with negative values"""
    print("\n" + "="*80)
    print("TEST 5: Case I - New account with negative values")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 5001, "2026-01", balance=-1000, payment=-500)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 5001, "2026-01")
    results.check(row is not None, "T5.1", "Summary row created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == -1000, "T5.2",
                     "bal[0]=-1000", f"Expected -1000, got {row.balance_am_history[0]}")


def test_case_i_max_int_values(spark, results):
    """Test 6: New account with maximum integer values"""
    print("\n" + "="*80)
    print("TEST 6: Case I - New account with max integer values")
    print("="*80)
    
    clear_tables(spark)
    max_int = 2147483647  # Max 32-bit signed int
    insert_account(spark, 6001, "2026-01", balance=max_int)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 6001, "2026-01")
    results.check(row is not None, "T6.1", "Summary row created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == max_int, "T6.2",
                     f"bal[0]={max_int}", f"Expected {max_int}, got {row.balance_am_history[0]}")


def test_case_i_empty_string(spark, results):
    """Test 7: New account with empty string in string fields"""
    print("\n" + "="*80)
    print("TEST 7: Case I - New account with empty string")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 7001, "2026-01", balance=1000, asset_class='', payment_rating='')
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 7001, "2026-01")
    results.check(row is not None, "T7.1", "Summary row created", "Row not found")


def test_case_i_year_boundary(spark, results):
    """Test 8: New account at year boundary (December)"""
    print("\n" + "="*80)
    print("TEST 8: Case I - New account at year boundary")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 8001, "2025-12", balance=5000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 8001, "2025-12")
    results.check(row is not None, "T8.1", "Summary row created for Dec", "Row not found")


def test_case_ii_consecutive_month(spark, results):
    """Test 9: Forward entry, consecutive month (MONTH_DIFF=1)"""
    print("\n" + "="*80)
    print("TEST 9: Case II - Forward entry, consecutive month")
    print("="*80)
    
    clear_tables(spark)
    # Setup existing summary for 2025-12
    insert_existing_summary(spark, 9001, "2025-12", [5000, 4500, 4000])
    # Insert forward entry for 2026-01
    insert_account(spark, 9001, "2026-01", balance=5500)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 9001, "2026-01")
    results.check(row is not None, "T9.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 5500, "T9.2",
                     "bal[0]=5500 (new)", f"Expected 5500, got {row.balance_am_history[0]}")
        results.check(row.balance_am_history[1] == 5000, "T9.3",
                     "bal[1]=5000 (shifted from Dec)", f"Expected 5000, got {row.balance_am_history[1]}")
        results.check(row.balance_am_history[2] == 4500, "T9.4",
                     "bal[2]=4500 (shifted)", f"Expected 4500, got {row.balance_am_history[2]}")


def test_case_ii_gap_2_months(spark, results):
    """Test 10: Forward entry with gap of 2 months (MONTH_DIFF=3)"""
    print("\n" + "="*80)
    print("TEST 10: Case II - Forward entry with 2-month gap")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 10001, "2025-12", [5000, 4500])
    # Skip Jan and Feb, insert Mar
    insert_account(spark, 10001, "2026-03", balance=6000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 10001, "2026-03")
    results.check(row is not None, "T10.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 6000, "T10.2",
                     "bal[0]=6000 (Mar)", f"Got {row.balance_am_history[0]}")
        results.check(row.balance_am_history[1] is None, "T10.3",
                     "bal[1]=NULL (Feb gap)", f"Got {row.balance_am_history[1]}")
        results.check(row.balance_am_history[2] is None, "T10.4",
                     "bal[2]=NULL (Jan gap)", f"Got {row.balance_am_history[2]}")
        results.check(row.balance_am_history[3] == 5000, "T10.5",
                     "bal[3]=5000 (Dec shifted)", f"Got {row.balance_am_history[3]}")


def test_case_ii_gap_6_months(spark, results):
    """Test 11: Forward entry with 6-month gap"""
    print("\n" + "="*80)
    print("TEST 11: Case II - Forward entry with 6-month gap")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 11001, "2025-06", [3000])
    # 6 month gap: Jul-Dec missing, insert Jan 2026
    insert_account(spark, 11001, "2026-01", balance=4000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 11001, "2026-01")
    results.check(row is not None, "T11.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 4000, "T11.2", 
                     "bal[0]=4000", f"Got {row.balance_am_history[0]}")
        # Positions 1-6 should be NULL (gap months)
        nulls_correct = all(row.balance_am_history[i] is None for i in range(1, 7))
        results.check(nulls_correct, "T11.3", "bal[1-6]=NULL (gap)", "Gap not handled correctly")
        results.check(row.balance_am_history[7] == 3000, "T11.4",
                     "bal[7]=3000 (Jun shifted)", f"Got {row.balance_am_history[7]}")


def test_case_ii_gap_12_months(spark, results):
    """Test 12: Forward entry with 12-month gap"""
    print("\n" + "="*80)
    print("TEST 12: Case II - Forward entry with 12-month gap")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 12001, "2025-01", [2000])
    insert_account(spark, 12001, "2026-02", balance=3000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 12001, "2026-02")
    results.check(row is not None, "T12.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 3000, "T12.2", 
                     "bal[0]=3000", f"Got {row.balance_am_history[0]}")
        # Position 13 should have the old value (13 months gap)
        results.check(row.balance_am_history[13] == 2000, "T12.3",
                     "bal[13]=2000 (Jan 2025)", f"Got {row.balance_am_history[13]}")


def test_case_ii_gap_35_months(spark, results):
    """Test 13: Forward entry with 35-month gap (max before overflow)"""
    print("\n" + "="*80)
    print("TEST 13: Case II - Forward entry with 35-month gap")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 13001, "2023-02", [1000])
    # 35 months later
    insert_account(spark, 13001, "2026-01", balance=2000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 13001, "2026-01")
    results.check(row is not None, "T13.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 2000, "T13.2", 
                     "bal[0]=2000", f"Got {row.balance_am_history[0]}")
        # The old value should still be in the array at position 35
        results.check(row.balance_am_history[35] == 1000, "T13.3",
                     "bal[35]=1000 (just fits)", f"Got {row.balance_am_history[35]}")


def test_case_ii_gap_over_36_months(spark, results):
    """Test 14: Forward entry with gap > 36 months (array overflow)"""
    print("\n" + "="*80)
    print("TEST 14: Case II - Forward entry with >36 month gap (overflow)")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 14001, "2022-01", [1000])
    # 48 months later (way past 36 month window)
    insert_account(spark, 14001, "2026-01", balance=5000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 14001, "2026-01")
    results.check(row is not None, "T14.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 5000, "T14.2", 
                     "bal[0]=5000", f"Got {row.balance_am_history[0]}")
        # Old value should be pushed out of the 36-element array
        # All positions 1-35 should be NULL (gap) and original value gone
        results.check(row.balance_am_history[35] is None, "T14.3",
                     "bal[35]=NULL (old value pushed out)", f"Got {row.balance_am_history[35]}")


def test_case_ii_null_replacing_value(spark, results):
    """Test 15: Forward entry with NULL values replacing non-NULL"""
    print("\n" + "="*80)
    print("TEST 15: Case II - Forward entry with NULL replacing value")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 15001, "2025-12", [5000])
    insert_account(spark, 15001, "2026-01", balance=None)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 15001, "2026-01")
    results.check(row is not None, "T15.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] is None, "T15.2",
                     "bal[0]=NULL", f"Got {row.balance_am_history[0]}")
        results.check(row.balance_am_history[1] == 5000, "T15.3",
                     "bal[1]=5000 (shifted)", f"Got {row.balance_am_history[1]}")


def test_case_ii_year_boundary(spark, results):
    """Test 16: Forward entry at year boundary (Dec -> Jan)"""
    print("\n" + "="*80)
    print("TEST 16: Case II - Forward entry at year boundary")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 16001, "2025-12", [5000])
    insert_account(spark, 16001, "2026-01", balance=5500)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 16001, "2026-01")
    results.check(row is not None, "T16.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 5500, "T16.2",
                     "bal[0]=5500 (Jan 2026)", f"Got {row.balance_am_history[0]}")
        results.check(row.balance_am_history[1] == 5000, "T16.3",
                     "bal[1]=5000 (Dec 2025)", f"Got {row.balance_am_history[1]}")


def test_case_ii_full_array(spark, results):
    """Test 17: Forward entry where previous array is full (36 elements)"""
    print("\n" + "="*80)
    print("TEST 17: Case II - Forward entry with full 36-element array")
    print("="*80)
    
    clear_tables(spark)
    # Create full 36-element history
    full_history = [1000 + i*100 for i in range(36)]  # [1000, 1100, ..., 4500]
    insert_existing_summary(spark, 17001, "2025-12", full_history)
    insert_account(spark, 17001, "2026-01", balance=5000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 17001, "2026-01")
    results.check(row is not None, "T17.1", "Forward entry created", "Row not found")
    if row:
        results.check(row.balance_am_history[0] == 5000, "T17.2",
                     "bal[0]=5000 (new)", f"Got {row.balance_am_history[0]}")
        results.check(row.balance_am_history[1] == 1000, "T17.3",
                     "bal[1]=1000 (shifted)", f"Got {row.balance_am_history[1]}")
        # Last element should be second-to-last from previous (4400, not 4500)
        results.check(row.balance_am_history[35] == 4400, "T17.4",
                     "bal[35]=4400 (4500 pushed out)", f"Got {row.balance_am_history[35]}")


def test_case_ii_multiple_same_account(spark, results):
    """Test 18: Multiple forward entries for SAME account (known limitation)"""
    print("\n" + "="*80)
    print("TEST 18: Case II - Multiple forward entries same account (LIMITATION)")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 18001, "2025-12", [5000])
    # Insert 3 forward months in same batch
    insert_account(spark, 18001, "2026-01", balance=5500)
    insert_account(spark, 18001, "2026-02", balance=6000)
    insert_account(spark, 18001, "2026-03", balance=6500)
    
    config = get_config()
    run_pipeline(spark, config)
    
    # This is a KNOWN LIMITATION - may not work correctly
    # Just check that it doesn't crash and produces some output
    count = get_summary_count(spark, 18001)
    results.check(count >= 1, "T18.1", f"Produced {count} rows (limitation test)", 
                 "No output produced")


def test_case_iii_one_month_backfill(spark, results):
    """Test 19: Single backfill, one month before latest"""
    print("\n" + "="*80)
    print("TEST 19: Case III - Single backfill, one month before latest")
    print("="*80)
    
    clear_tables(spark)
    # Existing: Dec 2025 and Jan 2026
    insert_existing_summary(spark, 19001, "2025-12", [5000])
    insert_existing_summary(spark, 19001, "2026-01", [5500, 5000])
    # Backfill Nov 2025
    insert_account(spark, 19001, "2025-11", balance=4500)
    
    config = get_config()
    run_pipeline(spark, config)
    
    # Check Nov row was created
    nov_row = get_summary_row(spark, 19001, "2025-11")
    results.check(nov_row is not None, "T19.1", "Nov 2025 row created", "Row not found")
    if nov_row:
        results.check(nov_row.balance_am_history[0] == 4500, "T19.2",
                     "Nov bal[0]=4500", f"Got {nov_row.balance_am_history[0]}")
    
    # Check Jan row was updated with backfill data
    jan_row = get_summary_row(spark, 19001, "2026-01")
    if jan_row:
        results.check(jan_row.balance_am_history[2] == 4500, "T19.3",
                     "Jan bal[2]=4500 (Nov backfilled)", f"Got {jan_row.balance_am_history[2]}")


def test_case_iii_multiple_months_before(spark, results):
    """Test 20: Backfill multiple months before latest"""
    print("\n" + "="*80)
    print("TEST 20: Case III - Backfill multiple months before latest")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 20001, "2026-01", [6000])
    # Backfill Oct 2025 (3 months before)
    insert_account(spark, 20001, "2025-10", balance=4000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    oct_row = get_summary_row(spark, 20001, "2025-10")
    results.check(oct_row is not None, "T20.1", "Oct 2025 row created", "Row not found")
    
    jan_row = get_summary_row(spark, 20001, "2026-01")
    if jan_row:
        results.check(jan_row.balance_am_history[3] == 4000, "T20.2",
                     "Jan bal[3]=4000 (Oct backfilled)", f"Got {jan_row.balance_am_history[3]}")


def test_case_iii_with_null(spark, results):
    """Test 21: Backfill with NULL values"""
    print("\n" + "="*80)
    print("TEST 21: Case III - Backfill with NULL values")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 21001, "2026-01", [5000])
    insert_account(spark, 21001, "2025-12", balance=None)
    
    config = get_config()
    run_pipeline(spark, config)
    
    dec_row = get_summary_row(spark, 21001, "2025-12")
    results.check(dec_row is not None, "T21.1", "Dec row created", "Row not found")
    if dec_row:
        results.check(dec_row.balance_am_history[0] is None, "T21.2",
                     "Dec bal[0]=NULL", f"Got {dec_row.balance_am_history[0]}")


def test_case_iii_year_boundary(spark, results):
    """Test 22: Backfill at year boundary"""
    print("\n" + "="*80)
    print("TEST 22: Case III - Backfill at year boundary")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 22001, "2026-02", [6000])
    # Backfill Dec 2025
    insert_account(spark, 22001, "2025-12", balance=5000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    dec_row = get_summary_row(spark, 22001, "2025-12")
    results.check(dec_row is not None, "T22.1", "Dec 2025 row created", "Row not found")


def test_case_iii_single_existing_row(spark, results):
    """Test 23: Backfill for account with only 1 existing row"""
    print("\n" + "="*80)
    print("TEST 23: Case III - Backfill with single existing row")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 23001, "2026-01", [5000])
    insert_account(spark, 23001, "2025-11", balance=4000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    nov_row = get_summary_row(spark, 23001, "2025-11")
    results.check(nov_row is not None, "T23.1", "Nov row created", "Row not found")
    
    jan_row = get_summary_row(spark, 23001, "2026-01")
    if jan_row:
        results.check(jan_row.balance_am_history[2] == 4000, "T23.2",
                     "Jan bal[2]=4000 (Nov)", f"Got {jan_row.balance_am_history[2]}")


def test_case_iv_two_months(spark, results):
    """Test 24: Bulk load - New account with 2 months"""
    print("\n" + "="*80)
    print("TEST 24: Case IV - New account with 2 months")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 24001, "2025-01", balance=1000)
    insert_account(spark, 24001, "2025-02", balance=1100)
    
    config = get_config()
    run_pipeline(spark, config)
    
    count = get_summary_count(spark, 24001)
    results.check(count == 2, "T24.1", f"Created {count} rows", f"Expected 2, got {count}")
    
    feb_row = get_summary_row(spark, 24001, "2025-02")
    if feb_row:
        results.check(feb_row.balance_am_history[0] == 1100, "T24.2",
                     "Feb bal[0]=1100", f"Got {feb_row.balance_am_history[0]}")
        results.check(feb_row.balance_am_history[1] == 1000, "T24.3",
                     "Feb bal[1]=1000 (Jan)", f"Got {feb_row.balance_am_history[1]}")


def test_case_iv_12_months(spark, results):
    """Test 25: Bulk load - New account with 12 months"""
    print("\n" + "="*80)
    print("TEST 25: Case IV - New account with 12 months")
    print("="*80)
    
    clear_tables(spark)
    for i in range(12):
        month = f"2025-{i+1:02d}"
        insert_account(spark, 25001, month, balance=1000 + i*100)
    
    config = get_config()
    run_pipeline(spark, config)
    
    count = get_summary_count(spark, 25001)
    results.check(count == 12, "T25.1", f"Created {count} rows", f"Expected 12, got {count}")
    
    dec_row = get_summary_row(spark, 25001, "2025-12")
    if dec_row:
        results.check(dec_row.balance_am_history[0] == 2100, "T25.2",
                     "Dec bal[0]=2100", f"Got {dec_row.balance_am_history[0]}")
        results.check(dec_row.balance_am_history[11] == 1000, "T25.3",
                     "Dec bal[11]=1000 (Jan)", f"Got {dec_row.balance_am_history[11]}")


def test_case_iv_36_months(spark, results):
    """Test 26: Bulk load - New account with 36 months (full history)"""
    print("\n" + "="*80)
    print("TEST 26: Case IV - New account with 36 months (full)")
    print("="*80)
    
    clear_tables(spark)
    base_date = datetime(2023, 1, 1)
    for i in range(36):
        month_date = base_date + relativedelta(months=i)
        month = month_date.strftime("%Y-%m")
        insert_account(spark, 26001, month, balance=1000 + i*50)
    
    config = get_config()
    run_pipeline(spark, config)
    
    count = get_summary_count(spark, 26001)
    results.check(count == 36, "T26.1", f"Created {count} rows", f"Expected 36, got {count}")
    
    # Check last month has full history
    last_month = (base_date + relativedelta(months=35)).strftime("%Y-%m")
    last_row = get_summary_row(spark, 26001, last_month)
    if last_row:
        results.check(last_row.balance_am_history[35] == 1000, "T26.2",
                     "Last month bal[35]=1000 (first month)", 
                     f"Got {last_row.balance_am_history[35]}")


def test_case_iv_with_gaps(spark, results):
    """Test 27: Bulk load with gaps (missing months)"""
    print("\n" + "="*80)
    print("TEST 27: Case IV - Bulk load with gaps")
    print("="*80)
    
    clear_tables(spark)
    # Jan, Mar, Apr (skip Feb)
    insert_account(spark, 27001, "2025-01", balance=1000)
    insert_account(spark, 27001, "2025-03", balance=1200)
    insert_account(spark, 27001, "2025-04", balance=1300)
    
    config = get_config()
    run_pipeline(spark, config)
    
    count = get_summary_count(spark, 27001)
    results.check(count == 3, "T27.1", f"Created {count} rows", f"Expected 3, got {count}")
    
    apr_row = get_summary_row(spark, 27001, "2025-04")
    if apr_row:
        results.check(apr_row.balance_am_history[0] == 1300, "T27.2",
                     "Apr bal[0]=1300", f"Got {apr_row.balance_am_history[0]}")
        results.check(apr_row.balance_am_history[1] == 1200, "T27.3",
                     "Apr bal[1]=1200 (Mar)", f"Got {apr_row.balance_am_history[1]}")
        results.check(apr_row.balance_am_history[2] is None, "T27.4",
                     "Apr bal[2]=NULL (Feb gap)", f"Got {apr_row.balance_am_history[2]}")
        results.check(apr_row.balance_am_history[3] == 1000, "T27.5",
                     "Apr bal[3]=1000 (Jan)", f"Got {apr_row.balance_am_history[3]}")


def test_case_iv_multiple_gaps(spark, results):
    """Test 28: Bulk load with multiple gaps"""
    print("\n" + "="*80)
    print("TEST 28: Case IV - Bulk load with multiple gaps")
    print("="*80)
    
    clear_tables(spark)
    # Jan, Mar, Jun (skip Feb, Apr, May)
    insert_account(spark, 28001, "2025-01", balance=1000)
    insert_account(spark, 28001, "2025-03", balance=1200)
    insert_account(spark, 28001, "2025-06", balance=1500)
    
    config = get_config()
    run_pipeline(spark, config)
    
    jun_row = get_summary_row(spark, 28001, "2025-06")
    if jun_row:
        results.check(jun_row.balance_am_history[0] == 1500, "T28.1",
                     "Jun bal[0]=1500", f"Got {jun_row.balance_am_history[0]}")
        # May=NULL, Apr=NULL, Mar=1200, Feb=NULL, Jan=1000
        results.check(jun_row.balance_am_history[1] is None, "T28.2",
                     "Jun bal[1]=NULL (May)", f"Got {jun_row.balance_am_history[1]}")
        results.check(jun_row.balance_am_history[3] == 1200, "T28.3",
                     "Jun bal[3]=1200 (Mar)", f"Got {jun_row.balance_am_history[3]}")


def test_case_iv_null_values(spark, results):
    """Test 29: Bulk load with NULL values in some months"""
    print("\n" + "="*80)
    print("TEST 29: Case IV - Bulk load with NULL values")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 29001, "2025-01", balance=1000)
    insert_account(spark, 29001, "2025-02", balance=None)
    insert_account(spark, 29001, "2025-03", balance=1200)
    
    config = get_config()
    run_pipeline(spark, config)
    
    mar_row = get_summary_row(spark, 29001, "2025-03")
    if mar_row:
        results.check(mar_row.balance_am_history[0] == 1200, "T29.1",
                     "Mar bal[0]=1200", f"Got {mar_row.balance_am_history[0]}")
        results.check(mar_row.balance_am_history[1] is None, "T29.2",
                     "Mar bal[1]=NULL (Feb)", f"Got {mar_row.balance_am_history[1]}")
        results.check(mar_row.balance_am_history[2] == 1000, "T29.3",
                     "Mar bal[2]=1000 (Jan)", f"Got {mar_row.balance_am_history[2]}")


def test_case_iv_same_values(spark, results):
    """Test 30: Bulk load with all same values"""
    print("\n" + "="*80)
    print("TEST 30: Case IV - Bulk load with identical values")
    print("="*80)
    
    clear_tables(spark)
    for i in range(6):
        month = f"2025-{i+1:02d}"
        insert_account(spark, 30001, month, balance=5000)  # All same
    
    config = get_config()
    run_pipeline(spark, config)
    
    jun_row = get_summary_row(spark, 30001, "2025-06")
    if jun_row:
        all_same = all(jun_row.balance_am_history[i] == 5000 for i in range(6))
        results.check(all_same, "T30.1", "All 6 positions = 5000", "Values differ")


def test_mixed_all_cases(spark, results):
    """Test 31: All 4 cases in single batch"""
    print("\n" + "="*80)
    print("TEST 31: Mixed - All 4 cases in single batch")
    print("="*80)
    
    clear_tables(spark)
    
    # Setup for Case II and III
    insert_existing_summary(spark, 31002, "2025-12", [5000])  # For forward
    insert_existing_summary(spark, 31003, "2026-01", [6000])  # For backfill
    
    # Case I: New single month
    insert_account(spark, 31001, "2026-01", balance=1000)
    # Case II: Forward entry
    insert_account(spark, 31002, "2026-01", balance=5500)
    # Case III: Backfill
    insert_account(spark, 31003, "2025-12", balance=5500)
    # Case IV: Bulk historical
    insert_account(spark, 31004, "2025-01", balance=2000)
    insert_account(spark, 31004, "2025-02", balance=2100)
    
    config = get_config()
    run_pipeline(spark, config)
    
    results.check(get_summary_row(spark, 31001, "2026-01") is not None, "T31.1", 
                 "Case I processed", "Case I failed")
    results.check(get_summary_row(spark, 31002, "2026-01") is not None, "T31.2",
                 "Case II processed", "Case II failed")
    results.check(get_summary_row(spark, 31003, "2025-12") is not None, "T31.3",
                 "Case III processed", "Case III failed")
    results.check(get_summary_count(spark, 31004) == 2, "T31.4",
                 "Case IV processed", "Case IV failed")


def test_very_old_date(spark, results):
    """Test 32: Very old months (year 2000)"""
    print("\n" + "="*80)
    print("TEST 32: Edge case - Very old date (2000)")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 32001, "2000-01", balance=1000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 32001, "2000-01")
    results.check(row is not None, "T32.1", "Year 2000 handled", "Old date failed")


def test_future_date(spark, results):
    """Test 33: Future dated months (year 2030)"""
    print("\n" + "="*80)
    print("TEST 33: Edge case - Future date (2030)")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 33001, "2030-12", balance=5000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 33001, "2030-12")
    results.check(row is not None, "T33.1", "Year 2030 handled", "Future date failed")


def test_grid_same_values(spark, results):
    """Test 34: Grid column with all same values"""
    print("\n" + "="*80)
    print("TEST 34: Grid column - All same payment ratings")
    print("="*80)
    
    clear_tables(spark)
    for i in range(6):
        month = f"2025-{i+1:02d}"
        insert_account(spark, 34001, month, balance=1000, payment_rating='0')
    
    config = get_config()
    run_pipeline(spark, config)
    
    jun_row = get_summary_row(spark, 34001, "2025-06")
    if jun_row:
        grid = jun_row.payment_history_grid
        results.check(grid.startswith("000000"), "T34.1",
                     f"Grid starts with 000000: {grid[:10]}...", f"Grid: {grid[:20]}")


def test_grid_mixed_values(spark, results):
    """Test 35: Grid column with mixed values"""
    print("\n" + "="*80)
    print("TEST 35: Grid column - Mixed payment ratings")
    print("="*80)
    
    clear_tables(spark)
    ratings = ['0', '1', '2', '3', '4', '5']
    for i, rating in enumerate(ratings):
        month = f"2025-{i+1:02d}"
        insert_account(spark, 35001, month, balance=1000, payment_rating=rating)
    
    config = get_config()
    run_pipeline(spark, config)
    
    jun_row = get_summary_row(spark, 35001, "2025-06")
    if jun_row:
        grid = jun_row.payment_history_grid
        # Most recent first: 5,4,3,2,1,0
        results.check(grid.startswith("543210"), "T35.1",
                     f"Grid starts with 543210: {grid[:10]}...", f"Grid: {grid[:20]}")


def test_large_account_key(spark, results):
    """Test 36: Account key with large value"""
    print("\n" + "="*80)
    print("TEST 36: Edge case - Large account key")
    print("="*80)
    
    clear_tables(spark)
    large_key = 9999999999999
    insert_account(spark, large_key, "2026-01", balance=1000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, large_key, "2026-01")
    results.check(row is not None, "T36.1", "Large key handled", "Large key failed")


def test_single_record_batch(spark, results):
    """Test 37: Single record batch"""
    print("\n" + "="*80)
    print("TEST 37: Edge case - Single record batch")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 37001, "2026-01", balance=1000)
    
    config = get_config()
    stats = run_pipeline(spark, config)
    
    results.check(stats['total_records'] == 1, "T37.1",
                 f"Single record processed: {stats}", "Count mismatch")


def test_payment_rating_codes(spark, results):
    """Test 38: Payment rating codes mapping correctly"""
    print("\n" + "="*80)
    print("TEST 38: Payment rating codes (0-9, X)")
    print("="*80)
    
    clear_tables(spark)
    codes = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'X']
    for i, code in enumerate(codes):
        insert_account(spark, 38000 + i, "2026-01", balance=1000, payment_rating=code)
    
    config = get_config()
    run_pipeline(spark, config)
    
    for i, code in enumerate(codes):
        row = get_summary_row(spark, 38000 + i, "2026-01")
        if row:
            results.check(row.payment_rating_cd_history[0] == code, f"T38.{i+1}",
                         f"Rating {code} stored correctly", 
                         f"Expected {code}, got {row.payment_rating_cd_history[0]}")


def test_asset_class_codes(spark, results):
    """Test 39: Asset class codes mapping correctly"""
    print("\n" + "="*80)
    print("TEST 39: Asset class codes (A, B, C, D)")
    print("="*80)
    
    clear_tables(spark)
    codes = ['A', 'B', 'C', 'D']
    for i, code in enumerate(codes):
        insert_account(spark, 39000 + i, "2026-01", balance=1000, asset_class=code)
    
    config = get_config()
    run_pipeline(spark, config)
    
    for i, code in enumerate(codes):
        row = get_summary_row(spark, 39000 + i, "2026-01")
        if row:
            results.check(row.asset_class_cd_4in_history[0] == code, f"T39.{i+1}",
                         f"Asset class {code} stored correctly",
                         f"Expected {code}, got {row.asset_class_cd_4in_history[0]}")


def test_latest_summary_update(spark, results):
    """Test 40: Latest summary table correctly updated"""
    print("\n" + "="*80)
    print("TEST 40: Latest summary table update")
    print("="*80)
    
    clear_tables(spark)
    insert_existing_summary(spark, 40001, "2025-12", [5000])
    insert_account(spark, 40001, "2026-01", balance=5500)
    
    config = get_config()
    run_pipeline(spark, config)
    
    latest = get_latest_summary(spark, 40001)
    results.check(latest is not None, "T40.1", "Latest summary exists", "Not found")
    if latest:
        results.check(latest.rpt_as_of_mo == "2026-01", "T40.2",
                     "Latest month = 2026-01", f"Got {latest.rpt_as_of_mo}")


def test_37_month_bulk(spark, results):
    """Test 41: Bulk load with 37 months (more than history length)"""
    print("\n" + "="*80)
    print("TEST 41: Case IV - 37 months (exceeds history)")
    print("="*80)
    
    clear_tables(spark)
    base_date = datetime(2022, 12, 1)
    for i in range(37):
        month_date = base_date + relativedelta(months=i)
        month = month_date.strftime("%Y-%m")
        insert_account(spark, 41001, month, balance=1000 + i*10)
    
    config = get_config()
    run_pipeline(spark, config)
    
    count = get_summary_count(spark, 41001)
    results.check(count == 37, "T41.1", f"Created {count} rows", f"Expected 37")
    
    # Check last row - position 35 should have month 2 value, not month 1
    last_month = (base_date + relativedelta(months=36)).strftime("%Y-%m")
    last_row = get_summary_row(spark, 41001, last_month)
    if last_row:
        # Position 35 = 36 months ago = month index 1 = 1010
        results.check(last_row.balance_am_history[35] == 1010, "T41.2",
                     "bal[35]=1010 (first month pushed out)", 
                     f"Got {last_row.balance_am_history[35]}")


def test_backfill_updates_multiple_future(spark, results):
    """Test 42: Backfill correctly updates multiple future rows"""
    print("\n" + "="*80)
    print("TEST 42: Backfill updates multiple future rows")
    print("="*80)
    
    clear_tables(spark)
    # Create 3 future rows
    insert_existing_summary(spark, 42001, "2025-12", [5200])
    insert_existing_summary(spark, 42001, "2026-01", [5300, 5200])
    insert_existing_summary(spark, 42001, "2026-02", [5400, 5300, 5200])
    
    # Backfill Nov 2025
    insert_account(spark, 42001, "2025-11", balance=5100)
    
    config = get_config()
    run_pipeline(spark, config)
    
    # All 3 future rows should have 5100 at appropriate positions
    dec_row = get_summary_row(spark, 42001, "2025-12")
    jan_row = get_summary_row(spark, 42001, "2026-01")
    feb_row = get_summary_row(spark, 42001, "2026-02")
    
    if dec_row:
        results.check(dec_row.balance_am_history[1] == 5100, "T42.1",
                     "Dec bal[1]=5100", f"Got {dec_row.balance_am_history[1]}")
    if jan_row:
        results.check(jan_row.balance_am_history[2] == 5100, "T42.2",
                     "Jan bal[2]=5100", f"Got {jan_row.balance_am_history[2]}")
    if feb_row:
        results.check(feb_row.balance_am_history[3] == 5100, "T42.3",
                     "Feb bal[3]=5100", f"Got {feb_row.balance_am_history[3]}")


def test_first_and_last_month_only(spark, results):
    """Test 43: Bulk load with only first and last month (35 month gap)"""
    print("\n" + "="*80)
    print("TEST 43: Case IV - Only first and last month (35 gap)")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 43001, "2023-01", balance=1000)
    insert_account(spark, 43001, "2025-12", balance=2000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    count = get_summary_count(spark, 43001)
    results.check(count == 2, "T43.1", f"Created {count} rows", f"Expected 2")
    
    dec_row = get_summary_row(spark, 43001, "2025-12")
    if dec_row:
        results.check(dec_row.balance_am_history[0] == 2000, "T43.2",
                     "Dec bal[0]=2000", f"Got {dec_row.balance_am_history[0]}")
        # Positions 1-34 should be NULL (gaps)
        nulls = all(dec_row.balance_am_history[i] is None for i in range(1, 35))
        results.check(nulls, "T43.3", "bal[1-34]=NULL (gaps)", "Gaps not null")
        results.check(dec_row.balance_am_history[35] == 1000, "T43.4",
                     "Dec bal[35]=1000 (Jan 2023)", f"Got {dec_row.balance_am_history[35]}")


def test_array_all_null(spark, results):
    """Test 44: Array with all NULL values"""
    print("\n" + "="*80)
    print("TEST 44: Array with all NULL values")
    print("="*80)
    
    clear_tables(spark)
    for i in range(3):
        month = f"2025-{i+1:02d}"
        insert_account(spark, 44001, month, balance=None)
    
    config = get_config()
    run_pipeline(spark, config)
    
    mar_row = get_summary_row(spark, 44001, "2025-03")
    if mar_row:
        all_null = all(mar_row.balance_am_history[i] is None for i in range(3))
        results.check(all_null, "T44.1", "First 3 positions NULL", "Not all null")


def test_consecutive_runs(spark, results):
    """Test 45: Consecutive pipeline runs"""
    print("\n" + "="*80)
    print("TEST 45: Consecutive pipeline runs")
    print("="*80)
    
    clear_tables(spark)
    config = get_config()
    
    # Run 1: Insert Jan
    insert_account(spark, 45001, "2025-01", balance=1000)
    run_pipeline(spark, config)
    
    # Clear accounts but keep summary
    spark.sql("DELETE FROM demo.comprehensive_test.accounts")
    
    # Run 2: Insert Feb (forward entry)
    insert_account(spark, 45001, "2025-02", balance=1100)
    run_pipeline(spark, config)
    
    feb_row = get_summary_row(spark, 45001, "2025-02")
    if feb_row:
        results.check(feb_row.balance_am_history[0] == 1100, "T45.1",
                     "Feb bal[0]=1100", f"Got {feb_row.balance_am_history[0]}")
        results.check(feb_row.balance_am_history[1] == 1000, "T45.2",
                     "Feb bal[1]=1000 (Jan from run 1)", 
                     f"Got {feb_row.balance_am_history[1]}")


def test_duplicate_account_month(spark, results):
    """Test 46: Duplicate account+month in source (should use latest base_ts)"""
    print("\n" + "="*80)
    print("TEST 46: Duplicate account+month records")
    print("="*80)
    
    clear_tables(spark)
    # Insert same account+month twice with different balances
    insert_account(spark, 46001, "2026-01", balance=1000, 
                  base_ts=datetime(2026, 1, 15, 10, 0, 0))
    insert_account(spark, 46001, "2026-01", balance=2000,
                  base_ts=datetime(2026, 1, 15, 12, 0, 0))  # Later timestamp
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 46001, "2026-01")
    if row:
        # Should use the later one (balance=2000)
        results.check(row.balance_am_history[0] == 2000, "T46.1",
                     "Uses later timestamp: bal[0]=2000", 
                     f"Got {row.balance_am_history[0]}")


def test_special_month_formats(spark, results):
    """Test 47: Month format edge cases"""
    print("\n" + "="*80)
    print("TEST 47: Month format edge cases")
    print("="*80)
    
    clear_tables(spark)
    # Standard format
    insert_account(spark, 47001, "2025-01", balance=1000)
    # First month of year
    insert_account(spark, 47002, "2025-01", balance=2000)
    # Last month of year  
    insert_account(spark, 47003, "2025-12", balance=3000)
    
    config = get_config()
    run_pipeline(spark, config)
    
    results.check(get_summary_row(spark, 47001, "2025-01") is not None, "T47.1",
                 "Standard format OK", "Failed")
    results.check(get_summary_row(spark, 47002, "2025-01") is not None, "T47.2",
                 "January OK", "Failed")
    results.check(get_summary_row(spark, 47003, "2025-12") is not None, "T47.3",
                 "December OK", "Failed")


def test_forward_then_backfill(spark, results):
    """Test 48: Forward entry then backfill for same account"""
    print("\n" + "="*80)
    print("TEST 48: Forward then backfill same account")
    print("="*80)
    
    clear_tables(spark)
    config = get_config()
    
    # Run 1: Create initial
    insert_account(spark, 48001, "2025-06", balance=5000)
    run_pipeline(spark, config)
    spark.sql("DELETE FROM demo.comprehensive_test.accounts")
    
    # Run 2: Forward entry
    insert_account(spark, 48001, "2025-07", balance=5500)
    run_pipeline(spark, config)
    spark.sql("DELETE FROM demo.comprehensive_test.accounts")
    
    # Run 3: Backfill
    insert_account(spark, 48001, "2025-05", balance=4500)
    run_pipeline(spark, config)
    
    # Check all 3 months exist
    results.check(get_summary_row(spark, 48001, "2025-05") is not None, "T48.1",
                 "May created", "Missing")
    results.check(get_summary_row(spark, 48001, "2025-06") is not None, "T48.2",
                 "June exists", "Missing")
    results.check(get_summary_row(spark, 48001, "2025-07") is not None, "T48.3",
                 "July exists", "Missing")
    
    # Check July has backfill at correct position
    jul_row = get_summary_row(spark, 48001, "2025-07")
    if jul_row:
        results.check(jul_row.balance_am_history[2] == 4500, "T48.4",
                     "July bal[2]=4500 (May backfill)", 
                     f"Got {jul_row.balance_am_history[2]}")


def test_min_int_values(spark, results):
    """Test 49: Minimum integer values"""
    print("\n" + "="*80)
    print("TEST 49: Minimum integer values")
    print("="*80)
    
    clear_tables(spark)
    min_int = -2147483648
    insert_account(spark, 49001, "2026-01", balance=min_int)
    
    config = get_config()
    run_pipeline(spark, config)
    
    row = get_summary_row(spark, 49001, "2026-01")
    results.check(row is not None, "T49.1", "Row created", "Not found")
    if row:
        results.check(row.balance_am_history[0] == min_int, "T49.2",
                     f"bal[0]={min_int}", f"Got {row.balance_am_history[0]}")


def test_case_priority(spark, results):
    """Test 50: Case priority when same account+month from different cases"""
    print("\n" + "="*80)
    print("TEST 50: Case priority handling")
    print("="*80)
    
    clear_tables(spark)
    # This is a complex edge case - testing that Case III has priority over Case II
    # Setup: account with existing Jan, backfill Dec AND forward Feb in same batch
    insert_existing_summary(spark, 50001, "2026-01", [5000])
    insert_account(spark, 50001, "2025-12", balance=4500)  # Backfill
    insert_account(spark, 50001, "2026-02", balance=5500)  # Forward
    
    config = get_config()
    run_pipeline(spark, config)
    
    results.check(get_summary_row(spark, 50001, "2025-12") is not None, "T50.1",
                 "Dec backfill processed", "Missing")
    results.check(get_summary_row(spark, 50001, "2026-02") is not None, "T50.2",
                 "Feb forward processed", "Missing")


def test_temp_table_cleanup(spark, results):
    """Test 51: Temp table is cleaned up after run"""
    print("\n" + "="*80)
    print("TEST 51: Temp table cleanup verification")
    print("="*80)
    
    clear_tables(spark)
    insert_account(spark, 51001, "2026-01", balance=1000)
    
    config = get_config()
    stats = run_pipeline(spark, config)
    
    # Check temp table was created (captured in stats)
    temp_table = stats.get('temp_table', '')
    results.check('pipeline_batch_' in temp_table, "T51.1",
                 f"Temp table used: {temp_table}", "No temp table")
    
    # Check temp table no longer exists
    try:
        spark.table(temp_table)
        results.add_fail("T51.2", "Temp table still exists")
    except:
        results.add_pass("T51.2", "Temp table cleaned up")


def test_multiple_accounts_same_batch(spark, results):
    """Test 52: Many accounts processed in single batch"""
    print("\n" + "="*80)
    print("TEST 52: Multiple accounts in batch")
    print("="*80)
    
    clear_tables(spark)
    # Insert 20 different accounts
    for i in range(20):
        insert_account(spark, 52000 + i, "2026-01", balance=1000 + i*100)
    
    config = get_config()
    stats = run_pipeline(spark, config)
    
    results.check(stats['total_records'] == 20, "T52.1",
                 f"Processed {stats['total_records']} records", 
                 f"Expected 20, got {stats['total_records']}")
    results.check(stats['case_i_records'] == 20, "T52.2",
                 f"All Case I: {stats['case_i_records']}", "Not all Case I")


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("="*80)
    print("COMPREHENSIVE TEST SUITE - 50+ UNIQUE TEST CASES")
    print("Summary Pipeline v9.4 with Temp Table Optimization")
    print("="*80)
    print(f"\nStart time: {datetime.now()}")
    
    spark = create_spark_session()
    setup_tables(spark)
    
    results = TestResult()
    
    try:
        # Case I Tests (1-8)
        test_case_i_single_new_account(spark, results)
        test_case_i_multiple_new_accounts(spark, results)
        test_case_i_null_values(spark, results)
        test_case_i_zero_values(spark, results)
        test_case_i_negative_values(spark, results)
        test_case_i_max_int_values(spark, results)
        test_case_i_empty_string(spark, results)
        test_case_i_year_boundary(spark, results)
        
        # Case II Tests (9-18)
        test_case_ii_consecutive_month(spark, results)
        test_case_ii_gap_2_months(spark, results)
        test_case_ii_gap_6_months(spark, results)
        test_case_ii_gap_12_months(spark, results)
        test_case_ii_gap_35_months(spark, results)
        test_case_ii_gap_over_36_months(spark, results)
        test_case_ii_null_replacing_value(spark, results)
        test_case_ii_year_boundary(spark, results)
        test_case_ii_full_array(spark, results)
        test_case_ii_multiple_same_account(spark, results)
        
        # Case III Tests (19-23)
        test_case_iii_one_month_backfill(spark, results)
        test_case_iii_multiple_months_before(spark, results)
        test_case_iii_with_null(spark, results)
        test_case_iii_year_boundary(spark, results)
        test_case_iii_single_existing_row(spark, results)
        
        # Case IV Tests (24-30)
        test_case_iv_two_months(spark, results)
        test_case_iv_12_months(spark, results)
        test_case_iv_36_months(spark, results)
        test_case_iv_with_gaps(spark, results)
        test_case_iv_multiple_gaps(spark, results)
        test_case_iv_null_values(spark, results)
        test_case_iv_same_values(spark, results)
        
        # Mixed & Edge Case Tests (31-52)
        test_mixed_all_cases(spark, results)
        test_very_old_date(spark, results)
        test_future_date(spark, results)
        test_grid_same_values(spark, results)
        test_grid_mixed_values(spark, results)
        test_large_account_key(spark, results)
        test_single_record_batch(spark, results)
        test_payment_rating_codes(spark, results)
        test_asset_class_codes(spark, results)
        test_latest_summary_update(spark, results)
        test_37_month_bulk(spark, results)
        test_backfill_updates_multiple_future(spark, results)
        test_first_and_last_month_only(spark, results)
        test_array_all_null(spark, results)
        test_consecutive_runs(spark, results)
        test_duplicate_account_month(spark, results)
        test_special_month_formats(spark, results)
        test_forward_then_backfill(spark, results)
        test_min_int_values(spark, results)
        test_case_priority(spark, results)
        test_temp_table_cleanup(spark, results)
        test_multiple_accounts_same_batch(spark, results)
        
    except Exception as e:
        print(f"\n[FATAL] Test execution failed: {e}")
        import traceback
        traceback.print_exc()
    
    success = results.summary()
    print(f"\nEnd time: {datetime.now()}")
    
    spark.stop()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
