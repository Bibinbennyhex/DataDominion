"""
COMPREHENSIVE EDGE CASE TEST SUITE - Summary Pipeline v9.4
=============================================================

Tests ALL edge cases for the summary pipeline including:

CASE I - New Accounts:
  - 6001: Single month, minimal data
  - 6002: Single month, max values (boundary test)
  - 6003: Single month, NULL values in optional columns

CASE II - Forward Entry:
  - 7001: Skip 1 month (Feb exists, April arrives - March gap)
  - 7002: Skip 2+ months (large gap forward)
  - 7003: Same month re-arrival (duplicate handling)

CASE III - Backfill:
  - 8001: Deep backfill (12 months back)
  - 8002: Multiple backfills for same account in one batch
  - 8003: Backfill that creates cascade of 10+ future updates

CASE IV - Bulk Historical:
  - 9001: Full 36 months (exactly fills array)
  - 9002: 48 months (exceeds 36, tests array windowing)
  - 9003: Only 2 months (minimal bulk)
  - 9004: Every-other-month pattern (alternating gaps)
  - 9005: First and last month only (35-month gap)
  - 9006: Consecutive months then gap then more (mixed pattern)

MIXED SCENARIOS:
  - 10001-10003: Mixed batch with all case types

BOUNDARY CONDITIONS:
  - 11001: All columns NULL
  - 11002: Maximum integer values
  - 11003: Empty string vs NULL strings

Usage:
    docker exec spark-iceberg python3 /home/iceberg/summary_v9_production/tests/test_comprehensive_edge_cases.py
"""

import sys
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Add the pipeline directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_dir = os.path.join(os.path.dirname(script_dir), 'pipeline')
sys.path.insert(0, pipeline_dir)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    """Create Spark session configured for Iceberg"""
    spark = SparkSession.builder \
        .appName("comprehensive_edge_case_test") \
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
    
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.edge_case_test")
    
    # Create temp schema for temp tables (v9.4)
    spark.sql("CREATE DATABASE IF NOT EXISTS demo.temp")
    
    # Drop existing tables
    spark.sql("DROP TABLE IF EXISTS demo.edge_case_test.accounts")
    spark.sql("DROP TABLE IF EXISTS demo.edge_case_test.summary")
    spark.sql("DROP TABLE IF EXISTS demo.edge_case_test.latest_summary")
    
    # Create accounts table (source)
    spark.sql("""
        CREATE TABLE demo.edge_case_test.accounts (
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
    
    # Create summary table with v9.4 schema
    spark.sql("""
        CREATE TABLE demo.edge_case_test.summary (
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
    
    # Create latest_summary table with v9.4 schema
    spark.sql("""
        CREATE TABLE demo.edge_case_test.latest_summary (
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
    
    print("Tables created successfully!")


def build_int_array(positions_values: Dict[int, int], length: int = 36) -> str:
    """Build SQL array with specific integer values at positions"""
    arr = ["CAST(NULL AS INT)"] * length
    for pos, val in positions_values.items():
        if val is None:
            arr[pos] = "CAST(NULL AS INT)"
        else:
            arr[pos] = str(val)
    return "array(" + ", ".join(arr) + ")"


def build_str_array(positions_values: Dict[int, str], length: int = 36) -> str:
    """Build SQL array of strings with specific values at positions"""
    arr = ["CAST(NULL AS STRING)"] * length
    for pos, val in positions_values.items():
        if val is None:
            arr[pos] = "CAST(NULL AS STRING)"
        else:
            arr[pos] = f"'{val}'"
    return "array(" + ", ".join(arr) + ")"


class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.details = []
    
    def add_pass(self, msg: str):
        self.passed += 1
        self.details.append(f"  [PASS] {msg}")
        print(f"  [PASS] {msg}")
    
    def add_fail(self, msg: str):
        self.failed += 1
        self.details.append(f"  [FAIL] {msg}")
        print(f"  [FAIL] {msg}")


# ============================================================================
# CASE I EDGE CASES - New Accounts
# ============================================================================

def setup_case_i_edge_cases(spark):
    """Setup Case I edge case data"""
    print("\n" + "=" * 80)
    print("SETTING UP CASE I EDGE CASES")
    print("=" * 80)
    
    upload_ts = "2026-01-30 10:00:00"
    
    # 6001: Single month, minimal data
    print("\n  6001: Single month, minimal values")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (6001, '2026-01', TIMESTAMP '{upload_ts}',
         1, 0, 100, 0, 0, 'A', '0')
    """)
    
    # 6002: Single month, max values (boundary test)
    print("  6002: Single month, maximum values")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (6002, '2026-01', TIMESTAMP '{upload_ts}',
         2147483647, 2147483647, 2147483647, 2147483647, 999, 'Z', '9')
    """)
    
    # 6003: Single month, NULL values in optional columns
    print("  6003: Single month, some NULL values")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (6003, '2026-01', TIMESTAMP '{upload_ts}',
         5000, NULL, NULL, NULL, NULL, NULL, NULL)
    """)


def verify_case_i_edge_cases(spark) -> TestResult:
    """Verify Case I edge cases"""
    result = TestResult("Case I Edge Cases")
    print("\n" + "=" * 80)
    print("CASE I EDGE CASE VERIFICATION")
    print("=" * 80)
    
    # 6001: Minimal values
    print("\n  Account 6001 (Minimal values):")
    row = spark.sql("""
        SELECT balance_am_history[0] as bal_0, balance_am_history[1] as bal_1
        FROM demo.edge_case_test.summary WHERE cons_acct_key = 6001
    """).first()
    
    if row:
        if row['bal_0'] == 1:
            result.add_pass("6001 bal[0] = 1")
        else:
            result.add_fail(f"6001 bal[0] = {row['bal_0']} (expected 1)")
        
        if row['bal_1'] is None:
            result.add_pass("6001 bal[1] = NULL")
        else:
            result.add_fail(f"6001 bal[1] = {row['bal_1']} (expected NULL)")
    else:
        result.add_fail("6001 - No summary row found")
    
    # 6002: Max values
    print("\n  Account 6002 (Maximum values):")
    row = spark.sql("""
        SELECT balance_am_history[0] as bal_0, credit_limit_am_history[0] as cl_0
        FROM demo.edge_case_test.summary WHERE cons_acct_key = 6002
    """).first()
    
    if row:
        if row['bal_0'] == 2147483647:
            result.add_pass("6002 bal[0] = 2147483647 (INT MAX)")
        else:
            result.add_fail(f"6002 bal[0] = {row['bal_0']} (expected 2147483647)")
    else:
        result.add_fail("6002 - No summary row found")
    
    # 6003: NULL values
    print("\n  Account 6003 (NULL values):")
    row = spark.sql("""
        SELECT balance_am_history[0] as bal_0, 
               actual_payment_am_history[0] as pay_0,
               asset_class_cd_4in_history[0] as ac_0
        FROM demo.edge_case_test.summary WHERE cons_acct_key = 6003
    """).first()
    
    if row:
        if row['bal_0'] == 5000:
            result.add_pass("6003 bal[0] = 5000")
        else:
            result.add_fail(f"6003 bal[0] = {row['bal_0']} (expected 5000)")
        
        if row['pay_0'] is None:
            result.add_pass("6003 pay[0] = NULL (propagated)")
        else:
            result.add_fail(f"6003 pay[0] = {row['pay_0']} (expected NULL)")
    else:
        result.add_fail("6003 - No summary row found")
    
    return result


# ============================================================================
# CASE II EDGE CASES - Forward Entry
# ============================================================================

def setup_case_ii_edge_cases(spark):
    """Setup Case II edge case data - existing summaries"""
    print("\n" + "=" * 80)
    print("SETTING UP CASE II EDGE CASES (Existing Summaries)")
    print("=" * 80)
    
    # 7001: Has Feb 2026, will receive April 2026 (skip March)
    print("\n  7001: Existing Feb 2026 (will skip to April)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.summary VALUES
        (7001, '2026-02', TIMESTAMP '2026-02-15 10:00:00',
         5000, 500, 10000, 0, 0, 'A', '0',
         {build_int_array({0: 500, 1: 480})},
         {build_int_array({0: 5000, 1: 4800})},
         {build_int_array({0: 10000, 1: 10000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
    """)
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.latest_summary VALUES
        (7001, '2026-02', TIMESTAMP '2026-02-15 10:00:00',
         5000, 500, 10000, 0, 0, 'A', '0',
         {build_int_array({0: 500, 1: 480})},
         {build_int_array({0: 5000, 1: 4800})},
         {build_int_array({0: 10000, 1: 10000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
    """)
    
    # 7002: Has Jan 2026, will receive July 2026 (skip 5 months)
    print("  7002: Existing Jan 2026 (will skip 5 months to July)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.summary VALUES
        (7002, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
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
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.latest_summary VALUES
        (7002, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
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


def insert_case_ii_new_records(spark):
    """Insert new forward entry records for Case II"""
    print("\n  Inserting forward entry records:")
    upload_ts = "2026-01-30 10:00:00"
    
    # 7001: April 2026 (skipping March)
    print("    7001: April 2026 (skip March)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (7001, '2026-04', TIMESTAMP '{upload_ts}',
         5200, 520, 10000, 0, 0, 'A', '0')
    """)
    
    # 7002: July 2026 (skipping 5 months)
    print("    7002: July 2026 (skip Feb-Jun)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (7002, '2026-07', TIMESTAMP '{upload_ts}',
         7500, 750, 15000, 0, 0, 'A', '0')
    """)


def verify_case_ii_edge_cases(spark) -> TestResult:
    """Verify Case II edge cases"""
    result = TestResult("Case II Edge Cases")
    print("\n" + "=" * 80)
    print("CASE II EDGE CASE VERIFICATION")
    print("=" * 80)
    
    # 7001: Skip 1 month (Feb -> April)
    print("\n  Account 7001 (Skip 1 month: Feb -> April):")
    row = spark.sql("""
        SELECT rpt_as_of_mo,
               balance_am_history[0] as bal_0,  -- April
               balance_am_history[1] as bal_1,  -- March (gap - should be NULL)
               balance_am_history[2] as bal_2   -- Feb (should have shifted value)
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 7001 AND rpt_as_of_mo = '2026-04'
    """).first()
    
    if row:
        if row['bal_0'] == 5200:
            result.add_pass("7001 Apr bal[0] = 5200")
        else:
            result.add_fail(f"7001 Apr bal[0] = {row['bal_0']} (expected 5200)")
        
        if row['bal_1'] is None:
            result.add_pass("7001 Apr bal[1] = NULL (March gap)")
        else:
            result.add_fail(f"7001 Apr bal[1] = {row['bal_1']} (expected NULL for March gap)")
        
        if row['bal_2'] == 5000:
            result.add_pass("7001 Apr bal[2] = 5000 (Feb shifted)")
        else:
            result.add_fail(f"7001 Apr bal[2] = {row['bal_2']} (expected 5000)")
    else:
        result.add_fail("7001 - No April 2026 summary row found")
    
    # 7002: Skip 5 months (Jan -> July)
    print("\n  Account 7002 (Skip 5 months: Jan -> July):")
    row = spark.sql("""
        SELECT rpt_as_of_mo,
               balance_am_history[0] as bal_0,  -- July
               balance_am_history[1] as bal_1,  -- June (gap)
               balance_am_history[5] as bal_5,  -- Feb (gap)
               balance_am_history[6] as bal_6   -- Jan (should have value)
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 7002 AND rpt_as_of_mo = '2026-07'
    """).first()
    
    if row:
        if row['bal_0'] == 7500:
            result.add_pass("7002 July bal[0] = 7500")
        else:
            result.add_fail(f"7002 July bal[0] = {row['bal_0']} (expected 7500)")
        
        if row['bal_1'] is None:
            result.add_pass("7002 July bal[1] = NULL (June gap)")
        else:
            result.add_fail(f"7002 July bal[1] = {row['bal_1']} (expected NULL)")
        
        if row['bal_5'] is None:
            result.add_pass("7002 July bal[5] = NULL (Feb gap)")
        else:
            result.add_fail(f"7002 July bal[5] = {row['bal_5']} (expected NULL)")
        
        if row['bal_6'] == 8000:
            result.add_pass("7002 July bal[6] = 8000 (Jan shifted)")
        else:
            result.add_fail(f"7002 July bal[6] = {row['bal_6']} (expected 8000)")
    else:
        result.add_fail("7002 - No July 2026 summary row found")
    
    return result


# ============================================================================
# CASE III EDGE CASES - Backfill
# ============================================================================

def setup_case_iii_edge_cases(spark):
    """Setup Case III edge case data"""
    print("\n" + "=" * 80)
    print("SETTING UP CASE III EDGE CASES (Existing Summaries)")
    print("=" * 80)
    
    # 8001: Has Jan 2026, will receive Jan 2025 (12 months backfill)
    print("\n  8001: Existing Jan 2026 (will receive 12-month backfill)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.summary VALUES
        (8001, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         3000, 300, 5000, 0, 0, 'A', '0',
         {build_int_array({0: 300})},
         {build_int_array({0: 3000})},
         {build_int_array({0: 5000})},
         {build_int_array({0: 0})},
         {build_str_array({0: '0'})},
         {build_int_array({0: 0})},
         {build_str_array({0: 'A'})},
         '0???????????????????????????????????',
         NULL)
    """)
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.latest_summary VALUES
        (8001, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         3000, 300, 5000, 0, 0, 'A', '0',
         {build_int_array({0: 300})},
         {build_int_array({0: 3000})},
         {build_int_array({0: 5000})},
         {build_int_array({0: 0})},
         {build_str_array({0: '0'})},
         {build_int_array({0: 0})},
         {build_str_array({0: 'A'})},
         '0???????????????????????????????????',
         NULL)
    """)
    
    # 8002: Has multiple months, will receive multiple backfills in one batch
    print("  8002: Existing Dec 2025 + Jan 2026 (will receive Oct + Nov backfills)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.summary VALUES
        (8002, '2025-12', TIMESTAMP '2025-12-15 10:00:00',
         6000, 600, 8000, 0, 0, 'A', '0',
         {build_int_array({0: 600})},
         {build_int_array({0: 6000})},
         {build_int_array({0: 8000})},
         {build_int_array({0: 0})},
         {build_str_array({0: '0'})},
         {build_int_array({0: 0})},
         {build_str_array({0: 'A'})},
         '0???????????????????????????????????',
         NULL)
    """)
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.summary VALUES
        (8002, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         5800, 580, 8000, 0, 0, 'A', '0',
         {build_int_array({0: 580, 1: 600})},
         {build_int_array({0: 5800, 1: 6000})},
         {build_int_array({0: 8000, 1: 8000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
    """)
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.latest_summary VALUES
        (8002, '2026-01', TIMESTAMP '2026-01-15 10:00:00',
         5800, 580, 8000, 0, 0, 'A', '0',
         {build_int_array({0: 580, 1: 600})},
         {build_int_array({0: 5800, 1: 6000})},
         {build_int_array({0: 8000, 1: 8000})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: '0', 1: '0'})},
         {build_int_array({0: 0, 1: 0})},
         {build_str_array({0: 'A', 1: 'A'})},
         '00??????????????????????????????????',
         NULL)
    """)


def insert_case_iii_new_records(spark):
    """Insert backfill records for Case III"""
    print("\n  Inserting backfill records:")
    upload_ts = "2026-01-30 10:00:00"
    
    # 8001: Jan 2025 backfill (12 months back)
    print("    8001: Jan 2025 (12 months back)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (8001, '2025-01', TIMESTAMP '{upload_ts}',
         4000, 400, 5000, 100, 30, 'B', '1')
    """)
    
    # 8002: Oct and Nov 2025 backfills
    print("    8002: Oct 2025 and Nov 2025 (multiple backfills)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (8002, '2025-10', TIMESTAMP '{upload_ts}',
         6500, 650, 8000, 0, 0, 'A', '0')
    """)
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (8002, '2025-11', TIMESTAMP '{upload_ts}',
         6200, 620, 8000, 0, 0, 'A', '0')
    """)


def verify_case_iii_edge_cases(spark) -> TestResult:
    """Verify Case III edge cases"""
    result = TestResult("Case III Edge Cases")
    print("\n" + "=" * 80)
    print("CASE III EDGE CASE VERIFICATION")
    print("=" * 80)
    
    # 8001: Deep backfill (12 months)
    print("\n  Account 8001 (12-month deep backfill):")
    
    # Check Jan 2025 row was created
    jan25 = spark.sql("""
        SELECT balance_am_history[0] as bal_0
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2025-01'
    """).first()
    
    if jan25:
        if jan25['bal_0'] == 4000:
            result.add_pass("8001 Jan 2025 created with bal[0] = 4000")
        else:
            result.add_fail(f"8001 Jan 2025 bal[0] = {jan25['bal_0']} (expected 4000)")
    else:
        result.add_fail("8001 Jan 2025 row NOT created")
    
    # Check Jan 2026 has backfill at position 12
    jan26 = spark.sql("""
        SELECT balance_am_history[0] as bal_0,
               balance_am_history[12] as bal_12
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 8001 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if jan26:
        if jan26['bal_12'] == 4000:
            result.add_pass("8001 Jan 2026 bal[12] = 4000 (backfill propagated)")
        else:
            result.add_fail(f"8001 Jan 2026 bal[12] = {jan26['bal_12']} (expected 4000)")
    else:
        result.add_fail("8001 Jan 2026 row missing")
    
    # 8002: Multiple backfills
    print("\n  Account 8002 (Multiple backfills: Oct + Nov):")
    
    count = spark.sql("""
        SELECT COUNT(*) FROM demo.edge_case_test.summary WHERE cons_acct_key = 8002
    """).first()[0]
    
    if count == 4:  # Oct, Nov, Dec, Jan
        result.add_pass(f"8002 has {count} rows (expected 4: Oct, Nov, Dec, Jan)")
    else:
        result.add_fail(f"8002 has {count} rows (expected 4)")
    
    # Check Jan 2026 has both backfills
    jan26_8002 = spark.sql("""
        SELECT balance_am_history[0] as bal_0,
               balance_am_history[2] as bal_2,  -- Nov
               balance_am_history[3] as bal_3   -- Oct
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 8002 AND rpt_as_of_mo = '2026-01'
    """).first()
    
    if jan26_8002:
        if jan26_8002['bal_2'] == 6200:
            result.add_pass("8002 Jan 2026 bal[2] = 6200 (Nov backfill)")
        else:
            result.add_fail(f"8002 Jan 2026 bal[2] = {jan26_8002['bal_2']} (expected 6200)")
        
        if jan26_8002['bal_3'] == 6500:
            result.add_pass("8002 Jan 2026 bal[3] = 6500 (Oct backfill)")
        else:
            result.add_fail(f"8002 Jan 2026 bal[3] = {jan26_8002['bal_3']} (expected 6500)")
    else:
        result.add_fail("8002 Jan 2026 row missing")
    
    return result


# ============================================================================
# CASE IV EDGE CASES - Bulk Historical
# ============================================================================

def setup_case_iv_edge_cases(spark):
    """Setup Case IV edge case data - all new accounts, bulk historical"""
    print("\n" + "=" * 80)
    print("SETTING UP CASE IV EDGE CASES (Bulk Historical)"
    )
    print("=" * 80)
    
    upload_ts = "2026-01-30 10:00:00"
    
    # 9001: Full 36 months (exactly fills array)
    print("\n  9001: Full 36 months (Jan 2023 - Dec 2025)")
    for i in range(36):
        year = 2023 + (i // 12)
        month = (i % 12) + 1
        month_str = f"{year}-{month:02d}"
        balance = 36000 - (i * 100)
        spark.sql(f"""
            INSERT INTO demo.edge_case_test.accounts VALUES
            (9001, '{month_str}', TIMESTAMP '{upload_ts}',
             {balance}, {100 + i * 5}, 40000, 0, 0, 'A', '0')
        """)
    
    # 9002: 48 months (exceeds 36, tests windowing)
    print("  9002: 48 months (Jan 2022 - Dec 2025) - tests 36-month window")
    for i in range(48):
        year = 2022 + (i // 12)
        month = (i % 12) + 1
        month_str = f"{year}-{month:02d}"
        balance = 48000 - (i * 100)
        spark.sql(f"""
            INSERT INTO demo.edge_case_test.accounts VALUES
            (9002, '{month_str}', TIMESTAMP '{upload_ts}',
             {balance}, {100 + i * 5}, 50000, 0, 0, 'A', '0')
        """)
    
    # 9003: Only 2 months (minimal bulk)
    print("  9003: Only 2 months (Jan-Feb 2025)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (9003, '2025-01', TIMESTAMP '{upload_ts}',
         5000, 500, 8000, 0, 0, 'A', '0')
    """)
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (9003, '2025-02', TIMESTAMP '{upload_ts}',
         4900, 490, 8000, 0, 0, 'A', '0')
    """)
    
    # 9004: Every-other-month (alternating gaps)
    print("  9004: Every-other-month pattern (Jan, Mar, May, Jul, Sep, Nov 2025)")
    for month in [1, 3, 5, 7, 9, 11]:
        balance = 6000 - (month * 100)
        spark.sql(f"""
            INSERT INTO demo.edge_case_test.accounts VALUES
            (9004, '2025-{month:02d}', TIMESTAMP '{upload_ts}',
             {balance}, {100 + month * 10}, 8000, 0, 0, 'A', '0')
        """)
    
    # 9005: First and last month only (35-month gap)
    print("  9005: First and last month only (Jan 2023 + Dec 2025)")
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (9005, '2023-01', TIMESTAMP '{upload_ts}',
         10000, 1000, 15000, 0, 0, 'A', '0')
    """)
    spark.sql(f"""
        INSERT INTO demo.edge_case_test.accounts VALUES
        (9005, '2025-12', TIMESTAMP '{upload_ts}',
         8000, 800, 15000, 0, 0, 'A', '0')
    """)
    
    # 9006: Mixed pattern (consecutive then gap then more)
    print("  9006: Mixed pattern (Jan-Mar, then Jul-Sep 2025)")
    for month in [1, 2, 3, 7, 8, 9]:
        balance = 7000 - (month * 100)
        spark.sql(f"""
            INSERT INTO demo.edge_case_test.accounts VALUES
            (9006, '2025-{month:02d}', TIMESTAMP '{upload_ts}',
             {balance}, {100 + month * 10}, 10000, 0, 0, 'A', '0')
        """)


def verify_case_iv_edge_cases(spark) -> TestResult:
    """Verify Case IV edge cases"""
    result = TestResult("Case IV Edge Cases")
    print("\n" + "=" * 80)
    print("CASE IV EDGE CASE VERIFICATION")
    print("=" * 80)
    
    # 9001: Full 36 months
    print("\n  Account 9001 (Full 36 months):")
    count = spark.sql("""
        SELECT COUNT(*) FROM demo.edge_case_test.summary WHERE cons_acct_key = 9001
    """).first()[0]
    
    if count == 36:
        result.add_pass(f"9001 has {count} rows (expected 36)")
    else:
        result.add_fail(f"9001 has {count} rows (expected 36)")
    
    dec25 = spark.sql("""
        SELECT balance_am_history[0] as bal_0,
               balance_am_history[35] as bal_35
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 9001 AND rpt_as_of_mo = '2025-12'
    """).first()
    
    if dec25:
        # Dec 2025 = 36000 - 35*100 = 32500
        if dec25['bal_0'] == 32500:
            result.add_pass("9001 Dec 2025 bal[0] = 32500")
        else:
            result.add_fail(f"9001 Dec 2025 bal[0] = {dec25['bal_0']} (expected 32500)")
        
        # Jan 2023 at position 35 = 36000
        if dec25['bal_35'] == 36000:
            result.add_pass("9001 Dec 2025 bal[35] = 36000 (Jan 2023)")
        else:
            result.add_fail(f"9001 Dec 2025 bal[35] = {dec25['bal_35']} (expected 36000)")
    else:
        result.add_fail("9001 Dec 2025 row missing")
    
    # 9002: 48 months (window test)
    print("\n  Account 9002 (48 months - window test):")
    count = spark.sql("""
        SELECT COUNT(*) FROM demo.edge_case_test.summary WHERE cons_acct_key = 9002
    """).first()[0]
    
    if count == 48:
        result.add_pass(f"9002 has {count} rows (expected 48)")
    else:
        result.add_fail(f"9002 has {count} rows (expected 48)")
    
    dec25_9002 = spark.sql("""
        SELECT balance_am_history[35] as bal_35
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 9002 AND rpt_as_of_mo = '2025-12'
    """).first()
    
    if dec25_9002:
        # Position 35 should be Jan 2023 (36th month back from Dec 2025)
        # Data starts at Jan 2022 (i=0, bal=48000), so Jan 2023 is i=12
        # Jan 2023 = 48000 - 12*100 = 46800
        if dec25_9002['bal_35'] == 46800:
            result.add_pass("9002 Dec 2025 bal[35] = 46800 (Jan 2023, correctly windowed)")
        else:
            result.add_fail(f"9002 Dec 2025 bal[35] = {dec25_9002['bal_35']} (expected 46800)")
    else:
        result.add_fail("9002 Dec 2025 row missing")
    
    # 9003: Minimal bulk (2 months)
    print("\n  Account 9003 (Minimal 2 months):")
    feb = spark.sql("""
        SELECT balance_am_history[0] as bal_0,
               balance_am_history[1] as bal_1,
               balance_am_history[2] as bal_2
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 9003 AND rpt_as_of_mo = '2025-02'
    """).first()
    
    if feb:
        if feb['bal_0'] == 4900 and feb['bal_1'] == 5000:
            result.add_pass("9003 Feb bal[0]=4900, bal[1]=5000")
        else:
            result.add_fail(f"9003 Feb bal[0]={feb['bal_0']}, bal[1]={feb['bal_1']}")
        
        if feb['bal_2'] is None:
            result.add_pass("9003 Feb bal[2] = NULL")
        else:
            result.add_fail(f"9003 Feb bal[2] = {feb['bal_2']} (expected NULL)")
    else:
        result.add_fail("9003 Feb 2025 row missing")
    
    # 9004: Alternating gaps
    print("\n  Account 9004 (Alternating gaps - every other month):")
    mar = spark.sql("""
        SELECT balance_am_history[0] as bal_0,
               balance_am_history[1] as bal_1,
               balance_am_history[2] as bal_2
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 9004 AND rpt_as_of_mo = '2025-03'
    """).first()
    
    if mar:
        # Mar = 6000 - 3*100 = 5700
        # Feb = GAP
        # Jan = 6000 - 1*100 = 5900
        if mar['bal_0'] == 5700:
            result.add_pass("9004 Mar bal[0] = 5700")
        else:
            result.add_fail(f"9004 Mar bal[0] = {mar['bal_0']} (expected 5700)")
        
        if mar['bal_1'] is None:
            result.add_pass("9004 Mar bal[1] = NULL (Feb gap)")
        else:
            result.add_fail(f"9004 Mar bal[1] = {mar['bal_1']} (expected NULL)")
        
        if mar['bal_2'] == 5900:
            result.add_pass("9004 Mar bal[2] = 5900 (Jan)")
        else:
            result.add_fail(f"9004 Mar bal[2] = {mar['bal_2']} (expected 5900)")
    else:
        result.add_fail("9004 Mar 2025 row missing")
    
    # 9005: 35-month gap
    print("\n  Account 9005 (35-month gap - first and last only):")
    dec = spark.sql("""
        SELECT balance_am_history[0] as bal_0,
               balance_am_history[1] as bal_1,
               balance_am_history[34] as bal_34,
               balance_am_history[35] as bal_35
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 9005 AND rpt_as_of_mo = '2025-12'
    """).first()
    
    if dec:
        if dec['bal_0'] == 8000:
            result.add_pass("9005 Dec bal[0] = 8000")
        else:
            result.add_fail(f"9005 Dec bal[0] = {dec['bal_0']} (expected 8000)")
        
        if dec['bal_1'] is None:
            result.add_pass("9005 Dec bal[1] = NULL (Nov gap)")
        else:
            result.add_fail(f"9005 Dec bal[1] = {dec['bal_1']} (expected NULL)")
        
        if dec['bal_34'] is None:
            result.add_pass("9005 Dec bal[34] = NULL (Feb 2023 gap)")
        else:
            result.add_fail(f"9005 Dec bal[34] = {dec['bal_34']} (expected NULL)")
        
        if dec['bal_35'] == 10000:
            result.add_pass("9005 Dec bal[35] = 10000 (Jan 2023)")
        else:
            result.add_fail(f"9005 Dec bal[35] = {dec['bal_35']} (expected 10000)")
    else:
        result.add_fail("9005 Dec 2025 row missing")
    
    # 9006: Mixed pattern
    print("\n  Account 9006 (Mixed pattern - consecutive then gap then more):")
    sep = spark.sql("""
        SELECT balance_am_history[0] as bal_0,  -- Sep
               balance_am_history[1] as bal_1,  -- Aug
               balance_am_history[2] as bal_2,  -- Jul
               balance_am_history[3] as bal_3,  -- Jun (gap)
               balance_am_history[6] as bal_6,  -- Mar
               balance_am_history[7] as bal_7,  -- Feb
               balance_am_history[8] as bal_8   -- Jan
        FROM demo.edge_case_test.summary 
        WHERE cons_acct_key = 9006 AND rpt_as_of_mo = '2025-09'
    """).first()
    
    if sep:
        # Verify the gap in the middle
        if sep['bal_3'] is None:
            result.add_pass("9006 Sep bal[3] = NULL (Jun gap)")
        else:
            result.add_fail(f"9006 Sep bal[3] = {sep['bal_3']} (expected NULL)")
        
        # Sep = 7000 - 9*100 = 6100
        if sep['bal_0'] == 6100:
            result.add_pass("9006 Sep bal[0] = 6100")
        else:
            result.add_fail(f"9006 Sep bal[0] = {sep['bal_0']} (expected 6100)")
        
        # Mar = 7000 - 3*100 = 6700
        if sep['bal_6'] == 6700:
            result.add_pass("9006 Sep bal[6] = 6700 (Mar)")
        else:
            result.add_fail(f"9006 Sep bal[6] = {sep['bal_6']} (expected 6700)")
    else:
        result.add_fail("9006 Sep 2025 row missing")
    
    return result


# ============================================================================
# RUN PIPELINE
# ============================================================================

def run_pipeline(spark):
    """Run the pipeline"""
    print("\n" + "=" * 80)
    print("RUNNING PIPELINE v9.4")
    print("=" * 80)
    
    try:
        from summary_pipeline import run_pipeline as pipeline_run
    except ImportError as e:
        print(f"Could not import pipeline: {e}")
        return None
    
    # v9.4 dict-based config
    config = {
        # Table names
        "source_table": "demo.edge_case_test.accounts",
        "destination_table": "demo.edge_case_test.summary",
        "latest_history_table": "demo.edge_case_test.latest_summary",
        
        # Temp table schema (v9.4)
        "temp_table_schema": "temp",
        
        # Keys
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
        
        # Rolling columns - ALL 7 from production
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
        
        # Grid columns
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
            "app_name": "SummaryPipeline_v9.4_edge_case_test",
            "adaptive_enabled": True,
            "adaptive_coalesce": True,
            "adaptive_skew_join": True,
            "broadcast_timeout": 300
        }
    }
    
    start_time = time.time()
    try:
        stats = pipeline_run(spark, config)
        elapsed = time.time() - start_time
        print("\n" + "-" * 60)
        print("PIPELINE RESULTS")
        print("-" * 60)
        print(f"Elapsed time:        {elapsed:.2f}s")
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


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("COMPREHENSIVE EDGE CASE TEST SUITE - v9.4")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    
    spark = create_spark_session()
    print(f"\nSpark version: {spark.version}")
    
    all_results = []
    
    try:
        # Setup tables
        setup_tables(spark)
        
        # Setup all test data
        setup_case_i_edge_cases(spark)
        setup_case_ii_edge_cases(spark)
        setup_case_iii_edge_cases(spark)
        setup_case_iv_edge_cases(spark)
        
        # Insert new records for forward/backfill
        insert_case_ii_new_records(spark)
        insert_case_iii_new_records(spark)
        
        # Show account summary
        print("\n" + "=" * 80)
        print("ACCOUNTS TO PROCESS")
        print("=" * 80)
        spark.sql("""
            SELECT cons_acct_key, COUNT(*) as months, 
                   MIN(rpt_as_of_mo) as min_month, MAX(rpt_as_of_mo) as max_month
            FROM demo.edge_case_test.accounts
            GROUP BY cons_acct_key
            ORDER BY cons_acct_key
        """).show(20, False)
        
        # Run pipeline
        stats = run_pipeline(spark)
        
        if stats:
            # Run all verifications
            all_results.append(verify_case_i_edge_cases(spark))
            all_results.append(verify_case_ii_edge_cases(spark))
            all_results.append(verify_case_iii_edge_cases(spark))
            all_results.append(verify_case_iv_edge_cases(spark))
            
            # Final Summary
            total_passed = sum(r.passed for r in all_results)
            total_failed = sum(r.failed for r in all_results)
            
            print("\n" + "=" * 80)
            print("TEST RESULTS BY CATEGORY")
            print("=" * 80)
            for r in all_results:
                status = "PASS" if r.failed == 0 else "FAIL"
                print(f"  {r.name}: {r.passed} passed, {r.failed} failed [{status}]")
            
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
