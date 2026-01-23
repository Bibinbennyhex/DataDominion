#!/usr/bin/env python3
"""
Test Script for v5 Incremental Mode - Mixed Scenario
====================================================

This script tests the v5 pipeline's incremental mode by:
1. Creating a temporary source table with mixed scenario data
2. Running the pipeline
3. Validating results

Current State (before test):
- Summary: 100 accounts, 60 months (2020-01 to 2024-12)
- Latest timestamp: 2026-02-20

Test Data:
- 20 Forward entries (accounts 1-20, month 2025-01) 
- 10 Backfill entries (accounts 21-30, month 2024-11, with corrections)
- 10 New accounts (accounts 101-110, month 2025-01)

Author: DataDominion Team
"""

import json
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create Spark session  
spark = SparkSession.builder \
    .appName("Test Incremental Mixed Scenario") \
    .getOrCreate()

print("=" * 80)
print("SETTING UP MIXED SCENARIO TEST FOR V5 INCREMENTAL MODE")
print("=" * 80)

# Get current state
print("\n1. CURRENT STATE:")
print("-" * 80)

current_summary = spark.sql("""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT cons_acct_key) as accounts,
        COUNT(DISTINCT rpt_as_of_mo) as months,
        MIN(rpt_as_of_mo) as start_month,
        MAX(rpt_as_of_mo) as end_month,
        MAX(base_ts) as latest_ts
    FROM default.summary
""").first()

print(f"  Records:      {current_summary['total_records']:,}")
print(f"  Accounts:     {current_summary['accounts']}")
print(f"  Months:       {current_summary['months']}")
print(f"  Date Range:   {current_summary['start_month']} to {current_summary['end_month']}")
print(f"  Latest TS:    {current_summary['latest_ts']}")

# Create temporary source table for testing
print("\n2. CREATING TEMPORARY SOURCE TABLE:")
print("-" * 80)

# Get the summary schema to create matching source data
summary_schema = spark.sql("SELECT * FROM default.summary LIMIT 1").schema
print(f"  Summary has {len(summary_schema)} columns")

# Create a simple source table with just the columns we need
# We'll read existing data and modify it to create our test scenarios

new_ts = datetime.now()
print(f"  New timestamp: {new_ts}")

# Create source table if not exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS default.source_test (
        cons_acct_key BIGINT,
        rpt_as_of_mo STRING,
        acct_dt DATE,
        balance_am DECIMAL(15,2),
        credit_limit_am DECIMAL(15,2),
        past_due_am DECIMAL(15,2),
        actual_payment_am DECIMAL(15,2),
        days_past_due INT,
        asset_class_cd STRING,
        base_ts TIMESTAMP,
        bureau_member_id STRING,
        portfolio_rating_type_cd STRING,
        acct_type_dtl_cd STRING,
        open_dt DATE,
        closed_dt DATE,
        pymt_terms_cd STRING,
        pymt_terms_dtl_cd STRING,
        acct_stat_cd STRING,
        acct_pymt_stat_cd STRING,
        acct_pymt_stat_dtl_cd STRING,
        last_payment_dt DATE,
        schd_pymt_dt DATE,
        emi_amt DECIMAL(15,2),
        collateral_cd STRING,
        orig_pymt_due_dt DATE,
        dflt_status_dt DATE,
        write_off_am DECIMAL(15,2),
        hi_credit_am DECIMAL(15,2),
        cash_limit_am DECIMAL(15,2),
        collateral_am DECIMAL(15,2),
        charge_off_am DECIMAL(15,2),
        principal_write_off_am DECIMAL(15,2),
        settled_am DECIMAL(15,2),
        interest_rate DECIMAL(8,4),
        suit_filed_willful_dflt STRING,
        written_off_and_settled_status STRING
    )
    USING iceberg
    PARTITIONED BY (rpt_as_of_mo)
""")

# Truncate if exists
spark.sql("DELETE FROM default.source_test WHERE 1=1")
print("  Created/cleared source_test table")

# Generate Case II: Forward entries (accounts 1-20, month 2025-01)
print("\n  Generating Case II (Forward): 20 records for 2025-01")
forward_data = []
for acct_id in range(1, 21):
    forward_data.append((
        acct_id, "2025-01", datetime(2025,1,31).date(), 
        float(10000 + acct_id * 100), 50000.0, 0.0, 550.0,
        0, "1", new_ts,
        "BMB001", "R", "5", datetime(2020,1,1).date(), None,
        "M", "01", "A", "01", "01",
        datetime(2025,1,15).date(), datetime(2025,2,1).date(), 500.0, None, None, None,
        None, 50000.0, None, None, None, None, None, 0.12, None, None
    ))

forward_df = spark.createDataFrame(forward_data, [
    "cons_acct_key", "rpt_as_of_mo", "acct_dt", "balance_am", "credit_limit_am",
    "past_due_am", "actual_payment_am", "days_past_due", "asset_class_cd", "base_ts",
    "bureau_member_id", "portfolio_rating_type_cd", "acct_type_dtl_cd", "open_dt", "closed_dt",
    "pymt_terms_cd", "pymt_terms_dtl_cd", "acct_stat_cd", "acct_pymt_stat_cd", "acct_pymt_stat_dtl_cd",
    "last_payment_dt", "schd_pymt_dt", "emi_amt", "collateral_cd", "orig_pymt_due_dt", "dflt_status_dt",
    "write_off_am", "hi_credit_am", "cash_limit_am", "collateral_am", "charge_off_am",
    "principal_write_off_am", "settled_am", "interest_rate", "suit_filed_willful_dflt", "written_off_and_settled_status"
])
forward_df.writeTo("default.source_test").append()

# Generate Case III: Backfill entries (accounts 21-30, correction for 2024-11)
print("  Generating Case III (Backfill): 10 records for 2024-11 (corrections)")
backfill_data = []
for acct_id in range(21, 31):
    backfill_data.append((
        acct_id, "2024-11", datetime(2024,11,30).date(),
        99999.0, 50000.0, 888.0, 0.0,  # CORRECTED VALUES
        45, "2", new_ts,  # 30 DPD, Asset class 2
        "BMB001", "R", "5", datetime(2020,1,1).date(), None,
        "M", "01", "A", "02", "02",  # Delinquent
        datetime(2024,10,15).date(), datetime(2024,12,1).date(), 500.0, None, None, None,
        None, 50000.0, None, None, None, None, None, 0.12, None, None
    ))

backfill_df = spark.createDataFrame(backfill_data, [
    "cons_acct_key", "rpt_as_of_mo", "acct_dt", "balance_am", "credit_limit_am",
    "past_due_am", "actual_payment_am", "days_past_due", "asset_class_cd", "base_ts",
    "bureau_member_id", "portfolio_rating_type_cd", "acct_type_dtl_cd", "open_dt", "closed_dt",
    "pymt_terms_cd", "pymt_terms_dtl_cd", "acct_stat_cd", "acct_pymt_stat_cd", "acct_pymt_stat_dtl_cd",
    "last_payment_dt", "schd_pymt_dt", "emi_amt", "collateral_cd", "orig_pymt_due_dt", "dflt_status_dt",
    "write_off_am", "hi_credit_am", "cash_limit_am", "collateral_am", "charge_off_am",
    "principal_write_off_am", "settled_am", "interest_rate", "suit_filed_willful_dflt", "written_off_and_settled_status"
])
backfill_df.writeTo("default.source_test").append()

# Generate Case I: New accounts (accounts 101-110, month 2025-01)
print("  Generating Case I (New Accounts): 10 new accounts for 2025-01")
new_acct_data = []
for acct_id in range(101, 111):
    new_acct_data.append((
        acct_id, "2025-01", datetime(2025,1,31).date(),
        float(5000 + acct_id * 50), 25000.0, 0.0, 200.0,
        0, "1", new_ts,
        "BMB002", "I", "214", datetime(2025,1,1).date(), None,  # New account opened Jan 2025
        "M", "01", "A", "01", "01",
        None, datetime(2025,2,1).date(), 200.0, None, None, None,
        None, 25000.0, None, None, None, None, None, 0.10, None, None
    ))

new_acct_df = spark.createDataFrame(new_acct_data, [
    "cons_acct_key", "rpt_as_of_mo", "acct_dt", "balance_am", "credit_limit_am",
    "past_due_am", "actual_payment_am", "days_past_due", "asset_class_cd", "base_ts",
    "bureau_member_id", "portfolio_rating_type_cd", "acct_type_dtl_cd", "open_dt", "closed_dt",
    "pymt_terms_cd", "pymt_terms_dtl_cd", "acct_stat_cd", "acct_pymt_stat_cd", "acct_pymt_stat_dtl_cd",
    "last_payment_dt", "schd_pymt_dt", "emi_amt", "collateral_cd", "orig_pymt_due_dt", "dflt_status_dt",
    "write_off_am", "hi_credit_am", "cash_limit_am", "collateral_am", "charge_off_am",
    "principal_write_off_am", "settled_am", "interest_rate", "suit_filed_willful_dflt", "written_off_and_settled_status"
])
new_acct_df.writeTo("default.source_test").append()

# Verify source data
source_count = spark.sql("SELECT COUNT(*) as cnt FROM default.source_test").first()['cnt']
print(f"\n  Total records in source_test: {source_count}")

print("\n  Sample of test data:")
spark.sql("""
    SELECT cons_acct_key, rpt_as_of_mo, balance_am, days_past_due, base_ts
    FROM default.source_test
    ORDER BY rpt_as_of_mo, cons_acct_key
    LIMIT 10
""").show(10, False)

print("\n" + "=" * 80)
print("TEST DATA SETUP COMPLETE")
print("=" * 80)
print("""
Next Steps:
-----------
1. Update config to use source_test table, then run the pipeline:

   # First update summary_config.json to use "source_table": "default.source_test"
   # Then run:
   spark-submit /home/iceberg/notebooks/notebooks/summary_v5/summary_pipeline.py \\
       --config /home/iceberg/notebooks/notebooks/summary_v5/summary_config.json

2. The pipeline should:
   - Detect 40 new records (base_ts > previous max)
   - Classify: 10 Case I, 20 Case II, 10 Case III
   - Process in order: Backfill -> New Accounts -> Forward

3. Then run validation:
   spark-submit /home/iceberg/notebooks/notebooks/summary_v5/validate_incremental_test.py
""")

spark.stop()
