#!/usr/bin/env python3
"""
Test Script for v5 Incremental Mode - Mixed Scenario
====================================================

This script tests the v5 pipeline's incremental mode by creating
a realistic production scenario with:
- Forward entries (2025-01, new month)
- Backfill entries (corrections to 2024-11)
- New accounts (first-time accounts)

Current State (before test):
- Summary: 100 accounts, 60 months (2020-01 to 2024-12)
- Latest timestamp: 2026-02-20

Test Data:
- 20 Forward entries (accounts 1-20, month 2025-01)
- 10 Backfill entries (accounts 21-30, month 2024-11, with corrections)
- 10 New accounts (accounts 101-110, month 2025-01)

Expected After Test:
- Total months: 61 (2020-01 to 2025-01)
- Total accounts: 110 (100 existing + 10 new)
- Backfill accounts should have corrected 2024-11 values in history

Author: DataDominion Team
"""

import json
import sys
from datetime import datetime
from decimal import Decimal
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType
)

# Load config
CONFIG_PATH = "/home/iceberg/notebooks/notebooks/summary_v5/summary_config.json"

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

# Create Spark session
spark = SparkSession.builder \
    .appName("Test Incremental Mixed Scenario") \
    .getOrCreate()

print("=" * 80)
print("MIXED SCENARIO TEST FOR V5 INCREMENTAL MODE")
print("=" * 80)

# Get current state
print("\n1. CURRENT STATE:")
print("-" * 80)

current_summary = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT cons_acct_key) as accounts,
        COUNT(DISTINCT rpt_as_of_mo) as months,
        MIN(rpt_as_of_mo) as start_month,
        MAX(rpt_as_of_mo) as end_month,
        MAX(base_ts) as latest_ts
    FROM {config['destination_table']}
""").first()

print(f"  Records:      {current_summary['total_records']:,}")
print(f"  Accounts:     {current_summary['accounts']}")
print(f"  Months:       {current_summary['months']}")
print(f"  Date Range:   {current_summary['start_month']} to {current_summary['end_month']}")
print(f"  Latest TS:    {current_summary['latest_ts']}")

# Generate test data
print("\n2. GENERATING MIXED TEST DATA:")
print("-" * 80)

# New timestamp for all test data (must be > current max)
new_timestamp = datetime.now()
print(f"  New timestamp: {new_timestamp}")

# Schema matching source table
schema = StructType([
    StructField("cons_acct_key", IntegerType(), False),
    StructField("rpt_as_of_mo", StringType(), False),
    StructField("rpt_as_of_d", StringType(), False),
    StructField("balance_am", DecimalType(15, 2), True),
    StructField("credit_limit_am", DecimalType(15, 2), True),
    StructField("past_due_am", DecimalType(15, 2), True),
    StructField("current_due_am", DecimalType(15, 2), True),
    StructField("scheduled_payment_am", DecimalType(15, 2), True),
    StructField("actual_payment_am", DecimalType(15, 2), True),
    StructField("payment_rating_cd", StringType(), True),
    StructField("special_comment_cd", StringType(), True),
    StructField("account_status_cd", StringType(), True),
    StructField("base_ts", TimestampType(), False),
])

test_records = []

# Case II: Forward entries (20 existing accounts, new month 2025-01)
print("  Generating Case II (Forward): 20 records for 2025-01")
for acct_id in range(1, 21):
    record = (
        acct_id,                                    # cons_acct_key
        "2025-01",                                  # rpt_as_of_mo (NEW MONTH)
        "2025-01-31",                               # rpt_as_of_d
        Decimal(f"{10000 + acct_id * 100}.00"),     # balance_am
        Decimal("50000.00"),                        # credit_limit_am
        Decimal("0.00"),                            # past_due_am
        Decimal(f"{500 + acct_id * 10}.00"),        # current_due_am
        Decimal("500.00"),                          # scheduled_payment_am
        Decimal("550.00"),                          # actual_payment_am
        "01",                                       # payment_rating_cd (on time)
        "",                                         # special_comment_cd
        "11",                                       # account_status_cd (open)
        new_timestamp                               # base_ts
    )
    test_records.append(record)

# Case III: Backfill entries (10 accounts, correction for 2024-11)
print("  Generating Case III (Backfill): 10 records for 2024-11 (corrections)")
for acct_id in range(21, 31):
    record = (
        acct_id,                                    # cons_acct_key
        "2024-11",                                  # rpt_as_of_mo (EXISTING MONTH - backfill!)
        "2024-11-30",                               # rpt_as_of_d
        Decimal(f"{99999}.00"),                     # balance_am (CORRECTED VALUE)
        Decimal("50000.00"),                        # credit_limit_am
        Decimal(f"{888}.00"),                       # past_due_am (CORRECTED VALUE)
        Decimal(f"{500}.00"),                       # current_due_am
        Decimal("500.00"),                          # scheduled_payment_am
        Decimal("0.00"),                            # actual_payment_am (MISSED PAYMENT)
        "02",                                       # payment_rating_cd (30 days late)
        "B",                                        # special_comment_cd
        "11",                                       # account_status_cd
        new_timestamp                               # base_ts (newer than existing)
    )
    test_records.append(record)

# Case I: New accounts (10 brand new accounts, month 2025-01)
print("  Generating Case I (New Accounts): 10 new accounts for 2025-01")
for acct_id in range(101, 111):
    record = (
        acct_id,                                    # cons_acct_key (NEW ACCOUNT)
        "2025-01",                                  # rpt_as_of_mo
        "2025-01-31",                               # rpt_as_of_d
        Decimal(f"{5000 + acct_id * 50}.00"),       # balance_am
        Decimal("25000.00"),                        # credit_limit_am
        Decimal("0.00"),                            # past_due_am
        Decimal("200.00"),                          # current_due_am
        Decimal("200.00"),                          # scheduled_payment_am
        Decimal("200.00"),                          # actual_payment_am
        "01",                                       # payment_rating_cd
        "",                                         # special_comment_cd
        "11",                                       # account_status_cd
        new_timestamp                               # base_ts
    )
    test_records.append(record)

# Create DataFrame and insert
test_df = spark.createDataFrame(test_records, schema)

print(f"\n  Total test records: {len(test_records)}")
print(f"    - Forward (Case II):     20 (accounts 1-20, month 2025-01)")
print(f"    - Backfill (Case III):   10 (accounts 21-30, month 2024-11)")
print(f"    - New (Case I):          10 (accounts 101-110, month 2025-01)")

# Show sample
print("\n  Sample of test data:")
test_df.select("cons_acct_key", "rpt_as_of_mo", "balance_am", "payment_rating_cd", "base_ts").show(5, False)

# Insert into source table
print("\n3. INSERTING TEST DATA INTO SOURCE TABLE:")
print("-" * 80)

test_df.writeTo(config['source_table']).append()
print(f"  Inserted {len(test_records)} records into {config['source_table']}")

# Verify source table
source_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {config['source_table']}").first()['cnt']
print(f"  Source table now has: {source_count:,} records")

# Show what new data looks like
print("\n  New data (base_ts > previous max):")
spark.sql(f"""
    SELECT cons_acct_key, rpt_as_of_mo, balance_am, base_ts
    FROM {config['source_table']}
    WHERE base_ts > TIMESTAMP '{current_summary['latest_ts']}'
    ORDER BY rpt_as_of_mo, cons_acct_key
    LIMIT 10
""").show(10, False)

print("\n" + "=" * 80)
print("TEST DATA SETUP COMPLETE")
print("=" * 80)
print("""
Next Steps:
-----------
1. Run the v5 pipeline in incremental mode:

   spark-submit /home/iceberg/notebooks/notebooks/summary_v5/summary_pipeline.py \\
       --config /home/iceberg/notebooks/notebooks/summary_v5/summary_config.json

2. The pipeline should:
   - Detect 40 new records (base_ts > previous max)
   - Classify: 10 Case I, 20 Case II, 10 Case III
   - Process in order: Backfill -> New Accounts -> Forward

3. Expected Results:
   - Summary months: 61 (was 60, added 2025-01)
   - Summary accounts: 110 (was 100, added 10 new)
   - Backfill accounts (21-30): balance_am = 99999 for 2024-11
""")

spark.stop()
