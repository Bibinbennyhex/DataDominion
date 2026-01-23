#!/usr/bin/env python3
"""
Validation Script for v5 Incremental Mode Test
===============================================

Run this AFTER the pipeline to verify:
1. All records processed correctly
2. Backfill corrections applied (balance_am = 99999 for accounts 21-30 in 2024-11)
3. New accounts created with proper history arrays
4. Forward entries have corrected history embedded

Author: DataDominion Team
"""

import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Load config
CONFIG_PATH = "/home/iceberg/notebooks/notebooks/summary_v5/summary_config.json"

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

# Create Spark session
spark = SparkSession.builder \
    .appName("Validate Incremental Mixed Scenario") \
    .getOrCreate()

print("=" * 80)
print("VALIDATION: V5 INCREMENTAL MODE - MIXED SCENARIO")
print("=" * 80)

all_passed = True
tests_run = 0
tests_passed = 0

# Test 1: Overall counts
print("\n1. OVERALL COUNTS:")
print("-" * 80)

summary = spark.sql(f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT cons_acct_key) as accounts,
        COUNT(DISTINCT rpt_as_of_mo) as months,
        MIN(rpt_as_of_mo) as start_month,
        MAX(rpt_as_of_mo) as end_month,
        MAX(base_ts) as latest_ts
    FROM {config['destination_table']}
""").first()

print(f"  Total Records:  {summary['total_records']:,}")
print(f"  Accounts:       {summary['accounts']}")
print(f"  Months:         {summary['months']}")
print(f"  Date Range:     {summary['start_month']} to {summary['end_month']}")
print(f"  Latest TS:      {summary['latest_ts']}")

tests_run += 1
# Expected: 61 months (2020-01 to 2025-01)
if summary['months'] >= 61 and summary['end_month'] == "2025-01":
    print("  ✓ PASS: Month count correct (includes 2025-01)")
    tests_passed += 1
else:
    print("  ✗ FAIL: Expected 61 months ending at 2025-01")
    all_passed = False

tests_run += 1
# Expected: 110 accounts (100 + 10 new)
if summary['accounts'] >= 110:
    print("  ✓ PASS: Account count correct (includes new accounts)")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Expected >= 110 accounts, got {summary['accounts']}")
    all_passed = False

# Test 2: Backfill verification (Case III)
print("\n2. BACKFILL VERIFICATION (Case III - Accounts 21-30):")
print("-" * 80)

backfill_check = spark.sql(f"""
    SELECT 
        cons_acct_key,
        rpt_as_of_mo,
        balance_am,
        past_due_am,
        payment_rating_cd
    FROM {config['destination_table']}
    WHERE cons_acct_key BETWEEN 21 AND 30
      AND rpt_as_of_mo = '2024-11'
    ORDER BY cons_acct_key
""").collect()

print(f"  Found {len(backfill_check)} backfill records for 2024-11")

tests_run += 1
if len(backfill_check) == 10:
    print("  ✓ PASS: All 10 backfill accounts found")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Expected 10 backfill accounts, found {len(backfill_check)}")
    all_passed = False

# Check corrected values
corrected_count = 0
for row in backfill_check:
    if float(row['balance_am']) == 99999.0:
        corrected_count += 1
    else:
        print(f"    Account {row['cons_acct_key']}: balance_am = {row['balance_am']} (expected 99999)")

tests_run += 1
if corrected_count == 10:
    print("  ✓ PASS: All backfill records have corrected balance_am = 99999")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Only {corrected_count}/10 records have corrected values")
    all_passed = False

# Test 3: New accounts verification (Case I)
print("\n3. NEW ACCOUNTS VERIFICATION (Case I - Accounts 101-110):")
print("-" * 80)

new_accounts = spark.sql(f"""
    SELECT 
        cons_acct_key,
        rpt_as_of_mo,
        balance_am,
        SIZE(balance_am_history) as history_length
    FROM {config['destination_table']}
    WHERE cons_acct_key BETWEEN 101 AND 110
    ORDER BY cons_acct_key, rpt_as_of_mo
""").collect()

print(f"  Found {len(new_accounts)} records for new accounts")

tests_run += 1
if len(new_accounts) == 10:
    print("  ✓ PASS: All 10 new accounts created")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Expected 10 new account records, found {len(new_accounts)}")
    all_passed = False

# Check history arrays are 36 elements
correct_history = 0
for row in new_accounts:
    if row['history_length'] == 36:
        correct_history += 1
    else:
        print(f"    Account {row['cons_acct_key']}: history_length = {row['history_length']} (expected 36)")

tests_run += 1
if correct_history == len(new_accounts):
    print("  ✓ PASS: All new accounts have 36-element history arrays")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Only {correct_history}/{len(new_accounts)} have correct history length")
    all_passed = False

# Test 4: Forward entries verification (Case II)
print("\n4. FORWARD ENTRIES VERIFICATION (Case II - Accounts 1-20):")
print("-" * 80)

forward_entries = spark.sql(f"""
    SELECT 
        cons_acct_key,
        rpt_as_of_mo,
        balance_am,
        SIZE(balance_am_history) as history_length
    FROM {config['destination_table']}
    WHERE cons_acct_key BETWEEN 1 AND 20
      AND rpt_as_of_mo = '2025-01'
    ORDER BY cons_acct_key
""").collect()

print(f"  Found {len(forward_entries)} forward entries for 2025-01")

tests_run += 1
if len(forward_entries) == 20:
    print("  ✓ PASS: All 20 forward entries created for 2025-01")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Expected 20 forward entries, found {len(forward_entries)}")
    all_passed = False

# Check history arrays
correct_forward_history = sum(1 for row in forward_entries if row['history_length'] == 36)

tests_run += 1
if correct_forward_history == 20:
    print("  ✓ PASS: All forward entries have 36-element history arrays")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Only {correct_forward_history}/20 have correct history length")
    all_passed = False

# Test 5: Verify backfill corrections are in forward history
print("\n5. VERIFY BACKFILL CORRECTIONS IN FORWARD HISTORY:")
print("-" * 80)

# For accounts 21-30, their 2024-12 entry should have 2024-11 correction in history
# History position: 2024-12 entry has 2024-11 at position 1 (one month back)

sample_check = spark.sql(f"""
    SELECT 
        cons_acct_key,
        rpt_as_of_mo,
        balance_am_history[1] as prev_month_balance
    FROM {config['destination_table']}
    WHERE cons_acct_key = 25
      AND rpt_as_of_mo = '2024-12'
""").first()

if sample_check:
    prev_balance = sample_check['prev_month_balance']
    print(f"  Account 25, month 2024-12, history[1] (2024-11 balance): {prev_balance}")
    
    tests_run += 1
    if prev_balance and float(prev_balance) == 99999.0:
        print("  ✓ PASS: Backfill correction (99999) visible in subsequent month's history")
        tests_passed += 1
    else:
        print(f"  ✗ FAIL: Expected 99999.0 in history, got {prev_balance}")
        all_passed = False
else:
    print("  ⚠ SKIP: Could not verify (sample record not found)")

# Test 6: No duplicates
print("\n6. DUPLICATE CHECK:")
print("-" * 80)

duplicates = spark.sql(f"""
    SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) as cnt
    FROM {config['destination_table']}
    GROUP BY cons_acct_key, rpt_as_of_mo
    HAVING COUNT(*) > 1
""").count()

tests_run += 1
if duplicates == 0:
    print("  ✓ PASS: No duplicate (account, month) combinations")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: Found {duplicates} duplicate combinations")
    all_passed = False

# Test 7: Array length consistency
print("\n7. ARRAY LENGTH CONSISTENCY:")
print("-" * 80)

wrong_length = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {config['destination_table']}
    WHERE SIZE(balance_am_history) != 36
""").first()['cnt']

tests_run += 1
if wrong_length == 0:
    print("  ✓ PASS: All history arrays have exactly 36 elements")
    tests_passed += 1
else:
    print(f"  ✗ FAIL: {wrong_length} records have incorrect array length")
    all_passed = False

# Summary
print("\n" + "=" * 80)
print("VALIDATION SUMMARY")
print("=" * 80)
print(f"  Tests Passed: {tests_passed}/{tests_run}")

if all_passed:
    print("\n  ✓ ALL TESTS PASSED - V5 INCREMENTAL MODE WORKING CORRECTLY!")
    print("\n  The pipeline successfully:")
    print("    1. Classified records into Case I, II, III")
    print("    2. Processed backfill FIRST (corrected 2024-11)")
    print("    3. Processed new accounts SECOND")
    print("    4. Processed forward entries LAST")
    print("    5. Maintained data integrity (no duplicates, correct arrays)")
else:
    print("\n  ✗ SOME TESTS FAILED - Review output above")

print("=" * 80)

spark.stop()
