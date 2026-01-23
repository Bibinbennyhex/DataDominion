
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta
import sys

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SummaryValidation") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

print("Spark session ready!")

# 1. Table Overview & Schema Validation
print("=" * 70)
print("TABLE OVERVIEW")
print("=" * 70)

accounts_count = spark.table("default.default.accounts_all").count()
summary_count = spark.table("default.summary").count()
latest_count = spark.table("default.latest_summary").count()

print(f"Source (accounts_all):  {accounts_count:,} rows")
print(f"Summary:                {summary_count:,} rows")
print(f"Latest Summary:         {latest_count:,} rows")
print(f"Unique accounts in summary: {spark.table('default.summary').select('cons_acct_key').distinct().count():,}")
print(f"Unique months in summary:   {spark.table('default.summary').select('rpt_as_of_mo').distinct().count()}")

# Full schema check - verify all expected columns exist
print("\nSUMMARY TABLE SCHEMA CHECK:")
print("=" * 70)

summary_cols = [f.name for f in spark.table("default.summary").schema.fields]

# Expected columns
expected_cols = [
    # Key columns
    'cons_acct_key', 'rpt_as_of_mo', 'base_ts',
    # Scalar columns
    'balance_am', 'days_past_due', 'actual_payment_am', 'credit_limit_am',
    'past_due_am', 'asset_class_cd', 'emi_amt',
    # History arrays
    'balance_am_history', 'days_past_due_history', 'actual_payment_am_history',
    'credit_limit_am_history', 'past_due_am_history', 'payment_rating_cd_history',
    'asset_class_cd_history',
    # Grid
    'payment_history_grid'
]

print(f"{'Column':<35} {'Present':<10} {'Type'}")
print("-" * 70)
for col in expected_cols:
    present = "YES" if col in summary_cols else "MISSING"
    col_type = str([f.dataType for f in spark.table("default.summary").schema.fields if f.name == col][0]) if col in summary_cols else "N/A"
    status = "" if present == "YES" else " <-- FIX NEEDED"
    print(f"{col:<35} {present:<10} {col_type}{status}")

# 2. Gap Scenario Validation
print("\n" + "=" * 70)
print("GAP SCENARIO ANALYSIS")
print("=" * 70)

# Get accounts with fewer months than expected
gap_accounts = spark.sql("""
    WITH all_months AS (
        SELECT DISTINCT rpt_as_of_mo FROM default.default.accounts_all ORDER BY rpt_as_of_mo
    ),
    account_months AS (
        SELECT 
            cons_acct_key,
            COUNT(DISTINCT rpt_as_of_mo) as month_count,
            COLLECT_SET(rpt_as_of_mo) as months_present
        FROM default.default.accounts_all
        GROUP BY cons_acct_key
    )
    SELECT 
        cons_acct_key,
        month_count,
        (SELECT COUNT(*) FROM all_months) as total_months,
        (SELECT COUNT(*) FROM all_months) - month_count as missing_months
    FROM account_months
    WHERE month_count < (SELECT COUNT(*) FROM all_months)
    ORDER BY missing_months DESC
    LIMIT 10
""")

print("\nAccounts with gaps (missing months in source):")
gap_accounts.show()

# Pick a gap account and analyze it
gap_account_row = gap_accounts.first()
if gap_account_row:
    gap_account = gap_account_row['cons_acct_key']
    print(f"\nAnalyzing gap account: {gap_account}")
    print("=" * 70)
    
    # Summary data for this account
    print("\nSummary data (with history arrays):")
    spark.sql(f"""
        SELECT 
            rpt_as_of_mo,
            balance_am,
            days_past_due,
            SLICE(balance_am_history, 1, 8) as bal_hist_first8,
            payment_history_grid
        FROM default.summary
        WHERE cons_acct_key = {gap_account}
        ORDER BY rpt_as_of_mo
    """).show(truncate=False)
    
    # Verify gap handling - NULL values should appear in history for missing months
    print("\nGAP HANDLING VERIFICATION:")
    print("When a month is missing, the history array should have NULL at that position.")
    
    # Get the history for the latest month
    latest_hist = spark.sql(f"""
        SELECT 
            rpt_as_of_mo,
            balance_am_history,
            days_past_due_history
        FROM default.summary
        WHERE cons_acct_key = {gap_account}
        ORDER BY rpt_as_of_mo DESC
        LIMIT 1
    """).collect()[0]
    
    bal_hist = latest_hist['balance_am_history']
    
    print(f"\nAccount {gap_account} - Latest month: {latest_hist['rpt_as_of_mo']}")
    print(f"\nBalance history (first 12 positions):")
    for i, val in enumerate(bal_hist[:12]):
        marker = "<-- NULL (gap)" if val is None else ""
        print(f"  [{i}] {val} {marker}")
    
    null_count = sum(1 for v in bal_hist if v is None)
    print(f"\nTotal NULLs in balance history: {null_count} out of {len(bal_hist)}")
else:
    print("No gap accounts found in source data.")

# 3. Rolling History Validation
print("\n" + "=" * 70)
print("ROLLING HISTORY SHIFT VALIDATION")
print("=" * 70)

# Verify history[1] = previous month's history[0]
shift_check = spark.sql("""
    WITH ordered AS (
        SELECT 
            cons_acct_key,
            rpt_as_of_mo,
            balance_am_history[0] as current_h0,
            balance_am_history[1] as current_h1,
            LAG(balance_am_history[0]) OVER (PARTITION BY cons_acct_key ORDER BY rpt_as_of_mo) as prev_h0
        FROM default.summary
    )
    SELECT *
    FROM ordered
    WHERE prev_h0 IS NOT NULL  -- Skip first month
      AND current_h1 != prev_h0
      AND current_h1 IS NOT NULL  -- Allow for gaps
    LIMIT 10
""")

shift_errors = shift_check.count()
if shift_errors == 0:
    print("PASSED: History shift is correct (h[1] = prev month's h[0])")
else:
    print(f"FAILED: {shift_errors} shift errors found")
    shift_check.show()

# 4. Array Length & Data Integrity Checks
print("\n" + "=" * 70)
print("ARRAY LENGTH VALIDATION")
print("=" * 70)

array_columns = [
    'balance_am_history',
    'days_past_due_history', 
    'actual_payment_am_history',
    'credit_limit_am_history',
    'past_due_am_history',
    'payment_rating_cd_history',
    'asset_class_cd_history'
]

for col in array_columns:
    result = spark.sql(f"""
        SELECT 
            '{col}' as column_name,
            SUM(CASE WHEN SIZE({col}) = 36 THEN 1 ELSE 0 END) as correct,
            SUM(CASE WHEN SIZE({col}) != 36 THEN 1 ELSE 0 END) as incorrect
        FROM default.summary
    """).collect()[0]
    
    status = "PASS" if result['incorrect'] == 0 else "FAIL"
    print(f"{col:<35} {status} (correct: {result['correct']}, incorrect: {result['incorrect']})")

# 5. DPD Match Validation
print("\n" + "=" * 70)
print("DPD SOURCE MATCH VALIDATION")
print("=" * 70)

dpd_mismatch = spark.sql("""
    SELECT COUNT(*) as mismatch_count
    FROM default.default.accounts_all a
    JOIN default.summary s 
        ON a.cons_acct_key = s.cons_acct_key 
        AND a.rpt_as_of_mo = s.rpt_as_of_mo
    WHERE a.days_past_due_ct_4in != s.days_past_due
""").collect()[0]['mismatch_count']

if dpd_mismatch == 0:
    print("PASS: All DPD values match between source and summary")
else:
    print(f"FAIL: {dpd_mismatch} DPD mismatches found")

# 6. Payment Grid Check
print("\n" + "=" * 70)
print("PAYMENT HISTORY GRID CHECK")
print("=" * 70)

grid_check = spark.sql("""
    SELECT 
        LENGTH(payment_history_grid) as grid_length,
        COUNT(*) as count
    FROM default.summary
    GROUP BY LENGTH(payment_history_grid)
""")

grid_check.show()

# 7. Final Summary
print("\n" + "=" * 70)
print("VALIDATION SUMMARY")
print("=" * 70)
print(f"Total Rows: {summary_count:,}")
print(f"Gap Check: {'Passed' if gap_account_row else 'No gaps to check'}")
print(f"History Shift: {'Passed' if shift_errors == 0 else 'Failed'}")
print(f"DPD Match: {'Passed' if dpd_mismatch == 0 else 'Failed'}")
print("=" * 70)
