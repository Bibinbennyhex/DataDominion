
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("VerifyBackfill") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

test_account = 100
expected_balance = 888888
backfill_month = "2024-02"

print(f"Verifying backfill for Account {test_account}")
print("=" * 60)

# 1. Check the specific month updated
print(f"\nChecking {backfill_month} (should have balance {expected_balance}):")
row = spark.sql(f"""
    SELECT rpt_as_of_mo, balance_am
    FROM default.summary
    WHERE cons_acct_key = {test_account} AND rpt_as_of_mo = '{backfill_month}'
""").collect()[0]

actual_balance = row['balance_am']
print(f"Actual Balance: {actual_balance}")

if actual_balance == expected_balance:
    print("PASS: Balance updated correctly")
else:
    print(f"FAIL: Expected {expected_balance}, got {actual_balance}")

# 2. Check future months' history arrays
print(f"\nChecking history propagation to future months:")
print("-" * 60)
print(f"{'Month':<10} {'Current':<10} {'History[0]':<10} {'History[1]':<10} {'History[2]':<10} {'History[3]':<10}")

history_rows = spark.sql(f"""
    SELECT 
        rpt_as_of_mo,
        balance_am,
        balance_am_history[0] as h0,
        balance_am_history[1] as h1,
        balance_am_history[2] as h2,
        balance_am_history[3] as h3
    FROM default.summary
    WHERE cons_acct_key = {test_account} AND rpt_as_of_mo >= '{backfill_month}'
    ORDER BY rpt_as_of_mo
""").collect()

for r in history_rows:
    print(f"{r['rpt_as_of_mo']:<10} {r['balance_am']:<10} {r['h0']:<10} {r['h1']:<10} {r['h2']:<10} {r['h3']:<10}")

# Verify propagation logic
# For 2024-03, h[1] should be 888888
# For 2024-04, h[2] should be 888888
# etc.

failures = 0
for i, r in enumerate(history_rows):
    month = r['rpt_as_of_mo']
    
    # Calculate expected offset of the backfilled value
    # i=0 is 2024-02, so offset is 0
    # i=1 is 2024-03, so offset is 1
    # etc.
    offset = i
    
    # Get value at that offset (dynamically accessing h0, h1, etc is hard in loop, so manual check)
    val_at_offset = None
    if offset == 0: val_at_offset = r['h0']
    elif offset == 1: val_at_offset = r['h1']
    elif offset == 2: val_at_offset = r['h2']
    elif offset == 3: val_at_offset = r['h3']
    
    if val_at_offset == expected_balance:
        print(f"PASS: {month} has correct backfilled value at index {offset}")
    else:
        print(f"FAIL: {month} expected {expected_balance} at index {offset}, got {val_at_offset}")
        failures += 1

if failures == 0:
    print("\nSUCCESS: Backfill propagated correctly to all future history arrays!")
else:
    print(f"\nFAILURE: {failures} propagation errors found")
