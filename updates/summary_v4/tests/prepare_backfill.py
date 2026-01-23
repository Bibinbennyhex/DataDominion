
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp, to_timestamp
from datetime import datetime

spark = SparkSession.builder \
    .appName("BackfillTest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

# 1. Select a test account
test_account = 100
print(f"Testing backfill on Account: {test_account}")

# 2. Get existing data state
print("\n--- BEFORE BACKFILL ---")
spark.sql(f"""
    SELECT rpt_as_of_mo, balance_am, balance_am_history[0] as current_hist, balance_am_history[1] as prev_hist
    FROM default.summary
    WHERE cons_acct_key = {test_account}
    ORDER BY rpt_as_of_mo
""").show()

# 3. Create a backfill record
# We'll take the record for 2024-02 and update it with a new balance
# IMPORTANT: base_ts must be newer than existing to trigger update
print("\nInserting late-arriving record for 2024-02...")

# Get template from existing source
df = spark.table("default.default.accounts_all") \
    .filter(f"cons_acct_key = {test_account} AND rpt_as_of_mo = '2024-02'")

if df.count() == 0:
    print("Error: Month 2024-02 not found for account 100")
    exit(1)

# Update balance and timestamp
NEW_BALANCE = 888888
backfill_record = df \
    .withColumn("acct_bal_am", lit(NEW_BALANCE)) \
    .withColumn("base_ts", current_timestamp() + F.expr("INTERVAL 1 HOUR"))

# Write to source table
backfill_record.writeTo("default.default.accounts_all").append()

print(f"Inserted record for 2024-02 with balance {NEW_BALANCE}")
