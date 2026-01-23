
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DateType, TimestampType
)
from datetime import datetime, date
import sys

# Import session creator from main script
from iceberg_account_summary import create_spark_session

# Re-use the schema from accounts_all
ACCOUNTS_ALL_SCHEMA = StructType([
    StructField("cons_acct_key", LongType(), False),
    StructField("acct_dt", DateType(), False),
    StructField("rpt_as_of_mo", StringType(), False),
    StructField("current_balance", IntegerType(), True),
    StructField("current_dpd", IntegerType(), True),
    StructField("payment_am", IntegerType(), True),
    StructField("status_cd", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("created_ts", TimestampType(), True),
    StructField("updated_ts", TimestampType(), True),
    StructField("base_ts", TimestampType(), True)
])

def generate_scenarios(spark: SparkSession):
    data = []

    # Helper to construct row
    def add_row(k, acct_dt_str, base_ts_str, bal=1000, dpd=0, stat="CURRENT"):
        dt_obj = datetime.strptime(acct_dt_str, "%Y-%m-%d").date()
        rpt_mo = acct_dt_str[:7]
        
        base_ts = datetime.strptime(base_ts_str, "%Y-%m-%d %H:%M:%S")
        # created/updated same as base for simplicity in these scenarios, or slightly before
        created_ts = base_ts
        updated_ts = base_ts

        data.append((
            k, dt_obj, rpt_mo, bal, dpd, 50, "0", stat,
            created_ts, updated_ts, base_ts
        ))

    print("Generating Scenario Data...")

    # ---------------------------------------------------------
    # Scenario 1: Future Account Date (Account 10)
    # ---------------------------------------------------------
    # Ingested: July 1st, 2025
    # Report Date: Sept 15th, 2025 (Future)
    # Use Case: Testing handling of future dated records.
    print("  -> Generating Scenario 1 (Future Date)")
    add_row(10, "2025-09-15", "2025-07-01 10:00:00")

    # ---------------------------------------------------------
    # Scenario 2: Backfill / Late Arrival (Account 5)
    # ---------------------------------------------------------
    # Ingested: July 2nd, 2025
    # Report Date: Sept 25th, 2016 (Filling the Index 20 gap)
    # Use Case: Testing update logic for old history.
    print("  -> Generating Scenario 2 (Backfill)")
    add_row(5, "2016-09-25", "2025-07-02 10:00:00", bal=9999, dpd=0)

    # ---------------------------------------------------------
    # Scenario 3: Multi-Month Batch (Account 11)
    # ---------------------------------------------------------
    # Ingested: July 3rd, 2025 (Single Batch)
    # Report Dates: April, May, June 2025
    # Use Case: Testing processing of multiple months arriving at once.
    print("  -> Generating Scenario 3 (Multi-Month Batch)")
    common_base = "2025-07-03 14:30:00"
    add_row(11, "2025-04-20", common_base, bal=500, dpd=0)
    add_row(11, "2025-05-20", common_base, bal=450, dpd=0)
    add_row(11, "2025-06-20", common_base, bal=400, dpd=0)

    # ---------------------------------------------------------
    # Scenario 4: Bulk Loading (Accounts 1, 5, 7 ONLY)
    # ---------------------------------------------------------
    # Goal: >500 rows total using only 3 accounts.
    # Strategy: High frequency reporting (multiple reports per month)
    print("  -> Generating Scenario 4 (Bulk Load ~500+ rows for Accts 1, 5, 7)")
    import random
    random.seed(1000) # Updated seed for second batch/higher density

    batch_base_ts_str = "2025-07-05 09:00:00"
    
    target_accounts = [1, 5, 7]
    
    for acct_id in target_accounts:
        # Generate dense history: Jan 2023 - Dec 2025 (36 Months)
        # To get ~170 rows per account, we need ~5 rows per month on average
        for i in range(36):
            year = 2023 + i // 12
            month = 1 + i % 12
            
            # Generate 4 to 7 reports for THIS month (Increased density)
            # Simulating weekly updates or corrections
            num_reports = random.randint(4, 7)
            
            for _ in range(num_reports):
                # Random day in the month
                day = random.randint(1, 28)
                date_str = f"{year}-{month:02d}-{day:02d}"
                
                # Fluctuate balance slightly to show activity
                bal = random.randint(1000, 5000)
                
                # Occasional DPD spike for Account 7 (to keep it interesting)
                dpd = 0
                if acct_id == 7 and random.random() < 0.3:
                    dpd = random.randint(30, 90)

                add_row(acct_id, date_str, batch_base_ts_str, bal=bal, dpd=dpd)

    # Convert to DF
    df = spark.createDataFrame(data, schema=ACCOUNTS_ALL_SCHEMA)
    return df

def main():
    spark = create_spark_session()
    
    # Generate
    df_new = generate_scenarios(spark)
    
    # Append to Iceberg Table
    print(f"Appending {df_new.count()} scenario rows to default.accounts_all...")
    df_new.writeTo("default.default.accounts_all").append()
    
    print("✓ Scenario data appended successfully.")

    # Show added data
    print("\n--- Added Records (base_ts > 2025-06-25) ---")
    spark.sql("""
        SELECT cons_acct_key, acct_dt, rpt_as_of_mo, base_ts 
        FROM default.default.accounts_all 
        WHERE base_ts > timestamp('2025-06-25 00:00:00')
        ORDER BY base_ts, cons_acct_key, acct_dt
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
