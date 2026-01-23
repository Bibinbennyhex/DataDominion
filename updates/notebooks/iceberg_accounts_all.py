
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DateType, TimestampType
)
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from datetime import datetime, timedelta
import sys
import random
# Import shared Spark session creator
from iceberg_account_summary import create_spark_session

# -------------------------------------------------------------------------
# 1. Define Schema for accounts_all
# -------------------------------------------------------------------------
ACCOUNTS_ALL_SCHEMA = StructType([
    StructField("cons_acct_key", LongType(), False),
    StructField("acct_dt", DateType(), False),        # The specific date of the report
    StructField("rpt_as_of_mo", StringType(), False), # yyyy-MM
    StructField("current_balance", IntegerType(), True),
    StructField("current_dpd", IntegerType(), True),
    StructField("payment_am", IntegerType(), True),
    StructField("status_cd", StringType(), True),     # e.g., '0', '30', 'L'
    StructField("account_status", StringType(), True),
    StructField("created_ts", TimestampType(), True),
    StructField("updated_ts", TimestampType(), True),
    StructField("base_ts", TimestampType(), True)
])



def create_accounts_all_table(spark: SparkSession):
    """
    Creates the 'default.accounts_all' table if it doesn't exist.
    """
    full_table_name = "default.default.accounts_all"
    
    # Drop table if exists to ensure requested schema
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    
    create_sql = f"""
    CREATE TABLE {full_table_name} (
        cons_acct_key       BIGINT      NOT NULL,
        acct_dt             DATE        NOT NULL,
        rpt_as_of_mo        STRING      NOT NULL,
        current_balance     INT,
        current_dpd         INT,
        payment_am          INT,
        status_cd           STRING,
        account_status      STRING,
        created_ts          TIMESTAMP,
        updated_ts          TIMESTAMP,
        base_ts             TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (rpt_as_of_mo)
    TBLPROPERTIES (
        'format-version' = '2',
        'write.metadata.compression-codec' = 'gzip'
    )
    """
    spark.sql(create_sql)
    print(f"✓ Created Iceberg table: {full_table_name}")

def generate_sample_data(spark: SparkSession):
    """
    Generates sample data for accounts_all.
    Broadly reflects the accounts generated in the summary table:
      - Account 1: Sporadic dates
      - Account 5: 120 months history
      - Account 6, 7, 8: Varied patterns
    Also includes MULTIPLE reports for the same month to test deduplication.
    """
    random.seed(42)
    data = []
    
    # Helper to create row tuple with RANDOMIZED TIMESTAMPS
    # (key, acct_dt_str, rpt_mo, bal, dpd, pay, status_cd, acct_status)
    def add_row(k, dt_str, bal, dpd, pay, stat_cd, acct_stat):
        # rpt_as_of_mo is first 7 chars of dt_str (yyyy-MM)
        rpt_mo = dt_str[:7]
        # acct_dt convert to date object
        dt_obj = datetime.strptime(dt_str, "%Y-%m-%d").date()
        
        # Generate authentic timestamps relative to the account date
        # created_ts: Same day as report, random time 08:00 - 23:59
        base_time = datetime.combine(dt_obj, datetime.min.time())
        random_hour = random.randint(8, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        created_ts = base_time + timedelta(hours=random_hour, minutes=random_minute, seconds=random_second)
        
        # updated_ts: created_ts + random lag (0 to 48 hours) represents processing delay
        lag_hours = random.randint(0, 48)
        updated_ts = created_ts + timedelta(hours=lag_hours)
        
        # base_ts: same as created_ts for simplicity
        base_ts = created_ts

        data.append((
            k, dt_obj, rpt_mo, bal, dpd, pay, stat_cd, acct_stat,
            created_ts, updated_ts, base_ts
        ))

    # --- Account 1: Recent Activity ---
    # Single entry for 2025-05
    add_row(1, "2025-05-20", 4, 0, 100, "0", "CURRENT")
    # Multiple entries for 2025-06!
    # Valid scenario: An update happened mid-month or a correction was sent.
    # Entry 1: Early in month
    add_row(1, "2025-06-05", 2, 0, 50,  "0", "CURRENT")
    # Entry 2: Later in month (This should be the winner in 'latest' logic)
    add_row(1, "2025-06-25", 1, 0, 200, "0", "CURRENT") # Lower balance after payment

    # --- Account 5: Long History (Sample a few years to match Summary) ---
    # Generating 120 months (2015-01 to 2024-12)
    start_year = 2015
    for i in range(120):
        year = start_year + (i // 12)
        month = (i % 12) + 1
        
        # Randomize day between 20th and 28th to simulate variation
        day = random.randint(20, 28)
        date_str = f"{year}-{month:02d}-{day:02d}"
        
        bal = 1000 + i
        dpd = 0
        status = "CURRENT"

        # Simulate Non-Reporting: Skip these months entirely
        if i in [20, 50, 51, 52]:
            print(f"  -> Account 5: Simulating non-reporting (Skipping) for Month Index {i} ({date_str})")
            continue

        add_row(5, date_str, bal, dpd, 50, "0", status)

    # --- Account 7: Delinquent (2023-2024) ---
    # Increasing DPD
    for i in range(24):
        year = 2023 + (i // 12)
        month = (i % 12) + 1
        
        # Randomize day
        day = random.randint(18, 25)
        date_str = f"{year}-{month:02d}-{day:02d}"
        
        dpd = i * 5
        bal = 2000
        # Payment 0
        stat_cd = str(dpd) if dpd > 0 else "0"
        if dpd > 30:
            acct_st = "DELINQUENT"
        else:
            acct_st = "CURRENT"
            
        add_row(7, date_str, bal, dpd, 0, stat_cd, acct_st)

    # --- Convert to DataFrame ---
    df = spark.createDataFrame(data, schema=ACCOUNTS_ALL_SCHEMA)
    
    # Note: Timestamps are now already populated in the data tuples
    
    print(f"Generated {df.count()} rows of sample data.")
    return df

def main():
    spark = create_spark_session()
    
    # 1. Create Table
    create_accounts_all_table(spark)
    
    # 2. Generate Data
    df_data = generate_sample_data(spark)
    
    # 3. Write to Table
    print("Writing data to default.accounts_all...")
    df_data.writeTo("default.default.accounts_all").append()
    
    # 4. Verify
    print("Verification Query:")
    spark.sql("SELECT * FROM default.default.accounts_all WHERE cons_acct_key = 1 ORDER BY acct_dt").show()
    
    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
