
from iceberg_account_summary import create_spark_session
from pyspark.sql.functions import col, month, year, dayofmonth

def verify_timestamps():
    spark = create_spark_session()
    
    print("Verifying Timestamp Randomization...")
    
    # 1. Check Date Variation for Account 5 (should be 20th-28th)
    print("\n--- Checking Day Variation (Account 5) ---")
    df_days = spark.sql("""
        SELECT day(acct_dt) as d, count(*) as cnt 
        FROM default.default.accounts_all 
        WHERE cons_acct_key = 5 
        GROUP BY day(acct_dt) 
        ORDER BY d
    """)
    df_days.show()
    day_count = df_days.count()
    if day_count > 1:
        print(f"✓ Days are varied: Found {day_count} distinct days.")
    else:
        print("✗ FAIL: Days are not varied!")

    # 2. Check Timestamp Alignment
    print("\n--- Checking Timestamp Alignment ---")
    # created_ts should be close to acct_dt
    df_sample = spark.sql("""
        SELECT acct_dt, created_ts, updated_ts 
        FROM default.default.accounts_all 
        WHERE cons_acct_key = 5 
        LIMIT 5
    """)
    df_sample.show()
    
    # Verify year matches
    row = df_sample.first()
    if row['acct_dt'].year == row['created_ts'].year:
         print(f"✓ Year matches: {row['acct_dt'].year}")
    else:
         print(f"✗ FAIL: Date Year {row['acct_dt'].year} != TS Year {row['created_ts'].year}")

    spark.stop()

if __name__ == "__main__":
    verify_timestamps()
