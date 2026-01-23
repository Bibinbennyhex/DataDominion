
from iceberg_account_summary import create_spark_session
from pyspark.sql.functions import col

def verify_summary_timestamps():
    spark = create_spark_session()
    
    print("Verifying Timestamp Propagation from Accounts All to Summary...")
    
    # 1. Fetch a specific record from accounts_all (Account 5)
    print("\n--- Fetching Random Record from Accounts All ---")
    row_raw = spark.sql("""
        SELECT rpt_as_of_mo, created_ts, updated_ts 
        FROM default.default.accounts_all 
        WHERE cons_acct_key = 5 
        LIMIT 1
    """).first()
    
    if not row_raw:
        print("✗ FAIL: Could not find raw record for Account 5")
        return

    rpt_mo = row_raw['rpt_as_of_mo']
    raw_created = row_raw['created_ts']
    raw_updated = row_raw['updated_ts']
    print(f"Raw Record ({rpt_mo}): Created={raw_created}, Updated={raw_updated}")

    # 2. Fetch corresponding record from summary
    print(f"\n--- Fetching Correspoding Record from Summary ({rpt_mo}) ---")
    row_summ = spark.sql(f"""
        SELECT created_ts, updated_ts 
        FROM default.summary 
        WHERE cons_acct_key = 5 AND rpt_as_of_mo = '{rpt_mo}'
    """).first()

    if not row_summ:
        print(f"✗ FAIL: Could not find summary record for {rpt_mo}")
        return

    summ_created = row_summ['created_ts']
    summ_updated = row_summ['updated_ts']
    print(f"Summ Record ({rpt_mo}): Created={summ_created}, Updated={summ_updated}")
    
    # 3. Compare
    if raw_created == summ_created and raw_updated == summ_updated:
        print("✓ SUCCESS: Timestamps match exactly.")
    else:
        print("✗ FAIL: Timestamps do not match!")

    spark.stop()

if __name__ == "__main__":
    verify_summary_timestamps()
