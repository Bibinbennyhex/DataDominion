
from iceberg_account_summary import create_spark_session

def check_acct1():
    spark = create_spark_session()
    print("Checking Account 1 Timestamps...")
    
    df = spark.sql("""
        SELECT cons_acct_key, acct_dt, created_ts, updated_ts 
        FROM default.default.accounts_all 
        WHERE cons_acct_key = 1
    """)
    df.show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    check_acct1()
