
from iceberg_account_summary import create_spark_session
from pyspark.sql.functions import col

def verify_gaps():
    spark = create_spark_session()
    
    print("Verifying Gaps for Account 5...")
    
    # Gap 1: 2016-09 was skipped.
    # Look at 2016-10 report. History index 1 should be NULL.
    df_1 = spark.sql("""
        SELECT rpt_as_of_mo, bal_history, dpd_history 
        FROM default.summary 
        WHERE cons_acct_key = 5 AND rpt_as_of_mo = '2016-10'
    """)
    if df_1.count() > 0:
        row = df_1.first()
        bal_hist = row['bal_history']
        print(f"Report 2016-10: History[1] (2016-09) = {bal_hist[1]}")
        if bal_hist[1] is None:
            print("✓ Gap 1 Verified: Value is None as expected.")
        else:
            print(f"✗ Gap 1 Failed: Value is {bal_hist[1]}")
    else:
        print("✗ Record for 2016-10 not found!")

    # Gap 2: 2019-03 was skipped.
    # Look at 2019-04 report. History index 1 should be NULL.
    df_2 = spark.sql("""
        SELECT rpt_as_of_mo, bal_history, dpd_history 
        FROM default.summary 
        WHERE cons_acct_key = 5 AND rpt_as_of_mo = '2019-04'
    """)
    if df_2.count() > 0:
        row = df_2.first()
        bal_hist = row['bal_history']
        print(f"Report 2019-04: History[1] (2019-03) = {bal_hist[1]}")
        if bal_hist[1] is None:
            print("✓ Gap 2 Verified: Value is None as expected.")
        else:
            print(f"✗ Gap 2 Failed: Value is {bal_hist[1]}")
    else:
        print("✗ Record for 2019-04 not found!")

    spark.stop()

if __name__ == "__main__":
    verify_gaps()
