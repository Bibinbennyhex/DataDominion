from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format, expr, floor, to_date,date_sub,trunc, month, rand
from pyspark.sql.types import IntegerType,LongType,DoubleType
from datetime import date
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
import datetime
import time

start_time = time.time()

spark = SparkSession.builder \
    .appName("Ascend_Dummy_Data_Generator") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold","-1") \
    .config("spark.sql.join.preferSortMergeJoin", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true")\
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")\
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.sql.defaultCatalog", "ascend") \
    .config("spark.sql.catalog.ascend", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ascend.warehouse", "s3://lf-test-1-bucket/ascend-dpd") \
    .config("spark.sql.catalog.ascend.type", "hadoop") \
    .config("spark.sql.catalog.ascend.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.sources.bucketing.enabled","true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.files.maxPartitionBytes", "134217728") \
    .config("spark.sql.catalog.ascend.write.metadata.delete-after-commit.enabled", "true")\
    .config("spark.sql.catalog.ascend.write.metadata.previous-versions-max", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

def generate_iceberg_schema():
    spark.sql("""create table if not exists ascenddb.accounts_all_bucketed (
                        CONS_ACCT_KEY BIGINT,
                        ACCT_BAL_AM BIGINT,
                        ACCT_CLOSED_DT DATE,
                        ACCT_CREDIT_EXT_AM BIGINT,
                        ACCT_DT DATE,
                        ACCT_OPEN_DT DATE,
                        ACCT_PYMT_STAT_CD VARCHAR(255),
                        ACCT_PYMT_STAT_DTL_CD VARCHAR(255),
                        ACCT_STAT_CD VARCHAR(255),
                        ACCT_TYPE_DTL_CD VARCHAR(255),
                        ACTUAL_PYMT_AM BIGINT,
                        ASSET_CLASS_CD VARCHAR(255),
                        BUREAU_MBR_ID VARCHAR(255),
                        CASH_LIMIT_AM BIGINT,
                        COLLATERAL_AM BIGINT,
                        COLLATERAL_CD  VARCHAR(255),
                        DAYS_PAST_DUE_CT INT,
                        HI_CREDIT_AM BIGINT,
                        INTEREST_RATE FLOAT,
                        INTEREST_RATE_4IN FLOAT,
                        LAST_PYMT_DT DATE,
                        NEXT_SCHD_PYMT_AM BIGINT,
                        ORIG_LOAN_AMT BIGINT,
                        ORIG_PYMT_DUE_DT DATE,
                        PAST_DUE_AM BIGINT,
                        PORT_TYPE_CD  VARCHAR(255),
                        PRINCIPAL_WRITE_OFF_AM BIGINT,
                        PYMT_TERMS_CD  VARCHAR(255),
                        PYMT_TERMS_DTL_CD  VARCHAR(255),
                        SCHD_PYMT_DT DATE,
                        SETTLED_AM BIGINT,
                        SUIT_FILED_WILFUL_DEF_STAT_CD  VARCHAR(255),
                        TOTAL_WRITE_OFF_AM BIGINT,
                        WO_SETTLED_STAT_CD  VARCHAR(255),
                        WRITE_OFF_AM BIGINT,
                        WRITE_OFF_DT DATE,
                        MSUBID VARCHAR(255),
                        CREDITLIMITAM BIGINT,
                        BALANCE_AM BIGINT,
                        BALANCE_DT DATE,
                        DFLTSTATUSDT DATE,
                        RESPONSIBILITY_CD VARCHAR(255),
                        CHARGEOFFAM BIGINT,
                        EMI_AMT BIGINT,
                        TENURE BIGINT,
                        PAYMENTRATINGCD VARCHAR(255),
                        PINCODE VARCHAR(255)
                        )
                        USING ICEBERG
                        PARTITIONED BY (months(ACCT_DT))
                        CLUSTERED BY (CONS_ACCT_KEY) INTO 64 BUCKETS""")

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Created schema for accounts_all")

    spark.sql("""
    CREATE TABLE if not exists ascenddb.summary(
                        CONS_ACCT_KEY BIGINT,
                        BUREAU_MBR_ID VARCHAR(255),
                        PORT_TYPE_CD VARCHAR(255),
                        ACCT_TYPE_DTL_CD VARCHAR(255),
                        ACCT_OPEN_DT DATE,
                        ORIG_LOAN_AMT BIGINT,
                        ACCT_CLOSED_DT DATE,
                        PYMT_TERMS_CD VARCHAR(255),
                        PYMT_TERMS_DTL_CD VARCHAR(255),
                        ACCT_DT DATE,
                        DPD_GRID STRING,
                        ACCT_STAT_CD VARCHAR(255),
                        ACCT_PYMT_STAT_CD VARCHAR(255),
                        ACCT_PYMT_STAT_DTL_CD VARCHAR(255),
                        ACCT_CREDIT_EXT_AM BIGINT,
                        ACCT_BAL_AM BIGINT,
                        PAST_DUE_AM BIGINT,
                        ACTUAL_PYMT_AM BIGINT,
                        LAST_PYMT_DT DATE,
                        SCHD_PYMT_DT DATE,
                        NEXT_SCHD_PYMT_AM BIGINT,
                        INTEREST_RATE FLOAT,
                        COLLATERAL_CD VARCHAR(255),
                        ORIG_PYMT_DUE_DT DATE,
                        WRITE_OFF_DT DATE,
                        WRITE_OFF_AM BIGINT,
                        ASSET_CLASS_CD VARCHAR(255),
                        DAYS_PAST_DUE INT,
                        DAYS_PAST_DUE_CT INT,
                        HI_CREDIT_AM BIGINT,
                        CASH_LIMIT_AM BIGINT,
                        COLLATERAL_AM BIGINT,
                        TOTAL_WRITE_OFF_AM BIGINT,
                        PRINCIPAL_WRITE_OFF_AM BIGINT,
                        SETTLED_AM BIGINT,
                        INTEREST_RATE_4IN FLOAT,
                        SUIT_FILED_WILFUL_DEF_STAT_CD VARCHAR(255),
                        WO_SETTLED_STAT_CD VARCHAR(255),
                        MSUBID VARCHAR(255),
                        CREDITLIMITAM BIGINT,
                        BALANCE_AM BIGINT,
                        BALANCE_DT DATE,
                        DFLTSTATUSDT DATE,
                        RESPONSIBILITY_CD VARCHAR(255),
                        CHARGEOFFAM BIGINT,
                        EMI_AMT BIGINT,
                        TENURE BIGINT,
                        PAYMENTRATINGCD VARCHAR(255),
                        PINCODE VARCHAR(255)          
        )
    USING iceberg
    PARTITIONED BY (months(ACCT_DT))
    CLUSTERED BY (CONS_ACCT_KEY) INTO 64 BUCKETS""")

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Created schema for summary")

    spark.sql("""
    CREATE TABLE if not exists ascenddb.latest_dpd_summary(
        CONS_ACCT_KEY INT,
        ACCT_DT DATE, 
        DPD_GRID STRING
        )
    USING iceberg
    CLUSTERED BY (CONS_ACCT_KEY) INTO 64 BUCKETS""")

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Created schema for latest_dpd_summary")

def dummy_data_creation():
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Started with generation of dummy data")
    num_accounts = 720000000
    # months_needed = 335  # e.g., 10 years
    months_needed = 1  # e.g., 10 years
    total_records = num_accounts * months_needed

    # Step 1: Create DataFrame for all combinations
    df = spark.range(0, total_records)

    # Step 2: Generate account keys and month indices
    df = df.withColumn("CONS_ACCT_KEY", (col("id") / months_needed).cast("int") + 1) \
        .withColumn("month_index", (col("id") % months_needed).cast("int"))

    # Step 3: Generate date for each month (15th of month starting from Jan 2025)
    start_date = datetime.date(2018, 1, 15)
    dates = [start_date + relativedelta(months=i) for i in range(months_needed)]
    date_map = spark.createDataFrame([(i, d.strftime("%m/%d/%Y")) for i, d in enumerate(dates)],
                                    ["month_index", "ACCT_DT"])

    # Step 4: Join to get ACCT_DT
    df = df.join(date_map, on="month_index", how="left")

    # Step 5: Add DPD (random or deterministic)
    df = df.withColumn("DAYS_PAST_DUE_CT", ((col("CONS_ACCT_KEY") + col("month_index")) % 100 + 1))


    # Add dummy columns
    df = df.withColumn("BUREAU_MBR_ID", expr("concat('MBR', CONS_ACCT_KEY)")) \
           .withColumn("ACCT_BAL_AM", (rand() * 100000).cast("bigint")) \
           .withColumn("ACCT_CLOSED_DT", to_date(lit("2025-12-31"))) \
           .withColumn("ACCT_CREDIT_EXT_AM", (rand() * 50000).cast("bigint")) \
           .withColumn("ACCT_OPEN_DT", to_date(lit("2018-01-01"))) \
           .withColumn("ACCT_PYMT_STAT_CD", lit("A")) \
           .withColumn("ACCT_PYMT_STAT_DTL_CD", lit("A1")) \
           .withColumn("ACCT_STAT_CD", lit("O")) \
           .withColumn("ACCT_TYPE_DTL_CD", lit("T1")) \
           .withColumn("ACTUAL_PYMT_AM", (rand() * 10000).cast("bigint")) \
           .withColumn("ASSET_CLASS_CD", lit("STD")) \
           .withColumn("CASH_LIMIT_AM", (rand() * 20000).cast("bigint")) \
           .withColumn("COLLATERAL_AM", (rand() * 15000).cast("bigint")) \
           .withColumn("COLLATERAL_CD", lit("C1")) \
           .withColumn("HI_CREDIT_AM", (rand() * 120000).cast("bigint")) \
           .withColumn("INTEREST_RATE", (rand() * 15).cast("float")) \
           .withColumn("INTEREST_RATE_4IN", (rand() * 15).cast("float")) \
           .withColumn("LAST_PYMT_DT", to_date(lit("2025-06-30"))) \
           .withColumn("NEXT_SCHD_PYMT_AM", (rand() * 8000).cast("bigint")) \
           .withColumn("ORIG_LOAN_AMT", (rand() * 100000).cast("bigint")) \
           .withColumn("ORIG_PYMT_DUE_DT", to_date(lit("2025-01-15"))) \
           .withColumn("PAST_DUE_AM", (rand() * 5000).cast("bigint")) \
           .withColumn("PORT_TYPE_CD", lit("PT1")) \
           .withColumn("PRINCIPAL_WRITE_OFF_AM", (rand() * 3000).cast("bigint")) \
           .withColumn("PYMT_TERMS_CD", lit("M")) \
           .withColumn("PYMT_TERMS_DTL_CD", lit("M1")) \
           .withColumn("SCHD_PYMT_DT", to_date(lit("2025-07-15"))) \
           .withColumn("SETTLED_AM", (rand() * 2000).cast("bigint")) \
           .withColumn("SUIT_FILED_WILFUL_DEF_STAT_CD", lit("N")) \
           .withColumn("TOTAL_WRITE_OFF_AM", (rand() * 4000).cast("bigint")) \
           .withColumn("WO_SETTLED_STAT_CD", lit("Y")) \
           .withColumn("WRITE_OFF_AM", (rand() * 3500).cast("bigint")) \
           .withColumn("WRITE_OFF_DT", to_date(lit("2025-05-31"))) \
           .withColumn("CREDITLIMITAM", (rand() * 4000).cast("bigint")) \
           .withColumn("BALANCE_AM", (rand() * 4000).cast("bigint")) \
           .withColumn("CHARGEOFFAM", (rand() * 4000).cast("bigint")) \
           .withColumn("EMI_AMT", (rand() * 4000).cast("bigint")) \
           .withColumn("TENURE", (rand() * 4000).cast("bigint")) \
           .withColumn("MSUBID", lit("MSUB")) \
           .withColumn("RESPONSIBILITY_CD", lit("PT1")) \
           .withColumn("PAYMENTRATINGCD", lit("PT1")) \
           .withColumn("PINCODE", lit("687562")) \
           .withColumn("BALANCE_DT", to_date(lit("2025-05-31"))) \
           .withColumn("DFLTSTATUSDT", to_date(lit("2025-05-31")))


    df = df.select(
                'CONS_ACCT_KEY',
                'ACCT_BAL_AM',
                'ACCT_CLOSED_DT',
                'ACCT_CREDIT_EXT_AM',
                'ACCT_DT',
                'ACCT_OPEN_DT',
                'ACCT_PYMT_STAT_CD',
                'ACCT_PYMT_STAT_DTL_CD',
                'ACCT_STAT_CD',
                'ACCT_TYPE_DTL_CD',
                'ACTUAL_PYMT_AM',
                'ASSET_CLASS_CD',
                'BUREAU_MBR_ID',
                'CASH_LIMIT_AM',
                'COLLATERAL_AM',
                'COLLATERAL_CD',
                'DAYS_PAST_DUE_CT',
                'HI_CREDIT_AM',
                'INTEREST_RATE',
                'INTEREST_RATE_4IN',
                'LAST_PYMT_DT',
                'NEXT_SCHD_PYMT_AM',
                'ORIG_LOAN_AMT',
                'ORIG_PYMT_DUE_DT',
                'PAST_DUE_AM',
                'PORT_TYPE_CD',
                'PRINCIPAL_WRITE_OFF_AM',
                'PYMT_TERMS_CD',
                'PYMT_TERMS_DTL_CD',
                'SCHD_PYMT_DT',
                'SETTLED_AM',
                'SUIT_FILED_WILFUL_DEF_STAT_CD',
                'TOTAL_WRITE_OFF_AM',
                'WO_SETTLED_STAT_CD',
                'WRITE_OFF_AM',
                'WRITE_OFF_DT',
                'MSUBID',
                'CREDITLIMITAM',
                'BALANCE_AM',
                'BALANCE_DT',
                'DFLTSTATUSDT',
                'RESPONSIBILITY_CD',
                'CHARGEOFFAM',
                'EMI_AMT',
                'TENURE',
                'PAYMENTRATINGCD',
                'PINCODE'
    )

    df = df.withColumn("ACCT_DT",to_date(col("ACCT_DT"), "MM/dd/yyyy"))

    #df = df.withColumn("ACCT_BAL_AM", col("ACCT_BAL_AM").cast("bigint"))
    
    df.printSchema()
    
    spark.table("ascenddb.accounts_all_bucketed").printSchema()

    # Step 6: Sort within partitions for optimized joins
    df = df.sortWithinPartitions("CONS_ACCT_KEY", "ACCT_DT")

    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Writing Data to Table")

    df.write\
    .format("iceberg")\
    .mode("append")\
    .option("compression", "snappy")\
    .option("target-file-size-bytes", 536870912) \
    .insertInto("ascenddb.accounts_all_bucketed")

    df.explain()
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Data generated and saved to accounts_all_bucketed")

def scenario_creation():
    queries = [
        "DELETE from ascenddb.accounts_all_bucketed where CONS_ACCT_KEY = 336 AND ACCT_DT = '2019-01-15'",
        "DELETE FROM ascenddb.accounts_all_bucketed WHERE CONS_ACCT_KEY = 337 AND ACCT_DT = '2019-01-15'",
        "DELETE FROM ascenddb.accounts_all_bucketed WHERE CONS_ACCT_KEY = 337 AND ACCT_DT = '2019-02-15'",
        "DELETE FROM ascenddb.accounts_all_bucketed WHERE CONS_ACCT_KEY = 338 AND ACCT_DT = '2019-01-15'",
    ]

    for query in queries:
        spark.sql(query)    
    
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Scenario Created")

#generate_iceberg_schema()
dummy_data_creation()
scenario_creation()

processing_time = (start_time - time.time())/60
print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Execution Completed in {processing_time} minutes")


