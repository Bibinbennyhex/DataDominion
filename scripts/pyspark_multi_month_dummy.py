from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr, rand, to_date
from datetime import date
from dateutil.relativedelta import relativedelta
import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Ascend_Dummy_Data_Generator") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "ascend") \
    .config("spark.sql.catalog.ascend", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ascend.type", "hadoop") \
    .config("spark.sql.catalog.ascend.warehouse", "s3://lf-test-1-bucket/ascend-dpd") \
    .config("spark.sql.catalog.ascend.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.sources.bucketing.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
    .config("spark.sql.catalog.ascend.write.metadata.delete-after-commit.enabled", "true") \
    .config("spark.sql.catalog.ascend.write.metadata.previous-versions-max", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")


def generate_data_for_month(month_date: str, batch_size=10_000_000):
    total_records = 720_000_000
    num_batches = total_records // batch_size

    for batch_id in range(num_batches):
        print(f"[{datetime.datetime.now()}] Processing batch {batch_id + 1}/{num_batches} for {month_date}")

        batch_start = batch_id * batch_size
        batch_end = batch_start + batch_size

        df = spark.range(batch_start, batch_end) \
            .withColumn("CONS_ACCT_KEY", (col("id") + 1).cast("long")) \
            .withColumn("ACCT_DT", to_date(lit(month_date))) \
            .withColumn("DAYS_PAST_DUE_CT", ((col("CONS_ACCT_KEY") % 100) + 1)) \
            .withColumn("BUREAU_MBR_ID", expr("concat('MBR', CONS_ACCT_KEY)")) \
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
            .withColumn("MSUBID", lit("MSUB")) \
            .withColumn("CREDITLIMITAM", (rand() * 4000).cast("bigint")) \
            .withColumn("BALANCE_AM", (rand() * 4000).cast("bigint")) \
            .withColumn("BALANCE_DT", to_date(lit("2025-05-31"))) \
            .withColumn("DFLTSTATUSDT", to_date(lit("2025-05-31"))) \
            .withColumn("RESPONSIBILITY_CD", lit("PT1")) \
            .withColumn("CHARGEOFFAM", (rand() * 4000).cast("bigint")) \
            .withColumn("EMI_AMT", (rand() * 4000).cast("bigint")) \
            .withColumn("TENURE", (rand() * 4000).cast("bigint")) \
            .withColumn("PAYMENTRATINGCD", lit("PT1")) \
            .withColumn("PINCODE", lit("687562"))

        df = df.sortWithinPartitions("CONS_ACCT_KEY", "ACCT_DT")
        df = df.repartition(200, "CONS_ACCT_KEY")

        df.write \
            .format("iceberg") \
            .mode("append") \
            .option("compression", "snappy") \
            .option("target-file-size-bytes", 536870912) \
            .option("fanout-enabled", "true") \
            .option("fanout-writer.max-output-files", "200") \
            .insertInto("ascenddb.accounts_all_bucketed")


# Generate data for N months
def generate_multiple_months(start_date_str="2018-01-15", num_months=1):
    start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    for i in range(num_months):
        month_date = (start_date + relativedelta(months=i)).strftime('%Y-%m-%d')
        generate_data_for_month(month_date)


# Run for 1 month as example
generate_multiple_months("2018-01-15", num_months=1)
print("[DONE] Dummy data generated.")
