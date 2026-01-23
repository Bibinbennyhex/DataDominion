from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session():
    return SparkSession.builder.appName('ExportSummary')\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
        .config("spark.hadoop.fs.s3a.access.key", "admin")\
        .config("spark.hadoop.fs.s3a.secret.key", "password")\
        .config("spark.hadoop.fs.s3a.path.style.access", "true")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .set("spark.sql.catalog.spark_catalog.type", "hive") \
        .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .set("spark.sql.catalog.local.type", "hadoop") \
        .set("spark.sql.catalog.local.warehouse", "warehouse") \
        .getOrCreate()

spark = create_spark_session()

print("Reading default.summary...")
# Note: Using spark_catalog (Hive) so it might be default.summary or default.default.summary depending on previous setup.
# The ETL script created 'default.summary'.
try:
    df = spark.table("default.summary")
except:
    df = spark.table("default.default.summary") # Fallback

print(f"Row count: {df.count()}")

print("Casting array columns to string for CSV export...")
df_export = df.withColumn("bal_history", col("bal_history").cast("string")) \
              .withColumn("dpd_history", col("dpd_history").cast("string")) \
              .withColumn("payment_history", col("payment_history").cast("string")) \
              .withColumn("status_history", col("status_history").cast("string"))

output_path = "/home/iceberg/data/summary.csv"
print(f"Saving to {output_path}...")

df_export.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .format("csv") \
    .save(output_path)

print("✓ Export complete.")
spark.stop()
