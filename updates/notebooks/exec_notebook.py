from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, DateType, ArrayType,
    DecimalType, IntegerType, StringType, TimestampType
)
from pyspark.sql.functions import (
    col, lit, array, current_timestamp, explode, posexplode,
    when, size, element_at, expr, months_between, add_months
)
from datetime import date
from decimal import Decimal

# Cell 1: Setup
print("--- executing cell 1 ---")
def create_spark_session() -> SparkSession:
    """
    Create a Spark session with Iceberg REST Catalog and MinIO S3 storage.
    """
    spark = SparkSession.builder.appName('Ascend')\
                                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
                                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
                                .config("spark.hadoop.fs.s3a.access.key", "admin")\
                                .config("spark.hadoop.fs.s3a.secret.key", "password")\
                                .config("spark.hadoop.fs.s3a.path.style.access", "true")\
                                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
                                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
                                .config("spark.sql.adaptive.enabled", "true")\
                                .config("spark.sql.adaptive.skewJoin.enabled", "true")\
                                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")\
                                .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")\
                                .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")    
    return spark

spark = create_spark_session()
print("Spark Session Created")

# Cell 2: Query
print("\n--- executing cell 2 ---")
print("Querying default.summary...")
df = spark.sql("SELECT * FROM default.summary")
df.show()

# Cell 3: Schema
print("\n--- executing cell 3 ---")
df.printSchema()

# Cell 4: Stop
print("\n--- executing cell 4 ---")
spark.stop()
