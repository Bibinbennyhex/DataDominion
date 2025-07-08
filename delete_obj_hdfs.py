from pyspark.sql import SparkSession
from py4j.java_gateway import java_import

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Delete S3 Parquet Files") \
    .getOrCreate()

# Your S3 path (replace with your bucket and path)
s3_path = "s3a://your-bucket/path/to/parquet/files/"

# Get Hadoop FileSystem
hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

# Delete recursively (True for recursive)
path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)
fs.delete(path, True)

print(f"Deleted files under {s3_path}")

spark.stop()
