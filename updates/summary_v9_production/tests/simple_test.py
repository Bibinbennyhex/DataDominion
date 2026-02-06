from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print("Spark is running")
print(spark.range(5).collect())
spark.stop()
