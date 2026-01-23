
from pyspark.sql import SparkSession

def verify_bulk_volume():
    from pyspark.conf import SparkConf
    conf = (
        SparkConf()
        .setAppName("VerifyBulkVolume")
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalog.spark_catalog.type", "hive")
        .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.local.type", "hadoop")
        .set("spark.sql.catalog.local.warehouse", "warehouse")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    print("Verifying Bulk Data Volume...")
    
    # Filter for the bulk batch
    df = spark.read.format("iceberg").load("default.accounts_all").filter("base_ts = '2025-07-05 09:00:00'")
    
    total_count = df.count()
    print(f"Total Rows in Bulk Batch: {total_count}")
    
    print("Row Count per Account:")
    df.groupBy("cons_acct_key").count().show()
    
    if total_count > 500:
        print("SUCCESS: Volume requirement met (>500 rows).")
    else:
        print("FAILURE: Volume requirement NOT met (<500 rows).")

if __name__ == "__main__":
    verify_bulk_volume()
