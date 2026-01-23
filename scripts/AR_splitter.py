from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, when

# Get existing Spark Session
spark = SparkSession.builder.getOrCreate()

# Read from Iceberg table
# Option 1: Using Glue catalog
iceberg_df = spark.read.format("iceberg").load("glue_catalog.your_database.your_iceberg_table")

# Option 2: Using S3 path directly
# iceberg_df = spark.read.format("iceberg").load("s3://your-bucket/path/to/iceberg/table")

# Define the array columns to flatten
array_columns = [
    "actual_payment_am_history",
    "balance_am_history",
    "credit_limit_am_history",
    "past_due_am_history",
    "days_past_due_history",
    "asset_class_cd_4in_history"
]

# Start with the base columns (non-array columns)
base_columns = ["cons_acct_key", "acct_dt", "rpt_as_of_mo", "base_ts"]
select_expr = [col(c) for c in base_columns]

# Flatten each array column into 36 individual columns
for array_col in array_columns:
    # Remove '_history' suffix for cleaner column names
    col_prefix = array_col.replace("_history", "")
    
    for i in range(36):
        # Create column name like actual_payment_am_01, actual_payment_am_02, etc.
        new_col_name = f"{col_prefix}_{str(i+1).zfill(2)}"
        
        # Extract array element and handle nulls/empty strings
        select_expr.append(
            coalesce(
                when(col(array_col)[i].isNull(), lit(""))
                .when(col(array_col)[i] == "", lit(""))
                .otherwise(col(array_col)[i]),
                lit("")
            ).alias(new_col_name)
        )

# Apply the transformation
flattened_df = iceberg_df.select(*select_expr)

# Hudi write configuration
hudi_options = {
    'hoodie.table.name': 'your_hudi_table_name',
    'hoodie.datasource.write.recordkey.field': 'cons_acct_key',
    'hoodie.datasource.write.partitionpath.field': 'rpt_as_of_mo',
    'hoodie.datasource.write.table.name': 'your_hudi_table_name',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'base_ts',
    
    # Parallelism
    'hoodie.upsert.shuffle.parallelism': 400,
    'hoodie.insert.shuffle.parallelism': 400,
    'hoodie.bulkinsert.shuffle.parallelism': 400,
    
    # Hive sync for Glue catalog
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': 'your_database_name',
    'hoodie.datasource.hive_sync.table': 'your_hudi_table_name',
    'hoodie.datasource.hive_sync.partition_fields': 'rpt_as_of_mo',
    'hoodie.datasource.hive_sync.mode': 'hms',
    'hoodie.datasource.hive_sync.support_timestamp': 'true',
    
    # Storage optimization
    'hoodie.parquet.compression.codec': 'snappy',
    'hoodie.parquet.max.file.size': 134217728,
    'hoodie.parquet.small.file.limit': 104857600
}

# S3 path for Hudi table
hudi_table_path = "s3://your-bucket/path/to/hudi/table"

# Write to Hudi table
flattened_df.write.format("hudi") \
    .options(**hudi_options) \
    .mode("overwrite") \
    .save(hudi_table_path)

print("Data successfully transformed and written to Hudi table")
print(f"Records processed: {flattened_df.count()}")
