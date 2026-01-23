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

# ============================================================
# Schema Definition
# ============================================================

ACCOUNT_SUMMARY_SCHEMA = StructType([
    StructField("cons_acct_key", LongType(), False),
    StructField("rpt_as_of_mo", StringType(), False),
    StructField("bal_history", ArrayType(IntegerType()), True),
    StructField("dpd_history", ArrayType(IntegerType()), True),
    StructField("payment_history", ArrayType(IntegerType()), True),
    StructField("status_history", ArrayType(StringType()), True),
    StructField("current_balance", IntegerType(), True),
    StructField("current_dpd", IntegerType(), True),
    StructField("account_status", StringType(), True),
])

# ============================================================
# Spark Session Configuration with Iceberg (Docker Compose)
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Create a Spark session with Iceberg REST Catalog and MinIO S3 storage.
    
    This configuration is designed to work with the docker-compose environment:
    - iceberg-rest: REST catalog server on port 8181
    - minio: S3-compatible storage on port 9000
    
    When running inside the Docker network, use container names (iceberg-rest, minio).
    When running from host machine, use localhost with mapped ports.
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


# ============================================================
# Create Iceberg Table
# ============================================================

def create_iceberg_table(spark: SparkSession, database: str = "default", table_name: str = "summary"):
    """
    Create the Iceberg table with partitioning by reporting month.
    """
    full_table_name = f"{database}.{table_name}"
    
    # Drop if exists (for development)
    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
    
    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    
    # Create Iceberg table with month partitioning
    spark.sql(f"""
        CREATE TABLE {full_table_name} (
            cons_acct_key       BIGINT      NOT NULL,
            rpt_as_of_mo        STRING        NOT NULL,
            bal_history         ARRAY<INT>,
            dpd_history         ARRAY<INT>,
            payment_history     ARRAY<INT>,
            status_history      ARRAY<STRING>,
            current_balance     INT,
            current_dpd         INT,
            account_status      STRING,
            created_ts          TIMESTAMP,
            updated_ts          TIMESTAMP,
            base_ts             TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.metadata.compression-codec' = 'gzip'
        )
    """)
    
    print(f"✓ Created Iceberg table: {full_table_name}")
    return full_table_name


# ============================================================
# Sample Data Generation
# ============================================================

def generate_sample_data(spark: SparkSession):
    """
    Generate sample data matching the user's described pattern.
    All arrays must have exactly 36 elements.
    """
    
    def pad_array(arr, length=36, pad_value=None):
        """Ensure array has exactly 'length' elements."""
        if len(arr) < length:
            return arr + [pad_value] * (length - len(arr))
        return arr[:length]
    
    def pad_int_array(arr, length=36):
        """Pad integer array to exact length."""
        return pad_array(arr, length, None)
    
    def pad_string_array(arr, length=36):
        """Pad string array to exact length."""
        return pad_array(arr, length, None)
    
    sample_data = [
        # Account 1: Regular account with sporadic missing months (2025-05)
        (
            1,
            "2025-05",
            pad_int_array([
                4, 5, 6, 7, 8, 
                9, None, None, None, 11, 12, 13,
                14, 15, 16, 17, 18,
                19, 20, 21, 22, 23,
                24, None, None, 27, 28, 29,
                30, 31, 32, 33, None,
                34, 35, 36
            ]),
            pad_int_array([0, 0, 0, 30, 30, 0, None, None, None, 0, 0, 0, 0, 60, 30, 0, 0, 0, 0, 0, 0, 0, 0, None, None, 0, 0, 0, 0, 0, 0, 0, None, 0, 0, 0]),
            None,  # payment_history
            None,  # status_history
            4,
            0,
            "CURRENT"
        ),
        
        # Account 1: Next month (2025-06) - array shifted
        (
            1,
            "2025-06",
            pad_int_array([
                1, 4, 5, 6, 7,
                8, 9, None, None, None, 11, 12,
                13, 14, 15, 16, 17,
                18, 19, 20, 21, 22,
                23, 24, None, None, 27, 28,
                29, 30, 31, 32, 33,
                None, 34, 35
            ]),
            pad_int_array([0, 0, 0, 0, 30, 30, 0, None, None, None, 0, 0, 0, 0, 60, 30, 0, 0, 0, 0, 0, 0, 0, 0, None, None, 0, 0, 0, 0, 0, 0, 0, None, 0, 0]),
            None,
            None,
            1,
            0,
            "CURRENT"
        ),
        
        # Account 1: 2025-11 - shows NULLs for missing intermediate months
        (
            1,
            "2025-11",
            pad_int_array([
                5, None, None, None, None, 1, 4, 
                5, 6, 7, 8, 9,
                None, None, None, 11, 12, 13, 14,
                15, 16, 17, 18, 19,
                20, 21, 22, 23, 24,
                None, None, 27, 28, 29, 30,
                31
            ]),
            pad_int_array([0, None, None, None, None, 0, 0, 0, 0, 30, 30, 0, None, None, None, 0, 0, 0, 0, 60, 30, 0, 0, 0, 0, 0, 0, 0, 0, None, None, 0, 0, 0, 0, 0]),
            None,
            None,
            5,
            0,
            "CURRENT"
        ),
        
        # Account 2: Delinquent account (2025-05)
        (
            2,
            "2025-05",
            pad_int_array([1500 - i*50 for i in range(30)] + [None]*6),
            pad_int_array([90, 60, 30, 30, 30] + [0]*25 + [None]*6),
            None,
            None,
            1500,
            90,
            "DELINQUENT"
        ),
        
        # Account 2: Delinquent account (2025-06) - DPD increased
        (
            2,
            "2025-06",
            pad_int_array([1550 - i*50 for i in range(30)] + [None]*6),
            pad_int_array([120, 90, 60, 30, 30, 30] + [0]*24 + [None]*6),
            None,
            None,
            1550,
            120,
            "DELINQUENT"
        ),
        
        # Account 3: New account (2025-05) - only 3 months history
        (
            3,
            "2025-05",
            pad_int_array([250, 200, 150]),
            pad_int_array([0, 0, 0]),
            None,
            None,
            250,
            0,
            "CURRENT"
        ),
        
        # Account 3: New account (2025-06)
        (
            3,
            "2025-06",
            pad_int_array([300, 250, 200, 150]),
            pad_int_array([0, 0, 0, 0]),
            None,
            None,
            300,
            0,
            "CURRENT"
        ),
    ]

    # Generate 120 months of data for Account 5 (2015-01 to 2024-12) with sliding window history
    # and "NULL propagation" for missed reports.
    
    start_year = 2015
    months_count = 120
    
    # Step 1: Build Master Timeline
    # We'll use a dictionary or list to store the "true" status of the account for every month.
    # index i corresponds to month i.
    master_timeline = []
    
    for i in range(months_count):
        # Introduce Gaps/No Reporting at specific indexes
        if i in [20, 50]:
            master_timeline.append(None) # No report received this month
        elif i == 30:
             # Month 30 was reported, but with NULL balance (bad data)
             master_timeline.append("NULL_VALUE") 
        else:
            master_timeline.append(1000 + i)

    # Step 2: Generate Rows based on Master Timeline
    for i in range(months_count):
        # If current month is missing (None), we don't generate a row for the table
        current_status = master_timeline[i]
        
        if current_status is None:
            continue
            
        # Determine current values
        if current_status == "NULL_VALUE":
            current_val = None
            current_dpd = None
        else:
            current_val = current_status
            current_dpd = 0

        year = start_year + (i // 12)
        month = (i % 12) + 1
        rpt_date = f"{year}-{month:02d}"
        
        # Construct history array from master_timeline
        # Look back 35 months + current month = 36 elements
        history_list = []
        for j in range(36):
            hist_idx = i - j
            
            val = None
            if hist_idx >= 0:
                raw_val = master_timeline[hist_idx]
                if raw_val == "NULL_VALUE":
                    val = None
                else:
                    val = raw_val # Could be integer or None
            else:
                # Pre-history padding
                val = 1000
            
            history_list.append(val)

        sample_data.append((
            5,
            rpt_date,
            pad_int_array(history_list),
            pad_int_array([0] * 36),
            None,
            None,
            current_val,
            current_dpd,
            "CURRENT"
        ))

    # Account 6: Recent Account (Started 2024-01, 12 months history)
    start_year_6 = 2024
    for i in range(12):
        year = start_year_6 + (i // 12)
        month = (i % 12) + 1
        rpt_date = f"{year}-{month:02d}"
        
        current_bal = 500 + (i * 10)
        
        # History is short, padded with 0/None
        history_list = []
        for j in range(36):
            if j <= i:
                history_list.append(500 + ((i-j) * 10))
            else:
                history_list.append(None) # Pre-opening
                
        sample_data.append((
            6,
            rpt_date,
            pad_int_array(history_list),
            pad_int_array([0] * 36),
            None,
            None,
            current_bal,
            0,
            "CURRENT"
        ))

    # Account 7: Delinquent Account (24 months, increasing DPD)
    start_year_7 = 2023
    for i in range(24):
        year = start_year_7 + (i // 12)
        month = (i % 12) + 1
        rpt_date = f"{year}-{month:02d}"
        
        current_dpd = i * 5 # DPD increases by 5 every month
        if current_dpd > 30:
            status = "DELINQUENT"
        else:
            status = "CURRENT"

        history_dpd = []
        for j in range(36):
            if j <= i:
                h_dpd = (i-j) * 5
                history_dpd.append(h_dpd)
            else:
                history_dpd.append(0)
                
        sample_data.append((
            7,
            rpt_date,
            pad_int_array([2000] * 36),
            pad_int_array(history_dpd),
            None,
            None,
            2000,
            current_dpd,
            status
        ))

    # Account 8: Closed Account (Active 12 months, then Closed)
    # Active 2023-01 to 2023-12. Closed in 2024-01.
    # We generate reportings even after closing? Usually yes, showing 0 balance.
    # Let's generate 18 months (12 active + 6 closed)
    start_year_8 = 2023
    for i in range(18):
        year = start_year_8 + (i // 12)
        month = (i % 12) + 1
        rpt_date = f"{year}-{month:02d}"
        
        if i < 12:
            status = "CURRENT"
            bal = 500 - (i * 40) # Paying down
            dpd = 0
        else:
            status = "CLOSED"
            bal = 0
            dpd = 0
            
        # construct balance history
        history_bal = []
        for j in range(36):
            hist_idx = i - j
            if hist_idx < 0:
                history_bal.append(None)
            elif hist_idx < 12:
                # Active phase history
                history_bal.append(500 - (hist_idx * 40))
            else:
                # Closed phase history (0 balance)
                history_bal.append(0)

        sample_data.append((
            8,
            rpt_date,
            pad_int_array(history_bal),
            pad_int_array([0] * 36),
            None,
            None,
            bal,
            dpd,
            status
        ))
    
    df = spark.createDataFrame(sample_data, schema=ACCOUNT_SUMMARY_SCHEMA)
    
    # Add timestamps
    df = df.withColumn("created_ts", current_timestamp()) \
           .withColumn("updated_ts", current_timestamp()) \
           .withColumn("base_ts", current_timestamp())
    
    return df


# ============================================================
# Insert Data into Iceberg Table
# ============================================================

def insert_sample_data(spark: SparkSession, table_name: str = "default.account_summary"):
    """
    Insert sample data into the Iceberg table.
    """
    df = generate_sample_data(spark)
    df.writeTo(table_name).append()
    
    print(f"✓ Inserted {df.count()} rows into {table_name}")
    return df


# ============================================================
# Main Execution
# ============================================================

if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()

    print(spark)
    
    # # Create Iceberg table
    table_name = create_iceberg_table(spark)
    
    # # Insert sample data
    df = insert_sample_data(spark, table_name)
    
    # Display sample data
    print("\n" + "="*60)
    print("Sample Data Preview:")
    print("="*60)
    spark.sql(f"SELECT cons_acct_key, rpt_as_of_mo, current_balance, current_dpd, account_status FROM {table_name}").show()
    
    # Show table info
    print("\nTable History (Iceberg snapshots):")
    spark.sql(f"SELECT * FROM {table_name}.history").show(truncate=False)
    
    spark.stop()
