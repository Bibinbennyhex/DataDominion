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
import pandas as pd

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
    StructField("created_ts", TimestampType(), True),
    StructField("updated_ts", TimestampType(), True),
    StructField("base_ts", TimestampType(), True),
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
# Downstream Data Population (ETL from Accounts All)
# ============================================================

def build_summary_from_accounts(spark: SparkSession, summary_table: str = "default.summary"):
    """
    Populates the summary table by reading from default.accounts_all.
    Logic:
      1. Read accounts_all.
      2. For each account:
         - Deduplicate reports per month (keep latest acct_dt).
         - Construct 36-month history arrays (reverse chronological).
      3. Write to summary table.
    """
    # Read strict from proper catalog path
    df_raw = spark.table("base.accounts_all")
    
    # Define Pandas UDF for logic
    def process_account_history(pdf):
        # 1. Deduplicate: Sort by Date Desc, Drop Dups on Month (Keep First/Latest), Sort by Month Asc
        pdf['acct_dt'] = pd.to_datetime(pdf['acct_dt'])
        pdf = pdf.sort_values('acct_dt', ascending=False)
        pdf = pdf.drop_duplicates(subset=['rpt_as_of_mo'], keep='first')
        pdf = pdf.sort_values('rpt_as_of_mo', ascending=True)
        
        # 2. Build History Lookup
        # Convert YYYY-MM to integer for easy math (Year * 12 + Month)
        def mo_to_int(m_str):
            y, m = map(int, m_str.split('-'))
            return y * 12 + m
            
        history_map = {}
        for idx, row in pdf.iterrows():
            m_int = mo_to_int(row['rpt_as_of_mo'])
            history_map[m_int] = row.to_dict()
            
        output_rows = []
        
        # 3. Generate Summary Rows
        for idx, row in pdf.iterrows():
            curr_mo_int = mo_to_int(row['rpt_as_of_mo'])
            
            bal_hist = []
            dpd_hist = []
            pay_hist = []
            stat_hist = []
            
            # 36-month lookback (Index 0 is Current Month)
            for k in range(36):
                target = curr_mo_int - k
                if target in history_map:
                    h_row = history_map[target]
                    bal_hist.append(h_row['current_balance'])
                    dpd_hist.append(h_row['current_dpd'])
                    pay_hist.append(h_row['payment_am'])
                    stat_hist.append(str(h_row['status_cd']) if h_row['status_cd'] is not None else None)
                else:
                    bal_hist.append(None)
                    dpd_hist.append(None)
                    pay_hist.append(None)
                    stat_hist.append(None)
            
            # Construct row
            # Timestamps are passed through from the raw record
            out = {
                'cons_acct_key': row['cons_acct_key'],
                'rpt_as_of_mo': row['rpt_as_of_mo'],
                'bal_history': bal_hist,
                'dpd_history': dpd_hist,
                'payment_history': pay_hist,
                'status_history': stat_hist,
                'current_balance': row['current_balance'],
                'current_dpd': row['current_dpd'],
                'account_status': row['account_status'],
                'created_ts': row['created_ts'],
                'updated_ts': row['updated_ts'],
                'base_ts': row['base_ts']
            }
            output_rows.append(out)
            
        return pd.DataFrame(output_rows)

    # Apply UDF
    print("Processing accounts to generate summary history...")
    df_summary = df_raw.groupBy("cons_acct_key").applyInPandas(
        process_account_history, 
        schema=ACCOUNT_SUMMARY_SCHEMA
    )
    
    # Write
    print(f"Writing to {summary_table}...")
    df_summary.writeTo(summary_table).append()
    print("✓ Data populated.")
    return df_summary


# ============================================================
# Main Execution
# ============================================================

if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()

    print(spark)
    
    # # Create Iceberg table
    full_table_name = create_iceberg_table(spark, database="default", table_name="summary")
    
    # # Build/Populate from Accounts All
    df = build_summary_from_accounts(spark, full_table_name)
    
    # Display sample data
    print("\n" + "="*60)
    print("Sample Data Preview:")
    print("="*60)
    spark.sql(f"SELECT cons_acct_key, rpt_as_of_mo, current_balance, current_dpd, account_status FROM {full_table_name} LIMIT 20").show()
    
    # Show table info
    print("\nTable History (Iceberg snapshots):")
    spark.sql(f"SELECT * FROM {full_table_name}.history").show(truncate=False)
    
    spark.stop()
