from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

def create_spark_session():
    return SparkSession.builder \
        .appName("Ascend_DPD_Array_Processing") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.defaultCatalog", "ascend") \
        .config("spark.sql.catalog.ascend", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.ascend.warehouse", "s3://lf-test-1-bucket/ascend-dpd") \
        .config("spark.sql.catalog.ascend.type", "hadoop") \
        .config("spark.sql.catalog.ascend.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.files.maxPartitionBytes", "268435456") \
        .config("spark.sql.parquet.block.size", "268435456") \
        .config("spark.sql.sources.bucketing.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "2048") \
        .enableHiveSupport() \
        .getOrCreate()

def create_tables_if_not_exist(spark):
    """Create Iceberg tables with array schema"""
    
    # Summary table with 36-month arrays
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ascend.ascenddb.dpd_summary (
            CONS_ACCT_KEY STRING,
            BUREAU_MBR_ID STRING,
            PORT_TYPE_CD STRING,
            ACCT_TYPE_DTL_CD STRING,
            ACCT_OPEN_DT DATE,
            ORIG_LOAN_AMT DECIMAL(18,2),
            ACCT_CLOSED_DT DATE,
            PYMT_TERMS_CD STRING,
            PYMT_TERMS_DTL_CD STRING,
            ACCT_STAT_CD STRING,
            ACCT_PYMT_STAT_CD STRING,
            ACCT_PYMT_STAT_DTL_CD STRING,
            ACCT_CREDIT_EXT_AM DECIMAL(18,2),
            ACCT_BAL_AM DECIMAL(18,2),
            PAST_DUE_AM DECIMAL(18,2),
            ACTUAL_PYMT_AM DECIMAL(18,2),
            LAST_PYMT_DT DATE,
            SCHD_PYMT_DT DATE,
            NEXT_SCHD_PYMT_AM DECIMAL(18,2),
            INTEREST_RATE DECIMAL(10,4),
            COLLATERAL_CD STRING,
            ORIG_PYMT_DUE_DT DATE,
            WRITE_OFF_DT DATE,
            WRITE_OFF_AM DECIMAL(18,2),
            ASSET_CLASS_CD STRING,
            HI_CREDIT_AM DECIMAL(18,2),
            CASH_LIMIT_AM DECIMAL(18,2),
            COLLATERAL_AM DECIMAL(18,2),
            TOTAL_WRITE_OFF_AM DECIMAL(18,2),
            PRINCIPAL_WRITE_OFF_AM DECIMAL(18,2),
            SETTLED_AM DECIMAL(18,2),
            INTEREST_RATE_4IN DECIMAL(10,4),
            SUIT_FILED_WILFUL_DEF_STAT_CD STRING,
            WO_SETTLED_STAT_CD STRING,
            MSUBID STRING,
            CREDITLIMITAM DECIMAL(18,2),
            BALANCE_AM DECIMAL(18,2),
            BALANCE_DT DATE,
            DFLTSTATUSDT DATE,
            RESPONSIBILITY_CD STRING,
            CHARGEOFFAM DECIMAL(18,2),
            EMI_AMT DECIMAL(18,2),
            TENURE INT,
            PAYMENTRATINGCD STRING,
            PINCODE STRING,
            
            -- Array columns for 36-month history
            DPD_GRID ARRAY<INT>,
            ACCT_BAL_GRID ARRAY<DECIMAL(18,2)>,
            PAST_DUE_GRID ARRAY<DECIMAL(18,2)>,
            ACTUAL_PYMT_GRID ARRAY<DECIMAL(18,2)>,
            ACCT_STAT_GRID ARRAY<STRING>,
            
            -- Metadata
            IS_ACTIVE BOOLEAN,
            ACCT_DT STRING,
            LAST_UPDATED_TS TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.distribution-mode' = 'hash'
        )
    """)
    
    # Latest state tracker (slim table)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ascend.ascenddb.latest_dpd_state (
            CONS_ACCT_KEY STRING,
            ACCT_DT STRING,
            DPD_GRID ARRAY<INT>,
            ACCT_BAL_GRID ARRAY<DECIMAL(18,2)>,
            PAST_DUE_GRID ARRAY<DECIMAL(18,2)>,
            ACTUAL_PYMT_GRID ARRAY<DECIMAL(18,2)>,
            ACCT_STAT_GRID ARRAY<STRING>,
            LAST_UPDATED_TS TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

def process_month_with_arrays(spark, current_month, next_month):
    """
    Process one month with array-based DPD grid
    """
    print(f"Processing month: {current_month.strftime('%Y-%m')} ...")
    
    month_str = current_month.strftime('%Y-%m')
    
    # Column definitions
    base_columns = ["CONS_ACCT_KEY","BUREAU_MBR_ID","PORT_TYPE_CD","ACCT_TYPE_DTL_CD",
                   "ACCT_OPEN_DT","ORIG_LOAN_AMT","ACCT_CLOSED_DT","PYMT_TERMS_CD",
                   "PYMT_TERMS_DTL_CD","ACCT_DT","ACCT_STAT_CD","ACCT_PYMT_STAT_CD",
                   "ACCT_PYMT_STAT_DTL_CD","ACCT_CREDIT_EXT_AM","ACCT_BAL_AM","PAST_DUE_AM",
                   "ACTUAL_PYMT_AM","LAST_PYMT_DT","SCHD_PYMT_DT","NEXT_SCHD_PYMT_AM",
                   "INTEREST_RATE","COLLATERAL_CD","ORIG_PYMT_DUE_DT","WRITE_OFF_DT",
                   "WRITE_OFF_AM","ASSET_CLASS_CD","DAYS_PAST_DUE_CT","HI_CREDIT_AM",
                   "CASH_LIMIT_AM","COLLATERAL_AM","TOTAL_WRITE_OFF_AM","PRINCIPAL_WRITE_OFF_AM",
                   "SETTLED_AM","INTEREST_RATE_4IN","SUIT_FILED_WILFUL_DEF_STAT_CD",
                   "WO_SETTLED_STAT_CD","MSUBID","CREDITLIMITAM","BALANCE_AM","BALANCE_DT",
                   "DFLTSTATUSDT","RESPONSIBILITY_CD","CHARGEOFFAM","EMI_AMT","TENURE",
                   "PAYMENTRATINGCD","PINCODE"]
    
    # Read current month data
    current_df = spark.table("ascenddb.accounts_all") \
        .filter(col("ACCT_DT").between(lit(current_month), lit(next_month - relativedelta(days=1)))) \
        .select(*base_columns) \
        .repartition(2048, "CONS_ACCT_KEY")
    
    current_count = current_df.count()
    print(f"  Current month records: {current_count:,}")
    
    # Read latest state (previous cumulative)
    try:
        latest_prev = spark.table("ascend.ascenddb.latest_dpd_state")
        prev_exists = True
        prev_count = latest_prev.count()
        print(f"  Previous state records: {prev_count:,}")
    except:
        latest_prev = None
        prev_exists = False
        print(f"  No previous state (first month)")
    
    if not prev_exists:
        # First month - initialize arrays
        result_df = current_df.select(
            *base_columns,
            
            # Initialize arrays with current value at position 0, rest null
            array(
                col("DAYS_PAST_DUE_CT").cast("int"),
                *[lit(None).cast("int") for _ in range(35)]
            ).alias("DPD_GRID"),
            
            array(
                col("ACCT_BAL_AM"),
                *[lit(None).cast("decimal(18,2)") for _ in range(35)]
            ).alias("ACCT_BAL_GRID"),
            
            array(
                col("PAST_DUE_AM"),
                *[lit(None).cast("decimal(18,2)") for _ in range(35)]
            ).alias("PAST_DUE_GRID"),
            
            array(
                col("ACTUAL_PYMT_AM"),
                *[lit(None).cast("decimal(18,2)") for _ in range(35)]
            ).alias("ACTUAL_PYMT_GRID"),
            
            array(
                col("ACCT_STAT_CD"),
                *[lit(None).cast("string") for _ in range(35)]
            ).alias("ACCT_STAT_GRID"),
            
            lit(True).alias("IS_ACTIVE"),
            lit(month_str).alias("ACCT_DT"),
            current_timestamp().alias("LAST_UPDATED_TS")
        )
        
    else:
        # Subsequent months - merge with previous state
        latest_prev = latest_prev.repartition(2048, "CONS_ACCT_KEY")
        
        # Full outer join
        joined = current_df.alias("curr").join(
            latest_prev.alias("prev"),
            "CONS_ACCT_KEY",
            "full_outer"
        )
        
        # Calculate month difference for gap handling
        joined = joined.withColumn(
            "MONTH_DIFF",
            when(col("prev.ACCT_DT").isNotNull(),
                 months_between(
                     to_date(lit(month_str), "yyyy-MM"),
                     to_date(col("prev.ACCT_DT"), "yyyy-MM")
                 ).cast("int")
            ).otherwise(1)
        )
        
        # Build result with array shifting logic
        result_df = joined.select(
            # Take current month values, or previous if not present
            *[coalesce(col(f"curr.{c}"), col(f"prev.{c}")).alias(c) 
              for c in base_columns if c != "ACCT_DT"],
            
            # ACCT_DT is always current month
            lit(month_str).alias("ACCT_DT"),
            
            # ===== DPD_GRID Array Logic =====
            when(col("MONTH_DIFF") == 1,
                # Normal case: prepend current value, keep 35 from previous
                slice(
                    concat(
                        array(coalesce(col("curr.DAYS_PAST_DUE_CT").cast("int"), lit(None).cast("int"))),
                        coalesce(col("prev.DPD_GRID"), array(*[lit(None).cast("int") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).when(col("MONTH_DIFF") > 1,
                # Gap exists: prepend current, add nulls for gap, then previous
                slice(
                    concat(
                        array(coalesce(col("curr.DAYS_PAST_DUE_CT").cast("int"), lit(None).cast("int"))),
                        expr("array_repeat(cast(null as int), MONTH_DIFF - 1)"),
                        coalesce(col("prev.DPD_GRID"), array(*[lit(None).cast("int") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).otherwise(
                # New account or other case: just current value
                array(
                    coalesce(col("curr.DAYS_PAST_DUE_CT").cast("int"), lit(None).cast("int")),
                    *[lit(None).cast("int") for _ in range(35)]
                )
            ).alias("DPD_GRID"),
            
            # ===== ACCT_BAL_GRID Array Logic =====
            when(col("MONTH_DIFF") == 1,
                slice(
                    concat(
                        array(coalesce(col("curr.ACCT_BAL_AM"), lit(None).cast("decimal(18,2)"))),
                        coalesce(col("prev.ACCT_BAL_GRID"), array(*[lit(None).cast("decimal(18,2)") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).when(col("MONTH_DIFF") > 1,
                slice(
                    concat(
                        array(coalesce(col("curr.ACCT_BAL_AM"), lit(None).cast("decimal(18,2)"))),
                        expr("array_repeat(cast(null as decimal(18,2)), MONTH_DIFF - 1)"),
                        coalesce(col("prev.ACCT_BAL_GRID"), array(*[lit(None).cast("decimal(18,2)") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).otherwise(
                array(
                    coalesce(col("curr.ACCT_BAL_AM"), lit(None).cast("decimal(18,2)")),
                    *[lit(None).cast("decimal(18,2)") for _ in range(35)]
                )
            ).alias("ACCT_BAL_GRID"),
            
            # ===== PAST_DUE_GRID Array Logic =====
            when(col("MONTH_DIFF") == 1,
                slice(
                    concat(
                        array(coalesce(col("curr.PAST_DUE_AM"), lit(None).cast("decimal(18,2)"))),
                        coalesce(col("prev.PAST_DUE_GRID"), array(*[lit(None).cast("decimal(18,2)") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).when(col("MONTH_DIFF") > 1,
                slice(
                    concat(
                        array(coalesce(col("curr.PAST_DUE_AM"), lit(None).cast("decimal(18,2)"))),
                        expr("array_repeat(cast(null as decimal(18,2)), MONTH_DIFF - 1)"),
                        coalesce(col("prev.PAST_DUE_GRID"), array(*[lit(None).cast("decimal(18,2)") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).otherwise(
                array(
                    coalesce(col("curr.PAST_DUE_AM"), lit(None).cast("decimal(18,2)")),
                    *[lit(None).cast("decimal(18,2)") for _ in range(35)]
                )
            ).alias("PAST_DUE_GRID"),
            
            # ===== ACTUAL_PYMT_GRID Array Logic =====
            when(col("MONTH_DIFF") == 1,
                slice(
                    concat(
                        array(coalesce(col("curr.ACTUAL_PYMT_AM"), lit(None).cast("decimal(18,2)"))),
                        coalesce(col("prev.ACTUAL_PYMT_GRID"), array(*[lit(None).cast("decimal(18,2)") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).when(col("MONTH_DIFF") > 1,
                slice(
                    concat(
                        array(coalesce(col("curr.ACTUAL_PYMT_AM"), lit(None).cast("decimal(18,2)"))),
                        expr("array_repeat(cast(null as decimal(18,2)), MONTH_DIFF - 1)"),
                        coalesce(col("prev.ACTUAL_PYMT_GRID"), array(*[lit(None).cast("decimal(18,2)") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).otherwise(
                array(
                    coalesce(col("curr.ACTUAL_PYMT_AM"), lit(None).cast("decimal(18,2)")),
                    *[lit(None).cast("decimal(18,2)") for _ in range(35)]
                )
            ).alias("ACTUAL_PYMT_GRID"),
            
            # ===== ACCT_STAT_GRID Array Logic =====
            when(col("MONTH_DIFF") == 1,
                slice(
                    concat(
                        array(coalesce(col("curr.ACCT_STAT_CD"), lit(None).cast("string"))),
                        coalesce(col("prev.ACCT_STAT_GRID"), array(*[lit(None).cast("string") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).when(col("MONTH_DIFF") > 1,
                slice(
                    concat(
                        array(coalesce(col("curr.ACCT_STAT_CD"), lit(None).cast("string"))),
                        expr("array_repeat(cast(null as string), MONTH_DIFF - 1)"),
                        coalesce(col("prev.ACCT_STAT_GRID"), array(*[lit(None).cast("string") for _ in range(36)]))
                    ),
                    1, 36
                )
            ).otherwise(
                array(
                    coalesce(col("curr.ACCT_STAT_CD"), lit(None).cast("string")),
                    *[lit(None).cast("string") for _ in range(35)]
                )
            ).alias("ACCT_STAT_GRID"),
            
            # IS_ACTIVE flag
            when(col("curr.CONS_ACCT_KEY").isNotNull(), lit(True)).otherwise(lit(False)).alias("IS_ACTIVE"),
            
            # Timestamp
            current_timestamp().alias("LAST_UPDATED_TS")
        )
    
    # Write to summary table
    print(f"  Writing to dpd_summary...")
    result_df.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("ascend.ascenddb.dpd_summary")
    
    # Update latest state (overwrite with current state)
    print(f"  Updating latest_dpd_state...")
    result_df.select(
        "CONS_ACCT_KEY",
        "ACCT_DT",
        "DPD_GRID",
        "ACCT_BAL_GRID",
        "PAST_DUE_GRID",
        "ACTUAL_PYMT_GRID",
        "ACCT_STAT_GRID",
        "LAST_UPDATED_TS"
    ).write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("ascend.ascenddb.latest_dpd_state")
    
    result_count = result_df.count()
    print(f"  ✓ Processed {result_count:,} accounts")
    print(f"  Done\n")

def generate_dpd_grid(spark):
    """Main processing function"""
    start_month = date(1998, 1, 1)
    end_month = date(2000, 12, 15)
    
    # Create tables
    create_tables_if_not_exist(spark)
    
    current_month = start_month
    month_count = 0
    
    while current_month <= end_month:
        next_month = current_month + relativedelta(months=1)
        month_count += 1
        
        print(f"[Month {month_count}] {current_month.strftime('%Y-%m')}")
        process_month_with_arrays(spark, current_month, next_month)
        
        current_month = next_month
    
    print(f"\n{'='*60}")
    print(f"Processing complete! Processed {month_count} months")
    print(f"{'='*60}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n" + "="*60)
    print("Ascend DPD Array Processing")
    print("="*60 + "\n")
    
    generate_dpd_grid(spark)
    
    print("\n✅ All done!")
    spark.stop()

if __name__ == "__main__":
    main()