from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format, expr, floor, to_date,date_sub,trunc, month, rand
from pyspark.sql.types import IntegerType
from datetime import date
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
import datetime
import time
from pyspark.sql import Window

# LARGE SCALE CONFIGURATION for 750M records/month
# Requires cluster scaling beyond single m7g.8xlarge
spark = SparkSession.builder \
    .appName("Ascend_DPD_Processing_Large_Scale") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "268435456") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "536870912") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "536870912") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "10") \
    .config("spark.sql.autoBroadcastJoinThreshold", "1073741824") \  # 1GB for large datasets
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "536870912") \
    .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
    .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "1073741824") \
    .config("spark.network.timeout", "1800s") \  # Increased for large transfers
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.sql.defaultCatalog", "ascend") \
    .config("spark.sql.catalog.ascend", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ascend.warehouse", "s3://lf-test-1-bucket/ascend-dpd") \
    .config("spark.sql.catalog.ascend.type", "hadoop") \
    .config("spark.sql.catalog.ascend.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.files.maxPartitionBytes", "536870912") \  # 512MB per partition
    .config("spark.sql.parquet.block.size", "536870912") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.unsafe", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "50000") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.hadoop.parquet.block.size", "536870912") \
    .config("spark.hadoop.parquet.page.size", "8388608") \  # 8MB pages
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "10") \
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .config("spark.dynamicAllocation.initialExecutors", "20") \
    .config("spark.executor.cores", "5") \
    .config("spark.executor.memory", "20g") \
    .config("spark.executor.memoryFraction", "0.8") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:+UseCompressedOops") \
    .config("spark.driver.memory", "16g") \
    .config("spark.driver.maxResultSize", "8g") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:G1HeapRegionSize=32m") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def generate_dpd_grid_large_scale():
    """Optimized for 750M+ records per month"""
    start_month = datetime.date(2018, 1, 1)
    end_month = datetime.date(2020, 12, 15)
    
    month = start_month
    
    print("=== LARGE SCALE DPD PROCESSING (750M+ records/month) ===")
    
    # Pre-cache the latest summary in memory across executors
    latest_prev_base = spark.read.table("ascenddb.latest_dpd_summary") \
        .repartition(200, "CONS_ACCT_KEY") \
        .cache()
    
    # Warm up the cache
    print(f"Warming up cache with {latest_prev_base.count()} existing records")
    
    while month <= end_month:
        start_time = time.time()
        next_month = (month.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        
        print(f"\n=== Processing month: {month} ===")
        
        # PHASE 1: Load and partition current month data efficiently
        print("Phase 1: Loading current month data...")
        
        # Use predicate pushdown with minimal columns for initial processing
        current_processing_df = spark.sql(f"""
            SELECT CONS_ACCT_KEY, ACCT_DT, DAYS_PAST_DUE_CT
            FROM ascenddb.accounts_all
            WHERE ACCT_DT >= DATE '{month}' AND ACCT_DT < DATE '{next_month}'
        """).repartition(400, "CONS_ACCT_KEY").cache()  # 400 partitions for 750M records
        
        current_count = current_processing_df.count()
        print(f"Loaded {current_count:,} records for {month}")
        
        if current_count == 0:
            print(f"No records found for {month}, skipping...")
            month = next_month
            continue
        
        # PHASE 2: Optimize join strategy based on data size
        print("Phase 2: Optimizing join strategy...")
        
        latest_prev = latest_prev_base.withColumnRenamed("ACCT_DT", "ACCT_DT_prev")
        
        # Use broadcast join if latest_prev is small enough (< 1GB), otherwise sort-merge
        latest_prev_size = latest_prev.count()
        print(f"Latest previous records: {latest_prev_size:,}")
        
        if latest_prev_size < 10_000_000:  # < 10M records, likely fits in 1GB
            # Broadcast the smaller table
            latest_prev = latest_prev.repartition(50, "CONS_ACCT_KEY").cache()
            latest_prev.count()  # Force caching
            
            merged_processing_df = current_processing_df.join(
                F.broadcast(latest_prev), 
                on="CONS_ACCT_KEY", 
                how="left"
            )
        else:
            # Use bucketed sort-merge join for large tables
            latest_prev = latest_prev.repartition(400, "CONS_ACCT_KEY")
            
            merged_processing_df = current_processing_df.join(
                latest_prev, 
                on="CONS_ACCT_KEY", 
                how="left"
            )
        
        # PHASE 3: Vectorized DPD calculation
        print("Phase 3: Processing DPD grid...")
        
        merged_processing_df = merged_processing_df.withColumn(
            "MONTH_DIFF",
            F.when(F.col("ACCT_DT_prev").isNull(), 36)
            .otherwise(
                (F.month("ACCT_DT") - F.month("ACCT_DT_prev")) +
                (F.year("ACCT_DT") - F.year("ACCT_DT_prev")) * 12
            )
        )
        
        # Optimized array operations with bounds checking
        merged_processing_df = merged_processing_df.withColumn(
            "FILLER_COUNT",
            F.when(F.col("MONTH_DIFF") > 1, 
                   F.greatest(F.lit(0), F.least(F.lit(35), F.col("MONTH_DIFF") - 1)))
            .otherwise(F.lit(0))
        ).withColumn(
            "EXISTING_ARRAY",
            F.when(F.col("DPD_GRID").isNotNull(), 
                   F.split(F.col("DPD_GRID"), "~"))
            .otherwise(F.array())
        ).withColumn(
            "EXISTING_COUNT",
            F.size("EXISTING_ARRAY")
        )
        
        # Build DPD array with precise sizing
        merged_processing_df = merged_processing_df.withColumn(
            "TOTAL_NEEDED",
            F.lit(36)
        ).withColumn(
            "FILLER_ACTUAL", 
            F.least(F.col("FILLER_COUNT"), F.col("TOTAL_NEEDED") - 1)
        ).withColumn(
            "EXISTING_TAKE",
            F.greatest(F.lit(0), 
                      F.least(F.col("EXISTING_COUNT"), 
                             F.col("TOTAL_NEEDED") - 1 - F.col("FILLER_ACTUAL")))
        )
        
        # Build final array efficiently
        merged_processing_df = merged_processing_df.withColumn(
            "DPD_Array_Base",
            F.array(F.col("DAYS_PAST_DUE_CT").cast("string"))
        ).withColumn(
            "DPD_Array_With_Filler",
            F.when(F.col("FILLER_ACTUAL") > 0,
                   F.concat(F.col("DPD_Array_Base"),
                           F.expr("array_repeat('?', FILLER_ACTUAL)")))
            .otherwise(F.col("DPD_Array_Base"))
        ).withColumn(
            "DPD_Array_With_History",
            F.when(F.col("EXISTING_TAKE") > 0,
                   F.concat(F.col("DPD_Array_With_Filler"),
                           F.slice(F.col("EXISTING_ARRAY"), 1, F.col("EXISTING_TAKE"))))
            .otherwise(F.col("DPD_Array_With_Filler"))
        )
        
        # Final padding to exactly 36 elements
        merged_processing_df = merged_processing_df.withColumn(
            "DPD_Array_Final",
            F.when(F.size("DPD_Array_With_History") < 36,
                   F.concat(F.col("DPD_Array_With_History"),
                           F.expr("array_repeat('?', 36 - size(DPD_Array_With_History))")))
            .otherwise(F.slice(F.col("DPD_Array_With_History"), 1, 36))
        ).withColumn(
            "DPD_GRID_NEW",
            F.concat_ws("~", "DPD_Array_Final")
        ).withColumn(
            "DAYS_PAST_DUE", 
            F.col("DAYS_PAST_DUE_CT")
        )
        
        # PHASE 4: Prepare data for output (load full columns only for processed accounts)
        print("Phase 4: Preparing final output...")
        
        processed_accounts = merged_processing_df.select(
            "CONS_ACCT_KEY", 
            "DPD_GRID_NEW",
            "DAYS_PAST_DUE"
        ).repartition(200, "CONS_ACCT_KEY").cache()
        
        processed_count = processed_accounts.count()
        print(f"Processed {processed_count:,} unique accounts")
        
        # Load full columns only for accounts we processed
        output_columns = """CONS_ACCT_KEY, BUREAU_MBR_ID, PORT_TYPE_CD, ACCT_TYPE_DTL_CD, 
                           ACCT_OPEN_DT, ORIG_LOAN_AMT, ACCT_CLOSED_DT, PYMT_TERMS_CD, 
                           PYMT_TERMS_DTL_CD, ACCT_DT, ACCT_STAT_CD, ACCT_PYMT_STAT_CD, 
                           ACCT_PYMT_STAT_DTL_CD, ACCT_CREDIT_EXT_AM, ACCT_BAL_AM, 
                           PAST_DUE_AM, ACTUAL_PYMT_AM, LAST_PYMT_DT, SCHD_PYMT_DT, 
                           NEXT_SCHD_PYMT_AM, INTEREST_RATE, COLLATERAL_CD, ORIG_PYMT_DUE_DT, 
                           WRITE_OFF_DT, WRITE_OFF_AM, ASSET_CLASS_CD, DAYS_PAST_DUE_CT, 
                           HI_CREDIT_AM, CASH_LIMIT_AM, COLLATERAL_AM, TOTAL_WRITE_OFF_AM, 
                           PRINCIPAL_WRITE_OFF_AM, SETTLED_AM, INTEREST_RATE_4IN, 
                           SUIT_FILED_WILFUL_DEF_STAT_CD, WO_SETTLED_STAT_CD, MSUBID, 
                           CREDITLIMITAM, BALANCE_AM, BALANCE_DT, DFLTSTATUSDT, 
                           RESPONSIBILITY_CD, CHARGEOFFAM, EMI_AMT, TENURE, 
                           PAYMENTRATINGCD, PINCODE"""
        
        current_full_df = spark.sql(f"""
            SELECT {output_columns}
            FROM ascenddb.accounts_all
            WHERE ACCT_DT >= DATE '{month}' AND ACCT_DT < DATE '{next_month}'
        """).repartition(200, "CONS_ACCT_KEY")
        
        # Join processed results with full data
        final_df = current_full_df.join(
            processed_accounts.select("CONS_ACCT_KEY", "DPD_GRID_NEW", "DAYS_PAST_DUE"),
            "CONS_ACCT_KEY",
            "inner"
        ).withColumn("DPD_GRID", F.col("DPD_GRID_NEW")).drop("DPD_GRID_NEW")
        
        # PHASE 5: Optimized write with fewer large files
        print("Phase 5: Writing results...")
        
        # Write with optimal partitioning (aim for 512MB files)
        # 750M records * ~200 bytes = ~150GB, so ~300 files of 512MB each
        final_df.coalesce(300).write.format("iceberg").mode("append").saveAsTable("ascenddb.summary")
        
        # PHASE 6: Update latest_dpd_summary efficiently
        print("Phase 6: Updating latest summary...")
        
        current_month_latest = processed_accounts.select(
            "CONS_ACCT_KEY", 
            F.lit(month).cast("date").alias("ACCT_DT"),
            F.col("DPD_GRID_NEW").alias("DPD_GRID")
        )
        
        # Efficient update using anti-join
        latest_prev_clean = latest_prev_base.join(
            current_month_latest.select("CONS_ACCT_KEY"), 
            "CONS_ACCT_KEY", 
            "left_anti"
        )
        
        new_latest = current_month_latest.union(latest_prev_clean) \
            .repartition(200, "CONS_ACCT_KEY").cache()
        
        # Write and update cache
        new_latest.write.mode("overwrite").saveAsTable("ascenddb.latest_dpd_summary")
        
        # Update cache for next iteration
        latest_prev_base.unpersist()
        latest_prev_base = new_latest
        
        # Cleanup intermediate caches
        current_processing_df.unpersist()
        processed_accounts.unpersist()
        
        processing_time = time.time() - start_time
        processing_rate = current_count / processing_time if processing_time > 0 else 0
        
        print(f"✅ Month {month} completed in {processing_time/60:.1f} minutes")
        print(f"   Processing rate: {processing_rate:,.0f} records/second")
        print(f"   Memory usage: {spark.sparkContext.statusTracker().getExecutorInfos()}")
        
        month = next_month

def estimate_cluster_requirements():
    """Calculate optimal cluster configuration for 750M records/month"""
    
    records_per_month = 750_000_000
    estimated_size_gb = records_per_month * 200 / (1024**3)  # ~200 bytes per record
    
    print(f"""
    === CLUSTER REQUIREMENTS FOR 750M RECORDS/MONTH ===
    
    Estimated data size: ~{estimated_size_gb:.1f} GB per month
    Target processing time: 45-90 minutes
    
    RECOMMENDED CLUSTER CONFIGURATIONS:
    
    Option 1: Cost-Optimized
    - 1x m7g.8xlarge (driver)
    - 8x m7g.4xlarge (workers) 
    - Total: 144 vCPUs, 576 GB RAM
    - Est. time: 60-90 minutes
    - Est. cost: ~$15-20/hour
    
    Option 2: Performance-Optimized  
    - 1x m7g.12xlarge (driver)
    - 10x m7g.8xlarge (workers)
    - Total: 368 vCPUs, 1.4 TB RAM
    - Est. time: 45-60 minutes  
    - Est. cost: ~$40-50/hour
    
    Option 3: Maximum Performance
    - 1x m7g.16xlarge (driver) 
    - 15x m7g.12xlarge (workers)
    - Total: 784 vCPUs, 3.0 TB RAM
    - Est. time: 30-45 minutes
    - Est. cost: ~$80-100/hour
    """)

# Run the optimized large scale processing
generate_dpd_grid_large_scale()
estimate_cluster_requirements()