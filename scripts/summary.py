import json
import boto3
import logging
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, StringType
import datetime
from datetime import date
from datetime import datetime as datetime_sub
import sys
import time
import math
from dateutil.relativedelta import relativedelta

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False
start_time = time.time()

TARGET_RECORDS_PER_PARTITION = 10_000_000
MIN_PARTITIONS = 16
MAX_PARTITIONS = 8192
AVG_RECORD_SIZE_BYTES = 200
SNAPSHOT_INTERVAL = 12
MAX_FILE_SIZE = 256


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Summary_Grid_Generation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .config("spark.network.timeout", "800s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.defaultCatalog", "primary_catalog") \
        .config("spark.sql.catalog.primary_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.primary_catalog.warehouse", "s3://in-cs-ivaps-data-bucket-prod-ap-south-1/ivaps_prod/persistent/ascend_summary/summary_complete/") \
        .config("spark.sql.catalog.primary_catalog.type", "glue") \
        .config("spark.sql.catalog.primary_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
        .config("spark.executor.instances", "10") \
        .config("spark.executor.cores", "5") \
        .config("spark.executor.memory", "30g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.join.preferSortMergeJoin", "true") \
        .config("spark.sql.files.maxPartitionBytes", "268435456") \
        .config("spark.sql.parquet.block.size", "268435456") \
        .config("spark.sql.sources.bucketing.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "8") \
        .config("spark.dynamicAllocation.initialExecutors", "8") \
        .config("spark.dynamicAllocation.maxExecutors", "200") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

def optimize_iceberg_table(spark, latest_history_table):
    latest_history_table_name = latest_history_table.split('.')[-1]
    latest_history_db = latest_history_table.split('.')[1]
    latest_history_catalog = latest_history_table.split('.')[0]
    
    logging.info("--------------------OPTIMIZING LATEST SUMMARY--------------------")
    logging.info("REWRITING FILES")
    spark.sql(f"CALL {latest_history_catalog}.system.rewrite_data_files('{latest_history_db}.{latest_history_table_name}')").show()
    logging.info("EXPIRING SNAPSHOTS")
    spark.sql(f"CALL {latest_history_catalog}.system.expire_snapshots(table => '{latest_history_db}.{latest_history_table_name}', retain_last => 1)").show()
    logging.info("--------------------COMPLETED TABLE OPTIMIZATION--------------------")


def load_config(bucket, key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    config_data = json.loads(obj['Body'].read().decode('utf-8'))
    return config_data


def calculate_optimal_partitions(record_count, avg_record_size_bytes=200):
    """Calculate optimal partition count"""
    partitions_by_count = record_count / TARGET_RECORDS_PER_PARTITION
    data_size_mb = (record_count * avg_record_size_bytes) / (1024 * 1024)
    partitions_by_size = data_size_mb / MAX_FILE_SIZE
    
    optimal_partitions = max(partitions_by_count, partitions_by_size)
    power_of_2 = 2 ** round(math.log2(optimal_partitions))
    final_partitions = max(MIN_PARTITIONS, min(MAX_PARTITIONS, power_of_2))
    
    return int(final_partitions)


def configure_spark_for_partitions(spark, num_partitions):
    """Dynamically reconfigure Spark for current partition count"""
    spark.conf.set("spark.sql.shuffle.partitions", str(num_partitions))
    spark.conf.set("spark.default.parallelism", str(num_partitions))
    logging.info(f"Configured Spark for {num_partitions} partitions")


def replace_invalid_years(df, columns):
    for col_name in columns:
        df = df.withColumn(col_name, F.when(F.year(F.col(col_name)) < 1000, None).otherwise(F.to_date(F.col(col_name))))
    return df


def initial_rolling_column_mapper(rolling_mapper_column, rolling_data_type, num_cols):
    type_map = {"Integer": IntegerType(), "String": StringType()}
    
    initial_rolling_expr = F.array(
        F.col(rolling_mapper_column).cast(type_map[rolling_data_type]),
        *[F.lit(None).cast(type_map[rolling_data_type]) for _ in range(num_cols - 1)]
    )
    
    return initial_rolling_expr


def subsequent_rolling_column_mapper(
        rolling_col, rolling_mapper_column, rolling_data_type, num_cols
):
    type_map = {"Integer": IntegerType(), "String": StringType()}
    
    subsequent_rolling_expr = (
        F.when(
            F.col("MONTH_DIFF") == 1,
            # Normal case: prepend current value, keep n-1 from previous
            F.slice(
                F.concat(
                    F.array(
                        F.coalesce(
                            F.col(f"c.{rolling_mapper_column}").cast(
                                type_map[rolling_data_type]
                            ),
                            F.lit(None).cast(type_map[rolling_data_type])
                        )
                    ),
                    F.coalesce(
                        F.col(f"p.{rolling_col}_history"),
                        F.array(
                            *[
                                F.lit(None).cast(type_map[rolling_data_type])
                                for _ in range(num_cols)
                            ]
                        )
                    )
                ),
                1,
                num_cols
            )
        )
        .when(
            F.col("MONTH_DIFF") > 1,
            # Gap exists: prepend current, add nulls for gap, then previous
            F.slice(
                F.concat(
                    F.array(
                        F.coalesce(
                            F.col(f"c.{rolling_mapper_column}").cast(
                                type_map[rolling_data_type]
                            ),
                            F.lit(None).cast(type_map[rolling_data_type])
                        )
                    ),
                    F.array_repeat(
                        F.lit(None).cast(type_map[rolling_data_type]),
                        F.col("MONTH_DIFF") - F.lit(1)
                    ),
                    F.coalesce(
                        F.col(f"p.{rolling_col}_history"),
                        F.array(
                            *[
                                F.lit(None).cast(type_map[rolling_data_type])
                                for _ in range(num_cols)
                            ]
                        )
                    )
                ),
                1,
                num_cols
            )
        )
        .otherwise(
            # New account or other case: just current value
            F.array(
                F.coalesce(
                    F.col(f"c.{rolling_mapper_column}").cast(
                        type_map[rolling_data_type]
                    ),
                    F.lit(None).cast(type_map[rolling_data_type])
                ),
                *[
                    F.lit(None).cast(type_map[rolling_data_type])
                    for _ in range(num_cols - 1)
                ]
            )
        )
        .alias(f"{rolling_col}_history")
    )
    
    return subsequent_rolling_expr


def generate_dpd_grid(spark, start_month, end_month, is_initial, config):
    logging.info("Preparing Config")

    source = f"{config['source_table']}"
    destination = f"{config['destination_table']}"
    latest_history_table = f"{config['latest_history_table']}"

    primary_column = config['primary_column']
    partition_column = config['partition_column']
    primary_date_column = config['primary_date_column']
    max_identifier_column = config['max_identifier_column']

    column_mapping = config['columns']
    rolling_column_list = config['rolling_columns']
    grid_column_list = config['grid_columns']
    coalesce_exclusion_cols = config['coalesce_exclusion_cols']
    inferred_cols = config['inferred_columns']
    rolling_mapper_list = [i['mapper_column'] for i in config['rolling_columns']]

    column_transformations = config['column_transformations']

    date_col_list = config['date_col_list']
    latest_history_addon_cols = config['latest_history_addon_cols']

    coalesce_expr = []
    select_expr = []
    for key, value in column_mapping.items():
        if value not in coalesce_exclusion_cols and value not in rolling_mapper_list:
            coalesce_expr.append(F.col(f"c.{value}").alias(f"{value}"))
        select_expr.append(F.col(key).alias(value))

    for inferred_col in inferred_cols:
        coalesce_expr.append(F.col(f"c.{inferred_col['name']}").alias(f"{inferred_col['name']}"))
    
    rolling_column_select_expr = [
        f"{i['name']}_history" for i in rolling_column_list
    ]

    optimization_counter = 0

    month = start_month
    while month <= end_month:
        if optimization_counter == 12:
            optimize_iceberg_table(spark, latest_history_table)
            optimization_counter = 0
        
        month_start_time = time.time()
        next_month = (month.replace(day=28) + relativedelta(days=4)).replace(day=1)
        
        logging.info(f"Processing month: {month} ...")
        month_str = month.strftime('%Y-%m')
        
        if spark.catalog.tableExists(destination):
            max_month = spark.table(destination) \
                .agg(F.max(partition_column).alias("max_month")) \
                .first()["max_month"]
            
            if max_month != None:
                is_initial = False
            
            if max_month != None and max_month > month_str:
                logging.error("Processing Month Less than max entry in Summary")
                exit()

        if spark.catalog.tableExists(latest_history_table):
            max_month = spark.table(latest_history_table) \
                .agg(F.max(partition_column).alias("max_month")) \
                .first()["max_month"]
            
            if max_month == None:
                pass
            elif max_month > month_str:
                logging.error("Processing Month Less than max entry in Summary")
                exit()
            else:
                month_summary = datetime_sub.strptime(max_month, "%Y-%m")
                month_processing = datetime_sub.strptime(month_str, "%Y-%m")
                month_diff = (month_processing.year - month_summary.year) * 12 + (month_processing.month - month_summary.month)
                
                if month_diff > 1:
                    month = month - relativedelta(months=month_diff)
                    next_month = (month.replace(day=28) + relativedelta(days=4)).replace(day=1)
                    month_str = month.strftime('%Y-%m')

        accounts_all_df = spark.read \
                                .table(source) \
                                .select(*select_expr) \
                                .filter(F.col(f"{partition_column}") == f'{month.year:04d}-{month.month:02d}')

        # Deduplication Logic
        window_spec = Window.partitionBy(primary_column) \
                            .orderBy(F.col(max_identifier_column).desc())
        
        accounts_all_df = accounts_all_df.withColumn("rn", F.row_number().over(window_spec)) \
                                        .filter(F.col("rn") == 1) \
                                        .drop("rn")
        
        for i in column_transformations:
            accounts_all_df = accounts_all_df.withColumn(i['name'], F.expr(i['mapper_expr']))
        
        for i in inferred_cols:
            accounts_all_df = accounts_all_df.withColumn(i['name'], F.expr(i['mapper_expr']))
        
        current_count = accounts_all_df.count()
        num_partitions = calculate_optimal_partitions(current_count)
        configure_spark_for_partitions(spark, num_partitions)

        logging.info(f"Current Month Records: {current_count}")

        accounts_all_df = accounts_all_df.repartition(num_partitions, F.col(primary_column))
        accounts_all_df.cache()

        if is_initial:
            result = accounts_all_df
            logging.info("First Month")
            for i in rolling_column_list:
                rolling_col = i['name']
                rolling_mapper_expr = i['mapper_expr']
                rolling_mapper_column = i['mapper_column']
                num_cols = int(i['num_cols'])
                rolling_data_type = i['type']

                result = result.withColumn(rolling_mapper_column, F.expr(rolling_mapper_expr))

                result = result.withColumn(
                    f"{rolling_col}_history", 
                    initial_rolling_column_mapper(
                        rolling_mapper_column, rolling_data_type, num_cols
                    )
                )
            
            for i in grid_column_list:
                grid_col = i['name']
                grid_mapper_column = i['mapper_rolling_column']
                placeholder_value = i['placeholder']
                seperator_value = i['seperator']
                
                result = result.withColumn(
                    grid_col,
                    F.concat_ws(
                        seperator_value,
                        F.transform(
                            f"{grid_mapper_column}_history", 
                            lambda x: F.coalesce(
                                x.cast(StringType()), F.lit(placeholder_value))
                            )
                        )
                )

        else:
            latest_prev = spark.read.table(f"{latest_history_table}").select(
                primary_column,
                primary_date_column,
                partition_column,
                max_identifier_column,
                *rolling_column_select_expr
            )

            latest_prev_count = latest_prev.count()
            num_partitions = calculate_optimal_partitions(latest_prev_count)
            configure_spark_for_partitions(spark, num_partitions)

            logging.info(f"Previous Records: {latest_prev_count}")

            latest_prev = latest_prev.repartition(num_partitions, f"{primary_column}")

            for i in rolling_column_list:
                rolling_mapper_expr = i['mapper_expr']
                rolling_mapper_column = i['mapper_column']

                accounts_all_df = accounts_all_df.withColumn(rolling_mapper_column, F.expr(rolling_mapper_expr))

            joined = accounts_all_df.alias("c").join(
                        latest_prev.alias("p"),
                        primary_column,
                        "left"
                    )
            
            # Calculate month difference for gap handling
            joined = joined.withColumn(
                "MONTH_DIFF",
                F.when(F.col(f"p.{partition_column}").isNotNull(),
                    F.months_between(
                        F.to_date(F.lit(month_str), "yyyy-MM"),
                        F.to_date(F.col(f"p.{partition_column}"), "yyyy-MM")
                    ).cast(IntegerType())
                ).otherwise(1)
            ) 

            rolling_cols_expr = []            
            for i in rolling_column_list:
                rolling_col = i['name']
                rolling_mapper_expr = i['mapper_expr']
                rolling_mapper_column = i['mapper_column']
                num_cols = int(i['num_cols'])
                rolling_data_type = i['type']
                
                if rolling_data_type == "Integer":
                    rolling_cols_expr.append(F.coalesce(F.col(f"c.{rolling_mapper_column}"),F.lit(None).cast(IntegerType())).alias(f"{rolling_mapper_column}"))
                if rolling_data_type == "String":
                    rolling_cols_expr.append(F.coalesce(F.col(f"c.{rolling_mapper_column}"),F.lit(None).cast(StringType())).alias(f"{rolling_mapper_column}"))

                rolling_cols_expr.append(
                    subsequent_rolling_column_mapper(
                        rolling_col, rolling_mapper_column, rolling_data_type, num_cols
                    )
                )

            result = joined.select(
                *coalesce_expr,
                *rolling_cols_expr,
                F.coalesce(F.col(f"c.{primary_date_column}"), F.to_date(F.lit(month_str))).alias(f"{primary_date_column}"),
                F.coalesce(F.col(f"c.{partition_column}"), F.lit(f'{month.year:04d}-{month.month:02d}')).alias(f"{partition_column}")
            )

            for i in grid_column_list:
                grid_col = i['name']
                grid_mapper_column = i['mapper_rolling_column']
                placeholder_value = i['placeholder']
                seperator_value = i['seperator']
                
                result = result.withColumn(
                    grid_col,
                    F.concat_ws(
                        seperator_value, 
                        F.transform(
                            f"{grid_mapper_column}_history", 
                            lambda x: F.coalesce(
                                x.cast(StringType()), F.lit(placeholder_value))
                            )
                        )
                    )
                
        result = replace_invalid_years(result, date_col_list)
        result.cache()

        if result.count() >= 1:
            if is_initial == True:
                result.writeTo(f"{destination}") \
                .append()

                rolling_column_select_expr = [
                    f"{i['name']}_history" for i in rolling_column_list
                ]

                result.select(
                    primary_column,
                    primary_date_column,
                    partition_column,
                    max_identifier_column,
                    *latest_history_addon_cols,
                    *rolling_column_select_expr
                ).writeTo(f"{latest_history_table}") \
                .append()

                is_initial = False
                month_end_time = time.time()
                month_processing_time = (month_end_time - month_start_time)/60
                logging.info(
                    f"Completed for {month}, Processed {result.count()} Records | Execution Time :{month_processing_time:.2f} minutes"
                )
                logging.info("-"*80)
                month = next_month
                continue
            else:
                pass
        else:
            month= next_month

            month_end_time = time.time()
            month_processing_time = (month_end_time - month_start_time)/60
            logging.info(
                f"Completed for {month}, No Records to Process | Execution Time :{month_processing_time:.2f} minutes"
            )
            logging.info("-"*80)
            continue

        write_start_time = time.time()
        logging.info("Writing to Summary")
        result.createOrReplaceTempView("result")
        spark.sql(f"""MERGE INTO {destination} d
                    USING result r
                    ON d.{primary_column} = r.{primary_column} AND d.{partition_column} = r.{partition_column}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *""")
        
        write_end_time = time.time()
        write_total_time = (write_end_time - write_start_time)/60
        logging.info(f"Completed Writing to Summary | Execution Time :{write_total_time:.2f} minutes")
        
        write_start_time = time.time()
        logging.info("Writing to Latest_Summary")
        
        rolling_column_select_expr_str = [
            f"{i['name']}_history" for i in rolling_column_list
        ]
        result = result.select(
            primary_column,primary_date_column,partition_column,max_identifier_column,*latest_history_addon_cols, *rolling_column_select_expr_str 
        ).createOrReplaceTempView("result_latest")

        spark.sql(f"""MERGE INTO {latest_history_table} d
                    USING result_latest r
                    ON d.{primary_column} = r.{primary_column}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *""")
             
        write_end_time = time.time()
        write_total_time = (write_end_time - write_start_time)/60
        logging.info(f"Completed Writing to Latest_Summary | Execution Time :{write_total_time:.2f} minutes")

        month_end_time = time.time()
        month_processing_time = (month_end_time - month_start_time)/60
        logging.info(f"Completed for {month}, Processed {result.count()} Records | Execution Time :{month_processing_time:.2f} minutes")
        logging.info("-"*80)

        accounts_all_df.unpersist()
        result.unpersist()

        optimization_counter += 1
        month = next_month


    optimize_iceberg_table(spark, latest_history_table)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("INFO")

    is_initial = False
    start_month = datetime.date(2024, 4, 1)
    end_month = datetime.date(2025, 12, 31)

    bucket_name = 'in-cs-ivaps-data-bucket-prod-ap-south-1'
    config_key = 'ivaps_prod/persistent/ascend_summary/config/prod/summary.json'
    config = load_config(bucket_name, config_key)

    generate_dpd_grid(spark, start_month, end_month, is_initial, config)
    end_time = time.time()
    total_minutes = (end_time - start_time)/60
    logging.info(f"========================= COMPLETED =========================")
    logging.info(f"Total Execution Time :{total_minutes:.2f} minutes")
    spark.stop()

if __name__ == "__main__":
    main()