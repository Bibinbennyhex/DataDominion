from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, ArrayType, DoubleType, DecimalType, DateType
from pyspark import StorageLevel
import logging
import json
import argparse
import sys
import time
import boto3
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple, Any, Optional
from functools import reduce

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False
start_time = time.time()

TARGET_RECORDS_PER_PARTITION = 500_000_000
MIN_PARTITIONS = 16
MAX_PARTITIONS = 8192
AVG_RECORD_SIZE_BYTES = 200
SNAPSHOT_INTERVAL = 12
MAX_FILE_SIZE = 256
HISTORY_LENGTH = 36


def load_config(bucket, key):
    s3 = boto3.client("s3")
    logger.info(f"Loading configuration from: s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    config = json.loads(obj["Body"].read().decode("utf-8"))
    
    return config


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration"""
    required_fields = ["source_table", "partition_column", "destination_table", "latest_history_table", "primary_column", "primary_date_column", "max_identifier_column", "history_length", "columns", "column_transformations", "inferred_columns", "coalesce_exclusion_cols", "date_col_list", "latest_history_addon_cols", "rolling_columns", "grid_columns", "spark"]
    
    for field in required_fields:
        if field not in config or not config[field]:
            logger.error(f"Required config field '{field}' is missing or empty")
            return False
    
    return True


def create_spark_session(app_name: str, spark_config: Dict[str, Any]):
    spark_builder = SparkSession.builder.appName(app_name)
    for key,value in spark_config.items():
        if key != 'app_name':
            spark_builder = spark_builder.config(key,str(value))

    spark = spark_builder.enableHiveSupport().getOrCreate()

    return spark


def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM to integer"""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def prepare_source_data(df, config: Dict[str, Any]):
    """
    Prepare source data with:
    1. Column mappings (rename source -> destination)
    2. Column transformations (sentinel value handling)
    3. Inferred/derived columns
    4. Date column validation
    5. Deduplication
    """
    logger.info("Preparing source data...")
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    
    result = df
    deduplication_trackers = ['insert_ts','update_ts']
    
    # Step 1: Apply column mappings
    column_mappings = config.get('columns', {})
    if column_mappings:
        select_exprs = []
        for src_col, dst_col in column_mappings.items():
            if src_col in result.columns:
                select_exprs.append(F.col(src_col).alias(dst_col))
            else:
                logger.warning(f"Source column '{src_col}' not found, skipping")
        
        if select_exprs:
            result = result.select(*select_exprs, *deduplication_trackers)
            logger.info(f"Applied {len(column_mappings)} column mappings")
    
    # Step 2: Apply column transformations
    column_transformations = config.get('column_transformations', [])
    for transform in column_transformations:
        col_name = transform['name']
        # Use 'mapper_expr' to match original format
        expr = transform.get('mapper_expr', transform.get('expr', col_name))
        if col_name in result.columns or any(c in expr for c in result.columns):
            result = result.withColumn(col_name, F.expr(expr))
    
    if column_transformations:
        logger.info(f"Applied {len(column_transformations)} column transformations")
    
    # Step 3: Apply inferred/derived columns
    inferred_columns = config.get('inferred_columns', [])
    for inferred in inferred_columns:
        col_name = inferred['name']
        # Use 'mapper_expr' to match original format
        expr = inferred.get('mapper_expr', inferred.get('expr', ''))
        if expr:
            result = result.withColumn(col_name, F.expr(expr))
    
    if inferred_columns:
        logger.info(f"Created {len(inferred_columns)} inferred columns")
    
    # Step 4: Apply rolling column mappers (prepare values for history arrays)
    rolling_columns = config.get('rolling_columns', [])
    for rc in rolling_columns:
        result = result.withColumn(f"{rc['mapper_column']}", F.expr(rc['mapper_expr']))
    
    # Step 5: Validate date columns (replace invalid years with NULL)
    date_columns = config.get('date_col_list', config.get('date_columns', []))
    for date_col in date_columns:
        if date_col in result.columns:
            result = result.withColumn(
                date_col,
                F.when(F.year(F.col(date_col)) < 1000, None)
                 .otherwise(F.col(date_col))
            )
    
    if date_columns:
        logger.info(f"Validated {len(date_columns)} date columns")
    
    # Step 6: Deduplicate by primary key + partition, keeping latest timestamp
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc(), *[F.col(c).desc() for c in deduplication_trackers])
    
    result = (
        result
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn", *deduplication_trackers)
    )
    
    logger.info("Source data preparation complete")
    return result


def load_and_classify_accounts(spark: SparkSession, config: Dict[str, Any]):
    """
    Load accounts and classify into Case I/II/III
    
    Case I:   New accounts (not in summary)
    Case II:  Forward entries (month > max existing)
    Case III: Backfill (month <= max existing)
    Case IV: New account with MULTIPLE months - subsequent months

    Returns: DataFrame with case_type column
    """
    logger.info("=" * 80)
    logger.info("STEP 1: Load and Classify Accounts")
    logger.info("=" * 80)

    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    source_table = config['source_table']
    destination_table = config['destination_table']
    latest_summary_table = config['latest_history_table']

    if spark.catalog.tableExists(destination_table):
        max_base_ts = spark.table(destination_table) \
            .agg(F.max(ts).alias("max_base_ts")) \
            .first()["max_base_ts"]

        max_month_destination = spark.table(destination_table) \
            .agg(F.max(prt).alias("max_month")) \
            .first()["max_month"]

        min_month_destination = spark.table(destination_table) \
            .agg(F.min(prt).alias("min_month")) \
            .first()["min_month"]

        max_month_latest_history = spark.table(latest_summary_table) \
            .agg(F.max(prt).alias("max_month")) \
            .first()["max_month"]

        if max_month_destination != max_month_latest_history:
            raise ValueError(f"max_month_destination : {max_month_destination} and max_month_latest_history : {max_month_latest_history} does not match")                
    else:
        raise ValueError(f"{destination_table} does not exist")

    next_month = (datetime.strptime(max_month_destination, "%Y-%m") + relativedelta(months=1)).strftime("%Y-%m")


    # Load accounts
    logger.info(f"Loading accounts from {source_table}")
    accounts_df = spark.read.table(source_table)\
        .filter(
                (F.col(prt) >= f"{min_month_destination}")
                & (F.col(prt) < f"{next_month}")
                & (F.col(ts) > max_base_ts)
            )
    
    logging.info(f"Reading from {source_table} - ({prt} < {next_month}) & ({ts} > {max_base_ts}")

    # Prepare source data (mappings, transformations, deduplication)
    accounts_prepared = prepare_source_data(accounts_df, config)

    # Add month integer for comparison
    accounts_prepared = accounts_prepared.withColumn(
        "month_int",
        F.expr(month_to_int_expr(prt))
    )

    # Load summary metadata (small - can broadcast)
    logger.info(f"Loading summary metadata from {latest_summary_table}")
    try:
        summary_meta = spark.sql(f"""
            SELECT 
                {pk},
                {prt} as max_existing_month,
                {ts} as max_existing_ts,
                {month_to_int_expr(prt)} as max_month_int
            FROM {latest_summary_table}
        """)

        logger.info(f"Loaded metadata for existing accounts")
    except Exception as e:
        logger.warning(f"Could not load summary metadata: {e}")
        # No existing summary - all are Case I
        return accounts_prepared.withColumn("case_type", F.lit("CASE_I"))

    # Classify records
    logger.info("Classifying accounts into Case I/II/III/IV")

    # Initial classification (before detecting bulk historical)
    initial_classified = (
        accounts_prepared.alias("n")
        .join(
            summary_meta.alias("s"),
            F.col(f"n.{pk}") == F.col(f"s.{pk}"),
            "left"
        )
        .select(
            F.col("n.*"),
            F.col("s.max_existing_month"),
            F.col("s.max_existing_ts"),
            F.col("s.max_month_int")
        )
        .withColumn(
            "initial_case_type",
            F.when(F.col("max_existing_month").isNull(), F.lit("CASE_I"))
            .when(F.col("month_int") > F.col("max_month_int"), F.lit("CASE_II"))
            .otherwise(F.lit("CASE_III"))
        )
    )

    # Detect bulk historical: new accounts with multiple months in batch
    # For Case I accounts, find the earliest month per account
    window_spec = Window.partitionBy(pk)
    classified = (
        initial_classified
        .withColumn(
            "min_month_for_new_account",
            F.when(
                F.col("initial_case_type") == "CASE_I",
                F.min("month_int").over(window_spec)
            )
        )
        .withColumn(
            "count_months_for_new_account",
            F.when(
                F.col("initial_case_type") == "CASE_I",
                F.count("*").over(window_spec)
            ).otherwise(F.lit(1))
        )
        .withColumn(
            "case_type",
            F.when(
                # Case I: New account with ONLY single month in batch
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") == 1),
                F.lit("CASE_I")
            ).when(
                # Case IV: New account with MULTIPLE months - this is the first/earliest month
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") > 1) &
                (F.col("month_int") == F.col("min_month_for_new_account")),
                F.lit("CASE_I")  # First month treated as Case I
            ).when(
                # Case IV: New account with MULTIPLE months - subsequent months
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") > 1) &
                (F.col("month_int") > F.col("min_month_for_new_account")),
                F.lit("CASE_IV")  # Bulk historical - will build on earlier months in batch
            ).otherwise(
                # Keep existing classification for Case II and III
                F.col("initial_case_type")
            )
        )
        .withColumn(
            "MONTH_DIFF",
            F.when(
                F.col("case_type") == "CASE_II",
                F.col("month_int") - F.col("max_month_int")
            ).when(
                # For Case IV, calculate diff from earliest month in batch
                F.col("case_type") == "CASE_IV",
                F.col("month_int") - F.col("min_month_for_new_account")
            ).otherwise(F.lit(1))
        )
        .drop("initial_case_type")
    )

    return classified


def process_case_i(case_i_df, config: Dict[str, Any]):
    """
    Process Case I - create initial arrays for new accounts
    
    Arrays are initialized as: [current_value, NULL, NULL, ..., NULL] (36 elements)
    """
    logger.info("=" * 80)
    logger.info("STEP 2a: Process Case I (New Accounts)")
    logger.info("=" * 80)

    logger.info(f"Processing new accounts")

    result = case_i_df
    history_len = config.get('history_length', HISTORY_LENGTH)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])

    # Create initial arrays for each rolling column
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']

        # Array: [current_value, NULL, NULL, ..., NULL]
        null_array = ", ".join(["NULL" for _ in range(history_len - 1)])
        result = result.withColumn(
            array_name,
            F.expr(f"array({mapper_column}, {null_array})")
        )

    # Generate grid columns
    for gc in grid_columns:
        # Use 'mapper_rolling_column' to match original format
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))

        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )

    # Drop classification and temporary columns
    drop_cols = [
        "month_int",
        "max_existing_month",
        "max_existing_ts",
        "max_month_int",
        "case_type",
        "MONTH_DIFF",
        "min_month_for_new_account",
        "count_months_for_new_account",
    ]
    # Also drop prepared columns
    drop_cols.extend([c for c in result.columns if c.startswith("_prepared_")])
    result = result.drop(*[c for c in drop_cols if c in result.columns])

    return result


def process_case_ii(spark: SparkSession, case_ii_df, config: Dict[str, Any]):
    """
    Process Case II - shift existing arrays for forward entries
    
    Logic:
    - MONTH_DIFF == 1: [new_value, prev[0:35]]
    - MONTH_DIFF > 1:  [new_value, NULLs_for_gap, prev[0:35-gap]]
    """
    process_start_time = time.time()
    logger.info("=" * 80)
    logger.info("STEP 2b: Process Case II (Forward Entries)")
    logger.info("=" * 80)

    logger.info(f"Processing forward entries")

    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    latest_summary_table = config['latest_history_table']
    history_len = config.get('history_length', HISTORY_LENGTH)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])

    # Get affected accounts
    affected_keys = case_ii_df.select(pk).distinct()

    # Load latest summary for affected accounts
    logger.info(f"Loading latest summary for Affected Accounts")

    # Select needed columns from latest summary
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]

    # Create temp view for affected keys
    affected_keys.createOrReplaceTempView("case_ii_affected_keys")

    # Build column list
    cols_select = ", ".join([pk, prt] + history_cols)

    # Get data from latest_summary for the affected_keys
    latest_for_affected_sql = f"""
        SELECT {cols_select}
        FROM {latest_summary_table} s
        WHERE s.{pk} IN (SELECT {pk} FROM case_ii_affected_keys)
    """

    latest_for_affected = spark.sql(latest_for_affected_sql)

    # Collect peer forward entries into MAP for gap filling (Chaining)
    # Map Key: month_int, Map Value: Struct(val_col1, val_col2...)
    val_struct_fields = []
    for rc in rolling_columns:
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in case_ii_df.columns else src_col
        val_struct_fields.append(F.col(value_col).alias(f"val_{rc['name']}"))

    peer_window = Window.partitionBy(pk)
    case_ii_df = case_ii_df.withColumn(
        "peer_map", 
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("month_int"), 
                    F.struct(*val_struct_fields)
                )
            ).over(peer_window)
        )
    )

    # Create temp views for SQL
    case_ii_df.createOrReplaceTempView("case_ii_records")
    latest_for_affected.createOrReplaceTempView("latest_summary_affected")

    # Build SQL for array shifting
    shift_exprs = []
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']
        val_col_name = f"val_{rc['name']}"

        shift_expr = f"""
            CASE
                WHEN c.MONTH_DIFF = 1 THEN
                    slice(concat(array(c.{mapper_column}), p.{array_name}), 1, {history_len})
                WHEN c.MONTH_DIFF > 1 THEN
                    slice(
                        concat(
                            array(c.{mapper_column}),
                            transform(
                                sequence(1, c.MONTH_DIFF - 1),
                                i -> c.peer_map[CAST(c.month_int - i AS INT)].{val_col_name}
                            ),
                            p.{array_name}
                        ),
                        1, {history_len}
                    )
                ELSE
                    concat(array(c.{mapper_column}), array_repeat(NULL, {history_len - 1}))
            END as {array_name}
        """
        shift_exprs.append(shift_expr)

    # Get non-array columns from current record
    exclude_cols = {
        "month_int",
        "max_existing_month",
        "max_existing_ts",
        "max_month_int",
        "case_type",
        "MONTH_DIFF",
        "min_month_for_new_account",
        "count_months_for_new_account",
        "peer_map"
    }
    exclude_cols.update(history_cols)
    exclude_cols.update([c for c in case_ii_df.columns if c.startswith("_prepared_")])
    current_cols = [c for c in case_ii_df.columns if c not in exclude_cols]
    current_select = ", ".join([f"c.{col}" for col in current_cols])

    sql = f"""
        SELECT 
            {current_select},
            {', '.join(shift_exprs)}
        FROM case_ii_records c
        JOIN latest_summary_affected p ON c.{pk} = p.{pk}
    """

    result = spark.sql(sql)

    # Generate grid columns
    for gc in grid_columns:
        # Use 'mapper_rolling_column' to match original format
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))

        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )

    result.coalesce(100).writeTo("temp_catalog.checkpointdb.case_2").create()

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case II Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
    logger.info("-" * 60)

    return


def process_case_iii(spark: SparkSession, case_iii_df, config: Dict[str, Any]):
    """
    Process Case III - rebuild history arrays for backfill
    1. Creates NEW summary rows for backfill months (with inherited history)
    2. Updates ALL future summary rows with backfill data
    
    Logic:
    A. For each backfill record, find the closest PRIOR summary
    B. Create a new summary row for the backfill month with:
       - Position 0: backfill data
       - Position 1-N: shifted data from prior summary (based on month gap)
    C. Update all FUTURE summaries with backfill data at correct position
    """
    process_start_time = time.time()

    logger.info("=" * 80)
    logger.info("STEP 2c: Process Case III (Backfill)")
    logger.info("=" * 80)

    logger.info(f"CASE III - Processing backfill records")

    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = config['destination_table']
    history_len = config.get('history_length', HISTORY_LENGTH)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])

    # Get affected accounts
    affected_accounts = case_iii_df.select(pk).distinct()

    # Get the min/max backfill months to calculate partition range
    backfill_range = case_iii_df.agg(
        F.min(prt).alias("min_backfill_month"),
        F.max(prt).alias("max_backfill_month")
    ).first()

    min_backfill = backfill_range['min_backfill_month']
    max_backfill = backfill_range['max_backfill_month']

    logger.info(f"Backfill month range: {min_backfill} to {max_backfill}")

    # Calculate the partition range we need to read
    # We need prior months (up to 36 months before min backfill) for history inheritance
    # We need future months (up to 36 months after max backfill) for updating
    # Convert to month_int for calculation
    min_backfill_int = int(min_backfill[:4]) * 12 + int(min_backfill[5:7])
    max_backfill_int = int(max_backfill[:4]) * 12 + int(max_backfill[5:7])

    # Calculate boundary months
    earliest_needed_int = min_backfill_int - history_len
    latest_needed_int = max_backfill_int + history_len

    earliest_year = earliest_needed_int // 12
    earliest_month = earliest_needed_int % 12
    if earliest_month == 0:
        earliest_month = 12
        earliest_year -= 1
    earliest_partition = f"{earliest_year}-{earliest_month:02d}"

    latest_year = latest_needed_int // 12
    latest_month = latest_needed_int % 12
    if latest_month == 0:
        latest_month = 12
        latest_year -= 1
    latest_partition = f"{latest_year}-{latest_month:02d}"

    logger.info(f"Reading summary partitions from {earliest_partition} to {latest_partition}")

    # Load history columns list
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]

    # Build select for summary - include all columns we need
    summary_select_cols = [pk, prt, ts] + history_cols 

    # PARTITION-PRUNED READ: Only read the partitions we need with partition filter to leverage Iceberg's partition pruning
    partition_filter_sql = f"""
        SELECT {', '.join(summary_select_cols)}
        FROM {summary_table}
        WHERE {prt} >= '{earliest_partition}' AND {prt} <= '{latest_partition}'
    """
    summary_df = spark.sql(partition_filter_sql)

    logger.info(f"Applied partition filter: {prt} BETWEEN '{earliest_partition}' AND '{latest_partition}'")

    summary_filtered = summary_df.join(
        F.broadcast(affected_accounts),
        pk,
        "left_semi"
    )

    # =========================================================================
    # Pre-calculate peer backfills for gap filling (A+B Combined)
    # =========================================================================
    # Collect all backfill values for the account into a MAP to fill gaps immediately
    # Map Key: month_int, Map Value: Struct(val_col1, val_col2...)

    val_struct_fields = []
    for rc in rolling_columns:
        mapper_column = rc['mapper_column']
        val_struct_fields.append(F.col(mapper_column).alias(f"val_{rc['name']}"))

    peer_window = Window.partitionBy(pk)
    case_iii_with_peers = case_iii_df.withColumn(
        "peer_map", 
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("month_int"), 
                    F.struct(*val_struct_fields)
                )
            ).over(peer_window)
        )
    )

    summary_filtered = summary_filtered.withColumn("month_int",F.expr(month_to_int_expr(prt)))

    # Create temp views
    case_iii_with_peers.createOrReplaceTempView("backfill_records")
    summary_filtered.createOrReplaceTempView("summary_affected")

    # =========================================================================
    # PART A: Create NEW summary rows for backfill months
    # =========================================================================
    logger.info("Part A: Creating new summary rows for backfill months...")

    # Find the closest PRIOR summary for each backfill record
    prior_summary_joined = spark.sql(f"""
        SELECT 
            b.*,
            s.{prt} as prior_month,
            s.month_int as prior_month_int,            
            s.{ts} as prior_ts,
            {', '.join([f's.{arr} as prior_{arr}' for arr in history_cols])},
            (
                b.month_int - s.month_int
            ) as months_since_prior,
            ROW_NUMBER() OVER (
                PARTITION BY b.{pk}, b.{prt} 
                ORDER BY s.month_int DESC
            ) as rn
        FROM backfill_records b
        LEFT JOIN summary_affected s 
            ON b.{pk} = s.{pk} 
            AND s.month_int < b.month_int
    """)

    # Keep only the closest prior summary (rn = 1)
    prior_summary = prior_summary_joined.filter(F.col("rn") == 1).drop("rn")
    prior_summary.createOrReplaceTempView("backfill_with_prior")

    # Build expressions for creating new summary row
    new_row_exprs = []
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']
        val_col_name = f"val_{rc['name']}"

        # Logic:
        # 1. Start with current value
        # 2. Fill Gap: Use transform on sequence to lookup peer_map
        #    Map lookup returns NULL if key not found (for gaps)
        # 3. Append Prior History - AND PATCH IT with peer_map to resolve conflicts

        new_row_expr = f"""
            CASE
                WHEN prior_month IS NOT NULL AND months_since_prior > 0 THEN
                    slice(
                        concat(
                            array({mapper_column}),
                            CASE 
                                WHEN months_since_prior > 1 THEN 
                                    transform(
                                        sequence(1, months_since_prior - 1),
                                        i -> peer_map[CAST(month_int - i AS INT)].{val_col_name}
                                    )
                                ELSE array()
                            END,
                            transform(
                                prior_{array_name},
                                (val, i) -> CASE 
                                    WHEN peer_map[CAST(prior_month_int - i AS INT)] IS NOT NULL
                                    THEN peer_map[CAST(prior_month_int - i AS INT)].{val_col_name}
                                    ELSE val
                                END
                            )
                        ),
                        1, {history_len}
                    )
                ELSE
                    concat(
                        array({mapper_column}),
                        transform(
                            sequence(1, {history_len} - 1),
                            i -> peer_map[CAST(month_int - i AS INT)].{val_col_name}
                        )
                    )
            END as {array_name}
        """
        new_row_exprs.append(new_row_expr)

    # Get columns from backfill record (excluding temp columns)
    exclude_backfill = {
        "month_int",
        "max_existing_month",
        "max_existing_ts",
        "max_month_int",
        "case_type",
        "MONTH_DIFF",
        "prior_month",
        "prior_ts",
        "months_since_prior",
        "min_month_for_new_account",
        "count_months_for_new_account",
    }
    exclude_backfill.update([f"prior_{arr}" for arr in history_cols])
    exclude_backfill.update([c for c in case_iii_df.columns if c.startswith("_prepared_")])

    backfill_cols = [c for c in case_iii_df.columns if c not in exclude_backfill]
    backfill_select = ", ".join(backfill_cols)

    new_rows_sql = f"""
        SELECT 
            {backfill_select},
            {', '.join(new_row_exprs)}
        FROM backfill_with_prior
    """

    new_backfill_rows = spark.sql(new_rows_sql)

    # Generate grid columns for new rows
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))

        new_backfill_rows = new_backfill_rows.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )

    # Deduplicate new backfill rows - keep only one row per account+month
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
    new_backfill_rows = (
        new_backfill_rows
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    new_backfill_rows.coalesce(100).writeTo("temp_catalog.checkpointdb.case_3a").create()

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case III Part A Generated | Time Elapsed: {process_total_minutes:.2f} minutes")    
    logger.info("-" * 60)


    # =========================================================================
    # PART B: Update FUTURE summary rows with backfill data
    # =========================================================================
    logger.info("Part B: Updating future summary rows...")
    process_start_time = time.time()

    # Build value column references for backfill
    backfill_value_cols = []
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']
        backfill_value_cols.append(f'b.{mapper_column} as backfill_{rc["name"]}')

    # Join backfill with future summaries
    future_joined = spark.sql(f"""
        SELECT 
            b.{pk},
            b.{ts} as backfill_ts,
            {', '.join(backfill_value_cols)},
            s.{prt} as summary_month,
            s.{ts} as summary_ts,
            {', '.join([f's.{arr} as existing_{arr}' for arr in history_cols])},
            (
                s.month_int - b.month_int
            ) as backfill_position
        FROM backfill_records b
        JOIN summary_affected s ON b.{pk} = s.{pk}
        WHERE (s.month_int >= b.month_int)
          AND (s.month_int - b.month_int) < {history_len}
    """)


    backfill_updates_df = None

    if not future_joined.isEmpty():
        future_joined.createOrReplaceTempView("future_backfill_joined")

        # =====================================================================
        # MULTIPLE BACKFILL
        # When multiple backfills arrive for the same account in the same batch,
        # we need to MERGE all their updates into the future summary rows.
        #
        # Strategy:
        # 1. Collect all (position, value) pairs for each account+month+array
        # 2. Create a map from positions to values
        # 3. Apply all updates in one pass using transform()
        # =====================================================================

        # First, collect backfill positions and values per account+month
        # We need to aggregate: for each (pk, summary_month), collect all backfills
        backfill_collect_cols = [f"backfill_{rc['name']}" for rc in rolling_columns]
        existing_array_cols = [f"existing_{rc['name']}_history" for rc in rolling_columns]

        # Create a struct with position and all backfill values, then collect as array
        struct_fields = ["backfill_position"] + backfill_collect_cols

        # For each account+month, collect all backfill structs and the existing arrays
        # We use first() for existing arrays since they're the same for all rows of same account+month
        agg_exprs = [
            F.collect_list(F.struct(*[F.col(c) for c in struct_fields])).alias("backfill_list")
        ]
        for rc in rolling_columns:
            array_name = f"{rc['name']}_history"
            agg_exprs.append(F.first(f"existing_{array_name}").alias(f"existing_{array_name}"))

        aggregated_df = future_joined.groupBy(pk, "summary_month").agg(*agg_exprs)
        aggregated_df.createOrReplaceTempView("aggregated_backfills")

        # Build array update expressions that apply ALL collected backfills
        # For each position in the array, check if any backfill targets that position
        update_exprs = []
        for rc in rolling_columns:
            array_name = f"{rc['name']}_history"
            backfill_col = f"backfill_{rc['name']}"

            # Use filter to find matching backfill for position i
            # If match found, use its value (even if NULL). If no match, keep existing x.
            update_expr = f"""
                transform(
                    existing_{array_name},
                    (x, i) -> CASE 
                        WHEN size(filter(backfill_list, b -> b.backfill_position = i)) > 0
                        THEN element_at(filter(backfill_list, b -> b.backfill_position = i), 1).{backfill_col}
                        ELSE x
                    END
                ) as {array_name}
            """
            update_exprs.append(update_expr)

        # Create DataFrame for backfill updates with all backfills merged
        backfill_updates_df = spark.sql(f"""
            SELECT 
                {pk},
                summary_month as {prt},
                {', '.join(update_exprs)}
            FROM aggregated_backfills
        """)


    # Generate grid columns for backfill_updates_df (for direct UPDATE)
    if backfill_updates_df is not None:
        for gc in grid_columns:
            source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
            source_history = f"{source_rolling}_history"
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('seperator', gc.get('separator', ''))

            backfill_updates_df = backfill_updates_df.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.col(source_history),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )

    backfill_updates_df.coalesce(100).writeTo("temp_catalog.checkpointdb.case_3b").create()    

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case III Part B Generated | Time Elapsed: {process_total_minutes:.2f} minutes")    
    logger.info("-" * 60)

    return


def process_case_iv(spark: SparkSession, case_iv_df, case_i_result, config: Dict[str, Any]):
    """
    Process Case IV - Bulk historical load for new accounts with multiple months
        
    Logic:
    - For new accounts with multiple months uploaded at once
    - Build rolling history arrays using window functions
    - Each month's array contains up to 36 months of prior data from the batch
    
    Args:
        spark: SparkSession
        case_iv_df: DataFrame with Case IV records (subsequent months for new accounts)
        case_i_result: DataFrame with Case I results (first month for each new account)
        config: Config dict
    
    Returns:
        None
    """
    logger.info("=" * 80)
    logger.info("STEP 2d: Process Case IV (Bulk Historical Load)")
    logger.info("=" * 80)
    process_start_time = time.time()
   
    logger.info(f"Processing bulk historical records")
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    history_len = config.get('history_length', HISTORY_LENGTH)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    
    # Combine Case I results with Case IV records to build complete history
    # Case I records are the "first month" for each new account
    # Case IV records are "subsequent months" that need rolling history
    
    # Get unique accounts in Case IV
    case_iv_accounts = case_iv_df.select(pk).distinct()
      
    case_iv_df.createOrReplaceTempView("case_iv_records")
    
    # Get all months for bulk historical accounts (including the first month)
    # We need to include Case I records to build complete history
    logger.info("Building complete history using window functions")
    
    all_new_account_months = spark.sql(f"""
        SELECT * FROM case_iv_records
    """)
    
    # Add Case I first months if available
    if case_i_result is not None and case_i_result.count() > 0:
        # Get Case I records that belong to accounts with Case IV records
        case_i_for_iv = case_i_result.join(case_iv_accounts, pk, "inner")
        
        # We need to ensure the columns match
        common_cols = list(set(case_iv_df.columns) & set(case_i_for_iv.columns))
        if common_cols:
            # Add month_int if not present in case_i_result
            if "month_int" not in case_i_for_iv.columns:
                case_i_for_iv = case_i_for_iv.withColumn(
                    "month_int",
                    F.expr(month_to_int_expr(prt))
                )
            
            # Select matching columns and union
            case_iv_cols = case_iv_df.columns
            
            # Add missing columns to case_i_for_iv with NULL values
            for col in case_iv_cols:
                if col not in case_i_for_iv.columns:
                    case_i_for_iv = case_i_for_iv.withColumn(col, F.lit(None))
            
            case_i_for_iv = case_i_for_iv.select(*case_iv_cols)
            all_new_account_months = case_iv_df.unionByName(case_i_for_iv, allowMissingColumns=True)
    
    all_new_account_months.createOrReplaceTempView("all_bulk_records")
    
    # =========================================================================
    #  GAP HANDLING
    # =========================================================================
    #  MAP_FROM_ENTRIES + TRANSFORM to look up values by month_int
    #   - Create MAP of month_int -> value for each account
    #   - For each row, generate positions 0-35
    #   - Look up value for (current_month_int - position)
    #   - Missing months return NULL automatically
    # =========================================================================
    
    # Step 1: Build MAP for each column by account
    # MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, value)))
    map_build_exprs = []
    for rc in rolling_columns:
        col_name = rc['name']
        mapper_column = rc['mapper_column']             
 
        map_build_exprs.append(f"""
            MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, {mapper_column}))) as value_map_{col_name}
        """)
    
    # Create account-level maps
    map_sql = f"""
        SELECT 
            {pk},
            {', '.join(map_build_exprs)}
        FROM all_bulk_records
        GROUP BY {pk}
    """
    
    account_maps = spark.sql(map_sql)
    account_maps.createOrReplaceTempView("account_value_maps")
    
    # Step 2: Join maps back to records and build arrays using TRANSFORM
    # For each position 0-35, look up value from map using (month_int - position)
    
    array_build_exprs = []
    for rc in rolling_columns:
        col_name = rc['name']
        # Use 'type' to match original format (instead of 'data_type')
        data_type = rc.get('type', rc.get('data_type', 'String'))
        array_name = f"{col_name}_history"
        
        # TRANSFORM generates array by looking up each position
        # Position 0 = current month, Position N = N months ago
        # If month doesn't exist in map, returns NULL (correct for gaps!)
        array_build_exprs.append(f"""
            TRANSFORM(
                SEQUENCE(0, {history_len - 1}),
                pos -> CAST(m.value_map_{col_name}[r.month_int - pos] AS {data_type.upper()})
            ) as {array_name}
        """)
    
    # Get base columns from the records table
    exclude_cols = set(["month_int", "max_existing_month", "max_existing_ts", "max_month_int",
                        "case_type", "MONTH_DIFF", "min_month_for_new_account", 
                        "count_months_for_new_account"])
    exclude_cols.update([c for c in all_new_account_months.columns if c.startswith("_prepared_")])
    
    base_cols = [f"r.{c}" for c in all_new_account_months.columns if c not in exclude_cols]
    
    # Build final SQL joining records with maps
    final_sql = f"""
        SELECT 
            {', '.join(base_cols)},
            {', '.join(array_build_exprs)}
        FROM all_bulk_records r
        JOIN account_value_maps m ON r.{pk} = m.{pk}
    """
    
    result = spark.sql(final_sql)
    
    # Generate grid columns
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        
        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )
    
    # Filter to only Case IV records (exclude the first month which is already processed as Case I)
    # Join back with original case_iv_df to get only the subsequent months
    result = result.join(
        case_iv_df.select(pk, prt).distinct(),
        [pk, prt],
        "inner"
    )
    
    result.coalesce(100).writeTo("temp_catalog.checkpointdb.case_4").create()

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case IV Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
    logger.info("-" * 60)
    return


def write_backfill_results(spark: SparkSession, config: Dict[str, Any]):
    summary_table = config['destination_table']
    latest_summary_table = config['latest_history_table']
    pk = config['primary_column']
    prt = config['partition_column']

    try:
        logger.info("-" * 60)
        logger.info("MERGING RECORDS:")
        process_start_time = time.time()

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3a"):
            case_3a_df = spark.read.table('temp_catalog.checkpointdb.case_3a').coalesce(200)
            case_3a_df.createOrReplaceTempView("case_3a")

            spark.sql(
                f"""           
                    MERGE INTO {summary_table} s
                    USING case_3a c
                    ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """
            )
        
        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"MERGED - CASE III-A (NEW summary rows for backfill months with inherited history)| Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)

        process_start_time = time.time()

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3b"):
            case_3a_df = spark.read.table('temp_catalog.checkpointdb.case_3a').select(pk,prt)
            case_3b_df = spark.read.table('temp_catalog.checkpointdb.case_3b').coalesce(200)
            case_3b_filtered_df = case_3b_df.join(case_3a_df,[pk, prt],"left_anti")
            case_3b_filtered_df.createOrReplaceTempView("case_3b")

            spark.sql(
                f"""           
                    MERGE INTO {summary_table} s
                    USING case_3b c
                    ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                    WHEN MATCHED THEN UPDATE SET *
                """
            )    

        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Updated Summary | MERGED - CASE III-B (future summary rows with backfill data) | Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)

        logger.info("APPENDING RECORDS:")
        process_start_time = time.time()

        append_tables = ["temp_catalog.checkpointdb.case_1","temp_catalog.checkpointdb.case_4"]

        dfs = []
        for t in append_tables:
            if spark.catalog.tableExists(t):
                dfs.append(spark.read.table(t))

        append_df = reduce(lambda a, b: a.unionByName(b), dfs)

        append_df.repartition(10).writeTo(summary_table).append()

        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Updated Summary | APPENDED - CASE I & IV (New Records + Bulk Historical) | Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)

        logger.info("UPDATING LATEST SUMMARY:")
        process_start_time = time.time()
        # Get Latest from append_df - case I & IV for appending to latest_summary
        window_spec = Window.partitionBy(pk).orderBy(F.col(prt).desc())
        latest_append_df = (
            append_df
            .withColumn("_rn", F.row_number().over(window_spec))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        latest_append_df.writeTo(latest_summary_table).append()
        logger.info(f"Updated latest_summary | APPENDED - CASE I & IV (New Records + Bulk Historical) | Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)


    except Exception as e:
        logger.error(f"BACKFILL MERGE FAILED: {e}", exc_info=True)
        raise


def write_forward_results(spark: SparkSession, config: Dict[str, Any]):
    summary_table = config['destination_table']
    latest_summary_table = config['latest_history_table']
    pk = config['primary_column']
    prt = config['partition_column']
        
    try:
        logger.info("-" * 60)
        logger.info("MERGING FORWARD RECORDS:")
        process_start_time = time.time()

        if spark.catalog.tableExists('temp_catalog.checkpointdb.case_2'):
            case_2_df = spark.read.table('temp_catalog.checkpointdb.case_2')
            case_2_df.repartition(10).writeTo(summary_table).append()

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | APPENDED - CASE II (Forward Records) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)


            process_start_time = time.time()
            # Get Latest from case 2 for merging to latest_summary
            window_spec = Window.partitionBy(pk).orderBy(F.col(prt).desc())
            latest_case_2_df = (
                case_2_df
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            latest_case_2_df.createOrReplaceTempView("case_2")

            spark.sql(
                f"""           
                    MERGE INTO {latest_summary_table} s
                    USING case_2 c
                    ON s.{pk} = c.{pk} AND s.{prt} < c.{prt}
                    WHEN MATCHED THEN UPDATE SET *
                """
            )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated latest_summary | MERGE - CASE II (Forward Records) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)
        
        else:
            logger.info(f"No forward records to update")
            logger.info("-" * 60)

    except Exception as e:
        logger.error(f"FORWARD MERGE FAILED: {e}", exc_info=True)
        raise


def run_pipeline(spark: SparkSession, config: Dict[str, Any]):
    """  
    Pipeline Processing Order (CRITICAL for correctness):
    1. Backfill FIRST (rebuilds history with corrections)
    2. New accounts SECOND (no dependencies)
    3. Bulk Historical THIRD (builds arrays for new multi-month accounts)
    4. Forward LAST (uses corrected history from backfill)
    
    Args:
        spark: SparkSession
        config: Config dict
        filter_expr: Optional SQL filter for accounts table
    
    Returns:
        None
    """

    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE - START")
    logger.info("=" * 80)

    stats = {
        'total_records': 0,
        'case_i_records': 0,
        'case_ii_records': 0,
        'case_iii_records': 0,
        'case_iv_records': 0,
        'records_written': 0,
        'temp_table': None
    }

    try:
        process_start_time = time.time()    

        # Step 1: Load and classify
        classified = load_and_classify_accounts(spark, config)
        classified.persist(StorageLevel.DISK_ONLY)

        case_stats = classified.groupBy("case_type").count().collect()
        logger.info("-" * 60)
        logger.info("CLASSIFICATION RESULTS:")
        for row in case_stats:
            logger.info(f"  {row['case_type']}: {row['count']:,} records")
        logger.info("-" * 60)

        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Classification | Time Elapsed: {process_total_minutes:.2f} minutes")

        case_dict = {row["case_type"]: row["count"] for row in case_stats}
        stats['case_i_records'] = case_dict.get('CASE_I', 0)
        stats['case_ii_records'] = case_dict.get('CASE_II', 0)
        stats['case_iii_records'] = case_dict.get('CASE_III', 0)
        stats['case_iv_records'] = case_dict.get('CASE_IV', 0)
        stats['total_records'] = sum(case_dict.values())

        # =========================================================================
        # STEP 2: PROCESS EACH CASE AND WRITE TO TEMP TABLE
        # Process in CORRECT order: Backfill -> New -> Bulk Historical -> Forward
        # =========================================================================

        # 2a. Process Case III (Backfill) - HIGHEST PRIORITY
        case_iii_df = classified.filter(F.col("case_type") == "CASE_III")
        if stats['case_iii_records'] > 0:
            logger.info(f"\n>>> PROCESSING BACKFILL ({stats['case_iii_records']:,} records)")
            process_case_iii(spark, case_iii_df, config)

        # 2b. Process Case I (New Accounts - first month only)
        case_i_df = classified.filter(F.col("case_type") == "CASE_I")
        if stats['case_i_records'] > 0:
            process_start_time = time.time()
            logger.info(f"\n>>> PROCESSING NEW ACCOUNTS ({stats['case_i_records']:,} records)")
            case_i_result = process_case_i(case_i_df, config)
            case_i_result.persist(StorageLevel.MEMORY_AND_DISK)
            count = case_i_result.count()
            case_i_result.coalesce(100).writeTo("temp_catalog.checkpointdb.case_1").create()

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Case I Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        # 2c. Process Case IV (Bulk Historical) - after Case I so we have first months
        case_iv_df = classified.filter(F.col("case_type") == "CASE_IV")
        if stats['case_iv_records'] > 0:
            logger.info(f"\n>>> PROCESSING BULK HISTORICAL ({stats['case_iv_records']:,} records)")
            process_case_iv(spark, case_iv_df, case_i_result, config)
            case_i_result.unpersist()

        write_backfill_results(spark, config)

        # 2d. Process Case II (Forward Entries) - LOWEST PRIORITY
        case_ii_df = classified.filter(F.col("case_type") == "CASE_II")
        if stats['case_ii_records'] > 0:
            logger.info(f"\n>>> PROCESSING FORWARD ENTRIES ({stats['case_ii_records']:,} records)")
            process_case_ii(spark, case_ii_df, config)
            write_forward_results(spark, config)

        logger.info("PROCESS COMPLETED - Deleting the persisted classification results")
        classified.unpersist()

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


def cleanup(spark: SparkSession):
    cleanup_tables = ['case_1','case_2','case_3a','case_3b','case_4']

    for table in cleanup_tables:
        spark.sql(f"DROP TABLE IF EXISTS temp_catalog.checkpointdb.{table}")
        logger.info(f"Cleaned {table}")

    logger.info("CLEANUP COMPLETED")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Summary Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--config', type=json.loads, required=True, help='{"bucket_name":{bucket_name},"key":{key}}Path to pipeline config JSON')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental',
                       help='Processing mode')
    
    args = parser.parse_args()
    
    # Load config from s3
    config = load_config(args.config['bucket_name'], args.config['key'])
    if not validate_config(config):
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session(config['spark']['app_name'], config['spark'])
    spark.sparkContext.setLogLevel("WARN")
    
    try: 
        # Cleanup of existing temp_catalog      
        cleanup(spark)
        run_pipeline(spark, config)
        logger.info(f"Pipeline completed")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

    finally:

        end_time = time.time()
        total_minutes = (end_time-start_time)/60
        logger.info("COMPLETED".center(80,"="))    
        logging.info(f"Total Execution Time :{total_minutes:.2f} minutes")
        spark.stop()


if __name__ == "__main__":
    main()

