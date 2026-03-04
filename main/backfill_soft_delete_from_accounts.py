import logging
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# Fixed execution settings (no runtime args / no config-file wiring).
SOURCE_TABLE = "spark_catalog.edf_gold.ivaps_consumer_accounts_all"
SUMMARY_TABLE = "primary_catalog.edf_gold.summary"
LATEST_SUMMARY_TABLE = "primary_catalog.edf_gold.latest_summary"
PK = "cons_acct_key"
PRT = "rpt_as_of_mo"
TS = "base_ts"
SOFT_DELETE_COLUMN = "soft_del_cd"
DELETE_CODES = ["1", "4"]
HISTORY_LENGTH = 36
CASE_TEMP_BUCKET_COUNT = 64
CASE_3D_MONTH_TABLE = "temp_catalog.checkpointdb.case_3d_month"
CASE_3D_FUTURE_TABLE = "temp_catalog.checkpointdb.case_3d_future"
MONTH_CHUNK_OVERFLOW = 0.10
MIN_WRITE_PARTITIONS = 16
MAX_WRITE_PARTITIONS = 8192
DEFAULT_WRITE_PARTITIONS = 200
GRID_SPECS = [
    {
        "name": "payment_history_grid",
        "source_history": "payment_rating_cd_history",
        "placeholder": "?",
        "separator": "",
    }
]


def _normalize_soft_delete_col(df):
    if SOFT_DELETE_COLUMN not in df.columns:
        df = df.withColumn(SOFT_DELETE_COLUMN, F.lit("0"))
    return df.withColumn(
        SOFT_DELETE_COLUMN,
        F.when(
            F.trim(F.coalesce(F.col(SOFT_DELETE_COLUMN).cast(StringType()), F.lit(""))) == "",
            F.lit("0"),
        ).otherwise(F.trim(F.col(SOFT_DELETE_COLUMN).cast(StringType()))),
    )


def _month_int_to_str(month_int: int) -> str:
    year = month_int // 12
    month = month_int % 12
    if month == 0:
        month = 12
        year -= 1
    return f"{year}-{month:02d}"


def _month_to_int_expr(col_name: str) -> str:
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def _get_write_partitions(spark: SparkSession, expected_rows=None) -> int:
    configured = spark.conf.get("spark.sql.shuffle.partitions", str(DEFAULT_WRITE_PARTITIONS))
    try:
        base = int(configured)
    except Exception:
        base = DEFAULT_WRITE_PARTITIONS
    base = max(base, spark.sparkContext.defaultParallelism)
    if expected_rows is None:
        return max(MIN_WRITE_PARTITIONS, min(MAX_WRITE_PARTITIONS, base))
    # 2M rows/partition as a generic write sizing baseline.
    row_based = max(1, int((int(expected_rows) + 1_999_999) / 2_000_000))
    return max(MIN_WRITE_PARTITIONS, min(MAX_WRITE_PARTITIONS, max(base, row_based)))


def _align_for_merge(spark: SparkSession, df, expected_rows=None):
    parts = _get_write_partitions(spark, expected_rows=expected_rows)
    return df.repartition(parts, F.col(PRT), F.col(PK)).sortWithinPartitions(PRT, PK)


def _write_case_table_bucketed(spark: SparkSession, df, table_name: str, stage: str, expected_rows=None):
    if PK not in df.columns or PRT not in df.columns:
        raise ValueError(f"{table_name} write requires {PK} and {PRT}")

    aligned_df = _align_for_merge(spark, df, expected_rows=expected_rows)
    temp_view = f"_tmp_{table_name.replace('.', '_')}_{int(time.time() * 1000)}"
    namespace = table_name.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    aligned_df.createOrReplaceTempView(temp_view)
    spark.sql(
        f"""
            CREATE TABLE {table_name}
            USING iceberg
            PARTITIONED BY ({PRT}, bucket({CASE_TEMP_BUCKET_COUNT}, {PK}))
            TBLPROPERTIES (
                'write.distribution-mode'='hash',
                'write.merge.distribution-mode'='hash'
            )
            AS SELECT * FROM {temp_view}
        """
    )
    spark.catalog.dropTempView(temp_view)
    logger.info(f"Wrote {stage}: {table_name}")


def _build_month_chunks(df, overflow_ratio=0.10):
    month_weights = [
        (r[PRT], float(r["count"]))
        for r in df.groupBy(PRT).count().collect()
        if r[PRT] is not None and r["count"] is not None and r["count"] > 0
    ]
    if not month_weights:
        return []

    month_weights = sorted(month_weights, key=lambda x: x[1], reverse=True)
    target = month_weights[0][1]
    allowed = target * (1.0 + max(0.0, overflow_ratio))

    bins = []
    for month, weight in month_weights:
        if not bins:
            bins.append({"load": weight, "months": [month]})
            continue
        lightest_idx = min(range(len(bins)), key=lambda i: bins[i]["load"])
        if bins[lightest_idx]["load"] + weight <= allowed:
            bins[lightest_idx]["load"] += weight
            bins[lightest_idx]["months"].append(month)
        else:
            bins.append({"load": weight, "months": [month]})
    return [sorted(b["months"]) for b in bins]


def _prepare_case_tables(spark: SparkSession):
    # Avoid stale temp tables from prior runs.
    spark.sql(f"DROP TABLE IF EXISTS {CASE_3D_MONTH_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {CASE_3D_FUTURE_TABLE}")

    # Ensure target schema has soft delete column.
    summary_cols = spark.table(SUMMARY_TABLE).columns
    if SOFT_DELETE_COLUMN not in summary_cols:
        logger.info(f"Adding {SOFT_DELETE_COLUMN} to {SUMMARY_TABLE}")
        spark.sql(f"ALTER TABLE {SUMMARY_TABLE} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")
        summary_cols = spark.table(SUMMARY_TABLE).columns

    if spark.catalog.tableExists(LATEST_SUMMARY_TABLE):
        latest_cols = spark.table(LATEST_SUMMARY_TABLE).columns
        if SOFT_DELETE_COLUMN not in latest_cols:
            logger.info(f"Adding {SOFT_DELETE_COLUMN} to {LATEST_SUMMARY_TABLE}")
            spark.sql(f"ALTER TABLE {LATEST_SUMMARY_TABLE} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")

    history_cols = sorted([c for c in summary_cols if c.endswith("_history")])
    grid_specs = [g for g in GRID_SPECS if g["name"] in summary_cols and g["source_history"] in history_cols]

    src = spark.table(SOURCE_TABLE)
    if "insert_ts" not in src.columns:
        src = src.withColumn("insert_ts", F.col(TS))
    if "update_ts" not in src.columns:
        src = src.withColumn("update_ts", F.col(TS))
    if SOFT_DELETE_COLUMN not in src.columns:
        src = src.withColumn(SOFT_DELETE_COLUMN, F.lit("0"))

    src = _normalize_soft_delete_col(
        src.select(PK, PRT, TS, "insert_ts", "update_ts", SOFT_DELETE_COLUMN)
    )

    latest_window = Window.partitionBy(PK, PRT).orderBy(
        F.col(TS).desc(),
        F.col("insert_ts").desc(),
        F.col("update_ts").desc(),
    )
    deleted_latest = (
        src.withColumn("_rn", F.row_number().over(latest_window))
        .filter((F.col("_rn") == 1) & F.col(SOFT_DELETE_COLUMN).isin(*DELETE_CODES))
        .drop("_rn", "insert_ts", "update_ts")
    )

    delete_count = deleted_latest.count()
    logger.info(f"Latest deleted source rows: {delete_count:,}")
    if delete_count == 0:
        return False, history_cols, grid_specs

    delete_range = deleted_latest.agg(F.min(PRT).alias("min_mo"), F.max(PRT).alias("max_mo")).first()
    min_delete_mo = delete_range["min_mo"]
    max_delete_mo = delete_range["max_mo"]
    logger.info(f"Delete month range: {min_delete_mo} to {max_delete_mo}")

    summary_keys = spark.sql(
        f"""
            SELECT {PK}, {PRT}
            FROM {SUMMARY_TABLE}
            WHERE {PRT} >= '{min_delete_mo}' AND {PRT} <= '{max_delete_mo}'
        """
    )
    delete_existing = deleted_latest.join(summary_keys, [PK, PRT], "inner")
    delete_existing_count = delete_existing.count()
    logger.info(f"Delete rows matching existing summary month rows: {delete_existing_count:,}")
    if delete_existing_count == 0:
        return False, history_cols, grid_specs

    # Part A: month row update table (only soft_del_cd + base_ts).
    case_3d_month_df = delete_existing.select(PK, PRT, TS, SOFT_DELETE_COLUMN)
    _write_case_table_bucketed(
        spark=spark,
        df=case_3d_month_df,
        table_name=CASE_3D_MONTH_TABLE,
        stage="case_3d_month_temp",
        expected_rows=delete_existing_count,
    )
    logger.info("Generated temp table: case_3d_month")

    # Part B: future month array nullification table.
    min_delete_int = int(min_delete_mo[:4]) * 12 + int(min_delete_mo[5:7])
    max_delete_int = int(max_delete_mo[:4]) * 12 + int(max_delete_mo[5:7])
    earliest_partition = _month_int_to_str(min_delete_int - HISTORY_LENGTH)
    latest_partition = _month_int_to_str(max_delete_int + HISTORY_LENGTH)

    affected_accounts = delete_existing.select(PK).distinct()
    summary_filtered = (
        spark.sql(
            f"""
                SELECT *
                FROM {SUMMARY_TABLE}
                WHERE {PRT} >= '{earliest_partition}' AND {PRT} <= '{latest_partition}'
            """
        )
        .join(affected_accounts, PK, "left_semi")
        .withColumn("month_int", F.expr(_month_to_int_expr(PRT)))
    )

    delete_keys = (
        delete_existing
        .withColumn("delete_month_int", F.expr(_month_to_int_expr(PRT)))
        .select(
            F.col(PK),
            F.col(PRT).alias("delete_month"),
            F.col("delete_month_int"),
            F.col(TS).alias("delete_ts"),
        )
    )

    summary_future = summary_filtered.select(PK, PRT, TS, "month_int", *history_cols)

    joined = (
        delete_keys.alias("d")
        .join(
            summary_future.alias("s"),
            (F.col(f"s.{PK}") == F.col(f"d.{PK}"))
            & (F.col("s.month_int") > F.col("d.delete_month_int"))
            & ((F.col("s.month_int") - F.col("d.delete_month_int")) < F.lit(HISTORY_LENGTH)),
            "inner",
        )
        .select(
            F.col(f"d.{PK}").alias(PK),
            F.col("d.delete_ts").alias("delete_ts"),
            F.col(f"s.{PRT}").alias("summary_month"),
            F.col(f"s.{TS}").alias("summary_ts"),
            (F.col("s.month_int") - F.col("d.delete_month_int")).alias("delete_position"),
            *[F.col(f"s.{c}").alias(f"existing_{c}") for c in history_cols],
        )
    )

    if joined.isEmpty():
        logger.info("No future month patches required")
        return True, history_cols, grid_specs

    agg_exprs = [
        F.collect_set("delete_position").alias("delete_positions"),
        F.max("delete_ts").alias("new_base_ts"),
        F.first("summary_ts").alias("existing_summary_ts"),
    ]
    for c in history_cols:
        agg_exprs.append(F.first(f"existing_{c}").alias(f"existing_{c}"))

    aggregated = joined.groupBy(PK, "summary_month").agg(*agg_exprs)

    update_exprs = [
        f"""
            transform(
                existing_{c},
                (x, i) -> CASE WHEN array_contains(delete_positions, i) THEN NULL ELSE x END
            ) AS {c}
        """
        for c in history_cols
    ]
    case_3d_future_df = aggregated.selectExpr(
        PK,
        f"summary_month AS {PRT}",
        f"GREATEST(existing_summary_ts, new_base_ts) AS {TS}",
        *update_exprs,
    )

    for grid in grid_specs:
        case_3d_future_df = case_3d_future_df.withColumn(
            grid["name"],
            F.concat_ws(
                grid["separator"],
                F.transform(
                    F.col(grid["source_history"]),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(grid["placeholder"])),
                ),
            ),
        )

    future_count = case_3d_future_df.count()
    _write_case_table_bucketed(
        spark=spark,
        df=case_3d_future_df,
        table_name=CASE_3D_FUTURE_TABLE,
        stage="case_3d_future_temp",
        expected_rows=future_count,
    )
    logger.info(f"Generated temp table: case_3d_future ({future_count:,} rows)")
    return True, history_cols, grid_specs


def _merge_case_tables_chunked(spark: SparkSession, history_cols, grid_specs):
    case_3d_month_df = None
    case_3d_future_df = None

    if spark.catalog.tableExists(CASE_3D_MONTH_TABLE):
        case_3d_month_df = spark.read.table(CASE_3D_MONTH_TABLE)
        month_chunks = _build_month_chunks(case_3d_month_df, overflow_ratio=MONTH_CHUNK_OVERFLOW)
        logger.info(f"Case III Soft Delete Month merge chunks: {len(month_chunks)}")
        for idx, months in enumerate(month_chunks, 1):
            chunk_df = case_3d_month_df.filter(F.col(PRT).isin(months))
            chunk_df = _align_for_merge(spark, chunk_df)
            chunk_df.createOrReplaceTempView("case_3d_month_chunk")
            spark.sql(
                f"""
                    MERGE INTO {SUMMARY_TABLE} s
                    USING case_3d_month_chunk c
                    ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                    WHEN MATCHED THEN UPDATE SET
                        s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN},
                        s.{TS} = GREATEST(s.{TS}, c.{TS})
                """
            )
            logger.info(
                f"MERGED - CASE III Soft Delete Month chunk {idx}/{len(month_chunks)} (months={months})"
            )

    if spark.catalog.tableExists(CASE_3D_FUTURE_TABLE):
        case_3d_future_df = spark.read.table(CASE_3D_FUTURE_TABLE)
        future_chunks = _build_month_chunks(case_3d_future_df, overflow_ratio=MONTH_CHUNK_OVERFLOW)
        logger.info(f"Case III Soft Delete Future merge chunks: {len(future_chunks)}")

        set_expr = [f"s.{TS} = GREATEST(s.{TS}, c.{TS})"]
        set_expr += [f"s.{c} = c.{c}" for c in history_cols]
        set_expr += [f"s.{g['name']} = c.{g['name']}" for g in grid_specs]

        for idx, months in enumerate(future_chunks, 1):
            chunk_df = case_3d_future_df.filter(F.col(PRT).isin(months))
            chunk_df = _align_for_merge(spark, chunk_df)
            chunk_df.createOrReplaceTempView("case_3d_future_chunk")
            spark.sql(
                f"""
                    MERGE INTO {SUMMARY_TABLE} s
                    USING case_3d_future_chunk c
                    ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                    WHEN MATCHED THEN UPDATE SET {', '.join(set_expr)}
                """
            )
            logger.info(
                f"MERGED - CASE III Soft Delete Future chunk {idx}/{len(future_chunks)} (months={months})"
            )

    # Update latest_summary with future array/grid patches when latest month matches.
    if case_3d_future_df is not None and spark.catalog.tableExists(LATEST_SUMMARY_TABLE):
        latest_cols = set(spark.table(LATEST_SUMMARY_TABLE).columns)
        grid_cols = [g["name"] for g in grid_specs]
        patch_cols = [c for c in ([TS] + history_cols + grid_cols) if c in latest_cols and c in case_3d_future_df.columns]
        if patch_cols:
            # Prevent merge cardinality violations when multiple delete rows
            # produce duplicate source rows for the same account+month.
            patch_df = (
                case_3d_future_df
                .select(PK, PRT, *patch_cols)
                .dropDuplicates([PK, PRT])
            )
            patch_df.createOrReplaceTempView("latest_case_3d_future")

            set_expr = []
            for c in patch_cols:
                if c == TS:
                    set_expr.append(f"s.{c} = GREATEST(s.{c}, c.{c})")
                else:
                    set_expr.append(f"s.{c} = c.{c}")

            spark.sql(
                f"""
                    MERGE INTO {LATEST_SUMMARY_TABLE} s
                    USING latest_case_3d_future c
                    ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                    WHEN MATCHED
                    THEN UPDATE SET {', '.join(set_expr)}
                """
            )
            logger.info("Updated latest_summary | Applied soft-delete future patches")

    # Update latest_summary soft_del_cd for deleted month rows when latest month is deleted.
    if case_3d_month_df is not None and spark.catalog.tableExists(LATEST_SUMMARY_TABLE):
        latest_cols = set(spark.table(LATEST_SUMMARY_TABLE).columns)
        if SOFT_DELETE_COLUMN in latest_cols:
            (
                case_3d_month_df
                .select(PK, PRT, TS, SOFT_DELETE_COLUMN)
                .dropDuplicates([PK, PRT])
                .createOrReplaceTempView("latest_case_3d_month")
            )
            spark.sql(
                f"""
                    MERGE INTO {LATEST_SUMMARY_TABLE} s
                    USING latest_case_3d_month c
                    ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                    WHEN MATCHED THEN UPDATE SET
                        s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN},
                        s.{TS} = GREATEST(s.{TS}, c.{TS})
                """
            )
            logger.info("Updated latest_summary | Applied soft-delete month flags")

    # Reconstruct latest_summary rows if latest month itself was soft-deleted.
    if case_3d_month_df is not None and spark.catalog.tableExists(LATEST_SUMMARY_TABLE):
        delete_codes_sql = ",".join([f"'{code}'" for code in DELETE_CODES])
        case_3d_month_df.select(PK, PRT).distinct().createOrReplaceTempView("case_3d_deleted_months")

        deleted_latest_accounts = spark.sql(
            f"""
                SELECT l.{PK} as {PK}
                FROM {LATEST_SUMMARY_TABLE} l
                JOIN case_3d_deleted_months d
                  ON l.{PK} = d.{PK}
                 AND l.{PRT} = d.{PRT}
                GROUP BY l.{PK}
            """
        )

        if not deleted_latest_accounts.isEmpty():
            deleted_latest_accounts.createOrReplaceTempView("deleted_latest_accounts")
            replacement_df = spark.sql(
                f"""
                    SELECT *
                    FROM (
                        SELECT
                            s.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY s.{PK}
                                ORDER BY s.{PRT} DESC, s.{TS} DESC
                            ) as _rn
                        FROM {SUMMARY_TABLE} s
                        JOIN deleted_latest_accounts a ON s.{PK} = a.{PK}
                        WHERE COALESCE(s.{SOFT_DELETE_COLUMN}, '0') NOT IN ({delete_codes_sql})
                    ) x
                    WHERE _rn = 1
                """
            ).drop("_rn")

            if not replacement_df.isEmpty():
                latest_cols = spark.table(LATEST_SUMMARY_TABLE).columns
                common_cols = [c for c in latest_cols if c in replacement_df.columns]
                if PK in common_cols:
                    replacement_df.select(*common_cols).createOrReplaceTempView("latest_delete_replacements")
                    update_set = ", ".join([f"s.{c} = c.{c}" for c in common_cols if c != PK])
                    if update_set:
                        spark.sql(
                            f"""
                                MERGE INTO {LATEST_SUMMARY_TABLE} s
                                USING latest_delete_replacements c
                                ON s.{PK} = c.{PK}
                                WHEN MATCHED THEN UPDATE SET {update_set}
                            """
                        )
                        logger.info("Updated latest_summary | Reconstructed deleted-latest accounts")

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

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    start = time.time()
    try:
        has_updates, history_cols, grid_specs = _prepare_case_tables(spark)
        if not has_updates:
            logger.info("No soft-delete updates to apply.")
            return
        _merge_case_tables_chunked(spark, history_cols, grid_specs)
        logger.info("Soft-delete backfill completed.")
    finally:
        logger.info(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
        spark.stop()


if __name__ == "__main__":
    main()
