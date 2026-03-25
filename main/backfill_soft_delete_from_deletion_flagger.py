import json
import logging
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


MAIN_DIR = os.path.abspath(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(MAIN_DIR, "config.json")

# Standalone fixed tables for this one-time utility.
DELETION_FLAGGER_TABLE = "primary_catalog.edf_gold.deletion_flagger"
SUMMARY_TABLE = "primary_catalog.edf_gold.summary"
LATEST_SUMMARY_TABLE = "primary_catalog.edf_gold.latest_summary"

PK = "cons_acct_key"
PRT = "rpt_as_of_mo"
TS = "base_ts"
SOFT_DELETE_COLUMN = "soft_del_cd"
DELETE_CODES = ("1", "4")
HISTORY_LENGTH = 36
MONTH_CHUNK_OVERFLOW = 0.10

GRID_SPECS = [
    {
        "name": "payment_history_grid",
        "source_history": "payment_rating_cd_history",
        "placeholder": "?",
        "separator": "",
    }
]


def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def create_spark_session(app_name: str, spark_config: dict) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in (spark_config or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _month_to_int_expr(col_name: str) -> str:
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def _month_int_to_str(month_int: int) -> str:
    year = month_int // 12
    month = month_int % 12
    if month == 0:
        month = 12
        year -= 1
    return f"{year}-{month:02d}"


def _normalize_soft_del_col(df):
    if SOFT_DELETE_COLUMN not in df.columns:
        df = df.withColumn(SOFT_DELETE_COLUMN, F.lit(""))
    return df.withColumn(
        SOFT_DELETE_COLUMN,
        F.when(
            F.trim(F.coalesce(F.col(SOFT_DELETE_COLUMN).cast("string"), F.lit(""))) == "",
            F.lit(""),
        ).otherwise(F.trim(F.col(SOFT_DELETE_COLUMN).cast("string"))),
    )


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


def _align_for_merge(spark: SparkSession, df):
    base_parts = 200
    try:
        cfg_parts = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
        base_parts = max(base_parts, cfg_parts)
    except Exception:
        pass
    return df.repartition(base_parts, F.col(PRT), F.col(PK)).sortWithinPartitions(PRT, PK)


def ensure_target_columns(spark: SparkSession):
    if not spark.catalog.tableExists(SUMMARY_TABLE):
        raise RuntimeError(f"Summary table not found: {SUMMARY_TABLE}")

    summary_cols = set(spark.table(SUMMARY_TABLE).columns)
    if SOFT_DELETE_COLUMN not in summary_cols:
        logger.info(f"Adding {SOFT_DELETE_COLUMN} to {SUMMARY_TABLE}")
        spark.sql(f"ALTER TABLE {SUMMARY_TABLE} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")

    if spark.catalog.tableExists(LATEST_SUMMARY_TABLE):
        latest_cols = set(spark.table(LATEST_SUMMARY_TABLE).columns)
        if SOFT_DELETE_COLUMN not in latest_cols:
            logger.info(f"Adding {SOFT_DELETE_COLUMN} to {LATEST_SUMMARY_TABLE}")
            spark.sql(f"ALTER TABLE {LATEST_SUMMARY_TABLE} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")


def build_case_inputs(spark: SparkSession):
    if not spark.catalog.tableExists(DELETION_FLAGGER_TABLE):
        raise RuntimeError(f"Table not found: {DELETION_FLAGGER_TABLE}")

    summary_df = spark.table(SUMMARY_TABLE)
    summary_cols = summary_df.columns
    history_cols = sorted([c for c in summary_cols if c.endswith("_history")])
    grid_specs = [g for g in GRID_SPECS if g["name"] in summary_cols and g["source_history"] in history_cols]

    src = _normalize_soft_del_col(
        spark.table(DELETION_FLAGGER_TABLE).select(PK, PRT, SOFT_DELETE_COLUMN)
    ).filter(F.col(SOFT_DELETE_COLUMN).isin(*DELETE_CODES))

    # Deterministic dedupe in case subset has duplicate key-month records.
    src = src.dropDuplicates([PK, PRT, SOFT_DELETE_COLUMN])
    src = src.groupBy(PK, PRT).agg(F.max(F.col(SOFT_DELETE_COLUMN)).alias(SOFT_DELETE_COLUMN))

    candidate_count = src.count()
    logger.info(f"Deletion flagger candidate rows (soft_del_cd IN {DELETE_CODES}): {candidate_count:,}")
    if candidate_count == 0:
        return None, None, history_cols, grid_specs

    summary_keys = summary_df.select(PK, PRT).distinct()
    delete_existing = src.join(summary_keys, [PK, PRT], "inner")
    delete_existing_count = delete_existing.count()
    logger.info(f"Delete rows matching existing summary rows: {delete_existing_count:,}")
    if delete_existing_count == 0:
        return None, None, history_cols, grid_specs

    delete_range = delete_existing.agg(F.min(PRT).alias("min_mo"), F.max(PRT).alias("max_mo")).first()
    min_delete_mo = delete_range["min_mo"]
    max_delete_mo = delete_range["max_mo"]
    logger.info(f"Delete month range: {min_delete_mo} to {max_delete_mo}")

    case_month_df = delete_existing.select(PK, PRT, SOFT_DELETE_COLUMN)

    # Build future patch rows: null delete positions in summary future months.
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
        )
    )

    if not history_cols:
        logger.info("No *_history columns found in summary; only month-row soft_del updates will be applied.")
        return case_month_df, None, history_cols, grid_specs

    summary_future = summary_filtered.select(PK, PRT, "month_int", *history_cols)

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
            F.col(f"s.{PRT}").alias("summary_month"),
            (F.col("s.month_int") - F.col("d.delete_month_int")).alias("delete_position"),
            *[F.col(f"s.{c}").alias(f"existing_{c}") for c in history_cols],
        )
    )

    if joined.isEmpty():
        logger.info("No future month array/grid patches required.")
        return case_month_df, None, history_cols, grid_specs

    agg_exprs = [
        F.collect_set("delete_position").alias("delete_positions"),
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
    case_future_df = aggregated.selectExpr(
        PK,
        f"summary_month AS {PRT}",
        *update_exprs,
    )

    for grid in grid_specs:
        case_future_df = case_future_df.withColumn(
            grid["name"],
            F.concat_ws(
                grid["separator"],
                F.transform(
                    F.col(grid["source_history"]),
                    lambda x: F.coalesce(x.cast("string"), F.lit(grid["placeholder"])),
                ),
            ),
        )

    future_count = case_future_df.count()
    logger.info(f"Generated future patch rows: {future_count:,}")
    return case_month_df, case_future_df, history_cols, grid_specs


def merge_summary_updates(spark: SparkSession, case_month_df, case_future_df, history_cols, grid_specs):
    if case_month_df is not None:
        month_chunks = _build_month_chunks(case_month_df, overflow_ratio=MONTH_CHUNK_OVERFLOW)
        logger.info(f"Summary month-update chunks: {len(month_chunks)}")
        for idx, months in enumerate(month_chunks, 1):
            chunk_df = _align_for_merge(spark, case_month_df.filter(F.col(PRT).isin(months)))
            chunk_df.createOrReplaceTempView("case_month_chunk")
            spark.sql(
                f"""
                    MERGE INTO {SUMMARY_TABLE} s
                    USING case_month_chunk c
                    ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                    WHEN MATCHED THEN UPDATE SET
                        s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN}
                """
            )
            logger.info(f"Merged summary month chunk {idx}/{len(month_chunks)}")

    if case_future_df is not None:
        future_chunks = _build_month_chunks(case_future_df, overflow_ratio=MONTH_CHUNK_OVERFLOW)
        logger.info(f"Summary future-patch chunks: {len(future_chunks)}")
        set_expr = [f"s.{c} = c.{c}" for c in history_cols]
        set_expr += [f"s.{g['name']} = c.{g['name']}" for g in grid_specs]

        for idx, months in enumerate(future_chunks, 1):
            chunk_df = _align_for_merge(spark, case_future_df.filter(F.col(PRT).isin(months)))
            chunk_df.createOrReplaceTempView("case_future_chunk")
            spark.sql(
                f"""
                    MERGE INTO {SUMMARY_TABLE} s
                    USING case_future_chunk c
                    ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                    WHEN MATCHED THEN UPDATE SET {', '.join(set_expr)}
                """
            )
            logger.info(f"Merged summary future chunk {idx}/{len(future_chunks)}")


def merge_latest_updates(spark: SparkSession, case_month_df, case_future_df, history_cols, grid_specs):
    if not spark.catalog.tableExists(LATEST_SUMMARY_TABLE):
        logger.warning(f"{LATEST_SUMMARY_TABLE} not found; skipped latest updates.")
        return

    latest_cols = set(spark.table(LATEST_SUMMARY_TABLE).columns)

    if case_future_df is not None:
        grid_cols = [g["name"] for g in grid_specs]
        patch_cols = [c for c in (history_cols + grid_cols) if c in latest_cols and c in case_future_df.columns]
        if patch_cols:
            (
                case_future_df.select(PK, PRT, *patch_cols)
                .dropDuplicates([PK, PRT])
                .createOrReplaceTempView("latest_future_patch")
            )

            set_expr = [f"s.{c} = c.{c}" for c in patch_cols]

            spark.sql(
                f"""
                    MERGE INTO {LATEST_SUMMARY_TABLE} s
                    USING latest_future_patch c
                    ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                    WHEN MATCHED THEN UPDATE SET {', '.join(set_expr)}
                """
            )
            logger.info("Updated latest_summary future arrays/grid.")

    if case_month_df is not None and SOFT_DELETE_COLUMN in latest_cols:
        (
            case_month_df
            .select(PK, PRT, SOFT_DELETE_COLUMN)
            .dropDuplicates([PK, PRT])
            .createOrReplaceTempView("latest_month_flag_patch")
        )
        spark.sql(
            f"""
                MERGE INTO {LATEST_SUMMARY_TABLE} s
                USING latest_month_flag_patch c
                ON s.{PK} = c.{PK} AND s.{PRT} = c.{PRT}
                WHEN MATCHED THEN UPDATE SET
                    s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN}
            """
        )
        logger.info("Updated latest_summary month flags.")

    # Reconstruct latest for accounts whose latest month itself became deleted.
    if case_month_df is not None:
        delete_codes_sql = ",".join([f"'{c}'" for c in DELETE_CODES])
        case_month_df.select(PK, PRT).distinct().createOrReplaceTempView("deleted_month_rows")

        deleted_latest_accounts = spark.sql(
            f"""
                SELECT l.{PK} AS {PK}
                FROM {LATEST_SUMMARY_TABLE} l
                JOIN deleted_month_rows d
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
                            ) AS _rn
                        FROM {SUMMARY_TABLE} s
                        JOIN deleted_latest_accounts a ON s.{PK} = a.{PK}
                        WHERE COALESCE(s.{SOFT_DELETE_COLUMN}, '') NOT IN ({delete_codes_sql})
                    ) x
                    WHERE _rn = 1
                """
            ).drop("_rn")

            if not replacement_df.isEmpty():
                latest_cols_list = spark.table(LATEST_SUMMARY_TABLE).columns
                common_cols = [c for c in latest_cols_list if c in replacement_df.columns]
                if PK in common_cols:
                    replacement_df.select(*common_cols).createOrReplaceTempView("latest_replacements")
                    update_set = ", ".join([f"s.{c} = c.{c}" for c in common_cols if c != PK])
                    if update_set:
                        spark.sql(
                            f"""
                                MERGE INTO {LATEST_SUMMARY_TABLE} s
                                USING latest_replacements c
                                ON s.{PK} = c.{PK}
                                WHEN MATCHED THEN UPDATE SET {update_set}
                            """
                        )
                        logger.info("Reconstructed latest_summary for deleted-latest accounts.")


def run_job():
    config = load_config(CONFIG_PATH)
    spark = create_spark_session(
        config.get("spark", {}).get("app_name", "SummaryPipeline") + "_soft_delete_flagger_backfill",
        config.get("spark", {}),
    )
    spark.sparkContext.setLogLevel("WARN")
    start = time.time()
    try:
        ensure_target_columns(spark)
        case_month_df, case_future_df, history_cols, grid_specs = build_case_inputs(spark)

        if case_month_df is None and case_future_df is None:
            logger.info("No soft-delete updates to apply from deletion_flagger.")
            return

        merge_summary_updates(spark, case_month_df, case_future_df, history_cols, grid_specs)
        merge_latest_updates(spark, case_month_df, case_future_df, history_cols, grid_specs)
        logger.info("Soft-delete backfill from deletion_flagger completed successfully.")
    finally:
        logger.info(f"Elapsed: {(time.time() - start) / 60:.2f} minutes")
        spark.stop()


if __name__ == "__main__":
    run_job()
