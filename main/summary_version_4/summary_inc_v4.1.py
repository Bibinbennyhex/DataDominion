from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark import StorageLevel
import logging
import json
import argparse
import sys
import time
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple, Any, Optional
from functools import reduce
import math


TARGET_RECORDS_PER_PARTITION = 500_000_000
MIN_PARTITIONS = 16
MAX_PARTITIONS = 8192
AVG_RECORD_SIZE_BYTES = 200
SNAPSHOT_INTERVAL = 12
MAX_FILE_SIZE = 256
HISTORY_LENGTH = 36
CASE_TEMP_BUCKET_COUNT = 64
CASE3_LATEST_MONTH_PATCH_TABLE = "temp_catalog.checkpointdb.case_3_latest_month_patch"
CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE = "temp_catalog.checkpointdb.case_3_unified_latest_month_patch"
CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE = "temp_catalog.checkpointdb.case_3d_latest_history_context_patch"
CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE = "temp_catalog.checkpointdb.case_3d_unified_latest_history_patch"
TRACKER_SOURCE_SUMMARY = "summary"
TRACKER_SOURCE_LATEST_SUMMARY = "latest_summary"
TRACKER_SOURCE_COMMITTED = "committed_ingestion_watermark"
TRACKER_STATUS_RUNNING = "RUNNING"
TRACKER_STATUS_SUCCESS = "SUCCESS"
TRACKER_STATUS_FAILURE = "FAILURE"
SOFT_DELETE_COLUMN = "soft_del_cd"
SOFT_DELETE_CODES = ["1", "4"]
LATEST_HISTORY_MIN_LEN_V4 = 72


def setup_logger(log_level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    handler.setFormatter(formatter)


    logger = logging.getLogger("summary_pipeline")
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.propagate = False

    return logger

logger = setup_logger("INFO")


logger = logging.getLogger("summary_pipeline")


def load_config(bucket, key):
    s3 = boto3.client("s3")
    logger.info(f"Loading configuration from: s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    config = json.loads(obj["Body"].read().decode("utf-8"))
    
    return config


def get_watermark_tracker_table(config: Dict[str, Any]) -> str:
    """Resolve tracker table name from config or derive from destination namespace."""
    explicit_table = config.get("watermark_tracker_table")
    if explicit_table:
        return explicit_table

    destination_table = config["destination_table"]
    if "." not in destination_table:
        return f"{destination_table}_watermark_tracker"

    namespace, _ = destination_table.rsplit(".", 1)
    return f"{namespace}.summary_watermark_tracker"


def _ensure_watermark_tracker_table(spark: SparkSession, tracker_table: str) -> None:
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {tracker_table} (
                source_name STRING,
                source_table STRING,
                max_base_ts TIMESTAMP,
                max_rpt_as_of_mo STRING,
                updated_at TIMESTAMP,
                previous_successful_snapshot_id BIGINT,
                current_snapshot_id BIGINT,
                status STRING,
                run_id STRING,
                error_message STRING
            )
            USING iceberg
        """
    )
    existing_cols = set(spark.table(tracker_table).columns)
    required_cols = {
        "previous_successful_snapshot_id": "BIGINT",
        "current_snapshot_id": "BIGINT",
        "status": "STRING",
        "run_id": "STRING",
        "error_message": "STRING",
    }
    for col_name, data_type in required_cols.items():
        if col_name not in existing_cols:
            spark.sql(f"ALTER TABLE {tracker_table} ADD COLUMN {col_name} {data_type}")


def _capture_table_state(
    spark: SparkSession,
    table_name: str,
    partition_col: str,
    ts_col: str,
) -> Dict[str, Optional[Any]]:
    state = {
        "max_base_ts": None,
        "max_rpt_as_of_mo": None,
        "snapshot_id": None,
    }
    if not spark.catalog.tableExists(table_name):
        return state

    stats = (
        spark.table(table_name)
        .agg(
            F.max(F.col(ts_col)).alias("max_base_ts"),
            F.max(F.col(partition_col)).alias("max_rpt_as_of_mo"),
        )
        .first()
    )
    state["max_base_ts"] = stats["max_base_ts"]
    state["max_rpt_as_of_mo"] = stats["max_rpt_as_of_mo"]

    try:
        snap = spark.sql(
            f"""
                SELECT snapshot_id
                FROM {table_name}.snapshots
                ORDER BY committed_at DESC
                LIMIT 1
            """
        ).collect()
        if snap:
            state["snapshot_id"] = snap[0]["snapshot_id"]
    except Exception:
        # Table may exist without snapshots yet in some early lifecycle states.
        pass

    return state


def _read_tracker_rows(spark: SparkSession, tracker_table: str) -> Dict[str, Dict[str, Any]]:
    if not spark.catalog.tableExists(tracker_table):
        return {}
    rows = spark.table(tracker_table).collect()
    return {row["source_name"]: row.asDict() for row in rows}


def _upsert_tracker_rows(
    spark: SparkSession,
    tracker_table: str,
    rows: List[Tuple[Any, ...]],
) -> None:
    update_df = spark.createDataFrame(
        rows,
        schema=(
            "source_name STRING, source_table STRING, max_base_ts TIMESTAMP, "
            "max_rpt_as_of_mo STRING, updated_at TIMESTAMP, "
            "previous_successful_snapshot_id BIGINT, current_snapshot_id BIGINT, "
            "status STRING, run_id STRING, error_message STRING"
        ),
    )
    update_df.createOrReplaceTempView("watermark_tracker_updates")
    spark.sql(
        f"""
            MERGE INTO {tracker_table} t
            USING watermark_tracker_updates u
            ON t.source_name = u.source_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    )


def _capture_run_snapshot_states(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    prt = config["partition_column"]
    ts = config["max_identifier_column"]
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    return {
        TRACKER_SOURCE_SUMMARY: _capture_table_state(spark, summary_table, prt, ts),
        TRACKER_SOURCE_LATEST_SUMMARY: _capture_table_state(spark, latest_summary_table, prt, ts),
    }


def log_current_snapshot_state(spark: SparkSession, config: Dict[str, Any], label: str) -> Dict[str, Dict[str, Any]]:
    states = _capture_run_snapshot_states(spark, config)
    logger.info(f"{label} snapshot state:")
    logger.info(
        f"  summary: snapshot_id={states[TRACKER_SOURCE_SUMMARY]['snapshot_id']}, "
        f"max_base_ts={states[TRACKER_SOURCE_SUMMARY]['max_base_ts']}, "
        f"max_month={states[TRACKER_SOURCE_SUMMARY]['max_rpt_as_of_mo']}"
    )
    logger.info(
        f"  latest_summary: snapshot_id={states[TRACKER_SOURCE_LATEST_SUMMARY]['snapshot_id']}, "
        f"max_base_ts={states[TRACKER_SOURCE_LATEST_SUMMARY]['max_base_ts']}, "
        f"max_month={states[TRACKER_SOURCE_LATEST_SUMMARY]['max_rpt_as_of_mo']}"
    )
    return states


def _parse_catalog_and_identifier(table_name: str) -> Tuple[str, str]:
    parts = table_name.split(".")
    if len(parts) < 2:
        raise ValueError(f"Invalid table name for rollback: {table_name}")
    catalog = parts[0]
    identifier = ".".join(parts[1:])
    return catalog, identifier


def _rollback_table_to_snapshot(
    spark: SparkSession,
    table_name: str,
    snapshot_id: Optional[Any],
) -> bool:
    """
    Rollback a single Iceberg table to a given snapshot id.
    Returns True when rollback statement executed successfully.
    """
    if snapshot_id is None:
        logger.warning(f"Rollback skipped for {table_name}: start snapshot is NULL")
        return False
    if not spark.catalog.tableExists(table_name):
        logger.warning(f"Rollback skipped for {table_name}: table does not exist")
        return False

    catalog, identifier = _parse_catalog_and_identifier(table_name)
    snapshot_id_int = int(snapshot_id)

    # Try named-argument call first, then positional fallback.
    try:
        spark.sql(
            f"""
                CALL {catalog}.system.rollback_to_snapshot(
                    table => '{identifier}',
                    snapshot_id => {snapshot_id_int}
                )
            """
        )
        return True
    except Exception:
        spark.sql(
            f"CALL {catalog}.system.rollback_to_snapshot('{identifier}', {snapshot_id_int})"
        )
        return True


def rollback_tables_to_run_start(
    spark: SparkSession,
    config: Dict[str, Any],
    start_states: Dict[str, Dict[str, Any]],
) -> Dict[str, str]:
    """
    Best-effort rollback for summary and latest_summary to their pre-run snapshots.
    Returns per-table status messages.
    """
    statuses: Dict[str, str] = {}
    table_map = {
        TRACKER_SOURCE_SUMMARY: config["destination_table"],
        TRACKER_SOURCE_LATEST_SUMMARY: config["latest_history_table"],
    }

    for source_name, table_name in table_map.items():
        start_snapshot = start_states.get(source_name, {}).get("snapshot_id")
        try:
            executed = _rollback_table_to_snapshot(spark, table_name, start_snapshot)
            if executed:
                statuses[source_name] = f"ROLLED_BACK_TO_{start_snapshot}"
                logger.info(
                    f"Rollback complete for {source_name} ({table_name}) "
                    f"to snapshot_id={start_snapshot}"
                )
            else:
                statuses[source_name] = f"SKIPPED_START_SNAPSHOT_{start_snapshot}"
        except Exception as rollback_error:
            statuses[source_name] = f"ROLLBACK_FAILED_{rollback_error}"
            logger.error(
                f"Rollback failed for {source_name} ({table_name}) "
                f"to snapshot_id={start_snapshot}: {rollback_error}"
            )

    return statuses


def mark_run_started(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str,
    start_states: Dict[str, Dict[str, Any]],
) -> None:
    tracker_table = get_watermark_tracker_table(config)
    _ensure_watermark_tracker_table(spark, tracker_table)
    existing = _read_tracker_rows(spark, tracker_table)

    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    now = datetime.utcnow()

    update_rows = []
    for source_name, source_table in [
        (TRACKER_SOURCE_SUMMARY, summary_table),
        (TRACKER_SOURCE_LATEST_SUMMARY, latest_summary_table),
    ]:
        existing_row = existing.get(source_name, {})
        prev_success_id = existing_row.get("previous_successful_snapshot_id")
        if prev_success_id is None and existing_row.get("status") == TRACKER_STATUS_SUCCESS:
            prev_success_id = existing_row.get("current_snapshot_id")
        state = start_states[source_name]
        update_rows.append(
            (
                source_name,
                source_table,
                state["max_base_ts"],
                state["max_rpt_as_of_mo"],
                now,
                prev_success_id,
                state["snapshot_id"],
                TRACKER_STATUS_RUNNING,
                run_id,
                None,
            )
        )

    committed_existing = existing.get(TRACKER_SOURCE_COMMITTED, {})
    committed_ts = committed_existing.get("max_base_ts")
    committed_month = committed_existing.get("max_rpt_as_of_mo")
    if committed_ts is None:
        committed_ts = _safe_min_datetime(
            start_states[TRACKER_SOURCE_SUMMARY]["max_base_ts"],
            start_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_base_ts"],
        )
    if committed_month is None:
        committed_month = _safe_min_month(
            start_states[TRACKER_SOURCE_SUMMARY]["max_rpt_as_of_mo"],
            start_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_rpt_as_of_mo"],
        )
    update_rows.append(
        (
            TRACKER_SOURCE_COMMITTED,
            config["source_table"],
            committed_ts,
            committed_month,
            now,
            committed_existing.get("previous_successful_snapshot_id"),
            committed_existing.get("current_snapshot_id"),
            TRACKER_STATUS_RUNNING,
            run_id,
            None,
        )
    )

    _upsert_tracker_rows(spark, tracker_table, update_rows)
    logger.info(f"Marked tracker RUNNING for run_id={run_id}")


def finalize_run_tracking(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str,
    start_states: Dict[str, Dict[str, Any]],
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    tracker_table = get_watermark_tracker_table(config)
    _ensure_watermark_tracker_table(spark, tracker_table)
    existing = _read_tracker_rows(spark, tracker_table)
    end_states = _capture_run_snapshot_states(spark, config)
    now = datetime.utcnow()

    status = TRACKER_STATUS_SUCCESS if success else TRACKER_STATUS_FAILURE
    update_rows = []

    for source_name, source_table in [
        (TRACKER_SOURCE_SUMMARY, config["destination_table"]),
        (TRACKER_SOURCE_LATEST_SUMMARY, config["latest_history_table"]),
    ]:
        existing_row = existing.get(source_name, {})
        if success:
            prev_success_id = start_states[source_name]["snapshot_id"]
        else:
            prev_success_id = existing_row.get("previous_successful_snapshot_id")
            if prev_success_id is None and existing_row.get("status") == TRACKER_STATUS_SUCCESS:
                prev_success_id = existing_row.get("current_snapshot_id")

        update_rows.append(
            (
                source_name,
                source_table,
                end_states[source_name]["max_base_ts"],
                end_states[source_name]["max_rpt_as_of_mo"],
                now,
                prev_success_id,
                end_states[source_name]["snapshot_id"],
                status,
                run_id,
                None if success else (error_message[:500] if error_message else "pipeline_failed"),
            )
        )

    committed_existing = existing.get(TRACKER_SOURCE_COMMITTED, {})
    if success:
        committed_ts = _safe_min_datetime(
            end_states[TRACKER_SOURCE_SUMMARY]["max_base_ts"],
            end_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_base_ts"],
        )
        committed_month = _safe_min_month(
            end_states[TRACKER_SOURCE_SUMMARY]["max_rpt_as_of_mo"],
            end_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_rpt_as_of_mo"],
        )
    else:
        committed_ts = committed_existing.get("max_base_ts")
        committed_month = committed_existing.get("max_rpt_as_of_mo")

    update_rows.append(
        (
            TRACKER_SOURCE_COMMITTED,
            config["source_table"],
            committed_ts,
            committed_month,
            now,
            committed_existing.get("previous_successful_snapshot_id"),
            committed_existing.get("current_snapshot_id"),
            status,
            run_id,
            None if success else (error_message[:500] if error_message else "pipeline_failed"),
        )
    )

    _upsert_tracker_rows(spark, tracker_table, update_rows)
    logger.info(
        f"Finalized tracker for run_id={run_id}, status={status}, "
        f"tracker_table={tracker_table}"
    )


def _safe_min_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    if a is None:
        return b
    if b is None:
        return a
    return a if a <= b else b


def _safe_min_month(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if a is None:
        return b
    if b is None:
        return a
    return a if a <= b else b


def get_committed_ingestion_watermark(
    spark: SparkSession,
    config: Dict[str, Any],
    fallback_base_ts: Optional[datetime],
    fallback_rpt_as_of_mo: Optional[str],
) -> Tuple[Optional[datetime], Optional[str], str]:
    """
    Return committed ingestion watermark from tracker if available, else fallback.

    This watermark is used to filter source increments and is advanced only after
    successful end-to-end run completion.
    """
    tracker_table = get_watermark_tracker_table(config)

    if spark.catalog.tableExists(tracker_table):
        row = (
            spark.table(tracker_table)
            .filter(F.col("source_name") == TRACKER_SOURCE_COMMITTED)
            .select("max_base_ts", "max_rpt_as_of_mo")
            .limit(1)
            .collect()
        )
        if row:
            committed_ts = row[0]["max_base_ts"]
            committed_month = row[0]["max_rpt_as_of_mo"]
            if committed_ts is not None:
                return committed_ts, committed_month, "tracker"

    return fallback_base_ts, fallback_rpt_as_of_mo, "fallback_summary"


def refresh_watermark_tracker(
    spark: SparkSession,
    config: Dict[str, Any],
    mark_committed: bool = True,
) -> None:
    """
    Persist latest watermark snapshot for summary and latest_summary tables.

    Tracker schema:
      source_name: 'summary' | 'latest_summary' | 'committed_ingestion_watermark'
      source_table: fully qualified table name
      max_base_ts: latest base_ts in source table
      max_rpt_as_of_mo: latest rpt_as_of_mo in source table
      updated_at: tracker refresh timestamp (UTC)
    """
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    prt = config["partition_column"]
    ts = config["max_identifier_column"]
    tracker_table = get_watermark_tracker_table(config)

    _ensure_watermark_tracker_table(spark, tracker_table)

    summary_stats = (
        spark.table(summary_table)
        .agg(
            F.max(F.col(ts)).alias("max_base_ts"),
            F.max(F.col(prt)).alias("max_rpt_as_of_mo"),
        )
        .first()
    )

    latest_summary_stats = (
        spark.table(latest_summary_table)
        .agg(
            F.max(F.col(ts)).alias("max_base_ts"),
            F.max(F.col(prt)).alias("max_rpt_as_of_mo"),
        )
        .first()
    )

    update_rows = [
        (
            TRACKER_SOURCE_SUMMARY,
            summary_table,
            summary_stats["max_base_ts"],
            summary_stats["max_rpt_as_of_mo"],
            datetime.utcnow(),
        ),
        (
            TRACKER_SOURCE_LATEST_SUMMARY,
            latest_summary_table,
            latest_summary_stats["max_base_ts"],
            latest_summary_stats["max_rpt_as_of_mo"],
            datetime.utcnow(),
        ),
    ]

    if mark_committed:
        committed_base_ts = _safe_min_datetime(
            summary_stats["max_base_ts"],
            latest_summary_stats["max_base_ts"],
        )
        committed_rpt_as_of_mo = _safe_min_month(
            summary_stats["max_rpt_as_of_mo"],
            latest_summary_stats["max_rpt_as_of_mo"],
        )
        update_rows.append(
            (
                TRACKER_SOURCE_COMMITTED,
                config["source_table"],
                committed_base_ts,
                committed_rpt_as_of_mo,
                datetime.utcnow(),
            )
        )

    update_df = spark.createDataFrame(
        update_rows,
        schema="source_name STRING, source_table STRING, max_base_ts TIMESTAMP, max_rpt_as_of_mo STRING, updated_at TIMESTAMP",
    )
    update_df.createOrReplaceTempView("watermark_tracker_updates")

    spark.sql(
        f"""
            MERGE INTO {tracker_table} t
            USING watermark_tracker_updates u
            ON t.source_name = u.source_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    )

    logger.info(
        f"Refreshed watermark tracker table: {tracker_table} "
        f"(mark_committed={mark_committed})"
    )


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration"""
    required_fields = ["source_table", "partition_column", "destination_table", "latest_history_table", "primary_column", "primary_date_column", "max_identifier_column", "history_length", "columns", "column_transformations", "inferred_columns", "coalesce_exclusion_cols", "date_col_list", "latest_history_addon_cols", "rolling_columns", "grid_columns", "spark"]
    
    for field in required_fields:
        if field not in config or not config[field]:
            logger.error(f"Required config field '{field}' is missing or empty")
            return False
    
    return True


def ensure_soft_delete_columns(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Ensure summary/latest_summary tables both expose soft_del_cd.
    """
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]

    if spark.catalog.tableExists(summary_table):
        summary_cols = _read_table_columns(spark, summary_table)
        if SOFT_DELETE_COLUMN not in summary_cols:
            logger.info(f"Adding {SOFT_DELETE_COLUMN} to {summary_table}")
            spark.sql(f"ALTER TABLE {summary_table} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")

    if spark.catalog.tableExists(latest_summary_table):
        latest_cols = _read_table_columns(spark, latest_summary_table)
        if SOFT_DELETE_COLUMN not in latest_cols:
            logger.info(f"Adding {SOFT_DELETE_COLUMN} to {latest_summary_table}")
            spark.sql(f"ALTER TABLE {latest_summary_table} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")


def _read_table_columns(spark: SparkSession, table_name: str) -> List[str]:
    return spark.table(table_name).columns


def preload_run_table_columns(spark: SparkSession, config: Dict[str, Any]) -> None:
    runtime_cache = config.setdefault("_runtime_cache", {})
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]

    runtime_cache["summary_cols"] = (
        _read_table_columns(spark, summary_table)
        if spark.catalog.tableExists(summary_table)
        else []
    )
    runtime_cache["latest_summary_cols"] = (
        _read_table_columns(spark, latest_summary_table)
        if spark.catalog.tableExists(latest_summary_table)
        else []
    )

    logger.info(
        f"Preloaded run schema cache | summary_cols={len(runtime_cache['summary_cols'])}, "
        f"latest_summary_cols={len(runtime_cache['latest_summary_cols'])}"
    )


def get_summary_cols(config: Dict[str, Any]) -> List[str]:
    runtime_cache = config.get("_runtime_cache", {})
    if "summary_cols" not in runtime_cache:
        raise ValueError("summary_cols missing from runtime cache; call preload_run_table_columns first")
    return runtime_cache["summary_cols"]


def get_latest_cols(config: Dict[str, Any]) -> List[str]:
    runtime_cache = config.get("_runtime_cache", {})
    if "latest_summary_cols" not in runtime_cache:
        raise ValueError("latest_summary_cols missing from runtime cache; call preload_run_table_columns first")
    return runtime_cache["latest_summary_cols"]


def get_case3_latest_month_patch_table(config: Dict[str, Any]) -> str:
    return config.get("_case3_latest_month_patch_table", CASE3_LATEST_MONTH_PATCH_TABLE)


def get_case3d_latest_history_patch_table(config: Dict[str, Any]) -> str:
    return config.get("_case3d_latest_history_patch_table", CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE)


def get_summary_history_len(config: Dict[str, Any]) -> int:
    return _to_int(config.get("history_length", HISTORY_LENGTH), HISTORY_LENGTH)


def get_latest_history_len(config: Dict[str, Any]) -> int:
    configured_latest_len = _to_int(
        config.get("latest_history_window_months", LATEST_HISTORY_MIN_LEN_V4),
        LATEST_HISTORY_MIN_LEN_V4,
    )
    return max(configured_latest_len, get_summary_history_len(config))


def align_history_arrays_to_length(df, rolling_columns: List[Dict[str, Any]], target_len: int):
    if target_len <= 0:
        return df
    for rc in rolling_columns:
        history_col = f"{rc['name']}_history"
        if history_col in df.columns:
            df = df.withColumn(
                history_col,
                F.transform(
                    F.sequence(F.lit(0), F.lit(target_len - 1)),
                    lambda i: F.element_at(F.col(history_col), i + F.lit(1)),
                ),
            )
    return df


def latest_history_preserve_tail_expr(
    array_name: str,
    summary_history_len: int,
    latest_history_len: int,
) -> str:
    return (
        f"transform(sequence(0, {latest_history_len - 1}), i -> "
        f"CASE WHEN i < {summary_history_len} "
        f"THEN element_at(c.{array_name}, i + 1) "
        f"ELSE element_at(s.{array_name}, i + 1) END)"
    )


def summary_history_trim_expr(array_name: str, summary_history_len: int) -> str:
    return f"slice(c.{array_name}, 1, {summary_history_len})"


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


def _to_int(value: Any, default: int) -> int:
    """Best-effort int conversion with default fallback."""
    try:
        return int(value)
    except Exception:
        return default


def get_write_partitions(
    spark: SparkSession,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
    scale_factor: float = 1.0,
    stage: str = ""
) -> int:
    """
    Compute write partitions efficiently and effectively.

    Strategy:
      1. Respect explicit override `write_partitions` if provided.
      2. Cache runtime defaults (shuffle/defaultParallelism) once per run.
      3. If expected_rows is known, blend row-based sizing with a baseline
         parallelism floor to avoid under-utilization.
    """
    configured = config.get("write_partitions")
    if configured is not None:
        parts = _to_int(configured, MIN_PARTITIONS)
        parts = max(MIN_PARTITIONS, min(MAX_PARTITIONS, parts))
        return parts

    runtime_cache = config.setdefault("_runtime_cache", {})
    write_cache = runtime_cache.get("write_partitions")
    if write_cache is None:
        shuffle_parts = _to_int(spark.conf.get("spark.sql.shuffle.partitions", "200"), 200)
        default_parallelism = _to_int(spark.sparkContext.defaultParallelism, MIN_PARTITIONS)
        base_parallelism = max(shuffle_parts, default_parallelism)

        perf_cfg = config.get("performance", {})
        min_parts = _to_int(
            config.get("min_write_partitions", perf_cfg.get("min_partitions", MIN_PARTITIONS)),
            MIN_PARTITIONS
        )
        max_parts = _to_int(
            config.get("max_write_partitions", perf_cfg.get("max_partitions", MAX_PARTITIONS)),
            MAX_PARTITIONS
        )
        target_rows_per_partition = _to_int(
            config.get("target_rows_per_partition", perf_cfg.get("target_records_per_partition", 2_000_000)),
            2_000_000
        )
        min_parallelism_factor = float(
            config.get("min_parallelism_factor", perf_cfg.get("min_parallelism_factor", 0.25))
        )

        write_cache = {
            "base_parallelism": base_parallelism,
            "min_parts": max(1, min_parts),
            "max_parts": max(1, max_parts),
            "target_rows_per_partition": max(1, target_rows_per_partition),
            "min_parallelism_factor": max(0.0, min_parallelism_factor),
        }
        runtime_cache["write_partitions"] = write_cache

    base_parallelism = write_cache["base_parallelism"]
    min_parts = write_cache["min_parts"]
    max_parts = write_cache["max_parts"]
    target_rows_per_partition = write_cache["target_rows_per_partition"]
    min_parallelism_factor = write_cache["min_parallelism_factor"]

    scaled_base = max(1, int(round(base_parallelism * scale_factor)))

    if expected_rows is not None and expected_rows > 0:
        row_based = int(math.ceil(float(expected_rows) / float(target_rows_per_partition)))
        floor_parallel = int(max(min_parts, round(scaled_base * min_parallelism_factor)))
        desired = max(row_based, floor_parallel)
    else:
        desired = scaled_base

    parts = max(min_parts, min(max_parts, desired))

    if stage:
        logger.info(
            f"Write partitions [{stage}]: {parts} "
            f"(base={base_parallelism}, expected_rows={expected_rows}, scale={scale_factor})"
        )

    return parts


def align_for_summary_merge(
    spark: SparkSession,
    df,
    config: Dict[str, Any],
    stage: str,
    expected_rows: Optional[int] = None,
):
    """
    Align merge source layout with summary table distribution:
    repartition on (partition_column, primary_column) and sort within partition.
    """
    pk = config["primary_column"]
    prt = config["partition_column"]
    write_parts = get_write_partitions(spark, config, expected_rows=expected_rows, stage=stage)
    return df.repartition(write_parts, F.col(prt), F.col(pk)).sortWithinPartitions(prt, pk)


def write_case_table_bucketed(
    spark: SparkSession,
    df,
    table_name: str,
    config: Dict[str, Any],
    stage: str,
    expected_rows: Optional[int] = None,
) -> None:
    """
    Materialize case_* table as Iceberg table partitioned by month and bucketed by account key.
    """
    pk = config["primary_column"]
    prt = config["partition_column"]
    bucket_count = int(config.get("case_temp_bucket_count", CASE_TEMP_BUCKET_COUNT))

    if pk not in df.columns or prt not in df.columns:
        raise ValueError(f"{table_name} write requires columns '{pk}' and '{prt}'")

    aligned_df = align_for_summary_merge(
        spark=spark,
        df=df,
        config=config,
        stage=stage,
        expected_rows=expected_rows,
    )
    temp_view = f"_tmp_{table_name.replace('.', '_')}_{int(time.time() * 1000)}"
    namespace = table_name.rsplit(".", 1)[0]

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    aligned_df.createOrReplaceTempView(temp_view)
    create_sql = f"""
        CREATE TABLE {table_name}
        USING iceberg
        PARTITIONED BY ({prt}, bucket({bucket_count}, {pk}))
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.merge.distribution-mode'='hash'
        )
        AS SELECT * FROM {temp_view}
    """
    max_retries = 3
    try:
        for attempt in range(1, max_retries + 1):
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            try:
                spark.sql(create_sql)
                break
            except Exception as e:
                msg = str(e).lower()
                if "already exists" in msg and attempt < max_retries:
                    time.sleep(1.0 * attempt)
                    continue
                raise
    finally:
        spark.catalog.dropTempView(temp_view)


def drop_case_tables(
    spark: SparkSession,
    table_stage_map: Dict[str, str],
) -> None:
    for table_name in table_stage_map.keys():
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def snapshot_case_tables(
    spark: SparkSession,
    table_stage_map: Dict[str, str],
    lane_tag: str,
) -> Dict[str, str]:
    snapshot_tables: Dict[str, str] = {}
    for table_name in table_stage_map.keys():
        if spark.catalog.tableExists(table_name):
            snapshot_name = f"{table_name}_{lane_tag}"
            spark.sql(f"DROP TABLE IF EXISTS {snapshot_name}")
            spark.sql(f"CREATE TABLE {snapshot_name} USING iceberg AS SELECT * FROM {table_name}")
            snapshot_tables[table_name] = snapshot_name
    return snapshot_tables


def cleanup_snapshot_tables(
    spark: SparkSession,
    snapshot_groups: List[Dict[str, str]],
) -> None:
    for snapshot_map in snapshot_groups:
        for snapshot_name in snapshot_map.values():
            spark.sql(f"DROP TABLE IF EXISTS {snapshot_name}")


def combine_case_table_snapshots(
    spark: SparkSession,
    config: Dict[str, Any],
    table_stage_map: Dict[str, str],
    snapshot_groups: List[Dict[str, str]],
    expected_rows: Optional[int] = None,
) -> None:
    for table_name, stage_name in table_stage_map.items():
        parts = []
        for snapshot_map in snapshot_groups:
            snapshot_name = snapshot_map.get(table_name)
            if snapshot_name and spark.catalog.tableExists(snapshot_name):
                parts.append(spark.read.table(snapshot_name))

        if not parts:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            continue

        combined_df = parts[0]
        for extra_df in parts[1:]:
            combined_df = combined_df.unionByName(extra_df, allowMissingColumns=True)
        combined_df = combined_df.dropDuplicates()

        write_case_table_bucketed(
            spark=spark,
            df=combined_df,
            table_name=table_name,
            config=config,
            stage=stage_name,
            expected_rows=expected_rows,
        )


def build_balanced_month_chunks(
    month_weights: List[Tuple[str, float]],
    overflow_ratio: float = 0.10,
) -> List[List[str]]:
    """
    Build balanced month chunks using a greedy bin-packing strategy.

    Args:
        month_weights: list of (rpt_as_of_mo, weighted_load)
        overflow_ratio: allowed overflow over the target chunk load.

    Returns:
        List of month lists, each list representing one merge chunk.
    """
    if not month_weights:
        return []

    # Largest month is used as baseline target load.
    sorted_weights = sorted(
        [(m, float(w)) for m, w in month_weights if m is not None and w is not None and w > 0],
        key=lambda x: x[1],
        reverse=True,
    )
    if not sorted_weights:
        return []

    target = sorted_weights[0][1]
    allowed = target * (1.0 + max(0.0, overflow_ratio))

    bins: List[Dict[str, Any]] = []
    for month, weight in sorted_weights:
        if not bins:
            bins.append({"load": weight, "months": [month]})
            continue

        # Place next month into the lightest chunk when possible.
        lightest_idx = min(range(len(bins)), key=lambda i: bins[i]["load"])
        if bins[lightest_idx]["load"] + weight <= allowed:
            bins[lightest_idx]["load"] += weight
            bins[lightest_idx]["months"].append(month)
        else:
            bins.append({"load": weight, "months": [month]})

    chunks = [sorted(b["months"]) for b in bins if b["months"]]
    return chunks


def build_month_chunks_from_df(
    df,
    prt: str,
    overflow_ratio: float = 0.10,
) -> List[List[str]]:
    """
    Build balanced month chunks from a dataframe using per-month row counts as weights.
    """
    if df is None:
        return []

    month_weights = [
        (r[prt], float(r["count"]))
        for r in df.groupBy(prt).count().select(prt, "count").collect()
        if r[prt] is not None and r["count"] is not None and r["count"] > 0
    ]

    month_chunks = build_balanced_month_chunks(month_weights, overflow_ratio=overflow_ratio)
    if month_chunks:
        return month_chunks

    return [[r[prt]] for r in df.select(prt).distinct().orderBy(prt).collect() if r[prt] is not None]





def prepare_source_data(df, date_df, config: Dict[str, Any]):
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
    pdt = config['primary_date_column']
    
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
    
    date_df = date_df.withColumn(prt, F.date_format(F.col(pdt), "yyyy-MM"))
    hist_dedup_order_cols = [ts, "insert_dt", "update_dt", "insert_time", "update_time"]
    hist_dedup_order_exprs = [
        F.col(col_name).desc()
        for col_name in hist_dedup_order_cols
        if col_name in date_df.columns
    ]
    if not hist_dedup_order_exprs:
        hist_dedup_order_exprs = [F.col(ts).desc()]

    date_df_window_spec = Window.partitionBy(pk, prt).orderBy(*hist_dedup_order_exprs)

    date_df = (
        date_df
        .withColumn("_rn", F.row_number().over(date_df_window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    r = result.alias("r")
    d = F.broadcast(date_df.alias("d"))
    delete_codes_sql = ",".join([f"'{code}'" for code in SOFT_DELETE_CODES])

    result_final = r.join(
        d,
        (
            (F.col(f"r.{pk}") == F.col(f"d.{pk}")) &
            (F.col(f"r.{prt}") == F.col(f"d.{prt}"))
        ),
        "left"
    ).select(
        *[F.col(f"r.{c}") for c in result.columns if c not in [pdt,"soft_del_cd"]],
        F.expr(
            f"""
                CASE
                    WHEN d.{pk} IS NULL
                    THEN r.{pdt}
                    WHEN COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql})
                        AND COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql})
                    THEN NULL
                    WHEN COALESCE(r.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql}) OR r.{pdt} > d.{pdt})
                    THEN r.{pdt}
                    WHEN d.{pk} IS NOT NULL
                        AND COALESCE(d.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql}) OR d.{pdt} > r.{pdt})
                    THEN d.{pdt}
                    ELSE r.{pdt}
                END
            """
        ).alias(pdt),
        F.expr(
            f"""
                CASE
                    WHEN d.{pk} IS NULL
                    THEN r.soft_del_cd
                    WHEN COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql})
                        AND COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql})
                    THEN r.soft_del_cd
                    WHEN COALESCE(r.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql}) OR r.{pdt} > d.{pdt})
                    THEN r.soft_del_cd
                    WHEN d.{pk} IS NOT NULL
                        AND COALESCE(d.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql}) OR d.{pdt} > r.{pdt})
                    THEN d.soft_del_cd
                    ELSE r.soft_del_cd
                END
            """
        ).alias("soft_del_cd")
    )

    logger.info("Source data preparation complete")
    return result_final


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
    hist_rpt_dt_table = config['hist_rpt_dt_table']
    hist_rpt_dt_cols = config['hist_rpt_dt_cols']

    if spark.catalog.tableExists(destination_table):
        fallback_max_base_ts = spark.table(destination_table) \
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

    effective_max_base_ts, _, watermark_source = get_committed_ingestion_watermark(
        spark,
        config,
        fallback_max_base_ts,
        max_month_destination,
    )
    if effective_max_base_ts is None:
        effective_max_base_ts = datetime(1900, 1, 1)
    logger.info(
        f"Using ingestion watermark ({watermark_source}) "
        f"for source filter: {ts} > {effective_max_base_ts}"
    )

    # Load accounts
    logger.info(f"Loading accounts from {source_table}")
    if max_month_destination is None or min_month_destination is None:
        next_month = None
        accounts_df = spark.read.table(source_table).filter(F.col(ts) > F.lit(effective_max_base_ts))
        logging.info(f"Reading from {source_table} - {ts} > {effective_max_base_ts}")
    else:
        next_month = (datetime.strptime(max_month_destination, "%Y-%m") + relativedelta(months=1)).strftime("%Y-%m")
        accounts_df = spark.read.table(source_table)\
            .filter(
                    (F.col(prt) >= f"{min_month_destination}")
                    & (F.col(prt) < f"{next_month}")
                    & (F.col(ts) > F.lit(effective_max_base_ts))
                )
        
        logging.info(f"Reading from {source_table} - ({prt} < {next_month}) & ({ts} > {effective_max_base_ts})")

    # Load account_hist_rpt_dt
    logger.info(f"Loading account_hist_rpt_dt from {hist_rpt_dt_table}")
    hist_rpt_dt_df = spark.read.table(hist_rpt_dt_table).select(*hist_rpt_dt_cols).filter(F.col(ts) > F.lit(effective_max_base_ts))
    
    logging.info(f"Reading from {hist_rpt_dt_table} - {ts} > {effective_max_base_ts}")

    # Prepare source data (mappings, transformations, deduplication)
    accounts_prepared = prepare_source_data(accounts_df, hist_rpt_dt_df, config)

    # Add month integer for comparison
    accounts_prepared = accounts_prepared.withColumn(
        "month_int",
        F.expr(month_to_int_expr(prt))
    )
    accounts_prepared = accounts_prepared.withColumn(
        "_is_soft_delete",
        F.coalesce(F.col(SOFT_DELETE_COLUMN), F.lit("")).isin(*SOFT_DELETE_CODES)
    )

    history_columns = [f"{rolling_col['name']}_history" for rolling_col in config.get('rolling_columns', [])]

    # Load summary metadata (small - can broadcast)
    logger.info(f"Loading summary metadata from {latest_summary_table}")
    try:
        summary_meta = spark.sql(f"""
            SELECT 
                {pk},
                {prt} as max_existing_month,
                {ts} as max_existing_ts,
                {month_to_int_expr(prt)} as max_month_int,
                {', '.join(history_columns)}
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
            F.col("s.max_month_int"),
            *[F.col(f"s.{col_name}") for col_name in history_columns]
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
            ).when(
                F.col("case_type") == "CASE_III",
                F.col("max_month_int") - F.col("month_int")
            ).otherwise(F.lit(1))
        )
        .drop("initial_case_type")
    )

    return classified


def categorize_updates(spark: SparkSession, case_iii_df, config: Dict[str, Any], expected_rows: Optional[int] = None):
    logger.info("=" * 80)
    logger.info("[Case III]: Categorization")
    logger.info("=" * 80)

    pk = config['primary_column']
    hot_window = 36

    hot_df  = case_iii_df.filter(F.col("MONTH_DIFF") <= hot_window)
    cold_df = case_iii_df.filter(F.col("MONTH_DIFF") > hot_window)

    _hot_df_exists = hot_df.isEmpty()
    _cold_df_exists = cold_df.isEmpty()

    if not _hot_df_exists and not _cold_df_exists:
        overlap_accounts = hot_df.select(pk).intersect(cold_df.select(pk)).distinct()
        has_account_overlap = overlap_accounts.limit(1).count() > 0

        if has_account_overlap:
            mixed_df = case_iii_df.join(overlap_accounts, on=pk, how="left_semi")
            hot_only_df = hot_df.join(overlap_accounts, on=pk, how="left_anti")
            cold_only_df = cold_df.join(overlap_accounts, on=pk, how="left_anti")
            logger.info("Case III split mode: account-level lanes active (mixed + hot-only + cold-only)")

            process_case_iii_hot(spark, hot_only_df, config, expected_rows=expected_rows)
            process_case_iii_cold(spark, cold_only_df, config, expected_rows=expected_rows)
            process_case_iii_mixed(spark, mixed_df, config, expected_rows=expected_rows)
        else:
            logger.info("Case III split mode: account-level lanes active (hot + cold)")
            process_case_iii_hot(spark, hot_df, config, expected_rows=expected_rows)
            process_case_iii_cold(spark, cold_df, config, expected_rows=expected_rows)            
    elif not _hot_df_exists and _cold_df_exists:
        logger.info("Case III split mode: HOT lane only")
        process_case_iii_hot(spark, hot_df, config, expected_rows=expected_rows)
    elif _hot_df_exists and not _cold_df_exists:
        logger.info("Case III split mode: COLD lane only")
        process_case_iii_cold(spark, cold_df, config, expected_rows=expected_rows)
    else:
        logger.info("Case III split mode: no records to process")
        
    return


# ─────────────────────────────────────────────────────────────────────────────
# Lane helpers — private, not called directly by callers outside this module
# ─────────────────────────────────────────────────────────────────────────────

def _backfill_hot(
    spark: SparkSession,
    df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> bool:
    return _backfill_hot_from_classified(spark, df, config, expected_rows=expected_rows)


def _backfill_hot_from_classified(
    spark: SparkSession,
    df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> bool:
    """
    Hot-lane backfill driven by classified-row latest-history context only.

    Arrays are patched by MONTH_DIFF positions and reused to derive:
      1) latest_summary history patch (full latest-history length),
      2) case_3a rows (backfill month rows),
      3) case_3b rows (future summary propagation rows),
      4) latest-month scalar patch table (MONTH_DIFF == 0).
    """
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = config["destination_table"]
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    case3_latest_patch_table = get_case3_latest_month_patch_table(config)

    if df is None or df.isEmpty():
        return True

    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    required_cols = ["max_existing_month", "max_existing_ts", "max_month_int", "MONTH_DIFF"] + history_cols
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.warning(f"_backfill_hot_from_classified: missing classified columns {missing_cols}; signalling fallback")
        return False

    latest_cols = set(get_latest_cols(config))
    if not all(c in latest_cols for c in history_cols):
        logger.warning("_backfill_hot_from_classified: latest_summary schema missing required *_history columns")
        return False

    dedup_w = Window.partitionBy(pk, "MONTH_DIFF").orderBy(F.col(ts).desc(), F.col("month_int").desc())
    dedup_df = (
        df
        .withColumn("_rn", F.row_number().over(dedup_w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    if dedup_df.isEmpty():
        return True

    split_counts = dedup_df.agg(
        F.sum(F.when(F.col("MONTH_DIFF") == 0, 1).otherwise(0)).alias("latest_count"),
        F.sum(F.when(F.col("MONTH_DIFF") > 0, 1).otherwise(0)).alias("older_count"),
    ).first()
    latest_count = int(split_counts["latest_count"] or 0)
    older_count = int(split_counts["older_count"] or 0)
    logger.info(
        f"_backfill_hot_from_classified: rows={latest_count + older_count:,} "
        f"latest_month_rows={latest_count:,} older_rows={older_count:,}"
    )

    temp_exclusions = {
        "month_int", "max_existing_month", "max_existing_ts", "max_month_int",
        "_is_soft_delete", "case_type", "MONTH_DIFF",
        "min_month_for_new_account", "count_months_for_new_account",
    }

    # latest-month scalar patch table (MONTH_DIFF == 0 only)
    if latest_count > 0:
        latest_dedup = (
            dedup_df
            .filter(F.col("MONTH_DIFF") == 0)
        )
        src_cols = [c for c in latest_dedup.columns if c not in temp_exclusions and not c.startswith("_prepared_")]
        val_cols = [F.col(rc['mapper_column']).alias(f"latest_val_{rc['name']}") for rc in rolling_columns]
        write_case_table_bucketed(
            spark=spark,
            df=latest_dedup.select(*[F.col(c) for c in src_cols], *val_cols),
            table_name=case3_latest_patch_table,
            config=config,
            stage=f"{case3_latest_patch_table.split('.')[-1]}_temp",
            expected_rows=latest_count,
        )

    # Build per-account MONTH_DIFF map from backfill rows.
    val_struct_fields = [
        F.col(f"_prepared_{rc['name']}" if f"_prepared_{rc['name']}" in dedup_df.columns else rc['mapper_column'])
        .alias(f"val_{rc['name']}")
        for rc in rolling_columns
    ]
    peer_window = Window.partitionBy(pk)
    backfill_all = dedup_df.withColumn(
        "peer_map",
        F.map_from_entries(
            F.collect_list(F.struct(F.col("MONTH_DIFF").cast("int"), F.struct(*val_struct_fields))).over(peer_window)
        ),
    )
    account_patch_base = (
        backfill_all.groupBy(pk).agg(
            F.first("max_existing_month", ignorenulls=True).alias("anchor_month"),
            F.max("max_existing_ts").alias("anchor_ts"),
            F.first("max_month_int", ignorenulls=True).alias("anchor_month_int"),
            F.max(F.col(ts)).alias("max_backfill_ts"),
            F.max("MONTH_DIFF").alias("max_month_diff"),
            F.first("peer_map", ignorenulls=True).alias("peer_map"),
            *[F.first(F.col(h), ignorenulls=True).alias(h) for h in history_cols],
        )
        .filter(F.col("anchor_month_int").isNotNull())
        .withColumn(
            "required_size",
            F.greatest(
                F.size(F.col(history_cols[0])),
                F.col("max_month_diff") + F.lit(1),
                F.lit(latest_history_len),
            ),
        )
    )
    if account_patch_base.isEmpty():
        logger.warning("_backfill_hot_from_classified: no account anchor context rows; signalling fallback")
        return False

    account_patch_base.createOrReplaceTempView("_hot_patch_base")
    hist_patch_exprs = []
    for rc in rolling_columns:
        hcol = f"{rc['name']}_history"
        vcol = f"val_{rc['name']}"
        dtype = rc.get('type', rc.get('data_type', 'String')).upper()
        hist_patch_exprs.append(f"""
            transform(
                sequence(0, required_size - 1),
                i -> CASE
                    WHEN peer_map[CAST(i AS INT)] IS NOT NULL
                    THEN CAST(peer_map[CAST(i AS INT)].{vcol} AS {dtype})
                    ELSE element_at(
                        concat({hcol}, array_repeat(CAST(NULL AS {dtype}), greatest(required_size - size({hcol}), 0))),
                        i + 1
                    )
                END
            ) AS {hcol}
        """)

    latest_hist_patch_df = spark.sql(f"""
        SELECT {pk}, anchor_month AS {prt},
               GREATEST(anchor_ts, max_backfill_ts) AS {ts},
               {', '.join(hist_patch_exprs)}
        FROM _hot_patch_base
    """)
    for rc in rolling_columns:
        latest_hist_patch_df = latest_hist_patch_df.withColumn(
            f"{rc['name']}_history",
            F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
        )
    latest_hist_patch_df = _apply_grid_columns(latest_hist_patch_df, grid_columns, history_len)
    write_case_table_bucketed(
        spark=spark,
        df=latest_hist_patch_df,
        table_name="temp_catalog.checkpointdb.case_3_latest_history_context_patch",
        config=config,
        stage="case_3_latest_history_context_patch_temp",
        expected_rows=expected_rows,
    )

    # case_3a: build backfill month rows from patched latest anchor.
    older_ctx = (
        backfill_all.filter(F.col("MONTH_DIFF") > 0).alias("c")
        .join(
            latest_hist_patch_df.select(pk, *history_cols).alias("l"),
            pk,
            "inner",
        )
        .select(
            F.col("c.*"),
            *[F.col(f"l.{h}").alias(f"patched_{h}") for h in history_cols],
        )
    )
    older_ctx.createOrReplaceTempView("_hot_older_ctx")

    exclude_base = temp_exclusions | {"peer_map"} | set(history_cols) | {c for c in dedup_df.columns if c.startswith("_prepared_")}
    base_cols = [f"c.{c}" for c in dedup_df.columns if c not in exclude_base]

    case3a_exprs = []
    for rc in rolling_columns:
        hcol = f"{rc['name']}_history"
        dtype = rc.get('type', rc.get('data_type', 'String')).upper()
        case3a_exprs.append(f"""
            transform(
                sequence(0, {history_len - 1}),
                pos -> CAST(element_at(c.patched_{hcol}, c.MONTH_DIFF + pos + 1) AS {dtype})
            ) AS {hcol}
        """)

    case3a_df = spark.sql(f"""
        SELECT {', '.join(base_cols)}, {', '.join(case3a_exprs)}
        FROM _hot_older_ctx c
    """)
    case3a_df = _apply_grid_columns(case3a_df, grid_columns, history_len)
    case3a_df = (
        case3a_df
        .withColumn("_rn", F.row_number().over(Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())))
        .filter(F.col("_rn") == 1).drop("_rn")
    )
    write_case_table_bucketed(
        spark=spark, df=case3a_df,
        table_name="temp_catalog.checkpointdb.case_3a",
        config=config, stage="case_3a_temp", expected_rows=expected_rows,
    )

    # case_3b: update future summary rows from patched latest anchor.
    account_stats = (
        dedup_df.groupBy(pk).agg(
            F.min("month_int").alias("min_backfill_int"),
            F.max("month_int").alias("max_backfill_int"),
            F.max(F.col(ts)).alias("max_backfill_ts"),
        )
    )
    summary_targets = (
        spark.table(summary_table)
        .select(pk, prt, ts)
        .join(account_stats, pk, "inner")
        .withColumn("summary_month_int", F.expr(month_to_int_expr(prt)))
        .filter(
            (F.col("summary_month_int") >= F.col("min_backfill_int"))
            & (F.col("summary_month_int") <= F.col("max_backfill_int") + F.lit(history_len - 1))
        )
    )
    future_ctx = (
        summary_targets.alias("s")
        .join(account_patch_base.select(pk, "anchor_month_int").alias("a"), pk, "inner")
        .join(latest_hist_patch_df.select(pk, *history_cols).alias("l"), pk, "inner")
        .withColumn("anchor_diff", F.col("a.anchor_month_int") - F.col("s.summary_month_int"))
        .filter(F.col("anchor_diff") >= 0)
        .select(
            F.col("s.*"),
            F.col("a.anchor_month_int").alias("anchor_month_int"),
            F.col("anchor_diff"),
            *[F.col(f"l.{h}").alias(f"patched_{h}") for h in history_cols],
        )
    )
    if not future_ctx.isEmpty():
        future_ctx.createOrReplaceTempView("_hot_future_ctx")
        case3b_exprs = []
        for rc in rolling_columns:
            hcol = f"{rc['name']}_history"
            dtype = rc.get('type', rc.get('data_type', 'String')).upper()
            case3b_exprs.append(f"""
                transform(
                    sequence(0, {history_len - 1}),
                    pos -> CAST(element_at(s.patched_{hcol}, anchor_diff + pos + 1) AS {dtype})
                ) AS {hcol}
            """)
        case3b_df = spark.sql(f"""
            SELECT s.{pk}, s.{prt},
                   GREATEST(s.{ts}, s.max_backfill_ts) AS {ts},
                   {', '.join(case3b_exprs)}
            FROM _hot_future_ctx s
        """)
        case3b_df = _apply_grid_columns(case3b_df, grid_columns, history_len)
        write_case_table_bucketed(
            spark=spark, df=case3b_df,
            table_name="temp_catalog.checkpointdb.case_3b",
            config=config, stage="case_3b_temp", expected_rows=expected_rows,
        )

    return True


def _softdelete_hot(
    spark: SparkSession,
    df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> bool:
    """
    Hot-lane soft-delete: nullify deleted month positions using latest_summary context.
    Returns False to signal the caller to fall back to the legacy path.
    """
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    case3d_latest_patch_table = get_case3d_latest_history_patch_table(config)

    if df is None or df.isEmpty():
        return True

    latest_cols = set(get_latest_cols(config))
    if not all(c in latest_cols for c in history_cols):
        return False

    delete_df = df.withColumn("delete_month_int", F.expr(month_to_int_expr(prt)))
    affected = delete_df.select(pk).distinct()

    latest_ctx = (
        spark.table(latest_summary_table)
        .select(pk, prt, ts, *history_cols)
        .join(affected, pk, "inner")
        .withColumn("latest_month_int", F.expr(month_to_int_expr(prt)))
    )
    if latest_ctx.isEmpty():
        return False

    # Only act on delete rows that actually exist in the summary month table.
    delete_months = delete_df.select(prt).distinct()
    summary_month_keys = (
        spark.table(summary_table)
        .select(pk, prt)
        .join(F.broadcast(delete_months), prt, "inner")
    )
    delete_existing = (
        delete_df.alias("d")
        .join(
            summary_month_keys.alias("s"),
            (F.col(f"d.{pk}") == F.col(f"s.{pk}")) & (F.col(f"d.{prt}") == F.col(f"s.{prt}")),
            "inner",
        )
        .select("d.*")
    )
    if delete_existing.isEmpty():
        logger.info("_softdelete_hot: no matching summary rows for soft-delete records")
        return True

    # Part A: month-row flag update (soft_del_cd + ts only, arrays unchanged)
    write_case_table_bucketed(
        spark=spark,
        df=delete_existing.select(pk, prt, ts, SOFT_DELETE_COLUMN),
        table_name="temp_catalog.checkpointdb.case_3d_month",
        config=config,
        stage="case_3d_month_temp",
        expected_rows=expected_rows,
    )

    # Part B: future-month patches — nullify deleted position in each future row
    future_candidates = (
        delete_existing.alias("d")
        .join(latest_ctx.select(pk, "latest_month_int").alias("l"), pk, "inner")
        .filter(F.col("l.latest_month_int") > F.col("d.delete_month_int"))
        .withColumn(
            "target_month_int",
            F.explode(
                F.sequence(
                    F.col("d.delete_month_int") + F.lit(1),
                    F.least(
                        F.col("d.delete_month_int") + F.lit(history_len - 1),
                        F.col("l.latest_month_int"),
                    ),
                )
            ),
        )
        .select(
            F.col(f"d.{pk}").alias(pk),
            F.col("target_month_int"),
            (F.col("target_month_int") - F.col("d.delete_month_int")).alias("delete_position"),
            F.col(f"d.{ts}").alias("delete_ts"),
        )
    )

    if not future_candidates.isEmpty():
        future_agg = (
            future_candidates
            .groupBy(pk, "target_month_int")
            .agg(
                F.collect_set("delete_position").alias("delete_positions"),
                F.max("delete_ts").alias("new_base_ts"),
            )
        )
        future_ctx = future_agg.join(latest_ctx, pk, "inner")
        future_ctx.createOrReplaceTempView("_hot_sd_future_ctx")

        target_month_expr = (
            "format_string('%04d-%02d', "
            "CAST(floor((target_month_int - 1) / 12) AS INT), "
            "CAST(((target_month_int - 1) % 12) + 1 AS INT))"
        )
        patch_exprs = []
        for rc in rolling_columns:
            hcol = f"{rc['name']}_history"
            dtype = rc.get('type', rc.get('data_type', 'String')).upper()
            patch_exprs.append(f"""
                transform(
                    sequence(0, {history_len - 1}),
                    pos -> CASE
                        WHEN array_contains(delete_positions, pos) THEN CAST(NULL AS {dtype})
                        ELSE CAST(
                            element_at({hcol}, (latest_month_int - target_month_int + pos + 1))
                            AS {dtype}
                        )
                    END
                ) AS {hcol}
            """)
        future_patch_df = spark.sql(f"""
            SELECT {pk}, {target_month_expr} AS {prt},
                   GREATEST({ts}, new_base_ts) AS {ts},
                   {', '.join(patch_exprs)}
            FROM _hot_sd_future_ctx
        """)
        future_patch_df = _apply_grid_columns(future_patch_df, grid_columns, history_len)
        write_case_table_bucketed(
            spark=spark, df=future_patch_df,
            table_name="temp_catalog.checkpointdb.case_3d_future",
            config=config, stage="case_3d_future_temp", expected_rows=expected_rows,
        )

    # Part C: latest-summary history nullification from earliest delete month onward
    delete_scope = (
        delete_existing
        .groupBy(pk)
        .agg(
            F.min("delete_month_int").alias("min_delete_month_int"),
            F.max(F.col(ts)).alias("max_delete_ts"),
        )
    )
    latest_patch_base = (
        latest_ctx.join(delete_scope, pk, "inner")
        .filter(F.col("latest_month_int") >= F.col("min_delete_month_int"))
    )
    if not latest_patch_base.isEmpty():
        latest_patch_base.createOrReplaceTempView("_hot_sd_latest_base")
        patch_hist_exprs = []
        for rc in rolling_columns:
            hcol = f"{rc['name']}_history"
            dtype = rc.get('type', rc.get('data_type', 'String')).upper()
            patch_hist_exprs.append(f"""
                transform(
                    sequence(0, {latest_history_len - 1}),
                    i -> CASE
                        WHEN (latest_month_int - i) >= min_delete_month_int THEN CAST(NULL AS {dtype})
                        ELSE element_at({hcol}, i + 1)
                    END
                ) AS {hcol}
            """)
        latest_del_patch_df = spark.sql(f"""
            SELECT {pk}, {prt},
                   GREATEST({ts}, max_delete_ts) AS {ts},
                   {', '.join(patch_hist_exprs)}
            FROM _hot_sd_latest_base
        """)
        for rc in rolling_columns:
            latest_del_patch_df = latest_del_patch_df.withColumn(
                f"{rc['name']}_history",
                F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
            )
        latest_del_patch_df = _apply_grid_columns(latest_del_patch_df, grid_columns, history_len)
        write_case_table_bucketed(
            spark=spark, df=latest_del_patch_df,
            table_name=case3d_latest_patch_table,
            config=config,
            stage=f"{case3d_latest_patch_table.split('.')[-1]}_temp",
            expected_rows=expected_rows,
        )

    return True


def _backfill_legacy(
    spark: SparkSession,
    df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
    broadcast_input: bool = False,
) -> None:
    """
    Legacy backfill path: partition-pruned summary scan, peer-map gap filling.
    Shared by cold and mixed lanes.
    """
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    latest_cols = set(get_latest_cols(config))
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    case3_latest_patch_table = get_case3_latest_month_patch_table(config)
    delete_codes_sql = ",".join([f"'{c}'" for c in SOFT_DELETE_CODES])

    if df is None or df.isEmpty():
        return

    latest_rows = df.filter(F.col("MONTH_DIFF") == 0)
    older_df = df.filter(F.col("MONTH_DIFF") > 0)
    latest_count = latest_rows.count() if not latest_rows.isEmpty() else 0
    older_count  = older_df.count() if not older_df.isEmpty() else 0

    temp_exclusions = {
        "month_int", "max_existing_month", "max_existing_ts", "max_month_int",
        "_is_soft_delete", "case_type", "MONTH_DIFF",
        "min_month_for_new_account", "count_months_for_new_account",
    }
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]

    # ── latest-month patch ────────────────────────────────────────────────
    if latest_count > 0:
        dedup_w = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
        latest_dedup = (
            latest_rows
            .withColumn("_rn", F.row_number().over(dedup_w))
            .filter(F.col("_rn") == 1).drop("_rn")
        )
        src_cols = [c for c in latest_dedup.columns if c not in temp_exclusions and not c.startswith("_prepared_")]
        val_cols = [F.col(rc['mapper_column']).alias(f"latest_val_{rc['name']}") for rc in rolling_columns]
        write_case_table_bucketed(
            spark=spark,
            df=latest_dedup.select(*[F.col(c) for c in src_cols], *val_cols),
            table_name=case3_latest_patch_table,
            config=config,
            stage=f"{case3_latest_patch_table.split('.')[-1]}_temp",
            expected_rows=latest_count,
        )

    if older_count == 0:
        return

    if broadcast_input:
        row_cap = int(config.get("cold_case3_broadcast_row_cap", 10_000_000))
        if older_count > row_cap:
            raise ValueError(
                f"_backfill_legacy broadcast guard: older_rows={older_count:,} > cap={row_cap:,}"
            )
        older_df = F.broadcast(older_df)

    # ── partition-pruned summary read ─────────────────────────────────────
    backfill_range = older_df.agg(
        F.min(prt).alias("min_b"), F.max(prt).alias("max_b")
    ).first()
    min_bi = int(backfill_range["min_b"][:4]) * 12 + int(backfill_range["min_b"][5:7])
    max_bi = int(backfill_range["max_b"][:4]) * 12 + int(backfill_range["max_b"][5:7])

    def _int_to_partition(i: int) -> str:
        y, m = divmod(i, 12)
        if m == 0:
            m, y = 12, y - 1
        return f"{y}-{m:02d}"

    earliest_partition = _int_to_partition(min_bi - history_len)
    latest_partition   = _int_to_partition(max_bi + history_len)

    affected = older_df.select(pk).distinct()
    summary_df = (
        spark.sql(f"""
            SELECT {', '.join([pk, prt, ts] + history_cols)}
            FROM {summary_table}
            WHERE {prt} >= '{earliest_partition}' AND {prt} <= '{latest_partition}'
              AND COALESCE({SOFT_DELETE_COLUMN}, '') NOT IN ({delete_codes_sql})
        """)
        .join(F.broadcast(affected), pk, "left_semi")
        .withColumn("month_int", F.expr(month_to_int_expr(prt)))
    )

    val_struct_fields = [F.col(rc['mapper_column']).alias(f"val_{rc['name']}") for rc in rolling_columns]
    peer_window = Window.partitionBy(pk)
    older_with_peers = older_df.withColumn(
        "peer_map",
        F.map_from_entries(
            F.collect_list(F.struct(F.col("month_int"), F.struct(*val_struct_fields))).over(peer_window)
        ),
    )

    older_with_peers.createOrReplaceTempView("_legacy_backfill")
    summary_df.createOrReplaceTempView("_legacy_summary")
    older_with_peers.cache()
    summary_df.cache()

    # Part A: new summary rows for backfill months
    prior_joined = spark.sql(f"""
        SELECT /*+ BROADCAST(b) */
            b.*,
            s.{prt}       AS prior_month,
            s.month_int   AS prior_month_int,
            s.{ts}        AS prior_ts,
            {', '.join([f's.{a} AS prior_{a}' for a in history_cols])},
            (b.month_int - s.month_int) AS months_since_prior,
            ROW_NUMBER() OVER (
                PARTITION BY b.{pk}, b.{prt} ORDER BY s.month_int DESC
            ) AS rn
        FROM _legacy_backfill b
        LEFT JOIN _legacy_summary s ON b.{pk} = s.{pk} AND s.month_int < b.month_int
    """)
    prior = prior_joined.filter(F.col("rn") == 1).drop("rn")
    prior.createOrReplaceTempView("_legacy_prior")

    new_row_exprs = []
    for rc in rolling_columns:
        arr = f"{rc['name']}_history"
        col = rc['mapper_column']
        vcol = f"val_{rc['name']}"
        new_row_exprs.append(f"""
            CASE
                WHEN prior_month IS NOT NULL AND months_since_prior > 0 THEN
                    slice(
                        concat(
                            array({col}),
                            CASE WHEN months_since_prior > 1 THEN
                                transform(sequence(1, months_since_prior - 1), i -> peer_map[CAST(month_int - i AS INT)].{vcol})
                            ELSE array() END,
                            transform(
                                prior_{arr},
                                (val, i) -> CASE
                                    WHEN peer_map[CAST(prior_month_int - i AS INT)] IS NOT NULL
                                    THEN peer_map[CAST(prior_month_int - i AS INT)].{vcol}
                                    ELSE val
                                END
                            )
                        ), 1, {history_len}
                    )
                ELSE
                    concat(
                        array({col}),
                        transform(sequence(1, {history_len} - 1), i -> peer_map[CAST(month_int - i AS INT)].{vcol})
                    )
            END AS {arr}
        """)

    # Drop inherited history cols from classified input; rebuilt history aliases below
    # must be the only *_history columns to avoid ambiguous references in grid derivation.
    exclude_backfill = temp_exclusions | set(history_cols) | {"prior_month", "prior_ts", "months_since_prior"} | \
                       {f"prior_{a}" for a in history_cols} | \
                       {c for c in df.columns if c.startswith("_prepared_")}
    backfill_cols = [c for c in df.columns if c not in exclude_backfill]

    case3a_df = spark.sql(f"""
        SELECT {', '.join(backfill_cols)}, {', '.join(new_row_exprs)}
        FROM _legacy_prior
    """)
    case3a_df = _apply_grid_columns(case3a_df, grid_columns, history_len)
    case3a_df = (
        case3a_df
        .withColumn("_rn", F.row_number().over(Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())))
        .filter(F.col("_rn") == 1).drop("_rn")
    )
    write_case_table_bucketed(
        spark=spark, df=case3a_df,
        table_name="temp_catalog.checkpointdb.case_3a",
        config=config, stage="case_3a_temp", expected_rows=expected_rows,
    )

    # Part B: update future summary rows
    account_stats = older_with_peers.groupBy(pk).agg(
        F.max(F.col(ts)).alias("max_backfill_ts")
    )
    backfill_value_cols = [
        f"b.{rc['mapper_column']} AS backfill_{rc['name']}" for rc in rolling_columns
    ]
    future_joined = spark.sql(f"""
        SELECT /*+ BROADCAST(b) */
            b.{pk}, b.{ts} AS backfill_ts,
            {', '.join(backfill_value_cols)},
            s.{prt} AS summary_month, s.{ts} AS summary_ts,
            {', '.join([f's.{a} AS existing_{a}' for a in history_cols])},
            (s.month_int - b.month_int) AS backfill_position
        FROM _legacy_backfill b
        JOIN _legacy_summary s ON b.{pk} = s.{pk}
        WHERE s.month_int >= b.month_int
          AND (s.month_int - b.month_int) < {history_len}
    """)

    if not future_joined.isEmpty():
        backfill_collect_cols = [f"backfill_{rc['name']}" for rc in rolling_columns]
        struct_fields = ["backfill_position"] + backfill_collect_cols
        agg_exprs = [
            F.collect_list(F.struct(*[F.col(c) for c in struct_fields])).alias("backfill_list"),
            F.max("backfill_ts").alias("new_base_ts"),
            F.first("summary_ts").alias("existing_summary_ts"),
        ]
        for rc in rolling_columns:
            agg_exprs.append(F.first(f"existing_{rc['name']}_history").alias(f"existing_{rc['name']}_history"))

        aggregated = future_joined.groupBy(pk, "summary_month").agg(*agg_exprs)
        aggregated.createOrReplaceTempView("_legacy_aggregated")

        update_exprs = []
        for rc in rolling_columns:
            arr = f"{rc['name']}_history"
            bcol = f"backfill_{rc['name']}"
            update_exprs.append(f"""
                transform(
                    existing_{arr},
                    (x, i) -> CASE
                        WHEN size(filter(backfill_list, b -> b.backfill_position = i)) > 0
                        THEN element_at(filter(backfill_list, b -> b.backfill_position = i), 1).{bcol}
                        ELSE x
                    END
                ) AS {arr}
            """)
        case3b_df = spark.sql(f"""
            SELECT {pk}, summary_month AS {prt},
                   GREATEST(existing_summary_ts, new_base_ts) AS {ts},
                   {', '.join(update_exprs)}
            FROM _legacy_aggregated
        """)
        case3b_df = _apply_grid_columns(case3b_df, grid_columns, history_len)
        write_case_table_bucketed(
            spark=spark, df=case3b_df,
            table_name="temp_catalog.checkpointdb.case_3b",
            config=config, stage="case_3b_temp", expected_rows=expected_rows,
        )

    # latest-summary history patch (if history cols exist in latest)
    if all(c in latest_cols for c in history_cols):
        latest_ctx = (
            spark.table(latest_summary_table)
            .select(pk, prt, ts, *history_cols)
            .join(affected, pk, "inner")
            .withColumn("latest_month_int", F.expr(month_to_int_expr(prt)))
        )
        account_stats2 = older_with_peers.groupBy(pk).agg(
            F.min("month_int").alias("min_backfill_int"),
            F.max("month_int").alias("max_backfill_int"),
            F.max(F.col(ts)).alias("max_backfill_ts"),
        )
        peer_per_account = older_with_peers.select(pk, "peer_map").dropDuplicates([pk])
        patch_base = (
            latest_ctx.join(peer_per_account, pk, "inner").join(account_stats2, pk, "inner")
            .withColumn(
                "required_size",
                F.greatest(
                    F.size(F.col(history_cols[0])),
                    F.col("latest_month_int") - F.col("min_backfill_int") + F.lit(1),
                    F.lit(latest_history_len),
                ),
            )
        )
        if not patch_base.isEmpty():
            patch_base.createOrReplaceTempView("_legacy_latest_patch_base")
            hist_patch_exprs = []
            for rc in rolling_columns:
                hcol = f"{rc['name']}_history"
                vcol = f"val_{rc['name']}"
                dtype = rc.get('type', rc.get('data_type', 'String')).upper()
                hist_patch_exprs.append(f"""
                    transform(
                        sequence(0, required_size - 1),
                        i -> CASE
                            WHEN peer_map[CAST(latest_month_int - i AS INT)] IS NOT NULL
                            THEN CAST(peer_map[CAST(latest_month_int - i AS INT)].{vcol} AS {dtype})
                            ELSE element_at(
                                concat({hcol}, array_repeat(CAST(NULL AS {dtype}), greatest(required_size - size({hcol}), 0))),
                                i + 1
                            )
                        END
                    ) AS {hcol}
                """)
            latest_patch_df = spark.sql(f"""
                SELECT {pk}, {prt},
                       GREATEST({ts}, max_backfill_ts) AS {ts},
                       {', '.join(hist_patch_exprs)}
                FROM _legacy_latest_patch_base
            """)
            for rc in rolling_columns:
                latest_patch_df = latest_patch_df.withColumn(
                    f"{rc['name']}_history",
                    F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
                )
            latest_patch_df = _apply_grid_columns(latest_patch_df, grid_columns, history_len)
            write_case_table_bucketed(
                spark=spark, df=latest_patch_df,
                table_name="temp_catalog.checkpointdb.case_3_latest_history_context_patch",
                config=config, stage="case_3_latest_history_context_patch_temp",
                expected_rows=expected_rows,
            )

    older_with_peers.unpersist()
    summary_df.unpersist()


def _softdelete_legacy(
    spark: SparkSession,
    df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
    broadcast_input: bool = False,
) -> None:
    """
    Legacy soft-delete path: partition-pruned summary scan to nullify positions.
    Shared by cold and mixed lanes.
    """
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    latest_cols = set(get_latest_cols(config))
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    case3d_latest_patch_table = get_case3d_latest_history_patch_table(config)

    if df is None or df.isEmpty():
        return

    # Partition-pruned summary read around delete month range
    backfill_range = df.agg(F.min(prt).alias("min_b"), F.max(prt).alias("max_b")).first()
    min_bi = int(backfill_range["min_b"][:4]) * 12 + int(backfill_range["min_b"][5:7])
    max_bi = int(backfill_range["max_b"][:4]) * 12 + int(backfill_range["max_b"][5:7])

    def _int_to_partition(i: int) -> str:
        y, m = divmod(i, 12)
        if m == 0:
            m, y = 12, y - 1
        return f"{y}-{m:02d}"

    earliest_partition = _int_to_partition(min_bi - history_len)
    latest_partition   = _int_to_partition(max_bi + history_len)
    affected = df.select(pk).distinct()

    summary_filtered = (
        spark.sql(f"""
            SELECT * FROM {summary_table}
            WHERE {prt} >= '{earliest_partition}' AND {prt} <= '{latest_partition}'
        """)
        .join(affected, pk, "left_semi")
        .withColumn("month_int", F.expr(month_to_int_expr(prt)))
    )

    delete_existing = (
        df.alias("d")
        .join(
            summary_filtered.select(pk, prt).alias("s"),
            (F.col(f"d.{pk}") == F.col(f"s.{pk}")) & (F.col(f"d.{prt}") == F.col(f"s.{prt}")),
            "inner",
        )
        .select("d.*")
    )
    if delete_existing.isEmpty():
        logger.info("_softdelete_legacy: no matching summary rows; nothing to update")
        return

    delete_existing = delete_existing.withColumn("delete_month_int", F.expr(month_to_int_expr(prt)))
    if broadcast_input:
        row_cap = int(config.get("cold_case3d_broadcast_row_cap", config.get("cold_case3_broadcast_row_cap", 10_000_000)))
        count = delete_existing.count()
        if count > row_cap:
            raise ValueError(f"_softdelete_legacy broadcast guard: rows={count:,} > cap={row_cap:,}")
        delete_existing = F.broadcast(delete_existing)

    # Part A: month-row flag update
    write_case_table_bucketed(
        spark=spark,
        df=delete_existing.select(pk, prt, ts, SOFT_DELETE_COLUMN),
        table_name="temp_catalog.checkpointdb.case_3d_month",
        config=config, stage="case_3d_month_temp", expected_rows=expected_rows,
    )

    # Part B: future-month position nullification
    summary_future = summary_filtered.select(pk, prt, ts, "month_int", *history_cols)
    future_joined = (
        delete_existing.alias("d")
        .join(
            summary_future.alias("s"),
            (F.col(f"s.{pk}") == F.col(f"d.{pk}"))
            & (F.col("s.month_int") > F.col("d.delete_month_int"))
            & ((F.col("s.month_int") - F.col("d.delete_month_int")) < F.lit(history_len)),
            "inner",
        )
        .select(
            F.col(f"d.{pk}").alias(pk),
            F.col("d.delete_ts" if "delete_ts" in delete_existing.columns else f"d.{ts}").alias("delete_ts"),
            F.col(f"s.{prt}").alias("summary_month"),
            F.col(f"s.{ts}").alias("summary_ts"),
            (F.col("s.month_int") - F.col("d.delete_month_int")).alias("delete_position"),
            *[F.col(f"s.{a}").alias(f"existing_{a}") for a in history_cols],
        )
    )

    if not future_joined.isEmpty():
        agg_exprs = [
            F.collect_set("delete_position").alias("delete_positions"),
            F.max("delete_ts").alias("new_base_ts"),
            F.first("summary_ts").alias("existing_summary_ts"),
        ]
        for a in history_cols:
            agg_exprs.append(F.first(f"existing_{a}").alias(f"existing_{a}"))
        aggregated = future_joined.groupBy(pk, "summary_month").agg(*agg_exprs)
        aggregated.createOrReplaceTempView("_legacy_sd_aggregated")

        update_exprs = [
            f"""
                transform(existing_{rc['name']}_history, (x, i) -> CASE
                    WHEN array_contains(delete_positions, i) THEN NULL ELSE x
                END) AS {rc['name']}_history
            """
            for rc in rolling_columns
        ]
        case3d_future_df = spark.sql(f"""
            SELECT {pk}, summary_month AS {prt},
                   GREATEST(existing_summary_ts, new_base_ts) AS {ts},
                   {', '.join(update_exprs)}
            FROM _legacy_sd_aggregated
        """)
        case3d_future_df = _apply_grid_columns(case3d_future_df, grid_columns, history_len)
        write_case_table_bucketed(
            spark=spark, df=case3d_future_df,
            table_name="temp_catalog.checkpointdb.case_3d_future",
            config=config, stage="case_3d_future_temp", expected_rows=expected_rows,
        )

    # Part C: latest-summary history nullification
    if all(c in latest_cols for c in history_cols):
        delete_scope = (
            delete_existing.groupBy(pk).agg(
                F.min("delete_month_int").alias("min_delete_month_int"),
                F.max(F.col(ts)).alias("max_delete_ts"),
            )
        )
        latest_ctx = (
            spark.table(latest_summary_table)
            .select(pk, prt, ts, *history_cols)
            .join(delete_scope, pk, "inner")
            .withColumn("latest_month_int", F.expr(month_to_int_expr(prt)))
            .filter(F.col("latest_month_int") >= F.col("min_delete_month_int"))
        )
        if not latest_ctx.isEmpty():
            latest_ctx.createOrReplaceTempView("_legacy_sd_latest_base")
            patch_exprs = []
            for rc in rolling_columns:
                hcol = f"{rc['name']}_history"
                dtype = rc.get('type', rc.get('data_type', 'String')).upper()
                patch_exprs.append(f"""
                    transform(sequence(0, {latest_history_len - 1}), i -> CASE
                        WHEN (latest_month_int - i) >= min_delete_month_int THEN CAST(NULL AS {dtype})
                        ELSE element_at({hcol}, i + 1)
                    END) AS {hcol}
                """)
            latest_del_patch_df = spark.sql(f"""
                SELECT {pk}, {prt},
                       GREATEST({ts}, max_delete_ts) AS {ts},
                       {', '.join(patch_exprs)}
                FROM _legacy_sd_latest_base
            """)
            for rc in rolling_columns:
                latest_del_patch_df = latest_del_patch_df.withColumn(
                    f"{rc['name']}_history",
                    F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
                )
            latest_del_patch_df = _apply_grid_columns(latest_del_patch_df, grid_columns, history_len)
            write_case_table_bucketed(
                spark=spark, df=latest_del_patch_df,
                table_name=case3d_latest_patch_table,
                config=config,
                stage=f"{case3d_latest_patch_table.split('.')[-1]}_temp",
                expected_rows=expected_rows,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Shared utility
# ─────────────────────────────────────────────────────────────────────────────

def _apply_grid_columns(df, grid_columns: list, history_len: int):
    """Derive all grid (concat_ws) columns from their source rolling history."""
    for gc in grid_columns:
        source_history = f"{gc.get('mapper_rolling_column', gc.get('source_history', ''))}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        df = df.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.slice(F.col(source_history), 1, history_len),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                ),
            ),
        )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Public lane entry points
# ─────────────────────────────────────────────────────────────────────────────

def process_case_iii_hot(
    spark: SparkSession,
    hot_df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> None:
    """
    Hot lane: accounts whose backfill/delete months all fall within the hot window.

    Uses latest_summary history arrays to avoid broad summary scans.
    Falls back transparently to the legacy summary-scan path per sub-operation
    if the latest_summary context is insufficient.

    Input df must contain both regular and soft-delete rows (discriminated
    by _is_soft_delete).  Snapshot tables are written and combined internally.
    """
    if hot_df is None or hot_df.isEmpty():
        return

    t0 = time.time()
    logger.info("=" * 80)
    logger.info("Case III HOT lane — latest_summary context path")
    logger.info("=" * 80)

    pk = config['primary_column']

    split_case_tables = {
        "temp_catalog.checkpointdb.case_3a": "case_3a_hot_temp",
        "temp_catalog.checkpointdb.case_3b": "case_3b_hot_temp",
        "temp_catalog.checkpointdb.case_3_latest_history_context_patch": "case_3_lhcp_hot_temp",
        get_case3_latest_month_patch_table(config): "case_3_lmp_hot_temp",
        "temp_catalog.checkpointdb.case_3d_month": "case_3d_month_hot_temp",
        "temp_catalog.checkpointdb.case_3d_future": "case_3d_future_hot_temp",
        get_case3d_latest_history_patch_table(config): "case_3d_lhp_hot_temp",
    }
    snapshot_groups: List[Dict[str, str]] = []

    backfill_df = hot_df.filter(~F.col("_is_soft_delete"))
    delete_df   = hot_df.filter( F.col("_is_soft_delete"))

    backfill_count = backfill_df.count() if not backfill_df.isEmpty() else 0
    delete_count   = delete_df.count()   if not delete_df.isEmpty()   else 0
    logger.info(f"Hot lane: backfill_rows={backfill_count:,} delete_rows={delete_count:,}")

    # ── backfill ──────────────────────────────────────────────────────────
    if backfill_count > 0:
        drop_case_tables(spark, split_case_tables)
        handled = _backfill_hot(spark, backfill_df, config, expected_rows=backfill_count)
        if not handled:
            logger.info("Hot backfill fell back to legacy path")
            _backfill_legacy(spark, backfill_df, config, expected_rows=backfill_count)
        snapshot_groups.append(snapshot_case_tables(spark, split_case_tables, "hot_backfill"))

    # ── soft deletes ──────────────────────────────────────────────────────
    if delete_count > 0:
        drop_case_tables(spark, split_case_tables)
        handled = _softdelete_hot(spark, delete_df, config, expected_rows=delete_count)
        if not handled:
            logger.info("Hot soft-delete fell back to legacy path")
            _softdelete_legacy(spark, delete_df, config, expected_rows=delete_count)
        snapshot_groups.append(snapshot_case_tables(spark, split_case_tables, "hot_delete"))

    combine_case_table_snapshots(
        spark=spark,
        config=config,
        table_stage_map=split_case_tables,
        snapshot_groups=snapshot_groups,
        expected_rows=expected_rows,
    )
    cleanup_snapshot_tables(spark, snapshot_groups)
    logger.info(f"Case III HOT lane done | {(time.time() - t0) / 60:.2f} min")


def process_case_iii_cold(
    spark: SparkSession,
    cold_df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> None:
    """
    Cold lane: accounts whose backfill/delete months are older than the hot window.

    Always uses the legacy summary-scan path — these months are too old to be
    covered by latest_summary arrays. Input is typically small enough to broadcast.
    """
    if cold_df is None or cold_df.isEmpty():
        return

    t0 = time.time()
    logger.info("=" * 80)
    logger.info("Case III COLD lane — legacy summary-scan path")
    logger.info("=" * 80)

    pk = config['primary_column']
    broadcast = bool(config.get("force_cold_case3_broadcast", True))

    split_case_tables = {
        "temp_catalog.checkpointdb.case_3a": "case_3a_cold_temp",
        "temp_catalog.checkpointdb.case_3b": "case_3b_cold_temp",
        "temp_catalog.checkpointdb.case_3_latest_history_context_patch": "case_3_lhcp_cold_temp",
        get_case3_latest_month_patch_table(config): "case_3_lmp_cold_temp",
        "temp_catalog.checkpointdb.case_3d_month": "case_3d_month_cold_temp",
        "temp_catalog.checkpointdb.case_3d_future": "case_3d_future_cold_temp",
        get_case3d_latest_history_patch_table(config): "case_3d_lhp_cold_temp",
    }
    snapshot_groups: List[Dict[str, str]] = []

    backfill_df = cold_df.filter(~F.col("_is_soft_delete"))
    delete_df   = cold_df.filter( F.col("_is_soft_delete"))

    backfill_count = backfill_df.count() if not backfill_df.isEmpty() else 0
    delete_count   = delete_df.count()   if not delete_df.isEmpty()   else 0
    logger.info(f"Cold lane: backfill_rows={backfill_count:,} delete_rows={delete_count:,}")

    if backfill_count > 0:
        drop_case_tables(spark, split_case_tables)
        _backfill_legacy(
            spark, backfill_df, config,
            expected_rows=backfill_count,
            broadcast_input=broadcast,
        )
        snapshot_groups.append(snapshot_case_tables(spark, split_case_tables, "cold_backfill"))

    if delete_count > 0:
        drop_case_tables(spark, split_case_tables)
        _softdelete_legacy(
            spark, delete_df, config,
            expected_rows=delete_count,
            broadcast_input=broadcast,
        )
        snapshot_groups.append(snapshot_case_tables(spark, split_case_tables, "cold_delete"))

    combine_case_table_snapshots(
        spark=spark,
        config=config,
        table_stage_map=split_case_tables,
        snapshot_groups=snapshot_groups,
        expected_rows=expected_rows,
    )
    cleanup_snapshot_tables(spark, snapshot_groups)
    logger.info(f"Case III COLD lane done | {(time.time() - t0) / 60:.2f} min")


def process_case_iii_mixed(
    spark: SparkSession,
    mixed_df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> None:
    """
    Mixed lane: accounts that have months spanning both hot and cold windows.

    Default: unified legacy summary-scan path (safe — avoids partial-context
    errors that arise when an account's peer_map spans both windows).

    Set config['mixed_use_split_context'] = True to enable a split approach:
    hot months within the account are processed via _backfill_hot, cold months
    via _backfill_legacy, results combined.  Use only when you have confirmed
    that the two sub-sets for a given account do not produce conflicting writes
    to the same summary row.
    """
    if mixed_df is None or mixed_df.isEmpty():
        return

    t0 = time.time()
    logger.info("=" * 80)
    logger.info("Case III MIXED lane")
    logger.info("=" * 80)

    pk = config['primary_column']
    prt = config['partition_column']

    split_case_tables = {
        "temp_catalog.checkpointdb.case_3a": "case_3a_mixed_temp",
        "temp_catalog.checkpointdb.case_3b": "case_3b_mixed_temp",
        "temp_catalog.checkpointdb.case_3_latest_history_context_patch": "case_3_lhcp_mixed_temp",
        get_case3_latest_month_patch_table(config): "case_3_lmp_mixed_temp",
        "temp_catalog.checkpointdb.case_3d_month": "case_3d_month_mixed_temp",
        "temp_catalog.checkpointdb.case_3d_future": "case_3d_future_mixed_temp",
        get_case3d_latest_history_patch_table(config): "case_3d_lhp_mixed_temp",
    }
    snapshot_groups: List[Dict[str, str]] = []

    use_split = bool(config.get("mixed_use_split_context", False))

    backfill_df = mixed_df.filter(~F.col("_is_soft_delete"))
    delete_df   = mixed_df.filter( F.col("_is_soft_delete"))
    backfill_count = backfill_df.count() if not backfill_df.isEmpty() else 0
    delete_count   = delete_df.count()   if not delete_df.isEmpty()   else 0
    logger.info(
        f"Mixed lane: backfill_rows={backfill_count:,} delete_rows={delete_count:,} "
        f"split_context={use_split}"
    )

    if use_split:
        # ── experimental split path ───────────────────────────────────────
        hot_window = 36

        for op_df, op_count, label, hot_fn, cold_fn in [
            (backfill_df, backfill_count, "backfill", _backfill_hot,    _backfill_legacy),
            (delete_df,   delete_count,   "delete",   _softdelete_hot,  _softdelete_legacy),
        ]:
            if op_count == 0:
                continue
            hot_sub  = op_df.filter(F.col("MONTH_DIFF") <= F.lit(hot_window))
            cold_sub = op_df.filter(F.col("MONTH_DIFF") >  F.lit(hot_window))

            drop_case_tables(spark, split_case_tables)
            if not hot_sub.isEmpty():
                handled = hot_fn(spark, hot_sub, config, expected_rows=op_count)
                if not handled:
                    logger.info(f"Mixed {label} hot sub-lane fell back to legacy")
                    cold_fn(spark, hot_sub, config, expected_rows=op_count)
            if not cold_sub.isEmpty():
                cold_fn(spark, cold_sub, config, expected_rows=op_count, broadcast_input=False)
            snapshot_groups.append(
                snapshot_case_tables(spark, split_case_tables, f"mixed_{label}_split")
            )
    else:
        # ── default: unified legacy path ──────────────────────────────────
        if backfill_count > 0:
            drop_case_tables(spark, split_case_tables)
            _backfill_legacy(spark, backfill_df, config, expected_rows=backfill_count)
            snapshot_groups.append(snapshot_case_tables(spark, split_case_tables, "mixed_backfill"))

        if delete_count > 0:
            drop_case_tables(spark, split_case_tables)
            _softdelete_legacy(spark, delete_df, config, expected_rows=delete_count)
            snapshot_groups.append(snapshot_case_tables(spark, split_case_tables, "mixed_delete"))

    combine_case_table_snapshots(
        spark=spark,
        config=config,
        table_stage_map=split_case_tables,
        snapshot_groups=snapshot_groups,
        expected_rows=expected_rows,
    )
    cleanup_snapshot_tables(spark, snapshot_groups)
    logger.info(f"Case III MIXED lane done | {(time.time() - t0) / 60:.2f} min")





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
    history_len = get_summary_history_len(config)
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
        "_is_soft_delete",
        "case_type",
        "MONTH_DIFF",
        "min_month_for_new_account",
        "count_months_for_new_account",
    ]
    # Also drop prepared columns
    drop_cols.extend([c for c in result.columns if c.startswith("_prepared_")])
    result = result.drop(*[c for c in drop_cols if c in result.columns])

    return result


def process_case_ii(spark: SparkSession, case_ii_df, config: Dict[str, Any], expected_rows: Optional[int] = None):
    """
    Process Case II from classified latest-summary anchor arrays.

    Flow:
      1) Build per-account full latest-history anchor (latest_history_len) by
         patching forward values by MONTH_DIFF relative to max_existing_month.
      2) Derive each forward row's 36-month summary history from the full anchor.
      3) Persist summary result to case_2 and full latest result to case_2_latest.
    """
    process_start_time = time.time()
    logger.info("=" * 80)
    logger.info("STEP 2b: Process Case II (Forward Entries)")
    logger.info("=" * 80)

    logger.info(f"Processing forward entries")

    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]

    required_cols = ["max_existing_month", "max_existing_ts", "max_month_int", "MONTH_DIFF"] + history_cols
    missing_cols = [c for c in required_cols if c not in case_ii_df.columns]
    if missing_cols:
        logger.warning(f"Case II missing classified context columns {missing_cols}; skipping Case II processing")
        return

    # Resolve same-account, same MONTH_DIFF collisions by latest timestamp.
    dedup_w = Window.partitionBy(pk, "MONTH_DIFF").orderBy(F.col(ts).desc(), F.col("month_int").desc())
    dedup_df = (
        case_ii_df
        .withColumn("_rn", F.row_number().over(dedup_w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    if dedup_df.isEmpty():
        logger.info("No Case II rows after dedupe")
        return

    # Skip and log unresolved/no-context rows.
    anchor_available = F.col("max_month_int").isNotNull()
    for hcol in history_cols:
        anchor_available = anchor_available & F.col(hcol).isNotNull()
    case_ii_resolved = dedup_df.filter(anchor_available)
    total_rows = dedup_df.count()
    resolved_rows = case_ii_resolved.count()
    unresolved_rows = total_rows - resolved_rows
    if unresolved_rows > 0:
        logger.warning(f"Case II unresolved rows skipped (missing latest_summary anchor context): {unresolved_rows:,}")
    if resolved_rows == 0:
        logger.warning("No Case II rows with anchor context; skipping Case II processing")
        return

    # Build per-account peer map keyed by MONTH_DIFF.
    val_struct_fields = []
    for rc in rolling_columns:
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        value_col = f"_prepared_{rc['name']}" if f"_prepared_{rc['name']}" in case_ii_resolved.columns else src_col
        val_struct_fields.append(F.col(value_col).alias(f"val_{rc['name']}"))

    peer_window = Window.partitionBy(pk)
    case_ii_with_peers = case_ii_resolved.withColumn(
        "peer_map",
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("MONTH_DIFF").cast("int"),
                    F.struct(*val_struct_fields)
                )
            ).over(peer_window)
        )
    )

    # Per-account full latest-history anchor context.
    account_anchor = (
        case_ii_with_peers.groupBy(pk).agg(
            F.first("max_existing_month", ignorenulls=True).alias("anchor_month"),
            F.max("max_existing_ts").alias("anchor_ts"),
            F.first("max_month_int", ignorenulls=True).alias("anchor_month_int"),
            F.max("MONTH_DIFF").alias("max_forward_diff"),
            F.max(F.col(ts)).alias("max_forward_ts"),
            F.first("peer_map", ignorenulls=True).alias("peer_map"),
            *[F.first(F.col(h), ignorenulls=True).alias(h) for h in history_cols],
        )
        .withColumn(
            "required_size",
            F.greatest(
                F.size(F.col(history_cols[0])) + F.col("max_forward_diff"),
                F.lit(latest_history_len),
            ),
        )
    )
    account_anchor.createOrReplaceTempView("case_ii_anchor_base")

    full_anchor_exprs = []
    for rc in rolling_columns:
        hcol = f"{rc['name']}_history"
        vcol = f"val_{rc['name']}"
        dtype = rc.get('type', rc.get('data_type', 'String')).upper()
        full_anchor_exprs.append(f"""
            transform(
                sequence(0, required_size - 1),
                i -> CASE
                    WHEN peer_map[CAST(max_forward_diff - i AS INT)] IS NOT NULL
                    THEN CAST(peer_map[CAST(max_forward_diff - i AS INT)].{vcol} AS {dtype})
                    WHEN (max_forward_diff - i) <= 0 THEN
                        element_at(
                            concat({hcol}, array_repeat(CAST(NULL AS {dtype}), greatest(required_size - size({hcol}), 0))),
                            CAST(i - max_forward_diff + 1 AS INT)
                        )
                    ELSE CAST(NULL AS {dtype})
                END
            ) AS {hcol}
        """)

    latest_month_expr = (
        "format_string('%04d-%02d', "
        "CAST(floor((anchor_month_int + max_forward_diff - 1) / 12) AS INT), "
        "CAST(((anchor_month_int + max_forward_diff - 1) % 12) + 1 AS INT))"
    )
    full_anchor_df = spark.sql(f"""
        SELECT {pk},
               max_forward_diff,
               {latest_month_expr} AS latest_forward_month,
               GREATEST(anchor_ts, max_forward_ts) AS latest_forward_ts,
               {', '.join(full_anchor_exprs)}
        FROM case_ii_anchor_base
    """)
    for rc in rolling_columns:
        full_anchor_df = full_anchor_df.withColumn(
            f"{rc['name']}_history",
            F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
        )

    full_anchor_df.createOrReplaceTempView("case_ii_full_anchor")

    # Non-history carry-through columns from input records.
    exclude_cols = {
        "month_int",
        "max_existing_month",
        "max_existing_ts",
        "max_month_int",
        "_is_soft_delete",
        "case_type",
        "MONTH_DIFF",
        "min_month_for_new_account",
        "count_months_for_new_account",
        "peer_map"
    }
    exclude_cols.update(history_cols)
    exclude_cols.update([c for c in case_ii_with_peers.columns if c.startswith("_prepared_")])
    current_cols = [c for c in case_ii_with_peers.columns if c not in exclude_cols]
    current_select = ", ".join([f"r.{col}" for col in current_cols])

    case_ii_with_peers.createOrReplaceTempView("case_ii_rows")
    summary_hist_exprs = []
    for rc in rolling_columns:
        hcol = f"{rc['name']}_history"
        dtype = rc.get('type', rc.get('data_type', 'String')).upper()
        summary_hist_exprs.append(f"""
            transform(
                sequence(0, {history_len - 1}),
                pos -> CAST(
                    element_at(a.{hcol}, (a.max_forward_diff - r.MONTH_DIFF) + pos + 1)
                    AS {dtype}
                )
            ) AS {hcol}
        """)

    result = spark.sql(f"""
        SELECT
            {current_select},
            {', '.join(summary_hist_exprs)}
        FROM case_ii_rows r
        JOIN case_ii_full_anchor a
          ON r.{pk} = a.{pk}
    """)

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

    write_case_table_bucketed(
        spark=spark,
        df=result,
        table_name="temp_catalog.checkpointdb.case_2",
        config=config,
        stage="case_2_temp",
        expected_rows=expected_rows,
    )

    # Latest-summary target rows (full latest-history arrays from patched anchor).
    latest_meta_w = Window.partitionBy(pk).orderBy(F.col("MONTH_DIFF").desc(), F.col(ts).desc(), F.col("month_int").desc())
    latest_meta = (
        case_ii_resolved
        .withColumn("_rn", F.row_number().over(latest_meta_w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    latest_meta_cols = [c for c in current_cols if c not in {prt, ts}]
    latest_case2_df = (
        full_anchor_df
        .select(
            F.col(pk),
            F.col("latest_forward_month").alias(prt),
            F.col("latest_forward_ts").alias(ts),
            *[F.col(h) for h in history_cols],
        )
        .join(latest_meta.select(pk, *[F.col(c) for c in latest_meta_cols]), on=pk, how="left")
    )
    latest_case2_df = _apply_grid_columns(latest_case2_df, grid_columns, history_len)
    write_case_table_bucketed(
        spark=spark,
        df=latest_case2_df,
        table_name="temp_catalog.checkpointdb.case_2_latest",
        config=config,
        stage="case_2_latest_temp",
        expected_rows=resolved_rows,
    )

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case II Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
    logger.info("-" * 60)

    return


def process_case_iv(spark: SparkSession, case_iv_df, case_i_result, config: Dict[str, Any], expected_rows: Optional[int] = None):
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
    history_len = get_summary_history_len(config)
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
            MIN(month_int) as min_month_int,
            MAX(month_int) as max_month_int,
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
                        "_is_soft_delete", "case_type", "MONTH_DIFF", "min_month_for_new_account", 
                        "count_months_for_new_account"])
    exclude_cols.update([f"{rc['name']}_history" for rc in rolling_columns])
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
    
    write_case_table_bucketed(
        spark=spark,
        df=result,
        table_name="temp_catalog.checkpointdb.case_4",
        config=config,
        stage="case_4_temp",
        expected_rows=expected_rows,
    )

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case IV Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
    logger.info("-" * 60)
    return


def build_latest_merge_columns(
    config: Dict[str, Any],
    source_cols: List[str],
    pk: str,
) -> Tuple[List[str], str]:
    latest_cols = get_latest_cols(config)
    shared_cols = [c for c in source_cols if c in latest_cols]
    if pk not in shared_cols:
        raise ValueError(f"Primary key '{pk}' missing from latest_summary merge columns")
    update_cols = [c for c in shared_cols if c != pk]
    update_set_expr = ", ".join([f"s.{c} = c.{c}" for c in update_cols])
    return shared_cols, update_set_expr


def write_backfill_results(spark: SparkSession, config: Dict[str, Any], expected_rows_append: Optional[int] = None):
    summary_table = config['destination_table']
    latest_summary_table = config['latest_history_table']
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    grid_cols = [gc['name'] for gc in grid_columns]
    case3_latest_month_patch_tables = [
        CASE3_LATEST_MONTH_PATCH_TABLE,
        CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE,
    ]
    case3d_latest_history_patch_tables = [
        CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE,
        CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE,
    ]

    try:
        logger.info("-" * 60)
        logger.info("MERGING RECORDS:")
        process_start_time = time.time()

        case_3a_df = None
        case_3b_filtered_df = None
        case_3_latest_month_patch_df = None
        case_3d_month_df = None
        case_3d_future_df = None
        case_3d_latest_history_patch_df = None
        month_chunks: List[List[str]] = []

        def _read_union_if_exists(table_names: List[str]):
            dfs = [spark.read.table(t) for t in table_names if spark.catalog.tableExists(t)]
            if not dfs:
                return None
            merged = dfs[0]
            for d in dfs[1:]:
                merged = merged.unionByName(d, allowMissingColumns=True)
            return merged

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3a"):
            case_3a_df = spark.read.table('temp_catalog.checkpointdb.case_3a')

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3b"):
            case_3b_df = spark.read.table('temp_catalog.checkpointdb.case_3b')
            if case_3a_df is not None:
                case_3b_filtered_df = case_3b_df.join(case_3a_df.select(pk, prt), [pk, prt], "left_anti")
            else:
                case_3b_filtered_df = case_3b_df

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3d_month"):
            case_3d_month_df = spark.read.table("temp_catalog.checkpointdb.case_3d_month")

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3d_future"):
            case_3d_future_df = spark.read.table("temp_catalog.checkpointdb.case_3d_future")

        case_3_latest_month_patch_df = _read_union_if_exists(case3_latest_month_patch_tables)
        case_3d_latest_history_patch_df = _read_union_if_exists(case3d_latest_history_patch_tables)

        if case_3a_df is not None or case_3b_filtered_df is not None:
            case3b_weight = float(config.get("case3_merge_case3b_weight", 1.3))
            overflow_ratio = float(config.get("case3_merge_overflow_ratio", 0.10))
            logger.info(
                f"Case III merge chunking enabled "
                f"(case3b_weight={case3b_weight}, overflow_ratio={overflow_ratio})"
            )

            month_load_df = None
            if case_3a_df is not None:
                month_load_df = (
                    case_3a_df.groupBy(prt)
                    .count()
                    .withColumnRenamed("count", "case3a_count")
                )

            if case_3b_filtered_df is not None:
                case_3b_month_load = (
                    case_3b_filtered_df.groupBy(prt)
                    .count()
                    .withColumnRenamed("count", "case3b_count")
                )
                if month_load_df is None:
                    month_load_df = case_3b_month_load.withColumn("case3a_count", F.lit(0))
                else:
                    month_load_df = month_load_df.join(case_3b_month_load, prt, "full_outer")

            month_load_df = (
                month_load_df
                .na.fill(0, ["case3a_count", "case3b_count"])
                .withColumn(
                    "weighted_load",
                    F.col("case3a_count") + (F.lit(case3b_weight) * F.col("case3b_count")),
                )
            )

            month_weights = [
                (r[prt], float(r["weighted_load"]))
                for r in month_load_df.select(prt, "weighted_load").collect()
                if r[prt] is not None
            ]

            month_chunks = build_balanced_month_chunks(month_weights, overflow_ratio=overflow_ratio)

            if month_chunks:
                logger.info(f"Case III merge month chunks: {len(month_chunks)}")
                for idx, months in enumerate(month_chunks, 1):
                    logger.info(f"  Chunk {idx}: months={months}")

        if case_3a_df is not None:
            if not month_chunks:
                month_chunks = [[r[prt]] for r in case_3a_df.select(prt).distinct().orderBy(prt).collect()]
            case_3a_month_set = {
                r[prt] for r in case_3a_df.select(prt).distinct().collect() if r[prt] is not None
            }

            for idx, months in enumerate(month_chunks, 1):
                case_3a_chunk_months = [m for m in months if m in case_3a_month_set]
                if not case_3a_chunk_months:
                    continue

                case_3a_chunk_df = case_3a_df.filter(F.col(prt).isin(case_3a_chunk_months))
                case_3a_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3a_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3a_chunk_{idx}",
                )
                case_3a_chunk_df.createOrReplaceTempView("case_3a_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3a_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """
                )
                logger.info(
                    f"MERGED - CASE III-A chunk {idx}/{len(month_chunks)} "
                    f"(months={case_3a_chunk_months})"
                )
        
        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"MERGED - CASE III-A (NEW summary rows for backfill months with inherited history)| Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)

        process_start_time = time.time()

        if case_3b_filtered_df is not None:
            update_cols = list(dict.fromkeys([ts] + history_cols + grid_cols))

            update_set_exprs = []
            for col_name in update_cols:
                if col_name == ts:
                    update_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                elif col_name in history_cols:
                    update_set_exprs.append(
                        f"s.{col_name} = {summary_history_trim_expr(col_name, summary_history_len)}"
                    )
                else:
                    update_set_exprs.append(f"s.{col_name} = c.{col_name}")

            if not month_chunks:
                month_chunks = [[r[prt]] for r in case_3b_filtered_df.select(prt).distinct().orderBy(prt).collect()]
            case_3b_month_set = {
                r[prt] for r in case_3b_filtered_df.select(prt).distinct().collect() if r[prt] is not None
            }

            for idx, months in enumerate(month_chunks, 1):
                case_3b_chunk_months = [m for m in months if m in case_3b_month_set]
                if not case_3b_chunk_months:
                    continue

                case_3b_chunk_df = case_3b_filtered_df.filter(F.col(prt).isin(case_3b_chunk_months))
                case_3b_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3b_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3b_chunk_{idx}",
                )
                case_3b_chunk_df.createOrReplaceTempView("case_3b_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3b_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET {', '.join(update_set_exprs)}
                    """
                )
                logger.info(
                    f"MERGED - CASE III-B chunk {idx}/{len(month_chunks)} "
                    f"(months={case_3b_chunk_months})"
                )

        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Updated Summary | MERGED - CASE III-B (future summary rows with backfill data) | Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)

        process_start_time = time.time()
        if case_3_latest_month_patch_df is not None:
            latest_value_cols = [f"latest_val_{rc['name']}" for rc in rolling_columns]
            summary_cols_set = set(get_summary_cols(config))

            scalar_update_cols = [
                c for c in case_3_latest_month_patch_df.columns
                if c in summary_cols_set
                and c not in set([pk, prt, ts] + history_cols + grid_cols + latest_value_cols)
            ]

            history_update_sql_map: Dict[str, str] = {}
            for rc in rolling_columns:
                array_name = f"{rc['name']}_history"
                latest_val_col = f"latest_val_{rc['name']}"
                data_type = rc.get('type', rc.get('data_type', 'String'))
                history_update_sql_map[array_name] = (
                    f"transform(s.{array_name}, (x, i) -> CASE "
                    f"WHEN i = 0 THEN CAST(c.{latest_val_col} AS {data_type.upper()}) "
                    f"ELSE x END)"
                )

            update_set_exprs = [f"s.{ts} = GREATEST(s.{ts}, c.{ts})"]
            for col_name in scalar_update_cols:
                update_set_exprs.append(f"s.{col_name} = c.{col_name}")
            for array_name in history_cols:
                update_set_exprs.append(f"s.{array_name} = {history_update_sql_map[array_name]}")

            for gc in grid_columns:
                source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
                source_history = f"{source_rolling}_history"
                placeholder = gc.get('placeholder', '?').replace("'", "''")
                separator = gc.get('seperator', gc.get('separator', '')).replace("'", "''")
                source_expr = history_update_sql_map.get(source_history, f"s.{source_history}")
                grid_expr = (
                    f"concat_ws('{separator}', "
                    f"transform({source_expr}, x -> coalesce(cast(x as string), '{placeholder}')))"
                )
                update_set_exprs.append(f"s.{gc['name']} = {grid_expr}")

            case_3_latest_patch_overflow = float(
                config.get("case3_latest_month_merge_overflow_ratio", config.get("case3_merge_overflow_ratio", 0.10))
            )
            case_3_latest_patch_chunks = build_month_chunks_from_df(
                case_3_latest_month_patch_df,
                prt,
                overflow_ratio=case_3_latest_patch_overflow,
            )
            logger.info(f"Case III Latest-Month Patch merge chunks: {len(case_3_latest_patch_chunks)}")
            for idx, months in enumerate(case_3_latest_patch_chunks, 1):
                case_3_latest_patch_chunk_df = case_3_latest_month_patch_df.filter(F.col(prt).isin(months))
                case_3_latest_patch_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3_latest_patch_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3_latest_month_patch_chunk_{idx}",
                )
                case_3_latest_patch_chunk_df.createOrReplaceTempView("case_3_latest_month_patch_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3_latest_month_patch_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED AND c.{ts} >= s.{ts}
                        THEN UPDATE SET {', '.join(update_set_exprs)}
                    """
                )
                logger.info(
                    f"MERGED - CASE III Latest-Month Patch chunk {idx}/{len(case_3_latest_patch_chunks)} "
                    f"(months={months})"
                )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated Summary | MERGED - CASE III Latest-Month Patch | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        process_start_time = time.time()
        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3_latest_history_context_patch"):
            latest_case_3_history_df = spark.read.table("temp_catalog.checkpointdb.case_3_latest_history_context_patch")
            latest_case_3_history_df = align_history_arrays_to_length(
                latest_case_3_history_df,
                rolling_columns,
                latest_history_len,
            )
            latest_shared_cols, latest_update_set_expr = build_latest_merge_columns(
                config,
                latest_case_3_history_df.columns,
                pk,
            )
            latest_case_3_history_df.select(*latest_shared_cols).createOrReplaceTempView("latest_case_3_history_patch")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3_history_patch c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        c.{prt} > s.{prt}
                        OR (c.{prt} = s.{prt} AND c.{ts} >= s.{ts})
                    ) THEN UPDATE SET {latest_update_set_expr}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated latest_summary | MERGE - CASE III history patches | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3d_month_df is not None:
            case_3d_month_overflow = float(
                config.get("case3d_month_merge_overflow_ratio", config.get("case3_merge_overflow_ratio", 0.10))
            )
            case_3d_month_chunks = build_month_chunks_from_df(
                case_3d_month_df,
                prt,
                overflow_ratio=case_3d_month_overflow,
            )
            logger.info(f"Case III Soft Delete Month merge chunks: {len(case_3d_month_chunks)}")
            for idx, months in enumerate(case_3d_month_chunks, 1):
                case_3d_month_chunk_df = case_3d_month_df.filter(F.col(prt).isin(months))
                case_3d_month_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3d_month_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3d_month_chunk_{idx}",
                )
                case_3d_month_chunk_df.createOrReplaceTempView("case_3d_month_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3d_month_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET
                            s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN},
                            s.{ts} = GREATEST(s.{ts}, c.{ts})
                    """
                )
                logger.info(
                    f"MERGED - CASE III Soft Delete Month chunk {idx}/{len(case_3d_month_chunks)} "
                    f"(months={months})"
                )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE III Soft Delete Month Rows | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3d_future_df is not None:
            soft_delete_update_cols = list(dict.fromkeys([ts] + history_cols + grid_cols))
            soft_delete_set_exprs = []
            for col_name in soft_delete_update_cols:
                if col_name == ts:
                    soft_delete_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                else:
                    soft_delete_set_exprs.append(f"s.{col_name} = c.{col_name}")

            case_3d_future_overflow = float(
                config.get("case3d_future_merge_overflow_ratio", config.get("case3_merge_overflow_ratio", 0.10))
            )
            case_3d_future_chunks = build_month_chunks_from_df(
                case_3d_future_df,
                prt,
                overflow_ratio=case_3d_future_overflow,
            )
            logger.info(f"Case III Soft Delete Future merge chunks: {len(case_3d_future_chunks)}")
            for idx, months in enumerate(case_3d_future_chunks, 1):
                case_3d_future_chunk_df = case_3d_future_df.filter(F.col(prt).isin(months))
                case_3d_future_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3d_future_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3d_future_chunk_{idx}",
                )
                case_3d_future_chunk_df.createOrReplaceTempView("case_3d_future_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3d_future_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET {', '.join(soft_delete_set_exprs)}
                    """
                )
                logger.info(
                    f"MERGED - CASE III Soft Delete Future chunk {idx}/{len(case_3d_future_chunks)} "
                    f"(months={months})"
                )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE III Soft Delete Future Patches | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        logger.info("MERGING NEW/BULK RECORDS:")
        process_start_time = time.time()

        append_tables = ["temp_catalog.checkpointdb.case_1","temp_catalog.checkpointdb.case_4"]

        dfs = []
        for t in append_tables:
            if spark.catalog.tableExists(t):
                dfs.append(spark.read.table(t))

        if dfs:
            append_df = reduce(lambda a, b: a.unionByName(b), dfs)

            merge_window = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
            append_merge_df = (
                append_df
                .withColumn("_rn", F.row_number().over(merge_window))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            case_1_4_overflow = float(config.get("case1_4_merge_overflow_ratio", 0.10))
            case_1_4_chunks = build_month_chunks_from_df(
                append_merge_df,
                prt,
                overflow_ratio=case_1_4_overflow,
            )
            logger.info(f"Case I/IV summary merge chunks: {len(case_1_4_chunks)}")

            for idx, months in enumerate(case_1_4_chunks, 1):
                case_1_4_chunk_df = append_merge_df.filter(F.col(prt).isin(months))
                case_1_4_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_1_4_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_1_4_chunk_{idx}",
                    expected_rows=expected_rows_append,
                )
                case_1_4_chunk_df.createOrReplaceTempView("case_1_4_merge_chunk")

                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_1_4_merge_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED AND c.{ts} >= s.{ts} THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """
                )
                logger.info(
                    f"MERGED - CASE I/IV chunk {idx}/{len(case_1_4_chunks)} "
                    f"(months={months})"
                )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE I & IV (New Records + Bulk Historical) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

            logger.info("UPDATING LATEST SUMMARY:")
            process_start_time = time.time()
            # Get Latest from append_df - case I & IV for appending to latest_summary
            window_spec = Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())
            latest_append_df = (
                append_merge_df
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
            latest_append_df = align_history_arrays_to_length(
                latest_append_df,
                rolling_columns,
                latest_history_len,
            )
            latest_cols_set = set(get_latest_cols(config))
            latest_shared_cols = [c for c in latest_append_df.columns if c in latest_cols_set]
            if pk not in latest_shared_cols:
                raise ValueError(f"Primary key '{pk}' missing from latest_summary merge columns")

            latest_update_set_exprs = []
            for col_name in latest_shared_cols:
                if col_name == pk:
                    continue
                if col_name in history_cols:
                    latest_update_set_exprs.append(
                        f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                    )
                else:
                    latest_update_set_exprs.append(f"s.{col_name} = c.{col_name}")

            latest_append_df.select(*latest_shared_cols).createOrReplaceTempView("latest_case_1_4")

            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_1_4 c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        s.{prt} < c.{prt}
                        OR (s.{prt} = c.{prt} AND s.{ts} <= c.{ts})
                    ) THEN UPDATE SET {', '.join(latest_update_set_exprs)}
                    WHEN NOT MATCHED THEN INSERT ({', '.join(latest_shared_cols)})
                    VALUES ({', '.join([f'c.{col}' for col in latest_shared_cols])})
                """
            )

            logger.info(f"Updated latest_summary | MERGED - CASE I & IV (New Records + Bulk Historical) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)
        else:
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"No Case I/IV records to merge | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        logger.info("UPDATING LATEST SUMMARY FROM CASE III:")
        process_start_time = time.time()

        update_cols = list(dict.fromkeys([prt, ts] + history_cols + grid_cols))
        case3_select_cols = [pk] + update_cols

        case_3b_latest_df = None
        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3b"):
            case_3b_latest_df = spark.read.table("temp_catalog.checkpointdb.case_3b").select(*case3_select_cols)
            case_3b_latest_df = (
                case_3b_latest_df
                .withColumn("_rn", F.row_number().over(Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

        case_3a_latest_df = None
        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3a"):
            case_3a_latest_df = spark.read.table("temp_catalog.checkpointdb.case_3a").select(*case3_select_cols)
            if case_3b_latest_df is not None:
                case_3a_latest_df = case_3a_latest_df.join(
                    case_3b_latest_df.select(pk).distinct(),
                    [pk],
                    "left_anti",
                )
            case_3a_latest_df = (
                case_3a_latest_df
                .withColumn("_rn", F.row_number().over(Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

        case3_latest_df = None
        if case_3b_latest_df is not None:
            case3_latest_df = case_3b_latest_df
        if case_3a_latest_df is not None:
            case3_latest_df = case_3a_latest_df if case3_latest_df is None else case3_latest_df.unionByName(case_3a_latest_df)

        if case3_latest_df is not None:
            case3_latest_df = (
                case3_latest_df
                .withColumn("_rn", F.row_number().over(Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
            case3_latest_df.createOrReplaceTempView("latest_case_3")

            update_set_exprs = []
            for col_name in update_cols:
                if col_name == ts:
                    update_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                elif col_name in history_cols:
                    update_set_exprs.append(
                        f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                    )
                else:
                    update_set_exprs.append(f"s.{col_name} = c.{col_name}")

            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3 c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        s.{prt} < c.{prt}
                        OR (s.{prt} = c.{prt} AND s.{ts} <= c.{ts})
                    ) THEN UPDATE SET {', '.join(update_set_exprs)}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated latest_summary | MERGE - CASE III candidates | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)
        else:
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"No Case III candidates for latest_summary | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3_latest_month_patch_df is not None:
            latest_value_cols = [f"latest_val_{rc['name']}" for rc in rolling_columns]
            latest_cols_set = set(get_latest_cols(config))

            latest_scalar_update_cols = [
                c for c in case_3_latest_month_patch_df.columns
                if c in latest_cols_set
                and c not in set([pk, prt, ts] + history_cols + grid_cols + latest_value_cols)
            ]

            latest_history_update_sql_map: Dict[str, str] = {}
            for rc in rolling_columns:
                array_name = f"{rc['name']}_history"
                latest_val_col = f"latest_val_{rc['name']}"
                data_type = rc.get('type', rc.get('data_type', 'String'))
                latest_history_update_sql_map[array_name] = (
                    f"transform(sequence(0, {latest_history_len - 1}), i -> CASE "
                    f"WHEN i = 0 THEN CAST(c.{latest_val_col} AS {data_type.upper()}) "
                    f"ELSE element_at(s.{array_name}, i + 1) END)"
                )

            latest_update_set_exprs = [f"s.{ts} = GREATEST(s.{ts}, c.{ts})"]
            for col_name in latest_scalar_update_cols:
                latest_update_set_exprs.append(f"s.{col_name} = c.{col_name}")
            for array_name in history_cols:
                if array_name in latest_cols_set:
                    latest_update_set_exprs.append(f"s.{array_name} = {latest_history_update_sql_map[array_name]}")

            for gc in grid_columns:
                if gc['name'] not in latest_cols_set:
                    continue
                source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
                source_history = f"{source_rolling}_history"
                placeholder = gc.get('placeholder', '?').replace("'", "''")
                separator = gc.get('seperator', gc.get('separator', '')).replace("'", "''")
                source_expr = latest_history_update_sql_map.get(source_history, f"s.{source_history}")
                grid_expr = (
                    f"concat_ws('{separator}', "
                    f"transform(slice({source_expr}, 1, {summary_history_len}), "
                    f"x -> coalesce(cast(x as string), '{placeholder}')))"
                )
                latest_update_set_exprs.append(f"s.{gc['name']} = {grid_expr}")

            case_3_latest_month_patch_df.createOrReplaceTempView("latest_case_3_latest_month_patch")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3_latest_month_patch c
                    ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                    WHEN MATCHED AND c.{ts} >= s.{ts}
                    THEN UPDATE SET {', '.join(latest_update_set_exprs)}
                """
            )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated latest_summary | MERGE - CASE III Latest-Month Patch | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        # Apply soft-delete future array/grid patches to latest_summary
        # only when latest row month matches the patched summary month.
        process_start_time = time.time()
        if case_3d_future_df is not None:
            latest_soft_delete_cols = list(dict.fromkeys([ts] + history_cols + grid_cols))
            latest_soft_delete_set_exprs = []
            for col_name in latest_soft_delete_cols:
                if col_name == ts:
                    latest_soft_delete_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                elif col_name in history_cols:
                    latest_soft_delete_set_exprs.append(
                        f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                    )
                else:
                    latest_soft_delete_set_exprs.append(f"s.{col_name} = c.{col_name}")

            # Guard against accidental duplicate source keys for latest merge.
            latest_case_3d_future_df = (
                case_3d_future_df
                .select(pk, prt, *latest_soft_delete_cols)
                .dropDuplicates([pk, prt])
            )
            latest_case_3d_future_df.createOrReplaceTempView("latest_case_3d_future")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3d_future c
                    ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                    WHEN MATCHED
                    THEN UPDATE SET {', '.join(latest_soft_delete_set_exprs)}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated latest_summary | MERGE - CASE III Soft Delete Future Patches | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3d_latest_history_patch_df is not None:
            latest_case_3d_history_df = case_3d_latest_history_patch_df
            latest_case_3d_history_df = align_history_arrays_to_length(
                latest_case_3d_history_df,
                rolling_columns,
                latest_history_len,
            )
            latest_shared_cols, latest_update_set_expr = build_latest_merge_columns(
                config,
                latest_case_3d_history_df.columns,
                pk,
            )
            latest_case_3d_history_df.select(*latest_shared_cols).createOrReplaceTempView("latest_case_3d_history_patch")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3d_history_patch c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        c.{prt} > s.{prt}
                        OR (c.{prt} = s.{prt} AND c.{ts} >= s.{ts})
                    ) THEN UPDATE SET {latest_update_set_expr}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated latest_summary | MERGE - CASE III soft-delete history patches | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        # Apply month-row soft-delete flags to latest_summary when latest row month was deleted.
        process_start_time = time.time()
        if case_3d_month_df is not None:
            latest_cols = set(get_latest_cols(config))
            if SOFT_DELETE_COLUMN in latest_cols:
                latest_case_3d_month_df = (
                    case_3d_month_df
                    .select(pk, prt, ts, SOFT_DELETE_COLUMN)
                    .dropDuplicates([pk, prt])
                )
                latest_case_3d_month_df.createOrReplaceTempView("latest_case_3d_month")
                spark.sql(
                    f"""
                        MERGE INTO {latest_summary_table} s
                        USING latest_case_3d_month c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET
                            s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN},
                            s.{ts} = GREATEST(s.{ts}, c.{ts})
                    """
                )
                process_end_time = time.time()
                process_total_minutes = (process_end_time - process_start_time) / 60
                logger.info(
                    f"Updated latest_summary | MERGE - CASE III Soft Delete Month Flags | "
                    f"Time Elapsed: {process_total_minutes:.2f} minutes"
                )
                logger.info("-" * 60)

        # Reconstruct latest_summary when the latest month itself was soft-deleted.
        process_start_time = time.time()
        if case_3d_month_df is not None:
            case_3d_month_df.select(pk, prt).distinct().createOrReplaceTempView("case_3d_deleted_months")
            delete_codes_sql = ",".join([f"'{code}'" for code in SOFT_DELETE_CODES])

            deleted_latest_accounts = spark.sql(
                f"""
                    SELECT l.{pk} as {pk}
                    FROM {latest_summary_table} l
                    JOIN case_3d_deleted_months d
                      ON l.{pk} = d.{pk}
                     AND l.{prt} = d.{prt}
                    GROUP BY l.{pk}
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
                                    PARTITION BY s.{pk}
                                    ORDER BY s.{prt} DESC, s.{ts} DESC
                                ) as _rn
                            FROM {summary_table} s
                            JOIN deleted_latest_accounts a ON s.{pk} = a.{pk}
                            WHERE COALESCE(s.{SOFT_DELETE_COLUMN}, '') NOT IN ({delete_codes_sql})
                        ) x
                        WHERE _rn = 1
                    """
                ).drop("_rn")

                if not replacement_df.isEmpty():
                    latest_cols_set = set(get_latest_cols(config))
                    replacement_cols = [c for c in replacement_df.columns if c in latest_cols_set]
                    if pk not in replacement_cols:
                        raise ValueError(f"Primary key '{pk}' missing from latest_summary replacement columns")
                    replacement_update_exprs = []
                    for col_name in replacement_cols:
                        if col_name == pk:
                            continue
                        if col_name in history_cols:
                            replacement_update_exprs.append(
                                f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                            )
                        else:
                            replacement_update_exprs.append(f"s.{col_name} = c.{col_name}")
                    replacement_df.select(*replacement_cols).createOrReplaceTempView("latest_delete_replacements")
                    spark.sql(
                        f"""
                            MERGE INTO {latest_summary_table} s
                            USING latest_delete_replacements c
                            ON s.{pk} = c.{pk}
                            WHEN MATCHED THEN UPDATE SET {', '.join(replacement_update_exprs)}
                        """
                    )
                    logger.info("Updated latest_summary | Reconstructed deleted-latest accounts")
                else:
                    logger.info("No latest_summary reconstruction candidates after soft deletes")
            else:
                logger.info("No latest_summary rows pointed to deleted months")

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Latest reconstruction for soft deletes | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)


    except Exception as e:
        logger.error(f"BACKFILL MERGE FAILED: {e}", exc_info=True)
        raise


def write_forward_results(spark: SparkSession, config: Dict[str, Any], expected_rows: Optional[int] = None):
    summary_table = config['destination_table']
    latest_summary_table = config["latest_history_table"]
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
        
    try:
        logger.info("-" * 60)
        logger.info("MERGING FORWARD RECORDS:")
        process_start_time = time.time()

        if spark.catalog.tableExists('temp_catalog.checkpointdb.case_2'):
            case_2_df = spark.read.table('temp_catalog.checkpointdb.case_2')
            merge_window = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
            case_2_merge_df = (
                case_2_df
                .withColumn("_rn", F.row_number().over(merge_window))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            case_2_overflow = float(config.get("case2_merge_overflow_ratio", 0.10))
            case_2_chunks = build_month_chunks_from_df(
                case_2_merge_df,
                prt,
                overflow_ratio=case_2_overflow,
            )
            logger.info(f"Case II summary merge chunks: {len(case_2_chunks)}")

            for idx, months in enumerate(case_2_chunks, 1):
                case_2_chunk_df = case_2_merge_df.filter(F.col(prt).isin(months))
                case_2_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_2_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_2_chunk_{idx}",
                    expected_rows=expected_rows,
                )
                case_2_chunk_df.createOrReplaceTempView("case_2_summary_chunk")

                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_2_summary_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED AND c.{ts} >= s.{ts} THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """
                )
                logger.info(
                    f"MERGED - CASE II chunk {idx}/{len(case_2_chunks)} "
                    f"(months={months})"
                )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE II (Forward Records) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)


            process_start_time = time.time()
            latest_case2_full_exists = spark.catalog.tableExists("temp_catalog.checkpointdb.case_2_latest")
            if latest_case2_full_exists:
                latest_case_2_df = spark.read.table("temp_catalog.checkpointdb.case_2_latest")
                logger.info("Case II latest merge source: case_2_latest (full latest-history anchor)")
            else:
                latest_case_2_df = case_2_merge_df
                logger.info("Case II latest merge source: case_2 (summary history + preserved tail)")

            window_spec = Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())
            latest_case_2_df = (
                latest_case_2_df
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
            if latest_case_2_df.isEmpty():
                logger.info("No Case II latest rows available for latest_summary merge")
                process_end_time = time.time()
                process_total_minutes = (process_end_time - process_start_time) / 60
                logger.info(f"Skipped latest_summary update for Case II | Time Elapsed: {process_total_minutes:.2f} minutes")
                logger.info("-" * 60)
                return

            latest_case_2_df = align_history_arrays_to_length(
                latest_case_2_df,
                rolling_columns,
                latest_history_len,
            )
            latest_cols_set = set(get_latest_cols(config))
            latest_shared_cols = [c for c in latest_case_2_df.columns if c in latest_cols_set]
            if pk not in latest_shared_cols:
                raise ValueError(f"Primary key '{pk}' missing from latest_summary merge columns")
            latest_update_set_exprs = []
            for col_name in latest_shared_cols:
                if col_name == pk:
                    continue
                if col_name in history_cols:
                    if latest_case2_full_exists:
                        latest_update_set_exprs.append(f"s.{col_name} = c.{col_name}")
                    else:
                        latest_update_set_exprs.append(
                            f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                        )
                else:
                    latest_update_set_exprs.append(f"s.{col_name} = c.{col_name}")
            latest_case_2_df.select(*latest_shared_cols).createOrReplaceTempView("case_2")

            spark.sql(
                f"""           
                    MERGE INTO {latest_summary_table} s
                    USING case_2 c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        c.{prt} > s.{prt}
                        OR (c.{prt} = s.{prt} AND c.{ts} >= s.{ts})
                    ) THEN UPDATE SET {', '.join(latest_update_set_exprs)}
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

    run_id = f"summary_inc_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
    run_start_states: Optional[Dict[str, Dict[str, Any]]] = None

    try:
        preload_run_table_columns(spark, config)
        run_start_states = log_current_snapshot_state(spark, config, label="Pre-run")
        mark_run_started(spark, config, run_id=run_id, start_states=run_start_states)
        process_start_time = time.time()    

        # Step 1: Load and classify
        classified = load_and_classify_accounts(spark, config)
        classified.persist(StorageLevel.DISK_ONLY)
        spark.sql("DROP TABLE IF EXISTS execution_catalog.checkpointdb.classified")
        classified.repartition(500).writeTo("execution_catalog.checkpointdb.classified").create()

        case_stats = classified.groupBy("case_type", "_is_soft_delete").count().collect()
        case_breakdown: Dict[str, Dict[str, int]] = {}
        soft_delete_count = 0
        for row in case_stats:
            case_type = row["case_type"]
            is_delete = bool(row["_is_soft_delete"])
            count = int(row["count"])
            if case_type not in case_breakdown:
                case_breakdown[case_type] = {"normal": 0, "delete": 0}
            if is_delete:
                case_breakdown[case_type]["delete"] += count
                soft_delete_count += count
            else:
                case_breakdown[case_type]["normal"] += count

        logger.info("-" * 60)
        logger.info("CLASSIFICATION RESULTS:")
        for case_type in sorted(case_breakdown.keys()):
            total_count = case_breakdown[case_type]["normal"] + case_breakdown[case_type]["delete"]
            delete_count = case_breakdown[case_type]["delete"]
            if delete_count > 0:
                logger.info(f"  {case_type}: {total_count:,} records (soft-delete={delete_count:,})")
            else:
                logger.info(f"  {case_type}: {total_count:,} records")
        logger.info("-" * 60)

        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Classification | Time Elapsed: {process_total_minutes:.2f} minutes")

        stats['case_i_records'] = case_breakdown.get('CASE_I', {}).get("normal", 0) + case_breakdown.get('CASE_I', {}).get("delete", 0)
        stats['case_ii_records'] = case_breakdown.get('CASE_II', {}).get("normal", 0) + case_breakdown.get('CASE_II', {}).get("delete", 0)
        stats['case_iii_records'] = case_breakdown.get('CASE_III', {}).get("normal", 0) + case_breakdown.get('CASE_III', {}).get("delete", 0)
        stats['case_iv_records'] = case_breakdown.get('CASE_IV', {}).get("normal", 0) + case_breakdown.get('CASE_IV', {}).get("delete", 0)
        stats['total_records'] = (
            stats['case_i_records'] + stats['case_ii_records'] + stats['case_iii_records'] + stats['case_iv_records']
        )
        logger.info(f"Soft-delete rows in batch: {soft_delete_count:,}")

        # =========================================================================
        # STEP 2: PROCESS EACH CASE AND WRITE TO TEMP TABLE
        # Process in CORRECT order: Backfill -> New -> Bulk Historical -> Forward
        # =========================================================================

        # 2a. Process Case III (Backfill) - HIGHEST PRIORITY
        case_iii_df_all = classified.filter(F.col("case_type") == "CASE_III")
        case_iii_all_count = case_breakdown.get("CASE_III", {}).get("normal", 0) + case_breakdown.get("CASE_III", {}).get("delete", 0)
        if case_iii_all_count > 0:
            logger.info(f"\n>>> PROCESSING BACKFILL ({case_iii_all_count:,} records)")
            categorize_updates(spark, case_iii_df_all, config, expected_rows=case_iii_all_count)



        # 2b. Process Case I (New Accounts - first month only)
        case_i_df = classified.filter((F.col("case_type") == "CASE_I") & (~F.col("_is_soft_delete")))
        case_i_count = case_breakdown.get("CASE_I", {}).get("normal", 0)
        case_i_result = None
        if case_i_count > 0:
            process_start_time = time.time()
            logger.info(f"\n>>> PROCESSING NEW ACCOUNTS ({case_i_count:,} records)")
            case_i_result = process_case_i(case_i_df, config)
            case_i_result.persist(StorageLevel.MEMORY_AND_DISK)
            write_case_table_bucketed(
                spark=spark,
                df=case_i_result,
                table_name="temp_catalog.checkpointdb.case_1",
                config=config,
                stage="case_1_temp",
                expected_rows=case_i_count,
            )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Case I Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        # 2c. Process Case IV (Bulk Historical) - after Case I so we have first months
        case_iv_df = classified.filter((F.col("case_type") == "CASE_IV") & (~F.col("_is_soft_delete")))
        case_iv_count = case_breakdown.get("CASE_IV", {}).get("normal", 0)
        if case_iv_count > 0:
            logger.info(f"\n>>> PROCESSING BULK HISTORICAL ({case_iv_count:,} records)")
            if case_i_result is not None:
                process_case_iv(spark, case_iv_df, case_i_result, config, expected_rows=case_iv_count)
                case_i_result.unpersist()
                case_i_result = None
            else:
                logger.warning("Skipping Case IV processing due to missing active Case I base rows")

        write_backfill_results(
            spark,
            config,
            expected_rows_append=(case_i_count + case_iv_count)
        )

        # 2d. Process Case II (Forward Entries) - LOWEST PRIORITY
        case_ii_df = classified.filter((F.col("case_type") == "CASE_II") & (~F.col("_is_soft_delete")))
        case_ii_count = case_breakdown.get("CASE_II", {}).get("normal", 0)
        if case_ii_count > 0:
            logger.info(f"\n>>> PROCESSING FORWARD ENTRIES ({case_ii_count:,} records)")
            process_case_ii(spark, case_ii_df, config, expected_rows=case_ii_count)
            write_forward_results(spark, config, expected_rows=case_ii_count)

        logger.info("PROCESS COMPLETED - Deleting the persisted classification results")
        classified.unpersist()
        finalize_run_tracking(
            spark=spark,
            config=config,
            run_id=run_id,
            start_states=run_start_states,
            success=True,
            error_message=None,
        )
        log_current_snapshot_state(spark, config, label="Post-run")

    except Exception as e:
        failure_message = str(e)
        try:
            if run_start_states is not None:
                rollback_statuses = rollback_tables_to_run_start(
                    spark=spark,
                    config=config,
                    start_states=run_start_states,
                )
                rollback_status_message = "; ".join(
                    [f"{k}={v}" for k, v in sorted(rollback_statuses.items())]
                )
                if rollback_status_message:
                    failure_message = f"{failure_message} | rollback: {rollback_status_message}"
                finalize_run_tracking(
                    spark=spark,
                    config=config,
                    run_id=run_id,
                    start_states=run_start_states,
                    success=False,
                    error_message=failure_message,
                )
            else:
                refresh_watermark_tracker(spark, config, mark_committed=False)
        except Exception as tracker_error:
            logger.warning(f"Failed to refresh watermark tracker after pipeline error: {tracker_error}")
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


def cleanup(spark: SparkSession):
    cleanup_tables = [
        'case_1',
        'case_2',
        'case_2_latest',
        'case_3a',
        'case_3b',
        'case_3_latest_month_patch',
        'case_3_unified_latest_month_patch',
        'case_3_latest_history_context_patch',
        'case_3d_month',
        'case_3d_future',
        'case_3d_latest_history_context_patch',
        'case_3d_unified_latest_history_patch',
        'case_4',
    ]

    for table in cleanup_tables:
        spark.sql(f"DROP TABLE IF EXISTS temp_catalog.checkpointdb.{table}")
        logger.info(f"Cleaned {table}")

    logger.info("CLEANUP COMPLETED")
    logger.info("=" * 60)


def main():
    start_time = time.time()
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

