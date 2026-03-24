import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F


MAIN_DIR = os.path.abspath(os.path.dirname(__file__))


def create_spark_session(app_name: str, spark_config: dict) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in (spark_config or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def month_to_int_expr(col_name: str) -> str:
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def to_sql_scalar_type(rc_type: str) -> str:
    t = (rc_type or "STRING").strip().upper()
    if t in ("INT", "INTEGER"):
        return "INT"
    if t in ("BIGINT", "LONG"):
        return "BIGINT"
    if t in ("DOUBLE", "FLOAT", "DECIMAL"):
        return "DOUBLE"
    return "STRING"


def resolve_source_col(summary_cols: set, rolling_col_cfg: dict) -> str:
    mapper = rolling_col_cfg.get("mapper_column")
    name = rolling_col_cfg.get("name")
    if mapper and mapper in summary_cols:
        return mapper
    if name and name in summary_cols:
        return name
    raise ValueError(
        f"No source column found in summary for rolling column '{name}'. "
        f"Checked mapper_column='{mapper}' and name='{name}'."
    )


def generate_latest_summary_from_summary(spark: SparkSession, config: dict, output_table: str):
    pk = config["primary_column"]
    prt = config["partition_column"]
    ts = config["max_identifier_column"]
    summary_table = config["destination_table"]
    history_len = int(config.get("history_length", 36))
    rolling_columns = config.get("rolling_columns", [])
    grid_columns = config.get("grid_columns", [])
    unbounded_suffix = config.get("unbounded_history_suffix", "_history_unbounded")

    summary_all = spark.table(summary_table)
    summary_cols = set(summary_all.columns)

    source_col_by_name = {
        rc["name"]: resolve_source_col(summary_cols, rc)
        for rc in rolling_columns
    }

    dedupe_w = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
    summary_dedup = (
        summary_all
        .withColumn("_rn", F.row_number().over(dedupe_w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    latest_w = Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())
    latest_rows = (
        summary_dedup
        .withColumn("_rn", F.row_number().over(latest_w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    maps_src = (
        summary_dedup
        .select(pk, prt, *source_col_by_name.values())
        .withColumn("month_int", F.expr(month_to_int_expr(prt)))
    )

    map_aggs = [
        F.min("month_int").alias("min_month_int"),
        F.max("month_int").alias("max_month_int"),
    ]

    for rc in rolling_columns:
        rc_name = rc["name"]
        src_col = source_col_by_name[rc_name]
        map_aggs.append(
            F.map_from_entries(
                F.collect_list(F.struct(F.col("month_int"), F.col(src_col)))
            ).alias(f"value_map_{rc_name}")
        )

    maps = maps_src.groupBy(pk).agg(*map_aggs)
    result_df = latest_rows.join(maps, on=pk, how="inner")

    for rc in rolling_columns:
        col_name = rc["name"]
        scalar_type = to_sql_scalar_type(rc.get("type", rc.get("data_type", "STRING")))
        bounded_col = f"{col_name}_history"
        unbounded_col = f"{col_name}{unbounded_suffix}"

        result_df = result_df.withColumn(
            unbounded_col,
            F.expr(
                f"transform(sequence(max_month_int, min_month_int, -1), "
                f"mo -> CAST(value_map_{col_name}[mo] AS {scalar_type}))"
            ),
        )
        result_df = result_df.withColumn(
            bounded_col,
            F.expr(
                f"transform(sequence(1, {history_len}), i -> "
                f"CAST(element_at({unbounded_col}, i) AS {scalar_type}))"
            ),
        )

    for gc in grid_columns:
        grid_name = gc["name"]
        source_rolling = gc.get("mapper_rolling_column", gc.get("source_history", ""))
        source_history = source_rolling if source_rolling.endswith("_history") else f"{source_rolling}_history"
        if source_history not in result_df.columns:
            continue

        placeholder = gc.get("placeholder", "?")
        separator = gc.get("seperator", gc.get("separator", ""))
        grid_len = 36 if grid_name == "payment_history_grid" else history_len

        result_df = result_df.withColumn(
            grid_name,
            F.concat_ws(
                separator,
                F.transform(
                    F.sequence(F.lit(1), F.lit(grid_len)),
                    lambda i: F.coalesce(F.element_at(F.col(source_history), i).cast("string"), F.lit(placeholder)),
                ),
            ),
        )

    drop_cols = ["min_month_int", "max_month_int"] + [f"value_map_{rc['name']}" for rc in rolling_columns]
    result_df = result_df.drop(*drop_cols)

    temp_view = "latest_from_summary_new_table"
    result_df.createOrReplaceTempView(temp_view)
    spark.sql(f"CREATE OR REPLACE TABLE {output_table} USING iceberg AS SELECT * FROM {temp_view}")


def main():
    parser = argparse.ArgumentParser(
        description="Standalone builder: generate latest_summary-style table from summary with bounded and unbounded arrays."
    )
    parser.add_argument(
        "--config",
        default=os.path.join(MAIN_DIR, "config.json"),
        help="Path to config JSON (default: main/config.json)",
    )
    parser.add_argument(
        "--output-table",
        default=None,
        help="Output table name to create/replace. Default: <latest_history_table>_from_summary",
    )
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    default_out = f"{config['latest_history_table']}_from_summary"
    output_table = args.output_table or default_out

    spark = create_spark_session(
        (config.get("spark", {}).get("app_name", "SummaryPipeline") + "_generate_latest_summary_table"),
        config.get("spark", {}),
    )
    try:
        generate_latest_summary_from_summary(spark, config, output_table)
        print(f"Created table: {output_table}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
