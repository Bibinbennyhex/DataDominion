import argparse
import json
import logging
from typing import Tuple

import summary_inc as pipeline


logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def load_local_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_fqn(table_name: str) -> Tuple[str, str, str]:
    parts = table_name.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"Expected 3-part table name <catalog>.<namespace>.<table>, got: {table_name}"
        )
    return parts[0], parts[1], parts[2]


def create_table_from_template(
    spark,
    target_table: str,
    template_table: str,
    partition_spec_sql: str,
    drop_existing: bool,
) -> None:
    if drop_existing and spark.catalog.tableExists(target_table):
        logger.info(f"Dropping existing table: {target_table}")
        spark.sql(f"DROP TABLE {target_table}")

    if spark.catalog.tableExists(target_table):
        logger.info(f"Table already exists, skipping: {target_table}")
        return

    logger.info(f"Creating table: {target_table}")
    spark.sql(
        f"""
        CREATE TABLE {target_table}
        USING iceberg
        PARTITIONED BY ({partition_spec_sql})
        AS SELECT * FROM {template_table} WHERE 1 = 0
        """
    )

    # Helpful defaults for merge-heavy workloads.
    spark.sql(
        f"""
        ALTER TABLE {target_table} SET TBLPROPERTIES (
            'write.distribution-mode' = 'hash',
            'write.merge.distribution-mode' = 'hash'
        )
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create Iceberg accounts_all/summary/latest_summary tables with "
            "partition/bucket spec using template schemas."
        )
    )
    parser.add_argument("--config-file", default="main/config.json", help="Path to config JSON")
    parser.add_argument(
        "--target-catalog",
        default=None,
        help="Target catalog for created tables (default: catalog from destination_table)",
    )
    parser.add_argument(
        "--target-namespace",
        default=None,
        help="Target namespace/database (default: namespace from destination_table)",
    )
    parser.add_argument(
        "--accounts-table-name",
        default="accounts_all",
        help="Target accounts table name",
    )
    parser.add_argument(
        "--summary-table-name",
        default="summary",
        help="Target summary table name",
    )
    parser.add_argument(
        "--latest-table-name",
        default="latest_summary",
        help="Target latest_summary table name",
    )
    parser.add_argument(
        "--accounts-template",
        default=None,
        help="Template source for accounts schema (default: config source_table)",
    )
    parser.add_argument(
        "--summary-template",
        default=None,
        help="Template source for summary schema (default: config destination_table)",
    )
    parser.add_argument(
        "--latest-template",
        default=None,
        help="Template source for latest_summary schema (default: config latest_history_table)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop target tables first if they already exist",
    )
    args = parser.parse_args()

    config = load_local_config(args.config_file)
    if not pipeline.validate_config(config):
        raise ValueError("Invalid config")

    dest_catalog, dest_namespace, _ = parse_fqn(config["destination_table"])
    target_catalog = args.target_catalog or dest_catalog
    target_namespace = args.target_namespace or dest_namespace

    accounts_target = f"{target_catalog}.{target_namespace}.{args.accounts_table_name}"
    summary_target = f"{target_catalog}.{target_namespace}.{args.summary_table_name}"
    latest_target = f"{target_catalog}.{target_namespace}.{args.latest_table_name}"

    accounts_template = args.accounts_template or config["source_table"]
    summary_template = args.summary_template or config["destination_table"]
    latest_template = args.latest_template or config["latest_history_table"]

    spark = pipeline.create_spark_session(config["spark"]["app_name"], config["spark"])
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target_catalog}.{target_namespace}")

        create_table_from_template(
            spark=spark,
            target_table=accounts_target,
            template_table=accounts_template,
            partition_spec_sql="rpt_as_of_mo",
            drop_existing=args.drop_existing,
        )
        create_table_from_template(
            spark=spark,
            target_table=summary_target,
            template_table=summary_template,
            partition_spec_sql="rpt_as_of_mo, bucket(64, cons_acct_key)",
            drop_existing=args.drop_existing,
        )
        create_table_from_template(
            spark=spark,
            target_table=latest_target,
            template_table=latest_template,
            partition_spec_sql="bucket(64, cons_acct_key)",
            drop_existing=args.drop_existing,
        )

        logger.info("Created/validated tables:")
        logger.info(f"- {accounts_target}  [PARTITIONED BY (rpt_as_of_mo)]")
        logger.info(
            f"- {summary_target}   [PARTITIONED BY (rpt_as_of_mo, bucket(64, cons_acct_key))]"
        )
        logger.info(
            f"- {latest_target}    [PARTITIONED BY (bucket(64, cons_acct_key))]"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
