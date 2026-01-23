#!/usr/bin/env python3
"""
Table Setup Script for Summary Pipeline v4.0
=============================================

Creates the source (accounts_all), summary, and latest_summary tables
with production-matching schema.

Usage:
    spark-submit setup_tables.py --config summary_config.json [--drop-existing]

Author: DataDominion Team
"""

import json
import argparse
import logging
import sys
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, IntegerType, StringType,
    DateType, TimestampType, ArrayType, DecimalType
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================
# Schema Definitions
# ============================================================

def get_accounts_all_schema() -> StructType:
    """
    Schema for accounts_all source table.
    Matches production ivaps_consumer_accounts_all structure.
    """
    return StructType([
        # Primary identifiers
        StructField("cons_acct_key", LongType(), False),
        StructField("bureau_mbr_id", StringType(), True),
        
        # Account type and terms
        StructField("port_type_cd", StringType(), True),
        StructField("acct_type_dtl_cd", StringType(), True),
        StructField("pymt_terms_cd", StringType(), True),
        StructField("pymt_terms_dtl_cd", StringType(), True),
        
        # Dates
        StructField("acct_open_dt", DateType(), True),
        StructField("acct_closed_dt", DateType(), True),
        StructField("acct_dt", DateType(), True),
        StructField("last_pymt_dt", DateType(), True),
        StructField("schd_pymt_dt", DateType(), True),
        StructField("orig_pymt_due_dt", DateType(), True),
        StructField("write_off_dt", DateType(), True),
        
        # Status codes
        StructField("acct_stat_cd", StringType(), True),
        StructField("acct_pymt_stat_cd", StringType(), True),
        StructField("acct_pymt_stat_dtl_cd", StringType(), True),
        
        # Amounts
        StructField("acct_credit_ext_am", IntegerType(), True),  # credit_limit_am
        StructField("acct_bal_am", IntegerType(), True),         # balance_am
        StructField("past_due_am", IntegerType(), True),
        StructField("actual_pymt_am", IntegerType(), True),      # actual_payment_am
        StructField("next_schd_pymt_am", IntegerType(), True),   # emi_amt
        StructField("write_off_am", IntegerType(), True),
        
        # 4IN fields
        StructField("asset_class_cd_4in", StringType(), True),
        StructField("days_past_due_ct_4in", IntegerType(), True),
        StructField("high_credit_am_4in", IntegerType(), True),
        StructField("cash_limit_am_4in", IntegerType(), True),
        StructField("collateral_am_4in", IntegerType(), True),
        StructField("total_write_off_am_4in", IntegerType(), True),
        StructField("principal_write_off_am_4in", IntegerType(), True),
        StructField("settled_am_4in", IntegerType(), True),
        StructField("interest_rate_4in", DecimalType(10, 4), True),
        StructField("suit_filed_wilful_def_stat_cd_4in", StringType(), True),
        StructField("wo_settled_stat_cd_4in", StringType(), True),
        
        # Collateral
        StructField("collateral_cd", StringType(), True),
        
        # Partition and timestamp
        StructField("rpt_as_of_mo", StringType(), False),
        StructField("base_ts", TimestampType(), True),
    ])


def get_summary_schema(config: Dict) -> StructType:
    """
    Schema for summary table.
    Includes all mapped columns + rolling history arrays.
    """
    fields = []
    
    # Primary key
    fields.append(StructField("cons_acct_key", LongType(), False))
    
    # All mapped columns (destination names)
    string_cols = [
        "bureau_member_id", "portfolio_rating_type_cd", "acct_type_dtl_cd",
        "pymt_terms_cd", "pymt_terms_dtl_cd", "acct_stat_cd",
        "acct_pymt_stat_cd", "acct_pymt_stat_dtl_cd", "collateral_cd",
        "asset_class_cd", "suit_filed_willful_dflt", "written_off_and_settled_status"
    ]
    
    date_cols = [
        "open_dt", "closed_dt", "acct_dt", "last_payment_dt",
        "schd_pymt_dt", "orig_pymt_due_dt", "dflt_status_dt"
    ]
    
    int_cols = [
        "credit_limit_am", "balance_am", "past_due_am", "actual_payment_am",
        "emi_amt", "write_off_am", "days_past_due", "hi_credit_am",
        "cash_limit_am", "collateral_am", "charge_off_am",
        "principal_write_off_am", "settled_am", "orig_loan_am"
    ]
    
    for col in string_cols:
        fields.append(StructField(col, StringType(), True))
    
    for col in date_cols:
        fields.append(StructField(col, DateType(), True))
    
    for col in int_cols:
        fields.append(StructField(col, IntegerType(), True))
    
    # Interest rate (decimal)
    fields.append(StructField("interest_rate", DecimalType(10, 4), True))
    
    # Rolling history columns
    history_length = config.get('history_length', 36)
    
    for rolling_col in config.get('rolling_columns', []):
        name = rolling_col['name']
        dtype = rolling_col['type']
        
        if dtype == "Integer":
            array_type = ArrayType(IntegerType())
        else:
            array_type = ArrayType(StringType())
        
        fields.append(StructField(f"{name}_history", array_type, True))
    
    # Grid columns
    for grid_col in config.get('grid_columns', []):
        fields.append(StructField(grid_col['name'], StringType(), True))
    
    # Partition column
    fields.append(StructField("rpt_as_of_mo", StringType(), False))
    
    # Timestamps
    fields.append(StructField("base_ts", TimestampType(), True))
    
    return StructType(fields)


def get_latest_summary_schema(config: Dict) -> StructType:
    """
    Schema for latest_summary table.
    Subset of summary with rolling history columns.
    """
    fields = []
    
    # Primary key
    fields.append(StructField("cons_acct_key", LongType(), False))
    
    # Primary date column
    fields.append(StructField("acct_dt", DateType(), True))
    
    # Partition column
    fields.append(StructField("rpt_as_of_mo", StringType(), False))
    
    # Max identifier
    fields.append(StructField("base_ts", TimestampType(), True))
    
    # Latest history addon columns (subset of summary columns)
    addon_string = [
        "bureau_member_id", "acct_pymt_stat_cd", "acct_type_dtl_cd",
        "acct_stat_cd", "pymt_terms_cd", "asset_class_cd",
        "suit_filed_willful_dflt", "written_off_and_settled_status",
        "portfolio_rating_type_cd"
    ]
    
    addon_date = ["open_dt", "closed_dt", "dflt_status_dt", "last_payment_dt"]
    
    addon_int = [
        "emi_amt", "balance_am", "days_past_due", "past_due_am",
        "charge_off_am", "actual_payment_am", "credit_limit_am", "orig_loan_am"
    ]
    
    for col in addon_string:
        fields.append(StructField(col, StringType(), True))
    
    for col in addon_date:
        fields.append(StructField(col, DateType(), True))
    
    for col in addon_int:
        fields.append(StructField(col, IntegerType(), True))
    
    # Interest rate
    fields.append(StructField("interest_rate", DecimalType(10, 4), True))
    
    # Grid column
    fields.append(StructField("payment_history_grid", StringType(), True))
    
    # Rolling history columns
    for rolling_col in config.get('rolling_columns', []):
        name = rolling_col['name']
        dtype = rolling_col['type']
        
        if dtype == "Integer":
            array_type = ArrayType(IntegerType())
        else:
            array_type = ArrayType(StringType())
        
        fields.append(StructField(f"{name}_history", array_type, True))
    
    return StructType(fields)


# ============================================================
# Table Creation
# ============================================================

def create_iceberg_table(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    partition_cols: List[str] = None,
    drop_existing: bool = False
):
    """Create an Iceberg table with the given schema."""
    
    if drop_existing:
        logger.info(f"Dropping table if exists: {table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    # Check if table exists
    parts = table_name.split('.')
    db_name = parts[-2] if len(parts) >= 2 else "default"
    tbl_name = parts[-1]
    
    if spark.catalog.tableExists(table_name):
        logger.info(f"Table {table_name} already exists, skipping creation")
        return
    
    # Create empty DataFrame with schema
    df = spark.createDataFrame([], schema)
    
    # Write as Iceberg table
    writer = df.writeTo(table_name).using("iceberg")
    
    if partition_cols:
        for col in partition_cols:
            writer = writer.partitionedBy(col)
    
    writer.create()
    logger.info(f"Created table: {table_name}")


def setup_all_tables(spark: SparkSession, config: Dict, drop_existing: bool = False):
    """Create all required tables."""
    
    source_table = config['source_table']
    summary_table = config['destination_table']
    latest_table = config['latest_history_table']
    partition_col = config['partition_column']
    
    logger.info("=" * 60)
    logger.info("Setting up tables for Summary Pipeline v4.0")
    logger.info("=" * 60)
    
    # 1. Create accounts_all source table
    logger.info(f"\n1. Creating source table: {source_table}")
    accounts_schema = get_accounts_all_schema()
    create_iceberg_table(
        spark, source_table, accounts_schema,
        partition_cols=[partition_col],
        drop_existing=drop_existing
    )
    
    # 2. Create summary table
    logger.info(f"\n2. Creating summary table: {summary_table}")
    summary_schema = get_summary_schema(config)
    create_iceberg_table(
        spark, summary_table, summary_schema,
        partition_cols=[partition_col],
        drop_existing=drop_existing
    )
    
    # 3. Create latest_summary table
    logger.info(f"\n3. Creating latest_summary table: {latest_table}")
    latest_schema = get_latest_summary_schema(config)
    create_iceberg_table(
        spark, latest_table, latest_schema,
        partition_cols=None,  # Not partitioned for faster lookups
        drop_existing=drop_existing
    )
    
    logger.info("\n" + "=" * 60)
    logger.info("Table setup complete!")
    logger.info("=" * 60)


# ============================================================
# Main
# ============================================================

def create_spark_session() -> SparkSession:
    """Create Spark session for table setup."""
    return SparkSession.builder \
        .appName("SummaryPipeline_TableSetup") \
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .getOrCreate()


def main():
    parser = argparse.ArgumentParser(description='Setup tables for Summary Pipeline')
    parser.add_argument('--config', type=str, required=True, help='Config JSON path')
    parser.add_argument('--drop-existing', action='store_true', help='Drop existing tables')
    
    args = parser.parse_args()
    
    # Load config
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        setup_all_tables(spark, config, drop_existing=args.drop_existing)
    except Exception as e:
        logger.error(f"Setup failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
