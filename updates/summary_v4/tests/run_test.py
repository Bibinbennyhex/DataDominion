#!/usr/bin/env python3
"""
End-to-End Test Runner for Summary Pipeline v4.0
=================================================

Runs a complete test cycle:
1. Setup tables
2. Generate test data
3. Run forward processing
4. Validate results
5. Run backfill test
6. Validate final state

Usage:
    spark-submit run_test.py --config summary_config.json [options]

Author: DataDominion Team
"""

import json
import argparse
import logging
import sys
import time
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Import our modules
from setup_tables import setup_all_tables, get_accounts_all_schema
from generate_test_data import generate_test_dataset, write_to_iceberg
from summary_pipeline import SummaryConfig, SummaryPipeline, create_spark_session

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def validate_summary(spark: SparkSession, config: SummaryConfig) -> bool:
    """Validate summary table results."""
    logger.info("=" * 60)
    logger.info("Validating Results")
    logger.info("=" * 60)
    
    all_passed = True
    
    # Check 1: No duplicates
    logger.info("Check 1: No duplicates...")
    dup_check = spark.sql(f"""
        SELECT {config.primary_column}, {config.partition_column}, COUNT(*) as cnt
        FROM {config.destination_table}
        GROUP BY {config.primary_column}, {config.partition_column}
        HAVING COUNT(*) > 1
    """)
    dup_count = dup_check.count()
    if dup_count > 0:
        logger.error(f"FAILED: Found {dup_count} duplicate keys")
        dup_check.show(10)
        all_passed = False
    else:
        logger.info("PASSED: No duplicates")
    
    # Check 2: Array lengths
    logger.info("Check 2: Array lengths...")
    for rolling_col in config.rolling_columns:
        history_col = f"{rolling_col['name']}_history"
        expected_len = rolling_col['num_cols']
        
        bad_arrays = spark.sql(f"""
            SELECT {config.primary_column}, {config.partition_column}, 
                   SIZE({history_col}) as arr_len
            FROM {config.destination_table}
            WHERE SIZE({history_col}) != {expected_len}
        """)
        
        bad_count = bad_arrays.count()
        if bad_count > 0:
            logger.error(f"FAILED: {bad_count} rows with wrong {history_col} length")
            bad_arrays.show(5)
            all_passed = False
        else:
            logger.info(f"PASSED: All {history_col} arrays have length {expected_len}")
    
    # Check 3: Latest summary consistency
    logger.info("Check 3: Latest summary consistency...")
    latest_check = spark.sql(f"""
        SELECT s.{config.primary_column}
        FROM (
            SELECT {config.primary_column}, MAX({config.partition_column}) as max_month
            FROM {config.destination_table}
            GROUP BY {config.primary_column}
        ) s
        LEFT JOIN {config.latest_history_table} l
            ON s.{config.primary_column} = l.{config.primary_column}
        WHERE l.{config.primary_column} IS NULL
    """)
    
    missing = latest_check.count()
    if missing > 0:
        logger.error(f"FAILED: {missing} accounts missing from latest_summary")
        all_passed = False
    else:
        logger.info("PASSED: Latest summary consistent")
    
    # Stats
    logger.info("\nSummary Statistics:")
    summary_count = spark.table(config.destination_table).count()
    latest_count = spark.table(config.latest_history_table).count()
    months = spark.table(config.destination_table) \
        .select(config.partition_column).distinct().count()
    accounts = spark.table(config.destination_table) \
        .select(config.primary_column).distinct().count()
    
    logger.info(f"  Summary rows: {summary_count:,}")
    logger.info(f"  Latest summary rows: {latest_count:,}")
    logger.info(f"  Months: {months}")
    logger.info(f"  Unique accounts: {accounts:,}")
    
    return all_passed


def run_test(
    spark: SparkSession,
    config: SummaryConfig,
    num_accounts: int = 1000,
    num_months: int = 6,
    start_month_str: str = "2024-01"
):
    """Run complete test cycle."""
    
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("SUMMARY PIPELINE v4.0 - END-TO-END TEST")
    logger.info("=" * 60)
    logger.info(f"Accounts: {num_accounts}")
    logger.info(f"Months: {num_months}")
    logger.info(f"Start month: {start_month_str}")
    logger.info("=" * 60)
    
    start_month = datetime.strptime(start_month_str, '%Y-%m').date().replace(day=1)
    end_month = start_month
    for _ in range(num_months - 1):
        end_month = (end_month.replace(day=28) + relativedelta(days=4)).replace(day=1)
    
    # Step 1: Setup tables
    logger.info("\n[STEP 1] Setting up tables...")
    setup_all_tables(spark, config._config, drop_existing=True)
    
    # Step 2: Generate test data
    logger.info("\n[STEP 2] Generating test data...")
    records = generate_test_dataset(
        num_accounts=num_accounts,
        num_months=num_months,
        start_month=start_month,
        gap_pct=0.05,
        backfill_pct=0.02
    )
    write_to_iceberg(spark, records, config.source_table)
    
    # Step 3: Run forward processing
    logger.info("\n[STEP 3] Running forward processing...")
    pipeline = SummaryPipeline(spark, config)
    pipeline.run_forward(start_month, end_month, is_initial=True)
    
    # Step 4: Validate
    logger.info("\n[STEP 4] Validating results...")
    passed = validate_summary(spark, config)
    
    # Step 5: Backfill test (add more late-arriving data)
    logger.info("\n[STEP 5] Testing backfill...")
    
    # Generate some backfill records (2 months ago, arriving now)
    backfill_month = (start_month.replace(day=28) + relativedelta(months=2)).replace(day=1)
    backfill_records = generate_test_dataset(
        num_accounts=100,  # Smaller set for backfill
        num_months=1,
        start_month=backfill_month,
        gap_pct=0,
        backfill_pct=0
    )
    
    # Write with later timestamp to simulate late arrival
    for r in backfill_records:
        r['base_ts'] = datetime.now()
    
    write_to_iceberg(spark, backfill_records, config.source_table)
    
    # Run backfill mode
    pipeline2 = SummaryPipeline(spark, config)
    pipeline2.run_backfill(
        source_filter=f"base_ts > (SELECT MAX(base_ts) FROM {config.destination_table})"
    )
    
    # Final validation
    logger.info("\n[STEP 6] Final validation...")
    passed = validate_summary(spark, config) and passed
    
    # Summary
    total_time = time.time() - start_time
    logger.info("\n" + "=" * 60)
    if passed:
        logger.info("ALL TESTS PASSED!")
    else:
        logger.error("SOME TESTS FAILED!")
    logger.info(f"Total time: {total_time:.1f} seconds")
    logger.info("=" * 60)
    
    return passed


def main():
    parser = argparse.ArgumentParser(description='Run end-to-end test')
    parser.add_argument('--config', type=str, required=True, help='Config JSON path')
    parser.add_argument('--accounts', type=int, default=1000, help='Number of accounts')
    parser.add_argument('--months', type=int, default=6, help='Number of months')
    parser.add_argument('--start-month', type=str, default='2024-01', help='Start month')
    
    args = parser.parse_args()
    
    # Load config
    config = SummaryConfig(args.config)
    
    # Create Spark
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        passed = run_test(
            spark, config,
            num_accounts=args.accounts,
            num_months=args.months,
            start_month_str=args.start_month
        )
        
        sys.exit(0 if passed else 1)
        
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
