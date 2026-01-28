"""
Main script to run backfill operations.
Production-ready entry point for Iceberg history backfill.
"""

import sys
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import HISTORY_COLUMNS, MAX_HISTORY_MONTHS
from backfill_infrastructure import setup_infrastructure, insert_sample_backfill_data
from backfill_processor import IcebergBackfillProcessor
from backfill_merge import execute_backfill_merge
from backfill_batch import BatchBackfillOrchestrator, run_parallel_backfills
from backfill_sql import execute_backfill_sql
from utils import (
    get_backfill_status,
    verify_backfill,
    generate_backfill_report,
    estimate_backfill_time
)


def setup_logging(log_level: str = "INFO"):
    """Configure logging for the application"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'backfill_{logging.Formatter().formatTime(None, "%Y%m%d_%H%M%S")}.log')
        ]
    )


def create_spark_session(app_name: str = "IcebergHistoryBackfill") -> SparkSession:
    """Create and configure Spark session for Iceberg"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .getOrCreate()


def run_setup(args):
    """Setup infrastructure (tables)"""
    logging.info("Setting up backfill infrastructure...")
    
    spark = create_spark_session()
    setup_infrastructure(spark)
    
    if args.sample_data:
        insert_sample_backfill_data(spark)
    
    logging.info("Infrastructure setup complete")
    spark.stop()


def run_backfill(args):
    """Run a backfill operation"""
    logging.info(f"Starting backfill: {args.backfill_id} for month {args.backfill_month}")
    
    spark = create_spark_session()
    
    # Initialize processor
    processor = IcebergBackfillProcessor(
        spark=spark,
        target_table=args.target_table,
        history_columns=HISTORY_COLUMNS,
        max_history_months=MAX_HISTORY_MONTHS
    )
    
    # Choose execution method
    if args.method == "merge":
        # Partition-wise MERGE (recommended)
        orchestrator = BatchBackfillOrchestrator(
            processor=processor,
            max_parallel_partitions=args.max_parallel,
            retry_count=args.retry_count
        )
        
        result = orchestrator.run_backfill(
            backfill_id=args.backfill_id,
            backfill_month=args.backfill_month,
            resume_from=args.resume_from
        )
        
        logging.info(f"Backfill result: {result}")
        
    elif args.method == "sql":
        # SQL-based approach (simpler)
        execute_backfill_sql(
            spark=spark,
            target_table=args.target_table,
            backfill_month=args.backfill_month,
            backfill_id=args.backfill_id,
            history_columns=HISTORY_COLUMNS
        )
    
    # Generate report
    if args.report:
        generate_backfill_report(spark, args.backfill_id)
    
    # Verify if requested
    if args.verify:
        verify_result = verify_backfill(
            spark=spark,
            target_table=args.target_table,
            backfill_id=args.backfill_id,
            backfill_month=args.backfill_month
        )
        
        if verify_result:
            logging.info("Verification results:")
            verify_result.show(truncate=False)
    
    spark.stop()


def run_status(args):
    """Check status of a backfill"""
    logging.info(f"Checking status for backfill: {args.backfill_id}")
    
    spark = create_spark_session()
    
    status_df = get_backfill_status(spark, args.backfill_id)
    status_df.show(truncate=False)
    
    # Generate report
    generate_backfill_report(spark, args.backfill_id)
    
    spark.stop()


def run_estimate(args):
    """Estimate backfill time"""
    logging.info(f"Estimating backfill time for month {args.backfill_month}")
    
    spark = create_spark_session()
    
    estimate_backfill_time(
        spark=spark,
        target_table=args.target_table,
        backfill_month=args.backfill_month
    )
    
    spark.stop()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Iceberg History Backfill Tool")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Setup infrastructure")
    setup_parser.add_argument("--sample-data", action="store_true", help="Insert sample backfill data")
    
    # Backfill command
    backfill_parser = subparsers.add_parser("backfill", help="Run backfill operation")
    backfill_parser.add_argument("--target-table", required=True, help="Target table name")
    backfill_parser.add_argument("--backfill-id", required=True, help="Unique backfill identifier")
    backfill_parser.add_argument("--backfill-month", required=True, help="Month to backfill (YYYY-MM)")
    backfill_parser.add_argument("--method", choices=["merge", "sql"], default="merge", help="Execution method")
    backfill_parser.add_argument("--max-parallel", type=int, default=4, help="Max parallel partitions")
    backfill_parser.add_argument("--retry-count", type=int, default=3, help="Number of retries on failure")
    backfill_parser.add_argument("--resume-from", help="Resume from partition (for recovery)")
    backfill_parser.add_argument("--report", action="store_true", help="Generate report after completion")
    backfill_parser.add_argument("--verify", action="store_true", help="Verify backfill after completion")
    
    # Status command
    status_parser = subparsers.add_parser("status", help="Check backfill status")
    status_parser.add_argument("--backfill-id", required=True, help="Backfill identifier")
    
    # Estimate command
    estimate_parser = subparsers.add_parser("estimate", help="Estimate backfill time")
    estimate_parser.add_argument("--target-table", required=True, help="Target table name")
    estimate_parser.add_argument("--backfill-month", required=True, help="Month to backfill (YYYY-MM)")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Route to appropriate command
    if args.command == "setup":
        run_setup(args)
    elif args.command == "backfill":
        run_backfill(args)
    elif args.command == "status":
        run_status(args)
    elif args.command == "estimate":
        run_estimate(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()


# =============================================================================
# USAGE EXAMPLES
# =============================================================================

"""
# 1. Setup infrastructure
python run_backfill.py setup --sample-data

# 2. Estimate backfill time
python run_backfill.py estimate \\
    --target-table catalog.db.accounts_history \\
    --backfill-month 2023-12

# 3. Run backfill (MERGE method - recommended)
python run_backfill.py backfill \\
    --target-table catalog.db.accounts_history \\
    --backfill-id bf-2024-001 \\
    --backfill-month 2023-12 \\
    --method merge \\
    --max-parallel 4 \\
    --report \\
    --verify

# 4. Resume failed backfill
python run_backfill.py backfill \\
    --target-table catalog.db.accounts_history \\
    --backfill-id bf-2024-001 \\
    --backfill-month 2023-12 \\
    --method merge \\
    --resume-from 2024-05

# 5. Check backfill status
python run_backfill.py status --backfill-id bf-2024-001

# 6. Run with SQL method (simpler but slower)
python run_backfill.py backfill \\
    --target-table catalog.db.accounts_history \\
    --backfill-id bf-2024-002 \\
    --backfill-month 2023-11 \\
    --method sql
"""


# =============================================================================
# PROGRAMMATIC USAGE
# =============================================================================

def example_programmatic_usage():
    """Example of using the backfill system programmatically"""
    
    # Create Spark session
    spark = create_spark_session()
    
    # Setup infrastructure (one-time)
    setup_infrastructure(spark)
    
    # Insert backfill data
    backfill_data = [
        ("bf-001", 12345, "2023-12", 1500.00, 200.00, 5000.00, 0.30),
        ("bf-001", 67890, "2023-12", 2300.00, 150.00, 8000.00, 0.29),
    ]
    
    # ... insert data to backfill_staging table ...
    
    # Create processor
    processor = IcebergBackfillProcessor(
        spark=spark,
        target_table="catalog.db.accounts_history",
        history_columns=HISTORY_COLUMNS
    )
    
    # Create orchestrator
    orchestrator = BatchBackfillOrchestrator(processor)
    
    # Run backfill
    result = orchestrator.run_backfill(
        backfill_id="bf-001",
        backfill_month="2023-12"
    )
    
    print(f"Backfill complete: {result}")
    
    # Generate report
    report = generate_backfill_report(spark, "bf-001")
    
    # Verify
    verify_backfill(
        spark=spark,
        target_table="catalog.db.accounts_history",
        backfill_id="bf-001",
        backfill_month="2023-12"
    )
    
    spark.stop()
