"""
Example usage scripts for the Iceberg History Backfill System
"""

from pyspark.sql import functions as F
from run_backfill import create_spark_session
from backfill_infrastructure import setup_infrastructure
from backfill_processor import IcebergBackfillProcessor
from backfill_batch import BatchBackfillOrchestrator
from config import HISTORY_COLUMNS


def example_1_simple_backfill():
    """
    Example 1: Simple backfill for a single month
    """
    print("="*80)
    print("EXAMPLE 1: Simple Backfill")
    print("="*80)
    
    spark = create_spark_session()
    
    # Setup (one-time)
    setup_infrastructure(spark)
    
    # Insert backfill data into staging table
    backfill_data = spark.createDataFrame([
        ("bf-001", 12345, "2023-12", 1500.00, 200.00, 5000.00, 0.30),
        ("bf-001", 67890, "2023-12", 2300.00, 150.00, 8000.00, 0.29),
    ], ["backfill_id", "acct", "backfill_month", "balance", "payment", "credit_limit", "utilization"])
    
    # Add required columns
    backfill_data = backfill_data \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("status", F.lit("pending"))
    
    # Write to staging
    backfill_data.writeTo("backfill_staging").append()
    
    # Create processor
    processor = IcebergBackfillProcessor(
        spark=spark,
        target_table="catalog.db.accounts_history",
        history_columns=HISTORY_COLUMNS[:4]  # Just first 4 columns for example
    )
    
    # Run backfill
    orchestrator = BatchBackfillOrchestrator(processor)
    result = orchestrator.run_backfill(
        backfill_id="bf-001",
        backfill_month="2023-12"
    )
    
    print(f"\nResult: {result}")
    spark.stop()


def example_2_multiple_months():
    """
    Example 2: Backfill multiple months sequentially
    """
    print("="*80)
    print("EXAMPLE 2: Multiple Month Backfill")
    print("="*80)
    
    spark = create_spark_session()
    
    # Create processor once
    processor = IcebergBackfillProcessor(
        spark=spark,
        target_table="catalog.db.accounts_history",
        history_columns=HISTORY_COLUMNS
    )
    
    # Define multiple backfills
    backfills = [
        {"backfill_id": "bf-dec", "backfill_month": "2023-12"},
        {"backfill_id": "bf-nov", "backfill_month": "2023-11"},
        {"backfill_id": "bf-oct", "backfill_month": "2023-10"},
    ]
    
    # Run all backfills
    from backfill_batch import run_parallel_backfills
    results = run_parallel_backfills(processor, backfills)
    
    # Print summary
    for r in results:
        print(f"{r['backfill_id']}: {r['completed']}/{r['total']} completed")
    
    spark.stop()


def example_3_with_verification():
    """
    Example 3: Backfill with verification
    """
    print("="*80)
    print("EXAMPLE 3: Backfill with Verification")
    print("="*80)
    
    spark = create_spark_session()
    
    # Run backfill
    processor = IcebergBackfillProcessor(
        spark=spark,
        target_table="catalog.db.accounts_history",
        history_columns=HISTORY_COLUMNS
    )
    
    orchestrator = BatchBackfillOrchestrator(processor)
    result = orchestrator.run_backfill(
        backfill_id="bf-001",
        backfill_month="2023-12"
    )
    
    # Verify results
    from utils import verify_backfill, generate_backfill_report
    
    verify_df = verify_backfill(
        spark=spark,
        target_table="catalog.db.accounts_history",
        backfill_id="bf-001",
        backfill_month="2023-12",
        sample_accounts=[12345, 67890]
    )
    
    if verify_df:
        print("\nVerification Results:")
        verify_df.show(truncate=False)
    
    # Generate report
    report = generate_backfill_report(spark, "bf-001")
    print(f"\nReport: {report}")
    
    spark.stop()


def example_4_resume_from_failure():
    """
    Example 4: Resume backfill from failure
    """
    print("="*80)
    print("EXAMPLE 4: Resume from Failure")
    print("="*80)
    
    spark = create_spark_session()
    
    processor = IcebergBackfillProcessor(
        spark=spark,
        target_table="catalog.db.accounts_history",
        history_columns=HISTORY_COLUMNS
    )
    
    orchestrator = BatchBackfillOrchestrator(processor)
    
    # Try initial run (might fail)
    try:
        result = orchestrator.run_backfill(
            backfill_id="bf-001",
            backfill_month="2023-12"
        )
    except Exception as e:
        print(f"Initial run failed: {e}")
        
        # Check audit to find last successful partition
        from utils import get_backfill_status
        status_df = get_backfill_status(spark, "bf-001")
        status_df.show()
        
        # Resume from failed partition
        last_completed = status_df.filter(
            F.col("status") == "completed"
        ).agg(F.max("target_partition")).collect()[0][0]
        
        print(f"\nResuming from: {last_completed}")
        
        result = orchestrator.run_backfill(
            backfill_id="bf-001",
            backfill_month="2023-12",
            resume_from=last_completed
        )
        
        print(f"Resume result: {result}")
    
    spark.stop()


def example_5_estimate_before_run():
    """
    Example 5: Estimate time before running
    """
    print("="*80)
    print("EXAMPLE 5: Estimate Before Running")
    print("="*80)
    
    spark = create_spark_session()
    
    from utils import estimate_backfill_time
    
    # Estimate first
    estimated_seconds = estimate_backfill_time(
        spark=spark,
        target_table="catalog.db.accounts_history",
        backfill_month="2023-12"
    )
    
    print(f"\nEstimated time: {estimated_seconds/3600:.1f} hours")
    
    # Ask user for confirmation
    response = input("Proceed with backfill? (y/n): ")
    
    if response.lower() == 'y':
        processor = IcebergBackfillProcessor(
            spark=spark,
            target_table="catalog.db.accounts_history",
            history_columns=HISTORY_COLUMNS
        )
        
        orchestrator = BatchBackfillOrchestrator(processor)
        result = orchestrator.run_backfill(
            backfill_id="bf-001",
            backfill_month="2023-12"
        )
        
        print(f"Result: {result}")
    else:
        print("Backfill cancelled")
    
    spark.stop()


def example_6_sql_method():
    """
    Example 6: Using SQL method instead of MERGE
    """
    print("="*80)
    print("EXAMPLE 6: SQL Method")
    print("="*80)
    
    spark = create_spark_session()
    
    from backfill_sql import execute_backfill_sql
    
    execute_backfill_sql(
        spark=spark,
        target_table="catalog.db.accounts_history",
        backfill_month="2023-12",
        backfill_id="bf-001",
        history_columns=HISTORY_COLUMNS
    )
    
    print("SQL-based backfill completed")
    spark.stop()


def example_7_generate_sql_script():
    """
    Example 7: Generate SQL script for manual execution
    """
    print("="*80)
    print("EXAMPLE 7: Generate SQL Script")
    print("="*80)
    
    from backfill_sql import generate_sql_script
    
    generate_sql_script(
        target_table="catalog.db.accounts_history",
        backfill_month="2023-12",
        backfill_id="bf-001",
        history_columns=HISTORY_COLUMNS,
        output_file="backfill_2023_12.sql"
    )
    
    print("SQL script generated: backfill_2023_12.sql")
    print("You can now review and execute this manually")


if __name__ == "__main__":
    import sys
    
    examples = {
        "1": ("Simple backfill", example_1_simple_backfill),
        "2": ("Multiple months", example_2_multiple_months),
        "3": ("With verification", example_3_with_verification),
        "4": ("Resume from failure", example_4_resume_from_failure),
        "5": ("Estimate before run", example_5_estimate_before_run),
        "6": ("SQL method", example_6_sql_method),
        "7": ("Generate SQL script", example_7_generate_sql_script),
    }
    
    if len(sys.argv) < 2:
        print("Available examples:")
        for key, (desc, _) in examples.items():
            print(f"  {key}: {desc}")
        print(f"\nUsage: python {sys.argv[0]} <example_number>")
        sys.exit(1)
    
    example_num = sys.argv[1]
    if example_num in examples:
        desc, func = examples[example_num]
        print(f"\nRunning: {desc}\n")
        func()
    else:
        print(f"Unknown example: {example_num}")
        sys.exit(1)
