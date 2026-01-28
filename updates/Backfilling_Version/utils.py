"""
Utility functions for backfill operations.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Optional
import logging

from config import BACKFILL_AUDIT_TABLE

logger = logging.getLogger("BackfillUtils")


def log_audit(
    spark: SparkSession,
    backfill_id: str,
    target_partition: str,
    status: str,
    rows_updated: int = 0,
    accounts_updated: int = 0,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    error_message: Optional[str] = None
):
    """
    Log backfill operation to audit table.
    
    Args:
        spark: SparkSession
        backfill_id: Backfill identifier
        target_partition: Partition that was processed
        status: Status ('completed', 'failed', etc.)
        rows_updated: Number of rows updated
        accounts_updated: Number of accounts updated
        start_time: Operation start time
        end_time: Operation end time
        error_message: Error message if failed
    """
    if start_time and end_time:
        duration_seconds = int((end_time - start_time).total_seconds())
    else:
        duration_seconds = 0
    
    # Prepare values
    started_at = start_time.strftime("%Y-%m-%d %H:%M:%S") if start_time else "NULL"
    completed_at = end_time.strftime("%Y-%m-%d %H:%M:%S") if end_time else "NULL"
    error_msg = f"'{error_message}'" if error_message else "NULL"
    
    insert_sql = f"""
        INSERT INTO {BACKFILL_AUDIT_TABLE} VALUES (
            '{backfill_id}',
            '{target_partition}',
            {accounts_updated},
            {rows_updated},
            timestamp'{started_at}',
            timestamp'{completed_at}',
            {duration_seconds},
            '{status}',
            {error_msg}
        )
    """
    
    try:
        spark.sql(insert_sql)
        logger.debug(f"Audit logged: {backfill_id}/{target_partition} - {status}")
    except Exception as e:
        logger.error(f"Failed to log audit: {e}")


def get_backfill_status(spark: SparkSession, backfill_id: str):
    """
    Get status of a backfill operation.
    
    Returns:
        DataFrame with audit records
    """
    return spark.sql(f"""
        SELECT 
            backfill_id,
            target_partition,
            status,
            rows_updated,
            duration_seconds,
            started_at,
            completed_at,
            error_message
        FROM {BACKFILL_AUDIT_TABLE}
        WHERE backfill_id = '{backfill_id}'
        ORDER BY target_partition
    """)


def verify_backfill(
    spark: SparkSession,
    target_table: str,
    backfill_id: str,
    backfill_month: str,
    sample_accounts: list = None
):
    """
    Verify backfill was applied correctly by checking sample accounts.
    
    Args:
        spark: SparkSession
        target_table: Table to verify
        backfill_id: Backfill identifier
        backfill_month: Month that was backfilled
        sample_accounts: List of account IDs to check (defaults to first 5)
    
    Returns:
        DataFrame with verification results
    """
    from pyspark.sql import functions as F
    from config import BACKFILL_STAGING_TABLE
    
    logger.info(f"Verifying backfill {backfill_id} for month {backfill_month}")
    
    # Get sample accounts if not provided
    if sample_accounts is None:
        sample_accounts = [
            row.acct for row in
            spark.read.table(BACKFILL_STAGING_TABLE)
            .filter(F.col("backfill_id") == backfill_id)
            .select("acct")
            .limit(5)
            .collect()
        ]
    
    if not sample_accounts:
        logger.warning("No sample accounts found")
        return None
    
    logger.info(f"Checking {len(sample_accounts)} sample accounts: {sample_accounts}")
    
    # Get a partition to check (first one after backfill month)
    check_partition = spark.sql(f"""
        SELECT MIN(month_partition) as partition
        FROM {target_table}
        WHERE month_partition > '{backfill_month}'
    """).collect()[0].partition
    
    logger.info(f"Checking partition: {check_partition}")
    
    # Verify data
    result = spark.sql(f"""
        SELECT 
            t.acct,
            t.month_partition,
            t.balance_history,
            b.balance as expected_balance
        FROM {target_table} t
        JOIN {BACKFILL_STAGING_TABLE} b ON t.acct = b.acct
        WHERE t.acct IN ({','.join(map(str, sample_accounts))})
          AND t.month_partition = '{check_partition}'
          AND b.backfill_id = '{backfill_id}'
    """)
    
    return result


def cleanup_old_audits(spark: SparkSession, days_to_keep: int = 90):
    """
    Clean up old audit records.
    
    Args:
        spark: SparkSession
        days_to_keep: Number of days of audit history to retain
    """
    logger.info(f"Cleaning up audit records older than {days_to_keep} days")
    
    deleted = spark.sql(f"""
        DELETE FROM {BACKFILL_AUDIT_TABLE}
        WHERE completed_at < current_timestamp() - INTERVAL {days_to_keep} DAYS
    """)
    
    logger.info(f"Cleaned up old audit records")


def generate_backfill_report(spark: SparkSession, backfill_id: str):
    """
    Generate summary report for a backfill operation.
    
    Args:
        spark: SparkSession
        backfill_id: Backfill identifier
    
    Returns:
        Dictionary with summary statistics
    """
    logger.info(f"Generating report for backfill: {backfill_id}")
    
    # Get summary stats
    summary = spark.sql(f"""
        SELECT 
            COUNT(*) as total_partitions,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_partitions,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_partitions,
            SUM(rows_updated) as total_rows_updated,
            AVG(duration_seconds) as avg_duration_seconds,
            MAX(duration_seconds) as max_duration_seconds,
            MIN(started_at) as first_start,
            MAX(completed_at) as last_completion
        FROM {BACKFILL_AUDIT_TABLE}
        WHERE backfill_id = '{backfill_id}'
    """).collect()[0]
    
    report = {
        "backfill_id": backfill_id,
        "total_partitions": summary.total_partitions,
        "completed_partitions": summary.completed_partitions,
        "failed_partitions": summary.failed_partitions,
        "success_rate": 100 * summary.completed_partitions / summary.total_partitions if summary.total_partitions > 0 else 0,
        "total_rows_updated": summary.total_rows_updated,
        "avg_duration_seconds": summary.avg_duration_seconds,
        "max_duration_seconds": summary.max_duration_seconds,
        "first_start": summary.first_start,
        "last_completion": summary.last_completion,
        "total_duration": (summary.last_completion - summary.first_start).total_seconds() if summary.last_completion and summary.first_start else 0
    }
    
    # Print report
    logger.info("="*60)
    logger.info(f"BACKFILL REPORT: {backfill_id}")
    logger.info("="*60)
    logger.info(f"Total Partitions: {report['total_partitions']}")
    logger.info(f"Completed: {report['completed_partitions']}")
    logger.info(f"Failed: {report['failed_partitions']}")
    logger.info(f"Success Rate: {report['success_rate']:.1f}%")
    logger.info(f"Total Rows Updated: {report['total_rows_updated']}")
    logger.info(f"Avg Duration per Partition: {report['avg_duration_seconds']:.1f}s")
    logger.info(f"Max Duration: {report['max_duration_seconds']:.1f}s")
    logger.info(f"Total Duration: {report['total_duration']:.1f}s")
    logger.info("="*60)
    
    return report


def estimate_backfill_time(
    spark: SparkSession,
    target_table: str,
    backfill_month: str,
    sample_partition: str = None
):
    """
    Estimate time required for backfill based on sample partition processing.
    
    Args:
        spark: SparkSession
        target_table: Table to backfill
        backfill_month: Month being backfilled
        sample_partition: Optional partition to use for timing test
    
    Returns:
        Estimated total time in seconds
    """
    from backfill_processor import get_affected_partitions
    
    # Get affected partitions
    partitions = get_affected_partitions(spark, target_table, backfill_month)
    
    if not partitions:
        return 0
    
    # Use first partition as sample if not specified
    if sample_partition is None:
        sample_partition = partitions[0]
    
    logger.info(f"Estimating backfill time using sample partition: {sample_partition}")
    
    # Get row count for sample
    sample_rows = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM {target_table}
        WHERE month_partition = '{sample_partition}'
    """).collect()[0].cnt
    
    # Estimate: ~1000 rows per second (conservative)
    # This is a rough estimate - adjust based on your cluster
    rows_per_second = 1000
    seconds_per_partition = sample_rows / rows_per_second
    
    total_estimate = seconds_per_partition * len(partitions)
    
    logger.info(f"Estimate: {total_estimate:.0f} seconds ({total_estimate/3600:.1f} hours) for {len(partitions)} partitions")
    logger.info(f"Based on: {sample_rows} rows/partition, {rows_per_second} rows/sec processing rate")
    
    return total_estimate
