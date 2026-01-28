"""
Partition-aware MERGE strategy for Iceberg backfill operations.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
from typing import List
import logging

from backfill_processor import (
    IcebergBackfillProcessor,
    get_affected_partitions,
    calculate_months_between
)
from utils import log_audit
from config import BACKFILL_STAGING_TABLE, MAX_HISTORY_MONTHS

logger = logging.getLogger("BackfillMerge")


def execute_backfill_merge(
    processor: IcebergBackfillProcessor,
    backfill_id: str,
    backfill_month: str,
    target_partitions: List[str] = None
):
    """
    Execute backfill using Iceberg MERGE.
    Processes partition-by-partition to manage memory and enable checkpointing.
    
    Args:
        processor: IcebergBackfillProcessor instance
        backfill_id: Unique identifier for this backfill batch
        backfill_month: Month being backfilled (e.g., "2023-12")
        target_partitions: Optional list of partitions to process (for resume)
    """
    spark = processor.spark
    
    logger.info(f"Starting backfill MERGE: {backfill_id} for month {backfill_month}")
    
    # 1. Load backfill data for this batch
    backfill_df = spark.read.table(BACKFILL_STAGING_TABLE).filter(
        (F.col("backfill_month") == backfill_month) &
        (F.col("status") == "pending") &
        (F.col("backfill_id") == backfill_id)
    ).cache()
    
    # Validate data
    if not processor.validate_backfill_data(backfill_df):
        raise ValueError("Backfill data validation failed")
    
    backfill_accounts = backfill_df.select("acct").distinct()
    account_count = backfill_accounts.count()
    logger.info(f"Processing backfill for {account_count} accounts")
    
    # 2. Get target partitions (months after backfill_month, within 36-month window)
    if target_partitions is None:
        target_partitions = get_affected_partitions(
            spark=spark,
            target_table=processor.target_table,
            backfill_month=backfill_month
        )
    
    if not target_partitions:
        logger.warning("No partitions to update")
        return
    
    logger.info(f"Target partitions: {len(target_partitions)} partitions from {target_partitions[0]} to {target_partitions[-1]}")
    
    # Mark as processing
    spark.sql(f"""
        UPDATE {BACKFILL_STAGING_TABLE}
        SET status = 'processing'
        WHERE backfill_id = '{backfill_id}'
          AND backfill_month = '{backfill_month}'
    """)
    
    # 3. Process each partition
    success_count = 0
    for i, partition_month in enumerate(target_partitions):
        try:
            logger.info(f"Processing partition {i+1}/{len(target_partitions)}: {partition_month}")
            
            process_single_partition(
                processor=processor,
                backfill_df=backfill_df,
                backfill_month=backfill_month,
                target_partition=partition_month,
                backfill_id=backfill_id
            )
            
            success_count += 1
            logger.info(f"Progress: {success_count}/{len(target_partitions)} partitions completed")
            
        except Exception as e:
            logger.error(f"Failed partition {partition_month}: {e}")
            log_audit(
                spark=spark,
                backfill_id=backfill_id,
                target_partition=partition_month,
                status="failed",
                error_message=str(e)
            )
            raise
    
    # 4. Mark backfill as completed
    spark.sql(f"""
        UPDATE {BACKFILL_STAGING_TABLE}
        SET status = 'completed', processed_at = current_timestamp()
        WHERE backfill_id = '{backfill_id}'
          AND backfill_month = '{backfill_month}'
    """)
    
    backfill_df.unpersist()
    logger.info(f"Backfill MERGE completed: {success_count}/{len(target_partitions)} partitions")


def process_single_partition(
    processor: IcebergBackfillProcessor,
    backfill_df: DataFrame,
    backfill_month: str,
    target_partition: str,
    backfill_id: str
):
    """
    Process a single partition using Iceberg MERGE.
    Leverages bucket clustering for efficient joins.
    
    Args:
        processor: IcebergBackfillProcessor instance
        backfill_df: DataFrame containing backfill data
        backfill_month: Month being backfilled
        target_partition: Partition to update
        backfill_id: Backfill identifier for audit
    """
    spark = processor.spark
    start_time = datetime.now()
    
    # Calculate position for this partition
    months_diff = calculate_months_between(target_partition, backfill_month)
    
    if months_diff >= processor.max_history_months:
        logger.info(f"Skipping {target_partition}: outside {processor.max_history_months}-month window (diff={months_diff})")
        return
    
    logger.info(f"Processing partition {target_partition}, position={months_diff}")
    
    # Prepare source data with position
    source_df = backfill_df.withColumn("update_position", F.lit(months_diff))
    
    # Build MERGE statement dynamically for all 20 columns
    update_set_clauses = []
    for hist_col in processor.history_columns:
        src_col = hist_col.backfill_source_column
        clause = f"""
            {hist_col.name} = CASE 
                WHEN {months_diff} < {processor.max_history_months} THEN
                    concat(
                        slice(target.{hist_col.name}, 1, {months_diff}),
                        array(source.{src_col}),
                        slice(target.{hist_col.name}, {months_diff + 2}, {processor.max_history_months - months_diff - 1})
                    )
                ELSE target.{hist_col.name}
            END
        """
        update_set_clauses.append(clause)
    
    update_set_sql = ",\n            ".join(update_set_clauses)
    
    # Create temp view for source
    source_df.createOrReplaceTempView("backfill_source")
    
    # Execute MERGE - Iceberg will use bucket info for efficient join
    merge_sql = f"""
        MERGE INTO {processor.target_table} AS target
        USING backfill_source AS source
        ON target.acct = source.acct 
           AND target.month_partition = '{target_partition}'
        WHEN MATCHED THEN UPDATE SET
            {update_set_sql}
    """
    
    logger.debug(f"Executing MERGE for partition {target_partition}")
    spark.sql(merge_sql)
    
    # Get row count for audit
    rows_updated = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM {processor.target_table} 
        WHERE month_partition = '{target_partition}'
          AND acct IN (SELECT DISTINCT acct FROM backfill_source)
    """).collect()[0].cnt
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Log audit
    log_audit(
        spark=spark,
        backfill_id=backfill_id,
        target_partition=target_partition,
        status="completed",
        rows_updated=rows_updated,
        start_time=start_time,
        end_time=end_time
    )
    
    logger.info(f"Completed {target_partition}: {rows_updated} rows updated in {duration:.1f}s")


def execute_backfill_merge_bulk(
    processor: IcebergBackfillProcessor,
    backfill_id: str,
    backfill_month: str,
    batch_size: int = 3
):
    """
    Execute backfill processing multiple partitions in batches.
    Useful for reducing overhead of single-partition processing.
    
    Args:
        processor: IcebergBackfillProcessor instance
        backfill_id: Unique identifier for this backfill batch
        backfill_month: Month being backfilled
        batch_size: Number of partitions to process in each batch
    """
    spark = processor.spark
    
    # Get all affected partitions
    all_partitions = get_affected_partitions(
        spark=spark,
        target_table=processor.target_table,
        backfill_month=backfill_month
    )
    
    # Process in batches
    for i in range(0, len(all_partitions), batch_size):
        batch = all_partitions[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}: {batch}")
        
        execute_backfill_merge(
            processor=processor,
            backfill_id=backfill_id,
            backfill_month=backfill_month,
            target_partitions=batch
        )
