"""
Batch processing orchestrator for large-scale backfill operations.
Handles parallelism, checkpointing, and retry logic.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
import time
import logging

from backfill_processor import IcebergBackfillProcessor, get_affected_partitions
from backfill_merge import process_single_partition
from config import BACKFILL_STAGING_TABLE, RETRY_COUNT, MAX_PARALLEL_PARTITIONS

logger = logging.getLogger("BackfillBatch")


class BatchBackfillOrchestrator:
    """
    Orchestrates backfill across multiple partitions with:
    - Controlled parallelism
    - Checkpointing for resume capability
    - Retry logic with exponential backoff
    - Progress tracking and audit
    """
    
    def __init__(
        self,
        processor: IcebergBackfillProcessor,
        max_parallel_partitions: int = MAX_PARALLEL_PARTITIONS,
        retry_count: int = RETRY_COUNT
    ):
        self.processor = processor
        self.max_parallel = max_parallel_partitions
        self.retry_count = retry_count
        
        logger.info(f"Initialized orchestrator: max_parallel={max_parallel_partitions}, retry={retry_count}")
    
    def run_backfill(
        self,
        backfill_id: str,
        backfill_month: str,
        resume_from: str = None  # For checkpoint recovery
    ) -> Dict[str, any]:
        """
        Main entry point for running a backfill job.
        
        Args:
            backfill_id: Unique identifier for this backfill batch
            backfill_month: Month being backfilled (e.g., "2023-12")
            resume_from: Optional partition to resume from (for recovery)
        
        Returns:
            Dictionary with completion stats
        """
        spark = self.processor.spark
        
        logger.info(f"="*80)
        logger.info(f"Starting backfill orchestration")
        logger.info(f"Backfill ID: {backfill_id}")
        logger.info(f"Backfill Month: {backfill_month}")
        logger.info(f"Resume From: {resume_from or 'Start'}")
        logger.info(f"="*80)
        
        # Get partitions, optionally resuming from checkpoint
        partitions = self._get_partitions(backfill_month, resume_from)
        total = len(partitions)
        
        if total == 0:
            logger.warning("No partitions to process")
            return {"completed": 0, "failed": [], "skipped": 0}
        
        logger.info(f"Total partitions to process: {total}")
        
        # Load and cache backfill data
        backfill_df = spark.read.table(BACKFILL_STAGING_TABLE).filter(
            (F.col("backfill_month") == backfill_month) &
            (F.col("backfill_id") == backfill_id) &
            (F.col("status").isin(["pending", "processing"]))
        )
        
        # Validate data
        if not self.processor.validate_backfill_data(backfill_df):
            raise ValueError("Backfill data validation failed")
        
        account_count = backfill_df.select("acct").distinct().count()
        logger.info(f"Accounts in backfill: {account_count}")
        
        # Broadcast for efficient joins (backfill data is small)
        backfill_broadcast = F.broadcast(backfill_df)
        backfill_broadcast.cache()
        
        # Mark as processing
        spark.sql(f"""
            UPDATE {BACKFILL_STAGING_TABLE}
            SET status = 'processing'
            WHERE backfill_id = '{backfill_id}' 
              AND backfill_month = '{backfill_month}'
        """)
        
        # Process partitions
        completed = 0
        failed = []
        
        logger.info("Starting partition processing...")
        
        for i, partition in enumerate(partitions):
            logger.info(f"\n{'-'*60}")
            logger.info(f"Partition {i+1}/{total}: {partition}")
            logger.info(f"{'-'*60}")
            
            success = self._process_with_retry(
                backfill_id=backfill_id,
                backfill_month=backfill_month,
                backfill_df=backfill_broadcast,
                partition=partition
            )
            
            if success:
                completed += 1
                progress_pct = 100 * completed / total
                logger.info(f"✓ Partition completed successfully")
                logger.info(f"Overall progress: {completed}/{total} ({progress_pct:.1f}%)")
            else:
                failed.append(partition)
                logger.error(f"✗ Partition failed after {self.retry_count} attempts")
        
        # Mark as completed or partially completed
        final_status = "completed" if len(failed) == 0 else "partial"
        spark.sql(f"""
            UPDATE {BACKFILL_STAGING_TABLE}
            SET status = '{final_status}', 
                processed_at = current_timestamp(),
                error_message = CASE 
                    WHEN '{final_status}' = 'partial' THEN 'Failed partitions: {",".join(failed)}'
                    ELSE NULL
                END
            WHERE backfill_id = '{backfill_id}' 
              AND backfill_month = '{backfill_month}'
        """)
        
        backfill_broadcast.unpersist()
        
        # Final report
        logger.info(f"\n{'='*80}")
        logger.info(f"BACKFILL COMPLETE")
        logger.info(f"{'='*80}")
        logger.info(f"Completed: {completed}/{total} partitions")
        logger.info(f"Failed: {len(failed)} partitions")
        
        if failed:
            logger.error(f"Failed partitions: {failed}")
            logger.info(f"To resume, use: resume_from='{failed[0]}'")
        
        logger.info(f"{'='*80}")
        
        return {
            "completed": completed,
            "failed": failed,
            "total": total,
            "success_rate": 100 * completed / total if total > 0 else 0
        }
    
    def _process_with_retry(
        self,
        backfill_id: str,
        backfill_month: str,
        backfill_df: DataFrame,
        partition: str
    ) -> bool:
        """
        Process a partition with retry logic and exponential backoff.
        
        Returns:
            True if successful, False otherwise
        """
        for attempt in range(self.retry_count):
            try:
                if attempt > 0:
                    wait_time = 2 ** attempt
                    logger.info(f"Retry attempt {attempt + 1}/{self.retry_count} after {wait_time}s delay")
                    time.sleep(wait_time)
                
                process_single_partition(
                    processor=self.processor,
                    backfill_df=backfill_df,
                    backfill_month=backfill_month,
                    target_partition=partition,
                    backfill_id=backfill_id
                )
                
                return True
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                
                if attempt == self.retry_count - 1:
                    logger.error(f"All retry attempts exhausted for partition {partition}")
                    return False
        
        return False
    
    def _get_partitions(self, backfill_month: str, resume_from: str) -> List[str]:
        """
        Get partitions to process, handling resume logic.
        
        Args:
            backfill_month: Month being backfilled
            resume_from: Optional partition to resume from
        
        Returns:
            List of partition names to process
        """
        all_partitions = get_affected_partitions(
            spark=self.processor.spark,
            target_table=self.processor.target_table,
            backfill_month=backfill_month
        )
        
        if resume_from:
            try:
                idx = all_partitions.index(resume_from)
                resumed_partitions = all_partitions[idx:]
                logger.info(f"Resuming from partition {resume_from}: {len(resumed_partitions)} partitions remaining")
                return resumed_partitions
            except ValueError:
                logger.warning(f"Resume partition '{resume_from}' not found, starting from beginning")
                return all_partitions
        
        return all_partitions


def run_parallel_backfills(
    processor: IcebergBackfillProcessor,
    backfills: List[Dict[str, str]]
):
    """
    Run multiple independent backfills sequentially.
    Each backfill is for a different month.
    
    Args:
        processor: IcebergBackfillProcessor instance
        backfills: List of dicts with 'backfill_id' and 'backfill_month'
    
    Example:
        backfills = [
            {"backfill_id": "bf-001", "backfill_month": "2023-12"},
            {"backfill_id": "bf-002", "backfill_month": "2023-11"},
        ]
    """
    orchestrator = BatchBackfillOrchestrator(processor)
    
    results = []
    for bf in backfills:
        logger.info(f"\n\n{'#'*80}")
        logger.info(f"Starting backfill: {bf['backfill_id']} for {bf['backfill_month']}")
        logger.info(f"{'#'*80}\n")
        
        result = orchestrator.run_backfill(
            backfill_id=bf['backfill_id'],
            backfill_month=bf['backfill_month']
        )
        
        result['backfill_id'] = bf['backfill_id']
        result['backfill_month'] = bf['backfill_month']
        results.append(result)
    
    # Summary
    logger.info(f"\n\n{'='*80}")
    logger.info("ALL BACKFILLS COMPLETE")
    logger.info(f"{'='*80}")
    
    for r in results:
        status = "✓ SUCCESS" if len(r['failed']) == 0 else "✗ PARTIAL"
        logger.info(f"{r['backfill_id']} ({r['backfill_month']}): {status} - {r['completed']}/{r['total']} partitions")
    
    logger.info(f"{'='*80}\n")
    
    return results
