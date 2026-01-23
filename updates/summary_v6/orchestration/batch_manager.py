"""
Summary Pipeline v6.0 - Batch Manager
======================================

Batch processing with retry logic and parallelism control.
"""

import time
import uuid
import logging
from typing import List, Callable, Optional, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import DataFrame

from core.config import PipelineConfig
from core.types import BatchInfo, BatchStatus, CaseType

logger = logging.getLogger("SummaryPipeline.BatchManager")


class BatchManager:
    """
    Manages batch processing with:
    - Retry with exponential backoff
    - Configurable parallelism
    - Progress tracking
    - Integration with audit and checkpoint
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.retry_count = config.resilience.retry_count
        self.backoff_base = config.resilience.retry_backoff_base
        self.max_delay = config.resilience.retry_max_delay_seconds
        self.batch_size = config.resilience.batch_size
    
    def create_batch_info(
        self,
        case_type: CaseType,
        record_count: int,
        partition_key: Optional[str] = None
    ) -> BatchInfo:
        """Create a new batch info object."""
        return BatchInfo(
            batch_id=str(uuid.uuid4())[:8],
            case_type=case_type,
            status=BatchStatus.PENDING,
            record_count=record_count,
            partition_key=partition_key
        )
    
    def process_with_retry(
        self,
        process_fn: Callable[[], Any],
        batch_info: BatchInfo,
        on_success: Optional[Callable[[BatchInfo], None]] = None,
        on_failure: Optional[Callable[[BatchInfo], None]] = None
    ) -> tuple[bool, BatchInfo]:
        """
        Execute a processing function with retry logic.
        
        Args:
            process_fn: Function to execute
            batch_info: Batch tracking information
            on_success: Callback on successful completion
            on_failure: Callback on final failure
            
        Returns:
            Tuple of (success, updated batch_info)
        """
        batch_info.start_time = datetime.now()
        
        for attempt in range(self.retry_count):
            try:
                if attempt > 0:
                    batch_info.status = BatchStatus.RETRYING
                    wait_time = min(
                        self.backoff_base ** attempt,
                        self.max_delay
                    )
                    logger.info(f"Retry {attempt + 1}/{self.retry_count} after {wait_time}s delay")
                    time.sleep(wait_time)
                
                batch_info.status = BatchStatus.RUNNING
                batch_info.retry_count = attempt
                
                result = process_fn()
                
                batch_info.status = BatchStatus.COMPLETED
                batch_info.end_time = datetime.now()
                
                if on_success:
                    on_success(batch_info)
                
                logger.info(
                    f"Batch {batch_info.batch_id} completed in "
                    f"{batch_info.duration_seconds:.1f}s"
                )
                
                return True, batch_info
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                batch_info.error_message = str(e)
                
                if attempt == self.retry_count - 1:
                    batch_info.status = BatchStatus.FAILED
                    batch_info.end_time = datetime.now()
                    
                    if on_failure:
                        on_failure(batch_info)
                    
                    logger.error(
                        f"Batch {batch_info.batch_id} failed after "
                        f"{self.retry_count} attempts: {e}"
                    )
                    return False, batch_info
        
        return False, batch_info
    
    def process_batches(
        self,
        batches: List[tuple[DataFrame, BatchInfo]],
        process_fn: Callable[[DataFrame, BatchInfo], Any],
        max_parallel: int = 1,
        on_batch_complete: Optional[Callable[[BatchInfo], None]] = None
    ) -> tuple[int, int, List[BatchInfo]]:
        """
        Process multiple batches with optional parallelism.
        
        Args:
            batches: List of (DataFrame, BatchInfo) tuples
            process_fn: Function to process each batch
            max_parallel: Maximum concurrent batches
            on_batch_complete: Callback after each batch
            
        Returns:
            Tuple of (completed_count, failed_count, all_batch_infos)
        """
        completed = 0
        failed = 0
        all_infos = []
        
        total = len(batches)
        logger.info(f"Processing {total} batches (max parallel: {max_parallel})")
        
        if max_parallel <= 1:
            # Sequential processing
            for i, (df, batch_info) in enumerate(batches):
                logger.info(f"Processing batch {i + 1}/{total}: {batch_info.batch_id}")
                
                success, updated_info = self.process_with_retry(
                    lambda: process_fn(df, batch_info),
                    batch_info
                )
                
                if success:
                    completed += 1
                else:
                    failed += 1
                
                all_infos.append(updated_info)
                
                if on_batch_complete:
                    on_batch_complete(updated_info)
                
                progress = 100 * (i + 1) / total
                logger.info(f"Progress: {completed}/{total} completed ({progress:.1f}%)")
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                futures = {}
                for df, batch_info in batches:
                    future = executor.submit(
                        self.process_with_retry,
                        lambda d=df, b=batch_info: process_fn(d, b),
                        batch_info
                    )
                    futures[future] = batch_info
                
                for future in as_completed(futures):
                    batch_info = futures[future]
                    try:
                        success, updated_info = future.result()
                        if success:
                            completed += 1
                        else:
                            failed += 1
                        all_infos.append(updated_info)
                        
                        if on_batch_complete:
                            on_batch_complete(updated_info)
                            
                    except Exception as e:
                        failed += 1
                        batch_info.status = BatchStatus.FAILED
                        batch_info.error_message = str(e)
                        all_infos.append(batch_info)
                        logger.error(f"Batch {batch_info.batch_id} failed: {e}")
        
        logger.info(f"Batch processing complete: {completed} succeeded, {failed} failed")
        
        return completed, failed, all_infos
    
    def split_dataframe_into_batches(
        self,
        df: DataFrame,
        case_type: CaseType,
        partition_column: Optional[str] = None
    ) -> List[tuple[DataFrame, BatchInfo]]:
        """
        Split a DataFrame into batches for processing.
        
        Args:
            df: DataFrame to split
            case_type: Case type for batch info
            partition_column: Optional column to partition by
            
        Returns:
            List of (DataFrame, BatchInfo) tuples
        """
        if partition_column and partition_column in df.columns:
            # Split by partition column values
            partitions = [row[partition_column] for row in 
                         df.select(partition_column).distinct().collect()]
            
            batches = []
            for partition_value in partitions:
                partition_df = df.filter(df[partition_column] == partition_value)
                count = partition_df.count()
                
                batch_info = self.create_batch_info(
                    case_type=case_type,
                    record_count=count,
                    partition_key=str(partition_value)
                )
                batches.append((partition_df, batch_info))
            
            return batches
        else:
            # Single batch
            count = df.count()
            batch_info = self.create_batch_info(
                case_type=case_type,
                record_count=count
            )
            return [(df, batch_info)]
