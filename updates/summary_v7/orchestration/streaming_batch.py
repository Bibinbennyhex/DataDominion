"""
Summary Pipeline v7.0 - Streaming Batch Manager
================================================

Streaming micro-batch processing for optimal resource utilization.
"""

import uuid
import time
import logging
from typing import List, Callable, Optional, Iterator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from core.config import PipelineConfig
from core.types import MicroBatch, BatchStatus, CaseType

logger = logging.getLogger("SummaryPipeline.StreamingBatch")


class StreamingBatchManager:
    """
    Manages streaming micro-batch processing.
    
    Instead of loading all data at once, processes in smaller batches
    for better memory management and checkpointing.
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.streaming_config = config.streaming
        self.batch_size = config.streaming.micro_batch_size
    
    def create_micro_batches(
        self,
        df: DataFrame,
        partition_column: str = None
    ) -> List[MicroBatch]:
        """
        Split DataFrame into micro-batches.
        
        Args:
            df: Input DataFrame
            partition_column: Optional column to partition by
            
        Returns:
            List of MicroBatch objects
        """
        total_count = df.count()
        
        if not self.streaming_config.enabled or total_count <= self.batch_size:
            # Single batch
            return [MicroBatch(
                batch_id=str(uuid.uuid4())[:8],
                batch_number=0,
                record_count=total_count,
                start_offset=0,
                end_offset=total_count
            )]
        
        # Calculate batch boundaries
        num_batches = (total_count + self.batch_size - 1) // self.batch_size
        batches = []
        
        for i in range(num_batches):
            start = i * self.batch_size
            end = min((i + 1) * self.batch_size, total_count)
            
            batches.append(MicroBatch(
                batch_id=str(uuid.uuid4())[:8],
                batch_number=i,
                record_count=end - start,
                start_offset=start,
                end_offset=end
            ))
        
        logger.info(f"Created {len(batches)} micro-batches from {total_count:,} records")
        
        return batches
    
    def get_batch_dataframe(
        self,
        df: DataFrame,
        batch: MicroBatch,
        order_column: str = None
    ) -> DataFrame:
        """
        Get DataFrame for a specific batch.
        
        Args:
            df: Full DataFrame
            batch: Batch to extract
            order_column: Column to order by for offset-based extraction
            
        Returns:
            Batch DataFrame
        """
        if order_column:
            # Use window function for precise batching
            windowed = df.withColumn(
                "_row_num",
                F.row_number().over(
                    F.Window.orderBy(order_column)
                )
            )
            
            batch_df = windowed.filter(
                (F.col("_row_num") > batch.start_offset) &
                (F.col("_row_num") <= batch.end_offset)
            ).drop("_row_num")
        else:
            # Use limit/offset approximation
            # Note: This is less precise but faster
            batch_df = df.limit(batch.end_offset) \
                        .subtract(df.limit(batch.start_offset))
        
        return batch_df
    
    def process_batches_streaming(
        self,
        batches: List[MicroBatch],
        get_batch_df: Callable[[MicroBatch], DataFrame],
        process_fn: Callable[[DataFrame, MicroBatch], None],
        on_batch_complete: Optional[Callable[[MicroBatch], None]] = None,
        max_concurrent: int = 1
    ) -> tuple[int, int, List[MicroBatch]]:
        """
        Process micro-batches with streaming semantics.
        
        Args:
            batches: List of batches to process
            get_batch_df: Function to get DataFrame for a batch
            process_fn: Processing function
            on_batch_complete: Callback after each batch
            max_concurrent: Maximum concurrent batches
            
        Returns:
            Tuple of (completed, failed, all_batches)
        """
        completed = 0
        failed = 0
        
        total = len(batches)
        logger.info(f"Starting streaming processing: {total} batches, {max_concurrent} concurrent")
        
        if max_concurrent <= 1:
            # Sequential processing
            for batch in batches:
                batch.start_time = datetime.now()
                batch.status = BatchStatus.RUNNING
                
                try:
                    batch_df = get_batch_df(batch)
                    process_fn(batch_df, batch)
                    
                    batch.status = BatchStatus.COMPLETED
                    batch.end_time = datetime.now()
                    completed += 1
                    
                    if on_batch_complete:
                        on_batch_complete(batch)
                    
                    logger.info(
                        f"Batch {batch.batch_number + 1}/{total} completed "
                        f"({batch.record_count:,} records in {batch.duration_seconds:.1f}s)"
                    )
                    
                except Exception as e:
                    batch.status = BatchStatus.FAILED
                    batch.end_time = datetime.now()
                    batch.error_message = str(e)
                    failed += 1
                    logger.error(f"Batch {batch.batch_number + 1} failed: {e}")
        else:
            # Concurrent processing
            with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                futures = {}
                
                for batch in batches:
                    future = executor.submit(
                        self._process_single_batch,
                        batch, get_batch_df, process_fn
                    )
                    futures[future] = batch
                
                for future in as_completed(futures):
                    batch = futures[future]
                    try:
                        future.result()
                        completed += 1
                        if on_batch_complete:
                            on_batch_complete(batch)
                    except Exception as e:
                        failed += 1
                        batch.error_message = str(e)
        
        logger.info(f"Streaming complete: {completed} succeeded, {failed} failed")
        
        return completed, failed, batches
    
    def _process_single_batch(
        self,
        batch: MicroBatch,
        get_batch_df: Callable,
        process_fn: Callable
    ):
        """Process a single batch."""
        batch.start_time = datetime.now()
        batch.status = BatchStatus.RUNNING
        
        batch_df = get_batch_df(batch)
        process_fn(batch_df, batch)
        
        batch.status = BatchStatus.COMPLETED
        batch.end_time = datetime.now()


def stream_dataframe_in_chunks(
    df: DataFrame,
    chunk_size: int,
    order_column: str
) -> Iterator[DataFrame]:
    """
    Generator that yields DataFrame chunks.
    
    Args:
        df: Input DataFrame
        chunk_size: Records per chunk
        order_column: Column to order by
        
    Yields:
        DataFrame chunks
    """
    total = df.count()
    offset = 0
    
    while offset < total:
        end = min(offset + chunk_size, total)
        
        chunk = df.withColumn(
            "_row_num",
            F.row_number().over(F.Window.orderBy(order_column))
        ).filter(
            (F.col("_row_num") > offset) &
            (F.col("_row_num") <= end)
        ).drop("_row_num")
        
        yield chunk
        offset = end
