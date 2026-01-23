"""
Summary Pipeline v7.0 - Parallel Orchestrator
==============================================

Orchestrates parallel processing of independent cases.
"""

import uuid
import time
import logging
from datetime import datetime
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, Future

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from core.config import PipelineConfig
from core.types import (
    CaseType, ProcessingMode, ProcessingMetrics,
    CaseMetrics, CheckpointState, MicroBatch
)
from core.session import configure_partitions
from processors import RecordClassifier, CaseIProcessor, CaseIIProcessor, CaseIIIProcessor
from optimizations import BloomFilterOptimizer, ColumnarProjectionOptimizer
from orchestration.streaming_batch import StreamingBatchManager
from orchestration.checkpoint import CheckpointManager
from utils.partitioning import calculate_optimal_partitions
from utils.optimization import TableOptimizer

logger = logging.getLogger("SummaryPipeline.Orchestrator")


class ParallelOrchestrator:
    """
    Main orchestrator with parallel case processing.
    
    Key optimizations:
    1. Bloom filter pre-filtering
    2. Columnar projection
    3. Parallel Case I + II processing
    4. Streaming micro-batches
    5. Detailed metrics collection
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        
        # Initialize processors
        self.classifier = RecordClassifier(spark, config)
        self.case_i = CaseIProcessor(spark, config)
        self.case_ii = CaseIIProcessor(spark, config)
        self.case_iii = CaseIIIProcessor(spark, config)
        
        # Initialize optimizers
        self.bloom_filter = BloomFilterOptimizer(spark, config)
        self.columnar = ColumnarProjectionOptimizer(config)
        
        # Initialize managers
        self.streaming = StreamingBatchManager(spark, config)
        self.checkpoint = CheckpointManager(spark, config)
        self.table_optimizer = TableOptimizer(spark, config)
        
        # Metrics
        self.metrics = None
    
    def run(self, resume_run_id: Optional[str] = None) -> ProcessingMetrics:
        """
        Run the optimized pipeline.
        
        Args:
            resume_run_id: Optional run ID to resume
            
        Returns:
            Processing metrics
        """
        run_id = resume_run_id or str(uuid.uuid4())[:12]
        
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE v7.0 - ULTIMATE PERFORMANCE")
        logger.info(f"Run ID: {run_id}")
        logger.info("=" * 80)
        
        # Initialize metrics
        self.metrics = ProcessingMetrics(
            run_id=run_id,
            mode=ProcessingMode.INCREMENTAL,
            start_time=datetime.now()
        )
        
        # Load checkpoint if resuming
        checkpoint_state = None
        if resume_run_id:
            checkpoint_state = self.checkpoint.load(resume_run_id)
        
        if checkpoint_state is None:
            checkpoint_state = CheckpointState(
                run_id=run_id,
                mode=ProcessingMode.INCREMENTAL
            )
        
        # Step 1: Get new data
        max_ts = self._get_max_timestamp()
        if max_ts is None:
            logger.warning("No existing data. Use initial mode.")
            return self.metrics
        
        logger.info(f"Loading data newer than: {max_ts}")
        
        batch_df = self.spark.sql(f"""
            SELECT * FROM {self.config.source_table}
            WHERE {self.config.timestamp_key} > TIMESTAMP '{max_ts}'
        """)
        
        record_count = batch_df.count()
        if record_count == 0:
            logger.info("No new records.")
            self.metrics.end_time = datetime.now()
            return self.metrics
        
        self.metrics.total_records = record_count
        logger.info(f"New records: {record_count:,}")
        
        # Step 2: Build Bloom filter for affected accounts
        if self.config.optimization.bloom_filter.enabled:
            self.bloom_filter.build_bloom_filter(batch_df, self.config.primary_key)
            self.metrics.bloom_filter_hits = 1  # Track usage
        
        # Step 3: Configure partitioning
        num_partitions = calculate_optimal_partitions(record_count, self.config)
        configure_partitions(self.spark, num_partitions)
        
        # Step 4: Prepare and classify
        batch_df = self._prepare_data(batch_df)
        classified = self.classifier.classify(batch_df)
        
        # Track case counts
        for case_type in CaseType:
            count = self.classifier.get_case_count(classified, case_type)
            self.metrics.case_metrics[case_type] = CaseMetrics(
                case_type=case_type,
                record_count=count
            )
        
        # Step 5: Process cases (with parallelism)
        self._process_all_cases(classified, checkpoint_state)
        
        # Cleanup
        classified.unpersist()
        self.bloom_filter.cleanup()
        
        # Step 6: Optimize and finalize
        self.table_optimizer.optimize_tables()
        self.checkpoint.clear(run_id)
        
        self.metrics.end_time = datetime.now()
        
        # Log summary
        self._log_summary()
        
        return self.metrics
    
    def _get_max_timestamp(self):
        """Get max timestamp from summary."""
        try:
            result = self.spark.sql(f"""
                SELECT MAX({self.config.timestamp_key}) as max_ts
                FROM {self.config.summary_table}
            """).collect()
            return result[0]["max_ts"]
        except:
            return None
    
    def _prepare_data(self, df: DataFrame) -> DataFrame:
        """Prepare source data with mappings."""
        select_exprs = [
            F.col(src).alias(dst)
            for src, dst in self.config.column_mappings.items()
        ]
        
        result = df.select(*select_exprs)
        
        # Deduplicate
        window = Window.partitionBy(
            self.config.primary_key,
            self.config.partition_key
        ).orderBy(F.col(self.config.timestamp_key).desc())
        
        result = result \
            .withColumn("_rn", F.row_number().over(window)) \
            .filter(F.col("_rn") == 1) \
            .drop("_rn")
        
        return result
    
    def _process_all_cases(
        self,
        classified: DataFrame,
        checkpoint_state: CheckpointState
    ):
        """
        Process all cases with optimized order and parallelism.
        
        Order: Case III (backfill) → (Case I + Case II in parallel)
        """
        
        # Case III first (required before I and II)
        if not self.checkpoint.should_skip_case(checkpoint_state, CaseType.CASE_III):
            if self.classifier.has_records(classified, CaseType.CASE_III):
                logger.info("=" * 60)
                logger.info("PHASE 1: Processing Case III (Backfill)")
                logger.info("=" * 60)
                
                self.metrics.case_metrics[CaseType.CASE_III].start_time = datetime.now()
                
                case_iii_df = self.classifier.get_case_dataframe(classified, CaseType.CASE_III)
                result = self.case_iii.process(case_iii_df)
                self._write_results(result)
                
                self.metrics.case_metrics[CaseType.CASE_III].end_time = datetime.now()
                checkpoint_state.mark_case_complete(CaseType.CASE_III)
                self.checkpoint.save(checkpoint_state)
        
        # Case I and II in parallel (if enabled)
        has_case_i = (
            not self.checkpoint.should_skip_case(checkpoint_state, CaseType.CASE_I) and
            self.classifier.has_records(classified, CaseType.CASE_I)
        )
        
        has_case_ii = (
            not self.checkpoint.should_skip_case(checkpoint_state, CaseType.CASE_II) and
            self.classifier.has_records(classified, CaseType.CASE_II)
        )
        
        if self.config.optimization.parallel_cases.enabled and has_case_i and has_case_ii:
            logger.info("=" * 60)
            logger.info("PHASE 2: Processing Case I + II (PARALLEL)")
            logger.info("=" * 60)
            
            self.metrics.parallel_cases_used = True
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_i = executor.submit(
                    self._process_case_i,
                    self.classifier.get_case_dataframe(classified, CaseType.CASE_I),
                    checkpoint_state
                )
                
                future_ii = executor.submit(
                    self._process_case_ii,
                    self.classifier.get_case_dataframe(classified, CaseType.CASE_II),
                    checkpoint_state
                )
                
                # Wait for both
                future_i.result()
                future_ii.result()
        else:
            # Sequential processing
            if has_case_i:
                logger.info("=" * 60)
                logger.info("PHASE 2a: Processing Case I (New Accounts)")
                logger.info("=" * 60)
                self._process_case_i(
                    self.classifier.get_case_dataframe(classified, CaseType.CASE_I),
                    checkpoint_state
                )
            
            if has_case_ii:
                logger.info("=" * 60)
                logger.info("PHASE 2b: Processing Case II (Forward)")
                logger.info("=" * 60)
                self._process_case_ii(
                    self.classifier.get_case_dataframe(classified, CaseType.CASE_II),
                    checkpoint_state
                )
    
    def _process_case_i(self, df: DataFrame, checkpoint_state: CheckpointState):
        """Process Case I."""
        self.metrics.case_metrics[CaseType.CASE_I].start_time = datetime.now()
        
        result = self.case_i.process(df)
        self._write_results(result)
        
        self.metrics.case_metrics[CaseType.CASE_I].end_time = datetime.now()
        checkpoint_state.mark_case_complete(CaseType.CASE_I)
        self.checkpoint.save(checkpoint_state)
    
    def _process_case_ii(self, df: DataFrame, checkpoint_state: CheckpointState):
        """Process Case II."""
        self.metrics.case_metrics[CaseType.CASE_II].start_time = datetime.now()
        
        result = self.case_ii.process(df)
        self._write_results(result)
        
        self.metrics.case_metrics[CaseType.CASE_II].end_time = datetime.now()
        checkpoint_state.mark_case_complete(CaseType.CASE_II)
        self.checkpoint.save(checkpoint_state)
    
    def _write_results(self, result: DataFrame):
        """Write results to tables."""
        result.createOrReplaceTempView("pipeline_result")
        
        primary = self.config.primary_key
        partition = self.config.partition_key
        
        # MERGE to summary
        self.spark.sql(f"""
            MERGE INTO {self.config.summary_table} t
            USING pipeline_result s
            ON t.{primary} = s.{primary} AND t.{partition} = s.{partition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # MERGE to latest_summary
        latest = self.spark.sql(f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {primary} ORDER BY {partition} DESC
                ) as rn FROM pipeline_result
            ) WHERE rn = 1
        """).drop("rn")
        
        latest.createOrReplaceTempView("latest_result")
        
        self.spark.sql(f"""
            MERGE INTO {self.config.latest_summary_table} t
            USING latest_result s
            ON t.{primary} = s.{primary}
            WHEN MATCHED AND s.{partition} >= t.{partition} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        count = result.count()
        self.metrics.records_written += count
    
    def _log_summary(self):
        """Log processing summary."""
        logger.info("=" * 80)
        logger.info("PROCESSING COMPLETE - SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Run ID: {self.metrics.run_id}")
        logger.info(f"Duration: {self.metrics.duration_minutes:.2f} minutes")
        logger.info(f"Total records: {self.metrics.total_records:,}")
        logger.info(f"Records written: {self.metrics.records_written:,}")
        logger.info(f"Throughput: {self.metrics.overall_throughput:,.0f} records/sec")
        logger.info("-" * 40)
        logger.info("Case breakdown:")
        for case_type, case_metrics in self.metrics.case_metrics.items():
            if case_metrics.record_count > 0:
                duration = case_metrics.duration_seconds or 0
                logger.info(f"  {case_type.value}: {case_metrics.record_count:,} records ({duration:.1f}s)")
        logger.info("-" * 40)
        logger.info(f"Optimizations used:")
        logger.info(f"  Bloom filter: {self.metrics.bloom_filter_hits > 0}")
        logger.info(f"  Parallel cases: {self.metrics.parallel_cases_used}")
        logger.info("=" * 80)
