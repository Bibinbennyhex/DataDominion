"""
Summary Pipeline v6.0 - Main Pipeline Orchestrator
===================================================

The main entry point for pipeline execution.
"""

import uuid
import time
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

from core.config import PipelineConfig
from core.types import (
    CaseType, ProcessingMode, ProcessingStats, 
    CheckpointState, BatchInfo, BatchStatus
)
from core.session import configure_partitions
from processors import (
    RecordClassifier, CaseIProcessor, 
    CaseIIProcessor, CaseIIIProcessor
)
from orchestration.batch_manager import BatchManager
from orchestration.checkpoint import CheckpointManager
from utils.partitioning import calculate_optimal_partitions
from utils.optimization import TableOptimizer

logger = logging.getLogger("SummaryPipeline.Pipeline")


class SummaryPipeline:
    """
    Main pipeline orchestrator.
    
    Supports four processing modes:
    1. incremental: Auto-detect and handle all cases (PRODUCTION DEFAULT)
    2. initial: First-time load from scratch
    3. forward: Sequential month processing
    4. backfill: Manual backfill with filter
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        
        # Initialize components
        self.classifier = RecordClassifier(spark, config)
        self.case_i_processor = CaseIProcessor(spark, config)
        self.case_ii_processor = CaseIIProcessor(spark, config)
        self.case_iii_processor = CaseIIIProcessor(spark, config)
        self.batch_manager = BatchManager(config)
        self.checkpoint_manager = CheckpointManager(spark, config)
        self.table_optimizer = TableOptimizer(spark, config)
        
        # Stats tracking
        self.stats = None
    
    def run_incremental(self, resume_run_id: Optional[str] = None) -> ProcessingStats:
        """
        Run incremental processing - the production default.
        
        Automatically handles all scenarios in a single run:
        - Case I:   New accounts (first appearance)
        - Case II:  Forward entries (month > max existing)
        - Case III: Backfill (month <= max existing, newer timestamp)
        
        Processing Order (CRITICAL):
        1. Backfill FIRST (rebuilds history with corrections)
        2. New accounts SECOND (no dependencies)
        3. Forward LAST (uses corrected history from backfill)
        
        Args:
            resume_run_id: Optional run ID to resume from checkpoint
            
        Returns:
            ProcessingStats with run summary
        """
        run_id = resume_run_id or str(uuid.uuid4())[:12]
        
        logger.info("=" * 80)
        logger.info("SUMMARY PIPELINE v6.0 - INCREMENTAL MODE")
        logger.info(f"Run ID: {run_id}")
        logger.info("=" * 80)
        
        # Initialize stats
        self.stats = ProcessingStats(
            run_id=run_id,
            mode=ProcessingMode.INCREMENTAL,
            start_time=datetime.now()
        )
        
        # Check for checkpoint to resume
        checkpoint_state = None
        if resume_run_id:
            checkpoint_state = self.checkpoint_manager.load_checkpoint(resume_run_id)
            if checkpoint_state:
                logger.info(f"Resuming from checkpoint: {len(checkpoint_state.completed_cases)} cases completed")
        
        # Step 1: Get latest timestamp from summary
        max_ts = self._get_max_summary_timestamp()
        
        if max_ts is None:
            logger.warning("No existing summary found. Use 'initial' mode for first load.")
            return self.stats
        
        logger.info(f"Latest timestamp in summary: {max_ts}")
        
        # Step 2: Read new data
        logger.info("Reading new data from source...")
        batch_df = self.spark.sql(f"""
            SELECT * FROM {self.config.source_table}
            WHERE {self.config.timestamp_key} > TIMESTAMP '{max_ts}'
        """)
        
        record_count = batch_df.count()
        if record_count == 0:
            logger.info("No new records to process. Summary is up to date.")
            self.stats.end_time = datetime.now()
            return self.stats
        
        self.stats.total_records = record_count
        logger.info(f"New records found: {record_count:,}")
        
        # Configure partitioning
        num_partitions = calculate_optimal_partitions(record_count, self.config)
        configure_partitions(self.spark, num_partitions)
        
        # Step 3: Prepare and classify
        logger.info("Preparing and classifying records...")
        batch_df = self._prepare_source_data(batch_df)
        classified = self.classifier.classify(batch_df)
        
        # Get counts
        self.stats.case_i_records = self.classifier.get_case_count(classified, CaseType.CASE_I)
        self.stats.case_ii_records = self.classifier.get_case_count(classified, CaseType.CASE_II)
        self.stats.case_iii_records = self.classifier.get_case_count(classified, CaseType.CASE_III)
        
        # Create/update checkpoint state
        if checkpoint_state is None:
            checkpoint_state = CheckpointState(
                run_id=run_id,
                mode=ProcessingMode.INCREMENTAL
            )
        
        # Step 4: Process in CORRECT order
        # CRITICAL: Backfill FIRST, then New, then Forward
        
        # 4a. Process Case III (Backfill) - HIGHEST PRIORITY
        if not self.checkpoint_manager.should_skip_case(checkpoint_state, CaseType.CASE_III):
            if self.classifier.has_records(classified, CaseType.CASE_III):
                logger.info("=" * 80)
                logger.info(f"STEP 1/3: Processing {self.stats.case_iii_records:,} backfill records (Case III)")
                logger.info("=" * 80)
                
                checkpoint_state.current_case = CaseType.CASE_III
                self.checkpoint_manager.save_checkpoint(checkpoint_state)
                
                case_iii_df = self.classifier.get_case_dataframe(classified, CaseType.CASE_III)
                self._process_case_iii(case_iii_df, checkpoint_state)
                
                checkpoint_state.mark_case_complete(CaseType.CASE_III)
                self.checkpoint_manager.save_checkpoint(checkpoint_state)
        else:
            logger.info("Case III already completed (from checkpoint)")
        
        # 4b. Process Case I (New Accounts)
        if not self.checkpoint_manager.should_skip_case(checkpoint_state, CaseType.CASE_I):
            if self.classifier.has_records(classified, CaseType.CASE_I):
                logger.info("=" * 80)
                logger.info(f"STEP 2/3: Processing {self.stats.case_i_records:,} new accounts (Case I)")
                logger.info("=" * 80)
                
                checkpoint_state.current_case = CaseType.CASE_I
                self.checkpoint_manager.save_checkpoint(checkpoint_state)
                
                case_i_df = self.classifier.get_case_dataframe(classified, CaseType.CASE_I)
                self._process_case_i(case_i_df, checkpoint_state)
                
                checkpoint_state.mark_case_complete(CaseType.CASE_I)
                self.checkpoint_manager.save_checkpoint(checkpoint_state)
        else:
            logger.info("Case I already completed (from checkpoint)")
        
        # 4c. Process Case II (Forward Entries) - LOWEST PRIORITY
        if not self.checkpoint_manager.should_skip_case(checkpoint_state, CaseType.CASE_II):
            if self.classifier.has_records(classified, CaseType.CASE_II):
                logger.info("=" * 80)
                logger.info(f"STEP 3/3: Processing {self.stats.case_ii_records:,} forward entries (Case II)")
                logger.info("=" * 80)
                
                checkpoint_state.current_case = CaseType.CASE_II
                self.checkpoint_manager.save_checkpoint(checkpoint_state)
                
                case_ii_df = self.classifier.get_case_dataframe(classified, CaseType.CASE_II)
                self._process_case_ii(case_ii_df, checkpoint_state)
                
                checkpoint_state.mark_case_complete(CaseType.CASE_II)
                self.checkpoint_manager.save_checkpoint(checkpoint_state)
        else:
            logger.info("Case II already completed (from checkpoint)")
        
        # Cleanup
        classified.unpersist()
        
        # Step 5: Finalize
        logger.info("=" * 80)
        logger.info("FINALIZING")
        logger.info("=" * 80)
        
        self.table_optimizer.optimize_tables()
        self.checkpoint_manager.clear_checkpoint(run_id)
        
        self.stats.end_time = datetime.now()
        
        # Log summary
        logger.info("=" * 80)
        logger.info("INCREMENTAL PROCESSING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Total records processed: {self.stats.total_records:,}")
        logger.info(f"  Case I (new):      {self.stats.case_i_records:,}")
        logger.info(f"  Case II (forward): {self.stats.case_ii_records:,}")
        logger.info(f"  Case III (backfill): {self.stats.case_iii_records:,}")
        logger.info(f"Duration: {self.stats.duration_minutes:.2f} minutes")
        logger.info("=" * 80)
        
        return self.stats
    
    def _get_max_summary_timestamp(self):
        """Get max timestamp from summary table."""
        try:
            result = self.spark.sql(f"""
                SELECT MAX({self.config.timestamp_key}) as max_ts
                FROM {self.config.summary_table}
            """).collect()
            return result[0]["max_ts"] if result else None
        except Exception:
            return None
    
    def _prepare_source_data(self, df: DataFrame) -> DataFrame:
        """Apply column mappings and transformations."""
        # Build select expressions from column mapping
        select_exprs = [
            F.col(src).alias(dst) 
            for src, dst in self.config.column_mappings.items()
        ]
        
        result = df.select(*select_exprs)
        
        # Deduplicate by primary key + partition, keeping latest
        window_spec = Window.partitionBy(
            self.config.primary_key,
            self.config.partition_key
        ).orderBy(F.col(self.config.timestamp_key).desc())
        
        result = result \
            .withColumn("_rn", F.row_number().over(window_spec)) \
            .filter(F.col("_rn") == 1) \
            .drop("_rn")
        
        # Apply transformations
        for transform in self.config.transformations:
            result = result.withColumn(
                transform['name'],
                F.expr(transform['expr'])
            )
        
        return result
    
    def _process_case_i(self, df: DataFrame, checkpoint_state: CheckpointState):
        """Process Case I with batch management."""
        result = self.case_i_processor.process(df)
        self._write_results(result)
        self.stats.batches_completed += 1
    
    def _process_case_ii(self, df: DataFrame, checkpoint_state: CheckpointState):
        """Process Case II with batch management."""
        result = self.case_ii_processor.process(df)
        self._write_results(result)
        self.stats.batches_completed += 1
    
    def _process_case_iii(self, df: DataFrame, checkpoint_state: CheckpointState):
        """Process Case III with batch management."""
        result = self.case_iii_processor.process(df)
        self._write_results(result)
        self.stats.batches_completed += 1
    
    def _write_results(self, result: DataFrame):
        """Write results to summary and latest_summary tables."""
        result.createOrReplaceTempView("pipeline_result")
        
        primary = self.config.primary_key
        partition = self.config.partition_key
        
        # MERGE to summary table
        logger.info(f"Merging to {self.config.summary_table}...")
        self.spark.sql(f"""
            MERGE INTO {self.config.summary_table} t
            USING pipeline_result s
            ON t.{primary} = s.{primary} AND t.{partition} = s.{partition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # MERGE to latest_summary (only latest month per account)
        logger.info(f"Merging to {self.config.latest_summary_table}...")
        
        latest_per_account = self.spark.sql(f"""
            SELECT * FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY {primary}
                        ORDER BY {partition} DESC
                    ) as rn
                FROM pipeline_result
            ) WHERE rn = 1
        """).drop("rn")
        
        latest_per_account.createOrReplaceTempView("latest_result")
        
        self.spark.sql(f"""
            MERGE INTO {self.config.latest_summary_table} t
            USING latest_result s
            ON t.{primary} = s.{primary}
            WHEN MATCHED AND s.{partition} >= t.{partition} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        count = result.count()
        self.stats.records_written += count
        logger.info(f"Written {count:,} records")
