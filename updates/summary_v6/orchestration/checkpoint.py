"""
Summary Pipeline v6.0 - Checkpoint Manager
===========================================

Checkpoint management for resume capability.
"""

import json
import logging
from datetime import datetime
from typing import Optional, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from core.config import PipelineConfig
from core.types import CheckpointState, CaseType, ProcessingMode

logger = logging.getLogger("SummaryPipeline.Checkpoint")


class CheckpointManager:
    """
    Manages checkpoints for resume capability.
    
    Persists checkpoint state to an Iceberg table, allowing pipeline
    to resume from the last successful point after failures.
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.enabled = config.resilience.checkpoint_enabled
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Create checkpoint table if it doesn't exist."""
        if not self.enabled:
            return
        
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.config.checkpoint_table} (
                    run_id STRING,
                    mode STRING,
                    current_case STRING,
                    completed_cases STRING,
                    current_batch_id STRING,
                    completed_batch_ids STRING,
                    last_updated TIMESTAMP,
                    is_active BOOLEAN
                ) USING iceberg
            """)
            logger.debug(f"Checkpoint table ready: {self.config.checkpoint_table}")
        except Exception as e:
            logger.warning(f"Could not create checkpoint table: {e}")
    
    def save_checkpoint(self, state: CheckpointState):
        """
        Save checkpoint state.
        
        Args:
            state: Current checkpoint state
        """
        if not self.enabled:
            return
        
        try:
            # Deactivate previous checkpoints for this run
            self.spark.sql(f"""
                UPDATE {self.config.checkpoint_table}
                SET is_active = false
                WHERE run_id = '{state.run_id}'
            """)
            
            # Insert new checkpoint
            completed_cases = json.dumps([c.value for c in state.completed_cases])
            completed_batches = json.dumps(state.completed_batch_ids)
            current_case = state.current_case.value if state.current_case else None
            
            self.spark.sql(f"""
                INSERT INTO {self.config.checkpoint_table} VALUES (
                    '{state.run_id}',
                    '{state.mode.value}',
                    {f"'{current_case}'" if current_case else 'NULL'},
                    '{completed_cases}',
                    {f"'{state.current_batch_id}'" if state.current_batch_id else 'NULL'},
                    '{completed_batches}',
                    current_timestamp(),
                    true
                )
            """)
            
            logger.debug(f"Checkpoint saved for run {state.run_id}")
            
        except Exception as e:
            logger.warning(f"Could not save checkpoint: {e}")
    
    def load_checkpoint(self, run_id: str) -> Optional[CheckpointState]:
        """
        Load the latest checkpoint for a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            CheckpointState if found, None otherwise
        """
        if not self.enabled:
            return None
        
        try:
            result = self.spark.sql(f"""
                SELECT *
                FROM {self.config.checkpoint_table}
                WHERE run_id = '{run_id}'
                  AND is_active = true
                ORDER BY last_updated DESC
                LIMIT 1
            """).collect()
            
            if not result:
                return None
            
            row = result[0]
            
            completed_cases = [
                CaseType(c) for c in json.loads(row.completed_cases)
            ]
            completed_batches = json.loads(row.completed_batch_ids)
            
            state = CheckpointState(
                run_id=row.run_id,
                mode=ProcessingMode(row.mode),
                current_case=CaseType(row.current_case) if row.current_case else None,
                completed_cases=completed_cases,
                current_batch_id=row.current_batch_id,
                completed_batch_ids=completed_batches,
                last_updated=row.last_updated
            )
            
            logger.info(f"Loaded checkpoint for run {run_id}")
            logger.info(f"  Completed cases: {[c.value for c in state.completed_cases]}")
            logger.info(f"  Completed batches: {len(state.completed_batch_ids)}")
            
            return state
            
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
            return None
    
    def clear_checkpoint(self, run_id: str):
        """
        Clear checkpoint after successful completion.
        
        Args:
            run_id: Run identifier
        """
        if not self.enabled:
            return
        
        try:
            self.spark.sql(f"""
                UPDATE {self.config.checkpoint_table}
                SET is_active = false
                WHERE run_id = '{run_id}'
            """)
            logger.info(f"Checkpoint cleared for run {run_id}")
        except Exception as e:
            logger.warning(f"Could not clear checkpoint: {e}")
    
    def should_skip_case(
        self,
        state: Optional[CheckpointState],
        case_type: CaseType
    ) -> bool:
        """Check if a case should be skipped based on checkpoint."""
        if state is None:
            return False
        return case_type in state.completed_cases
    
    def should_skip_batch(
        self,
        state: Optional[CheckpointState],
        batch_id: str
    ) -> bool:
        """Check if a batch should be skipped based on checkpoint."""
        if state is None:
            return False
        return batch_id in state.completed_batch_ids
    
    def cleanup_old_checkpoints(self, days_to_keep: int = 7):
        """
        Remove old checkpoint records.
        
        Args:
            days_to_keep: Number of days to retain
        """
        if not self.enabled:
            return
        
        try:
            self.spark.sql(f"""
                DELETE FROM {self.config.checkpoint_table}
                WHERE last_updated < current_timestamp() - INTERVAL {days_to_keep} DAYS
            """)
            logger.info(f"Cleaned up checkpoints older than {days_to_keep} days")
        except Exception as e:
            logger.warning(f"Could not cleanup checkpoints: {e}")
