"""
Summary Pipeline v7.0 - Checkpoint Manager
"""

import json
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from core.config import PipelineConfig
from core.types import CheckpointState, CaseType, ProcessingMode

logger = logging.getLogger("SummaryPipeline.Checkpoint")


class CheckpointManager:
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.enabled = config.resilience.checkpoint_enabled
        self._ensure_table()
    
    def _ensure_table(self):
        if not self.enabled:
            return
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.config.checkpoint_table} (
                    run_id STRING, mode STRING, current_case STRING,
                    completed_cases STRING, completed_batches STRING,
                    last_updated TIMESTAMP, is_active BOOLEAN
                ) USING iceberg
            """)
        except Exception as e:
            logger.warning(f"Could not create checkpoint table: {e}")
    
    def save(self, state: CheckpointState):
        if not self.enabled:
            return
        try:
            self.spark.sql(f"UPDATE {self.config.checkpoint_table} SET is_active = false WHERE run_id = '{state.run_id}'")
            
            completed_cases = json.dumps([c.value for c in state.completed_cases])
            completed_batches = json.dumps(state.completed_batches)
            current_case = f"'{state.current_case.value}'" if state.current_case else "NULL"
            
            self.spark.sql(f"""
                INSERT INTO {self.config.checkpoint_table} VALUES (
                    '{state.run_id}', '{state.mode.value}', {current_case},
                    '{completed_cases}', '{completed_batches}', current_timestamp(), true
                )
            """)
        except Exception as e:
            logger.warning(f"Could not save checkpoint: {e}")
    
    def load(self, run_id: str) -> Optional[CheckpointState]:
        if not self.enabled:
            return None
        try:
            result = self.spark.sql(f"""
                SELECT * FROM {self.config.checkpoint_table}
                WHERE run_id = '{run_id}' AND is_active = true
                ORDER BY last_updated DESC LIMIT 1
            """).collect()
            
            if not result:
                return None
            
            row = result[0]
            return CheckpointState(
                run_id=row.run_id,
                mode=ProcessingMode(row.mode),
                current_case=CaseType(row.current_case) if row.current_case else None,
                completed_cases=[CaseType(c) for c in json.loads(row.completed_cases)],
                completed_batches=json.loads(row.completed_batches),
                last_updated=row.last_updated
            )
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
            return None
    
    def clear(self, run_id: str):
        if not self.enabled:
            return
        try:
            self.spark.sql(f"UPDATE {self.config.checkpoint_table} SET is_active = false WHERE run_id = '{run_id}'")
        except:
            pass
    
    def should_skip_case(self, state: Optional[CheckpointState], case_type: CaseType) -> bool:
        return state is not None and case_type in state.completed_cases
