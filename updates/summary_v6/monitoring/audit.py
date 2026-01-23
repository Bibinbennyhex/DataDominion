"""
Summary Pipeline v6.0 - Audit Logger
=====================================

Full audit trail for pipeline operations.
"""

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from core.config import PipelineConfig
from core.types import BatchInfo, ProcessingStats, CaseType

logger = logging.getLogger("SummaryPipeline.Audit")


class AuditLogger:
    """
    Logs all pipeline operations to a dedicated audit table.
    
    Provides:
    - Per-batch audit records
    - Run-level summaries
    - Historical analysis
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.enabled = config.audit.enabled
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Create audit table if it doesn't exist."""
        if not self.enabled:
            return
        
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.config.audit_table} (
                    run_id STRING,
                    batch_id STRING,
                    case_type STRING,
                    partition_key STRING,
                    status STRING,
                    record_count BIGINT,
                    rows_written BIGINT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    duration_seconds DOUBLE,
                    error_message STRING,
                    retry_count INT,
                    created_at TIMESTAMP
                ) USING iceberg
                PARTITIONED BY (days(created_at))
            """)
            logger.debug(f"Audit table ready: {self.config.audit_table}")
        except Exception as e:
            logger.warning(f"Could not create audit table: {e}")
    
    def log_batch(self, run_id: str, batch_info: BatchInfo, rows_written: int = 0):
        """
        Log a batch operation.
        
        Args:
            run_id: Pipeline run identifier
            batch_info: Batch information
            rows_written: Number of rows written
        """
        if not self.enabled:
            return
        
        try:
            duration = batch_info.duration_seconds or 0
            
            self.spark.sql(f"""
                INSERT INTO {self.config.audit_table} VALUES (
                    '{run_id}',
                    '{batch_info.batch_id}',
                    '{batch_info.case_type.value}',
                    {f"'{batch_info.partition_key}'" if batch_info.partition_key else 'NULL'},
                    '{batch_info.status.value}',
                    {batch_info.record_count},
                    {rows_written},
                    {f"TIMESTAMP '{batch_info.start_time}'" if batch_info.start_time else 'NULL'},
                    {f"TIMESTAMP '{batch_info.end_time}'" if batch_info.end_time else 'NULL'},
                    {duration},
                    {f"'{batch_info.error_message}'" if batch_info.error_message else 'NULL'},
                    {batch_info.retry_count},
                    current_timestamp()
                )
            """)
            
            logger.debug(f"Audit logged: run={run_id}, batch={batch_info.batch_id}")
            
        except Exception as e:
            logger.warning(f"Could not log audit: {e}")
    
    def log_run_summary(self, stats: ProcessingStats):
        """
        Log a run-level summary.
        
        Args:
            stats: Processing statistics
        """
        if not self.enabled:
            return
        
        logger.info("=" * 60)
        logger.info(f"AUDIT SUMMARY: Run {stats.run_id}")
        logger.info("=" * 60)
        logger.info(f"Mode: {stats.mode.value}")
        logger.info(f"Duration: {stats.duration_minutes:.2f} minutes")
        logger.info(f"Total records: {stats.total_records:,}")
        logger.info(f"  Case I: {stats.case_i_records:,}")
        logger.info(f"  Case II: {stats.case_ii_records:,}")
        logger.info(f"  Case III: {stats.case_iii_records:,}")
        logger.info(f"Batches: {stats.batches_completed}/{stats.batches_total} completed")
        logger.info(f"Success rate: {stats.success_rate:.1f}%")
        logger.info("=" * 60)
    
    def get_run_history(self, run_id: str):
        """
        Get audit history for a run.
        
        Args:
            run_id: Pipeline run identifier
            
        Returns:
            DataFrame with audit records
        """
        return self.spark.sql(f"""
            SELECT *
            FROM {self.config.audit_table}
            WHERE run_id = '{run_id}'
            ORDER BY started_at
        """)
    
    def get_recent_runs(self, limit: int = 10):
        """
        Get recent run summaries.
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            DataFrame with run summaries
        """
        return self.spark.sql(f"""
            SELECT 
                run_id,
                COUNT(*) as batch_count,
                SUM(record_count) as total_records,
                SUM(rows_written) as total_written,
                MIN(started_at) as run_started,
                MAX(completed_at) as run_completed,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_batches,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_batches
            FROM {self.config.audit_table}
            GROUP BY run_id
            ORDER BY run_started DESC
            LIMIT {limit}
        """)
    
    def cleanup_old_audits(self, days_to_keep: Optional[int] = None):
        """
        Remove old audit records.
        
        Args:
            days_to_keep: Days to retain (defaults to config value)
        """
        if not self.enabled:
            return
        
        days = days_to_keep or self.config.audit.retain_days
        
        try:
            self.spark.sql(f"""
                DELETE FROM {self.config.audit_table}
                WHERE created_at < current_timestamp() - INTERVAL {days} DAYS
            """)
            logger.info(f"Cleaned up audit records older than {days} days")
        except Exception as e:
            logger.warning(f"Could not cleanup audit records: {e}")
