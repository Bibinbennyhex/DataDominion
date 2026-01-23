"""
Summary Pipeline v6.0 - Case II Processor
==========================================

Processes forward entries (Case II) - shifts existing history arrays.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

from core.config import PipelineConfig
from core.types import BatchInfo, BatchStatus, CaseType
from processors.array_builder import ArrayBuilder, generate_grid_column

logger = logging.getLogger("SummaryPipeline.CaseII")


class CaseIIProcessor:
    """
    Processes forward entries where month > max existing month.
    
    Shifts existing history arrays by prepending new value and handling gaps
    (missing months) by inserting NULLs.
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def process(self, case_ii_df: DataFrame) -> DataFrame:
        """
        Process Case II records - shift existing arrays.
        
        Args:
            case_ii_df: DataFrame with Case II records
            
        Returns:
            Processed DataFrame ready for writing
        """
        record_count = case_ii_df.count()
        logger.info(f"Processing Case II (forward entries): {record_count:,} records")
        
        # Get affected accounts
        affected_keys = case_ii_df.select(self.config.primary_key).distinct()
        
        # Get latest summary for affected accounts
        latest_summary = self.spark.read.table(self.config.latest_summary_table).alias("ls")
        latest_for_affected = latest_summary.join(
            affected_keys.alias("ak"),
            F.col(f"ls.{self.config.primary_key}") == F.col(f"ak.{self.config.primary_key}"),
            "inner"
        ).select("ls.*")
        
        logger.info(f"Loaded latest summary for {latest_for_affected.count():,} accounts")
        
        result = case_ii_df
        
        # Apply rolling column mapper expressions
        for rc in self.config.rolling_columns:
            result = result.withColumn(
                rc.source_column,
                F.expr(rc.mapper_expr)
            )
        
        # Join with latest summary
        joined = result.alias("c").join(
            latest_for_affected.alias("p"),
            F.col(f"c.{self.config.primary_key}") == F.col(f"p.{self.config.primary_key}"),
            "inner"
        )
        
        # Build select expressions
        select_exprs = []
        
        # Keep all columns from current except classification columns
        classification_cols = [
            "month_int", "max_existing_month", "max_existing_ts",
            "max_month_int", "case_type", "month_gap"
        ]
        
        for col in case_ii_df.columns:
            if col not in classification_cols:
                select_exprs.append(F.col(f"c.{col}"))
        
        # Add shifted history arrays
        for rc in self.config.rolling_columns:
            select_exprs.append(
                ArrayBuilder.create_shifted_array(
                    rc,
                    rc.source_column,
                    "MONTH_DIFF",
                    "c",
                    "p"
                )
            )
        
        result = joined.select(*select_exprs)
        
        # Drop MONTH_DIFF if present
        if "MONTH_DIFF" in result.columns:
            result = result.drop("MONTH_DIFF")
        
        # Generate grid columns
        for grid_col in self.config.grid_columns:
            result = generate_grid_column(result, grid_col, self.config.rolling_columns)
        
        logger.info("Case II processing complete - arrays shifted")
        
        return result
    
    def process_batch(
        self,
        case_ii_df: DataFrame,
        batch_info: BatchInfo
    ) -> tuple[DataFrame, BatchInfo]:
        """
        Process a batch of Case II records with tracking.
        
        Args:
            case_ii_df: DataFrame for this batch
            batch_info: Batch tracking information
            
        Returns:
            Tuple of (processed DataFrame, updated BatchInfo)
        """
        from datetime import datetime
        
        batch_info.status = BatchStatus.RUNNING
        batch_info.start_time = datetime.now()
        
        try:
            result = self.process(case_ii_df)
            batch_info.status = BatchStatus.COMPLETED
            batch_info.end_time = datetime.now()
            return result, batch_info
        except Exception as e:
            batch_info.status = BatchStatus.FAILED
            batch_info.end_time = datetime.now()
            batch_info.error_message = str(e)
            raise
