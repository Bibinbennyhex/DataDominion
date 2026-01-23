"""
Summary Pipeline v6.0 - Case I Processor
=========================================

Processes new accounts (Case I) - creates initial history arrays.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

from core.config import PipelineConfig
from core.types import BatchInfo, BatchStatus, CaseType
from processors.array_builder import ArrayBuilder, generate_grid_column

logger = logging.getLogger("SummaryPipeline.CaseI")


class CaseIProcessor:
    """
    Processes new accounts that have no existing data in the summary table.
    
    Creates initial history arrays with the first value at position 0
    and NULLs for all other positions.
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def process(self, case_i_df: DataFrame) -> DataFrame:
        """
        Process Case I records - create initial arrays.
        
        Args:
            case_i_df: DataFrame with Case I records (classification columns present)
            
        Returns:
            Processed DataFrame ready for writing
        """
        logger.info(f"Processing Case I (new accounts): {case_i_df.count():,} records")
        
        result = case_i_df
        
        # Apply rolling column mapper expressions
        for rc in self.config.rolling_columns:
            result = result.withColumn(
                rc.source_column,
                F.expr(rc.mapper_expr)
            )
        
        # Create initial history arrays
        for rc in self.config.rolling_columns:
            result = result.withColumn(
                rc.history_column_name,
                ArrayBuilder.create_initial_array(rc, rc.source_column)
            )
        
        # Generate grid columns
        for grid_col in self.config.grid_columns:
            result = generate_grid_column(result, grid_col, self.config.rolling_columns)
        
        # Drop classification columns
        columns_to_drop = [
            "month_int", "max_existing_month", "max_existing_ts",
            "max_month_int", "case_type", "month_gap", "MONTH_DIFF"
        ]
        result = result.drop(*[c for c in columns_to_drop if c in result.columns])
        
        logger.info("Case I processing complete - initial arrays created")
        
        return result
    
    def process_batch(
        self,
        case_i_df: DataFrame,
        batch_info: BatchInfo
    ) -> tuple[DataFrame, BatchInfo]:
        """
        Process a batch of Case I records with tracking.
        
        Args:
            case_i_df: DataFrame for this batch
            batch_info: Batch tracking information
            
        Returns:
            Tuple of (processed DataFrame, updated BatchInfo)
        """
        from datetime import datetime
        
        batch_info.status = BatchStatus.RUNNING
        batch_info.start_time = datetime.now()
        
        try:
            result = self.process(case_i_df)
            batch_info.status = BatchStatus.COMPLETED
            batch_info.end_time = datetime.now()
            return result, batch_info
        except Exception as e:
            batch_info.status = BatchStatus.FAILED
            batch_info.end_time = datetime.now()
            batch_info.error_message = str(e)
            raise
