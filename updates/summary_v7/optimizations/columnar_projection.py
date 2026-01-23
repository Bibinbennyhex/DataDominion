"""
Summary Pipeline v7.0 - Columnar Projection
============================================

Minimizes I/O by reading only required columns.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import List, Set
import logging

from core.config import PipelineConfig
from core.types import CaseType

logger = logging.getLogger("SummaryPipeline.ColumnarProjection")


class ColumnarProjectionOptimizer:
    """
    Optimizes reads by projecting only required columns.
    
    Different cases require different column subsets:
    - Case I: All columns (new records)
    - Case II: Primary key, partition, history arrays
    - Case III: Primary key, partition, timestamp, history arrays
    """
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.enabled = config.optimization.columnar_projection
    
    def get_columns_for_case(self, case_type: CaseType) -> List[str]:
        """
        Get minimal column list for a case type.
        
        Args:
            case_type: The case type
            
        Returns:
            List of column names
        """
        base_cols = [
            self.config.primary_key,
            self.config.partition_key,
            self.config.timestamp_key
        ]
        
        if case_type == CaseType.CASE_I:
            # New accounts need all columns
            return list(self.config.column_mappings.values())
        
        elif case_type == CaseType.CASE_II:
            # Forward entries need key + history arrays
            return base_cols + self.config.history_column_names
        
        elif case_type == CaseType.CASE_III:
            # Backfill needs key + timestamp + history arrays
            return base_cols + self.config.history_column_names
        
        return base_cols
    
    def read_with_projection(
        self,
        spark: SparkSession,
        table: str,
        case_type: CaseType,
        additional_columns: List[str] = None
    ) -> DataFrame:
        """
        Read table with column projection.
        
        Args:
            spark: SparkSession
            table: Table name
            case_type: Case type for column selection
            additional_columns: Extra columns to include
            
        Returns:
            DataFrame with projected columns
        """
        columns = self.get_columns_for_case(case_type)
        
        if additional_columns:
            columns = list(set(columns + additional_columns))
        
        if self.enabled:
            logger.info(f"Reading {table} with {len(columns)} columns (projection enabled)")
            return spark.read.table(table).select(*columns)
        else:
            logger.info(f"Reading {table} without projection")
            return spark.read.table(table)
    
    def estimate_io_savings(
        self,
        total_columns: int,
        projected_columns: int,
        record_count: int,
        avg_column_size: int = 20
    ) -> dict:
        """
        Estimate I/O savings from projection.
        
        Args:
            total_columns: Total columns in table
            projected_columns: Number of projected columns
            record_count: Number of records
            avg_column_size: Average column size in bytes
            
        Returns:
            Dictionary with savings estimates
        """
        total_bytes = total_columns * record_count * avg_column_size
        projected_bytes = projected_columns * record_count * avg_column_size
        savings = total_bytes - projected_bytes
        
        return {
            "total_bytes": total_bytes,
            "projected_bytes": projected_bytes,
            "savings_bytes": savings,
            "savings_percent": (savings / total_bytes * 100) if total_bytes > 0 else 0,
            "savings_gb": savings / (1024 ** 3)
        }
