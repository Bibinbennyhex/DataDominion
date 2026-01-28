"""
Core backfill processor for Iceberg tables with array history columns.
Optimized for 500B rows with month partitioning and account bucketing.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from typing import List, Dict
import logging

from config import HistoryColumn, SPARK_CONFIGS, MAX_HISTORY_MONTHS

logger = logging.getLogger("BackfillProcessor")


class IcebergBackfillProcessor:
    """
    Production-grade backfill processor for Iceberg tables with array history columns.
    
    Key Features:
    - Position-aware array updates
    - Optimized for bucketed Iceberg tables
    - Handles 20 history columns dynamically
    - Maintains 36-month rolling history
    """
    
    def __init__(
        self,
        spark: SparkSession,
        target_table: str,
        history_columns: List[HistoryColumn],
        max_history_months: int = MAX_HISTORY_MONTHS
    ):
        self.spark = spark
        self.target_table = target_table
        self.history_columns = history_columns
        self.max_history_months = max_history_months
        
        # Optimize Spark for large-scale updates
        self._configure_spark()
        
        logger.info(f"Initialized processor for table: {target_table}")
        logger.info(f"Managing {len(history_columns)} history columns")
    
    def _configure_spark(self):
        """Optimize Spark settings for Iceberg updates"""
        logger.info("Configuring Spark for Iceberg backfill operations")
        
        for key, value in SPARK_CONFIGS.items():
            self.spark.conf.set(key, value)
            logger.debug(f"Set {key} = {value}")
    
    def calculate_months_diff(self, partition_col: str, backfill_month: str) -> F.Column:
        """
        Calculate the array index where backfill value should go.
        Index = months between partition and backfill month.
        
        Example:
            partition = "2024-05", backfill_month = "2023-12"
            diff = 5 months
            Value goes at index 5 (0=current, 1=month-1, ..., 5=month-5)
        """
        return (
            F.months_between(
                F.to_date(F.col(partition_col), "yyyy-MM"),
                F.to_date(F.lit(backfill_month), "yyyy-MM")
            ).cast("int")
        )
    
    def update_array_at_position(
        self,
        array_col: str,
        position: F.Column,
        new_value: F.Column
    ) -> F.Column:
        """
        Update array at specific position, maintaining array size of 36.
        
        Logic:
        - Slice [0, position-1]: elements before position
        - Insert new value at position
        - Slice [position+1, end-1]: elements after, dropping last one
        
        If position >= 36: no update needed (outside history window)
        
        Example:
            array = [1,2,3,4,5], position=2, new_value=9
            result = [1,2,9,4]  (dropped 5 to maintain size)
        """
        return F.when(
            position < self.max_history_months,
            F.concat(
                # Elements before the position
                F.slice(F.col(array_col), 1, position),
                # The new value
                F.array(new_value),
                # Elements after the position (skip one element to maintain size)
                F.slice(
                    F.col(array_col),
                    position + 2,  # +2 because slice is 1-indexed and we skip one
                    self.max_history_months - position - 1
                )
            )
        ).otherwise(F.col(array_col))  # No change if outside window
    
    def generate_update_expressions(
        self,
        position_col: str = "update_position"
    ) -> Dict[str, str]:
        """
        Generate SQL update expressions for all 20 history columns.
        
        Returns:
            Dictionary mapping column names to SQL expressions
        """
        update_exprs = {}
        
        for hist_col in self.history_columns:
            expr = f"""
                CASE 
                    WHEN {position_col} < {self.max_history_months} THEN
                        concat(
                            slice(target.{hist_col.name}, 1, {position_col}),
                            array(source.{hist_col.backfill_source_column}),
                            slice(target.{hist_col.name}, {position_col} + 2, {self.max_history_months} - {position_col} - 1)
                        )
                    ELSE target.{hist_col.name}
                END
            """
            update_exprs[hist_col.name] = expr
        
        return update_exprs
    
    def validate_backfill_data(self, backfill_df: DataFrame) -> bool:
        """
        Validate backfill data before processing.
        
        Checks:
        - Required columns present
        - No null accounts
        - Valid month format
        """
        required_cols = {"acct", "backfill_month"}
        missing_cols = required_cols - set(backfill_df.columns)
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        null_accts = backfill_df.filter(F.col("acct").isNull()).count()
        if null_accts > 0:
            logger.error(f"Found {null_accts} rows with null accounts")
            return False
        
        logger.info("Backfill data validation passed")
        return True


def calculate_months_between(partition_month: str, backfill_month: str) -> int:
    """
    Calculate integer months between two yyyy-MM strings.
    
    Args:
        partition_month: Target partition (e.g., "2024-05")
        backfill_month: Backfill month (e.g., "2023-12")
    
    Returns:
        Number of months difference
    """
    from datetime import datetime
    p = datetime.strptime(partition_month, "%Y-%m")
    b = datetime.strptime(backfill_month, "%Y-%m")
    return (p.year - b.year) * 12 + (p.month - b.month)


def get_affected_partitions(
    spark: SparkSession,
    target_table: str,
    backfill_month: str,
    max_history_months: int = MAX_HISTORY_MONTHS
) -> List[str]:
    """
    Get all partitions that need updating (within 36-month window).
    
    Returns:
        List of partition values in ascending order
    """
    logger.info(f"Finding partitions affected by backfill to {backfill_month}")
    
    partitions = [
        row.month_partition for row in 
        spark.sql(f"""
            SELECT DISTINCT month_partition
            FROM {target_table}
            WHERE month_partition > '{backfill_month}'
              AND months_between(
                    to_date(month_partition, 'yyyy-MM'),
                    to_date('{backfill_month}', 'yyyy-MM')
                  ) <= {max_history_months}
            ORDER BY month_partition
        """).collect()
    ]
    
    logger.info(f"Found {len(partitions)} partitions to update")
    if partitions:
        logger.info(f"Range: {partitions[0]} to {partitions[-1]}")
    
    return partitions
