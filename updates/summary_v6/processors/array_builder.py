"""
Summary Pipeline v6.0 - Array Builder
======================================

Rolling column transformations for building and updating history arrays.
"""

from typing import List, Optional
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DecimalType, DateType

from core.types import RollingColumn, DataType, GridColumn

import logging

logger = logging.getLogger("SummaryPipeline.ArrayBuilder")


class ArrayBuilder:
    """
    Build and transform rolling history arrays.
    
    Handles:
    - Initial array creation: [value, NULL, NULL, ...]
    - Normal shift: [new, prev[0:n-1]]
    - Gap handling: [new, NULLs for gap, prev[0:n-gap-1]]
    - Position-aware backfill updates
    """
    
    TYPE_MAP = {
        DataType.INTEGER: IntegerType(),
        DataType.STRING: StringType(),
        DataType.DECIMAL: DecimalType(18, 2),
        DataType.DATE: DateType(),
    }
    
    @classmethod
    def get_spark_type(cls, data_type: DataType):
        """Get Spark type from DataType enum."""
        return cls.TYPE_MAP.get(data_type, StringType())
    
    @classmethod
    def create_initial_array(
        cls,
        rolling_col: RollingColumn,
        value_column: str
    ) -> Column:
        """
        Create initial history array: [value, NULL, NULL, ...].
        
        Args:
            rolling_col: Rolling column definition
            value_column: Name of column containing the value
            
        Returns:
            Column expression for initial history array
        """
        spark_type = cls.get_spark_type(rolling_col.data_type)
        num_cols = rolling_col.history_length
        
        return F.array(
            F.col(value_column).cast(spark_type),
            *[F.lit(None).cast(spark_type) for _ in range(num_cols - 1)]
        ).alias(rolling_col.history_column_name)
    
    @classmethod
    def create_shifted_array(
        cls,
        rolling_col: RollingColumn,
        value_column: str,
        month_diff_column: str = "MONTH_DIFF",
        current_alias: str = "c",
        previous_alias: str = "p"
    ) -> Column:
        """
        Create shifted history array with gap handling.
        
        Logic:
        - MONTH_DIFF == 1: Normal shift [new, prev[0:n-1]]
        - MONTH_DIFF > 1:  Gap handling [new, NULLs for gap, prev[0:n-gap-1]]
        - Otherwise:       New account [new, NULL, NULL, ...]
        
        Args:
            rolling_col: Rolling column definition
            value_column: Name of column containing new value
            month_diff_column: Name of column containing month difference
            current_alias: Alias for current record
            previous_alias: Alias for previous record
            
        Returns:
            Column expression for shifted history array
        """
        spark_type = cls.get_spark_type(rolling_col.data_type)
        num_cols = rolling_col.history_length
        history_col = rolling_col.history_column_name
        
        # Current value with NULL fallback
        current_value = F.coalesce(
            F.col(f"{current_alias}.{value_column}").cast(spark_type),
            F.lit(None).cast(spark_type)
        )
        
        # Previous history with empty array fallback
        prev_history = F.coalesce(
            F.col(f"{previous_alias}.{history_col}"),
            F.array(*[F.lit(None).cast(spark_type) for _ in range(num_cols)])
        )
        
        # Null array generator for gap filling
        null_array = F.array_repeat(
            F.lit(None).cast(spark_type),
            F.col(month_diff_column) - F.lit(1)
        )
        
        # New account array (no previous history)
        new_account_array = F.array(
            current_value,
            *[F.lit(None).cast(spark_type) for _ in range(num_cols - 1)]
        )
        
        return (
            F.when(
                F.col(month_diff_column) == 1,
                # Normal: prepend current, keep n-1 from previous
                F.slice(
                    F.concat(F.array(current_value), prev_history),
                    1, num_cols
                )
            )
            .when(
                F.col(month_diff_column) > 1,
                # Gap: prepend current, add null gaps, then previous
                F.slice(
                    F.concat(
                        F.array(current_value),
                        null_array,
                        prev_history
                    ),
                    1, num_cols
                )
            )
            .otherwise(new_account_array)
            .alias(history_col)
        )
    
    @classmethod
    def update_array_at_position(
        cls,
        rolling_col: RollingColumn,
        array_column: str,
        position: Column,
        new_value: Column
    ) -> Column:
        """
        Update array at specific position for backfill operations.
        
        Maintains array size by:
        - Taking elements before position
        - Inserting new value at position
        - Taking elements after, dropping last
        
        Args:
            rolling_col: Rolling column definition
            array_column: Name of array column to update
            position: Column expression for update position
            new_value: Column expression for new value
            
        Returns:
            Column expression for updated array
        """
        max_len = rolling_col.history_length
        
        return F.when(
            position < max_len,
            F.concat(
                # Elements before the position
                F.slice(F.col(array_column), 1, position),
                # The new value
                F.array(new_value),
                # Elements after the position (skip one to maintain size)
                F.slice(
                    F.col(array_column),
                    position + 2,  # +2 because slice is 1-indexed and we skip one
                    max_len - position - 1
                )
            )
        ).otherwise(F.col(array_column))  # No change if outside window
    
    @classmethod
    def rebuild_history_array(
        cls,
        rolling_col: RollingColumn,
        value_column: str,
        sql_type: str = "INT"
    ) -> str:
        """
        Generate SQL for rebuilding entire history array from collected values.
        
        Used for Case III backfill where we need to reconstruct history
        from all available data points.
        
        Args:
            rolling_col: Rolling column definition
            value_column: Name of value column in collected struct
            sql_type: SQL type for casting
            
        Returns:
            SQL expression string for history array reconstruction
        """
        num_cols = rolling_col.history_length
        
        return f"""
            TRANSFORM(
                SEQUENCE(0, {num_cols - 1}),
                offset -> (
                    AGGREGATE(
                        FILTER(e.month_values, mv -> mv.month_int = e.month_int - offset),
                        CAST(NULL AS {sql_type}),
                        (acc, x) -> x.{value_column}
                    )
                )
            ) as {rolling_col.history_column_name}
        """


def generate_grid_column(
    df: DataFrame,
    grid_col: GridColumn,
    rolling_columns: List[RollingColumn]
) -> DataFrame:
    """
    Generate grid column from rolling history array.
    
    Transforms array like ["0", "1", NULL, "2", ...] 
    into string like "01?2..." with placeholder for NULLs.
    
    Args:
        df: Input DataFrame
        grid_col: Grid column definition
        rolling_columns: List of rolling column definitions
        
    Returns:
        DataFrame with grid column added
    """
    # Find the source rolling column
    source_rc = None
    for rc in rolling_columns:
        if rc.name == grid_col.source_rolling:
            source_rc = rc
            break
    
    if source_rc is None:
        logger.warning(f"Source rolling column {grid_col.source_rolling} not found for grid {grid_col.name}")
        return df
    
    history_col = source_rc.history_column_name
    
    if history_col not in df.columns:
        logger.warning(f"History column {history_col} not found, skipping grid generation")
        return df
    
    # Transform array to string grid
    df = df.withColumn(
        grid_col.name,
        F.concat_ws(
            grid_col.separator,
            F.transform(
                F.col(history_col),
                lambda x: F.coalesce(x.cast(StringType()), F.lit(grid_col.null_placeholder))
            )
        )
    )
    
    return df
