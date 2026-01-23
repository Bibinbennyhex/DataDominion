"""
Summary Pipeline v6.0 - Case III Processor
===========================================

Processes backfill entries (Case III) - rebuilds entire history arrays.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

from core.config import PipelineConfig
from core.types import BatchInfo, BatchStatus, CaseType, DataType
from processors.array_builder import ArrayBuilder, generate_grid_column
from processors.classifier import month_to_int_expr

logger = logging.getLogger("SummaryPipeline.CaseIII")


class CaseIIIProcessor:
    """
    Processes backfill entries where month <= max existing month.
    
    This is the most complex case - requires rebuilding entire history arrays
    by combining backfill data with existing summary data, with backfill
    taking priority for conflicts.
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def process(self, case_iii_df: DataFrame) -> DataFrame:
        """
        Process Case III records - rebuild history arrays.
        
        Uses a 7-CTE approach to:
        1. Validate backfill records (newer timestamp wins)
        2. Get all existing data for affected accounts
        3. Combine backfill + existing (backfill has priority)
        4. Deduplicate keeping winner
        5. Collect all months per account
        6. Explode back to one row per month
        7. Rebuild history arrays for each month
        
        Args:
            case_iii_df: DataFrame with Case III records
            
        Returns:
            Processed DataFrame ready for writing
        """
        record_count = case_iii_df.count()
        logger.info(f"Processing Case III (backfill): {record_count:,} records")
        
        primary = self.config.primary_key
        partition = self.config.partition_key
        timestamp = self.config.timestamp_key
        history_len = self.config.history_length
        
        # Get affected accounts
        affected_accounts = case_iii_df.select(primary).distinct()
        affected_count = affected_accounts.count()
        
        # Get affected months
        affected_months = case_iii_df.select(partition).distinct().count()
        
        logger.info(f"Affected: {affected_count:,} accounts, {affected_months} unique months")
        logger.info("Rebuilding history arrays with corrected values...")
        
        # Apply rolling column mapper expressions
        result = case_iii_df
        for rc in self.config.rolling_columns:
            result = result.withColumn(
                rc.source_column,
                F.expr(rc.mapper_expr)
            )
        
        # Create temp views
        result.createOrReplaceTempView("backfill_records")
        affected_accounts.createOrReplaceTempView("affected_accounts")
        
        # Identify columns to preserve (excluding classification/key columns)
        exclude_cols = [
            primary, partition, timestamp, "month_int",
            "case_type", "month_gap", "max_existing_month",
            "max_existing_ts", "max_month_int", "MONTH_DIFF"
        ]
        preserve_cols = [c for c in result.columns if c not in exclude_cols]
        
        # Build rolling column SQL for history array rebuild
        rolling_col_history = []
        for rc in self.config.rolling_columns:
            sql_type = "INT" if rc.data_type == DataType.INTEGER else "STRING"
            rolling_col_history.append(f"""
                TRANSFORM(
                    SEQUENCE(0, {history_len - 1}),
                    offset -> (
                        AGGREGATE(
                            FILTER(e.month_values, mv -> mv.month_int = e.month_int - offset),
                            CAST(NULL AS {sql_type}),
                            (acc, x) -> x.{rc.source_column}
                        )
                    )
                ) as {rc.history_column_name}
            """)
        
        # Build scalar extraction SQL for non-rolling columns
        scalar_extract = []
        for col in preserve_cols:
            if col not in [rc.source_column for rc in self.config.rolling_columns]:
                scalar_extract.append(
                    f"element_at(FILTER(e.month_values, mv -> mv.month_int = e.month_int), 1).{col} as {col}"
                )
        
        struct_cols = ", ".join(preserve_cols)
        
        # Build existing data column selection
        existing_cols = []
        rolling_sources = [rc.source_column for rc in self.config.rolling_columns]
        
        for col in preserve_cols:
            if col in rolling_sources:
                # Get value from history array position 0
                rc = next(rc for rc in self.config.rolling_columns if rc.source_column == col)
                existing_cols.append(f"s.{rc.history_column_name}[0] as {col}")
            else:
                existing_cols.append(f"s.{col}")
        
        # Build the combined SQL query with 7 CTEs
        combined_sql = f"""
        WITH 
        -- CTE 1: Validate backfill records (only keep newer timestamps)
        backfill_validated AS (
            SELECT b.*
            FROM backfill_records b
            LEFT JOIN (
                SELECT {primary}, {partition}, {timestamp}
                FROM {self.config.summary_table}
                WHERE {primary} IN (SELECT {primary} FROM affected_accounts)
            ) e ON b.{primary} = e.{primary} AND b.{partition} = e.{partition}
            WHERE e.{timestamp} IS NULL OR b.{timestamp} > e.{timestamp}
        ),
        
        -- CTE 2: Get all existing data for affected accounts
        existing_data AS (
            SELECT 
                s.{primary},
                s.{partition},
                {month_to_int_expr(f"s.{partition}")} as month_int,
                {', '.join(existing_cols)},
                s.{timestamp},
                2 as priority
            FROM {self.config.summary_table} s
            WHERE s.{primary} IN (SELECT {primary} FROM affected_accounts)
        ),
        
        -- CTE 3: Combine backfill + existing (backfill has higher priority)
        combined_data AS (
            SELECT 
                {primary}, {partition}, month_int,
                {struct_cols},
                {timestamp},
                1 as priority
            FROM backfill_validated
            
            UNION ALL
            
            SELECT * FROM existing_data
        ),
        
        -- CTE 4: Deduplicate (keep backfill if exists, else existing)
        deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {primary}, {partition}
                    ORDER BY priority, {timestamp} DESC
                ) as rn
            FROM combined_data
        ),
        
        -- CTE 5: Keep only winning rows
        final_data AS (
            SELECT * FROM deduped WHERE rn = 1
        ),
        
        -- CTE 6: Collect all months per account into arrays
        account_months AS (
            SELECT 
                {primary},
                COLLECT_LIST(STRUCT(month_int, {struct_cols})) as month_values,
                COLLECT_LIST(STRUCT({partition}, month_int, {timestamp})) as month_metadata
            FROM final_data
            GROUP BY {primary}
        ),
        
        -- CTE 7: Explode back to one row per month (with full history available)
        exploded AS (
            SELECT 
                {primary},
                month_values,
                meta.{partition},
                meta.month_int,
                meta.{timestamp}
            FROM account_months
            LATERAL VIEW EXPLODE(month_metadata) t AS meta
        )
        
        -- Final: Rebuild history arrays for each month
        SELECT
            e.{primary},
            e.{partition},
            {', '.join(rolling_col_history)},
            {', '.join(scalar_extract) if scalar_extract else 'e.' + timestamp},
            e.{timestamp}
        FROM exploded e
        """
        
        result = self.spark.sql(combined_sql)
        
        # Generate grid columns
        for grid_col in self.config.grid_columns:
            result = generate_grid_column(result, grid_col, self.config.rolling_columns)
        
        logger.info("Case III processing complete - history arrays rebuilt")
        
        return result
    
    def process_batch(
        self,
        case_iii_df: DataFrame,
        batch_info: BatchInfo
    ) -> tuple[DataFrame, BatchInfo]:
        """
        Process a batch of Case III records with tracking.
        
        Args:
            case_iii_df: DataFrame for this batch
            batch_info: Batch tracking information
            
        Returns:
            Tuple of (processed DataFrame, updated BatchInfo)
        """
        from datetime import datetime
        
        batch_info.status = BatchStatus.RUNNING
        batch_info.start_time = datetime.now()
        
        try:
            result = self.process(case_iii_df)
            batch_info.status = BatchStatus.COMPLETED
            batch_info.end_time = datetime.now()
            return result, batch_info
        except Exception as e:
            batch_info.status = BatchStatus.FAILED
            batch_info.end_time = datetime.now()
            batch_info.error_message = str(e)
            raise
