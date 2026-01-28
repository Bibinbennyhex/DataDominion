"""
Pure SQL approach for Iceberg backfill operations.
Simpler implementation for straightforward cases.
"""

from pyspark.sql import SparkSession
from typing import List
import logging

from backfill_processor import calculate_months_between, get_affected_partitions
from config import HistoryColumn, MAX_HISTORY_MONTHS, BACKFILL_STAGING_TABLE

logger = logging.getLogger("BackfillSQL")


def execute_backfill_sql(
    spark: SparkSession,
    target_table: str,
    backfill_month: str,
    backfill_id: str,
    history_columns: List[HistoryColumn]
):
    """
    Pure SQL approach using Iceberg's MERGE.
    Better for simpler cases or when you want to run from a SQL interface.
    
    Args:
        spark: SparkSession
        target_table: Fully qualified table name
        backfill_month: Month being backfilled (e.g., "2023-12")
        backfill_id: Identifier for this backfill batch
        history_columns: List of history columns to update
    """
    logger.info(f"Starting SQL-based backfill for {backfill_month}")
    
    # Get affected partitions
    partitions = get_affected_partitions(
        spark=spark,
        target_table=target_table,
        backfill_month=backfill_month
    )
    
    if not partitions:
        logger.warning("No partitions to update")
        return
    
    logger.info(f"Processing {len(partitions)} partitions")
    
    # Process each partition
    for i, partition in enumerate(partitions):
        position = calculate_months_between(partition, backfill_month)
        
        if position >= MAX_HISTORY_MONTHS:
            logger.info(f"Skipping {partition}: outside {MAX_HISTORY_MONTHS}-month window")
            continue
        
        logger.info(f"Processing partition {i+1}/{len(partitions)}: {partition} (position={position})")
        
        # Build UPDATE SET for all columns
        update_clauses = []
        for hc in history_columns:
            update_clauses.append(f"""
                {hc.name} = concat(
                    slice(t.{hc.name}, 1, {position}),
                    array(b.{hc.backfill_source_column}),
                    slice(t.{hc.name}, {position + 2}, {MAX_HISTORY_MONTHS - position - 1})
                )
            """)
        
        update_sql = ",\n                ".join(update_clauses)
        
        merge_query = f"""
            MERGE INTO {target_table} AS t
            USING (
                SELECT * FROM {BACKFILL_STAGING_TABLE}
                WHERE backfill_month = '{backfill_month}' 
                  AND backfill_id = '{backfill_id}'
                  AND status = 'pending'
            ) AS b
            ON t.acct = b.acct AND t.month_partition = '{partition}'
            WHEN MATCHED THEN UPDATE SET
                {update_sql}
        """
        
        spark.sql(merge_query)
        logger.info(f"Completed partition: {partition}")
    
    # Mark as completed
    spark.sql(f"""
        UPDATE {BACKFILL_STAGING_TABLE}
        SET status = 'completed', processed_at = current_timestamp()
        WHERE backfill_id = '{backfill_id}' AND backfill_month = '{backfill_month}'
    """)
    
    logger.info(f"SQL-based backfill completed for {backfill_month}")


def execute_backfill_update_statement(
    spark: SparkSession,
    target_table: str,
    backfill_month: str,
    backfill_id: str,
    history_columns: List[HistoryColumn]
):
    """
    Alternative: Use single UPDATE statement with join (simpler but less efficient).
    
    Args:
        spark: SparkSession
        target_table: Fully qualified table name
        backfill_month: Month being backfilled
        backfill_id: Identifier for this backfill batch
        history_columns: List of history columns to update
    """
    logger.info(f"Executing UPDATE-based backfill for {backfill_month}")
    
    # Get affected partitions
    partitions = get_affected_partitions(
        spark=spark,
        target_table=target_table,
        backfill_month=backfill_month
    )
    
    for partition in partitions:
        position = calculate_months_between(partition, backfill_month)
        
        if position >= MAX_HISTORY_MONTHS:
            continue
        
        # Build SET clauses
        set_clauses = []
        for hc in history_columns:
            set_clauses.append(f"""
                {hc.name} = (
                    SELECT concat(
                        slice(t.{hc.name}, 1, {position}),
                        array(b.{hc.backfill_source_column}),
                        slice(t.{hc.name}, {position + 2}, {MAX_HISTORY_MONTHS - position - 1})
                    )
                    FROM {BACKFILL_STAGING_TABLE} b
                    WHERE b.acct = t.acct
                      AND b.backfill_month = '{backfill_month}'
                      AND b.backfill_id = '{backfill_id}'
                      AND b.status = 'pending'
                )
            """)
        
        set_sql = ",\n            ".join(set_clauses)
        
        update_query = f"""
            UPDATE {target_table} t
            SET {set_sql}
            WHERE t.month_partition = '{partition}'
              AND EXISTS (
                  SELECT 1 FROM {BACKFILL_STAGING_TABLE} b
                  WHERE b.acct = t.acct
                    AND b.backfill_month = '{backfill_month}'
                    AND b.backfill_id = '{backfill_id}'
                    AND b.status = 'pending'
              )
        """
        
        spark.sql(update_query)
        logger.info(f"Updated partition: {partition}")


def generate_sql_script(
    target_table: str,
    backfill_month: str,
    backfill_id: str,
    history_columns: List[HistoryColumn],
    output_file: str = "backfill_script.sql"
):
    """
    Generate standalone SQL script for manual execution.
    Useful for reviewing changes or running outside PySpark.
    
    Args:
        target_table: Fully qualified table name
        backfill_month: Month being backfilled
        backfill_id: Identifier for this backfill batch
        history_columns: List of history columns
        output_file: Output file path for SQL script
    """
    logger.info(f"Generating SQL script to {output_file}")
    
    sql_statements = []
    
    # Header
    sql_statements.append(f"""
-- Backfill Script
-- Backfill ID: {backfill_id}
-- Backfill Month: {backfill_month}
-- Generated: {{current_timestamp}}
-- Target Table: {target_table}

-- Step 1: Mark backfill as processing
UPDATE {BACKFILL_STAGING_TABLE}
SET status = 'processing'
WHERE backfill_id = '{backfill_id}' AND backfill_month = '{backfill_month}';
""")
    
    # Generate MERGE for sample partitions (user will replicate)
    sample_partitions = ["2024-01", "2024-02", "2024-03"]  # Example
    
    for partition in sample_partitions:
        position = calculate_months_between(partition, backfill_month)
        
        update_clauses = []
        for hc in history_columns:
            update_clauses.append(f"""
                {hc.name} = concat(
                    slice(t.{hc.name}, 1, {position}),
                    array(b.{hc.backfill_source_column}),
                    slice(t.{hc.name}, {position + 2}, {MAX_HISTORY_MONTHS - position - 1})
                )
            """)
        
        update_sql = ",\n            ".join(update_clauses)
        
        sql_statements.append(f"""
-- Partition: {partition} (position={position})
MERGE INTO {target_table} AS t
USING (
    SELECT * FROM {BACKFILL_STAGING_TABLE}
    WHERE backfill_month = '{backfill_month}' 
      AND backfill_id = '{backfill_id}'
      AND status = 'processing'
) AS b
ON t.acct = b.acct AND t.month_partition = '{partition}'
WHEN MATCHED THEN UPDATE SET
    {update_sql};
""")
    
    # Footer
    sql_statements.append(f"""
-- Step N: Mark backfill as completed
UPDATE {BACKFILL_STAGING_TABLE}
SET status = 'completed', processed_at = current_timestamp()
WHERE backfill_id = '{backfill_id}' AND backfill_month = '{backfill_month}';
""")
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write('\n'.join(sql_statements))
    
    logger.info(f"SQL script generated: {output_file}")
