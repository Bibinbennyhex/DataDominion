"""
Configuration for Iceberg History Backfill System
"""

from dataclasses import dataclass
from typing import List


@dataclass
class HistoryColumn:
    """Definition of a history array column"""
    name: str
    backfill_source_column: str  # Column name in backfill staging table


# Define all 20 history columns
HISTORY_COLUMNS: List[HistoryColumn] = [
    HistoryColumn("balance_history", "balance"),
    HistoryColumn("payment_history", "payment"),
    HistoryColumn("credit_limit_history", "credit_limit"),
    HistoryColumn("utilization_history", "utilization"),
    HistoryColumn("delinquency_history", "delinquency"),
    HistoryColumn("available_credit_history", "available_credit"),
    HistoryColumn("min_payment_history", "min_payment"),
    HistoryColumn("interest_charged_history", "interest_charged"),
    HistoryColumn("fees_charged_history", "fees_charged"),
    HistoryColumn("purchases_history", "purchases"),
    HistoryColumn("cash_advances_history", "cash_advances"),
    HistoryColumn("balance_transfers_history", "balance_transfers"),
    HistoryColumn("credit_score_history", "credit_score"),
    HistoryColumn("payment_status_history", "payment_status"),
    HistoryColumn("overlimit_history", "overlimit"),
    HistoryColumn("days_past_due_history", "days_past_due"),
    HistoryColumn("promotional_rate_history", "promotional_rate"),
    HistoryColumn("reward_points_history", "reward_points"),
    HistoryColumn("transaction_count_history", "transaction_count"),
    HistoryColumn("dispute_count_history", "dispute_count"),
]


# Spark configuration for 500B row table
SPARK_CONFIGS = {
    # Iceberg-specific optimizations
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.spark_catalog.type": "hive",
    "spark.sql.iceberg.planning.preserve-data-grouping": "true",
    
    # Parallelism for massive scale
    "spark.sql.shuffle.partitions": "20000",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    
    # Memory optimization
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
    "spark.sql.files.maxPartitionBytes": "256MB",
    
    # Iceberg write optimization
    "spark.sql.iceberg.vectorization.enabled": "true",
}


# Processing parameters
MAX_HISTORY_MONTHS = 36
MAX_PARALLEL_PARTITIONS = 4  # Conservative for stability
RETRY_COUNT = 3
BACKFILL_STAGING_TABLE = "backfill_staging"
BACKFILL_AUDIT_TABLE = "backfill_audit"
