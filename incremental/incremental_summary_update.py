"""
Incremental Summary Update Script
=================================
Handles all scenarios for updating default.summary_testing from default.default.accounts_all

Scenarios:
- Case I:   New cons_acct_key (NULL history padding)
- Case II:  Forward month entry (shift arrays, handle gaps with NULLs)
- Case III: Backfill (update existing + fix future rows)

Scale Target: 500B+ summary records, 1B monthly updates
Strategy: MERGE-only for atomicity
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, ArrayType,
    IntegerType, StringType, TimestampType
)
from pyspark.sql.functions import (
    col, lit, array, when, coalesce, expr, max as spark_max,
    row_number, broadcast, collect_list, struct, explode,
    slice, concat, transform, sequence, size
)
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================

ACCOUNTS_TABLE = "default.default.accounts_all"
SUMMARY_TABLE = "default.summary_testing"
HISTORY_LENGTH = 36

SUMMARY_SCHEMA = StructType([
    StructField("cons_acct_key", LongType(), False),
    StructField("rpt_as_of_mo", StringType(), False),
    StructField("bal_history", ArrayType(IntegerType()), True),
    StructField("dpd_history", ArrayType(IntegerType()), True),
    StructField("payment_history", ArrayType(IntegerType()), True),
    StructField("status_history", ArrayType(StringType()), True),
    StructField("current_balance", IntegerType(), True),
    StructField("current_dpd", IntegerType(), True),
    StructField("account_status", StringType(), True),
    StructField("created_ts", TimestampType(), True),
    StructField("updated_ts", TimestampType(), True),
    StructField("base_ts", TimestampType(), True),
])


# ============================================================
# Spark Session
# ============================================================

def create_spark_session() -> SparkSession:
    """Create Spark session with Iceberg configuration."""
    spark = SparkSession.builder \
        .appName('IncrementalSummaryUpdate') \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# Helper Functions
# ============================================================

def month_str_to_int(month_str: str) -> int:
    """Convert 'YYYY-MM' to integer (year * 12 + month)."""
    # This will be used in SQL expressions
    pass  # Implemented via expr()


def int_to_month_str(month_int: int) -> str:
    """Convert integer back to 'YYYY-MM'."""
    # This will be used in SQL expressions
    pass  # Implemented via expr()


def get_max_summary_base_ts(spark: SparkSession):
    """Get the maximum base_ts from summary table."""
    result = spark.table(SUMMARY_TABLE).selectExpr("max(base_ts) as max_ts").collect()
    max_ts = result[0]["max_ts"] if result else None
    logger.info(f"Max summary base_ts: {max_ts}")
    return max_ts


def get_new_accounts_batch(spark: SparkSession, max_base_ts):
    """
    Get new records from accounts_all since last summary update.
    Deduplicate by (cons_acct_key, rpt_as_of_mo) keeping latest.
    """
    accounts_df = spark.table(ACCOUNTS_TABLE)
    
    if max_base_ts is not None:
        accounts_df = accounts_df.filter(col("base_ts") > max_base_ts)
    
    # Deduplicate: keep latest per (account, month)
    window_spec = Window.partitionBy("cons_acct_key", "rpt_as_of_mo") \
        .orderBy(col("base_ts").desc(), col("created_ts").desc(), col("updated_ts").desc())
    
    latest_df = accounts_df \
        .withColumn("_rn", row_number().over(window_spec)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")
    
    count = latest_df.count()
    logger.info(f"New accounts batch size: {count}")
    return latest_df


def classify_records(spark: SparkSession, new_batch_df):
    """
    Classify each record into Case I, II, or III.
    Returns DataFrame with additional column 'case_type'.
    """
    # Get existing summary metadata per account
    summary_meta = spark.table(SUMMARY_TABLE) \
        .groupBy("cons_acct_key") \
        .agg(
            spark_max("rpt_as_of_mo").alias("max_existing_month"),
            spark_max("base_ts").alias("max_existing_base_ts")
        )
    
    # Join new batch with summary metadata
    classified = new_batch_df.alias("n").join(
        broadcast(summary_meta.alias("s")),
        col("n.cons_acct_key") == col("s.cons_acct_key"),
        "left"
    ).select(
        col("n.*"),
        col("s.max_existing_month"),
        col("s.max_existing_base_ts")
    )
    
    # Classify based on existence and month comparison
    classified = classified.withColumn(
        "case_type",
        when(col("max_existing_month").isNull(), lit("CASE_I"))  # New account
        .when(col("rpt_as_of_mo") > col("max_existing_month"), lit("CASE_II"))  # Forward
        .otherwise(lit("CASE_III"))  # Backfill
    )
    
    # Log classification counts
    case_counts = classified.groupBy("case_type").count().collect()
    for row in case_counts:
        logger.info(f"{row['case_type']}: {row['count']} records")
    
    return classified


# ============================================================
# Case I: New Account
# ============================================================

def build_new_account_rows(classified_df):
    """
    Build summary rows for new accounts (Case I).
    All history arrays are NULL-padded.
    """
    case_i = classified_df.filter(col("case_type") == "CASE_I")
    
    # Create NULL arrays of size HISTORY_LENGTH
    null_array_int = array([lit(None).cast(IntegerType()) for _ in range(HISTORY_LENGTH)])
    null_array_str = array([lit(None).cast(StringType()) for _ in range(HISTORY_LENGTH)])
    
    # Build summary row - current values at index 0
    new_rows = case_i.select(
        col("cons_acct_key"),
        col("rpt_as_of_mo"),
        # bal_history: current at index 0, rest NULL
        concat(
            array(col("current_balance")),
            array([lit(None).cast(IntegerType()) for _ in range(HISTORY_LENGTH - 1)])
        ).alias("bal_history"),
        concat(
            array(col("current_dpd")),
            array([lit(None).cast(IntegerType()) for _ in range(HISTORY_LENGTH - 1)])
        ).alias("dpd_history"),
        concat(
            array(col("payment_am")),
            array([lit(None).cast(IntegerType()) for _ in range(HISTORY_LENGTH - 1)])
        ).alias("payment_history"),
        concat(
            array(col("status_cd").cast(StringType())),
            array([lit(None).cast(StringType()) for _ in range(HISTORY_LENGTH - 1)])
        ).alias("status_history"),
        col("current_balance"),
        col("current_dpd"),
        col("account_status"),
        col("created_ts"),
        col("updated_ts"),
        col("base_ts")
    )
    
    return new_rows


# ============================================================
# Case II: Forward Month Entry
# ============================================================

def build_forward_rows(spark: SparkSession, classified_df):
    """
    Build summary rows for forward entries (Case II).
    Handles month gaps by inserting NULLs.
    """
    case_ii = classified_df.filter(col("case_type") == "CASE_II")
    
    if case_ii.count() == 0:
        return spark.createDataFrame([], SUMMARY_SCHEMA)
    
    # Get the last summary row for each affected account
    affected_accounts = case_ii.select("cons_acct_key").distinct()
    
    # Get max month's summary row per account
    window_spec = Window.partitionBy("cons_acct_key").orderBy(col("rpt_as_of_mo").desc())
    
    latest_summary = spark.table(SUMMARY_TABLE) \
        .join(broadcast(affected_accounts), "cons_acct_key") \
        .withColumn("_rn", row_number().over(window_spec)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")
    
    # Join new data with latest summary
    joined = case_ii.alias("n").join(
        latest_summary.alias("s"),
        col("n.cons_acct_key") == col("s.cons_acct_key"),
        "inner"
    )
    
    # Calculate month gap
    # Month gap = (new_month_int - existing_max_month_int) - 1
    joined = joined.withColumn(
        "new_month_int",
        (expr("CAST(SUBSTRING(n.rpt_as_of_mo, 1, 4) AS INT)") * 12 +
         expr("CAST(SUBSTRING(n.rpt_as_of_mo, 6, 2) AS INT)"))
    ).withColumn(
        "existing_month_int",
        (expr("CAST(SUBSTRING(s.rpt_as_of_mo, 1, 4) AS INT)") * 12 +
         expr("CAST(SUBSTRING(s.rpt_as_of_mo, 6, 2) AS INT)"))
    ).withColumn(
        "month_gap",
        col("new_month_int") - col("existing_month_int") - 1
    )
    
    # Build new history arrays:
    # 1. Prepend current value
    # 2. Insert month_gap NULLs
    # 3. Take from existing history (truncate to HISTORY_LENGTH)
    forward_rows = joined.select(
        col("n.cons_acct_key"),
        col("n.rpt_as_of_mo"),
        # Build shifted/gapped history
        expr(f"""
            slice(
                concat(
                    array(n.current_balance),
                    transform(sequence(1, COALESCE(month_gap, 0)), x -> CAST(NULL AS INT)),
                    s.bal_history
                ),
                1, {HISTORY_LENGTH}
            )
        """).alias("bal_history"),
        expr(f"""
            slice(
                concat(
                    array(n.current_dpd),
                    transform(sequence(1, COALESCE(month_gap, 0)), x -> CAST(NULL AS INT)),
                    s.dpd_history
                ),
                1, {HISTORY_LENGTH}
            )
        """).alias("dpd_history"),
        expr(f"""
            slice(
                concat(
                    array(n.payment_am),
                    transform(sequence(1, COALESCE(month_gap, 0)), x -> CAST(NULL AS INT)),
                    s.payment_history
                ),
                1, {HISTORY_LENGTH}
            )
        """).alias("payment_history"),
        expr(f"""
            slice(
                concat(
                    array(CAST(n.status_cd AS STRING)),
                    transform(sequence(1, COALESCE(month_gap, 0)), x -> CAST(NULL AS STRING)),
                    s.status_history
                ),
                1, {HISTORY_LENGTH}
            )
        """).alias("status_history"),
        col("n.current_balance"),
        col("n.current_dpd"),
        col("n.account_status"),
        col("n.created_ts"),
        col("n.updated_ts"),
        col("n.base_ts")
    )
    
    return forward_rows


# ============================================================
# Case III: Backfill
# ============================================================

def process_backfill(spark: SparkSession, classified_df):
    """
    Process backfill entries (Case III).
    Updates the backfilled row AND all affected future rows.
    Only updates if new.base_ts > existing.base_ts for existing months.
    """
    case_iii = classified_df.filter(col("case_type") == "CASE_III")
    
    if case_iii.count() == 0:
        return spark.createDataFrame([], SUMMARY_SCHEMA)
    
    # Get affected accounts
    affected_accounts = case_iii.select("cons_acct_key").distinct()
    
    # Get ALL summary rows for affected accounts
    affected_summary = spark.table(SUMMARY_TABLE) \
        .join(broadcast(affected_accounts), "cons_acct_key")
    
    # Check for existing month conflicts (III.1)
    # Only keep backfill records where:
    # - Month doesn't exist, OR
    # - Month exists BUT new.base_ts > existing.base_ts
    existing_months = affected_summary.select(
        "cons_acct_key",
        "rpt_as_of_mo",
        col("base_ts").alias("existing_base_ts")
    )
    
    backfill_validated = case_iii.alias("b").join(
        existing_months.alias("e"),
        (col("b.cons_acct_key") == col("e.cons_acct_key")) &
        (col("b.rpt_as_of_mo") == col("e.rpt_as_of_mo")),
        "left"
    ).filter(
        col("e.existing_base_ts").isNull() |  # Month doesn't exist
        (col("b.base_ts") > col("e.existing_base_ts"))  # Newer data
    ).select("b.*")
    
    if backfill_validated.count() == 0:
        logger.info("No valid backfill records after base_ts filter")
        return spark.createDataFrame([], SUMMARY_SCHEMA)
    
    # For each backfill, we need to:
    # 1. Create/update the backfilled month's row
    # 2. Update all future months' history arrays
    
    # This requires a per-account rebuild approach
    # Collect all data per affected account and recompute
    
    # Combine existing summary + new backfill data per account
    backfill_as_summary = backfill_validated.select(
        col("cons_acct_key"),
        col("rpt_as_of_mo"),
        col("current_balance"),
        col("current_dpd"),
        col("payment_am").alias("payment_history_val"),
        col("status_cd").cast(StringType()).alias("status_history_val"),
        col("account_status"),
        col("created_ts"),
        col("updated_ts"),
        col("base_ts"),
        lit(True).alias("is_new")
    )
    
    existing_as_source = affected_summary.select(
        col("cons_acct_key"),
        col("rpt_as_of_mo"),
        col("current_balance"),
        col("current_dpd"),
        col("payment_history")[0].alias("payment_history_val"),
        col("status_history")[0].alias("status_history_val"),
        col("account_status"),
        col("created_ts"),
        col("updated_ts"),
        col("base_ts"),
        lit(False).alias("is_new")
    )
    
    # Union and deduplicate (prefer new data)
    combined = backfill_as_summary.unionByName(existing_as_source)
    
    dedup_window = Window.partitionBy("cons_acct_key", "rpt_as_of_mo") \
        .orderBy(col("is_new").desc(), col("base_ts").desc())
    
    combined_dedup = combined \
        .withColumn("_rn", row_number().over(dedup_window)) \
        .filter(col("_rn") == 1) \
        .drop("_rn", "is_new")
    
    # Now rebuild history arrays for all months per account
    # Group by account, collect all months, rebuild arrays
    
    # Collect all data per account
    account_data = combined_dedup.groupBy("cons_acct_key").agg(
        collect_list(
            struct(
                "rpt_as_of_mo",
                "current_balance",
                "current_dpd",
                "payment_history_val",
                "status_history_val",
                "account_status",
                "created_ts",
                "updated_ts",
                "base_ts"
            )
        ).alias("months_data")
    )
    
    # Explode and rebuild (this is the rebuild logic)
    # For each month, look back 36 months and build arrays
    rebuilt = account_data.select(
        col("cons_acct_key"),
        explode(col("months_data")).alias("month_data")
    ).select(
        col("cons_acct_key"),
        col("month_data.rpt_as_of_mo").alias("rpt_as_of_mo"),
        col("month_data.current_balance"),
        col("month_data.current_dpd"),
        col("month_data.payment_history_val"),
        col("month_data.status_history_val"),
        col("month_data.account_status"),
        col("month_data.created_ts"),
        col("month_data.updated_ts"),
        col("month_data.base_ts")
    )
    
    # Self-join to build history arrays
    # For each row, join with all rows from same account within 36 month lookback
    rebuilt_with_int = rebuilt.withColumn(
        "month_int",
        expr("CAST(SUBSTRING(rpt_as_of_mo, 1, 4) AS INT) * 12 + CAST(SUBSTRING(rpt_as_of_mo, 6, 2) AS INT)")
    )
    
    # Create lookback for each current month
    # This is expensive but necessary for correctness
    history_joined = rebuilt_with_int.alias("curr").crossJoin(
        rebuilt_with_int.alias("hist").select(
            col("cons_acct_key").alias("h_acct"),
            col("month_int").alias("h_month_int"),
            col("current_balance").alias("h_bal"),
            col("current_dpd").alias("h_dpd"),
            col("payment_history_val").alias("h_pay"),
            col("status_history_val").alias("h_status")
        )
    ).filter(
        (col("curr.cons_acct_key") == col("h_acct")) &
        (col("h_month_int") <= col("curr.month_int")) &
        (col("h_month_int") > col("curr.month_int") - HISTORY_LENGTH)
    ).withColumn(
        "offset",
        col("curr.month_int") - col("h_month_int")
    )
    
    # Aggregate into arrays
    # Group by current row, order history by offset, collect
    history_window = Window.partitionBy(
        "curr.cons_acct_key", "curr.rpt_as_of_mo"
    ).orderBy("offset")
    
    # Collect history in order
    history_arrays = history_joined.groupBy(
        col("curr.cons_acct_key").alias("cons_acct_key"),
        col("curr.rpt_as_of_mo").alias("rpt_as_of_mo"),
        col("curr.current_balance").alias("current_balance"),
        col("curr.current_dpd").alias("current_dpd"),
        col("curr.account_status").alias("account_status"),
        col("curr.created_ts").alias("created_ts"),
        col("curr.updated_ts").alias("updated_ts"),
        col("curr.base_ts").alias("base_ts"),
        col("curr.month_int").alias("month_int")
    ).agg(
        collect_list(struct("offset", "h_bal", "h_dpd", "h_pay", "h_status")).alias("history_data")
    )
    
    # Sort and extract arrays
    # Need to pad to HISTORY_LENGTH
    final_rebuilt = history_arrays.select(
        "cons_acct_key",
        "rpt_as_of_mo",
        expr(f"""
            transform(
                sequence(0, {HISTORY_LENGTH - 1}),
                i -> 
                    CASE 
                        WHEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ) IS NOT NULL 
                        THEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ).h_bal
                        ELSE NULL
                    END
            )
        """).alias("bal_history"),
        expr(f"""
            transform(
                sequence(0, {HISTORY_LENGTH - 1}),
                i -> 
                    CASE 
                        WHEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ) IS NOT NULL 
                        THEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ).h_dpd
                        ELSE NULL
                    END
            )
        """).alias("dpd_history"),
        expr(f"""
            transform(
                sequence(0, {HISTORY_LENGTH - 1}),
                i -> 
                    CASE 
                        WHEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ) IS NOT NULL 
                        THEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ).h_pay
                        ELSE NULL
                    END
            )
        """).alias("payment_history"),
        expr(f"""
            transform(
                sequence(0, {HISTORY_LENGTH - 1}),
                i -> 
                    CASE 
                        WHEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ) IS NOT NULL 
                        THEN element_at(
                            filter(history_data, x -> x.offset = i), 1
                        ).h_status
                        ELSE NULL
                    END
            )
        """).alias("status_history"),
        "current_balance",
        "current_dpd",
        "account_status",
        "created_ts",
        "updated_ts",
        "base_ts"
    )
    
    return final_rebuilt


# ============================================================
# MERGE to Summary
# ============================================================

def merge_to_summary(spark: SparkSession, updates_df, source_name: str = "updates"):
    """
    MERGE updates into summary table using Iceberg MERGE INTO.
    """
    if updates_df.count() == 0:
        logger.info(f"No {source_name} to merge")
        return
    
    updates_df.createOrReplaceTempView(source_name)
    
    merge_sql = f"""
        MERGE INTO {SUMMARY_TABLE} t
        USING {source_name} s
        ON t.cons_acct_key = s.cons_acct_key AND t.rpt_as_of_mo = s.rpt_as_of_mo
        WHEN MATCHED THEN UPDATE SET
            bal_history = s.bal_history,
            dpd_history = s.dpd_history,
            payment_history = s.payment_history,
            status_history = s.status_history,
            current_balance = s.current_balance,
            current_dpd = s.current_dpd,
            account_status = s.account_status,
            created_ts = s.created_ts,
            updated_ts = s.updated_ts,
            base_ts = s.base_ts
        WHEN NOT MATCHED THEN INSERT *
    """
    
    logger.info(f"Merging {source_name}...")
    spark.sql(merge_sql)
    logger.info(f"✓ {source_name} merged successfully")


# ============================================================
# Main Pipeline
# ============================================================

def run_incremental_update(spark: SparkSession):
    """
    Main pipeline orchestrator.
    """
    logger.info("=" * 60)
    logger.info("Starting Incremental Summary Update")
    logger.info("=" * 60)
    
    # Step 1: Get max base_ts from summary
    max_base_ts = get_max_summary_base_ts(spark)
    
    # Step 2: Get new batch from accounts
    new_batch = get_new_accounts_batch(spark, max_base_ts)
    
    if new_batch.count() == 0:
        logger.info("No new records to process. Exiting.")
        return
    
    # Step 3: Classify records
    classified = classify_records(spark, new_batch)
    classified.cache()  # Cache for multiple uses
    
    # Step 4: Process each case
    logger.info("-" * 40)
    logger.info("Processing Case I: New Accounts")
    case_i_rows = build_new_account_rows(classified)
    
    logger.info("-" * 40)
    logger.info("Processing Case II: Forward Entries")
    case_ii_rows = build_forward_rows(spark, classified)
    
    logger.info("-" * 40)
    logger.info("Processing Case III: Backfill")
    case_iii_rows = process_backfill(spark, classified)
    
    # Step 5: Union all updates
    all_updates = case_i_rows.unionByName(case_ii_rows).unionByName(case_iii_rows)
    all_updates.cache()
    
    update_count = all_updates.count()
    logger.info(f"Total updates to merge: {update_count}")
    
    # Step 6: MERGE to summary
    logger.info("-" * 40)
    merge_to_summary(spark, all_updates, "all_updates")
    
    # Cleanup
    classified.unpersist()
    all_updates.unpersist()
    
    logger.info("=" * 60)
    logger.info("✓ Incremental Update Complete")
    logger.info("=" * 60)


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        run_incremental_update(spark)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
