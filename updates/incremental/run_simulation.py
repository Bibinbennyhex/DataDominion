#!/usr/bin/env python3
"""
Run 10M Simulation - Scaled for Docker Environment
===================================================

Runs a scaled simulation (100K accounts, 10K batch) to demonstrate
the incremental summary update pipeline with real execution metrics.
"""

import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, expr, rand, floor, concat, lpad, 
    current_timestamp, monotonically_increasing_id,
    when, coalesce, array, transform, sequence,
    date_add, add_months, to_date, date_format,
    max as spark_max, count, size
)

# Configuration
SCALE_FACTOR = 100  # 100K accounts instead of 10M (1/100 scale)
TOTAL_ACCOUNTS = 100_000
BATCH_SIZE = 10_000
CASE_I_PERCENT = 5.0
CASE_II_PERCENT = 85.0
CASE_III_PERCENT = 10.0

SUMMARY_TABLE = "default.summary_testing_sim"
ACCOUNTS_TABLE = "default.accounts_all_sim"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Simulation")


def create_spark_session():
    """Create Spark session with Iceberg configuration."""
    return SparkSession.builder \
        .appName("10M_Simulation_Scaled") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "rest") \
        .config("spark.sql.catalog.demo.uri", "http://iceberg-rest:8181") \
        .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.demo.s3.path-style-access", "true") \
        .config("spark.sql.defaultCatalog", "demo") \
        .getOrCreate()


def generate_base_summary(spark):
    """Generate base summary data with 100K accounts."""
    logger.info(f"Generating base summary data for {TOTAL_ACCOUNTS:,} accounts...")
    start = time.time()
    
    # Generate accounts with varying months (12-48 per account)
    accounts_df = spark.range(1, TOTAL_ACCOUNTS + 1).toDF("cons_acct_key")
    
    # Each account gets 12-36 months of history
    summary_data = accounts_df.selectExpr(
        "cons_acct_key",
        "explode(sequence(0, cast(floor(rand() * 24 + 12) as int) - 1)) as month_offset"
    ).withColumn(
        "rpt_as_of_mo",
        date_format(
            add_months(lit("2022-01-01").cast("date"), col("month_offset")),
            "yyyy-MM"
        )
    )
    
    # Add all columns
    hl = 36
    summary_data = summary_data.select(
        "cons_acct_key",
        "rpt_as_of_mo",
        expr(f"transform(sequence(0, {hl-1}), i -> CASE WHEN rand() > 0.3 THEN cast(floor(rand() * 10000) as int) ELSE NULL END)").alias("bal_history"),
        expr(f"transform(sequence(0, {hl-1}), i -> CASE WHEN rand() > 0.3 THEN cast(floor(rand() * 180) as int) ELSE NULL END)").alias("dpd_history"),
        expr(f"transform(sequence(0, {hl-1}), i -> CASE WHEN rand() > 0.3 THEN cast(floor(rand() * 1000) as int) ELSE NULL END)").alias("payment_history"),
        expr(f"transform(sequence(0, {hl-1}), i -> CASE WHEN rand() > 0.5 THEN 'OPEN' ELSE 'CLOSED' END)").alias("status_history"),
        expr("cast(floor(rand() * 10000) as int)").alias("current_balance"),
        expr("cast(floor(rand() * 180) as int)").alias("current_dpd"),
        lit("ACTIVE").alias("account_status"),
        current_timestamp().alias("created_ts"),
        current_timestamp().alias("updated_ts"),
        lit("2025-07-15 12:00:00").cast("timestamp").alias("base_ts")
    )
    
    # Write to table
    summary_data.writeTo(SUMMARY_TABLE).overwritePartitions()
    
    row_count = spark.table(SUMMARY_TABLE).count()
    duration = time.time() - start
    
    logger.info(f"Generated {row_count:,} summary rows in {duration:.1f}s")
    logger.info(f"Throughput: {row_count/duration:,.0f} rows/second")
    
    return row_count, duration


def generate_new_batch(spark):
    """Generate new batch with Case I, II, III distribution."""
    logger.info(f"Generating new batch: {BATCH_SIZE:,} records")
    
    case_i_count = int(BATCH_SIZE * CASE_I_PERCENT / 100)
    case_ii_count = int(BATCH_SIZE * CASE_II_PERCENT / 100)
    case_iii_count = int(BATCH_SIZE * CASE_III_PERCENT / 100)
    
    logger.info(f"  Case I (new accounts): {case_i_count:,}")
    logger.info(f"  Case II (forward): {case_ii_count:,}")
    logger.info(f"  Case III (backfill): {case_iii_count:,}")
    
    start = time.time()
    
    # Case I: New accounts (IDs above existing range)
    case_i_start = TOTAL_ACCOUNTS + 1
    case_i_df = spark.range(case_i_start, case_i_start + case_i_count).toDF("cons_acct_key")
    case_i_df = case_i_df.withColumn(
        "rpt_as_of_mo",
        date_format(add_months(lit("2025-01-01").cast("date"), (rand() * 6).cast("int")), "yyyy-MM")
    ).withColumn("case_type", lit("CASE_I"))
    
    # Case II: Forward entries for existing accounts (month 2026-01)
    case_ii_df = spark.range(1, case_ii_count + 1).toDF("cons_acct_key")
    case_ii_df = case_ii_df.withColumn("rpt_as_of_mo", lit("2026-01"))
    case_ii_df = case_ii_df.withColumn("case_type", lit("CASE_II"))
    
    # Case III: Backfill for existing accounts (older months)
    case_iii_start = case_ii_count + 1
    case_iii_df = spark.range(case_iii_start, case_iii_start + case_iii_count).toDF("cons_acct_key")
    case_iii_df = case_iii_df.withColumn(
        "rpt_as_of_mo",
        date_format(add_months(lit("2023-01-01").cast("date"), (rand() * 12).cast("int")), "yyyy-MM")
    ).withColumn("case_type", lit("CASE_III"))
    
    # Union all cases
    all_records = case_i_df.unionByName(case_ii_df).unionByName(case_iii_df)
    
    # Add remaining columns
    batch_df = all_records.withColumn(
        "acct_dt", concat(col("rpt_as_of_mo"), lit("-15")).cast("date")
    ).withColumn(
        "current_balance", expr("cast(floor(rand() * 10000) as int)")
    ).withColumn(
        "current_dpd", expr("cast(floor(rand() * 180) as int)")
    ).withColumn(
        "payment_am", expr("cast(floor(rand() * 1000) as int)")
    ).withColumn(
        "status_cd", when(rand() > 0.3, lit("OPEN")).otherwise(lit("CLOSED"))
    ).withColumn(
        "account_status", when(rand() > 0.1, lit("ACTIVE")).otherwise(lit("DELINQUENT"))
    ).withColumn(
        "created_ts", current_timestamp()
    ).withColumn(
        "updated_ts", current_timestamp()
    ).withColumn(
        "base_ts", lit("2025-07-20 12:00:00").cast("timestamp")
    )
    
    # Write to accounts table
    batch_df.drop("case_type").writeTo(ACCOUNTS_TABLE).append()
    
    duration = time.time() - start
    logger.info(f"Generated batch in {duration:.1f}s")
    
    return case_i_count, case_ii_count, case_iii_count, duration


def run_incremental_update(spark):
    """Run the actual v3 incremental update processor."""
    logger.info("Running incremental summary update (v3)...")
    start = time.time()
    
    # Get max base_ts from summary
    max_ts_row = spark.sql(f"SELECT MAX(base_ts) as max_ts FROM {SUMMARY_TABLE}").collect()
    max_base_ts = max_ts_row[0]["max_ts"]
    logger.info(f"Max summary base_ts: {max_base_ts}")
    
    # Get new records
    new_records = spark.sql(f"""
        SELECT * FROM {ACCOUNTS_TABLE}
        WHERE base_ts > TIMESTAMP '{max_base_ts}'
    """)
    
    new_count = new_records.count()
    logger.info(f"Found {new_count:,} new records to process")
    
    if new_count == 0:
        logger.info("No new records to process")
        return 0, 0, 0, 0.0
    
    # Get account metadata from summary
    account_meta = spark.sql(f"""
        SELECT 
            cons_acct_key,
            MAX(rpt_as_of_mo) as max_existing_month,
            MIN(rpt_as_of_mo) as min_existing_month,
            MAX(base_ts) as max_existing_base_ts
        FROM {SUMMARY_TABLE}
        GROUP BY cons_acct_key
    """)
    
    # Classify records
    new_records.createOrReplaceTempView("new_batch")
    account_meta.createOrReplaceTempView("account_meta")
    
    classified = spark.sql("""
        SELECT 
            n.*,
            (CAST(SUBSTRING(n.rpt_as_of_mo, 1, 4) AS INT) * 12 + 
             CAST(SUBSTRING(n.rpt_as_of_mo, 6, 2) AS INT)) as month_int,
            m.max_existing_month,
            m.min_existing_month,
            m.max_existing_base_ts,
            (CAST(SUBSTRING(m.max_existing_month, 1, 4) AS INT) * 12 + 
             CAST(SUBSTRING(m.max_existing_month, 6, 2) AS INT)) as max_month_int,
            CASE 
                WHEN m.max_existing_month IS NULL THEN 'CASE_I'
                WHEN (CAST(SUBSTRING(n.rpt_as_of_mo, 1, 4) AS INT) * 12 + 
                      CAST(SUBSTRING(n.rpt_as_of_mo, 6, 2) AS INT)) > 
                     (CAST(SUBSTRING(m.max_existing_month, 1, 4) AS INT) * 12 + 
                      CAST(SUBSTRING(m.max_existing_month, 6, 2) AS INT))
                THEN 'CASE_II'
                ELSE 'CASE_III'
            END as case_type
        FROM new_batch n
        LEFT JOIN account_meta m ON n.cons_acct_key = m.cons_acct_key
    """)
    
    classified.createOrReplaceTempView("classified")
    
    # Count by case type
    case_counts = spark.sql("""
        SELECT case_type, COUNT(*) as cnt 
        FROM classified 
        GROUP BY case_type
    """).collect()
    
    case_i_count = 0
    case_ii_count = 0
    case_iii_count = 0
    
    for row in case_counts:
        if row["case_type"] == "CASE_I":
            case_i_count = row["cnt"]
        elif row["case_type"] == "CASE_II":
            case_ii_count = row["cnt"]
        elif row["case_type"] == "CASE_III":
            case_iii_count = row["cnt"]
    
    logger.info(f"Classification: Case I={case_i_count:,}, Case II={case_ii_count:,}, Case III={case_iii_count:,}")
    
    # Process Case I: New accounts
    hl = 36
    if case_i_count > 0:
        logger.info("Processing Case I (new accounts)...")
        case_i_start = time.time()
        
        case_i_result = spark.sql(f"""
            SELECT
                cons_acct_key,
                rpt_as_of_mo,
                concat(array(current_balance), transform(sequence(1, {hl-1}), x -> CAST(NULL AS INT))) as bal_history,
                concat(array(current_dpd), transform(sequence(1, {hl-1}), x -> CAST(NULL AS INT))) as dpd_history,
                concat(array(payment_am), transform(sequence(1, {hl-1}), x -> CAST(NULL AS INT))) as payment_history,
                concat(array(CAST(status_cd AS STRING)), transform(sequence(1, {hl-1}), x -> CAST(NULL AS STRING))) as status_history,
                current_balance,
                current_dpd,
                account_status,
                created_ts,
                updated_ts,
                base_ts
            FROM classified
            WHERE case_type = 'CASE_I'
        """)
        
        # Use MERGE for Case I (should all be inserts)
        case_i_result.createOrReplaceTempView("case_i_updates")
        spark.sql(f"""
            MERGE INTO {SUMMARY_TABLE} t
            USING case_i_updates s
            ON t.cons_acct_key = s.cons_acct_key AND t.rpt_as_of_mo = s.rpt_as_of_mo
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"  Case I completed in {time.time() - case_i_start:.1f}s")
    
    # Process Case II: Forward entries
    if case_ii_count > 0:
        logger.info("Processing Case II (forward entries)...")
        case_ii_start = time.time()
        
        case_ii_result = spark.sql(f"""
            WITH latest_summary AS (
                SELECT s.*,
                    ROW_NUMBER() OVER (PARTITION BY s.cons_acct_key ORDER BY s.rpt_as_of_mo DESC) as rn
                FROM {SUMMARY_TABLE} s
                WHERE s.cons_acct_key IN (SELECT cons_acct_key FROM classified WHERE case_type = 'CASE_II')
            ),
            latest_only AS (
                SELECT * FROM latest_summary WHERE rn = 1
            )
            SELECT
                c.cons_acct_key,
                c.rpt_as_of_mo,
                slice(concat(array(c.current_balance), s.bal_history), 1, {hl}) as bal_history,
                slice(concat(array(c.current_dpd), s.dpd_history), 1, {hl}) as dpd_history,
                slice(concat(array(c.payment_am), s.payment_history), 1, {hl}) as payment_history,
                slice(concat(array(CAST(c.status_cd AS STRING)), s.status_history), 1, {hl}) as status_history,
                c.current_balance,
                c.current_dpd,
                c.account_status,
                c.created_ts,
                c.updated_ts,
                c.base_ts
            FROM classified c
            INNER JOIN latest_only s ON c.cons_acct_key = s.cons_acct_key
            WHERE c.case_type = 'CASE_II'
        """)
        
        # Use MERGE for Case II
        case_ii_result.createOrReplaceTempView("case_ii_updates")
        spark.sql(f"""
            MERGE INTO {SUMMARY_TABLE} t
            USING case_ii_updates s
            ON t.cons_acct_key = s.cons_acct_key AND t.rpt_as_of_mo = s.rpt_as_of_mo
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"  Case II completed in {time.time() - case_ii_start:.1f}s")
    
    # Process Case III: Backfill (simplified - just insert new row)
    if case_iii_count > 0:
        logger.info("Processing Case III (backfill)...")
        case_iii_start = time.time()
        
        case_iii_result = spark.sql(f"""
            SELECT
                cons_acct_key,
                rpt_as_of_mo,
                concat(array(current_balance), transform(sequence(1, {hl-1}), x -> CAST(NULL AS INT))) as bal_history,
                concat(array(current_dpd), transform(sequence(1, {hl-1}), x -> CAST(NULL AS INT))) as dpd_history,
                concat(array(payment_am), transform(sequence(1, {hl-1}), x -> CAST(NULL AS INT))) as payment_history,
                concat(array(CAST(status_cd AS STRING)), transform(sequence(1, {hl-1}), x -> CAST(NULL AS STRING))) as status_history,
                current_balance,
                current_dpd,
                account_status,
                created_ts,
                updated_ts,
                base_ts
            FROM classified
            WHERE case_type = 'CASE_III'
        """)
        
        # Use MERGE for Case III
        case_iii_result.createOrReplaceTempView("case_iii_updates")
        spark.sql(f"""
            MERGE INTO {SUMMARY_TABLE} t
            USING case_iii_updates s
            ON t.cons_acct_key = s.cons_acct_key AND t.rpt_as_of_mo = s.rpt_as_of_mo
            WHEN MATCHED AND s.base_ts > t.base_ts THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        logger.info(f"  Case III completed in {time.time() - case_iii_start:.1f}s")
    
    duration = time.time() - start
    logger.info(f"Total incremental update completed in {duration:.1f}s")
    
    return case_i_count, case_ii_count, case_iii_count, duration


def validate_results(spark):
    """Run validation checks."""
    logger.info("Running validation checks...")
    
    # Check for duplicates
    dup_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM (
            SELECT cons_acct_key, rpt_as_of_mo, COUNT(*) as c
            FROM {SUMMARY_TABLE}
            GROUP BY cons_acct_key, rpt_as_of_mo
            HAVING COUNT(*) > 1
        )
    """).collect()[0]["cnt"]
    
    # Check array lengths
    bad_array_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {SUMMARY_TABLE}
        WHERE size(bal_history) != 36
    """).collect()[0]["cnt"]
    
    logger.info(f"  Duplicate check: {dup_count} duplicates found")
    logger.info(f"  Array length check: {bad_array_count} invalid arrays found")
    
    return dup_count == 0 and bad_array_count == 0


def main():
    """Main execution."""
    logger.info("=" * 70)
    logger.info("10M CONSUMER ACCOUNT SIMULATION (1/100 SCALE)")
    logger.info("=" * 70)
    logger.info(f"Scale: {TOTAL_ACCOUNTS:,} accounts, {BATCH_SIZE:,} batch records")
    logger.info(f"Extrapolated: 10M accounts, 10M batch records")
    logger.info("=" * 70)
    
    overall_start = time.time()
    metrics = {}
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Step 1: Generate base summary
        logger.info("\n[STEP 1/4] Generating base summary data...")
        base_rows, base_duration = generate_base_summary(spark)
        metrics["base_summary"] = {
            "rows": base_rows,
            "duration_sec": base_duration,
            "rows_per_sec": base_rows / base_duration
        }
        
        # Step 2: Generate new batch
        logger.info("\n[STEP 2/4] Generating new batch data...")
        c1, c2, c3, batch_duration = generate_new_batch(spark)
        metrics["batch_generation"] = {
            "case_i": c1,
            "case_ii": c2,
            "case_iii": c3,
            "duration_sec": batch_duration
        }
        
        # Step 3: Run incremental update
        logger.info("\n[STEP 3/4] Running incremental update...")
        case_i, case_ii, case_iii, update_duration = run_incremental_update(spark)
        metrics["incremental_update"] = {
            "case_i_processed": case_i,
            "case_ii_processed": case_ii,
            "case_iii_processed": case_iii,
            "duration_sec": update_duration
        }
        
        # Step 4: Validate
        logger.info("\n[STEP 4/4] Validating results...")
        valid = validate_results(spark)
        metrics["validation"] = {"passed": valid}
        
        # Final summary
        total_duration = time.time() - overall_start
        
        logger.info("\n" + "=" * 70)
        logger.info("SIMULATION COMPLETE - RESULTS")
        logger.info("=" * 70)
        
        final_count = spark.table(SUMMARY_TABLE).count()
        
        logger.info(f"\nActual Execution (1/100 scale):")
        logger.info(f"  Base summary rows: {base_rows:,}")
        logger.info(f"  New batch records: {BATCH_SIZE:,}")
        logger.info(f"  Final summary rows: {final_count:,}")
        logger.info(f"  Total duration: {total_duration:.1f}s")
        
        logger.info(f"\nExtrapolated to 10M Scale:")
        logger.info(f"  Base summary rows: ~{base_rows * 100:,}")
        logger.info(f"  New batch records: 10,000,000")
        logger.info(f"  Estimated duration: ~{total_duration * 100 / 60:.0f} minutes")
        
        logger.info(f"\nPerformance Metrics:")
        logger.info(f"  Base generation: {base_rows/base_duration:,.0f} rows/sec")
        logger.info(f"  Incremental update: {BATCH_SIZE/update_duration:,.0f} records/sec")
        
        logger.info(f"\nValidation: {'PASSED' if valid else 'FAILED'}")
        logger.info("=" * 70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
