"""
PySpark Operations on Account Summary Iceberg Table
=====================================================
Common operations for analyzing account summary data with 36-month rolling history arrays.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, array, explode, posexplode, posexplode_outer,
    when, size, element_at, expr, slice, aggregate, filter,
    months_between, add_months, date_sub, coalesce, sum as spark_sum,
    avg, max as spark_max, min as spark_min, count, collect_list,
    transform, sequence, concat, array_contains, forall, exists,
    array_position, array_remove, array_distinct, array_except,
    array_union, array_intersect, reverse, flatten
)
from pyspark.sql.types import IntegerType, DecimalType
from datetime import date


# ============================================================
# Spark Session (Docker Compose configuration)
# ============================================================

def get_spark_session(
    catalog_name: str = "rest_catalog",
    rest_uri: str = "http://iceberg-rest:8181",
    s3_endpoint: str = "http://minio:9000"
) -> SparkSession:
    """
    Get or create Spark session configured for Docker Compose Iceberg environment.
    """
    return (
        SparkSession.builder
        .appName("IcebergOperations")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .config(f"spark.sql.catalog.{catalog_name}.uri", rest_uri)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", "s3://warehouse/")
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", catalog_name)
        .getOrCreate()
    )


# ============================================================
# 1. ARRAY EXPLORATION OPERATIONS
# ============================================================

def explode_history_by_month(df: DataFrame, array_col: str = "bal_history") -> DataFrame:
    """
    Explode array column to get one row per month with actual date.
    
    Returns: cons_acct_key, rpt_as_of_mo, month_offset, month_date, value
    """
    return (
        df.select(
            "cons_acct_key",
            "rpt_as_of_mo",
            posexplode_outer(col(array_col)).alias("month_offset", "value")
        )
        .withColumn(
            "month_date",
            expr("add_months(rpt_as_of_mo, -month_offset)")
        )
    )


def get_value_at_month_offset(df: DataFrame, array_col: str, offset: int) -> DataFrame:
    """
    Get value from array at specific offset (0 = current month, 1 = previous month, etc.)
    """
    return df.withColumn(
        f"{array_col}_{offset}m_ago",
        element_at(col(array_col), offset + 1)  # element_at is 1-indexed
    )


def get_non_null_months_count(df: DataFrame, array_col: str = "bal_history") -> DataFrame:
    """
    Count how many months have non-null values in the history array.
    """
    return df.withColumn(
        "non_null_months",
        size(filter(col(array_col), lambda x: x.isNotNull()))
    )


def get_null_positions(spark: SparkSession, df: DataFrame, array_col: str = "bal_history") -> DataFrame:
    """
    Get positions (month offsets) where values are NULL.
    Useful for identifying missing data periods.
    """
    # Register temp view to use SQL for complex array operations
    df.createOrReplaceTempView("account_data")
    
    return spark.sql(f"""
        SELECT 
            cons_acct_key,
            rpt_as_of_mo,
            FILTER(
                SEQUENCE(0, 35),
                i -> {array_col}[i] IS NULL
            ) as null_positions
        FROM account_data
    """)


# ============================================================
# 2. TREND ANALYSIS OPERATIONS
# ============================================================

def calculate_rolling_average(df: DataFrame, array_col: str, window_size: int = 3) -> DataFrame:
    """
    Calculate rolling average over the last N months.
    """
    # Get first N elements and calculate average
    return df.withColumn(
        f"{array_col}_avg_{window_size}m",
        aggregate(
            slice(col(array_col), 1, window_size),
            lit(0.0).cast(DecimalType(18, 2)),
            lambda acc, x: acc + coalesce(x, lit(0)),
            lambda acc: acc / window_size
        )
    )


def detect_increasing_trend(df: DataFrame, array_col: str = "dpd_history", months: int = 3) -> DataFrame:
    """
    Detect if values are consistently increasing over the last N months.
    Useful for identifying worsening delinquency.
    """
    return df.withColumn(
        "is_increasing_trend",
        expr(f"""
            size(
                filter(
                    transform(
                        sequence(0, {months - 2}),
                        i -> CASE 
                            WHEN {array_col}[i] IS NOT NULL AND {array_col}[i+1] IS NOT NULL 
                            THEN {array_col}[i] > {array_col}[i+1]
                            ELSE false 
                        END
                    ),
                    x -> x = true
                )
            ) = {months - 1}
        """)
    )


def calculate_month_over_month_change(df: DataFrame, array_col: str = "bal_history") -> DataFrame:
    """
    Calculate change between current month and previous month.
    """
    return df.withColumn(
        f"{array_col}_mom_change",
        element_at(col(array_col), 1) - element_at(col(array_col), 2)
    )


def calculate_yoy_change(df: DataFrame, array_col: str = "bal_history") -> DataFrame:
    """
    Calculate year-over-year change (current month vs 12 months ago).
    """
    return df.withColumn(
        f"{array_col}_yoy_change",
        element_at(col(array_col), 1) - element_at(col(array_col), 13)
    )


# ============================================================
# 3. DELINQUENCY ANALYSIS OPERATIONS
# ============================================================

def find_max_dpd_in_history(df: DataFrame) -> DataFrame:
    """
    Find the maximum DPD value in the entire 36-month history.
    """
    return df.withColumn(
        "max_dpd_ever",
        aggregate(
            col("dpd_history"),
            lit(0),
            lambda acc, x: when(x.isNotNull() & (x > acc), x).otherwise(acc)
        )
    )


def count_delinquent_months(df: DataFrame, dpd_threshold: int = 30) -> DataFrame:
    """
    Count months where DPD exceeded threshold.
    """
    return df.withColumn(
        f"months_dpd_gt_{dpd_threshold}",
        size(filter(col("dpd_history"), lambda x: x > dpd_threshold))
    )


def find_consecutive_delinquent_months(spark: SparkSession, df: DataFrame, dpd_threshold: int = 30) -> DataFrame:
    """
    Find the longest streak of consecutive months with DPD > threshold.
    """
    df.createOrReplaceTempView("dpd_data")
    
    return spark.sql(f"""
        WITH exploded AS (
            SELECT 
                cons_acct_key,
                rpt_as_of_mo,
                pos,
                val,
                CASE WHEN val > {dpd_threshold} THEN 1 ELSE 0 END as is_delinquent
            FROM dpd_data
            LATERAL VIEW posexplode_outer(dpd_history) t AS pos, val
        ),
        grouped AS (
            SELECT 
                cons_acct_key,
                rpt_as_of_mo,
                is_delinquent,
                pos - ROW_NUMBER() OVER (
                    PARTITION BY cons_acct_key, rpt_as_of_mo, is_delinquent 
                    ORDER BY pos
                ) as grp
            FROM exploded
            WHERE is_delinquent = 1
        )
        SELECT 
            cons_acct_key,
            rpt_as_of_mo,
            MAX(streak_length) as max_consecutive_delinquent_months
        FROM (
            SELECT 
                cons_acct_key,
                rpt_as_of_mo,
                COUNT(*) as streak_length
            FROM grouped
            GROUP BY cons_acct_key, rpt_as_of_mo, grp
        )
        GROUP BY cons_acct_key, rpt_as_of_mo
    """)


# ============================================================
# 4. DATA QUALITY OPERATIONS
# ============================================================

def validate_array_length(df: DataFrame, expected_length: int = 36) -> DataFrame:
    """
    Validate that all history arrays have the expected length.
    """
    return df.withColumn(
        "is_valid_bal_history",
        size(col("bal_history")) == expected_length
    ).withColumn(
        "is_valid_dpd_history",
        size(col("dpd_history")) == expected_length
    )


def find_gaps_in_history(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Find gaps (consecutive NULLs) in the balance history.
    Returns information about gap positions and lengths.
    """
    df.createOrReplaceTempView("gap_analysis")
    
    return spark.sql("""
        SELECT 
            cons_acct_key,
            rpt_as_of_mo,
            size(filter(sequence(0, 35), i -> bal_history[i] IS NULL)) as total_null_months,
            CASE 
                WHEN size(filter(sequence(0, 35), i -> bal_history[i] IS NULL)) > 0 
                THEN true 
                ELSE false 
            END as has_gaps
        FROM gap_analysis
    """)


def fill_null_with_previous(spark: SparkSession, df: DataFrame, array_col: str = "bal_history") -> DataFrame:
    """
    Forward-fill NULL values with the previous non-null value.
    Useful for data imputation.
    """
    df.createOrReplaceTempView("fill_data")
    
    return spark.sql(f"""
        SELECT 
            cons_acct_key,
            rpt_as_of_mo,
            transform(
                sequence(0, 35),
                i -> COALESCE(
                    {array_col}[i],
                    -- Look for next non-null value (going forward in time = backward in array)
                    {array_col}[i + 1],
                    {array_col}[i + 2],
                    {array_col}[i + 3]
                )
            ) as {array_col}_filled,
            current_balance,
            current_dpd,
            account_status
        FROM fill_data
    """)


# ============================================================
# 5. COMPARISON OPERATIONS
# ============================================================

def compare_accounts_at_same_period(
    spark: SparkSession, 
    df: DataFrame, 
    acct_key_1: int, 
    acct_key_2: int,
    rpt_month: str
) -> DataFrame:
    """
    Compare history arrays between two accounts for the same reporting month.
    """
    df.createOrReplaceTempView("compare_data")
    
    return spark.sql(f"""
        WITH acct1 AS (
            SELECT * FROM compare_data 
            WHERE cons_acct_key = {acct_key_1} AND rpt_as_of_mo = '{rpt_month}'
        ),
        acct2 AS (
            SELECT * FROM compare_data 
            WHERE cons_acct_key = {acct_key_2} AND rpt_as_of_mo = '{rpt_month}'
        )
        SELECT 
            {acct_key_1} as acct_1,
            {acct_key_2} as acct_2,
            pos as month_offset,
            a1.val as acct_1_balance,
            a2.val as acct_2_balance,
            COALESCE(a1.val, 0) - COALESCE(a2.val, 0) as difference
        FROM acct1
        LATERAL VIEW posexplode_outer(bal_history) a1 AS pos, val
        JOIN acct2
        LATERAL VIEW posexplode_outer(bal_history) a2 AS pos2, val
        WHERE a1.pos = a2.pos2
    """)


# ============================================================
# EXAMPLE USAGE
# ============================================================

if __name__ == "__main__":
    spark = get_spark_session()
    
    # Read from Iceberg table
    table_name = "default.account_summary"
    df = spark.table(table_name)
    
    print("="*60)
    print("1. Explode history to monthly rows")
    print("="*60)
    exploded = explode_history_by_month(df)
    exploded.filter(col("cons_acct_key") == 1).show(10)
    
    print("\n" + "="*60)
    print("2. Get value from 6 months ago")
    print("="*60)
    get_value_at_month_offset(df, "bal_history", 6).select(
        "cons_acct_key", "rpt_as_of_mo", "bal_history_6m_ago"
    ).show()
    
    print("\n" + "="*60)
    print("3. Count non-null months")
    print("="*60)
    get_non_null_months_count(df).select(
        "cons_acct_key", "rpt_as_of_mo", "non_null_months"
    ).show()
    
    print("\n" + "="*60)
    print("4. Calculate 3-month rolling average")
    print("="*60)
    calculate_rolling_average(df, "bal_history", 3).select(
        "cons_acct_key", "rpt_as_of_mo", "bal_history_avg_3m"
    ).show()
    
    print("\n" + "="*60)
    print("5. Find max DPD in history")
    print("="*60)
    find_max_dpd_in_history(df).select(
        "cons_acct_key", "rpt_as_of_mo", "current_dpd", "max_dpd_ever"
    ).show()
    
    print("\n" + "="*60)
    print("6. Count delinquent months (DPD > 30)")
    print("="*60)
    count_delinquent_months(df, 30).select(
        "cons_acct_key", "rpt_as_of_mo", "months_dpd_gt_30"
    ).show()
    
    print("\n" + "="*60)
    print("7. Find gaps in history")
    print("="*60)
    find_gaps_in_history(spark, df).show()
    
    spark.stop()
