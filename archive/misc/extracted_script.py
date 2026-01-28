from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
import datetime
from datetime import date
from datetime import datetime as datetime_sub
from dateutil.relativedelta import relativedelta
from pyspark import StorageLevel

# Initialize Spark Session (Assuming it's already available in the environment as 'spark')
# spark = SparkSession.builder.appName("ImageContextGeneration").getOrCreate()

# --- Image 1 Content ---

accounts_all_df = spark.sql("select * from spark_catalog.edf_gold.ivaps_consumer_accounts_all where base_ts >= date '2025-01-15' or rpt_as_of_mo >= '2026-01'")
accounts_window_spec = Window.partitionBy("cons_acct_key", "rpt_as_of_mo").orderBy(F.col("base_ts").desc(), F.col("insert_ts").desc(), F.col("update_ts").desc())

accounts_all_deduped_df = accounts_all_df.withColumn("rn", F.row_number().over(accounts_window_spec)).filter(F.col("rn") == 1).drop("rn")

latest_summary_metadata = spark.read.table("primary_catalog.edf_gold.latest_summary").select(
    "cons_acct_key",
    F.col("rpt_as_of_mo").alias("max_existing_month"),
    F.col("base_ts").alias("max_existing_base_ts")
)

classified = latest_summary_metadata.alias("s").join(
    accounts_all_deduped_df.alias("n"),
    F.col("n.cons_acct_key") == F.col("s.cons_acct_key"),
    "right"
).select(
    F.col("n.*"),
    F.col("s.max_existing_month"),
    F.col("s.max_existing_base_ts")
).withColumn(
    "case_type",
    F.when(F.col("max_existing_month").isNull(), F.lit("1"))   # New account
    .when(F.col("rpt_as_of_mo") > F.col("max_existing_month"), F.lit("2")) # Forward
    .otherwise(F.lit("3")) # Backfill
)

case_3 = classified.filter(F.col("case_type") == 3)
case_3_filtered = case_3.filter(F.col("rpt_as_of_mo") != '2025-12')
case_3_filtered.cache()
case_3_filtered.count()

# --- Image 2 Content ---

backfill_cons = case_3_filtered.select('cons_acct_key').distinct()

summary_base_df = (
    spark.read.table("primary_catalog.edf_gold.summary")
    .select(
        "cons_acct_key",
        F.col("rpt_as_of_mo").alias("summary_rpt_as_of_mo"),
        "payment_history_grid",
        "asset_class_cd_4in_history",
        "days_past_due_history",
        "payment_rating_cd_history",
        "past_due_am_history",
        "credit_limit_am_history",
        "balance_am_history",
        "actual_payment_am_history"
    )
)

summary_base_df_filtered = summary_base_df.join(F.broadcast(backfill_cons), "cons_acct_key", "left_semi")

combined_df = case_3_filtered.join(
    summary_base_df_filtered, "cons_acct_key", "left"
)

combined_df = combined_df.withColumn(
    "MONTH_DIFF",
    F.when(
        F.col("summary_rpt_as_of_mo").isNotNull(),
        F.months_between(
            F.to_date(F.col("rpt_as_of_mo"), "yyyy-MM"),
            F.to_date(F.col("summary_rpt_as_of_mo"), "yyyy-MM"),
        ).cast(IntegerType()),
    ).otherwise(F.lit(None))
)

combined_df.persist(StorageLevel.DISK_ONLY)

temp_filter = combined_df.filter((F.col("MONTH_DIFF") < 0) | ((F.col("MONTH_DIFF") >= 0) & (F.col("MONTH_DIFF") < 36)))

w_remover = Window.partitionBy("cons_acct_key")

df_out = (
    temp_filter.withColumn(
        "max_neg_diff",
        F.max(F.when(F.col("MONTH_DIFF") < 0, F.col("MONTH_DIFF"))).over(w_remover),
    )
    .filter(
        (F.col("MONTH_DIFF") >= 0)
        | (F.col("MONTH_DIFF").isNull())
        | (F.col("MONTH_DIFF") == F.col("max_neg_diff"))
    )
    .drop("max_neg_diff")
)

# --- Image 3 Content ---

# Assuming backfill_track_df comes from df_out
backfill_track_df = df_out

# Updating credit_limit_am_history array based on MONTH_DIFF
# Note: Using transform for compatibility, array_update is also an option if available.
backfill_track_df = backfill_track_df.withColumn(
    "credit_limit_am_history", 
    F.expr("transform(credit_limit_am_history, (x, i) -> IF(i == MONTH_DIFF, acct_credit_ext_am, x))")
)

# Verification step from image
backfill_track_df.filter(F.col("cons_acct_key") == 240002797).select(
    "cons_acct_key", "MONTH_DIFF", "summary_rpt_as_of_mo", "backfill_rpt_as_of_mo", 
    "acct_credit_ext_am", "credit_limit_am_history"
).orderBy(F.col("backfill_rpt_as_of_mo").desc()).show()

temp_df = backfill_track_df.filter(F.col("cons_acct_key") == 240002797).select(
    "cons_acct_key", "MONTH_DIFF", "summary_rpt_as_of_mo", "backfill_rpt_as_of_mo", 
    "acct_credit_ext_am", "credit_limit_am_history"
).orderBy(F.col("backfill_rpt_as_of_mo").desc())
temp_df.cache()
temp_df.count()

temp_df_agg = temp_df.groupBy("cons_acct_key", "summary_rpt_as_of_mo").agg(
    F.map_from_arrays(
        F.collect_list(F.col("MONTH_DIFF")),
        F.collect_list(F.struct("acct_credit_ext_am", "backfill_rpt_as_of_mo"))
    ).alias("update_map")
)

temp_df_agg = temp_df_agg.withColumn("rpt_as_of_mo", F.col("summary_rpt_as_of_mo"))

# Assuming temp_summary_df corresponds to summary_base_df or similar
temp_summary_df = summary_base_df

result = temp_summary_df.join(temp_df_agg, ["cons_acct_key", "rpt_as_of_mo"], "right").select(
    "cons_acct_key",
    F.col("credit_limit_am_history").alias("old_credit_limit_am_history"),
    F.array([
        F.coalesce(
            F.col("update_map")[F.lit(i)]["acct_credit_ext_am"],
            F.col("credit_limit_am_history")[i]
        )
        for i in range(36)
    ]).alias("new_credit_limit_am_history"),
    "rpt_as_of_mo"
)

# Show result (optional, inferred from potential final step)
result.show()
