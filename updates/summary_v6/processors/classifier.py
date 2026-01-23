"""
Summary Pipeline v6.0 - Record Classifier
==========================================

Classifies records into Case I, II, or III for appropriate processing.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import logging

from core.config import PipelineConfig
from core.types import CaseType

logger = logging.getLogger("SummaryPipeline.Classifier")


def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM column to integer."""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


class RecordClassifier:
    """
    Classifies records into processing cases.
    
    Case I:   New accounts (no existing data in summary)
    Case II:  Forward entries (month > max existing month for account)
    Case III: Backfill (month <= max existing month, with newer timestamp)
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def classify(self, batch_df: DataFrame) -> DataFrame:
        """
        Classify records into Case I, II, III.
        
        Args:
            batch_df: Prepared batch DataFrame with column mappings applied
            
        Returns:
            DataFrame with case_type, month_gap columns added (persisted)
        """
        logger.info("Classifying records into Case I/II/III...")
        
        primary = self.config.primary_key
        partition = self.config.partition_key
        timestamp = self.config.timestamp_key
        
        # Add month integer for comparison
        batch_with_month = batch_df.withColumn(
            "month_int",
            F.expr(month_to_int_expr(partition))
        )
        
        # Get summary metadata per account (max month, max timestamp)
        summary_meta = self.spark.sql(f"""
            SELECT 
                {primary},
                MAX({partition}) as max_existing_month,
                MAX({timestamp}) as max_existing_ts,
                {month_to_int_expr(f"MAX({partition})")} as max_month_int
            FROM {self.config.summary_table}
            GROUP BY {primary}
        """)
        
        logger.info(f"Loaded metadata for {summary_meta.count():,} existing accounts")
        
        # Join and classify
        classified = batch_with_month.alias("n").join(
            F.broadcast(summary_meta.alias("s")),
            F.col(f"n.{primary}") == F.col(f"s.{primary}"),
            "left"
        ).select(
            F.col("n.*"),
            F.col("s.max_existing_month"),
            F.col("s.max_existing_ts"),
            F.col("s.max_month_int")
        )
        
        # Apply classification logic
        classified = classified.withColumn(
            "case_type",
            F.when(
                F.col("max_existing_month").isNull(),
                F.lit(CaseType.CASE_I.value)
            ).when(
                F.col("month_int") > F.col("max_month_int"),
                F.lit(CaseType.CASE_II.value)
            ).otherwise(
                F.lit(CaseType.CASE_III.value)
            )
        )
        
        # Calculate month gap for Case II (used for gap-aware array shifting)
        classified = classified.withColumn(
            "month_gap",
            F.when(
                F.col("case_type") == CaseType.CASE_II.value,
                F.col("month_int") - F.col("max_month_int") - 1
            ).otherwise(F.lit(0))
        )
        
        # MONTH_DIFF for array builder (gap + 1)
        classified = classified.withColumn(
            "MONTH_DIFF",
            F.when(
                F.col("case_type") == CaseType.CASE_II.value,
                F.col("month_gap") + 1
            ).otherwise(F.lit(1))
        )
        
        # Persist for multiple filters
        classified = classified.persist()
        
        # Log classification counts
        case_counts = classified.groupBy("case_type").count().collect()
        case_dict = {row["case_type"]: row["count"] for row in case_counts}
        
        logger.info("-" * 60)
        logger.info("CLASSIFICATION RESULTS:")
        logger.info(f"  Case I   (New accounts):    {case_dict.get(CaseType.CASE_I.value, 0):>12,}")
        logger.info(f"  Case II  (Forward entries): {case_dict.get(CaseType.CASE_II.value, 0):>12,}")
        logger.info(f"  Case III (Backfill):        {case_dict.get(CaseType.CASE_III.value, 0):>12,}")
        logger.info(f"  TOTAL:                      {sum(case_dict.values()):>12,}")
        logger.info("-" * 60)
        
        return classified
    
    def get_case_dataframe(self, classified_df: DataFrame, case_type: CaseType) -> DataFrame:
        """
        Filter classified DataFrame for specific case type.
        
        Args:
            classified_df: Classified DataFrame
            case_type: Case type to filter for
            
        Returns:
            Filtered DataFrame
        """
        return classified_df.filter(F.col("case_type") == case_type.value)
    
    def has_records(self, classified_df: DataFrame, case_type: CaseType) -> bool:
        """Check if there are any records for the given case type."""
        return self.get_case_dataframe(classified_df, case_type).limit(1).count() > 0
    
    def get_case_count(self, classified_df: DataFrame, case_type: CaseType) -> int:
        """Get count of records for the given case type."""
        return self.get_case_dataframe(classified_df, case_type).count()
