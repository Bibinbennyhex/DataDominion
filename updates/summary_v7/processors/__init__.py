"""
Summary Pipeline v7.0 - Processors (Copied from v6 with optimizations)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
import logging

from core.config import PipelineConfig
from core.types import CaseType, DataType

logger = logging.getLogger("SummaryPipeline.Processors")


def month_to_int_expr(col_name: str) -> str:
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


class ArrayBuilder:
    """Rolling array transformations."""
    
    TYPE_MAP = {
        DataType.INTEGER: IntegerType(),
        DataType.STRING: StringType(),
    }
    
    @classmethod
    def get_spark_type(cls, data_type: DataType):
        return cls.TYPE_MAP.get(data_type, StringType())
    
    @classmethod
    def create_initial_array(cls, rolling_col, value_column: str):
        spark_type = cls.get_spark_type(rolling_col.data_type)
        num_cols = rolling_col.history_length
        
        return F.array(
            F.col(value_column).cast(spark_type),
            *[F.lit(None).cast(spark_type) for _ in range(num_cols - 1)]
        ).alias(rolling_col.history_column_name)
    
    @classmethod
    def create_shifted_array(cls, rolling_col, value_column: str,
                            month_diff_column: str = "MONTH_DIFF",
                            current_alias: str = "c", previous_alias: str = "p"):
        spark_type = cls.get_spark_type(rolling_col.data_type)
        num_cols = rolling_col.history_length
        history_col = rolling_col.history_column_name
        
        current_value = F.coalesce(
            F.col(f"{current_alias}.{value_column}").cast(spark_type),
            F.lit(None).cast(spark_type)
        )
        
        prev_history = F.coalesce(
            F.col(f"{previous_alias}.{history_col}"),
            F.array(*[F.lit(None).cast(spark_type) for _ in range(num_cols)])
        )
        
        null_array = F.array_repeat(
            F.lit(None).cast(spark_type),
            F.col(month_diff_column) - F.lit(1)
        )
        
        new_account_array = F.array(
            current_value,
            *[F.lit(None).cast(spark_type) for _ in range(num_cols - 1)]
        )
        
        return (
            F.when(F.col(month_diff_column) == 1,
                   F.slice(F.concat(F.array(current_value), prev_history), 1, num_cols))
            .when(F.col(month_diff_column) > 1,
                  F.slice(F.concat(F.array(current_value), null_array, prev_history), 1, num_cols))
            .otherwise(new_account_array)
            .alias(history_col)
        )


def generate_grid_column(df, grid_col, rolling_columns):
    source_rc = next((rc for rc in rolling_columns if rc.name == grid_col.source_rolling), None)
    if not source_rc:
        return df
    
    history_col = source_rc.history_column_name
    if history_col not in df.columns:
        return df
    
    return df.withColumn(
        grid_col.name,
        F.concat_ws(
            grid_col.separator,
            F.transform(F.col(history_col), lambda x: F.coalesce(x.cast(StringType()), F.lit(grid_col.null_placeholder)))
        )
    )


class RecordClassifier:
    """Classifies records into Case I/II/III."""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def classify(self, batch_df: DataFrame) -> DataFrame:
        primary = self.config.primary_key
        partition = self.config.partition_key
        timestamp = self.config.timestamp_key
        
        batch_with_month = batch_df.withColumn("month_int", F.expr(month_to_int_expr(partition)))
        
        summary_meta = self.spark.sql(f"""
            SELECT {primary},
                   MAX({partition}) as max_existing_month,
                   {month_to_int_expr(f"MAX({partition})")} as max_month_int
            FROM {self.config.summary_table}
            GROUP BY {primary}
        """)
        
        classified = batch_with_month.alias("n").join(
            F.broadcast(summary_meta.alias("s")),
            F.col(f"n.{primary}") == F.col(f"s.{primary}"),
            "left"
        ).select(F.col("n.*"), F.col("s.max_existing_month"), F.col("s.max_month_int"))
        
        classified = classified.withColumn(
            "case_type",
            F.when(F.col("max_existing_month").isNull(), F.lit(CaseType.CASE_I.value))
            .when(F.col("month_int") > F.col("max_month_int"), F.lit(CaseType.CASE_II.value))
            .otherwise(F.lit(CaseType.CASE_III.value))
        ).withColumn(
            "MONTH_DIFF",
            F.when(F.col("case_type") == CaseType.CASE_II.value,
                   F.col("month_int") - F.col("max_month_int"))
            .otherwise(F.lit(1))
        )
        
        return classified.persist()
    
    def get_case_dataframe(self, classified_df, case_type):
        return classified_df.filter(F.col("case_type") == case_type.value)
    
    def has_records(self, classified_df, case_type):
        return self.get_case_dataframe(classified_df, case_type).limit(1).count() > 0
    
    def get_case_count(self, classified_df, case_type):
        return self.get_case_dataframe(classified_df, case_type).count()


class CaseIProcessor:
    """New accounts processor."""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def process(self, case_i_df: DataFrame) -> DataFrame:
        logger.info(f"Processing Case I: {case_i_df.count():,} records")
        
        result = case_i_df
        for rc in self.config.rolling_columns:
            result = result.withColumn(rc.source_column, F.expr(rc.mapper_expr))
        
        for rc in self.config.rolling_columns:
            result = result.withColumn(rc.history_column_name,
                                       ArrayBuilder.create_initial_array(rc, rc.source_column))
        
        for gc in self.config.grid_columns:
            result = generate_grid_column(result, gc, self.config.rolling_columns)
        
        drop_cols = ["month_int", "max_existing_month", "max_month_int", "case_type", "MONTH_DIFF"]
        result = result.drop(*[c for c in drop_cols if c in result.columns])
        
        return result


class CaseIIProcessor:
    """Forward entries processor."""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def process(self, case_ii_df: DataFrame) -> DataFrame:
        logger.info(f"Processing Case II: {case_ii_df.count():,} records")
        
        affected_keys = case_ii_df.select(self.config.primary_key).distinct()
        
        # Use columnar projection - only read needed columns
        minimal_cols = self.config.get_minimal_columns_for_case_ii()
        latest = self.spark.read.table(self.config.latest_summary_table).select(*minimal_cols)
        latest_affected = latest.join(affected_keys, self.config.primary_key, "semi")
        
        result = case_ii_df
        for rc in self.config.rolling_columns:
            result = result.withColumn(rc.source_column, F.expr(rc.mapper_expr))
        
        joined = result.alias("c").join(
            latest_affected.alias("p"),
            F.col(f"c.{self.config.primary_key}") == F.col(f"p.{self.config.primary_key}"),
            "inner"
        )
        
        drop_cols = ["month_int", "max_existing_month", "max_month_int", "case_type"]
        select_exprs = [F.col(f"c.{c}") for c in case_ii_df.columns if c not in drop_cols]
        
        for rc in self.config.rolling_columns:
            select_exprs.append(ArrayBuilder.create_shifted_array(rc, rc.source_column))
        
        result = joined.select(*select_exprs)
        if "MONTH_DIFF" in result.columns:
            result = result.drop("MONTH_DIFF")
        
        for gc in self.config.grid_columns:
            result = generate_grid_column(result, gc, self.config.rolling_columns)
        
        return result


class CaseIIIProcessor:
    """Backfill processor with history rebuild."""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
    
    def process(self, case_iii_df: DataFrame) -> DataFrame:
        logger.info(f"Processing Case III: {case_iii_df.count():,} records")
        
        # Apply mappers
        result = case_iii_df
        for rc in self.config.rolling_columns:
            result = result.withColumn(rc.source_column, F.expr(rc.mapper_expr))
        
        # Build SQL for history rebuild (simplified for v7)
        primary = self.config.primary_key
        partition = self.config.partition_key
        timestamp = self.config.timestamp_key
        
        affected_accounts = result.select(primary).distinct()
        result.createOrReplaceTempView("backfill_records")
        affected_accounts.createOrReplaceTempView("affected_accounts")
        
        # Simplified rebuild using existing patterns
        preserve_cols = [c for c in result.columns 
                        if c not in [primary, partition, timestamp, "month_int", 
                                    "case_type", "MONTH_DIFF", "max_existing_month", "max_month_int"]]
        
        struct_cols = ", ".join(preserve_cols)
        history_len = self.config.history_length
        
        rolling_sql = []
        for rc in self.config.rolling_columns:
            sql_type = "INT" if rc.data_type == DataType.INTEGER else "STRING"
            rolling_sql.append(f"""
                TRANSFORM(SEQUENCE(0, {history_len - 1}), 
                    offset -> AGGREGATE(
                        FILTER(e.month_values, mv -> mv.month_int = e.month_int - offset),
                        CAST(NULL AS {sql_type}), (acc, x) -> x.{rc.source_column}
                    )) as {rc.history_column_name}
            """)
        
        combined_sql = f"""
        WITH combined AS (
            SELECT {primary}, {partition}, {month_to_int_expr(partition)} as month_int,
                   {struct_cols}, {timestamp}, 1 as priority
            FROM backfill_records
            UNION ALL
            SELECT s.{primary}, s.{partition}, {month_to_int_expr(f"s.{partition}")} as month_int,
                   {', '.join([f's.{c}' for c in preserve_cols])}, s.{timestamp}, 2
            FROM {self.config.summary_table} s
            WHERE s.{primary} IN (SELECT {primary} FROM affected_accounts)
        ),
        deduped AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary}, {partition} ORDER BY priority, {timestamp} DESC) as rn
            FROM combined
        ),
        account_months AS (
            SELECT {primary}, COLLECT_LIST(STRUCT(month_int, {struct_cols})) as month_values,
                   COLLECT_LIST(STRUCT({partition}, month_int, {timestamp})) as month_metadata
            FROM deduped WHERE rn = 1
            GROUP BY {primary}
        ),
        exploded AS (
            SELECT {primary}, month_values, meta.{partition}, meta.month_int, meta.{timestamp}
            FROM account_months LATERAL VIEW EXPLODE(month_metadata) t AS meta
        )
        SELECT e.{primary}, e.{partition}, {', '.join(rolling_sql)}, e.{timestamp}
        FROM exploded e
        """
        
        result = self.spark.sql(combined_sql)
        
        for gc in self.config.grid_columns:
            result = generate_grid_column(result, gc, self.config.rolling_columns)
        
        return result
