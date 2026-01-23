import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDate

object DpdGridJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                .appName("Ascend_DPD_Processing")
                .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.iceberg:iceberg-aws-bundle:1.5.2")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .config("spark.sql.join.preferSortMergeJoin", "true")
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
                .config("spark.network.timeout", "800s")
                .config("spark.executor.heartbeatInterval", "60s")
                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
                .config("spark.sql.defaultCatalog", "ascend")
                .config("spark.sql.catalog.ascend", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.ascend.warehouse", "s3://lf-test-1-bucket/ascend-dpd")
                .config("spark.sql.catalog.ascend.type", "hadoop")
                .config("spark.sql.catalog.ascend.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.files.maxPartitionBytes", "268435456")
                .config("spark.sql.parquet.block.size", "268435456")
                .config("spark.sql.sources.bucketing.enabled","true")
                .enableHiveSupport()
                .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val startMonth = LocalDate.parse(args(0))
    val endMonth = LocalDate.parse(args(1))

    generateDpdGrid(spark, startMonth, endMonth)
    spark.stop()
  }

  def generateDpdGrid(spark: SparkSession, startMonth: LocalDate, endMonth: LocalDate): Unit = {
    import spark.implicits._

    var currentMonth = startMonth

    while (!currentMonth.isAfter(endMonth)) {
      val nextMonth = currentMonth.plusMonths(1)

      println(s"Processing month: $currentMonth ...\n")

      val currentDf = spark.table("ascenddb.accounts_all")
        .filter(col("ACCT_DT").between(lit(currentMonth), lit(nextMonth.minusDays(1))))
        .select("CONS_ACCT_KEY","BUREAU_MBR_ID","PORT_TYPE_CD","ACCT_TYPE_DTL_CD","ACCT_OPEN_DT","ORIG_LOAN_AMT","ACCT_CLOSED_DT","PYMT_TERMS_CD","PYMT_TERMS_DTL_CD","ACCT_DT","ACCT_STAT_CD","ACCT_PYMT_STAT_CD","ACCT_PYMT_STAT_DTL_CD","ACCT_CREDIT_EXT_AM","ACCT_BAL_AM","PAST_DUE_AM","ACTUAL_PYMT_AM","LAST_PYMT_DT","SCHD_PYMT_DT","NEXT_SCHD_PYMT_AM","INTEREST_RATE","COLLATERAL_CD","ORIG_PYMT_DUE_DT","WRITE_OFF_DT","WRITE_OFF_AM","ASSET_CLASS_CD","DAYS_PAST_DUE_CT","HI_CREDIT_AM","CASH_LIMIT_AM","COLLATERAL_AM","TOTAL_WRITE_OFF_AM","PRINCIPAL_WRITE_OFF_AM","SETTLED_AM","INTEREST_RATE_4IN","SUIT_FILED_WILFUL_DEF_STAT_CD","WO_SETTLED_STAT_CD","MSUBID","CREDITLIMITAM","BALANCE_AM","BALANCE_DT","DFLTSTATUSDT","RESPONSIBILITY_CD","CHARGEOFFAM","EMI_AMT","TENURE","PAYMENTRATINGCD","PINCODE")

      val latestPrev = spark.read.table("ascenddb.latest_dpd_summary")
        .withColumnRenamed("ACCT_DT", "ACCT_DT_prev")

      var mergedDf = currentDf.join(latestPrev, Seq("CONS_ACCT_KEY"), "left")
        .withColumn("MONTH_DIFF", months_between(col("ACCT_DT"), col("ACCT_DT_prev")).cast("int"))
        .withColumn("FILLER_ARRAY",
          when(col("MONTH_DIFF") > 1, expr("transform(sequence(1, MONTH_DIFF - 1), x -> '?')"))
            .otherwise(array())
        )
        .withColumn("Merged_DPD_Array",
          concat(
            array(col("DAYS_PAST_DUE_CT")),
            col("FILLER_ARRAY"),
            when(col("DPD_GRID").isNotNull, split(col("DPD_GRID"), "~")).otherwise(array())
          )
        )
        .withColumn("DPD_Array_Trimmed",
          slice(concat(col("Merged_DPD_Array"), expr("array_repeat('?', 36)")), 1, 36)
        )
        .withColumn("DPD_GRID",
          concat_ws("~", col("DPD_Array_Trimmed"))
        )
        .withColumn("DAYS_PAST_DUE",
          col("DAYS_PAST_DUE_CT")
        )

      mergedDf.select("CONS_ACCT_KEY","BUREAU_MBR_ID","PORT_TYPE_CD","ACCT_TYPE_DTL_CD","ACCT_OPEN_DT","ORIG_LOAN_AMT","ACCT_CLOSED_DT","PYMT_TERMS_CD","PYMT_TERMS_DTL_CD","ACCT_DT","DPD_GRID","ACCT_STAT_CD","ACCT_PYMT_STAT_CD","ACCT_PYMT_STAT_DTL_CD","ACCT_CREDIT_EXT_AM","ACCT_BAL_AM","PAST_DUE_AM","ACTUAL_PYMT_AM","LAST_PYMT_DT","SCHD_PYMT_DT","NEXT_SCHD_PYMT_AM","INTEREST_RATE","COLLATERAL_CD","ORIG_PYMT_DUE_DT","WRITE_OFF_DT","WRITE_OFF_AM","ASSET_CLASS_CD","DAYS_PAST_DUE","DAYS_PAST_DUE_CT","HI_CREDIT_AM","CASH_LIMIT_AM","COLLATERAL_AM","TOTAL_WRITE_OFF_AM","PRINCIPAL_WRITE_OFF_AM","SETTLED_AM","INTEREST_RATE_4IN","SUIT_FILED_WILFUL_DEF_STAT_CD","WO_SETTLED_STAT_CD","MSUBID","CREDITLIMITAM","BALANCE_AM","BALANCE_DT","DFLTSTATUSDT","RESPONSIBILITY_CD","CHARGEOFFAM","EMI_AMT","TENURE","PAYMENTRATINGCD","PINCODE")
        .write
        .format("iceberg")
        .mode("append")
        .option("compression", "snappy")
        .insertInto("ascenddb.summary")

      val currentMonthDf = mergedDf.select("CONS_ACCT_KEY", "ACCT_DT", "DPD_GRID")
      val latestPrevRenamed = latestPrev.withColumnRenamed("ACCT_DT_prev", "ACCT_DT")

      val mergedLatest = latestPrevRenamed.union(currentMonthDf)
        .groupBy("CONS_ACCT_KEY")
        .agg(
          max_by(col("ACCT_DT"), col("ACCT_DT")).as("ACCT_DT"),
          max_by(col("DPD_GRID"), col("ACCT_DT")).as("DPD_GRID")
        )

      mergedLatest.writeTo("ascenddb.latest_dpd_summary")
      .overwritePartitions()


      println("Done\n")
      currentMonth = nextMonth
    }
  }
}

