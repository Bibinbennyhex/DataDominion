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

    spark.sql("""
                CREATE TABLE if not exists ascenddb.summary_stat(
                                    CONS_ACCT_KEY BIGINT,
                                    BUREAU_MBR_ID VARCHAR(255),
                                    PORT_TYPE_CD VARCHAR(255),
                                    ACCT_TYPE_DTL_CD VARCHAR(255),
                                    ACCT_OPEN_DT DATE,
                                    ORIG_LOAN_AMT BIGINT,
                                    ACCT_CLOSED_DT DATE,
                                    PYMT_TERMS_CD VARCHAR(255),
                                    PYMT_TERMS_DTL_CD VARCHAR(255),
                                    ACCT_DT DATE,
                                    DPD_GRID STRING,
                                    IS_ACTIVE BOOLEAN,
                                    ACCT_STAT_CD VARCHAR(255),
                                    ACCT_PYMT_STAT_CD VARCHAR(255),
                                    ACCT_PYMT_STAT_DTL_CD VARCHAR(255),
                                    ACCT_CREDIT_EXT_AM BIGINT,
                                    ACCT_BAL_AM BIGINT,
                                    PAST_DUE_AM BIGINT,
                                    ACTUAL_PYMT_AM BIGINT,
                                    LAST_PYMT_DT DATE,
                                    SCHD_PYMT_DT DATE,
                                    NEXT_SCHD_PYMT_AM BIGINT,
                                    INTEREST_RATE FLOAT,
                                    COLLATERAL_CD VARCHAR(255),
                                    ORIG_PYMT_DUE_DT DATE,
                                    WRITE_OFF_DT DATE,
                                    WRITE_OFF_AM BIGINT,
                                    ASSET_CLASS_CD VARCHAR(255),
                                    DAYS_PAST_DUE INT,
                                    DAYS_PAST_DUE_CT INT,
                                    HI_CREDIT_AM BIGINT,
                                    CASH_LIMIT_AM BIGINT,
                                    COLLATERAL_AM BIGINT,
                                    TOTAL_WRITE_OFF_AM BIGINT,
                                    PRINCIPAL_WRITE_OFF_AM BIGINT,
                                    SETTLED_AM BIGINT,
                                    INTEREST_RATE_4IN FLOAT,
                                    SUIT_FILED_WILFUL_DEF_STAT_CD VARCHAR(255),
                                    WO_SETTLED_STAT_CD VARCHAR(255),
                                    MSUBID VARCHAR(255),
                                    CREDITLIMITAM BIGINT,
                                    BALANCE_AM BIGINT,
                                    BALANCE_DT DATE,
                                    DFLTSTATUSDT DATE,
                                    RESPONSIBILITY_CD VARCHAR(255),
                                    CHARGEOFFAM BIGINT,
                                    EMI_AMT BIGINT,
                                    TENURE BIGINT,
                                    PAYMENTRATINGCD VARCHAR(255),
                                    PINCODE VARCHAR(255)          
                    )
                USING iceberg
                PARTITIONED BY (months(ACCT_DT))
                CLUSTERED BY (CONS_ACCT_KEY) INTO 64 BUCKETS""")

    while (!currentMonth.isAfter(endMonth)) {
      val nextMonth = currentMonth.plusMonths(1)
      val prevMonth = currentMonth.minusMonths(1)

      println(s"Processing month: $currentMonth ...\n")

      val currentDf = spark.table("ascenddb.accounts_all")
        .filter(col("ACCT_DT").between(lit(currentMonth), lit(nextMonth.minusDays(1))))
        .select("CONS_ACCT_KEY","BUREAU_MBR_ID","PORT_TYPE_CD","ACCT_TYPE_DTL_CD","ACCT_OPEN_DT","ORIG_LOAN_AMT","ACCT_CLOSED_DT","PYMT_TERMS_CD","PYMT_TERMS_DTL_CD","ACCT_DT","ACCT_STAT_CD","ACCT_PYMT_STAT_CD","ACCT_PYMT_STAT_DTL_CD","ACCT_CREDIT_EXT_AM","ACCT_BAL_AM","PAST_DUE_AM","ACTUAL_PYMT_AM","LAST_PYMT_DT","SCHD_PYMT_DT","NEXT_SCHD_PYMT_AM","INTEREST_RATE","COLLATERAL_CD","ORIG_PYMT_DUE_DT","WRITE_OFF_DT","WRITE_OFF_AM","ASSET_CLASS_CD","DAYS_PAST_DUE_CT","HI_CREDIT_AM","CASH_LIMIT_AM","COLLATERAL_AM","TOTAL_WRITE_OFF_AM","PRINCIPAL_WRITE_OFF_AM","SETTLED_AM","INTEREST_RATE_4IN","SUIT_FILED_WILFUL_DEF_STAT_CD","WO_SETTLED_STAT_CD","MSUBID","CREDITLIMITAM","BALANCE_AM","BALANCE_DT","DFLTSTATUSDT","RESPONSIBILITY_CD","CHARGEOFFAM","EMI_AMT","TENURE","PAYMENTRATINGCD","PINCODE")

      val pastDf = spark.table("ascenddb.summary_stat")
        .filter(col("ACCT_DT").between(lit(prevMonth), lit(currentMonth.minusDays(1))))
        .select("CONS_ACCT_KEY","ACCT_DT","DPD_GRID")

      currentDf.createOrReplaceTempView("current_data")
      pastDf.createOrReplaceTempView("past_data")
      
      val result = spark.sql(s"""SELECT 
                                    COALSCE(cd.CONS_ACCT_KEY,pd.CONS_ACCT_KEY),
                                    cd.BUREAU_MBR_ID,
                                    cd.PORT_TYPE_CD,
                                    cd.ACCT_TYPE_DTL_CD,
                                    cd.ACCT_OPEN_DT,
                                    cd.ORIG_LOAN_AMT,
                                    cd.ACCT_CLOSED_DT,
                                    cd.PYMT_TERMS_CD,
                                    cd.PYMT_TERMS_DTL_CD,
                                    COALSCE(cd.ACCT_DT,$currentMonth),
                                    CASE 
                                      WHEN cd.DAYS_PAST_DUE_CT IS NULL 
                                          THEN CONCAT('?','~',regexp_replace(pd.DPD_GRID,"~[^~]*$",''))
                                      WHEN pd.DPD_GRID IS NULL 
                                          THEN CONCAT(cd.DAYS_PAST_DUE_CT,'~',"?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?")
                                          ELSE CONCAT(cd.DAYS_PAST_DUE_CT,'~',pd.DPD_GRID)
                                    END AS DPD_GRID,
                                    cd.DAYS_PAST_DUE_CT IS NOT NULL AS IS_ACTIVE,
                                    cd.ACCT_STAT_CD,
                                    cd.ACCT_PYMT_STAT_CD,
                                    cd.ACCT_PYMT_STAT_DTL_CD,
                                    cd.ACCT_CREDIT_EXT_AM,
                                    cd.ACCT_BAL_AM,
                                    cd.PAST_DUE_AM,
                                    cd.ACTUAL_PYMT_AM,
                                    cd.LAST_PYMT_DT,
                                    cd.SCHD_PYMT_DT,
                                    cd.NEXT_SCHD_PYMT_AM,
                                    cd.INTEREST_RATE,
                                    cd.COLLATERAL_CD,
                                    cd.ORIG_PYMT_DUE_DT,
                                    cd.WRITE_OFF_DT,
                                    cd.WRITE_OFF_AM,
                                    cd.ASSET_CLASS_CD,
                                    cd.DAYS_PAST_DUE_CT AS DAYS_PAST_DUE,
                                    cd.DAYS_PAST_DUE_CT,
                                    cd.HI_CREDIT_AM,
                                    cd.CASH_LIMIT_AM,
                                    cd.COLLATERAL_AM,
                                    cd.TOTAL_WRITE_OFF_AM,
                                    cd.PRINCIPAL_WRITE_OFF_AM,
                                    cd.SETTLED_AM,
                                    cd.SUIT_FILED_WILFUL_DEF_STAT_CD,
                                    cd.WO_SETTLED_STAT_CD,
                                    cd.MSUBID,
                                    cd.CREDITLIMITAM,
                                    cd.BALANCE_AM,
                                    cd.BALANCE_DT,
                                    cd.INTEREST_RATE_4IN,
                                    cd.DFLTSTATUSDT,
                                    cd.RESPONSIBILITY_CD,
                                    cd.CHARGEOFFAM,
                                    cd.EMI_AMT,
                                    cd.TENURE,
                                    cd.PAYMENTRATINGCD,
                                    cd.PINCODE
                                FROM past_data pd
                                FULL OUTER JOIN current_data cd
                                ON pd.CONS_ACCT_KEY = cd.CONS_ACCT_KEY
                            """)

      result.createOrReplaceTempView("result")

      spark.sql("""MERGE INTO ascenddb.summary_stat AS target
                  USING result as src
                  ON target.CONS_ACCT_KEY = src.CONS_ACCT_KEY 
                  AND target.CONS_ACCT_KEY = src.CONS_ACCT_KEY 
                  AND target.ACCT_DT = src.ACCT_DT
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *
                """)

      println("Done\n")
      currentMonth = nextMonth
    }
  }
}

