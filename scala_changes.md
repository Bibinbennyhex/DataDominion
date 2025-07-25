# 🚀 Complete Step-by-Step Guide: From Zero to Running DPD Processing

## 📋 Prerequisites Check
```bash
# Check if you're on EMR master node
whoami
pwd

# Check Java installation
java -version

# Check if sbt is installed
which sbt || echo "sbt not installed"
```

## 📦 Step 1: Install sbt (if not already installed)

```bash
# Install sbt on EMR
sudo yum update -y
sudo curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
sudo mv sbt-rpm.repo /etc/yum.repos.d/
sudo yum install sbt -y

# Verify installation
sbt --version
```

## 📁 Step 2: Create Project Structure

```bash
# Create project directory
mkdir -p ~/ascend-dpd-processor/src/main/scala/com/ascend
cd ~/ascend-dpd-processor

# Create build.sbt
cat > build.sbt << 'EOF'
name := "ascend-dpd-processor"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
  "org.apache.iceberg" % "iceberg-spark-runtime-3.4_2.12" % "1.9.1",
  "org.apache.iceberg" % "iceberg-aws-bundle" % "1.9.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
EOF
```

## 📝 Step 3: Create the Corrected Scala Source File

```bash
# Create the corrected Scala file
cat > src/main/scala/com/ascend/CalendarAwareDPDProcessor.scala << 'EOF'
package com.ascend

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDateTime

object CalendarAwareDPDProcessor {
  
  def main(args: Array[String]): Unit = {
    val processor = new CalendarAwareDPDProcessor()
    processor.runProcessing()
  }
}

class CalendarAwareDPDProcessor {
  
  val spark = SparkSession.builder()
    .appName("Ascend_Calendar_Aware_DPD_Processing")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.defaultCatalog", "ascend")
    .config("spark.sql.catalog.ascend", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.ascend.warehouse", "s3://lf-test-1-bucket/ascend-dpd")
    .config("spark.sql.catalog.ascend.type", "hadoop")
    .config("spark.sql.catalog.ascend.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.sql.shuffle.partitions", "3200")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.columnVector.offheap.enabled", "true")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "16g")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def runProcessing(): Unit = {
    println(s"[${LocalDateTime.now()}] Starting Calendar-Aware DPD Processing")
    val startTime = System.currentTimeMillis()
    
    try {
      processWithCalendarAwareness()
      
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000 / 60
      println(s"[${LocalDateTime.now()}] Processing completed in ${duration} minutes")
      
      validateResults(336)
      
    } catch {
      case e: Exception =>
        println(s"[ERROR] Processing failed: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      spark.stop()
    }
  }

  def processWithCalendarAwareness(): Unit = {
    println(s"[${LocalDateTime.now()}] Creating calendar-aware DPD sequences...")
    
    // Step 1: Load all historical data
    val allHistoricalData = spark.sql("""
      SELECT 
        CONS_ACCT_KEY, 
        ACCT_DT, 
        DAYS_PAST_DUE_CT,
        BUREAU_MBR_ID, PORT_TYPE_CD, ACCT_TYPE_DTL_CD,
        ACCT_OPEN_DT, ORIG_LOAN_AMT, ACCT_CLOSED_DT, PYMT_TERMS_CD,
        PYMT_TERMS_DTL_CD, ACCT_STAT_CD, ACCT_PYMT_STAT_CD,
        ACCT_PYMT_STAT_DTL_CD, ACCT_CREDIT_EXT_AM, ACCT_BAL_AM, PAST_DUE_AM,
        ACTUAL_PYMT_AM, LAST_PYMT_DT, SCHD_PYMT_DT, NEXT_SCHD_PYMT_AM,
        INTEREST_RATE, COLLATERAL_CD, ORIG_PYMT_DUE_DT, WRITE_OFF_DT,
        WRITE_OFF_AM, ASSET_CLASS_CD, HI_CREDIT_AM, CASH_LIMIT_AM,
        COLLATERAL_AM, TOTAL_WRITE_OFF_AM, PRINCIPAL_WRITE_OFF_AM,
        SETTLED_AM, INTEREST_RATE_4IN, SUIT_FILED_WILFUL_DEF_STAT_CD,
        WO_SETTLED_STAT_CD, MSUBID, CREDITLIMITAM, BALANCE_AM, BALANCE_DT,
        DFLTSTATUSDT, RESPONSIBILITY_CD, CHARGEOFFAM, EMI_AMT, TENURE,
        PAYMENTRATINGCD, PINCODE
      FROM ascenddb.accounts_all
      ORDER BY CONS_ACCT_KEY, ACCT_DT
    """).cache()
    
    // Step 2: Add calendar month information
    val dataWithCalendarInfo = allHistoricalData
      .withColumn("calendar_month", date_trunc("month", col("ACCT_DT")))
      .withColumn("expected_sequence", 
        months_between(
          col("calendar_month"),
          first("calendar_month").over(Window.partitionBy("CONS_ACCT_KEY").orderBy("calendar_month"))
        ).cast("int"))
    
    // Step 3: Create proper DPD grid with calendar awareness
    val accountWindow = Window.partitionBy("CONS_ACCT_KEY").orderBy("calendar_month")
    
    val dpdGridDF = dataWithCalendarInfo
      // Collect data with calendar positions
      .withColumn("calendar_positions",
        collect_list(
          struct(
            col("expected_sequence"),
            col("DAYS_PAST_DUE_CT")
          )
        ).over(accountWindow.rowsBetween(-35, 0))
      )
      
      // Generate calendar-aware DPD grid
      .withColumn("DPD_GRID", 
        expr("""
          concat_ws('~',
            transform(
              sequence(0, 35),
              i -> {
                let current_pos = expected_sequence;
                let target_pos = current_pos - i;
                
                -- Find DPD value for target position
                let matching_record = filter(calendar_positions, x -> x.expected_sequence = target_pos);
                
                if (size(matching_record) > 0) 
                  cast(element_at(matching_record, 1).DAYS_PAST_DUE_CT as string)
                else 
                  '?'
              }
            )
          )
        """))
      
      .withColumn("DAYS_PAST_DUE", col("DAYS_PAST_DUE_CT"))
      .drop("calendar_positions", "calendar_month", "expected_sequence")
    
    println(s"[${LocalDateTime.now()}] Writing results...")
    
    // Step 4: Write results
    dpdGridDF.write
      .format("iceberg")
      .mode("overwrite")
      .option("partition-overwrite-mode", "dynamic")
      .saveAsTable("ascenddb.summary")
    
    // Step 5: Create latest summary
    val latestWindow = Window.partitionBy("CONS_ACCT_KEY").orderBy(desc("ACCT_DT"))
    val latestDPDSummary = dpdGridDF
      .withColumn("rn", row_number().over(latestWindow))
      .filter("rn = 1")
      .select("CONS_ACCT_KEY", "ACCT_DT", "DPD_GRID")
      .drop("rn")
    
    latestDPDSummary.write
      .mode("overwrite")
      .saveAsTable("ascenddb.latest_dpd_summary")
    
    // Cleanup
    allHistoricalData.unpersist()
    
    println(s"[${LocalDateTime.now()}] Calendar-aware processing completed")
  }

  def validateResults(accountKey: Long = 336): Unit = {
    println(s"[${LocalDateTime.now()}] Validating results for account: $accountKey")
    
    println("=== Calendar-Aware DPD Results (CORRECTED) ===")
    spark.sql(s"""
      SELECT CONS_ACCT_KEY, ACCT_DT, DAYS_PAST_DUE_CT, DPD_GRID 
      FROM ascenddb.summary 
      WHERE CONS_ACCT_KEY = $accountKey
      ORDER BY ACCT_DT
    """).show(false)
  }
}
EOF
```

## 🏗️ Step 4: Build the Project

```bash
# Navigate to project directory
cd ~/ascend-dpd-processor

# Clean and build
sbt clean assembly

# This will take 5-10 minutes and create the JAR file
# Final JAR location: target/scala-2.12/ascend-dpd-processor-assembly-1.0.jar
```

## ☁️ Step 5: Upload JAR to S3

```bash
# Upload the built JAR to your S3 bucket
aws s3 cp target/scala-2.12/ascend-dpd-processor-assembly-1.0.jar s3://your-bucket/dpd-processor/

# Verify upload
aws s3 ls s3://your-bucket/dpd-processor/
```

## 📝 Step 6: Create Submission Script

```bash
# Create submission script
cat > ~/submit-dpd-job.sh << 'EOF'
#!/bin/bash

# Set timestamp for log files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="/home/hadoop/logs"
LOG_FILE="$LOG_DIR/dpd_processing_$TIMESTAMP.log"
ERR_FILE="$LOG_DIR/dpd_processing_$TIMESTAMP.err"

# Create logs directory if it doesn't exist
mkdir -p $LOG_DIR

echo "Starting DPD processing job at $(date)"
echo "Logs will be written to: $LOG_FILE"
echo "Errors will be written to: $ERR_FILE"

# Submit Spark job with nohup and logging
nohup spark-submit \
  --class com.ascend.CalendarAwareDPDProcessor \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.memory=20g \
  --conf spark.executor.cores=6 \
  --conf spark.executor.instances=25 \
  --conf spark.driver.memory=16g \
  --conf spark.driver.cores=4 \
  --conf spark.sql.shuffle.partitions=3200 \
  --conf spark.sql.files.maxPartitionBytes=134217728 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=16g \
  --conf spark.sql.execution.arrow.maxRecordsPerBatch=5000 \
  s3://your-bucket/dpd-processor/ascend-dpd-processor-assembly-1.0.jar \
  > $LOG_FILE 2> $ERR_FILE &

# Capture the process ID
SPARK_PID=$!

echo "Spark job submitted with PID: $SPARK_PID"
echo "To monitor progress: tail -f $LOG_FILE"
echo "To check process: ps -p $SPARK_PID"

# Wait for the process to complete and capture exit code
wait $SPARK_PID
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "Spark job completed successfully at $(date)"
else
    echo "Spark job failed with exit code: $EXIT_CODE at $(date)"
    echo "Check error log: $ERR_FILE"
fi

exit $EXIT_CODE
EOF

# Make script executable
chmod +x ~/submit-dpd-job.sh
```

## 🚀 Step 7: Run the Processing Job

```bash
# Run the submission script
~/submit-dpd-job.sh

# OR run directly with nohup (simpler approach)
nohup spark-submit \
  --class com.ascend.CalendarAwareDPDProcessor \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.memory=20g \
  --conf spark.executor.cores=6 \
  --conf spark.executor.instances=25 \
  --conf spark.driver.memory=16g \
  --conf spark.driver.cores=4 \
  --conf spark.sql.shuffle.partitions=3200 \
  s3://your-bucket/dpd-processor/ascend-dpd-processor-assembly-1.0.jar \
  > dpd_processing_$(date +%Y%m%d_%H%M%S).log 2>&1 &

# Get the background process ID
echo "Background process ID: $!"
```

## 👀 Step 8: Monitor the Job

```bash
# Check if process is running
ps aux | grep spark

# Monitor logs
tail -f dpd_processing_*.log

# Check YARN applications
yarn application -list

# Get specific application details (replace with actual app ID)
yarn application -status application_1234567890123_0001

# Check Spark UI (access via browser)
# http://<EMR_MASTER_PUBLIC_IP>:4040
```

## 🔍 Step 9: Verify Results

```bash
# Check the output after job completion
spark-sql << 'EOF'
-- Check total records
SELECT COUNT(*) as total_records FROM ascenddb.summary;

-- Check latest summary records
SELECT COUNT(*) as latest_records FROM ascenddb.latest_dpd_summary;

-- Validate specific account (336) - THIS SHOULD SHOW CORRECT GAP HANDLING
SELECT CONS_ACCT_KEY, ACCT_DT, DAYS_PAST_DUE_CT, DPD_GRID 
FROM ascenddb.summary 
WHERE CONS_ACCT_KEY = 336
ORDER BY ACCT_DT;

-- Check latest record for account 336
SELECT CONS_ACCT_KEY, ACCT_DT, DPD_GRID 
FROM ascenddb.latest_dpd_summary 
WHERE CONS_ACCT_KEY = 336;
EOF
```

## 🛑 Step 10: Troubleshooting (if needed)

```bash
# If job fails, check error logs
cat dpd_processing_*.err

# Check YARN logs for specific application
yarn logs -applicationId <your_application_id>

# Kill running application if needed
yarn application -kill <your_application_id>

# Check cluster resources
yarn top
```

## 📋 Expected Timeline

```bash
# Timeline estimation:
# 0-5 min:   Job startup and initialization
# 5-215 min: Data loading (3.5 hours)
# 215-515 min: DPD grid generation (5 hours)  
# 515-875 min: Results writing (6 hours)
# 875-900 min: Cleanup and validation (15 min)
# Total: ~15 hours
```

## ✅ Success Verification

When successful, you should see output like:
```
[2024-01-16 02:00:10] Validating results for account: 336
=== Calendar-Aware DPD Results (CORRECTED) ===
+--------------+----------+------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|CONS_ACCT_KEY |ACCT_DT   |DAYS_PAST_DUE_CT  |DPD_GRID                                                                                                                                                                                                          |
+--------------+----------+------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|336           |2019-01-15|0                 |0~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?                                                                                                                                          |
|336           |2019-02-15|5                 |5~0~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?                                                                                                                                          |
|336           |2019-03-15|15                |15~5~0~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?                                                                                                                                          |
|336           |2019-04-15|30                |30~15~5~0~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?                                                                                                                                         |
|336           |2019-06-15|45                |45~?~30~15~5~0~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?~?                                                                                                                                      |
+--------------+----------+------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

## 🎉 You're Done!

This complete step-by-step guide will take you from zero to a fully running, correctly gap-handling DPD processing system on your EMR cluster!