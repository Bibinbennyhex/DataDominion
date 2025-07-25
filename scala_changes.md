# 🚀 Complete Guide: Scala Setup, Build, and Spark Submit

I'll walk you through everything step-by-step, assuming you know nothing about Scala!

## 📦 Prerequisites Installation

### 1. **Install Java 8 or 11**
```bash
# Check if Java is installed
java -version

# If not installed, install OpenJDK
sudo yum install java-1.8.0-openjdk-devel -y  # For Amazon Linux/EMR
# OR
sudo apt-get install openjdk-8-jdk -y          # For Ubuntu
```

### 2. **Install sbt (Scala Build Tool)**
```bash
# Download sbt
curl -L https://github.com/sbt/sbt/releases/download/v1.9.7/sbt-1.9.7.tgz -o sbt-1.9.7.tgz

# Extract
tar -xzf sbt-1.9.7.tgz

# Add to PATH (add to ~/.bashrc for permanent)
export PATH=$PATH:$(pwd)/sbt/bin

# Or install via package manager
curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo
sudo mv sbt-rpm.repo /etc/yum.repos.d/
sudo yum install sbt -y
```

## 📁 Project Setup

### 1. **Create Project Directory Structure**
```bash
mkdir -p ascend-dpd-processor/src/main/scala/com/ascend
cd ascend-dpd-processor
```

### 2. **Create `build.sbt` file**
```bash
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

### 3. **Create Scala Source File**
```bash
# Create the Scala file with our optimized code
cat > src/main/scala/com/ascend/MemoryOptimizedDPDProcessor.scala << 'EOF'
package com.ascend

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import java.time.LocalDateTime

object MemoryOptimizedDPDProcessor {
  
  def main(args: Array[String]): Unit = {
    val processor = new MemoryOptimizedDPDProcessor()
    processor.runProcessing()
  }
}

class MemoryOptimizedDPDProcessor {
  
  val spark = SparkSession.builder()
    .appName("Ascend_Memory_Optimized_DPD_Processing")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.1,org.apache.iceberg:iceberg-aws-bundle:1.9.1")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")
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
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
    .config("spark.sql.parquet.compression.codec", "zstd")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def runProcessing(): Unit = {
    println(s"[${LocalDateTime.now()}] Starting Memory-Optimized DPD Processing")
    val startTime = System.currentTimeMillis()
    
    try {
      processWithMemoryOptimization()
      
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

  def processWithMemoryOptimization(): Unit = {
    println(s"[${LocalDateTime.now()}] Loading and processing 27B records...")
    
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
    
    println(s"[${LocalDateTime.now()}] Creating DPD sequences with memory optimization...")
    
    val accountWindow = Window.partitionBy("CONS_ACCT_KEY").orderBy("ACCT_DT")
    
    val dpdGridDF = allHistoricalData
      .withColumn("DPD_Array",
        slice(
          collect_list("DAYS_PAST_DUE_CT").over(
            accountWindow.rowsBetween(-35, 0)
          ), 
          1, 36
        )
      )
      .withColumn("DPD_GRID", concat_ws("~", col("DPD_Array")))
      .withColumn("DAYS_PAST_DUE", col("DAYS_PAST_DUE_CT"))
      .drop("DPD_Array")
    
    dpdGridDF.cache()
    
    println(s"[${LocalDateTime.now()}] Writing summary data...")
    
    dpdGridDF.write
      .format("iceberg")
      .mode("overwrite")
      .option("partition-overwrite-mode", "dynamic")
      .option("write.format.default", "parquet")
      .saveAsTable("ascenddb.summary")
    
    println(s"[${LocalDateTime.now()}] Creating latest DPD summary...")
    
    val latestWindow = Window.partitionBy("CONS_ACCT_KEY").orderBy(desc("ACCT_DT"))
    val latestDPDSummary = dpdGridDF
      .withColumn("rn", row_number().over(latestWindow))
      .filter("rn = 1")
      .select("CONS_ACCT_KEY", "ACCT_DT", "DPD_GRID")
      .drop("rn")
    
    latestDPDSummary.write
      .mode("overwrite")
      .saveAsTable("ascenddb.latest_dpd_summary")
    
    allHistoricalData.unpersist()
    dpdGridDF.unpersist()
    
    println(s"[${LocalDateTime.now()}] Memory-optimized processing completed")
  }

  def validateResults(accountKey: Long = 336): Unit = {
    println(s"[${LocalDateTime.now()}] Validating results for account: $accountKey")
    
    println("=== Memory-Optimized DPD Results ===")
    spark.sql(s"""
      SELECT CONS_ACCT_KEY, ACCT_DT, DAYS_PAST_DUE_CT, DPD_GRID 
      FROM ascenddb.summary 
      WHERE CONS_ACCT_KEY = $accountKey
      ORDER BY ACCT_DT
      LIMIT 10
    """).show(false)
    
    println("=== Latest DPD Summary ===")
    spark.sql(s"""
      SELECT CONS_ACCT_KEY, ACCT_DT, DPD_GRID 
      FROM ascenddb.latest_dpd_summary 
      WHERE CONS_ACCT_KEY = $accountKey
    """).show(false)
  }
}
EOF
```

## 🏗️ Build the Project

### 1. **Build the JAR file**
```bash
# Navigate to project root
cd ascend-dpd-processor

# Clean and build
sbt clean assembly

# This will create the JAR file at:
# target/scala-2.12/ascend-dpd-processor-assembly-1.0.jar
```

### 2. **Upload JAR to S3**
```bash
# Upload the built JAR to S3
aws s3 cp target/scala-2.12/ascend-dpd-processor-assembly-1.0.jar s3://your-bucket/dpd-processor/
```

## 🚀 Spark Submit with Logging and nohup

### 1. **Create a submission script**
```bash
cat > submit-dpd-job.sh << 'EOF'
#!/bin/bash

# Set timestamp for log files
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="/home/hadoop/logs"
LOG_FILE="$LOG_DIR/dpd_processing_$TIMESTAMP.log"
ERR_FILE="$LOG_DIR/dpd_processing_$TIMESTAMP.err"

# Create logs directory if it doesn't exist
mkdir -p $LOG_DIR

# Submit Spark job with nohup and logging
nohup spark-submit \
  --class com.ascend.MemoryOptimizedDPDProcessor \
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
echo "Logs will be written to: $LOG_FILE"
echo "Errors will be written to: $ERR_FILE"
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

# Make the script executable
chmod +x submit-dpd-job.sh
```

### 2. **Alternative: Simple one-liner submission**
```bash
# Simple version with basic logging
nohup spark-submit \
  --class com.ascend.MemoryOptimizedDPDProcessor \
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

# Get the process ID
echo "Process ID: $!"
```

## 📊 Monitor Your Job

### 1. **Check Process Status**
```bash
# Check if process is running
ps aux | grep spark

# Check specific process
ps -p <PID>

# Kill if needed
kill <PID>
```

### 2. **Monitor Logs**
```bash
# Monitor main log
tail -f dpd_processing_*.log

# Monitor YARN logs
yarn logs -applicationId <application_id>

# Check Spark UI (if running)
# Access via: http://<EMR_MASTER_IP>:4040
```

### 3. **Check YARN Application Status**
```bash
# List running applications
yarn application -list

# Get application details
yarn application -status <application_id>

# Kill application if needed
yarn application -kill <application_id>
```

## 🎯 Complete Execution Flow

### 1. **Build and Deploy**
```bash
# 1. Build project
sbt clean assembly

# 2. Upload JAR
aws s3 cp target/scala-2.12/ascend-dpd-processor-assembly-1.0.jar s3://your-bucket/

# 3. Run submission script
./submit-dpd-job.sh
```

### 2. **Monitor Progress**
```bash
# Check logs
tail -f /home/hadoop/logs/dpd_processing_*.log

# Check YARN applications
yarn application -list

# Check Spark UI (port 4040 on master node)
```

## 🚨 Troubleshooting Common Issues

### **Issue 1: sbt command not found**
```bash
# Add to ~/.bashrc
export PATH=$PATH:/path/to/sbt/bin
source ~/.bashrc
```

### **Issue 2: Java version issues**
```bash
# Check Java version
java -version

# Set JAVA_HOME if needed
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
```

### **Issue 3: Memory errors**
```bash
# Reduce memory settings
--conf spark.executor.memory=16g \
--conf spark.driver.memory=12g \
--conf spark.memory.offHeap.size=8g \
```

### **Issue 4: Check logs for errors**
```bash
# Check error logs
cat /home/hadoop/logs/dpd_processing_*.err

# Check YARN logs
yarn logs -applicationId <app_id>
```

## 🎯 Expected Output

When your job runs successfully, you'll see:
```
[2024-01-15 10:00:00] Starting Memory-Optimized DPD Processing
[2024-01-15 13:30:00] Loading and processing 27B records...
[2024-01-15 18:30:00] Creating DPD sequences with memory optimization...
[2024-01-16 00:30:00] Writing summary data...
[2024-01-16 02:00:00] Processing completed in 960 minutes
```

This complete guide will get you from **zero to production** with Scala-based DPD processing on your EMR cluster!