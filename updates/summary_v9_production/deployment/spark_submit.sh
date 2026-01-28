#!/bin/bash
# =============================================================================
# Summary Pipeline v9.1 - Spark Submit Script
# =============================================================================

set -e

# Configuration
PIPELINE_PATH="s3://in-cs-ivaps-data-bucket-prod-ap-south-1/ivaps_prod/code/summary_pipeline.py"
CONFIG_PATH="s3://in-cs-ivaps-data-bucket-prod-ap-south-1/ivaps_prod/config/pipeline_config.json"
LOG_PATH="s3://in-cs-ivaps-data-bucket-prod-ap-south-1/ivaps_prod/logs/"

# Parse arguments
MODE=${1:-incremental}
FILTER=${2:-""}

echo "============================================================"
echo "Summary Pipeline v9.1 Fixed"
echo "============================================================"
echo "Mode: $MODE"
echo "Filter: $FILTER"
echo "Config: $CONFIG_PATH"
echo "============================================================"

# Build spark-submit command
SPARK_CMD="spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name DPD-Summary-Pipeline-v9-fixed \
    --conf spark.yarn.maxAppAttempts=2 \
    --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
    --conf spark.executor.instances=150 \
    --conf spark.executor.cores=5 \
    --conf spark.executor.memory=45g \
    --conf spark.executor.memoryOverhead=8g \
    --conf spark.driver.cores=8 \
    --conf spark.driver.memory=64g \
    --conf spark.driver.memoryOverhead=8g \
    --conf spark.driver.maxResultSize=8g \
    --conf spark.sql.shuffle.partitions=8192 \
    --conf spark.default.parallelism=8192 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.localShuffleReader.enabled=true \
    --conf spark.sql.autoBroadcastJoinThreshold=500m \
    --conf spark.sql.broadcastTimeout=1200 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=50 \
    --conf spark.dynamicAllocation.maxExecutors=250 \
    --conf spark.dynamicAllocation.initialExecutors=100 \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
    --conf spark.network.timeout=800s \
    --conf spark.executor.heartbeatInterval=60s \
    --conf spark.rpc.askTimeout=600s \
    --conf spark.speculation=true \
    --conf spark.speculation.multiplier=2 \
    --conf spark.speculation.quantile=0.9 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.ascend_iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.ascend_iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.ascend_iceberg.warehouse=s3://in-cs-ivaps-data-bucket-prod-ap-south-1/ivaps_prod/persistent/ascend_summary/summary_complete/ \
    --conf spark.sql.catalog.ascend_iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.hadoop.fs.s3a.connection.maximum=200 \
    --conf spark.hadoop.fs.s3a.fast.upload=true \
    --conf spark.hadoop.fs.s3a.multipart.size=512m \
    $PIPELINE_PATH \
    --config $CONFIG_PATH \
    --mode $MODE"

# Add filter if provided
if [ -n "$FILTER" ]; then
    SPARK_CMD="$SPARK_CMD --filter \"$FILTER\""
fi

echo "Executing spark-submit..."
echo ""

eval $SPARK_CMD

echo ""
echo "============================================================"
echo "Pipeline completed successfully"
echo "============================================================"
