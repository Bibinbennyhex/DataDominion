from pyspark.sql import SparkSession
import boto3

# Spark job for parallel S3 deletion
def delete_s3_objects_spark():
    spark = SparkSession.builder \
        .appName("S3MassDelete") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    # List objects and create RDD
    s3_client = boto3.client('s3')
    bucket_name = "your-bucket"
    
    # Get object list
    objects = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            objects.extend([obj['Key'] for obj in page['Contents']])
    
    # Create RDD and partition for parallel processing
    objects_rdd = spark.sparkContext.parallelize(objects, numSlices=1000)
    
    def delete_batch(partition):
        import boto3
        s3 = boto3.client('s3')
        batch = list(partition)
        
        # Delete in chunks of 1000
        for i in range(0, len(batch), 1000):
            chunk = batch[i:i+1000]
            delete_keys = [{'Key': key} for key in chunk]
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': delete_keys}
            )
        return len(batch)
    
    # Execute parallel deletion
    result = objects_rdd.mapPartitions(delete_batch).collect()
    print(f"Deleted {sum(result)} objects")