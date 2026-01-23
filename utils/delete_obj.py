#!/usr/bin/env python3
"""
Spark job for deleting millions of S3 objects in parallel using EMR
Run this script on EMR cluster with: spark-submit s3_mass_delete.py
"""

from pyspark.sql import SparkSession
import boto3
import logging
from typing import Iterator, List
import time
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with S3 optimizations"""
    return SparkSession.builder \
        .appName("S3MassDelete") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "200") \
        .config("spark.hadoop.fs.s3a.threads.max", "64") \
        .config("spark.hadoop.fs.s3a.max.total.tasks", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def get_s3_objects(bucket_name: str, prefix: str = "") -> List[str]:
    """Get list of all objects in S3 bucket"""
    s3_client = boto3.client('s3')
    objects = []
    
    logger.info(f"Listing objects in bucket: {bucket_name}")
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                batch_keys = [obj['Key'] for obj in page['Contents']]
                objects.extend(batch_keys)
                
                if len(objects) % 100000 == 0:
                    logger.info(f"Listed {len(objects)} objects so far...")
                    
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        raise
    
    logger.info(f"Total objects found: {len(objects)}")
    return objects

def delete_objects_partition(iterator: Iterator[str]) -> Iterator[int]:
    """Delete objects in a partition using batch delete API"""
    import boto3
    from botocore.exceptions import ClientError
    
    s3_client = boto3.client('s3')
    bucket_name = "your-bucket-name"  # Replace with your bucket
    
    objects_list = list(iterator)
    deleted_count = 0
    
    # Process in batches of 1000 (S3 delete_objects limit)
    for i in range(0, len(objects_list), 1000):
        batch = objects_list[i:i+1000]
        
        try:
            # Prepare delete request
            delete_keys = [{'Key': key} for key in batch]
            
            # Execute batch delete
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': delete_keys,
                    'Quiet': True  # Suppress successful delete responses
                }
            )
            
            # Count successful deletions
            deleted_count += len(batch)
            
            # Log any errors
            if 'Errors' in response:
                for error in response['Errors']:
                    logger.error(f"Delete error: {error}")
            
        except ClientError as e:
            logger.error(f"AWS error deleting batch: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
    
    yield deleted_count

def main():
    """Main function to execute the mass delete operation"""
    
    # Configuration
    BUCKET_NAME = "your-bucket-name"  # Replace with your bucket
    PREFIX = ""  # Optional: specify prefix to delete only certain objects
    NUM_PARTITIONS = 1000  # Adjust based on cluster size
    
    # Validate arguments
    if len(sys.argv) > 1:
        BUCKET_NAME = sys.argv[1]
    if len(sys.argv) > 2:
        PREFIX = sys.argv[2]
    
    logger.info(f"Starting mass delete for bucket: {BUCKET_NAME}")
    logger.info(f"Prefix filter: {PREFIX if PREFIX else 'None (all objects)'}")
    
    # Create Spark session
    spark = create_spark_session()
    sc = spark.sparkContext
    
    try:
        # Get list of objects to delete
        start_time = time.time()
        objects = get_s3_objects(BUCKET_NAME, PREFIX)
        
        if not objects:
            logger.info("No objects found to delete")
            return
        
        list_time = time.time() - start_time
        logger.info(f"Listed {len(objects)} objects in {list_time:.2f} seconds")
        
        # Create RDD with optimal partitioning
        objects_rdd = sc.parallelize(objects, numSlices=NUM_PARTITIONS)
        
        logger.info(f"Created RDD with {objects_rdd.getNumPartitions()} partitions")
        logger.info(f"Objects per partition: {len(objects) // objects_rdd.getNumPartitions()}")
        
        # Execute parallel deletion
        delete_start_time = time.time()
        
        deleted_counts = objects_rdd.mapPartitions(delete_objects_partition).collect()
        
        delete_time = time.time() - delete_start_time
        total_deleted = sum(deleted_counts)
        
        # Print results
        logger.info("="*50)
        logger.info("DELETION SUMMARY")
        logger.info("="*50)
        logger.info(f"Total objects found: {len(objects)}")
        logger.info(f"Total objects deleted: {total_deleted}")
        logger.info(f"Deletion time: {delete_time:.2f} seconds")
        logger.info(f"Deletion rate: {total_deleted / delete_time:.2f} objects/second")
        logger.info(f"Success rate: {(total_deleted / len(objects)) * 100:.2f}%")
        
        if total_deleted < len(objects):
            logger.warning(f"Some objects may not have been deleted. Check logs for errors.")
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()