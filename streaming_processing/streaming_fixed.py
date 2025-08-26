#!/usr/bin/env python3
"""
Fixed Streaming Script - Kafka to MinIO
"""
import os
import sys
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Environment setup
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
BUCKET_NAME = "raw"

def main():
    print("🚀 Starting Fixed Streaming Job...")
    
    try:
        # Create Spark session with correct configs
        spark = SparkSession.builder \
            .appName("FixedStreamingToMinIO") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/streaming_checkpoint") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark session created successfully")
        
        # Read from Kafka
        print("📡 Connecting to Kafka...")
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "streaming-broker:29092") \
            .option("subscribe", "device.iot.taxi_nyc_time_series") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✅ Connected to Kafka successfully")
        
        # Process the streaming data
        processed_df = df.select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("cdc_payload"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("processing_time"),
            date_format(current_timestamp(), "yyyy-MM-dd").alias("date_partition")
        )
        
        print("🔄 Starting streaming to MinIO...")
        
        # Write to MinIO as parquet with partitioning
        query = processed_df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", f"s3a://{BUCKET_NAME}/stream/") \
            .option("checkpointLocation", f"s3a://{BUCKET_NAME}/stream/checkpoint/") \
            .partitionBy("date_partition") \
            .trigger(processingTime="15 seconds") \
            .start()
        
        print("✅ Streaming started successfully!")
        print("⏳ Processing data for 45 seconds...")
        
        # Run for 45 seconds
        query.awaitTermination(45)
        
        print("✅ Streaming job completed successfully")
        
        # Show some stats
        try:
            # Try to read what we just wrote
            df_check = spark.read.parquet(f"s3a://{BUCKET_NAME}/stream/")
            count = df_check.count()
            print(f"📊 Total records written to MinIO: {count}")
        except Exception as e:
            print(f"⚠️ Could not read back data (normal if no data processed): {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Streaming job failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            spark.stop()
            print("🛑 Spark session stopped")
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
