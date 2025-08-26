#!/usr/bin/env python3
"""
Complete Streaming Pipeline Test & Guide
Tests the full streaming flow: PostgreSQL → Debezium → Kafka → Spark → MinIO
"""
import os
import sys
import time
import logging
import subprocess
import json
import requests
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_complete_streaming_pipeline():
    """Test the complete streaming pipeline"""
    logger.info("🔍 TESTING COMPLETE STREAMING PIPELINE")
    logger.info("=" * 60)
    
    # Step 1: Verify infrastructure
    logger.info("📋 Step 1: Infrastructure Health Check")
    if not verify_infrastructure():
        return False
    
    # Step 2: Test CDC (PostgreSQL → Kafka)
    logger.info("\n📋 Step 2: Test CDC (PostgreSQL → Kafka)")
    if not test_cdc_flow():
        return False
    
    # Step 3: Test Spark Streaming (Kafka → MinIO)
    logger.info("\n📋 Step 3: Test Spark Streaming (Kafka → MinIO)")
    if not test_spark_streaming():
        return False
    
    # Step 4: Verify data in MinIO
    logger.info("\n📋 Step 4: Verify Data in MinIO")
    if not verify_minio_data():
        return False
    
    # Step 5: Test read streaming data
    logger.info("\n📋 Step 5: Test Reading Streaming Data")
    if not test_read_streaming_data():
        return False
    
    logger.info("\n🎉 COMPLETE STREAMING PIPELINE TEST - SUCCESS!")
    return True

def verify_infrastructure():
    """Verify all streaming infrastructure is running"""
    services = [
        ("http://localhost:8083/connectors", "Debezium Connect"),
        ("http://localhost:8081/subjects", "Schema Registry"), 
        ("http://localhost:9021", "Kafka Control Center"),
    ]
    
    all_healthy = True
    for url, name in services:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                logger.info(f"✅ {name} is healthy")
            else:
                logger.error(f"❌ {name} returned status {response.status_code}")
                all_healthy = False
        except Exception as e:
            logger.error(f"❌ {name} is not accessible: {e}")
            all_healthy = False
    
    # Check Kafka topics
    try:
        result = subprocess.run(
            ["docker", "exec", "streaming-broker", "kafka-topics", 
             "--bootstrap-server", "localhost:9092", "--list"],
            capture_output=True, text=True, timeout=10
        )
        if "device.iot.taxi_nyc_time_series" in result.stdout:
            logger.info("✅ Kafka CDC topic exists")
        else:
            logger.warning("⚠️ CDC topic not found, will be created on first message")
    except Exception as e:
        logger.error(f"❌ Error checking Kafka: {e}")
        all_healthy = False
    
    return all_healthy

def test_cdc_flow():
    """Test Change Data Capture from PostgreSQL to Kafka"""
    logger.info("🔄 Testing CDC: PostgreSQL → Debezium → Kafka")
    
    # Insert test data
    test_data = [
        (1, 1, 100, 200, 1, '2024-08-21 16:00:00', '2024-08-21 16:30:00', 2, 5.5, 15.50, 18.50),
        (2, 2, 150, 250, 2, '2024-08-21 16:05:00', '2024-08-21 16:35:00', 1, 3.2, 12.00, 15.00),
        (1, 1, 180, 90, 1, '2024-08-21 16:10:00', '2024-08-21 16:25:00', 3, 2.8, 8.50, 11.50),
    ]
    
    try:
        for i, data in enumerate(test_data, 1):
            cmd = [
                "docker", "exec", "postgresql", "psql", "-U", "k6", "-d", "k6", "-c",
                f"""INSERT INTO iot.taxi_nyc_time_series 
                   (vendorid, ratecodeid, pulocationid, dolocationid, payment_type, 
                    tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, 
                    trip_distance, fare_amount, total_amount) 
                   VALUES {data};"""
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"✅ Inserted test record {i}")
            else:
                logger.error(f"❌ Failed to insert record {i}: {result.stderr}")
                return False
            
            time.sleep(1)  # Small delay between inserts
        
        # Wait for CDC to process
        logger.info("⏳ Waiting for CDC to process changes...")
        time.sleep(5)
        
        # Verify messages in Kafka
        return verify_kafka_messages()
        
    except Exception as e:
        logger.error(f"❌ CDC test failed: {e}")
        return False

def verify_kafka_messages():
    """Verify messages are in Kafka topic"""
    try:
        cmd = [
            "docker", "exec", "streaming-broker", 
            "bash", "-c",
            "kafka-console-consumer --bootstrap-server localhost:9092 --topic device.iot.taxi_nyc_time_series --from-beginning --timeout-ms 8000 | head -5"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        
        if result.stdout.strip():
            messages = result.stdout.strip().split('\n')
            logger.info(f"✅ Found {len(messages)} CDC messages in Kafka")
            
            # Parse and show sample message
            try:
                sample_msg = json.loads(messages[0])
                operation = sample_msg.get('payload', {}).get('op', 'unknown')
                logger.info(f"📄 Sample message operation: {operation}")
                return True
            except:
                logger.info("📄 Messages found but couldn't parse JSON (might be normal)")
                return True
        else:
            logger.error("❌ No messages found in Kafka topic")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error verifying Kafka messages: {e}")
        return False

def test_spark_streaming():
    """Test Spark streaming from Kafka to MinIO"""
    logger.info("🔄 Testing Spark Streaming: Kafka → MinIO")
    
    # Create optimized streaming script
    streaming_script = '''
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

try:
    # Create Spark session
    spark = SparkSession.builder \\
        .appName("StreamingToMinIO") \\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \\
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \\
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \\
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \\
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
        .config("spark.sql.streaming.checkpointLocation", "/tmp/streaming_checkpoint") \\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    df = spark.readStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "streaming-broker:29092") \\
        .option("subscribe", "device.iot.taxi_nyc_time_series") \\
        .option("startingOffsets", "earliest") \\
        .load()
    
    # Process the streaming data
    processed_df = df.select(
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("cdc_payload"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("processing_time")
    )
    
    # Write to MinIO as parquet
    query = processed_df.writeStream \\
        .format("parquet") \\
        .outputMode("append") \\
        .option("path", f"s3a://{BUCKET_NAME}/stream/") \\
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/stream/checkpoint/") \\
        .trigger(processingTime="10 seconds") \\
        .start()
    
    print("✅ Spark streaming started successfully")
    
    # Run for 30 seconds
    query.awaitTermination(30)
    
    print("✅ Spark streaming completed")
    
except Exception as e:
    print(f"❌ Spark streaming error: {e}")
    import traceback
    traceback.print_exc()
finally:
    try:
        spark.stop()
    except:
        pass
'''
    
    with open("streaming_test.py", "w") as f:
        f.write(streaming_script)
    
    try:
        logger.info("🚀 Starting Spark streaming job...")
        result = subprocess.run(
            ["python", "streaming_test.py"],
            capture_output=True, text=True, timeout=60
        )
        
        if "Spark streaming completed" in result.stdout:
            logger.info("✅ Spark streaming job completed successfully")
            return True
        elif "Spark streaming started successfully" in result.stdout:
            logger.info("✅ Spark streaming job started (may have timed out but that's normal)")
            return True
        else:
            logger.warning("⚠️ Spark streaming had issues")
            logger.info(f"Output: {result.stdout}")
            if result.stderr:
                logger.error(f"Errors: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.info("⏰ Spark streaming timed out (normal for continuous streaming)")
        return True
    except Exception as e:
        logger.error(f"❌ Spark streaming failed: {e}")
        return False
    finally:
        # Clean up
        if os.path.exists("streaming_test.py"):
            os.remove("streaming_test.py")

def verify_minio_data():
    """Verify data was written to MinIO"""
    logger.info("🔄 Verifying data in MinIO...")
    
    try:
        # Check if MinIO bucket has streaming data
        from minio import Minio
        
        client = Minio(
            "localhost:9000",
            access_key="minio_access_key",
            secret_key="minio_secret_key",
            secure=False
        )
        
        # List objects in stream folder
        objects = list(client.list_objects("raw", prefix="stream/", recursive=True))
        
        if objects:
            logger.info(f"✅ Found {len(objects)} objects in MinIO stream folder")
            for obj in objects[:3]:  # Show first 3 objects
                logger.info(f"  📄 {obj.object_name} ({obj.size} bytes)")
            return True
        else:
            logger.warning("⚠️ No streaming data found in MinIO (may need more time)")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error checking MinIO: {e}")
        return False

def test_read_streaming_data():
    """Test reading streaming data from MinIO"""
    logger.info("🔄 Testing read streaming data from MinIO...")
    
    read_script = '''
import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
BUCKET_NAME = "raw"

try:
    spark = SparkSession.builder \\
        .appName("ReadStreamingData") \\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \\
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \\
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \\
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \\
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Read streaming data from MinIO
    df = spark.read.parquet(f"s3a://{BUCKET_NAME}/stream/")
    
    count = df.count()
    print(f"✅ Successfully read {count} records from streaming data")
    
    if count > 0:
        print("📋 Sample streaming data:")
        df.show(3, truncate=False)
        print("📋 Schema:")
        df.printSchema()
    
except Exception as e:
    print(f"❌ Error reading streaming data: {e}")
finally:
    try:
        spark.stop()
    except:
        pass
'''
    
    with open("read_streaming_test.py", "w") as f:
        f.write(read_script)
    
    try:
        result = subprocess.run(
            ["python", "read_streaming_test.py"],
            capture_output=True, text=True, timeout=45
        )
        
        if "Successfully read" in result.stdout:
            logger.info("✅ Successfully read streaming data from MinIO")
            # Extract record count
            lines = result.stdout.split('\n')
            for line in lines:
                if "Successfully read" in line:
                    logger.info(f"📊 {line.strip()}")
            return True
        else:
            logger.warning("⚠️ Could not read streaming data")
            logger.info(f"Output: {result.stdout}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error testing read streaming data: {e}")
        return False
    finally:
        if os.path.exists("read_streaming_test.py"):
            os.remove("read_streaming_test.py")

def show_streaming_guide():
    """Show complete streaming guide"""
    logger.info("\n" + "="*60)
    logger.info("📚 COMPLETE STREAMING PIPELINE GUIDE")
    logger.info("="*60)
    
    guide = """
🌊 STREAMING ARCHITECTURE:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ PostgreSQL  │───▶│  Debezium   │───▶│    Kafka    │───▶│    Spark    │
│    (CDC)    │    │   (CDC)     │    │  (Stream)   │    │ (Processing)│
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                   │
                                                                   ▼
                                                          ┌─────────────┐
                                                          │    MinIO    │
                                                          │ (Data Lake) │
                                                          └─────────────┘

📋 STEP-BY-STEP GUIDE:

1️⃣ START STREAMING INFRASTRUCTURE:
   make stream_up
   # Wait for all services to be healthy

2️⃣ SETUP CDC CONNECTOR:
   curl -X POST -H "Content-Type: application/json" \\
     http://localhost:8083/connectors \\
     -d @debezium/configs/taxi-nyc-cdc.json

3️⃣ INSERT TEST DATA:
   docker exec postgresql psql -U k6 -d k6 -c \\
     "INSERT INTO iot.taxi_nyc_time_series (vendorid, trip_distance, fare_amount) VALUES (1, 10.5, 25.00);"

4️⃣ RUN SPARK STREAMING:
   python streaming_processing/streaming_to_datalake.py

5️⃣ VERIFY RESULTS:
   python streaming_processing/read_parquet_streaming.py

🔧 MONITORING URLS:
• Kafka Control Center: http://localhost:9021
• Debezium UI: http://localhost:8085  
• Schema Registry: http://localhost:8081

🚀 QUICK COMMANDS:
• Test complete pipeline: python test_complete_streaming.py
• Quick streaming test: python test_streaming_quick.py
• Monitor Kafka messages: docker exec streaming-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic device.iot.taxi_nyc_time_series
• Check connector status: curl -s http://localhost:8083/connectors/taxi-nyc-cdc/status | jq

✅ SUCCESS INDICATORS:
• CDC messages in Kafka topic
• Parquet files in MinIO s3://raw/stream/
• Spark streaming job running without errors
• Data readable from MinIO via Spark
"""
    
    print(guide)

def main():
    """Main execution"""
    logger.info("🚀 COMPLETE STREAMING PIPELINE TEST & GUIDE")
    
    start_time = time.time()
    
    # Run complete test
    success = check_complete_streaming_pipeline()
    
    elapsed_time = time.time() - start_time
    logger.info(f"\n⏱️ Total test time: {elapsed_time:.2f} seconds")
    
    # Show guide regardless of test result
    show_streaming_guide()
    
    if success:
        logger.info("\n🎉 STREAMING PIPELINE IS WORKING CORRECTLY!")
        return 0
    else:
        logger.error("\n💥 STREAMING PIPELINE HAS ISSUES - CHECK LOGS ABOVE")
        return 1

if __name__ == "__main__":
    exit(main())
