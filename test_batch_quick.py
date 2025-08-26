#!/usr/bin/env python3
"""
Quick Batch Processing Test Script - Only 2022 data for faster testing
"""
import os
import sys
import time
import logging
import subprocess
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command, description, timeout=300):
    """Run a command and log the results"""
    logger.info(f"🚀 {description}")
    logger.info(f"Running: {command}")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {description} - SUCCESS")
            if result.stdout.strip():
                logger.info(f"Output: {result.stdout}")
            return True
        else:
            logger.error(f"❌ {description} - FAILED")
            logger.error(f"Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ {description} - TIMEOUT after {timeout} seconds")
        return False
    except Exception as e:
        logger.error(f"💥 {description} - EXCEPTION: {e}")
        return False

def clear_previous_data():
    """Clear previous test data"""
    logger.info("🧹 Clearing previous test data...")
    
    # Clear MinIO buckets
    commands = [
        "docker exec test-minio mc rm --recursive --force minio/raw/ || true",
        "docker exec test-minio mc rm --recursive --force minio/processed/ || true",
        "docker exec test-postgres psql -U k6 -d k6 -c 'TRUNCATE TABLE staging.nyc_taxi;' || true"
    ]
    
    for cmd in commands:
        subprocess.run(cmd, shell=True, capture_output=True)
    
    logger.info("✅ Previous data cleared")

def test_quick_extract_load():
    """Quick extract and load - only 2 files"""
    logger.info("=" * 60)
    logger.info("QUICK TEST: EXTRACT & LOAD (2 FILES ONLY)")
    logger.info("=" * 60)
    
    # Create a temporary script for limited data
    quick_script = """
import sys
import os
from glob import glob
from minio import Minio

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

CFG_FILE = "./config/datalake.yaml"
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

client = MinIOClient(
    endpoint_url=datalake_cfg["endpoint"],
    access_key=datalake_cfg["access_key"],
    secret_key=datalake_cfg["secret_key"]
)

client.create_bucket(datalake_cfg["bucket_name_1"])

# Only upload 2 files for quick test
test_files = [
    "./data/2022/yellow_tripdata_2022-01.parquet",
    "./data/2022/green_tripdata_2022-01.parquet"
]

for fp in test_files:
    if os.path.exists(fp):
        print(f"Uploading {fp}")
        client_minio = client.create_conn()
        client_minio.fput_object(
            bucket_name=datalake_cfg["bucket_name_1"],
            object_name=os.path.join(datalake_cfg["folder_name"], os.path.basename(fp)),
            file_path=fp,
        )
    else:
        print(f"File not found: {fp}")
"""
    
    with open("quick_extract_load.py", "w") as f:
        f.write(quick_script)
    
    result = run_command(
        "python quick_extract_load.py",
        "Quick upload of 2 test files",
        timeout=60
    )
    
    # Clean up
    os.remove("quick_extract_load.py")
    return result

def test_quick_transform():
    """Quick transform - only process uploaded files"""
    logger.info("=" * 60)
    logger.info("QUICK TEST: TRANSFORM DATA")
    logger.info("=" * 60)
    
    # Create a temporary transform script for only 2022 data
    quick_script = """
import sys
import os
import pandas as pd
from glob import glob
from minio import Minio
import time
import s3fs

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

DATA_PATH = "data/2022/"  # Only 2022 data
TAXI_LOOKUP_PATH = os.path.join("scripts", "data", "taxi_lookup.csv")
CFG_FILE = "config/datalake.yaml"

def drop_column(df, file):
    if "store_and_fwd_flag" in df.columns:
        df = df.drop(columns=["store_and_fwd_flag"])
        print("Dropped column store_and_fwd_flag from file: " + file)
    return df

def merge_taxi_zone(df, file):
    df_lookup = pd.read_csv(TAXI_LOOKUP_PATH)
    
    def merge_and_rename(df, location_id, lat_col, long_col):
        df = df.merge(df_lookup, left_on=location_id, right_on="LocationID")
        df = df.drop(columns=["LocationID", "Borough", "service_zone", "zone"])
        df = df.rename(columns={
            "latitude" : lat_col,
            "longitude" : long_col
        })
        return df

    if "pickup_latitude" not in df.columns:
        df = merge_and_rename(df, "pulocationid", "pickup_latitude", "pickup_longitude")
        
    if "dropoff_latitude" not in df.columns:
        df = merge_and_rename(df, "dolocationid", "dropoff_latitude", "dropoff_longitude")

    df = df.drop(columns=[col for col in df.columns if "Unnamed" in col], errors='ignore').dropna()
    print("Merged data from file: " + file)
    return df

def process(df, file):
    if file.startswith("green"):
        df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "ehail_fee": "fee"
            },
            inplace=True
        )
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)
    elif file.startswith("yellow"):
        df.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "airport_fee": "fee"
            },
            inplace=True
        )

    # Fix data types
    for col in ["payment_type", "dolocationid", "pulocationid", "vendorid"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)
                
    df = df.dropna()
    df = df.reindex(sorted(df.columns), axis=1)
    print("Transformed data from file: " + file)
    return df

# Load config
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

s3_fs = s3fs.S3FileSystem(
    anon=False,
    key=datalake_cfg['access_key'],
    secret=datalake_cfg['secret_key'],
    client_kwargs={'endpoint_url': "".join(["http://", datalake_cfg['endpoint']])}
)

client = MinIOClient(
    endpoint_url=datalake_cfg['endpoint'],
    access_key=datalake_cfg['access_key'],
    secret_key=datalake_cfg['secret_key']
)

client.create_bucket(datalake_cfg['bucket_name_2'])

# Process only 2 test files
test_files = [
    "data/2022/yellow_tripdata_2022-01.parquet",
    "data/2022/green_tripdata_2022-01.parquet"
]

for file in test_files:
    if os.path.exists(file):
        file_name = file.split('/')[-1]
        print(f"Reading parquet file: {file_name}")
        
        df = pd.read_parquet(file, engine='pyarrow')
        df.columns = map(str.lower, df.columns)

        df = drop_column(df, file_name)
        df = merge_taxi_zone(df, file_name)
        df = process(df, file_name)

        path = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}/" + file_name
        df.to_parquet(path, index=False, filesystem=s3_fs, engine='pyarrow')
        print("Finished transforming data in file: " + path)
        print("="*100)
"""
    
    with open("quick_transform.py", "w") as f:
        f.write(quick_script)
    
    result = run_command(
        "python quick_transform.py",
        "Quick transform of 2 test files",
        timeout=120
    )
    
    # Clean up
    os.remove("quick_transform.py")
    return result

def main():
    """Main quick test execution"""
    logger.info("🚀 STARTING QUICK BATCH PROCESSING TEST")
    logger.info("Testing with only 2 files for faster execution")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    # Clear previous data first
    clear_previous_data()
    
    # Execute quick test steps
    steps = [
        ("Quick Extract & Load", test_quick_extract_load),
        ("Quick Transform", test_quick_transform),
        ("Create DB Objects", lambda: run_command("python utils/create_schema.py && python utils/create_table.py", "Creating database objects")),
        ("Quick Batch Processing", lambda: run_command("python batch_processing/datalake_to_dw.py", "Quick batch processing", timeout=180)),
    ]
    
    failed_steps = []
    
    for step_name, step_function in steps:
        logger.info(f"\n🔄 Starting: {step_name}")
        
        if step_function():
            logger.info(f"✅ {step_name} completed successfully")
        else:
            logger.error(f"❌ {step_name} failed")
            failed_steps.append(step_name)
    
    # Quick verification
    try:
        import psycopg2
        import dotenv
        dotenv.load_dotenv(".env")
        
        conn = psycopg2.connect(
            host='localhost', port=5432, database='k6', user='k6', password='k6'
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM staging.nyc_taxi;")
        count = cur.fetchone()[0]
        logger.info(f"📊 Records in staging table: {count}")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error checking results: {e}")
    
    # Summary
    elapsed_time = time.time() - start_time
    logger.info("=" * 80)
    logger.info("📋 QUICK TEST SUMMARY")
    logger.info("=" * 80)
    logger.info(f"⏱️  Total execution time: {elapsed_time:.2f} seconds")
    
    if failed_steps:
        logger.error(f"❌ Failed steps: {', '.join(failed_steps)}")
        return 1
    else:
        logger.info("🎉 QUICK TEST COMPLETED SUCCESSFULLY!")
        return 0

if __name__ == "__main__":
    exit(main())
