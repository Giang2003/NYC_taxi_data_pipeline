#!/usr/bin/env python3
"""
Test script for batch processing pipeline
"""
import os
import sys
import time
import logging
from pathlib import Path

# Add utils to path
utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(utils_path)

from minio_utils import MinIOClient
from helpers import load_cfg
import dotenv

# Load environment variables
dotenv.load_dotenv(".env")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_test_data():
    """Upload test parquet files to MinIO"""
    logger.info("Setting up test data in MinIO...")
    
    # Load config
    cfg = load_cfg("./config/datalake.yaml")
    datalake_cfg = cfg["datalake"]
    
    # Create MinIO client
    client = MinIOClient(
        endpoint_url=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"]
    )
    
    # Create bucket
    bucket_name = datalake_cfg['bucket_name_2']  # processed bucket
    client.create_bucket(bucket_name)
    
    # Upload a few test files
    test_files = [
        "./data/2022/yellow_tripdata_2022-01.parquet",
        "./data/2022/green_tripdata_2022-01.parquet"
    ]
    
    minio_client = client.create_conn()
    
    for file_path in test_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            object_name = f"batch/{file_name}"
            
            logger.info(f"Uploading {file_path} to {bucket_name}/{object_name}")
            minio_client.fput_object(bucket_name, object_name, file_path)
            logger.info(f"Successfully uploaded {file_name}")
        else:
            logger.warning(f"File not found: {file_path}")
    
    # List uploaded files
    logger.info("Files in MinIO bucket:")
    for file in client.list_parquet_files(bucket_name, prefix='batch/'):
        logger.info(f"  - {file}")

def check_postgres_data():
    """Check if data was loaded to PostgreSQL"""
    import psycopg2
    
    logger.info("Checking PostgreSQL data...")
    
    # Connection parameters
    conn_params = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': 5432,
        'database': os.getenv('POSTGRES_DB', 'k6'),
        'user': os.getenv('POSTGRES_USER', 'k6'),
        'password': os.getenv('POSTGRES_PASSWORD', 'k6')
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Check if staging table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'staging' 
                AND table_name = 'nyc_taxi'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            # Count records in staging table
            cur.execute("SELECT COUNT(*) FROM staging.nyc_taxi;")
            count = cur.fetchone()[0]
            logger.info(f"Total records in staging table: {count}")
            
            if count > 0:
                # Show sample data
                cur.execute("SELECT * FROM staging.nyc_taxi LIMIT 5;")
                rows = cur.fetchall()
                
                # Get column names
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'staging' AND table_name = 'nyc_taxi'
                    ORDER BY ordinal_position;
                """)
                columns = [row[0] for row in cur.fetchall()]
                
                logger.info("Sample data from staging table:")
                logger.info(f"Columns: {columns}")
                for row in rows:
                    logger.info(f"  {row}")
            else:
                logger.warning("No data found in staging table")
        else:
            logger.error("Staging table 'staging.nyc_taxi' does not exist")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")

def run_batch_processing():
    """Run the batch processing script"""
    logger.info("Running batch processing script...")
    
    # Run the batch processing script
    import subprocess
    
    try:
        result = subprocess.run(
            [sys.executable, "batch_processing/datalake_to_dw.py"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        if result.returncode == 0:
            logger.info("Batch processing completed successfully!")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Batch processing failed with return code {result.returncode}")
            logger.error(f"Error output: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("Batch processing timed out after 5 minutes")
    except Exception as e:
        logger.error(f"Error running batch processing: {e}")

def main():
    """Main test function"""
    logger.info("Starting batch processing test...")
    
    try:
        # Step 1: Setup test data in MinIO
        setup_test_data()
        
        # Step 2: Run batch processing
        run_batch_processing()
        
        # Step 3: Check results in PostgreSQL
        time.sleep(2)  # Wait a bit for data to be written
        check_postgres_data()
        
        logger.info("Batch processing test completed!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
