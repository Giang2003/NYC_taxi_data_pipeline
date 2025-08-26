#!/usr/bin/env python3
"""
Complete Batch Processing Test Script
Following the README.md guidelines
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

def check_infrastructure():
    """Check if required infrastructure is running"""
    logger.info("🔍 Checking infrastructure...")
    
    # Check Docker containers
    result = subprocess.run("docker ps", shell=True, capture_output=True, text=True)
    if "test-postgres" in result.stdout and "test-minio" in result.stdout:
        logger.info("✅ Infrastructure is running")
        return True
    else:
        logger.error("❌ Infrastructure not running. Please start with: make batch_up")
        return False

def check_data_files():
    """Check if sample data files exist"""
    logger.info("🔍 Checking data files...")
    
    sample_files = [
        "./data/2022/yellow_tripdata_2022-01.parquet",
        "./data/2022/green_tripdata_2022-01.parquet"
    ]
    
    missing_files = []
    for file_path in sample_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"❌ Missing data files: {missing_files}")
        return False
    
    logger.info("✅ Sample data files found")
    return True

def test_extract_load():
    """Step 1: Extract and Load data to raw bucket"""
    logger.info("=" * 60)
    logger.info("STEP 1: EXTRACT & LOAD TO RAW BUCKET")
    logger.info("=" * 60)
    
    return run_command(
        "python scripts/extract_load.py",
        "Uploading data from local to 'raw' bucket (MinIO)",
        timeout=120
    )

def test_transform_data():
    """Step 2: Transform data from raw to processed bucket"""
    logger.info("=" * 60)
    logger.info("STEP 2: TRANSFORM DATA TO PROCESSED BUCKET")
    logger.info("=" * 60)
    
    return run_command(
        "python scripts/transform_data.py",
        "Processing data from 'raw' to 'processed' bucket",
        timeout=180
    )

def test_create_database_objects():
    """Step 3: Create database schema and tables"""
    logger.info("=" * 60)
    logger.info("STEP 3: CREATE DATABASE SCHEMA & TABLES")
    logger.info("=" * 60)
    
    success1 = run_command(
        "python utils/create_schema.py",
        "Creating database schemas"
    )
    
    success2 = run_command(
        "python utils/create_table.py",
        "Creating staging tables"
    )
    
    return success1 and success2

def test_batch_processing():
    """Step 4: Run batch processing from datalake to data warehouse"""
    logger.info("=" * 60)
    logger.info("STEP 4: BATCH PROCESSING (DATALAKE TO DW)")
    logger.info("=" * 60)
    
    return run_command(
        "python batch_processing/datalake_to_dw.py",
        "Processing data from Datalake to Data Warehouse",
        timeout=900  # Tăng lên 15 phút cho việc xử lý dữ liệu lớn
    )

def verify_results():
    """Step 5: Verify the results in PostgreSQL"""
    logger.info("=" * 60)
    logger.info("STEP 5: VERIFY RESULTS")
    logger.info("=" * 60)
    
    try:
        import psycopg2
        import dotenv
        
        # Load environment
        dotenv.load_dotenv(".env")
        
        conn_params = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': 5432,
            'database': os.getenv('POSTGRES_DB', 'k6'),
            'user': os.getenv('POSTGRES_USER', 'k6'),
            'password': os.getenv('POSTGRES_PASSWORD', 'k6')
        }
        
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        # Check staging table
        cur.execute("SELECT COUNT(*) FROM staging.nyc_taxi;")
        count = cur.fetchone()[0]
        
        logger.info(f"📊 Total records in staging table: {count}")
        
        if count > 0:
            # Show sample data
            cur.execute("SELECT * FROM staging.nyc_taxi LIMIT 3;")
            rows = cur.fetchall()
            
            # Get column names
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'staging' AND table_name = 'nyc_taxi'
                ORDER BY ordinal_position;
            """)
            columns = [row[0] for row in cur.fetchall()]
            
            logger.info("📋 Sample data from staging table:")
            logger.info(f"Columns: {columns}")
            for i, row in enumerate(rows, 1):
                logger.info(f"Row {i}: {row}")
            
            logger.info("✅ Batch processing verification - SUCCESS")
            return True
        else:
            logger.error("❌ No data found in staging table")
            return False
            
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Error verifying results: {e}")
        return False

def main():
    """Main test execution"""
    logger.info("🚀 STARTING COMPLETE BATCH PROCESSING TEST")
    logger.info("Following the README.md guidelines")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    # Pre-checks
    if not check_infrastructure():
        logger.error("💥 Infrastructure check failed. Exiting.")
        return 1
    
    if not check_data_files():
        logger.error("💥 Data files check failed. Exiting.")
        return 1
    
    # Execute batch processing steps
    steps = [
        ("Extract & Load", test_extract_load),
        ("Transform Data", test_transform_data),
        ("Create DB Objects", test_create_database_objects),
        ("Batch Processing", test_batch_processing),
        ("Verify Results", verify_results)
    ]
    
    failed_steps = []
    
    for step_name, step_function in steps:
        logger.info(f"\n🔄 Starting: {step_name}")
        
        if step_function():
            logger.info(f"✅ {step_name} completed successfully")
        else:
            logger.error(f"❌ {step_name} failed")
            failed_steps.append(step_name)
    
    # Summary
    elapsed_time = time.time() - start_time
    logger.info("=" * 80)
    logger.info("📋 BATCH PROCESSING TEST SUMMARY")
    logger.info("=" * 80)
    logger.info(f"⏱️  Total execution time: {elapsed_time:.2f} seconds")
    
    if failed_steps:
        logger.error(f"❌ Failed steps: {', '.join(failed_steps)}")
        logger.error("💥 BATCH PROCESSING TEST - FAILED")
        return 1
    else:
        logger.info("🎉 ALL STEPS COMPLETED SUCCESSFULLY!")
        logger.info("✅ BATCH PROCESSING TEST - SUCCESS")
        return 0

if __name__ == "__main__":
    exit(main())
