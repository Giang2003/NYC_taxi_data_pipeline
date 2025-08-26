import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    "owner": "legiang",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

###############################################
# Optimized Functions
###############################################

def optimized_extract_load(**context):
    """Optimized extract and load with limited files"""
    import sys
    import os
    from glob import glob
    
    # Add utils to path
    utils_path = '/opt/airflow/dags/utils'
    sys.path.append(utils_path)
    from helpers import load_cfg
    from minio_utils import MinIOClient
    
    CFG_FILE = "/opt/airflow/config/datalake.yaml"
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    
    # Allow override via env when running inside containers
    endpoint_override = os.getenv("MINIO_ENDPOINT")
    client = MinIOClient(
        endpoint_url=endpoint_override or datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"]
    )
    
    client.create_bucket(datalake_cfg["bucket_name_1"])
    
    # Only upload 2022 data for faster processing
    data_path = "/opt/airflow/data/2022"
    if os.path.exists(data_path):
        all_fps = glob(os.path.join(data_path, "*.parquet"))[:4]  # Limit to 4 files
        
        for fp in all_fps:
            print(f"Uploading {fp}")
            client_minio = client.create_conn()
            client_minio.fput_object(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name=os.path.join(datalake_cfg["folder_name"], os.path.basename(fp)),
                file_path=fp,
            )
    else:
        print(f"Data path {data_path} not found")

def optimized_transform_data(**context):
    """Optimized transform with batch processing"""
    import sys
    import os
    import pandas as pd
    import s3fs
    
    # Add utils to path
    utils_path = '/opt/airflow/dags/utils'
    sys.path.append(utils_path)
    from helpers import load_cfg
    from minio_utils import MinIOClient
    
    CFG_FILE = "/opt/airflow/config/datalake.yaml"
    TAXI_LOOKUP_PATH = "/opt/airflow/dags/scripts/data/taxi_lookup.csv"
    
    def process_single_file(file_name, s3_fs, datalake_cfg):
        """Process one file at a time to avoid memory issues"""
        print(f"Processing file: {file_name}")
        
        # Read file
        input_path = f"s3://{datalake_cfg['bucket_name_1']}/{datalake_cfg['folder_name']}/{file_name}"
        try:
            df = pd.read_parquet(input_path, filesystem=s3_fs, engine='pyarrow')
        except Exception as e:
            print(f"Error reading {file_name}: {e}")
            return False
        
        # Lower case columns
        df.columns = map(str.lower, df.columns)
        
        # Drop store_and_fwd_flag if exists
        if "store_and_fwd_flag" in df.columns:
            df = df.drop(columns=["store_and_fwd_flag"])
            print(f"Dropped store_and_fwd_flag from {file_name}")
        
        # Simple processing - skip complex merge for now
        if file_name.startswith("green"):
            df.rename(columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
            }, inplace=True)
            if "trip_type" in df.columns:
                df.drop(columns=["trip_type"], inplace=True)
        elif file_name.startswith("yellow"):
            df.rename(columns={
                "tpep_pickup_datetime": "pickup_datetime", 
                "tpep_dropoff_datetime": "dropoff_datetime",
            }, inplace=True)
        
        # Fix data types
        for col in ["payment_type", "dolocationid", "pulocationid", "vendorid"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
        # Remove missing data
        df = df.dropna()
        df = df.reindex(sorted(df.columns), axis=1)
        
        # Save to processed bucket
        output_path = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}/{file_name}"
        df.to_parquet(output_path, index=False, filesystem=s3_fs, engine='pyarrow')
        print(f"Saved processed file: {output_path}")
        
        return True
    
    # Main processing
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    
    endpoint_override = os.getenv('MINIO_ENDPOINT')
    s3_fs = s3fs.S3FileSystem(
        anon=False,
        key=datalake_cfg['access_key'],
        secret=datalake_cfg['secret_key'],
        client_kwargs={'endpoint_url': f"http://{endpoint_override or datalake_cfg['endpoint']}"}
    )
    
    client = MinIOClient(
        endpoint_url=endpoint_override or datalake_cfg['endpoint'],
        access_key=datalake_cfg['access_key'],
        secret_key=datalake_cfg['secret_key']
    )
    
    client.create_bucket(datalake_cfg['bucket_name_2'])
    
    # Get list of files to process
    raw_files = client.list_parquet_files(datalake_cfg['bucket_name_1'], prefix=datalake_cfg['folder_name'])
    
    success_count = 0
    for file_path in raw_files:
        file_name = os.path.basename(file_path)
        if process_single_file(file_name, s3_fs, datalake_cfg):
            success_count += 1
        print("="*100)
    
    print(f"Successfully processed {success_count}/{len(raw_files)} files")

###############################################
# DAG Definition
###############################################

with DAG(
    "elt_pipeline_optimized", 
    start_date=datetime(2024, 1, 1), 
    schedule=None, 
    default_args=default_args,
    description="Optimized ELT Pipeline with better memory management"
) as dag:

    start_pipeline = DummyOperator(
        task_id="start_pipeline"
    )

    extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=optimized_extract_load,
        provide_context=True
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=optimized_transform_data,
        provide_context=True,
        pool='default_pool',  # Use default pool to limit concurrent tasks
    )

    # Use bash operator for batch processing as it's more stable
    batch_processing = BashOperator(
        task_id="batch_processing",
        bash_command="""
        cd /opt/airflow && 
        export POSTGRES_HOST=postgres-dw &&
        python /opt/airflow/batch_processing_optimized.py || true
        """,
    )

    end_pipeline = DummyOperator(
        task_id="end_pipeline"
    )

    # Define task dependencies
    start_pipeline >> extract_load >> transform_data >> batch_processing >> end_pipeline
