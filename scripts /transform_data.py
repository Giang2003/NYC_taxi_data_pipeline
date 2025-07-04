import sys
import os
import pandas as pd
from glob import glob
from minio import Minio
import time

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient
import s3fs

DATA_PATH = "data/"
YEARS = ["2021", "2022", "2023", "2024"]
CFG_FILE =  "config/datalake.yaml"
def drop_column(df, file):
    """
        Drop columns 'store_and_fwd_flag'
    """
    if "store_and_fwd_flag" in df.columns:
        df = df.drop(columns=["store_and_fwd_flag"])
        print("Dropped column store_and_fwd_flag from file: " + file)
    else:
        print("Column store_and_fwd_flag not found in file: " + file)
    return df

def process(df, file):
    """
    Green:
        Rename column: lpep_pickup_datetime, lpep_dropoff_datetime, ehail_fee
        Drop: trip_type
    Yellow:
        Rename column: tpep_pickup_datetime, tpep_dropoff_datetime, airport_fee
    """
    
    if file.startswith("green"):
        # rename columns
        df.rename(
            columns={
                "lpep_pickup_datetime": "pickup_datetime",
                "lpep_dropoff_datetime": "dropoff_datetime",
                "ehail_fee": "fee"
            },
            inplace=True
        )

        # drop column
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)

    elif file.startswith("yellow"):
        # rename columns
        df.rename(
            columns={
                "tpep_pickup_datetime": "pickup_datetime",
                "tpep_dropoff_datetime": "dropoff_datetime",
                "airport_fee": "fee"
            },
            inplace=True
        )

    # fix data type in columns 'payment_type', 'dolocationid', 'pulocationid', 'vendorid'
    # if "payment_type" in df.columns:
    #     df["payment_type"] = df["payment_type"].astype(int)
    # if "dolocationid" in df.columns:
    #     df["dolocationid"] = df["dolocationid"].astype(int)
    # if "pulocationid" in df.columns:
    #     df["pulocationid"] = df["pulocationid"].astype(int)
    # if "vendorid" in df.columns:
    #     df["vendorid"] = df["vendorid"].astype(int)
    for col_name in ["payment_type", "dolocationid", "pulocationid", "vendorid"]:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype(int)

    # drop column 'fee'
    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)
                
    # Remove missing data
    df = df.dropna()
    df = df.reindex(sorted(df.columns), axis=1)
    
    print("Transformed data from file: " + file)

    return df

def transform_data(endpoint_url, access_key, secret_key):
    """
        Transform data after loading into Datalake (MinIO)
    """

    # Load minio config
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    bucket_raw = datalake_cfg["bucket_name_1"]
    bucket_processed = datalake_cfg["bucket_name_2"]
    folder = datalake_cfg["folder_name"]

    s3_fs = s3fs.S3FileSystem(
        anon=False,
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': "".join(["http://", endpoint_url])}
    )

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    # Create bucket 'processed'
    client.create_bucket(bucket_processed)
    objects = client.client.list_objects(bucket_raw, prefix=folder + "/", recursive=True)
    for obj in objects:
        if not obj.object_name.endswith(".parquet"):
            continue

        file_name = obj.object_name.split("/")[-1]
        input_path = f"s3://{bucket_raw}/{obj.object_name}"
        output_path = f"s3://{bucket_processed}/{folder}/{file_name}"

        try:
            with s3_fs.open(input_path, 'rb') as f:
                df = pd.read_parquet(f, engine="pyarrow")
        except Exception as e:
            print(f"❌ Error reading {input_path}: {e}")
            continue

        df.columns = map(str.lower, df.columns)
        df = drop_column(df, file_name)
        df = process(df, file_name)

        df.to_parquet(output_path, index=False, filesystem=s3_fs, engine='pyarrow')
        print(f"✅ Wrote processed file to: {output_path}")
        print("=" * 100)


    # Transform data
    # for year in YEARS:
    #     all_fps = glob(os.path.join(DATA_PATH, year, "*.parquet"))

    #     for file in all_fps:
    #         file_name = file.split('/')[-1]
    #         print(f"Reading parquet file: {file_name}")
            
    #         df = pd.read_parquet(file, engine='pyarrow')

    #         # lower case all columns
    #         df.columns = map(str.lower, df.columns)

    #         df = drop_column(df, file_name)
    #         df = process(df, file_name)

    #         # save to parquet file
    #         path = f"s3://{datalake_cfg['bucket_name_2']}/{datalake_cfg['folder_name']}/" + file_name
    #         df.to_parquet(path, index=False, filesystem=s3_fs, engine='pyarrow')
    #         print("Finished transforming data in file: " + path)
    #         print("="*100)
if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    ENDPOINT_URL_LOCAL = datalake_cfg['endpoint']
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']

    transform_data(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL)