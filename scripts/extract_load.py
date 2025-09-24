import sys
import os
from glob import glob
import time
import logging
from minio.error import S3Error

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

###############################################
# Parameters & Arguments
###############################################
CFG_FILE = "./config/datalake.yaml"
YEARS = ["2021", "2022", "2023"]
###############################################


###############################################
# Main
###############################################
def extract_load(endpoint_url, access_key, secret_key, cfg=None):
    """Upload local parquet files to MinIO raw bucket.

    - Reuses a single MinIO connection
    - Skips upload if object already exists (idempotent)
    - Retries transient failures with backoff
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if cfg is None:
        cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]
    nyc_data_cfg = cfg["nyc_data"]

    client = MinIOClient(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

    client.create_bucket(datalake_cfg["bucket_name_1"])

    # Create one reusable MinIO connection
    client_minio = client.create_conn()

    def upload_with_retry(bucket_name, object_name, file_path, max_retries=3, base_sleep=1.5):
        # Idempotency: skip if object exists
        try:
            client_minio.stat_object(bucket_name, object_name)
            logging.info(f"Skip existing object: s3://{bucket_name}/{object_name}")
            return True
        except S3Error as e:
            if e.code != "NoSuchKey":
                logging.warning(f"stat_object warning on {object_name}: {e}")
                # continue to attempt upload

        attempt = 0
        while True:
            try:
                client_minio.fput_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    file_path=file_path,
                )
                logging.info(f"Uploaded: {file_path} → s3://{bucket_name}/{object_name}")
                return True
            except S3Error as e:
                attempt += 1
                logging.error(f"Upload failed (attempt {attempt}/{max_retries}) for {file_path}: {e}")
                if attempt >= max_retries:
                    return False
                sleep_s = base_sleep * attempt
                time.sleep(sleep_s)

    for year in YEARS:
        all_fps = glob(os.path.join(nyc_data_cfg["folder_path"], year, "*.parquet"))
        for fp in all_fps:
            object_name = os.path.join(datalake_cfg["folder_name"], os.path.basename(fp))
            logging.info(f"Uploading {fp} → s3://{datalake_cfg['bucket_name_1']}/{object_name}")
            ok = upload_with_retry(
                bucket_name=datalake_cfg["bucket_name_1"],
                object_name=object_name,
                file_path=fp,
            )
            if not ok:
                logging.error(f"Giving up after retries: {fp}")
###############################################


if __name__ == "__main__":
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["datalake"]

    ENDPOINT_URL_LOCAL = datalake_cfg['endpoint']
    ACCESS_KEY_LOCAL = datalake_cfg['access_key']
    SECRET_KEY_LOCAL = datalake_cfg['secret_key']

    extract_load(ENDPOINT_URL_LOCAL, ACCESS_KEY_LOCAL, SECRET_KEY_LOCAL, cfg=cfg)