import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
dotenv.load_dotenv(".env")

from pyspark import SparkContext

base_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
utils_path = os.path.join(base_dir, '..', 'utils')
sys.path.append(utils_path)
from helpers import load_cfg
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
warnings.filterwarnings('ignore')

###############################################
# Parameters & Arguments
###############################################
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
DB_STAGING_TABLE = os.getenv("DB_STAGING_TABLE")

CFG_FILE = os.path.join(base_dir, "config", "datalake.yaml")
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME = datalake_cfg['bucket_name_2']

CFG_FILE_SPARK = os.path.join(base_dir, "config", "spark.yaml")
cfg = load_cfg(CFG_FILE_SPARK)
spark_cfg = cfg["spark_config"]

# Safe memory retrieval or validation
MEMORY = spark_cfg.get('executor_memory', '4g')
if 'executor_memory' not in spark_cfg:
    logging.warning("spark_config.executor_memory missing; defaulting to '4g'")
###############################################

# Validate required environment variables early and fail fast
required_env_vars = [
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "POSTGRES_HOST",
    "DB_STAGING_TABLE",
]
missing_env = [k for k in required_env_vars if not os.getenv(k)]
if missing_env:
    raise ValueError(f"Missing required env vars: {missing_env}")


###############################################
# PySpark
###############################################
def create_spark_session():
    """
        Create the Spark Session with suitable configs
    """
    from pyspark.sql import SparkSession

    spark = None
    try: 
        spark = (SparkSession.builder.config("spark.executor.memory", MEMORY) \
                        .config(
                            "spark.jars", 
                            "jars/postgresql-42.4.3.jar,jars/aws-java-sdk-bundle-1.12.262.jar,jars/hadoop-aws-3.3.4.jar",
                        )
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                        .appName("Batch Processing Application")
                        .getOrCreate()
        )
        
        logging.info('Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        raise

    return spark


def load_minio_config(spark_context: SparkContext):
    """
        Establish the necessary configurations to access to MinIO
    """
    try:
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
        use_ssl = MINIO_ENDPOINT.strip().lower().startswith('https://')
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true" if use_ssl else "false")
        # Optionally set region if available in config
        region = datalake_cfg.get('region')
        if region:
            spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint.region", region)
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO config could not be created successfully due to exception: {e}")


def processing_dataframe(df, file_path):
    """
        Process data before loading to staging area
    """
    from pyspark.sql import functions as F 

    # Normalize columns to lowercase first for robust checks
    df = df.select([F.col(c).alias(c.lower()) for c in df.columns])
    cols_set = set(df.columns)
    if {'tpep_pickup_datetime','tpep_dropoff_datetime'}.issubset(cols_set):
        df = df.withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
               .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')
    elif {'lpep_pickup_datetime','lpep_dropoff_datetime'}.issubset(cols_set):
        df = df.withColumnRenamed('lpep_pickup_datetime','pickup_datetime') \
               .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')
    elif not {'pickup_datetime','dropoff_datetime'}.issubset(cols_set):
        raise ValueError(f"Unknown datetime columns in file: {file_path}")

    df = df.withColumn('pickup_datetime', F.col('pickup_datetime').cast('timestamp')) \
           .withColumn('dropoff_datetime', F.col('dropoff_datetime').cast('timestamp'))

    df2 = df.withColumn('year', F.year('pickup_datetime')) \
            .withColumn('month', F.date_format('pickup_datetime', 'MMMM')) \
            .withColumn('dow', F.date_format('pickup_datetime', 'EEEE'))

    # Normalize columns to lowercase
    df2 = df2.select([F.col(c).alias(c.lower()) for c in df2.columns])

    # Rename columns prior to grouping (only when present)
    present = set(df2.columns)
    rename_map = {
        'vendorid': 'vendor_id',
        'ratecodeid': 'rate_code_id',
        'pulocationid': 'pickup_location_id',
        'dolocationid': 'dropoff_location_id',
        'payment_type': 'payment_type_id',
    }
    for src, dst in rename_map.items():
        if src in present:
            df2 = df2.withColumnRenamed(src, dst)

    group_by_cols = ['year', 'month', 'dow']
    for c in ['vendor_id', 'rate_code_id', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id']:
        if c in df2.columns:
            group_by_cols.append(c)

    # Conditionally add geo columns if they exist
    lower_cols = [c.lower() for c in df2.columns]
    for geo_col in ['pickup_latitude','pickup_longitude','dropoff_latitude','dropoff_longitude']:
        if geo_col in lower_cols:
            group_by_cols.append(geo_col)

    agg_cols_present = [
        ('passenger_count', 'passenger_count'),
        ('trip_distance', 'trip_distance'),
        ('extra', 'extra'),
        ('mta_tax', 'mta_tax'),
        ('fare_amount', 'fare_amount'),
        ('tip_amount', 'tip_amount'),
        ('tolls_amount', 'tolls_amount'),
        ('total_amount', 'total_amount'),
        ('improvement_surcharge', 'improvement_surcharge'),
        ('congestion_surcharge', 'congestion_surcharge')
    ]

    agg_exprs = []
    for src_col, alias_col in agg_cols_present:
        if src_col in lower_cols:
            agg_exprs.append(F.sum(src_col).alias(alias_col))
        else:
            agg_exprs.append(F.sum(F.lit(0)).alias(alias_col))

    df_final = df2.groupBy(*group_by_cols).agg(*agg_exprs)

    # add 'service_type' column with default 0
    if 'yellow' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(1))
    elif 'green' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(2))
    else:
        df_final = df_final.withColumn('service_type', F.lit(0))

    return df_final


def load_to_staging_table(df):
    """
        Save data after processing to Staging Area (PostgreSQL)
    """
    URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

    # write data to PostgreSQL with explicit JDBC options
    (df.coalesce(4)
       .write
       .format("jdbc")
       .option("url", URL)
       .option("dbtable", DB_STAGING_TABLE)
       .option("user", POSTGRES_USER)
       .option("password", POSTGRES_PASSWORD)
       .option("driver", "org.postgresql.Driver")
       .option("batchsize", 10000)
       .mode('append')
       .save())
    # df.write.jdbc(url=URL, table= 'staging.nyc_taxi_test', mode='append', properties=properties)
###############################################


###############################################
# Main
###############################################
if __name__ == "__main__":
    start_time = time.time()

    spark = create_spark_session()
    if spark is None:
        sys.exit(1)
    load_minio_config(spark.sparkContext)

    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    for file in client.list_parquet_files(BUCKET_NAME, prefix='batch/'):
        try:
            path = f"s3a://{BUCKET_NAME}/" + file
            logging.info(f"Reading parquet file: {file}")

            df = spark.read.parquet(path)
            df_final = processing_dataframe(df, file)
            
            # load data to staging table in PostgreSQL
            load_to_staging_table(df_final)
            logging.info("="*100)
        except Exception as e:
            logging.error(f"Error processing file {file}: {e}")
            continue

    logging.info(f"Time to process: {time.time() - start_time}")
    logging.info("Batch processing successfully!")
    spark.stop()
###############################################
