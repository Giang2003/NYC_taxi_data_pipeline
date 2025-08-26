import sys
import os
import warnings
import traceback
import logging
import time
import dotenv
dotenv.load_dotenv(".env")

from pyspark import SparkConf, SparkContext

utils_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils'))
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

CFG_FILE = "./config/datalake.yaml"
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["datalake"]

MINIO_ENDPOINT = datalake_cfg["endpoint"]
MINIO_ACCESS_KEY = datalake_cfg["access_key"]
MINIO_SECRET_KEY = datalake_cfg["secret_key"]
BUCKET_NAME = datalake_cfg['bucket_name_2']

CFG_FILE_SPARK = "./config/spark.yaml"
cfg = load_cfg(CFG_FILE_SPARK)
spark_cfg = cfg["spark_config"]

MEMORY = "4g"  # Giảm memory để khởi động nhanh hơn
###############################################


###############################################
# PySpark - Optimized
###############################################
def create_spark_session():
    """
        Create the Spark Session with optimized configs for faster startup
    """
    from pyspark.sql import SparkSession

    try: 
        spark = (SparkSession.builder
                        .appName("Optimized Batch Processing")
                        .config("spark.executor.memory", MEMORY)
                        .config("spark.executor.cores", "2")  # Giảm cores để ít resource
                        .config("spark.sql.adaptive.enabled", "true")  # Adaptive query execution
                        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                        .config("spark.jars", 
                                "jars/postgresql-42.4.3.jar,jars/aws-java-sdk-bundle-1.12.262.jar,jars/hadoop-aws-3.3.4.jar")
                        .getOrCreate()
        )
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        logging.info('Optimized Spark session successfully created!')

    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Couldn't create the spark session due to exception: {e}")

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
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        logging.info('MinIO configuration is created successfully')
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"MinIO config could not be created successfully due to exception: {e}")


def processing_dataframe(df, file_path):
    """
        Process data before loading to staging area - Optimized version
    """
    from pyspark.sql import functions as F 

    # Handle different column names for pickup/dropoff datetime
    pickup_col = None
    dropoff_col = None
    
    columns = df.columns
    if 'pickup_datetime' in columns:
        pickup_col = 'pickup_datetime'
        dropoff_col = 'dropoff_datetime'
    elif 'tpep_pickup_datetime' in columns:
        pickup_col = 'tpep_pickup_datetime'
        dropoff_col = 'tpep_dropoff_datetime'
    elif 'lpep_pickup_datetime' in columns:
        pickup_col = 'lpep_pickup_datetime'
        dropoff_col = 'lpep_dropoff_datetime'
    else:
        raise ValueError(f"Unknown datetime column format in file: {file_path}")
    
    # Rename columns to standard names for consistency
    if pickup_col != 'pickup_datetime':
        df = df.withColumnRenamed(pickup_col, 'pickup_datetime') \
               .withColumnRenamed(dropoff_col, 'dropoff_datetime')

    # Add time-based columns
    df2 = df.withColumn('year', F.year('pickup_datetime')) \
            .withColumn('month', F.date_format('pickup_datetime', 'MMMM')) \
            .withColumn('dow', F.date_format('pickup_datetime', 'EEEE'))

    # Handle column name variations - make them lowercase for consistency
    df2 = df2.select([F.col(c).alias(c.lower()) for c in df2.columns])

    # Simplified aggregation - just basic stats
    group_by_cols = [
        'year',
        'month', 
        'dow',
        F.col('vendorid').alias('vendor_id'),
        F.col('ratecodeid').alias('rate_code_id'),
        F.col('pulocationid').alias('pickup_location_id'),
        F.col('dolocationid').alias('dropoff_location_id'),
        F.col('payment_type').alias('payment_type_id'),
        'pickup_datetime',
        'dropoff_datetime'
    ]
    
    # Add lat/long columns if they exist
    if 'pickup_latitude' in [c.lower() for c in columns]:
        group_by_cols.extend(['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude'])
    
    # Basic aggregation columns
    agg_columns = ['passenger_count', 'trip_distance', 'fare_amount', 'total_amount']
    
    agg_dict = {}
    for col in agg_columns:
        if col in [c.lower() for c in df2.columns]:
            agg_dict[col] = F.sum(col).alias(col)
        else:
            agg_dict[col] = F.lit(0).alias(col)
    
    # Add remaining columns with default values
    remaining_cols = ['extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'congestion_surcharge']
    for col in remaining_cols:
        if col in [c.lower() for c in df2.columns]:
            agg_dict[col] = F.sum(col).alias(col)
        else:
            agg_dict[col] = F.lit(0).alias(col)
    
    df_final = df2.groupBy(*group_by_cols).agg(*agg_dict.values())

    # Add service_type column
    if 'yellow' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(1))
    elif 'green' in file_path:
        df_final = df_final.withColumn('service_type', F.lit(2))

    return df_final


def load_to_staging_table(df):
    """
        Save data after processing to Staging Area (PostgreSQL) - Optimized
    """
    URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"

    properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "batchsize": "10000",  # Tăng batch size để write nhanh hơn
        "numPartitions": "4"   # Giảm số partitions
    }

    # Write data to PostgreSQL with optimized settings
    df.coalesce(4).write.jdbc(url=URL, table=DB_STAGING_TABLE, mode='append', properties=properties)
###############################################


###############################################
# Main - Optimized
###############################################
if __name__ == "__main__":
    start_time = time.time()
    logging.info("Starting optimized batch processing...")

    spark = create_spark_session()
    load_minio_config(spark.sparkContext)

    client = MinIOClient(
        endpoint_url=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY
    )

    files = client.list_parquet_files(BUCKET_NAME, prefix='batch/')
    logging.info(f"Found {len(files)} files to process")
    
    processed_count = 0
    for file in files:
        try:
            path = f"s3a://{BUCKET_NAME}/" + file
            logging.info(f"Processing file {processed_count + 1}/{len(files)}: {file}")

            df = spark.read.parquet(path)
            df_final = processing_dataframe(df, file)
            
            # Cache the result for faster processing
            df_final.cache()
            
            # Show progress
            record_count = df_final.count()
            logging.info(f"Records to load: {record_count}")
            
            # Load data to staging table
            load_to_staging_table(df_final)
            
            # Unpersist to free memory
            df_final.unpersist()
            
            processed_count += 1
            logging.info(f"✅ Completed file {processed_count}/{len(files)}")
            logging.info("="*50)
            
        except Exception as e:
            logging.error(f"❌ Error processing file {file}: {e}")
            continue

    elapsed_time = time.time() - start_time
    logging.info(f"✅ Optimized batch processing completed!")
    logging.info(f"📊 Processed {processed_count}/{len(files)} files")
    logging.info(f"⏱️  Total time: {elapsed_time:.2f} seconds")
    
    # Stop Spark session
    spark.stop()
###############################################
