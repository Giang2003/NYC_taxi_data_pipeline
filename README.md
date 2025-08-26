# [NYC_TAXI Data Pipeline 🚕](https://github.com/trannhatnguyen2/NYC_Taxi_Data_Pipeline)

# Table Of Contents

<!--ts-->
-  [Getting Started](#-getting-started)
- [How to Guide](#-how-to-guide)
- [References](#-references)
  <!--te-->


# 📁 Repository Structure

```shell
.
    ├── airflow/                                    /* airflow folder including dags,.. /*
    ├── batch_processing/
    │   └── datalake_to_dw.py                           /* ETL data from datalake to staging area /*
    ├── configs/                                    /* contain config files /*
    │   ├── spark.yaml
    │   └── datalake.yaml
    ├── data/                                       /* contain dataset /*
    │   ├── 2020/
    │   ├── 2021/
    │   ├── 2022/
    │       ├── green_tripdata_2022-01.parquet
    │       ├── green_tripdata_2022-02.parquet
    │       ├── green_tripdata_2022-03.parquet
    │       ├── ...
    │       ├── yellow_tripdata_2022-01.parquet
    │       ├── yellow_tripdata_2022-02.parquet
    │       ├── yellow_tripdata_2022-03.parquet
    │       └── ...
    │   ├── 2023/
    │   └── 2024/
    ├── data_validation/                            /* validate data before loading data warehouse /*
    │   ├── gx/
    │       ├── checkpoints/
    │       ├── expectations/
    │       ├── ...
    │       └── great_expections.yml
    │   ├── full_flow.ipynb
    │   └── reload_and_validate.ipynb
    ├── dbt_nyc/                                    /* data transformation folder /*
    ├── debezium/                                   /* CDC folder /*
    │    ├── configs/
    │       └──  taxi-nyc-cdc-json                           /* file config to connect between database and kafka through debezium  /*
    │    └── run.sh                                     /* run create connector */
    ├── imgs/
    ├── jars/                                       /* JAR files for Spark version 3.5.1 */
    ├── scripts/
    │   ├── data/
    │       └── taxi_lookup.csv                             /* CSV file to look up latitude and longitude */
    │   ├── extract_load.py                             /* upload data from local to 'raw' bucket (MinIO) */
    │   ├── transform_data.py                           /* transform data to 'processed' bucket (MinIO) */
    │   └── convert_to_delta.py                         /* convert data parquet file from 'processed' to 'delta' bucket (MinIO) */
    ├── streaming_processing/
    │    ├── read_parquet_streaming.py
    │    ├── schema_config.json
    │    └── streaming_to_datalake.py               /* read data stream in kafka topic and write to 'raw' bucket (Minio) */
    ├── trino/
    │    ├── catalog/
    │       └──  datalake.properties
    │    ├── etc/
    │       ├── config.properties
    │       ├── jvm.config
    │       └── node.properties
    ├── utils/                                     /* functions /*
    │    ├── create_schema.py
    │    ├── create_table.py
    │    ├── postgresql_client.py                       /* PostgreSQL Client: create connect, execute query, get columns in bucket /*
    │    ├── helper.py
    │    ├── minio_utils.py                             /* Minio Client: create connect, create bucket, list parquet files in bucket /*
    │    ├── streaming_data_json.py                     /* stream data json format into kafka */
    │    ├── streaming_data_db.py                       /* stream data into database */
    │    └── trino_db_scripts_generate.py
    ├── .env
    ├── .gitignore
    ├── airflow-docker-compose.yaml
    ├── docker-compose.yaml
    ├── Makefile
    ├── README.md
    ├── requirements.txt
    └── stream-docker-compose.yaml
```

# 🚀 Getting Started

1.  **Clone the repository**:

    ```bash
    git clone 
    ```

2.  **Start all infrastructures**:

    ```bash
    make run_all
    ```

    This command will download the necessary Docker images, create containers, and start the services in detached mode.

3.  **Setup environment**:

    ```bash
    conda create -n bigdata python==3.9
    y
    conda activate bigdata
    pip install -r requirements.txt
    ```

    Activate your conda environment and install required packages

4.  **Access the Services**:

    - Postgres is accessible on the default port `5432`.
    - Kafka Control Center is accessible at `http://localhost:9021`.
    - Debezium Connect REST API at `http://localhost:8083` and Debezium UI at `http://localhost:8085`.
    - MinIO is accessible at `http://localhost:9001`.
    - Airflow is accessible at `http://localhost:8080`.
    - Spark UI (when a Spark job is running) is at `http://localhost:4040` (or `4041+`).

5.  **Download Dataset**:
    You can download and use this dataset in here: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

6.  **Download JAR files for Spark**:

    ```bash
    mkdir jars
    cd jars
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
    curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.4.3/postgresql-42.4.3.jar
    curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.2.1/spark-sql-kafka-0-10_2.12-3.2.1.jar
    ```

# 🔍 How to Guide

## I. Batch Processing

### **Quick Start - Complete Batch Processing**

Run the complete batch processing pipeline with one command:

```bash
# Complete automated test (recommended)
python test_complete_batch.py
```

This will automatically execute all steps below and provide detailed progress tracking.

### 📋 **Manual Step-by-Step Process**

1.  **Start Infrastructure**:

```bash
# Start all required services
make batch_up
# Or manually start containers:
# docker run -d --name test-postgres -p 5432:5432 -e POSTGRES_USER=k6 -e POSTGRES_PASSWORD=k6 -e POSTGRES_DB=k6 postgres:13
# docker run -d --name test-minio -p 9000:9000 -p 9001:9001 -e MINIO_ACCESS_KEY=minio_access_key -e MINIO_SECRET_KEY=minio_secret_key minio/minio server /data --console-address ":9001"
```

2.  **Upload data from local to `raw` bucket (MinIO)**:

```bash
python scripts/extract_load.py
```

3. **Transform data from `raw` to `processed` bucket (MinIO)**:

```bash
python scripts/transform_data.py
```

4. **(Optional) Convert data to Delta Lake format**:

```bash
python scripts/convert_to_delta.py
```

5. **Create database schema and staging tables**:

```bash
python utils/create_schema.py
python utils/create_table.py
```

6. **Execute optimized Spark processing (Datalake → Data Warehouse)**:

```bash
# Use optimized version (recommended - faster and more reliable)
python batch_processing_optimized.py

# Or use original version
python batch_processing/datalake_to_dw.py
```

### ⚡ **Quick Testing Options**

```bash
# Quick test with limited data (2 files only - for development/testing)
python test_batch_quick.py

# Full test with progress tracking and error handling
python test_complete_batch.py
```

### 📊 **Performance Notes**

- **Complete pipeline**: ~7-15 minutes for full dataset
- **Optimized processing**: Handles 40M+ records efficiently  
- **Memory usage**: Optimized for 4GB RAM allocation
- **Timeout handling**: Automatic retry and progress tracking


6. **Validate data in Staging Area**

```bash
   cd data_validation
   great_expectations init
   Y
```

Then, run the file `full_flow.ipynb`
]

7. **Use DBT to transform the data and create a star schema in the data warehouse**

```bash
   cd dbt_nyc
```

See the DBT project guide here:

- DBT models and usage: [nyc_taxi/README.md](nyc_taxi/README.md)


## II. Stream Processing

### 🚀 Quick Start (CDC → Kafka → Spark → MinIO)

1) Start services
```bash
make stream_up   # Zookeeper, Kafka, Schema Registry, Debezium, Control Center
make batch_up    # PostgreSQL, MinIO, Hive Metastore, Trino
```

2) Prepare database schemas/tables
```bash
python utils/create_schema.py
python utils/create_table.py
```

3) Register Debezium connector (Postgres → Kafka)
```bash
cd debezium
curl -X POST -H "Content-Type: application/json" \
  --data @configs/taxi-nyc-cdc.json \
  http://localhost:8083/connectors
curl -X GET http://localhost:8083/connectors/taxi-nyc-cdc/status
cd ..
```

4) Produce data changes (CDC)
```bash
python utils/streaming_data_db.py   # Ctrl+C to stop when enough
```

5) Run Spark streaming job (Kafka → MinIO)
```bash
python streaming_processing/streaming_to_minio_fixed.py
```
Notes:
- Watch console logs; Spark UI appears at `http://localhost:4040` while running.
- Kafka Control Center: `http://localhost:9021` → Topics → `device.iot.taxi_nyc_time_series` → Messages.
- MinIO Console: `http://localhost:9001` → bucket `raw/stream/` for Parquet outputs.

### (Optional) Read/query streaming data in MinIO with Trino

After putting your files to ` MinIO`, please execute `trino` container by the following command:

```bash
docker exec -ti datalake-trino bash
trino
```

After that, run the following command to register a new schema for our data:

```sql

    CREATE SCHEMA IF NOT EXISTS datalake.stream
    WITH (location = 's3://raw/');

    CREATE TABLE IF NOT EXISTS datalake.stream.nyc_taxi(
        VendorID                INT,
        tpep_pickup_datetime    TIMESTAMP,
        tpep_dropoff_datetime   TIMESTAMP,
        passenger_count         DOUBLE,
        trip_distance           DOUBLE,
        RatecodeID              DOUBLE,
        store_and_fwd_flag      VARCHAR,
        PULocationID            INT,
        DOLocationID            INT,
        payment_type            INT,
        fare_amount             DOUBLE,
        extra                   DOUBLE,
        mta_tax                 DOUBLE,
        tip_amount              DOUBLE,
        tolls_amount            DOUBLE,
        improvement_surcharge   DOUBLE,
        total_amount            DOUBLE,
        congestion_surcharge    DOUBLE,
        airport_fee             DOUBLE
    ) WITH (
        external_location = 's3://raw/stream',
        format = 'PARQUET'
    );

```

## III. Airflow - Data Orchestration

```bash
   cd airflow/
```

See the detailed setup and usage in the Airflow README:

- Airflow guide: [airflow/README.md](airflow/README.md)

---

# 📌 References

[1] [NYC Taxi Trip Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

