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
    ├── airflow/                                    /* Airflow setup and DAGs */
    │   └── dags/
    │       ├── elt_pipeline_dag.py                 /* Basic ELT */
    │       └── elt_pipeline_optimized_dag.py       /* Optimized ELT incl. batch to DW */
    ├── batch_processing/
    │   └── datalake_to_dw.py                       /* Original batch (datalake → DW) */
    ├── batch_processing_optimized.py               /* Optimized batch entrypoint */
    ├── config/                                     /* Project configs */
    │   ├── spark.yaml
    │   ├── datalake.yaml
    │   └── dbt_profiles.yml
    ├── data/                                       /* Sample parquet datasets */
    │   ├── 2021/
    │   ├── 2022/
    │   ├── 2023/
    │   └── 2024/
    ├── data_validation/
    │   ├── gx/
    │   │   ├── checkpoints/
    │   │   ├── expectations/
    │   │   └── great_expectations.yml
    │   └── transform.ipynb
    ├── debezium/
    │   ├── configs/
    │   │   └── taxi-nyc-cdc.json                   /* Debezium connector config */
    │   └── run.sh
    ├── jars/                                       /* Spark extra JARs (S3A, Kafka, JDBC) */
    ├── scripts/
    │   ├── data/taxi_lookup.csv
    │   ├── extract_load.py                         /* local → MinIO raw */
    │   ├── transform_data.py                       /* raw → processed */
    │   └── convert_to_delta.py                     /* processed → delta (optional) */
    ├── streaming_processing/
    │   ├── read_parquet_streaming.py
    │   ├── schema_config.json
    │   ├── streaming_fixed.py
    │   ├── streaming_to_minio_fixed.py             /* Kafka → MinIO (parquet) */
    │   └── streaming_to_datalake.py
    ├── trino/
    │   ├── catalog/datalake.properties
    │   └── etc/{config.properties,jvm.config,node.properties}
    ├── utils/
    │   ├── create_schema.py
    │   ├── create_table.py
    │   ├── postgresql_client.py
    │   ├── helpers.py
    │   ├── minio_utils.py
    │   ├── streaming_data_json.py
    │   ├── streaming_data_db.py
    │   └── trinp_db.py
    ├── airflow-docker-compose.yaml
    ├── docker-compose.yaml
    ├── stream-docker-compose.yaml
    ├── Makefile
    ├── README.md
    └── requirements.txt
```

# 🚀 Getting Started

1.  **Clone the repository**:

    ```bash
    git clone <your-fork-or-repo-url>
    cd NYC_Taxi_Data_Pipeline
    ```

2.  **Start all infrastructures**:

    ```bash
    make run_all
    ```

    This command will download the necessary Docker images, create containers, and start the services in detached mode.

3.  **Setup environment**:

    ```bash
    conda create -n bigdata python==3.9 -y
    conda activate bigdata
    pip install -r requirements.txt
    ```

4.  **Access the Services**:

    - Postgres is accessible on the default port `5432`.
    - Kafka Control Center: `http://localhost:9021`.
    - Debezium Connect REST API: `http://localhost:8083` and Debezium UI: `http://localhost:8085`.
    - MinIO Console: `http://localhost:9001`.
    - Airflow UI: `http://localhost:8080`.
    - Spark UI (when a Spark job is running): `http://localhost:4040` (or `4041+`).

5.  **Download Dataset**:
    You can download and use this dataset here: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

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

### 📋 Manual Step-by-Step

1.  **Start Infrastructure**

    ```bash
    make batch_up
    # Or manually start containers as needed
    ```

2.  **Upload local parquet → MinIO `raw`**

    ```bash
    python scripts/extract_load.py
    ```

3. **Transform `raw` → `processed` (Parquet)**

    ```bash
    python scripts/transform_data.py
    ```

4. **(Optional) Convert `processed` → `delta`**

    ```bash
    python scripts/convert_to_delta.py
    ```

5. **Create database schema and staging tables**

    ```bash
    python utils/create_schema.py
    python utils/create_table.py
    ```

6. **Execute optimized Spark batch (Datalake → DW)**

    ```bash
    # Recommended
    python batch_processing_optimized.py

    # Or original version
    python batch_processing/datalake_to_dw.py
    ```

### 📊 Performance Notes

- Complete pipeline: ~7–15 minutes (sample dataset)
- Optimized processing: handles 40M+ records efficiently
- Memory usage: optimized for ~4GB
- Timeout handling: retry and progress logging

7. **Validate data (optional)**

    ```bash
    cd data_validation
    # Configure expectations in gx/ if needed
    # Open the notebook for a quick exploration
    # transform.ipynb
    ```

8. **DBT models (star schema)**

    ```bash
    cd nyc_taxi
    # Configure profiles using config/dbt_profiles.yml
    # dbt deps
    # dbt build
    ```

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

### (Optional) Query streaming data in MinIO with Trino

```bash
docker exec -ti datalake-trino bash
trino
```

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