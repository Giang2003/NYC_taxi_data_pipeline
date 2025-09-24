# Airflow Guide – NYC Taxi Data Pipeline

This folder contains the Airflow setup and DAGs to orchestrate the NYC Taxi batch pipeline (extract → transform → load to DW). There are two DAGs:
- `dags/elt_pipeline_dag.py`: original ELT (extract → transform → optional delta convert)
- `dags/elt_pipeline_optimized_dag.py`: optimized ELT including Spark batch to DW

## Prerequisites
- Docker Desktop (or Docker Engine) running
- (Optional) Python env for running project scripts locally: `pip install -r ../requirements.txt`
- Clone the root project

## Start Airflow
Use the root Makefile (recommended) or the compose file directly.

Recommended:
```bash
# from project root
make airflow_up
```
Direct compose:
```bash
docker compose -f airflow-docker-compose.yaml up -d --build
```

Services exposed:
- Webserver: `http://localhost:8080`
- Default credentials (if prompted): `airflow` / `airflow`

Stop Airflow:
```bash
make airflow_down
# or
docker compose -f airflow-docker-compose.yaml down
```

## DAGs Overview
- `elt_pipeline_dag.py` steps:
  1) Extract & load raw data to MinIO (`scripts/extract_load.py`)
  2) Transform raw → processed in MinIO (`scripts/transform_data.py`)
  3) (Optional) Convert processed → delta (`scripts/convert_to_delta.py`)
- `elt_pipeline_optimized_dag.py` steps:
  1) Extract & load to MinIO
  2) Transform in MinIO (process files one-by-one to avoid OOM)
  3) Batch processing to DW (`batch_processing_optimized.py` via BashOperator)

DAG files live in `airflow/dags/` and are mounted into the Airflow containers by the compose file.

## First Run – Quick Start
1) Start data infra (recommended):
```bash
make batch_up        # Postgres, MinIO, Metastore, Trino
```
2) Start Airflow:
```bash
make airflow_up
```
3) Open the UI: `http://localhost:8080` → Login `airflow/airflow` → Browse → DAGs
4) Turn on the desired DAG and click “Trigger DAG”:
   - `elt_pipeline_optimized`
   - or `elt_pipeline`
5) Monitor in UI: Graph view / Task Instances / Logs

## Trigger via CLI (optional)
Run inside the webserver container:
```bash
docker exec -it airflow-webserver bash
# list dags
airflow dags list
# trigger optimized
airflow dags trigger elt_pipeline_optimized
# watch runs
airflow dags list-runs -d elt_pipeline_optimized
# view task logs
airflow tasks logs -d elt_pipeline_optimized extract_load <run_id>
```

## Connections & Variables
The compose file sets up required services and mounts the project; tasks run Python scripts that use project configs. If you need Airflow Connections, typical ones are:
- Postgres (DW): host `dw-postgresql`, port `5432`, db `k6`, user `k6`, password `k6`
- MinIO (S3 compatible): endpoint `http://datalake-minio:9000`, access `minio_access_key`, secret `minio_secret_key`, path-style on

Create in UI: Admin → Connections → + (optional; most tasks use env/config directly).

## Monitoring Outputs
- MinIO Console: `http://localhost:9001` (check `raw/` and `processed/` buckets)
- Trino: connect to explore data if needed
- Airflow: task logs for per-step details

## Troubleshooting
- Web UI not reachable:
  - Ensure containers are up: `docker ps | grep airflow`
  - Restart: `make airflow_down && make airflow_up`
- DAG not showing:
  - Check `airflow/dags/` exists and is mounted
  - UI → “Browse → DAGs” → “Refresh”
- Import/module errors:
  - Rebuild images: `docker compose -f airflow-docker-compose.yaml up -d --build`
- Clean reset:
  - `docker compose -f airflow-docker-compose.yaml down -v` (removes metadata) then `up -d --build`

## Useful Links
- Root README Quick Start: `../README.md`
- Streaming guide: `../STREAMING_GUIDE.md`
