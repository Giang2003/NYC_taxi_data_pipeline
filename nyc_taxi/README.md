## dbt – NYC Taxi Warehouse Models

This folder contains dbt models for transforming staged NYC Taxi data into a star schema (dimensions + fact) for analytics.

### Project Layout
- `models/production`: core models
  - `dim_vendor.sql`, `dim_rate_code.sql`, `dim_payment.sql`, `dim_pickup_location.sql`, `dim_dropoff_location.sql`, `dim_service_type.sql`
  - `fact_trip.sql` joins dimensions with `staging.nyc_taxi` and generates a `trip_id` via `dbt_utils.surrogate_key`
- `models/example`: dbt starter examples (optional)
- `packages.yml`: includes `dbt_utils`
- `dbt_project.yml`: project config

### Prerequisites
- Data loaded to DW staging table: `staging.nyc_taxi` (via `batch_processing_optimized.py`)
- dbt profile configured in project root `config/dbt_profiles.yml` (profile name: `nyc_taxi`)

### Quickstart
```bash
cd nyc_taxi
# (optional) install packages
dbt deps

# Build all models
dbt build --profiles-dir ../config

# or run + test separately
dbt run --profiles-dir ../config
dbt test --profiles-dir ../config
```

### Notes
- Set database/host/user/password in `config/dbt_profiles.yml` to point to your Postgres DW (`dw-postgresql:5432`, db `k6`, user `k6`, password `k6` by default in docker-compose).
- Consider using `materialized='incremental'` for large models.
- Add tests (`unique`, `not_null`) to critical keys and surrogate keys.
