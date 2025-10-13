import os
import sys
from time import sleep
from pyarrow.parquet import ParquetFile
import pyarrow as pa 
import pandas as pd

from dotenv import load_dotenv
load_dotenv(".env")

from postgresql_client import PostgresSQLClient

###############################################
# Parameters & Arguments
###############################################
TABLE_NAME = "iot.taxi_nyc_time_series"
PARQUET_FILE = "./data/2024/yellow_tripdata_2024-01.parquet"
NUM_ROWS = 10000
###############################################


###############################################
# Main
###############################################
def main():

    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")
        # Fail fast to avoid using undefined 'columns'
        raise SystemExit(1)

    # Read a batch of rows
    pf = ParquetFile(PARQUET_FILE) 
    first_n_rows = next(pf.iter_batches(batch_size = NUM_ROWS)) 
    df = pa.Table.from_batches([first_n_rows]).to_pandas() 

    # Normalize datetime columns to strings to avoid server-side parsing ambiguity
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in df.columns:
            df[col] = df[col].astype(dtype='str')

    # Build batch insert base SQL for execute_values

    # Prepare values with safe access for optional fields
    values_list = []
    for _, row in df.iterrows():
        # Safe label-based access for Pandas Series (avoid positional indexing misalignment)
        # Convert NaN to None so psycopg2 inserts NULL
        params = tuple(
            (None if (col not in row.index or pd.isna(row[col])) else row[col])
            for col in columns
        )
        values_list.append(params)

    # Log a small sample of payloads being sent
    print(f"Sample payload: {values_list[0] if values_list else '[]'}")

    # Batch insert using execute_values for speed
    # Note: execute_values needs a base query like INSERT INTO ... VALUES %s
    base_sql = f"INSERT INTO {TABLE_NAME} ({','.join(columns)}) VALUES %s"
    pc.execute_values(base_sql, values_list)

def format_record(row):
    # Deprecated: keep for backward compatibility if needed; prefer logging params tuples
    try:
        return tuple(row.values())
    except Exception:
        return ()
###############################################


if __name__ == "__main__":
    main()

