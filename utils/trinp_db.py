import os
import pandas as pd

data_sample = "./data/2022/yellow_tripdata_2022-01.parquet"
df = pd.read_parquet(data_sample)
columns = df.columns

scripts = "CREATE SCHEMA IF NOT EXISTS datalake.batch \n WITH (location = 's3://raw/'); \n\n "

scripts += "CREATE TABLE IF NOT EXISTS datalake.batch.nyc_taxi( \n"
for i, col in enumerate(columns):
    # Stream & Batch in 'raw' bucket
    if col == "tpep_pickup_datetime" or col == "tpep_dropoff_datetime":
        scripts += col + " TIMESTAMP"
    elif df[col].dtype == "int64":
        scripts += col + " INT"
    elif df[col].dtype == "float64":
        scripts += col + " DOUBLE"
    else:
        scripts += col + " VARCHAR"

    if (i < len(columns) -1):
        scripts += ", \n"
    else: 
        scripts += "\n"

scripts = scripts[:-1] + "\n ) WITH ( \n external_location = 's3://raw/batch', \n format = 'PARQUET' \n );"

print(scripts)