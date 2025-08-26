import pandas as pd
import sys

parquet_file = sys.argv[1]
df = pd.read_parquet(parquet_file)

print("📌 Columns:")
print(df.dtypes)

print("\n🧾 Sample records:")
print(df.head(5))

# print("📌 Columns:")
# print(df.dtypes)

