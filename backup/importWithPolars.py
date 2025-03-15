import polars as pl

# Manually define column names
column_names = ["timestamp", "anon_key", "operation", "ttl", "key_size", "value_size"]

# Load dataset efficiently
df = pl.read_csv("data/cluster10_sample.sort", has_header=False, new_columns=column_names)

# Check the first few rows
print(df.head())

df.write_parquet("cache_trace.parquet")