import dask.dataframe as dd
import pandas as pd

baseFileName = "../data/cluster10"

# Read the Parquet file
df = dd.read_parquet(f"{baseFileName}.parquet")

print(df.head(100))

result = df['hour'].value_counts().compute()
print(list(result.items()))
exit()


# Group by 'datetime' and count the number of rows in each group
datetime_counts = df.groupby('day').size().compute()

# Convert the Dask Series to a Pandas DataFrame
result_df = datetime_counts.to_frame(name='count').reset_index()

print(result_df)
exit()

threshold = pd.Timestamp('1970-01-01 01:00:00')

    # Filter the DataFrame for datetimes greater than the threshold
filtered_df = df[df['datetime'] > threshold]
print(len(filtered_df))

# Calculate the number of distinct datetime values
distinct_df = filtered_df['day'].unique().compute()
print(len(distinct_df))
print(distinct_df.head())
exit()

print("DataFrame information:")
print(f"Columns: {df.columns.tolist()}")
print(f"Data types: \n{df.dtypes}")
print(f"Number of partitions: {df.npartitions}")

# Schema information
print("\nSchema information:")
schema = df._meta
print(schema)

# Compute basic statistics (this will trigger computation)
print("\nBasic statistics:")
print(df.describe().compute())

# print("\nParquet metadata:")
# import pyarrow.parquet as pq
# metadata = pq.read_metadata(f"{baseFileName}.parquet")
# print(f"Schema: {metadata.schema}")

# df["datetime"] = dd.to_datetime(df["timestamp"], unit="s")
# df.to_parquet(f"{baseFileName}.parquet")

# df2 = df.where(df["timestamp"] > 0).head(100)
# df3 = df.where(
#     (df["ttl"] < 0) |
#     (df["key_size"] < 0) |
#     (df["value_size"] < 0)
# ).head(100)

# print(df.head(100))
# print(df.tail(100))
# print(df2)
# print(df3)


# df["timestamp2"] = dd.to_datetime(df["timestamp"], unit="s")
# df2 = df[(df["timestamp"] > 1000) & (~df['timestamp'].isna())].compute()


# print(df2.head()['timestamp2'].dt.strftime('%Y-%m-%d %H:%M:%S'))

# unique_timestamps = df["timestamp"].unique().compute()
# print("Unique timestamp values:")
# print(unique_timestamps)