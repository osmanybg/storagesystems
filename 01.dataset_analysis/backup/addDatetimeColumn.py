import dask.dataframe as dd

baseFileName = "../data/cluster1"

# Read the Parquet file
df = dd.read_parquet(f"{baseFileName}.parquet")

print(len(df))

df["datetime"] = dd.to_datetime(df["timestamp"], unit="s")
df.to_parquet(f"{baseFileName}.parquet")

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