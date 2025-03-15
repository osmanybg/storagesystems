import timeit
import dask.dataframe as dd

file = "data/cluster10"

def readFromParquet():
    # Read the Parquet file
    df = dd.read_parquet(f"{file}.parquet")
    df.to_parquet(f"test1.parquet")

def readFromCSV():
    # Manually define column names
    column_names = ["timestamp", "anon_key", "key_size", "value_size", "client_id", "operation", "ttl"]

    # Load dataset efficiently
    df = dd.read_csv(f"{file}.sort", header=None, names=column_names)

    df.to_parquet(f"test2.parquet")

execution_time = timeit.timeit(readFromCSV, number=1)  # Runs once
print(f"readFromCSV Execution time: {execution_time:.4f} seconds")

execution_time = timeit.timeit(readFromParquet, number=1)  # Runs once
print(f"readFromParquet Execution time: {execution_time:.4f} seconds")
