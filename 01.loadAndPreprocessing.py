import sys
import os.path
import dask.dataframe as dd

def process(fileName):
    dirPath = os.path.dirname(fileName)
    baseName = os.path.splitext(os.path.basename(fileName))[0]
    baseFileName = os.path.join(dirPath, baseName)

    # Manually define column names
    column_names = ["timestamp", "anon_key", "key_size", "value_size", "client_id", "operation", "ttl"]

    # Load dataset efficiently
    df = dd.read_csv(fileName, header=None, names=column_names)

    # Filter out invalid rows (e.g., negative or missing values)
    df = df[(df["ttl"] >= 0) & (df["key_size"] >= 0) & (df["value_size"] >= 0)]

    # Add datetime column
    df["datetime"] = dd.to_datetime(df["timestamp"], unit="s")

    # Add day column
    df["day"] = df["datetime"].dt.floor("D")  # Round to nearest day

    # Add hour column
    df["hour"] = df["datetime"].dt.floor("h")  # Round to nearest hour

    # Check the first few rows
    print(df.head())
    df.to_parquet(f"{baseFileName}.parquet")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No filename provided")
        print("Usage: python importWithDask.py <filename>")
    else:
        fileName = sys.argv[1]
        process(fileName)