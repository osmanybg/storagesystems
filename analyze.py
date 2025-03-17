# Analyze Temporal Access Patterns
# To understand how cache accesses fluctuate over time:
# Plot Request Volume Over Time
# Insight: Identify peak cache access times.

import sys
import os.path
import dask.dataframe as dd
import matplotlib.pyplot as plt
import plotly.express as px
import pandas as pd

def getData(fileName):
    dirPath = os.path.dirname(fileName)
    baseName = os.path.splitext(os.path.basename(fileName))[0]
    baseFileName = os.path.join(dirPath, baseName)

    df = dd.read_parquet(f"{baseFileName}.parquet")

    return df

def process1(df):
    """
    Analyze temporal access patterns in cache traces.
    
    Args:
        df: Dask DataFrame with cache trace data
        
    Returns:
        Dictionary of temporal pattern metrics and visualizable data
    """
    # Optimize computation by properly partitioning by time
    df = df.repartition(partition_size="100MB")
    
    # Aggregate requests by hour to identify daily patterns
    hourly_counts = df.groupby(df.datetime.dt.floor('h')).size().compute()
    
    # Identify peak hours (top 5% of traffic)
    threshold = hourly_counts.quantile(0.95)
    peak_hours = hourly_counts[hourly_counts > threshold].index.tolist()
    
    # Calculate day of week patterns
    day_of_week = df.groupby(df.datetime.dt.dayofweek).size().compute()
    
    # Calculate inter-arrival times for frequently accessed keys
    # First get top 1000 keys by access frequency
    top_keys = df.anon_key.value_counts().nlargest(1000).compute().index
    
    # Filter for these keys and calculate time between accesses
    filtered_df = df[df.anon_key.isin(top_keys)]
    
    # Calculate inter-arrival times (this is a complex operation with dask)
    # We'll use a map-reduce approach
    def calculate_interarrival(partition):
        import pandas as pd
        result = []
        for key in top_keys:
            key_data = partition[partition.anon_key == key]
            if len(key_data) > 1:
                sorted_times = key_data.sort_values('timestamp')['timestamp']
                interarrivals = sorted_times.diff().dropna()
                result.append(pd.DataFrame({
                    'anon_key': [key] * len(interarrivals),
                    'interarrival': interarrivals.values
                }))
        return pd.concat(result) if result else pd.DataFrame({'anon_key': [], 'interarrival': []})
    
    interarrival_df = filtered_df.map_partitions(calculate_interarrival).compute()
    
    return {
        'hourly_pattern': hourly_counts,
        'day_of_week_pattern': day_of_week,
        'peak_hours': peak_hours,
        'interarrival_times': interarrival_df
    }

def analyze(df):
    result = process1(df)

    print(result)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No filename provided")
        print("Usage: python importWithDask.py <filename>")
    else:
        fileName = sys.argv[1]

        if fileName[-1] == '\\':
            fileName = fileName[0:-1]

        df = getData(fileName)
        analyze(df)
