# Analyze Temporal Access Patterns
# To understand how cache accesses fluctuate over time:
# Plot Request Volume Over Time
# Insight: Identify peak cache access times.

import sys
import os.path
import dask.dataframe as dd
import matplotlib.pyplot as plt

def getData(fileName):
    dirPath = os.path.dirname(fileName)
    baseName = os.path.splitext(os.path.basename(fileName))[0]
    baseFileName = os.path.join(dirPath, baseName)

    df = dd.read_parquet(f"{baseFileName}.parquet")

    return df

def analyze(df):
    df_pandas = df.to_pandas()  # Convert to Pandas for plotting

    # Group by hourly intervals
    df_grouped = df_pandas.groupby("hour").size()

    # Plot the trend
    plt.figure(figsize=(12, 6))
    plt.plot(df_grouped.index, df_grouped.values, marker="o", linestyle="-")
    plt.xlabel("Time (Hourly)")
    plt.ylabel("Number of Cache Requests")
    plt.title("Cache Access Patterns Over Time")
    plt.xticks(rotation=45)
    plt.show()


    # Analyze Workload Composition
    # Understanding GET/SET/DELETE distributions will help optimize caching strategies.
    # Operation Type Distribution
    # Insight: Find out which operations dominate (e.g., mostly GETs vs. SETs).
    import seaborn as sns

    # Count occurrences of each operation type
    operation_counts = df_pandas["operation"].value_counts()

    # Plot the distribution
    plt.figure(figsize=(8, 6))
    sns.barplot(x=operation_counts.index, y=operation_counts.values)
    plt.xlabel("Operation Type")
    plt.ylabel("Count")
    plt.title("Cache Workload Composition")
    plt.show()

    # Identify Peak Usage Periods
    # When Do Most Cache Requests Happen?
    # Insight: Determine if usage follows a daily pattern (e.g., peaks at certain hours).

    df_pandas["hour"] = df_pandas["hour"].dt.hour  # Extract hour of day
    hourly_counts = df_pandas.groupby("hour").size()

    plt.figure(figsize=(10, 5))
    plt.bar(hourly_counts.index, hourly_counts.values)
    plt.xlabel("Hour of Day")
    plt.ylabel("Request Count")
    plt.title("Cache Requests Per Hour of the Day")
    plt.xticks(range(24))
    plt.show()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No filename provided")
        print("Usage: python importWithDask.py <filename>")
    else:
        fileName = sys.argv[1]
        df = getData(fileName)
        analyze(df)