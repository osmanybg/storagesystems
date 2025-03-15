# Analyze Temporal Access Patterns
# To understand how cache accesses fluctuate over time:
# Plot Request Volume Over Time
# Insight: Identify peak cache access times.

import sys
import os.path
import dask.dataframe as dd
import matplotlib.pyplot as plt
import plotly.express as px

def getData(fileName):
    dirPath = os.path.dirname(fileName)
    baseName = os.path.splitext(os.path.basename(fileName))[0]
    baseFileName = os.path.join(dirPath, baseName)

    df = dd.read_parquet(f"{baseFileName}.parquet")

    return df


def basicSummary(df):
    ## Step 1: Dataset Exploration
    # Get basic summary
    summary = {
        "Total Records": len(df),
        "Time Range": (df["datetime"].min(), df["datetime"].max()),
        "Unique Operations": df["operation"].unique().compute()
    }

    print(summary)


def temporalAnalysis(df):
    ## Step 2: Temporal Analysis (Peak Access Times)
    # Count operations per hour
    # df["hour"] = df["datetime"].dt.floor("h")  # Round to nearest hour
    df_hourly = df.groupby("hour").size().compute()

    # Plot results
    fig = px.line(df_hourly, x=df_hourly.index, y=df_hourly.values, title="Cache Requests Over Time")
    fig.show()


def workloadComposition(df):
    ## Step 3: Workload Composition (Operation Proportions)
    # Operation Type Distribution
    df_operations = df.groupby("operation").size().compute()

    # Convert to percentage
    df_operations = (df_operations / df_operations.sum()) * 100

    # Plot results
    fig = px.pie(df_operations, names=df_operations.index, values=df_operations.values, title="Cache Operation Distribution")
    fig.show()


def visualizationsAndTrends(df):
    ## Step 4: Visualization & Trends
    # df["day"] = df["datetime"].dt.floor("D")  # Round to nearest day

    # Aggregate by day and operation type
    df_daily_operations = df.groupby(["day", "operation"]).size().compute().unstack()

    # Plot results
    fig = px.line(df_daily_operations, x=df_daily_operations.index, y=df_daily_operations.columns, title="Daily Operation Trends")
    fig.show()


def biasConsiderations(df):
    ## Step 5: Bias Consideration
    # Calculate percentage of total requests per day
    # df["day"] = df["datetime"].dt.floor("D")  # Round to nearest day
    df_bias_check = df.groupby("day").size().compute()
    df_bias_check = (df_bias_check / df_bias_check.sum()) * 100

    # Plot workload distribution
    fig = px.bar(df_bias_check, x=df_bias_check.index, y=df_bias_check.values, title="Workload Distribution Over Time")
    fig.show()


def analyze(df):
    print ("Basic Summary:")
    basicSummary(df)
    
    print ("Temporal Analysis:")
    temporalAnalysis(df)

    print ("Workload Composition:")
    workloadComposition(df)

    print ("Visualizations and Trends:")
    visualizationsAndTrends(df)

    print ("Bias Considerations:")
    biasConsiderations(df)


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