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
    print (baseFileName)
    df = dd.read_parquet(f"{baseFileName}.parquet")
    print(df.head())

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


def temporalHourAnalysis(df):
    ## Step 2: Temporal Analysis (Peak Access Times)
    # Count operations per hour
    df_hourly = df.groupby("hour").size().compute()

    # Plot results
    fig = px.line(df_hourly, x=df_hourly.index, y=df_hourly.values, title="Cache Requests Over Daily Hours")
    fig.show()


def temporalDayAnalysis(df):
    ## Step 2: Temporal Analysis (Peak Access Times)
    # Count operations per hour
    df_day = df.groupby("day").size().compute()

    # Plot results
    fig = px.line(df_day, x=df_day.index, y=df_day.values, title="Cache Requests Over Weekly Days")
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
    # Aggregate by day and operation type
    df_daily_operations = df.groupby(["day", "operation"]).size().compute().unstack()

    # Plot results
    fig = px.line(df_daily_operations, x=df_daily_operations.index, y=df_daily_operations.columns, title="Daily Operation Trends")
    fig.show()


def biasConsiderations(df):
    ## Step 5: Bias Consideration
    # Calculate percentage of total requests per day
    df_bias_check = df.groupby("day").size().compute()
    df_bias_check = (df_bias_check / df_bias_check.sum()) * 100

    # Plot workload distribution
    fig = px.bar(df_bias_check, x=df_bias_check.index, y=df_bias_check.values, title="Workload Distribution Over Time")
    fig.show()


def cacheHitMissAnalysis(df):
    ## Step 6: Cache Hit vs. Miss Analysis
    df_hit_miss = df.groupby("operation").size().compute()
    fig = px.bar(df_hit_miss, x=df_hit_miss.index, y=df_hit_miss.values, title="Cache Hits vs Misses")
    fig.show()


def keyValueSizeDistribution(df):
    ## Step 7: Key & Value Size Distribution
    fig = px.histogram(df, x="key_size", title="Key Size Distribution")
    fig.show()
    fig = px.histogram(df, x="value_size", title="Value Size Distribution")
    fig.show()


def topClientsByRequests(df):
    ## Step 8: Top Clients by Requests
    df_clients = df.groupby("client_id").size().compute().nlargest(10)
    fig = px.bar(df_clients, x=df_clients.index, y=df_clients.values, title="Top 10 Clients by Requests")
    fig.show()


def ttlEffectiveness(df):
    ## Step 9: TTL Effectiveness
    fig = px.histogram(df, x="ttl", title="TTL Distribution")
    fig.show()


def weekendVsWeekdayPatterns(df):
    ## Step 10: Weekend vs. Weekday Patterns
    df["weekday"] = df["datetime"].dt.weekday
    df_weekday = df.groupby("weekday").size().compute()
    fig = px.bar(df_weekday, x=df_weekday.index, y=df_weekday.values, title="Cache Requests: Weekday vs Weekend")
    fig.show()


def cacheAnomalyDetection(df):
    ## Step 11: Cache Anomaly Detection
    df_hourly = df.groupby("hour").size().compute()
    threshold = df_hourly.mean() + 3 * df_hourly.std()
    anomalies = df_hourly[df_hourly > threshold]
    fig = px.scatter(df_hourly, x=df_hourly.index, y=df_hourly.values, title="Cache Anomalies Detection")
    fig.add_scatter(x=anomalies.index, y=anomalies.values, mode='markers', marker=dict(color='red', size=10), name='Anomalies')
    fig.show()


def analyze(df):
    print ("Basic Summary:")
    basicSummary(df)
    
    # print ("Temporal Analysis:")
    # temporalHourAnalysis(df)

    # print ("Workload Composition:")
    # workloadComposition(df)

    # print ("Visualizations and Trends:")
    # visualizationsAndTrends(df)

    # print ("Bias Considerations:")
    # biasConsiderations(df)
    
    # print ("Cache Hit vs. Miss Analysis:")
    # cacheHitMissAnalysis(df)
    
    # print ("Key & Value Size Distribution:")
    # keyValueSizeDistribution(df)
    
    # print ("Top Clients by Requests:")
    # topClientsByRequests(df)
    
    # print ("TTL Effectiveness:")
    # ttlEffectiveness(df)
    
    # print ("Weekend vs. Weekday Patterns:")
    # weekendVsWeekdayPatterns(df)
    
    # print ("Cache Anomaly Detection:")
    # cacheAnomalyDetection(df)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No filename provided")
        print("Usage: python importWithDask.py <filename>")
    else:
        fileName = sys.argv[1]

        if fileName[-1] == '\\':
            fileName = fileName[0:-1]

        df = getData(fileName)
        #analyze(df)
