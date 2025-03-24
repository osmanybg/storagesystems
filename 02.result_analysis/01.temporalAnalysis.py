import json
import pandas as pd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os

inputDir = './data'
outputDir = './analysis_output'
visualizationDir = f'{outputDir}/visualizations'

# Create directories for saving visualizations
os.makedirs(visualizationDir, exist_ok=True)

# Load the JSON data
cluster1_temporal = json.load(open(f'{inputDir}/cluster1_temporal_analysis_output.json'))
cluster2_temporal = json.load(open(f'{inputDir}/cluster2_temporal_analysis_output.json'))
cluster3_temporal = json.load(open(f'{inputDir}/cluster3_temporal_analysis_output.json'))
cluster10_temporal = json.load(open(f'{inputDir}/cluster10_temporal_analysis_output.json'))

# Create a dictionary to store the data for easier access
clusters_temporal = {
    'Cluster 1': cluster1_temporal,
    'Cluster 2': cluster2_temporal,
    'Cluster 3': cluster3_temporal,
    'Cluster 10': cluster10_temporal
}

# 1. Hourly Request Distribution Visualization
def create_hourly_request_distribution():
    # Create a DataFrame for hourly patterns
    hourly_data = []
    for cluster_name, cluster_data in clusters_temporal.items():
        for hour, count in cluster_data['request_time_patterns']['hour_of_day'].items():
            hourly_data.append({
                'Cluster': cluster_name,
                'Hour': int(hour),
                'Request Count': count,
                'Normalized Count': count / cluster_data['request_volume']['total'] * 100
            })
    
    hourly_df = pd.DataFrame(hourly_data)
    
    # Create a plotly figure for hourly distribution
    fig = px.line(hourly_df, x='Hour', y='Normalized Count', color='Cluster',
                 title='Hourly Request Distribution (Normalized by Total Requests)',
                 labels={'Normalized Count': 'Percentage of Total Requests (%)', 'Hour': 'Hour of Day'},
                 markers=True)
    
    fig.update_layout(
        xaxis=dict(tickmode='linear', tick0=0, dtick=1),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template='plotly_white'
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/hourly_request_distribution.png', scale=2)
    return fig

# 2. Daily Request Distribution Visualization
def create_daily_request_distribution():
    # Create a DataFrame for daily patterns
    daily_data = []
    for cluster_name, cluster_data in clusters_temporal.items():
        for day, count in cluster_data['request_time_patterns']['day_of_week'].items():
            daily_data.append({
                'Cluster': cluster_name,
                'Day': day,
                'Request Count': count,
                'Normalized Count': count / cluster_data['request_volume']['total'] * 100
            })
    
    daily_df = pd.DataFrame(daily_data)
    
    # Define the order of days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daily_df['Day'] = pd.Categorical(daily_df['Day'], categories=day_order, ordered=True)
    daily_df = daily_df.sort_values('Day')
    
    # Create a plotly figure for daily distribution
    fig = px.bar(daily_df, x='Day', y='Normalized Count', color='Cluster', barmode='group',
                title='Daily Request Distribution (Normalized by Total Requests)',
                labels={'Normalized Count': 'Percentage of Total Requests (%)', 'Day': 'Day of Week'})
    
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template='plotly_white'
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/daily_request_distribution.png', scale=2)
    return fig

# 3. Peak Period Analysis Visualization
def create_peak_period_analysis():
    # Create a DataFrame for peak periods
    peak_data = []
    for cluster_name, cluster_data in clusters_temporal.items():
        peak_data.append({
            'Cluster': cluster_name,
            'Percentage of Peak Periods': cluster_data['peak_periods']['percentage_of_peak_periods'],
            'Ratio Peak to Non-Peak': cluster_data['peak_periods']['ratio_peak_to_non_peak'],
            'Number of Peak Periods': cluster_data['peak_periods']['num_peak_periods']
        })
    
    peak_df = pd.DataFrame(peak_data)
    
    # Create a plotly figure for peak period analysis
    fig = make_subplots(rows=1, cols=2, 
                        subplot_titles=('Percentage of Peak Periods', 'Ratio of Peak to Non-Peak Traffic'),
                        specs=[[{'type': 'bar'}, {'type': 'bar'}]])
    
    fig.add_trace(
        go.Bar(x=peak_df['Cluster'], y=peak_df['Percentage of Peak Periods'], name='Percentage of Peak Periods'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=peak_df['Cluster'], y=peak_df['Ratio Peak to Non-Peak'], name='Ratio Peak to Non-Peak'),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text='Peak Period Analysis Across Clusters',
        showlegend=False,
        template='plotly_white',
        height=500,
        width=1000
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/peak_period_analysis.png', scale=2)
    return fig

# 4. Peak Periods by Hour Visualization
def create_peak_periods_by_hour():
    # Create a DataFrame for peak periods by hour
    peak_hour_data = []
    for cluster_name, cluster_data in clusters_temporal.items():
        for hour, count in cluster_data['peak_periods']['peak_periods_by_hour'].items():
            peak_hour_data.append({
                'Cluster': cluster_name,
                'Hour': int(hour),
                'Number of Peak Periods': count
            })
    
    peak_hour_df = pd.DataFrame(peak_hour_data)
    
    # Create a plotly figure for peak periods by hour
    fig = px.bar(peak_hour_df, x='Hour', y='Number of Peak Periods', color='Cluster', barmode='group',
                title='Distribution of Peak Periods by Hour of Day',
                labels={'Number of Peak Periods': 'Count', 'Hour': 'Hour of Day'})
    
    fig.update_layout(
        xaxis=dict(tickmode='linear', tick0=0, dtick=1),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template='plotly_white'
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/peak_periods_by_hour.png', scale=2)
    return fig

# 5. Request Volume Comparison
def create_request_volume_comparison():
    # Create a DataFrame for request volume
    volume_data = []
    for cluster_name, cluster_data in clusters_temporal.items():
        volume_data.append({
            'Cluster': cluster_name,
            'Total Requests (Millions)': cluster_data['request_volume']['total'] / 1000000,
            'Daily Average (Millions)': cluster_data['request_volume']['daily_avg'] / 1000000,
            'Hourly Average (Millions)': cluster_data['request_volume']['hourly_avg'] / 1000000
        })
    
    volume_df = pd.DataFrame(volume_data)
    
    # Create a plotly figure for request volume comparison
    fig = make_subplots(rows=1, cols=3, 
                        subplot_titles=('Total Requests', 'Daily Average', 'Hourly Average'),
                        specs=[[{'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}]])
    
    fig.add_trace(
        go.Bar(x=volume_df['Cluster'], y=volume_df['Total Requests (Millions)'], name='Total Requests'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=volume_df['Cluster'], y=volume_df['Daily Average (Millions)'], name='Daily Average'),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Bar(x=volume_df['Cluster'], y=volume_df['Hourly Average (Millions)'], name='Hourly Average'),
        row=1, col=3
    )
    
    fig.update_layout(
        title_text='Request Volume Comparison Across Clusters (in Millions)',
        showlegend=False,
        template='plotly_white',
        height=500,
        width=1200
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/request_volume_comparison.png', scale=2)
    return fig

# 6. Request Variability Analysis
def create_request_variability_analysis():
    # Create a DataFrame for request variability
    variability_data = []
    for cluster_name, cluster_data in clusters_temporal.items():
        variability_data.append({
            'Cluster': cluster_name,
            'Hourly Standard Deviation (Millions)': cluster_data['request_volume']['hourly_std'] / 1000000,
            'Coefficient of Variation': cluster_data['request_volume']['hourly_std'] / cluster_data['request_volume']['hourly_avg'],
            'Max/Min Ratio': cluster_data['request_volume']['hourly_max'] / cluster_data['request_volume']['hourly_min']
        })
    
    variability_df = pd.DataFrame(variability_data)
    
    # Create a plotly figure for request variability analysis
    fig = make_subplots(rows=1, cols=2, 
                        subplot_titles=('Coefficient of Variation', 'Max/Min Ratio'),
                        specs=[[{'type': 'bar'}, {'type': 'bar'}]])
    
    fig.add_trace(
        go.Bar(x=variability_df['Cluster'], y=variability_df['Coefficient of Variation'], name='Coefficient of Variation'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=variability_df['Cluster'], y=variability_df['Max/Min Ratio'], name='Max/Min Ratio'),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text='Request Variability Analysis Across Clusters',
        showlegend=False,
        template='plotly_white',
        height=500,
        width=1000
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/request_variability_analysis.png', scale=2)
    return fig

# Generate all visualizations
hourly_fig = create_hourly_request_distribution()
daily_fig = create_daily_request_distribution()
peak_fig = create_peak_period_analysis()
peak_hour_fig = create_peak_periods_by_hour()
volume_fig = create_request_volume_comparison()
variability_fig = create_request_variability_analysis()

print("Temporal analysis visualizations created successfully!")
