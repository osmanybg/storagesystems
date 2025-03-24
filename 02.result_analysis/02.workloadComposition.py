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
cluster1_workload = json.load(open(f'{inputDir}/cluster1_workload_analysis_output.json'))
cluster2_workload = json.load(open(f'{inputDir}/cluster2_workload_analysis_output.json'))
cluster3_workload = json.load(open(f'{inputDir}/cluster3_workload_analysis_output.json'))
cluster10_workload = json.load(open(f'{inputDir}/cluster10_workload_analysis_output.json'))

# Create a dictionary to store the data for easier access
clusters_workload = {
    'Cluster 1': cluster1_workload,
    'Cluster 2': cluster2_workload,
    'Cluster 3': cluster3_workload,
    'Cluster 10': cluster10_workload
}

# 1. Operation Type Distribution Visualization
def create_operation_type_distribution():
    # Create a DataFrame for operation percentages
    operation_data = []
    for cluster_name, cluster_data in clusters_workload.items():
        for op_type, percentage in cluster_data['operation_percentages'].items():
            operation_data.append({
                'Cluster': cluster_name,
                'Operation Type': op_type,
                'Percentage': percentage
            })
    
    operation_df = pd.DataFrame(operation_data)
    
    # Create a plotly figure for operation type distribution
    fig = px.bar(operation_df, x='Cluster', y='Percentage', color='Operation Type', barmode='stack',
                title='Operation Type Distribution Across Clusters',
                labels={'Percentage': 'Percentage of Total Operations (%)'})
    
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template='plotly_white'
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/operation_type_distribution.png', scale=2)
    return fig

# 2. Key Size Distribution Visualization
def create_key_size_distribution():
    # Create a DataFrame for key size statistics
    key_size_data = []
    for cluster_name, cluster_data in clusters_workload.items():
        for op_type, stats in cluster_data['key_size_stats'].items():
            key_size_data.append({
                'Cluster': cluster_name,
                'Operation Type': op_type,
                'Mean Key Size (bytes)': stats['mean']
            })
    
    key_size_df = pd.DataFrame(key_size_data)
    
    # Create a plotly figure for key size distribution
    fig = px.bar(key_size_df, x='Cluster', y='Mean Key Size (bytes)', color='Operation Type', barmode='group',
                title='Mean Key Size by Operation Type Across Clusters',
                labels={'Mean Key Size (bytes)': 'Mean Key Size (bytes)'})
    
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template='plotly_white'
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/key_size_distribution.png', scale=2)
    return fig

# 3. Value Size Distribution for Add Operations
def create_value_size_distribution():
    # Create a DataFrame for value size statistics
    value_size_data = []
    for cluster_name, cluster_data in clusters_workload.items():
        if 'value_size_stats' in cluster_data and 'add' in cluster_data['value_size_stats']:
            stats = cluster_data['value_size_stats']['add']
            value_size_data.append({
                'Cluster': cluster_name,
                'Mean Value Size (bytes)': stats['mean'],
                'Min Value Size (bytes)': stats['min'],
                'Max Value Size (bytes)': stats['max']
            })
    
    value_size_df = pd.DataFrame(value_size_data)
    
    # Create a plotly figure for value size distribution
    fig = make_subplots(rows=1, cols=3, 
                        subplot_titles=('Mean Value Size', 'Min Value Size', 'Max Value Size'),
                        specs=[[{'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}]])
    
    fig.add_trace(
        go.Bar(x=value_size_df['Cluster'], y=value_size_df['Mean Value Size (bytes)'], name='Mean'),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(x=value_size_df['Cluster'], y=value_size_df['Min Value Size (bytes)'], name='Min'),
        row=1, col=2
    )
    
    fig.add_trace(
        go.Bar(x=value_size_df['Cluster'], y=value_size_df['Max Value Size (bytes)'], name='Max'),
        row=1, col=3
    )
    
    fig.update_layout(
        title_text='Value Size Statistics for Add Operations Across Clusters',
        showlegend=False,
        template='plotly_white',
        height=500,
        width=1200
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/value_size_distribution.png', scale=2)
    return fig

# 4. TTL Distribution for Add Operations
def create_ttl_distribution():
    # Create a DataFrame for TTL statistics
    ttl_data = []
    for cluster_name, cluster_data in clusters_workload.items():
        if 'ttl_stats' in cluster_data and 'add' in cluster_data['ttl_stats']:
            stats = cluster_data['ttl_stats']['add']
            ttl_data.append({
                'Cluster': cluster_name,
                'Mean TTL (seconds)': stats['mean'],
                'Min TTL (seconds)': stats['min'],
                'Max TTL (seconds)': stats['max']
            })
    
    ttl_df = pd.DataFrame(ttl_data)
    
    # Create a plotly figure for TTL distribution
    fig = px.bar(ttl_df, x='Cluster', y='Mean TTL (seconds)',
                title='Mean TTL for Add Operations Across Clusters',
                labels={'Mean TTL (seconds)': 'Mean TTL (seconds)'})
    
    fig.update_layout(
        template='plotly_white'
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/ttl_distribution.png', scale=2)
    return fig

# 5. Daily Operation Trends
def create_daily_operation_trends():
    # Create a DataFrame for daily operation trends
    daily_op_data = []
    
    for cluster_name, cluster_data in clusters_workload.items():
        for op_type, daily_counts in cluster_data['daily_operations'].items():
            for day, count in daily_counts.items():
                # Skip the last day (partial data)
                if day != '1970-01-08 00:00:00':
                    daily_op_data.append({
                        'Cluster': cluster_name,
                        'Operation Type': op_type,
                        'Day': day,
                        'Count': count
                    })
    
    daily_op_df = pd.DataFrame(daily_op_data)
    
    # Create a plotly figure for daily operation trends
    fig = px.line(daily_op_df, x='Day', y='Count', color='Operation Type', facet_col='Cluster',
                 title='Daily Operation Trends by Cluster',
                 labels={'Count': 'Number of Operations', 'Day': 'Day'})
    
    fig.update_layout(
        template='plotly_white',
        height=600,
        width=1200
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/daily_operation_trends.png', scale=2)
    return fig

# 6. Get vs Add Operations Ratio
def create_get_add_ratio():
    # Create a DataFrame for get vs add ratio
    ratio_data = []
    for cluster_name, cluster_data in clusters_workload.items():
        get_count = cluster_data['operation_counts'].get('get', 0)
        add_count = cluster_data['operation_counts'].get('add', 0)
        
        if add_count > 0:  # Avoid division by zero
            ratio = get_count / add_count
        else:
            ratio = 0
            
        ratio_data.append({
            'Cluster': cluster_name,
            'Get/Add Ratio': ratio
        })
    
    ratio_df = pd.DataFrame(ratio_data)
    
    # Create a plotly figure for get vs add ratio
    fig = px.bar(ratio_df, x='Cluster', y='Get/Add Ratio',
                title='Get to Add Operations Ratio Across Clusters',
                labels={'Get/Add Ratio': 'Get/Add Ratio'})
    
    fig.update_layout(
        template='plotly_white'
    )
    
    # Save the figure
    fig.write_image(f'{visualizationDir}/get_add_ratio.png', scale=2)
    return fig

# Generate all visualizations
operation_fig = create_operation_type_distribution()
key_size_fig = create_key_size_distribution()
value_size_fig = create_value_size_distribution()
ttl_fig = create_ttl_distribution()
daily_op_fig = create_daily_operation_trends()
ratio_fig = create_get_add_ratio()

print("Workload composition visualizations created successfully!")
