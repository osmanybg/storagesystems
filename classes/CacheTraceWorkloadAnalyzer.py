import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json
import numpy as np
from datetime import datetime
import os
from pathlib import Path


class CacheTraceWorkloadAnalyzer:
    """
    A class for analyzing cache workload composition from large trace datasets.
    Processes parquet files one by one to handle large datasets efficiently.
    """
    
    def __init__(self, data_dir, output_dir="./results"):
        """
        Initialize the analyzer with data and output directories.
        
        Args:
            data_dir (str): Directory containing parquet files
            output_dir (str): Directory to save analysis results
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Aggregated statistics
        self.operation_counts = pd.Series(dtype='int64')
        self.key_size_stats = {}
        self.value_size_stats = {}
        self.ttl_stats = {}
        self.client_ops = pd.DataFrame()
        self.daily_ops = pd.DataFrame()
        self.hourly_ops = pd.DataFrame()
        self.total_records = 0
        
        # Settings
        self.sample_limit = None  # Set to int value for debugging with sample
    
    def process_files(self):
        """Process all parquet files in the data directory."""
        print(f"Looking for parquet files in {self.data_dir}")
        parquet_files = list(self.data_dir.glob("*.parquet"))
        print(f"Found {len(parquet_files)} parquet files")
        
        for i, file_path in enumerate(parquet_files):
            print(f"Processing file {i+1}/{len(parquet_files)}: {file_path.name}")
            self._process_single_file(file_path)
        
        print(f"Completed processing {len(parquet_files)} files with {self.total_records} total records")
    
    def _process_single_file(self, file_path):
        """
        Process a single parquet file and update aggregated statistics.
        
        Args:
            file_path (Path): Path to parquet file
        """
        # Load data
        print(f"  Loading data from {file_path.name}...")
        df = pd.read_parquet(file_path)
        
        # Sample for debugging if needed
        if self.sample_limit:
            df = df.sample(min(self.sample_limit, len(df)))
        
        # Ensure datetime columns are properly formatted
        for col in ['datetime', 'day', 'hour']:
            if col in df.columns and df[col].dtype != 'datetime64[ns]':
                df[col] = pd.to_datetime(df[col])
        
        self.total_records += len(df)
        print(f"  Processing {len(df)} records...")
        
        # Update operation counts
        file_op_counts = df['operation'].value_counts()
        self.operation_counts = self.operation_counts.add(file_op_counts, fill_value=0)
        
        # Update daily operation counts
        if 'day' in df.columns:
            file_daily_ops = df.groupby([pd.Grouper(key='day'), 'operation']).size().unstack(fill_value=0)
            self.daily_ops = pd.concat([self.daily_ops, file_daily_ops]).groupby(level=0).sum()
        
        # Update hourly operation counts
        if 'hour' in df.columns:
            file_hourly_ops = df.groupby([pd.Grouper(key='hour'), 'operation']).size().unstack(fill_value=0)
            self.hourly_ops = pd.concat([self.hourly_ops, file_hourly_ops]).groupby(level=0).sum()
        
        # Update key size, value size, and TTL statistics by operation
        self._update_size_statistics(df)
        
        # Update client operations
        self._update_client_statistics(df)
        
        print(f"  Completed processing {file_path.name}")
    
    def _update_size_statistics(self, df):
        """Update key size, value size, and TTL statistics from dataframe."""
        operations = df['operation'].unique()
        
        for op in operations:
            op_df = df[df['operation'] == op]
            
            # Key sizes
            key_stats = op_df['key_size'].agg(['count', 'sum', 'min', 'max'])
            if op in self.key_size_stats:
                existing = self.key_size_stats[op]
                self.key_size_stats[op] = {
                    'count': existing['count'] + key_stats['count'],
                    'sum': existing['sum'] + key_stats['sum'],
                    'min': min(existing['min'], key_stats['min']),
                    'max': max(existing['max'], key_stats['max'])
                }
            else:
                self.key_size_stats[op] = key_stats.to_dict()
            
            # Value sizes (only for operations that have values)
            if op in ['set', 'add', 'replace']:
                value_stats = op_df['value_size'].agg(['count', 'sum', 'min', 'max'])
                if op in self.value_size_stats:
                    existing = self.value_size_stats[op]
                    self.value_size_stats[op] = {
                        'count': existing['count'] + value_stats['count'],  
                        'sum': existing['sum'] + value_stats['sum'],
                        'min': min(existing['min'], value_stats['min']),
                        'max': max(existing['max'], value_stats['max'])
                    }
                else:
                    self.value_size_stats[op] = value_stats.to_dict()
            
            # TTL statistics (only for operations that set TTL)
            if op in ['set', 'add', 'replace'] and 'ttl' in op_df.columns:
                ttl_stats = op_df['ttl'].agg(['count', 'sum', 'min', 'max'])
                if op in self.ttl_stats:
                    existing = self.ttl_stats[op]
                    self.ttl_stats[op] = {
                        'count': existing['count'] + ttl_stats['count'],
                        'sum': existing['sum'] + ttl_stats['sum'],
                        'min': min(existing['min'], ttl_stats['min']),
                        'max': max(existing['max'], ttl_stats['max'])
                    }
                else:
                    self.ttl_stats[op] = ttl_stats.to_dict()
    
    def _update_client_statistics(self, df):
        """Update client operation statistics from dataframe."""
        if 'client_id' in df.columns:
            # Get client operations by type
            file_client_ops = df.groupby('client_id')['operation'].value_counts().unstack(fill_value=0)
            
            # Merge with existing client operations
            if self.client_ops.empty:
                self.client_ops = file_client_ops
            else:
                self.client_ops = pd.concat([self.client_ops, file_client_ops]).groupby(level=0).sum()
    
    def generate_statistics(self):
        """Calculate derived statistics from aggregated data."""
        # Operation percentages
        self.operation_percentages = (self.operation_counts / self.operation_counts.sum() * 100).round(2)
        
        # Daily operation percentages
        if not self.daily_ops.empty:
            self.daily_ops_pct = self.daily_ops.div(self.daily_ops.sum(axis=1), axis=0) * 100
        
        # Hourly operation percentages
        if not self.hourly_ops.empty:
            self.hourly_ops_pct = self.hourly_ops.div(self.hourly_ops.sum(axis=1), axis=0) * 100
        
        # Calculate mean values for key_size, value_size, ttl
        for op, stats in self.key_size_stats.items():
            if stats['count'] > 0:
                stats['mean'] = stats['sum'] / stats['count']
            else:
                stats['mean'] = 0
                
        for op, stats in self.value_size_stats.items():
            if stats['count'] > 0:
                stats['mean'] = stats['sum'] / stats['count']  
            else:
                stats['mean'] = 0
                
        for op, stats in self.ttl_stats.items():
            if stats['count'] > 0:
                stats['mean'] = stats['sum'] / stats['count']
            else:
                stats['mean'] = 0
        
        # Client statistics
        if not self.client_ops.empty:
            self.client_total_ops = self.client_ops.sum(axis=1).sort_values(ascending=False)
            self.top_clients = self.client_total_ops.head(10)
            self.client_ops_pct = self.client_ops.div(self.client_ops.sum(axis=1), axis=0) * 100

    def save_results(self, filename="workload_analysis_output.json"):
        """
        Save the analysis results in JSON format.

        Args:
            filename (str): Name of the output JSON file.
        """

        def convert_keys(obj):
            """Recursively convert dictionary keys to strings."""
            if isinstance(obj, dict):
                return {str(k): convert_keys(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_keys(i) for i in obj]
            else:
                return obj

        results = {
            "total_records": self.total_records,
            "operation_counts": convert_keys(self.operation_counts.to_dict()),
            "operation_percentages": convert_keys(self.operation_percentages.to_dict()) if hasattr(self, "operation_percentages") else {},
            "key_size_stats": convert_keys(self.key_size_stats),
            "value_size_stats": convert_keys(self.value_size_stats),
            "ttl_stats": convert_keys(self.ttl_stats),
            "daily_operations": convert_keys(self.daily_ops.to_dict()) if not self.daily_ops.empty else {},
            "hourly_operations": convert_keys(self.hourly_ops.to_dict()) if not self.hourly_ops.empty else {},
            # "client_operations": convert_keys(self.client_ops.to_dict()) if not self.client_ops.empty else {},
            "top_clients": convert_keys(self.top_clients.to_dict()) if hasattr(self, "top_clients") else {}
        }

        data_dir = self.output_dir / "data"
        data_dir.mkdir(exist_ok=True)

        output_path = data_dir / filename
        with open(output_path, "w") as f:
            json.dump(results, f, indent=4, default=str)

        print(f"Results saved to {output_path}")

    def visualize_results(self):
        """Generate visualizations from the aggregated statistics."""
        print("Generating visualizations...")
        
        # Create visualizations directory
        vis_dir = self.output_dir / "visualizations"
        vis_dir.mkdir(exist_ok=True)
        
        # 1. Operation Distribution Visualizations
        self._visualize_operation_distribution(vis_dir)
        
        # 2. Temporal Analysis Visualizations
        self._visualize_temporal_patterns(vis_dir)
        
        # 3. Client Analysis Visualizations
        if not self.client_ops.empty:
            self._visualize_client_behavior(vis_dir)
        
        print(f"Visualizations saved to {vis_dir}")
    
    def _visualize_operation_distribution(self, vis_dir):
        """Create visualizations for operation distribution."""
        # Overall operation distribution
        plt.figure(figsize=(15, 7))
        
        # Pie chart
        plt.subplot(1, 2, 1)
        self.operation_counts.plot.pie(autopct='%1.1f%%', startangle=90, fontsize=10)
        plt.title('Overall Distribution of Cache Operations', fontsize=14)
        plt.ylabel('')
        
        # Bar chart
        plt.subplot(1, 2, 2)
        sns.barplot(x=self.operation_percentages.index, y=self.operation_percentages.values)
        plt.title('Percentage of Operation Types', fontsize=14)
        plt.ylabel('Percentage (%)')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(vis_dir / 'operation_distribution.png', dpi=300)
        plt.close()
        
        # Get/Set ratio if applicable
        if 'get' in self.operation_counts.index and 'set' in self.operation_counts.index:
            plt.figure(figsize=(10, 6))
            labels = ['GET', 'SET', 'Other']
            get_pct = self.operation_percentages.get('get', 0)
            set_pct = self.operation_percentages.get('set', 0)
            other_pct = 100 - get_pct - set_pct
            sizes = [get_pct, set_pct, other_pct]
            colors = ['#ff9999', '#66b3ff', '#c2c2f0']
            plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
            plt.axis('equal')
            plt.title('Read vs. Write Operations', fontsize=14)
            plt.savefig(vis_dir / 'read_write_ratio.png', dpi=300)
            plt.close()
    
    def _visualize_temporal_patterns(self, vis_dir):
        """Create visualizations for temporal patterns."""
        if not self.daily_ops.empty:
            # Daily operation distribution
            plt.figure(figsize=(15, 8))
            self.daily_ops_pct.plot.area(alpha=0.7)
            plt.title('Daily Operation Distribution (Percentage)', fontsize=14)
            plt.ylabel('Percentage (%)')
            plt.xlabel('Date')
            plt.legend(title='Operation Type')
            plt.grid(True, alpha=0.3)
            plt.savefig(vis_dir / 'daily_operation_distribution.png', dpi=300)
            plt.close()
            
            # Get/Set ratio over time
            if 'get' in self.daily_ops.columns and 'set' in self.daily_ops.columns:
                plt.figure(figsize=(15, 8))
                get_set_ratio = self.daily_ops['get'] / self.daily_ops['set'].replace(0, np.nan)
                get_set_ratio.plot(marker='o', linestyle='-')
                plt.title('Daily Get/Set Ratio', fontsize=14)
                plt.ylabel('Get/Set Ratio')
                plt.xlabel('Date')
                plt.grid(True, alpha=0.3)
                plt.savefig(vis_dir / 'daily_get_set_ratio.png', dpi=300)
                plt.close()
        
        if not self.hourly_ops.empty:
            # Convert hourly index to include day of week and hour information
            hourly_df = self.hourly_ops.copy()
            hourly_df.index = pd.to_datetime(hourly_df.index)
            
            # Extract day of week and hour
            hourly_df['day_of_week'] = hourly_df.index.dayofweek
            hourly_df['hour_of_day'] = hourly_df.index.hour
            
            # Choose the most frequent operation for the heatmap
            most_common_op = self.operation_counts.idxmax()
            
            if most_common_op in hourly_df.columns:
                # Create heatmap data
                heatmap_data = hourly_df.groupby(['day_of_week', 'hour_of_day'])[most_common_op].mean().unstack()
                
                plt.figure(figsize=(12, 8))
                sns.heatmap(heatmap_data, cmap='YlGnBu', cbar_kws={'label': f'Average {most_common_op} Operations'})
                plt.title(f'Weekly Pattern of {most_common_op} Operations', fontsize=14)
                plt.xlabel('Hour of Day')
                plt.ylabel('Day of Week (0=Monday, 6=Sunday)')
                plt.savefig(vis_dir / 'weekly_operation_pattern.png', dpi=300)
                plt.close()
    
    def _visualize_client_behavior(self, vis_dir):
        """Create visualizations for client behavior."""
        # Distribution of total operations per client
        plt.figure(figsize=(12, 8))
        sns.histplot(self.client_total_ops, log_scale=(False, True))
        plt.title('Distribution of Total Operations per Client', fontsize=14)
        plt.xlabel('Number of Operations')
        plt.ylabel('Number of Clients')
        plt.savefig(vis_dir / 'client_operation_distribution.png', dpi=300)
        plt.close()
        
        # Operation distribution for top clients
        plt.figure(figsize=(15, 8))
        top_client_ops_pct = self.client_ops_pct.loc[self.top_clients.index]
        top_client_ops_pct.plot(kind='bar', stacked=True)
        plt.title('Operation Distribution for Top 10 Clients', fontsize=14)
        plt.xlabel('Client ID')
        plt.ylabel('Percentage of Operations')
        plt.legend(title='Operation Type')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(vis_dir / 'top_client_operations.png', dpi=300)
        plt.close()
        
        # Get/Set ratio per client
        if 'get' in self.client_ops.columns and 'set' in self.client_ops.columns:
            # Calculate get/set ratio for clients with at least 10 set operations
            valid_clients = self.client_ops[(self.client_ops['set'] >= 10)].copy()
            valid_clients['get_set_ratio'] = valid_clients['get'] / valid_clients['set']
            
            plt.figure(figsize=(12, 8))
            sns.histplot(valid_clients['get_set_ratio'], kde=True)
            plt.title('Distribution of Get/Set Ratio Across Clients', fontsize=14)
            plt.xlabel('Get/Set Ratio')
            plt.ylabel('Number of Clients')
            plt.xlim(0, valid_clients['get_set_ratio'].quantile(0.95))  # Limit x-axis to avoid outliers
            plt.savefig(vis_dir / 'client_get_set_ratio.png', dpi=300)
            plt.close()
    
    def generate_report(self):
        """Generate a comprehensive report from the analysis."""
        print("Generating summary report...")
        
        report_path = self.output_dir / 'workload_composition_report.md'
        with open(report_path, 'w') as f:
            f.write("# Cache Workload Composition Analysis\n\n")
            f.write(f"Analysis generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total records analyzed: {self.total_records:,}\n\n")
            
            f.write("## 1. Overall Operation Distribution\n\n")
            f.write("### Operation Counts\n")
            f.write("```\n")
            f.write(self.operation_counts.to_string())
            f.write("\n```\n\n")
            
            f.write("### Operation Percentages\n")
            f.write("```\n")
            f.write(self.operation_percentages.to_string())
            f.write("\n```\n\n")
            
            f.write("## 2. Key and Value Size Statistics by Operation\n\n")
            f.write("### Key Size Statistics\n")
            key_stats_df = pd.DataFrame.from_dict(self.key_size_stats, orient='index')
            f.write("```\n")
            f.write(key_stats_df.to_string())
            f.write("\n```\n\n")
            
            f.write("### Value Size Statistics\n")
            if self.value_size_stats:
                value_stats_df = pd.DataFrame.from_dict(self.value_size_stats, orient='index')
                f.write("```\n")
                f.write(value_stats_df.to_string())
                f.write("\n```\n\n")
            else:
                f.write("No value size statistics available.\n\n")
            
            f.write("### TTL Statistics\n")
            if self.ttl_stats:
                ttl_stats_df = pd.DataFrame.from_dict(self.ttl_stats, orient='index')
                f.write("```\n")
                f.write(ttl_stats_df.to_string())
                f.write("\n```\n\n")
            else:
                f.write("No TTL statistics available.\n\n")
            
            f.write("## 3. Client Analysis\n\n")
            if not self.client_ops.empty:
                f.write(f"Total unique clients: {len(self.client_ops)}\n\n")
                
                f.write("### Top 10 Most Active Clients\n")
                f.write("```\n")
                f.write(self.top_clients.to_string())
                f.write("\n```\n\n")
            else:
                f.write("No client data available.\n\n")
            
            if 'get' in self.operation_counts.index and 'set' in self.operation_counts.index:
                overall_get_set = self.operation_counts['get'] / self.operation_counts['set']
                f.write(f"### Overall Get/Set Ratio: {overall_get_set:.2f}\n\n")
            
            f.write("## 4. Key Insights\n\n")
            f.write("### Workload Composition\n")
            most_common_op = self.operation_counts.idxmax()
            most_common_pct = self.operation_percentages.max()
            f.write(f"- The most common operation is '{most_common_op}' ({most_common_pct:.2f}% of all operations)\n")
            
            if 'get' in self.operation_counts.index and 'set' in self.operation_counts.index:
                get_pct = self.operation_percentages['get']
                set_pct = self.operation_percentages['set']
                f.write(f"- Read operations ('get') account for {get_pct:.2f}% of all operations\n")
                f.write(f"- Write operations ('set') account for {set_pct:.2f}% of all operations\n")
                f.write(f"- The overall read/write ratio is {get_pct/set_pct:.2f}\n")
            
            f.write("\n### Operation Characteristics\n")
            # Add insights about key and value sizes
            for op in self.operation_counts.index:
                if op in self.key_size_stats:
                    key_avg = self.key_size_stats[op]['mean']
                    f.write(f"- Average key size for '{op}' operations: {key_avg:.2f} bytes\n")
                
                if op in self.value_size_stats and op in ['set', 'add', 'replace']:
                    val_avg = self.value_size_stats[op]['mean']
                    f.write(f"- Average value size for '{op}' operations: {val_avg:.2f} bytes\n")
            
            if not self.client_ops.empty:
                f.write("\n### Client Behavior\n")
                top_client = self.top_clients.index[0]
                top_client_ops = self.top_clients.values[0]
                
                f.write(f"- The most active client (ID: {top_client}) performed {top_client_ops} operations\n")
                f.write(f"- The top 10 clients account for {self.top_clients.sum() / self.operation_counts.sum() * 100:.2f}% of all operations\n")
            
            f.write("\n## 5. Recommendations for Cache Optimization\n\n")
            f.write("Based on the analysis, consider the following optimizations:\n\n")
            
            if 'get' in self.operation_counts.index and 'set' in self.operation_counts.index:
                get_set_ratio = self.operation_counts['get'] / self.operation_counts['set']
                if get_set_ratio > 5:
                    f.write("- The workload is heavily read-oriented. Consider increasing cache size and implementing read-optimized eviction policies.\n")
                elif get_set_ratio < 1:
                    f.write("- The workload is write-heavy. Consider implementing write-back policies and optimizing for rapid updates.\n")
                else:
                    f.write("- The workload has a balanced read/write ratio. A general-purpose caching strategy should work well.\n")
            
            # Add more recommendations based on the analysis
            f.write("- Consider adjusting TTL values based on operation patterns to optimize cache efficiency.\n")
            f.write("- Monitor the behavior of top clients as they have a significant impact on overall cache performance.\n")
            f.write("- Analyze temporal patterns to identify if cache sizing should be adjusted during peak periods.\n")
            
        print(f"Report generated and saved to {report_path}")
    
    def run_analysis(self):
        """Run the complete analysis workflow."""
        print("Starting cache workload composition analysis...")
        self.process_files()
        self.generate_statistics()
        self.save_results()
        self.visualize_results()
        self.generate_report()
        print("Analysis completed successfully!")


def main():
    """Main function to run the analyzer."""
    # Example usage - modify data_directory with actual path
    data_directory = "/path/to/cache/trace/data"
    output_directory = "./cache_analysis_results"
    
    # For debugging with a small sample
    # analyzer = CacheWorkloadAnalyzer(data_directory, output_directory)
    # analyzer.sample_limit = 10000  # Set to None for full analysis
    
    analyzer = CacheTraceWorkloadAnalyzer(data_directory, output_directory)
    analyzer.run_analysis()


if __name__ == "__main__":
    main()