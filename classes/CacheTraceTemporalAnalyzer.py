import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import dask.dataframe as dd
from pathlib import Path
import matplotlib.dates as mdates
from statsmodels.tsa.seasonal import seasonal_decompose
import gc
import json
import warnings
warnings.filterwarnings('ignore')

class CacheTraceTemporalAnalyzer:
    """
    Analyzes temporal patterns in large cache trace datasets.
    Designed to handle datasets that don't fit in memory by processing in chunks.
    """
    
    def __init__(self, data_dir, output_dir, chunk_size=1_000_000):
        """
        Initialize the analyzer with data and output directories.
        
        Args:
            data_dir (str): Directory containing parquet files
            output_dir (str): Directory to save analysis outputs
            chunk_size (int): Number of rows to process at once
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.chunk_size = chunk_size
        
        # Create output directories if they don't exist
        os.makedirs(self.output_dir / "figures", exist_ok=True)
        os.makedirs(self.output_dir / "data", exist_ok=True)
        
        # Initialize data containers for aggregated metrics
        self.hourly_counts = {}
        self.daily_counts = {}
        self.hourly_op_counts = {}
        self.daily_op_counts = {}
        self.hour_of_day_counts = np.zeros(24)
        self.day_of_week_counts = np.zeros(7)
        self.operation_counts = {}
        self.peak_summary = {}
        
        # Date range tracking
        self.min_date = None
        self.max_date = None

    def process_files(self):
        """Process all parquet files in the data directory"""
        parquet_files = list(self.data_dir.glob("*.parquet"))
        total_files = len(parquet_files)
        
        print(f"Found {total_files} parquet files in {self.data_dir}")
        
        for i, file_path in enumerate(parquet_files):
            print(f"Processing file {i+1}/{total_files}: {file_path.name}")
            self._process_file(file_path)
            
        # Finalize aggregations
        self._finalize_aggregations()
        
    def _process_file(self, file_path):
        """
        Process a single parquet file in chunks
        
        Args:
            file_path (Path): Path to the parquet file
        """
        # Use dask to read the parquet file in chunks
        try:
            # First attempt: Try to process with dask
            print(f"  Reading {file_path} with dask...")
            ddf = dd.read_parquet(file_path)
            
            # Process in partitions
            partitions = max(1, ddf.npartitions)
            print(f"  Processing file in {partitions} partitions...")
            
            for i in range(partitions):
                df_chunk = ddf.get_partition(i).compute()
                print(f"  Processing partition {i+1}/{partitions} with {len(df_chunk)} rows")
                self._process_chunk(df_chunk)
                gc.collect()  # Force garbage collection
                
        except Exception as e:
            print(f"  Dask processing failed: {e}")
    
    def _process_chunk(self, df):
        """
        Process a chunk of the dataset
        
        Args:
            df (DataFrame): Pandas DataFrame chunk
        """
        # Track date range
        if self.min_date is None or df['datetime'].min() < self.min_date:
            self.min_date = df['datetime'].min()
        if self.max_date is None or df['datetime'].max() > self.max_date:
            self.max_date = df['datetime'].max()
        
        # Hourly counts
        hourly_series = df.groupby(df['datetime'].dt.floor('H')).size()
        for dt, count in hourly_series.items():
            self.hourly_counts[dt] = self.hourly_counts.get(dt, 0) + count
        
        # Daily counts
        daily_series = df.groupby(df['datetime'].dt.floor('D')).size()
        for dt, count in daily_series.items():
            self.daily_counts[dt] = self.daily_counts.get(dt, 0) + count
        

        # Hourly counts
        # df["day_of_week"] = df["datetime"].dt.day_name()
        # hourly_series = df.groupby(df['datetime'].dt.hour).size()
        df["hour_of_day"] = df["datetime"].dt.hour
        hourly_counts = df.groupby("hour_of_day").size().reset_index(name='count')

        for i, row in hourly_counts.iterrows():
            self.hour_of_day_counts[row['hour_of_day']] += row['count']
        
        # Day of week distribution
        df['day_of_week'] = df['datetime'].dt.dayofweek
        dow_counts = df.groupby('day_of_week').size().reset_index(name='count')

        for i, row in dow_counts.iterrows():
            self.day_of_week_counts[row['day_of_week']] += row['count']
        
        # # Day of week distribution
        # dow_counts = df.groupby(df['datetime'].dt.dayofweek).size().to_numpy()
        # for i, count in enumerate(dow_counts):
        #     if i < len(self.day_of_week_counts):
        #         self.day_of_week_counts[i] += count
        
        # Operation counts
        op_counts = df.groupby('operation').size()
        for op, count in op_counts.items():
            self.operation_counts[op] = self.operation_counts.get(op, 0) + count
        
        # Operation counts by hour
        hourly_op = df.groupby([df['datetime'].dt.floor('H'), 'operation']).size().reset_index(name='count')
        for _, row in hourly_op.iterrows():
            hour = row['datetime']
            op = row['operation']
            count = row['count']
            
            if hour not in self.hourly_op_counts:
                self.hourly_op_counts[hour] = {}
            
            self.hourly_op_counts[hour][op] = self.hourly_op_counts[hour].get(op, 0) + count
        
        # Operation counts by day
        daily_op = df.groupby([df['datetime'].dt.floor('D'), 'operation']).size().reset_index(name='count')
        for _, row in daily_op.iterrows():
            day = row['datetime']
            op = row['operation']
            count = row['count']
            
            if day not in self.daily_op_counts:
                self.daily_op_counts[day] = {}
            
            self.daily_op_counts[day][op] = self.daily_op_counts[day].get(op, 0) + count
    
    def _finalize_aggregations(self):
        """Convert aggregated data into DataFrames for analysis and visualization"""
        # Convert dictionaries to sorted DataFrames
        self.hourly_df = pd.DataFrame({
            'datetime': list(self.hourly_counts.keys()),
            'count': list(self.hourly_counts.values())
        }).sort_values('datetime').reset_index(drop=True)
        
        self.daily_df = pd.DataFrame({
            'date': list(self.daily_counts.keys()),
            'count': list(self.daily_counts.values())
        }).sort_values('date').reset_index(drop=True)
        
        # Create operation distribution DataFrame
        self.operation_df = pd.DataFrame({
            'operation': list(self.operation_counts.keys()),
            'count': list(self.operation_counts.values())
        }).sort_values('count', ascending=False).reset_index(drop=True)
        
        # Create hourly operation DataFrame
        hourly_op_data = []
        for dt, ops in self.hourly_op_counts.items():
            for op, count in ops.items():
                hourly_op_data.append({
                    'datetime': dt,
                    'operation': op,
                    'count': count
                })
        self.hourly_op_df = pd.DataFrame(hourly_op_data).sort_values('datetime').reset_index(drop=True)
        
        # Create daily operation DataFrame
        daily_op_data = []
        for dt, ops in self.daily_op_counts.items():
            for op, count in ops.items():
                daily_op_data.append({
                    'date': dt,
                    'operation': op,
                    'count': count
                })
        self.daily_op_df = pd.DataFrame(daily_op_data).sort_values('date').reset_index(drop=True)
    
    def generate_visualizations(self):
        """Generate all temporal analysis visualizations"""
        print("Generating visualizations...")
        
        self._plot_request_volume_over_time()
        self._plot_hourly_distribution()
        self._plot_daily_distribution()
        self._plot_operation_distribution()
        self._plot_operation_trends()
        self._find_peak_periods()
        self._save_summary_statistics()
        
        print(f"All visualizations and data saved to {self.output_dir}")
    
    def _plot_request_volume_over_time(self):
        """Plot request volume over time (hourly and daily)"""
        # Hourly volume
        plt.figure(figsize=(14, 7))
        plt.plot(self.hourly_df['datetime'], self.hourly_df['count'])
        plt.title('Cache Request Volume (Hourly)')
        plt.xlabel('Time')
        plt.ylabel('Number of Requests')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(self.output_dir / "figures" / "hourly_volume.png", dpi=300)
        plt.close()
        
        # Daily volume
        plt.figure(figsize=(14, 7))
        plt.plot(self.daily_df['date'], self.daily_df['count'])
        plt.title('Cache Request Volume (Daily)')
        plt.xlabel('Date')
        plt.ylabel('Number of Requests')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(self.output_dir / "figures" / "daily_volume.png", dpi=300)
        plt.close()
        
        # Save the data
        self.hourly_df.to_csv(self.output_dir / "data" / "hourly_volume.csv", index=False)
        self.daily_df.to_csv(self.output_dir / "data" / "daily_volume.csv", index=False)
        
        # Try to do seasonal decomposition if we have enough data
        if len(self.hourly_df) >= 24:
            try:
                # Resample to regular intervals if needed
                hourly_series = self.hourly_df.set_index('datetime')['count']
                hourly_series = hourly_series.resample('H').sum().fillna(0)
                
                # Only proceed if we have at least 2 full days of data
                if len(hourly_series) >= 48:
                    # Decompose the time series
                    decomposition = seasonal_decompose(hourly_series, model='additive', period=24)
                    
                    # Plot the decomposition
                    fig, axes = plt.subplots(4, 1, figsize=(14, 16))
                    decomposition.observed.plot(ax=axes[0])
                    axes[0].set_title('Observed')
                    axes[0].grid(True, alpha=0.3)
                    
                    decomposition.trend.plot(ax=axes[1])
                    axes[1].set_title('Trend')
                    axes[1].grid(True, alpha=0.3)
                    
                    decomposition.seasonal.plot(ax=axes[2])
                    axes[2].set_title('Seasonality')
                    axes[2].grid(True, alpha=0.3)
                    
                    decomposition.resid.plot(ax=axes[3])
                    axes[3].set_title('Residuals')
                    axes[3].grid(True, alpha=0.3)
                    
                    plt.tight_layout()
                    plt.savefig(self.output_dir / "figures" / "time_series_decomposition.png", dpi=300)
                    plt.close()
            except Exception as e:
                print(f"Warning: Could not perform time series decomposition: {e}")
    
    def _plot_hourly_distribution(self):
        """Plot request distribution by hour of day"""
        plt.figure(figsize=(12, 6))
        sns.barplot(x=np.arange(24), y=self.hour_of_day_counts)
        plt.title('Request Distribution by Hour of Day')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Requests')
        plt.xticks(np.arange(24))
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(self.output_dir / "figures" / "hour_of_day_distribution.png", dpi=300)
        plt.close()
        
        # Save the data
        pd.DataFrame({
            'hour': np.arange(24),
            'count': self.hour_of_day_counts
        }).to_csv(self.output_dir / "data" / "hour_of_day_distribution.csv", index=False)
    
    def _plot_daily_distribution(self):
        """Plot request distribution by day of week"""
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        plt.figure(figsize=(10, 6))
        sns.barplot(x=days, y=self.day_of_week_counts)
        plt.title('Request Distribution by Day of Week')
        plt.xlabel('Day of Week')
        plt.ylabel('Number of Requests')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(self.output_dir / "figures" / "day_of_week_distribution.png", dpi=300)
        plt.close()
        
        # Save the data
        pd.DataFrame({
            'day': days,
            'count': self.day_of_week_counts
        }).to_csv(self.output_dir/"data"/"day_of_week_distribution.csv", index=False)
    
    def _plot_operation_distribution(self):
        """Plot overall operation type distribution"""
        if len(self.operation_df) == 0:
            print("Warning: No operation data available to plot")
            return
            
        plt.figure(figsize=(10, 6))
        sns.barplot(x='operation', y='count', data=self.operation_df)
        plt.title('Distribution of Operation Types')
        plt.xlabel('Operation Type')
        plt.ylabel('Number of Requests')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(self.output_dir / "figures" / "operation_distribution.png", dpi=300)
        plt.close()
        
        # Save the data
        self.operation_df.to_csv(self.output_dir / "data" / "operation_distribution.csv", index=False)
        
        # Calculate percentages
        total = self.operation_df['count'].sum()
        self.operation_df['percentage'] = (self.operation_df['count'] / total * 100).round(2)
        
        # Pie chart
        plt.figure(figsize=(10, 8))
        plt.pie(
            self.operation_df['count'], 
            labels=self.operation_df['operation'],
            autopct='%1.1f%%',
            startangle=90,
            shadow=True
        )
        plt.axis('equal')
        plt.title('Operation Type Distribution')
        plt.tight_layout()
        plt.savefig(self.output_dir / "figures" / "operation_distribution_pie.png", dpi=300)
        plt.close()
    
    def _plot_operation_trends(self):
        """Plot operation type trends over time"""
        if len(self.daily_op_df) == 0:
            print("Warning: No daily operation data available to plot")
            return
            
        # Daily operation trends as stacked area chart
        try:
            pivot_df = self.daily_op_df.pivot_table(
                index='date', columns='operation', values='count', aggfunc='sum'
            ).fillna(0)
            
            plt.figure(figsize=(14, 7))
            pivot_df.plot.area(stacked=True, alpha=0.7, figsize=(14, 7))
            plt.title('Daily Distribution of Operations')
            plt.xlabel('Date')
            plt.ylabel('Number of Requests')
            plt.grid(True, alpha=0.3)
            plt.legend(title='Operation')
            plt.tight_layout()
            plt.savefig(self.output_dir / "figures" / "daily_operation_trends.png", dpi=300)
            plt.close()
            
            # Save the data
            pivot_df.reset_index().to_csv(self.output_dir / "data" / "daily_operation_trends.csv", index=False)
            
            # Calculate daily percentages
            daily_pct = pivot_df.div(pivot_df.sum(axis=1), axis=0) * 100
            
            plt.figure(figsize=(14, 7))
            daily_pct.plot.area(stacked=True, alpha=0.7, figsize=(14, 7))
            plt.title('Daily Percentage of Operation Types')
            plt.xlabel('Date')
            plt.ylabel('Percentage (%)')
            plt.grid(True, alpha=0.3)
            plt.legend(title='Operation')
            plt.tight_layout()
            plt.savefig(self.output_dir / "figures" / "daily_operation_percentage.png", dpi=300)
            plt.close()
            
            # Save percentage data
            daily_pct.reset_index().to_csv(self.output_dir / "data" / "daily_operation_percentage.csv", index=False)
        except Exception as e:
            print(f"Error in plotting daily operation trends: {e}")
    
    def _find_peak_periods(self):
        """Identify and analyze peak usage periods"""
        if len(self.hourly_df) == 0:
            print("Warning: No hourly data available for peak analysis")
            return
            
        try:
            # Calculate threshold for peak periods (top 10% of request volume)
            threshold = np.percentile(self.hourly_df['count'], 90)
            
            # Identify peak periods
            peak_periods = self.hourly_df[self.hourly_df['count'] >= threshold].copy()
            peak_periods['is_peak'] = 'Peak'
            
            # Non-peak periods
            non_peak_periods = self.hourly_df[self.hourly_df['count'] < threshold].copy()
            non_peak_periods['is_peak'] = 'Non-Peak'
            
            # Combine datasets
            combined = pd.concat([peak_periods, non_peak_periods]).sort_values('datetime')
            
            # Plot peaks
            plt.figure(figsize=(14, 7))
            sns.scatterplot(
                data=combined,
                x='datetime',
                y='count',
                hue='is_peak',
                palette={'Peak': 'red', 'Non-Peak': 'blue'},
                alpha=0.7,
                s=50
            )
            plt.axhline(y=threshold, color='r', linestyle='--', label=f'Peak Threshold ({threshold:.0f} requests)')
            plt.title('Peak Request Periods')
            plt.xlabel('Time')
            plt.ylabel('Number of Requests')
            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.tight_layout()
            plt.savefig(self.output_dir / "figures" / "peak_periods.png", dpi=300)
            plt.close()
            
            # Save peak periods data
            peak_periods.to_csv(self.output_dir / "data" / "peak_periods.csv", index=False)
            
            # Create a summary of peak periods
            self.peak_summary = {
                "threshold": float(threshold),
                "num_peak_periods": len(peak_periods),
                "percentage_of_peak_periods": (len(peak_periods) / len(self.hourly_df) * 100),
                "avg_request_volume_peak": float(peak_periods['count'].mean()),
                "avg_request_volume_non_peak": float(non_peak_periods['count'].mean()),
                "ratio_peak_to_non_peak": float(peak_periods['count'].mean() / non_peak_periods['count'].mean()),
            }
            
            # Extract peak periods by day of week
            peak_periods['day_of_week'] = peak_periods['datetime'].dt.day_name()
            peak_by_day = peak_periods.groupby('day_of_week').size().to_dict()
            self.peak_summary["peak_periods_by_day"] = peak_by_day
            
            # Extract peak periods by hour of day
            peak_periods['hour_of_day'] = peak_periods['datetime'].dt.hour
            peak_by_hour = peak_periods.groupby('hour_of_day').size().to_dict()
            # Convert int keys to strings for JSON
            peak_by_hour = {str(k): v for k, v in peak_by_hour.items()}
            self.peak_summary["peak_periods_by_hour"] = peak_by_hour
            
            # Save the summary
            # with open(self.output_dir / "data" / "peak_periods_summary.json", 'w') as f:
            #     json.dump(self.peak_summary, f, indent=2)
                
            # Plot peak distribution by hour
            plt.figure(figsize=(12, 6))
            hour_counts = peak_periods.groupby('hour_of_day').size()
            sns.barplot(x=hour_counts.index, y=hour_counts.values)
            plt.title('Distribution of Peak Periods by Hour of Day')
            plt.xlabel('Hour of Day')
            plt.ylabel('Number of Peak Periods')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(self.output_dir / "figures" / "peak_periods_by_hour.png", dpi=300)
            plt.close()
            
            # Extract operation distribution during peak periods
            if len(self.hourly_op_df) > 0:
                # Merge with peak periods
                hourly_op_with_peak = self.hourly_op_df.merge(
                    combined[['datetime', 'is_peak']], 
                    on='datetime', 
                    how='left'
                )
                
                # Aggregate by operation and peak status
                op_by_peak = hourly_op_with_peak.groupby(['operation', 'is_peak'])['count'].sum().reset_index()
                
                # Plot operation distribution during peak vs non-peak
                plt.figure(figsize=(12, 6))
                sns.barplot(x='operation', y='count', hue='is_peak', data=op_by_peak)
                plt.title('Operation Types during Peak vs Non-Peak Periods')
                plt.xlabel('Operation Type')
                plt.ylabel('Number of Requests')
                plt.xticks(rotation=45)
                plt.grid(True, alpha=0.3)
                plt.tight_layout()
                plt.savefig(self.output_dir / "figures" / "peak_vs_non_peak_operations.png", dpi=300)
                plt.close()
                
                # Calculate percentages within each group
                peak_ops = op_by_peak[op_by_peak['is_peak'] == 'Peak'].copy()
                peak_ops['percentage'] = peak_ops['count'] / peak_ops['count'].sum() * 100
                
                non_peak_ops = op_by_peak[op_by_peak['is_peak'] == 'Non-Peak'].copy()
                non_peak_ops['percentage'] = non_peak_ops['count'] / non_peak_ops['count'].sum() * 100
                
                # Save the data
                pd.concat([peak_ops, non_peak_ops]).to_csv(
                    self.output_dir / "data" / "peak_vs_non_peak_operations.csv", 
                    index=False
                )
        except Exception as e:
            print(f"Error in finding peak periods: {e}")
    
    def _save_summary_statistics(self):
        """Save summary statistics for the entire dataset"""
        try:
            summary = {
                "date_range": {
                    "start": str(self.min_date),
                    "end": str(self.max_date),
                    "days": (self.max_date - self.min_date).days if self.max_date and self.min_date else None
                },
                "request_volume": {
                    "total": int(sum(self.hourly_counts.values())),
                    "daily_avg": float(np.mean(list(self.daily_counts.values()))),
                    "hourly_avg": float(np.mean(list(self.hourly_counts.values()))),
                    "hourly_min": float(np.min(list(self.hourly_counts.values()))) if self.hourly_counts else None,
                    "hourly_max": float(np.max(list(self.hourly_counts.values()))) if self.hourly_counts else None,
                    "hourly_median": float(np.median(list(self.hourly_counts.values()))) if self.hourly_counts else None,
                    "hourly_std": float(np.std(list(self.hourly_counts.values()))) if self.hourly_counts else None
                },
                "operations": {
                    "total": sum(self.operation_counts.values()),
                    "types": len(self.operation_counts),
                    "distribution": {op: count for op, count in self.operation_counts.items()}
                },
                "request_time_patterns": {
                    "hour_of_day": {str(i): float(count) for i, count in enumerate(self.hour_of_day_counts)},
                    "day_of_week": dict(zip(
                        ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                        [float(x) for x in self.day_of_week_counts]
                    ))
                },
                "peak_periods": self.peak_summary
            }
            
            # # Save summary to JSON
            # with open(self.output_dir / "data" / "summary_statistics.json", 'w') as f:
            #     json.dump(summary, f, indent=2)
                
            # print("Summary statistics saved to", self.output_dir / "data" / "summary_statistics.json")

            # Save summary to JSON
            with open(self.output_dir / "data" / "temporal_analysis_output.json", 'w') as f:
                json.dump(summary, f, indent=2)
                
            print("Summary statistics saved to", self.output_dir / "data" / "temporal_analysis_output.json")

        except Exception as e:
            print(f"Error in saving summary statistics: {e}")
