#!/usr/bin/env python3
"""
Script to copy analysis output files from 01.dataset_analysis to 02.result_analysis/data
This script copies:
1. summary_statistics.json files from temporal_analysis to the data directory
2. workload_analysis_output.json files from workload_composition to the data directory
"""

import os
import shutil
import json

# Define the clusters to process
clusters = [1, 2, 3, 10]

# Define the base directories
base_dir = os.path.dirname(os.path.abspath(__file__))
source_base = os.path.join(base_dir, '..', '01.dataset_analysis', 'analysis_output')
target_dir = os.path.join(base_dir, 'data')

# Create target directory if it doesn't exist
os.makedirs(target_dir, exist_ok=True)

def copy_files():
    """Copy the analysis output files to the result analysis data directory"""
    
    # Process each cluster
    for cluster in clusters:
        # 1. Copy temporal analysis summary statistics
        source_temporal = os.path.join(
            source_base, 
            'temporal_analysis', 
            f'cluster{cluster}', 
            'data', 
            'summary_statistics.json'
        )
        target_temporal = os.path.join(
            target_dir, 
            f'cluster{cluster}_temporal_analysis_output.json'
        )
        
        # 2. Copy workload composition analysis output
        source_workload = os.path.join(
            source_base, 
            'workload_composition', 
            f'cluster{cluster}', 
            'data', 
            'workload_analysis_output.json'
        )
        target_workload = os.path.join(
            target_dir, 
            f'cluster{cluster}_workload_analysis_output.json'
        )
        
        # Copy the files if they exist
        try:
            if os.path.exists(source_temporal):
                print(f"Copying {source_temporal} to {target_temporal}")
                shutil.copy2(source_temporal, target_temporal)
            else:
                print(f"Warning: Source file not found: {source_temporal}")
                
            if os.path.exists(source_workload):
                print(f"Copying {source_workload} to {target_workload}")
                shutil.copy2(source_workload, target_workload)
            else:
                print(f"Warning: Source file not found: {source_workload}")
                
        except Exception as e:
            print(f"Error copying files for cluster {cluster}: {str(e)}")

if __name__ == "__main__":
    print("Starting file copy process...")
    copy_files()
    print("File copy process completed.")
