import sys
import os.path
from classes.CacheTraceTemporalAnalyzer import CacheTraceTemporalAnalyzer

def process(fileName):
    dirPath = os.path.dirname(fileName)
    baseName = os.path.splitext(os.path.basename(fileName))[0]
    baseFileName = os.path.join(dirPath, baseName)

    # Configure paths to your data and output directories
    DATA_DIR = f"{baseFileName}.parquet"  # Change this to your data directory
    OUTPUT_DIR = f"analysis_output/temporal_analysis/{baseName}"            # Change this to your desired output directory

    # Create analyzer instance with appropriate chunk size
    # You may need to adjust chunk_size based on your available RAM
    analyzer = CacheTraceTemporalAnalyzer(
        data_dir=DATA_DIR,
        output_dir=OUTPUT_DIR,
        chunk_size=1_000_000  # Process 1 million rows at a time
    )

    # Process all parquet files
    analyzer.process_files()

    # Generate visualizations and save results
    analyzer.generate_visualizations()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No filename provided")
        print("Usage: python importWithDask.py <filename>")
    else:
        fileName = sys.argv[1]
        
        if fileName[-1] == '\\':
            fileName = fileName[0:-1]

        process(fileName)

        