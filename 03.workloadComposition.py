import sys
import os.path
from classes.CacheTraceWorkloadAnalyzer  import CacheTraceWorkloadAnalyzer

def process(fileName):
    dirPath = os.path.dirname(fileName)
    baseName = os.path.splitext(os.path.basename(fileName))[0]
    baseFileName = os.path.join(dirPath, baseName)

    # Configure paths to your data and output directories
    DATA_DIR = f"{baseFileName}.parquet"  # Change this to your data directory
    OUTPUT_DIR = f"analysis_output/worload_composition/{baseName}"            # Change this to your desired output directory

    # Create analyzer instance with appropriate chunk size
    # You may need to adjust chunk_size based on your available RAM
    analyzer = CacheTraceWorkloadAnalyzer(
        data_dir=DATA_DIR,
        output_dir=OUTPUT_DIR
    )

    # Process all parquet files
    analyzer.run_analysis()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No filename provided")
        print("Usage: python importWithDask.py <filename>")
    else:
        fileName = sys.argv[1]
        
        if fileName[-1] == '\\':
            fileName = fileName[0:-1]

        process(fileName)

        