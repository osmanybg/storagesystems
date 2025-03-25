# Twitter Cache Trace Analysis Project Instructions

This document provides comprehensive instructions for setting up and executing all components of the Twitter Cache Trace Analysis project based on the GitHub repository at https://github.com/osmanybg/storagesystems.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Repository Structure](#repository-structure)
3. [Environment Setup](#environment-setup)
4. [Dataset Analysis](#dataset-analysis)
5. [Result Analysis](#result-analysis)
6. [Generating Visualizations](#generating-visualizations)
7. [Compiling the LaTeX Research Paper](#compiling-the-latex-research-paper)
8. [Troubleshooting](#troubleshooting)

## Project Overview

This project analyzes cache access patterns and workload composition using Twitter's publicly available cache traces. The analysis focuses on four clusters (1, 2, 3, and 10) and examines:
- Temporal access patterns (hourly and daily distributions)
- Workload composition (operation types, key/value sizes, TTL distributions)
- Implications for cache optimization strategies
- Possible biases

## Repository Structure

The repository is organized as follows:

```
storagesystems/
├── 01.dataset_analysis/                  # Scripts for initial data processing
│   ├── analysis_output/                  # Output from dataset analysis
│   ├── classes/                          # Python classes for data processing
│   ├── data/                             # Data to be analyzed (need to be created)
│   ├── 01.loadAndPreprocessing.py        # Script for loading and preprocessing data
│   ├── 02.temporalAnalysis.py            # Script for temporal analysis
│   ├── 03.workloadComposition.py         # Script for workload composition analysis
│   └── createSampleFile.py               # Utility script for creating sample files
├── 02.result_analysis/                   # Scripts for analyzing results
│   ├── analysis_output/visualizations/   # Generated visualization images
│   ├── data/                             # Processed data for analysis
│   ├── 01.temporalAnalysis.py            # Script for temporal analysis visualizations
│   └── 02.workloadComposition.py         # Script for workload composition visualizations
├── Osmany_Becerra.Final_Research_Project.Storage_systems.pdf  # Final PDF research paper
├── Osmany_Becerra.Final_Research_Project.Storage_systems.tex  # LaTeX source for research paper
├── project.txt                           # Project description
├── readme.md                             # Project overview and instructions to reproduce the results
└── requirements.txt                      # Python package requirements
```

## Environment Setup

NOTE: The paths uses the Windows notation, adapt it to other systems if apropriate.

### Prerequisites
- Python 3.8 or higher
- pip (Python package installer)
- LaTeX distribution (for compiling the research paper)

### Setting Up Python Virtual Environment

```bash
# Clone the repository
git clone https://github.com/osmanybg/storagesystems.git
cd storagesystems

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

The `requirements.txt` file includes the following packages:
```
dask
pandas
numpy
matplotlib
seaborn
pyarrow
fastparquet
statsmodels
plotly
kaleido
```

### Getting the data

Create the data directory

```bash
mkdir .\01.dataset_analysis\data
```

Download the dataset files form https://github.com/twitter/cache-trace/tree/master/samples/2020Mar and place them on storagesystems\01.dataset_analysis\data

Then uncompress them one by one, for example:

```bash
zstd -d .\cluster1.sort.zst
```

Once uncompressed, the .zst files can be deleted.

## Dataset Analysis

The dataset analysis is performed in three steps:

### 1. Load and Preprocess Data

This step loads the Twitter cache trace data and performs initial preprocessing. 

```bash
cd 01.dataset_analysis
```

The processing needs to be done for each dataset file, for example:

```bash
python 01.loadAndPreprocessing.py .\data\cluster1.sort
```

This script:
- Loads the raw Twitter cache trace data
- Cleans and preprocesses the data
- Saves the processed data in parquet format for further analysis 

Once the preprocessing is completed, the .sort files are no longer needed and can be deleted.

### 2. Temporal Analysis

This step analyzes the temporal patterns in the cache access data. The processing needs to be done for each dataset file, for example:

```bash
python 02.temporalAnalysis.py .\data\cluster1.parquet\
```

This script:
- Analyzes hourly request distributions
- Analyzes daily request distributions
- Identifies peak periods
- Calculates request variability metrics
- Saves the results to the `analysis_output` directory

### 3. Workload Composition Analysis

This step analyzes the composition of the cache workload. The processing needs to be done for each dataset file, for example:

```bash
python 03.workloadComposition.py .\data\cluster1.parquet\
```

This script:
- Analyzes operation type distributions
- Analyzes key and value size distributions
- Analyzes TTL distributions
- Calculates get/add ratios
- Saves the results to the `analysis_output` directory

## Result Analysis

The result analysis uses the output from the dataset analysis to create visualizations and derive insights.

### Prerequisites

### 1. Temporal Analysis Visualization

```bash
cd ../02.result_analysis
python 01.temporalAnalysis.py
```

This script:
- Loads the temporal analysis results from the dataset analysis
- Creates visualizations for hourly and daily patterns
- Generates comparative visualizations across clusters
- Saves the visualizations to the `analysis_output/visualizations` directory

### 2. Workload Composition Visualization

```bash
python 02.workloadComposition.py
```

This script:
- Loads the workload composition results from the dataset analysis
- Creates visualizations for operation types, key/value sizes, and TTL distributions
- Generates comparative visualizations across clusters
- Saves the visualizations to the `analysis_output/visualizations` directory

## Generating Visualizations

All visualizations are automatically generated by the analysis scripts and saved to the `02.result_analysis/analysis_output/visualizations` directory. The following types of visualizations are created:

1. **Temporal Analysis Visualizations**:
   - Hourly request distribution
   - Daily request distribution
   - Peak period analysis
   - Request volume comparison
   - Request variability analysis

2. **Workload Composition Visualizations**:
   - Operation type distribution
   - Key size distribution
   - Value size distribution
   - TTL distribution
   - Get/add ratio

To view all generated visualizations:

```bash
ls -la 02.result_analysis/analysis_output/visualizations/
```

## Compiling the LaTeX Research Paper

The repository includes the LaTeX source file for the research paper. To compile it:

### Prerequisites

Ensure you have a LaTeX distribution installed:

```bash
# On Ubuntu/Debian
sudo apt-get install texlive-latex-base texlive-fonts-recommended texlive-fonts-extra texlive-latex-extra texlive-science
```

### Compiling the Paper

```bash
# Compile the LaTeX document
pdflatex Osmany_Becerra.Final_Research_Project.Storage_systems.tex
pdflatex Osmany_Becerra.Final_Research_Project.Storage_systems.tex  # Run twice to resolve references
```

The compiled PDF will be available as `Osmany_Becerra.Final_Research_Project.Storage_systems.pdf` in the same directory.

## Troubleshooting

### Common Issues

1. **Missing Python packages**:
   - Ensure all required packages are installed: `pip install -r requirements.txt`
   - If specific errors mention missing packages, install them individually

2. **Data loading errors**:
   - Verify that the Twitter cache trace data files are in the correct location
   - Check file permissions

3. **Visualization errors**:
   - Ensure the output directories exist and are writable
   - If using a headless server, set the matplotlib backend: `export MPLBACKEND=Agg`

4. **LaTeX compilation errors**:
   - Ensure all required LaTeX packages are installed
   - Check for syntax errors in the LaTeX files
   - Run `pdflatex` with the `-interaction=nonstopmode` flag to see all errors

### Getting Help

If you encounter issues not covered in this guide:

1. Check the project repository for updated documentation
2. Open an issue on the GitHub repository with a detailed description of the problem
3. Consult the original Twitter Cache Trace repository for dataset-specific questions

## Citation

If you use this analysis in your research, please cite:

```
@misc{TwitterCacheAnalysis2025,
  author = {Osmany Becerra},
  title = {Analysis of Cache Access Patterns and Workload Composition Using Public Cache Traces},
  year = {2025},
  month = {March},
  note = {Final Research Project for Storage Systems Class}
}
```
