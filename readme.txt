https://github.com/twitter/cache-trace/blob/master/stat/2020Mar.md

Significance and Motivation
The topic I have chosen for my research paper is "Analysis of Cache Access Patterns and Workload Composition Using Public Cache Traces." This topic focuses on understanding the temporal patterns of cache accesses and the composition of operations (e.g., get, set, delete) in large-scale cache systems.

I chose this topic because cache performance is critical to modern storage systems, and analyzing real-world cache traces offers valuable insights into optimizing cache behavior. This topic aligns with my interest in practical data analysis and system optimization. Additionally, the availability of open-source cache trace datasets provides an excellent opportunity to work on real-world scenarios and develop skills in trace-based analysis.

Key Research Questions

What are the temporal access patterns in cache traces?
This question will guide my investigation into how access patterns vary over time and how peak usage periods influence cache performance.
What is the workload composition in terms of operation types?
By analyzing the proportions of operations (get, set, etc.), I aim to understand the dominant workload and its impact on caching strategies.
How do access patterns and operation distributions inform cache optimization strategies?
The insights from this analysis can help identify areas for improving cache hit rates and reducing latency.
Methodology

ok- Dataset Exploration:
I will use the publicly available Twitter cache traces, which include fields like timestamp, operation, key size, and value size.

ok- Data Cleaning and Aggregation:
I will preprocess the data to handle missing or invalid entries and aggregate requests by time intervals (e.g., hourly or daily).

Temporal Analysis:
I will analyze timestamp data to identify peak access times and correlate these with operation types.

Workload Composition:
I will calculate the proportion of each operation type (get, set, delete, etc.) to determine workload composition and understand its trends over time.

Visualization and Insights:
I will create visualizations to present the results and interpret the workload patterns and their implications.
ok- I will research for an appropriate tool for visualizations, including Plotly and how it may help present trends more effectively.

Bias consideration
I will investigate possible bias in the used dataset and how it might affect generalizability.
Challenges

Data Size and Complexity:
Cache trace datasets can be large and complex. To address this, I will use efficient data processing tools like Python and libraries.
Real-World Applicability:
While trace-based analysis is informative, it might not fully reflect all real-world scenarios. I will focus on deriving generalizable insights that can inform caching strategies in various contexts.
Understanding the Dataset:
Interpreting all fields and their implications (e.g., TTL, key size) may require additional research. I plan to consult documentation and related papers to address this.