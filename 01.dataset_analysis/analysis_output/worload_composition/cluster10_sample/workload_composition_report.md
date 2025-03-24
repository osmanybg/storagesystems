# Cache Workload Composition Analysis

Analysis generated on: 2025-03-17 17:25:54

Total records analyzed: 1,000

## 1. Overall Operation Distribution

### Operation Counts
```
operation
add    500.0
get    500.0
```

### Operation Percentages
```
operation
add    50.0
get    50.0
```

## 2. Key and Value Size Statistics by Operation

### Key Size Statistics
```
     count      sum   min   max  mean
add  500.0  11000.0  22.0  22.0  22.0
get  500.0  11000.0  22.0  22.0  22.0
```

### Value Size Statistics
```
     count        sum   min     max      mean
add  500.0  1287842.0  30.0  3543.0  2575.684
```

### TTL Statistics
```
     count        sum     min     max      mean
add  500.0  3600001.0  7200.0  7201.0  7200.002
```

## 3. Client Analysis

Total unique clients: 894

### Top 10 Most Active Clients
```
client_id
30812.0    4
24409.0    4
26672.0    4
26895.0    4
27448.0    4
30641.0    3
30930.0    3
25250.0    3
25145.0    3
28496.0    3
```

## 4. Key Insights

### Workload Composition
- The most common operation is 'add' (50.00% of all operations)

### Operation Characteristics
- Average key size for 'add' operations: 22.00 bytes
- Average value size for 'add' operations: 2575.68 bytes
- Average key size for 'get' operations: 22.00 bytes

### Client Behavior
- The most active client (ID: 30812.0) performed 4 operations
- The top 10 clients account for 3.50% of all operations

## 5. Recommendations for Cache Optimization

Based on the analysis, consider the following optimizations:

- Consider adjusting TTL values based on operation patterns to optimize cache efficiency.
- Monitor the behavior of top clients as they have a significant impact on overall cache performance.
- Analyze temporal patterns to identify if cache sizing should be adjusted during peak periods.
