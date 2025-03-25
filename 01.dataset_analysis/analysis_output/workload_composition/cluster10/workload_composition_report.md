# Cache Workload Composition Analysis

Analysis generated on: 2025-03-17 18:06:33

Total records analyzed: 139,150,615

## 1. Overall Operation Distribution

### Operation Counts
```
operation
add     69526999.0
get     69621161.0
gets        2455.0
```

### Operation Percentages
```
operation
add     49.97
get     50.03
gets      0.0
```

## 2. Key and Value Size Statistics by Operation

### Key Size Statistics
```
           count           sum   min   max  mean
add   69526999.0  1.529594e+09  22.0  22.0  22.0
get   69621161.0  1.531666e+09  22.0  22.0  22.0
gets      2455.0  5.401000e+04  22.0  22.0  22.0
```

### Value Size Statistics
```
          count           sum   min     max         mean
add  69526999.0  1.777821e+11  30.0  5409.0  2557.022177
```

### TTL Statistics
```
          count           sum     min     max         mean
add  69526999.0  5.005945e+11  7122.0  7201.0  7200.001481
```

## 3. Client Analysis

Total unique clients: 16445

### Top 10 Most Active Clients
```
client_id
24492.0    28894
23149.0    28706
24877.0    28507
25056.0    28374
25353.0    28139
28149.0    27633
24631.0    27079
29040.0    26842
19111.0    26417
28294.0    26236
```

## 4. Key Insights

### Workload Composition
- The most common operation is 'get' (50.03% of all operations)

### Operation Characteristics
- Average key size for 'add' operations: 22.00 bytes
- Average value size for 'add' operations: 2557.02 bytes
- Average key size for 'get' operations: 22.00 bytes
- Average key size for 'gets' operations: 22.00 bytes

### Client Behavior
- The most active client (ID: 24492.0) performed 28894 operations
- The top 10 clients account for 0.20% of all operations

## 5. Recommendations for Cache Optimization

Based on the analysis, consider the following optimizations:

- Consider adjusting TTL values based on operation patterns to optimize cache efficiency.
- Monitor the behavior of top clients as they have a significant impact on overall cache performance.
- Analyze temporal patterns to identify if cache sizing should be adjusted during peak periods.
