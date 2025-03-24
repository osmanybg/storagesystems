# Cache Workload Composition Analysis

Analysis generated on: 2025-03-17 20:48:00

Total records analyzed: 7,226,679,214

## 1. Overall Operation Distribution

### Operation Counts
```
operation
add       10076984.0
cas             12.0
get     7211621640.0
gets       4980578.0
```

### Operation Percentages
```
operation
add      0.14
cas       0.0
get     99.79
gets     0.07
```

## 2. Key and Value Size Statistics by Operation

### Key Size Statistics
```
             count           sum   min    max       mean
get   7.211622e+09  1.578777e+11  14.0  113.0  21.892116
add   1.007698e+07  2.109361e+08  14.0  113.0  20.932461
gets  4.980578e+06  1.040780e+08  15.0  109.0  20.896769
cas   1.200000e+01  4.210000e+02  20.0   85.0  35.083333
```

### Value Size Statistics
```
          count          sum   min      max       mean
add  10076984.0  765052000.0  30.0  25129.0  75.920732
```

### TTL Statistics
```
          count           sum      min       max           mean
add  10076984.0  2.751235e+12  21599.0  345601.0  273021.701824
```

## 3. Client Analysis

Total unique clients: 19065

### Top 10 Most Active Clients
```
client_id
1479.0    1735580.0
717.0     1689862.0
2564.0    1677230.0
984.0     1646115.0
1409.0    1643467.0
5113.0    1632691.0
2485.0    1613426.0
5208.0    1602687.0
3008.0    1599606.0
1857.0    1596662.0
```

## 4. Key Insights

### Workload Composition
- The most common operation is 'get' (99.79% of all operations)

### Operation Characteristics
- Average key size for 'add' operations: 20.93 bytes
- Average value size for 'add' operations: 75.92 bytes
- Average key size for 'cas' operations: 35.08 bytes
- Average key size for 'get' operations: 21.89 bytes
- Average key size for 'gets' operations: 20.90 bytes

### Client Behavior
- The most active client (ID: 1479.0) performed 1735580.0 operations
- The top 10 clients account for 0.23% of all operations

## 5. Recommendations for Cache Optimization

Based on the analysis, consider the following optimizations:

- Consider adjusting TTL values based on operation patterns to optimize cache efficiency.
- Monitor the behavior of top clients as they have a significant impact on overall cache performance.
- Analyze temporal patterns to identify if cache sizing should be adjusted during peak periods.
