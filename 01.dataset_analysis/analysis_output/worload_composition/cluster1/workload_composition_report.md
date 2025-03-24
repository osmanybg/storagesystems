# Cache Workload Composition Analysis

Analysis generated on: 2025-03-17 21:01:01

Total records analyzed: 6,461,081,324

## 1. Overall Operation Distribution

### Operation Counts
```
operation
add       29242157.0
cas        3525031.0
get     6422678822.0
gets       5635314.0
```

### Operation Percentages
```
operation
add      0.45
cas      0.05
get     99.41
gets     0.09
```

## 2. Key and Value Size Statistics by Operation

### Key Size Statistics
```
             count           sum   min    max       mean
get   6.422679e+09  3.763511e+11  24.0  120.0  58.597213
add   2.924216e+07  2.322487e+09  24.0  120.0  79.422572
cas   3.525031e+06  2.485590e+08  24.0  112.0  70.512573
gets  5.635314e+06  4.084974e+08  24.0  120.0  72.488852
```

### Value Size Statistics
```
          count           sum   min    max        mean
add  29242157.0  7.560674e+09  30.0  815.0  258.553921
```

### TTL Statistics
```
          count           sum    min    max       mean
add  29242157.0  7.018109e+09  219.0  241.0  239.99971
```

## 3. Client Analysis

Total unique clients: 17777

### Top 10 Most Active Clients
```
client_id
2860.0    1465894
325.0     1419456
1248.0    1405090
1864.0    1387988
2957.0    1386107
6047.0    1375230
1423.0    1370648
5035.0    1362637
700.0     1358445
279.0     1356380
```

## 4. Key Insights

### Workload Composition
- The most common operation is 'get' (99.41% of all operations)

### Operation Characteristics
- Average key size for 'add' operations: 79.42 bytes
- Average value size for 'add' operations: 258.55 bytes
- Average key size for 'cas' operations: 70.51 bytes
- Average key size for 'get' operations: 58.60 bytes
- Average key size for 'gets' operations: 72.49 bytes

### Client Behavior
- The most active client (ID: 2860.0) performed 1465894 operations
- The top 10 clients account for 0.21% of all operations

## 5. Recommendations for Cache Optimization

Based on the analysis, consider the following optimizations:

- Consider adjusting TTL values based on operation patterns to optimize cache efficiency.
- Monitor the behavior of top clients as they have a significant impact on overall cache performance.
- Analyze temporal patterns to identify if cache sizing should be adjusted during peak periods.
