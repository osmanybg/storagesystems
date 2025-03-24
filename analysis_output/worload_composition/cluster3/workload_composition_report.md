# Cache Workload Composition Analysis

Analysis generated on: 2025-03-17 18:24:13

Total records analyzed: 820,307,312

## 1. Overall Operation Distribution

### Operation Counts
```
operation
add       5581724.0
cas         91862.0
get     814533880.0
gets        99846.0
```

### Operation Percentages
```
operation
add     0.68
cas     0.01
get     99.3
gets    0.01
```

## 2. Key and Value Size Statistics by Operation

### Key Size Statistics
```
            count           sum   min   max       mean
get   814533880.0  1.547212e+10   8.0  19.0  18.995055
add     5581724.0  1.055364e+08   8.0  19.0  18.907484
gets      99846.0  1.890619e+06  11.0  19.0  18.935350
cas       91862.0  1.739840e+06  11.0  19.0  18.939714
```

### Value Size Statistics
```
         count          sum   min     max        mean
add  5581724.0  606164182.0  17.0  5423.0  108.598021
```

### TTL Statistics
```
         count           sum       min       max           mean
add  5581724.0  3.375827e+12  604724.0  604801.0  604799.996036
```

## 3. Client Analysis

Total unique clients: 13459

### Top 10 Most Active Clients
```
client_id
730.0     754681
476.0     723436
478.0     636710
774.0     613106
2295.0    510744
2039.0    509376
2239.0    505098
2201.0    493929
2171.0    488502
2649.0    483585
```

## 4. Key Insights

### Workload Composition
- The most common operation is 'get' (99.30% of all operations)

### Operation Characteristics
- Average key size for 'add' operations: 18.91 bytes
- Average value size for 'add' operations: 108.60 bytes
- Average key size for 'cas' operations: 18.94 bytes
- Average key size for 'get' operations: 19.00 bytes
- Average key size for 'gets' operations: 18.94 bytes

### Client Behavior
- The most active client (ID: 730.0) performed 754681 operations
- The top 10 clients account for 0.70% of all operations

## 5. Recommendations for Cache Optimization

Based on the analysis, consider the following optimizations:

- Consider adjusting TTL values based on operation patterns to optimize cache efficiency.
- Monitor the behavior of top clients as they have a significant impact on overall cache performance.
- Analyze temporal patterns to identify if cache sizing should be adjusted during peak periods.
