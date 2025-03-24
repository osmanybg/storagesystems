
# Related Works on Cache Analysis

## Key Papers

### 1. A Large-scale Analysis of Hundreds of In-memory Key-value Cache Clusters at Twitter
**Authors:** Juncheng Yang, Yao Yue, K. V. Rashmi
**Publication:** ACM Transactions on Storage, Vol. 17, No. 3, Article 17, August 2021

**Key Findings:**
- Analyzed traces from 153 Twemcache clusters at Twitter, sifting through over 80 TB of data
- In-memory caching doesn't always serve read-heavy workloads; write-heavy workloads are common (>30% write ratio) in more than 35% of the clusters
- TTL is critical in in-memory caching as it limits effective working set size
- Cache workloads follow Zipfian popularity distribution, sometimes with very high skew
- Object size distribution is not static over time, with some workloads showing diurnal patterns and sudden changes
- Under reasonable cache sizes, FIFO often shows similar performance to LRU
- Twitter uses Twemcache, a fork of Memcached, as their primary caching solution

**Methodology:**
- Collected production traces from 153 in-memory cache clusters
- Performed comprehensive analysis of traffic patterns, TTL, popularity distribution, and size distribution
- Interpreted workloads in the context of business logic
- Made traces available to the research community

### 2. FrozenHot Cache: Rethinking Cache Management for Modern Hardware
**Authors:** Ziyue Qiu, Juncheng Yang, Juncheng Zhang, Cheng Li, Xiaosong Ma, Qi Chen, Mao Yang, Yinlong Xu
**Publication:** Eighteenth European Conference on Computer Systems (EuroSys '23), May 8-12, 2023

**Key Findings:**
- Proposes a new cache management approach called FrozenHot
- Evaluated using production traces from MSR and Twitter
- Improves throughput of three baseline cache algorithms by up to 551%
- Partitions cache space into a frozen cache for hot objects and a dynamic cache
- Eliminates promotion and locking for hot objects, improving scalability

### 3. Twitter Cache-Trace Repository
**Source:** GitHub (https://github.com/twitter/cache-trace)

**Description:**
- Public repository containing anonymized production cache traces from Twitter
- Includes traces from 54 clusters
- Provides data for research on cache performance, workload analysis, and optimization strategies

## Common Themes in Cache Analysis Research

1. **Workload Characterization:**
   - Analysis of operation types (get, set, delete)
   - Request patterns and temporal variations
   - Key and value size distributions

2. **Performance Optimization:**
   - Cache replacement policies (LRU, FIFO, etc.)
   - TTL management and expired object removal
   - Scalability improvements for multi-core systems

3. **Challenges in Modern Caching Systems:**
   - Write-heavy workloads
   - Temporal variations in access patterns
   - Object size distribution changes
   - Balancing hit rate and latency

4. **Methodology Approaches:**
   - Trace-based analysis
   - Simulation of different cache policies
   - Production system implementation and testing

## Research Gaps

1. Limited public datasets for cache trace analysis
2. Few studies on the impact of TTL on cache performance
3. Limited research on write-heavy workloads
4. Need for more analysis of temporal patterns in cache access
5. Limited understanding of how workload composition affects optimal caching strategies

## Relevance to Current Project

The current project on "Analysis of Cache Access Patterns and Workload Composition Using Public Cache Traces" directly addresses several research gaps:

1. It utilizes the publicly available Twitter cache traces
2. It focuses on temporal access patterns, which is an area needing more research
3. It analyzes workload composition to understand its impact on caching strategies
4. It examines multiple clusters to identify patterns and differences across workloads

This project will contribute to the field by providing insights into how temporal patterns and workload composition can inform cache optimization strategies, potentially leading to improved cache hit rates and reduced latency in production systems.
