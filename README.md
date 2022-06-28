<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/UPocek/NPZ/blob/main/frontend/engine-api.png">
    <img alt="Flutter" src="[https://github.com/UPocek/Regression_For_Fuel_Consumption_Forecasting/blob/main/results/front_page.png](https://github.com/UPocek/NPZ/blob/main/frontend/engine-api.png)">
  </picture>
</h1>

# NO SQL Engine for big data applications

The definition of big data is data that contains greater variety, arriving in increasing volumes and with more velocity. This is also known as the three Vs.

Put simply, big data is larger, more complex data sets, especially from new data sources. These data sets are so voluminous that traditional data processing software just can’t manage them. But these massive volumes of data can be used to address business problems you wouldn’t have been able to tackle before.

## Implementation

Algorithms and data structures were implemented from scratch in the programing language Golang with the best possible performance in mind. Also, all engine layers could be configured from a single config.yaml file located at the root of this repo.

For testing purposes, we created an API and hosted it on: https://nasp.mattmarketing.rs/ so you can try it for yourself. Just click the + button on the login page to create your account and after successful login, you will be able to see are engine inspired by [CassandraDB](https://cassandra.apache.org/_/index.html) in action.

![Login Screen](https://github.com/UPocek/NPZ/blob/main/frontend/login.png)
![Main API](https://github.com/UPocek/NPZ/blob/main/frontend/engine-api.png)

## Data structures

- [Bloom Filter](https://www.geeksforgeeks.org/bloom-filters-introduction-and-python-implementation/)
- [Count Min Sketch](https://florian.github.io/count-min-sketch/)
- [Hyper Log Log](https://towardsdatascience.com/hyperloglog-a-simple-but-powerful-algorithm-for-data-scientists-aed50fe47869)
- [Memtable](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlHowDataWritten.html)
- [LRU Cache](https://www.interviewcake.com/concept/java/lru-cache)
- [Merkle Tree](https://www.geeksforgeeks.org/introduction-to-merkle-tree/)
- [SimHash](https://sauravomar01.medium.com/sim-hash-detection-of-duplicate-texts-d5dc2ce2538a)
- [SkipList](https://www.cs.cmu.edu/~ckingsf/bioinfo-lectures/skiplists.pdf)
- [Token Bucket](https://www.sciencedirect.com/topics/computer-science/token-bucket)
- [Write Ahead Log](https://www.bytebase.com/database-glossary/write-ahead-logs-wal)
