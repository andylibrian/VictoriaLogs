# External Resources

Foundational readings to deepen understanding of storage engine concepts.

## LSM Trees

### Essential Papers
| Paper | Authors | Year | Why Read |
|-------|---------|------|----------|
| [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) | O'Neil et al. | 1996 | Original LSM paper; establishes write amplification theory |
| [WiscKey: Separating Keys from Values in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) | Lu et al. | 2016 | Key-value separation; relevant to VictoriaLogs' indexdb/datadb split |

### Production Systems Documentation
| Resource | Link | Focus |
|----------|------|-------|
| RocksDB Wiki | [github.com/facebook/rocksdb/wiki](https://github.com/facebook/rocksdb/wiki) | Leveled compaction, Bloom filters, block cache |
| LevelDB Documentation | [github.com/google/leveldb/blob/main/doc/index.md](https://github.com/google/leveldb/blob/main/doc/index.md) | Simpler LSM; good for mental model |
| Cassandra Architecture | [cassandra.apache.org/doc/latest/cassandra/architecture/overview.html](https://cassandra.apache.org/doc/latest/cassandra/architecture/overview.html) | Tiered compaction, tombstones |
| HBase Architecture | [hbase.apache.org/book.html#arch.overview](https://hbase.apache.org/book.html#arch.overview) | LSM on HDFS, region splitting |

### Blog Posts & Tutorials
| Resource | Author | Topics |
|----------|--------|--------|
| [LSM Trees: What Powers Your Database](https://medium.com/databasss/on-disk-io-part-2-btrees-vs-lsm-trees-e7fcfe86102d) | Konstantin Slavnov | B-tree vs LSM tradeoffs |
| [Compaction Strategies in RocksDB](https://rockset.com/blog/compaction-strategies-in-rocksdb/) | Rockset | Tiered vs leveled deep dive |
| [Designing a LSM Tree Engine](https://sidshant.com/posts/2023-05-01-lsm-tree-storage-engine/) | Siddhant Shah | Step-by-step implementation |

## Bloom Filters

### Papers
| Paper | Authors | Year | Why Read |
|-------|---------|------|----------|
| [Space/Time Trade-offs in Hash Coding with Allowable Errors](http://dmod.eu/deca/ftps/bloom.pdf) | Bloom | 1970 | Original paper; surprisingly readable |
| [Bloom Filters in Probabilistic Verification](https://www.cs.cmu.edu/~dshafer/data/BloomFilters.pdf) | Dillinger & Manolios | 2004 | Parameter selection methodology |

### Interactive Resources
| Resource | Link |
|----------|------|
| Bloom Filter Visualization | [llimllib.github.io/bloomfilter-tutorial](https://llimllib.github.io/bloomfilter-tutorial/) |
| Joshua Andersen's Simulator | [joshuaaman.github.io/Bloom-Filter-Simulator](https://joshuaaman.github.io/Bloom-Filter-Simulator/) |

## Columnar Storage

### Papers
| Paper | Authors | Year | Why Read |
|-------|---------|------|----------|
| [The Design and Implementation of Modern Column-Oriented Database Systems](https://stratos.seas.harvard.edu/files/stratos/files/columnstoresftd.pdf) | Abadi et al. | 2013 | Comprehensive survey; compression, vectorization |
| [C-Store: A Column-oriented DBMS](http://www.cs.umd.edu/~abadi/vldb.pdf) | Stonebraker et al. | 2005 | Foundational columnar paper |

### Relevant Systems
| System | Link | Insight |
|--------|------|---------|
| Parquet Format | [parquet.apache.org/docs](https://parquet.apache.org/docs/) | Row groups, pages, encoding |
| ClickHouse | [clickhouse.com/docs/en/development/architecture](https://clickhouse.com/docs/en/development/architecture) | MergeTree engine similar to VictoriaLogs |

## Merge Algorithms

### Papers & Resources
| Resource | Authors | Focus |
|----------|--------|-------|
| [K-Way Merge Algorithms](https://en.wikipedia.org/wiki/K-way_merge_algorithm) | Wikipedia | Heap-based merge basics |
| [Optimal External Memory Interval Management](https://www.cs.duke.edu/~pankaj/publications/papers/btree-io.pdf) | Arge | External memory model |
| [mergeset Implementation](../vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset/) | VictoriaMetrics | VictoriaMetrics' LSM variant |

## Concurrency & Recovery

### Papers
| Paper | Authors | Year | Why Read |
|-------|---------|------|----------|
| [The Raft Consensus Algorithm](https://raft.github.io/raft.pdf) | Ongaro & Ousterhout | 2014 | Understandable consensus; WAL concepts |
| [ARIES: A Transaction Recovery Method](https://www.cs.berkeley.edu/~brewer/cs262/Aries.pdf) | Mohan et al. | 1992 | WAL recovery gold standard |

### Visual Guides
| Resource | Link |
|----------|------|
| Raft Visualization | [thesecretlivesofdata.com/raft](http://thesecretlivesofdata.com/raft/) |
| The Raft Scope | [raft.github.io/#implementations](https://raft.github.io/#implementations) |

## VictoriaMetrics Background

| Resource | Link |
|----------|------|
| VictoriaMetrics Architecture | [docs.victoriametrics.com/SingleServerVictoriaMetrics.html#architecture](https://docs.victoriametrics.com/SingleServerVictoriaMetrics.html#architecture) |
| mergeset Design | `vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset/README.md` |

## Recommended Reading Order

| Phase | Level | Primary | Secondary |
|-------|-------|---------|-----------|
| Foundations | 1-3 | LevelDB docs | Columnar survey (skim) |
| LSM Core | 5a-5c | Original LSM paper | RocksDB wiki |
| LSM Advanced | 5d-5h | WiscKey paper | RocksDB compaction docs |
| Query | 8 | Bloom filter tutorial | ClickHouse merge tree docs |
| Operations | 9-10 | Raft visualization | ARIES (recovery concepts) |

## How to Use These Resources

1. **Just-in-time**: Read when you hit a concept in the course
2. **Compare**: After each level, compare VictoriaLogs choices to alternatives in papers
3. **Tradeoff analysis**: Use paper citations to justify or challenge design decisions
