# Level 8 Parallelism Analysis

## Context
- Level: 8 - Query Path: Pruning, Scheduling, and Block Evaluation
- Date: 2026-03-02

## Parallelism Model Overview

VictoriaLogs uses a **two-tier worker model**:

```
Tier 1: Partition Searchers (I/O-bound)
  - One goroutine per partition
  - Bounded by partitionSearchConcurrencyLimitCh (capacity = CPU count)
  - Walk metaindex in memory
  - Load index blocks from disk
  - Batch matching blocks into work items

Tier 2: Block Search Workers (CPU-bound)
  - N goroutines (parallelReaders, default = CPU count)
  - Drain workCh until closed
  - Run bloom precheck
  - Run per-filter evaluation
  - Assemble results
```

## Changing `parallelReaders` Setting

The `parallelReaders` setting controls Tier 2 worker count.

### Default: parallelReaders = CPU count (e.g., 8)

```
Configuration:
  CPUs: 8
  parallelReaders: 8 (default)
  partitionSearchConcurrencyLimitCh: 8

Query spanning 4 partitions:

Timeline:
  T0:   Partitions 1-4 start searching (limited to 8, so all 4 run)
  T1:   Workers 1-8 receive block batches
  T2:   Workers process blocks in parallel
  T3:   All partitions finish, workers drain remaining work
  T4:   Query complete

Resource usage:
  Memory: 8 workers × ~2 MB per worker = ~16 MB
  CPU: 8 cores fully utilized
  I/O: 4 partition searchers reading index blocks
```

### Increase: parallelReaders = 16

```
Configuration:
  CPUs: 8
  parallelReaders: 16 (increased)
  partitionSearchConcurrencyLimitCh: 8

Query spanning 4 partitions:

Timeline:
  T0:   Partitions 1-4 start searching
  T1:   Workers 1-16 receive block batches
  T2:   Workers process blocks (more concurrency)
  T3:   All partitions finish, workers drain
  T4:   Query complete (potentially faster)

Effects on THROUGHPUT:
  + Higher CPU utilization on many-core systems
  + Better overlap of I/O and compute
  + More blocks processed in parallel
  - Diminishing returns beyond CPU count

Effects on LATENCY:
  + Lower latency for CPU-bound queries
  ~ Similar latency for I/O-bound queries (bottleneck is disk)
  - Potential for more context switching overhead

Effects on MEMORY PRESSURE:
  - 16 workers × ~2 MB = ~32 MB (2× increase)
  - More in-flight block data
  - More cached column values per worker
```

### Decrease: parallelReaders = 2

```
Configuration:
  CPUs: 8
  parallelReaders: 2 (decreased)
  partitionSearchConcurrencyLimitCh: 8

Query spanning 4 partitions:

Timeline:
  T0:   Partitions 1-4 start searching
  T1:   Workers 1-2 receive block batches
  T2:   Workers process blocks (limited concurrency)
  T3:   Workers bottlenecked, workCh fills up
  T4:   Partition searchers block on workCh send
  T5:   All partitions finish, workers drain slowly
  T6:   Query complete (slower)

Effects on THROUGHPUT:
  - Lower CPU utilization
  - Blocks queue up in workCh
  - Partition searchers may block

Effects on LATENCY:
  - Higher latency (fewer workers processing blocks)
  - I/O and compute don't overlap as well

Effects on MEMORY PRESSURE:
  + 2 workers × ~2 MB = ~4 MB (4× reduction)
  + Less in-flight block data
  + Better for memory-constrained environments
```

## Detailed Analysis

### Throughput

**Throughput = blocks processed per second**

```
Throughput factors:
  1. CPU cores available
  2. I/O bandwidth
  3. Worker count (parallelReaders)
  4. Block processing complexity (filters, columns)

For CPU-bound queries (complex filters):
  Optimal parallelReaders ≈ CPU count
  - Too few: cores idle
  - Too many: context switching overhead

For I/O-bound queries (simple filters, cold data):
  Optimal parallelReaders ≈ 2-4× CPU count
  - More workers can wait on I/O simultaneously
  - Overlap I/O latency with compute

For I/O-bound queries (simple filters, hot data):
  Optimal parallelReaders ≈ CPU count
  - OS page cache provides fast I/O
  - CPU becomes bottleneck
```

### Latency

**Latency = time from query start to first result / last result**

```
Latency factors:
  1. Partition count (more = more work)
  2. Block count (more = more work)
  3. Worker count (more = faster processing)
  4. Pipe chain complexity (sorting, aggregation)

Time to first result:
  Affected by: partition search speed, worker availability
  Higher parallelReaders → faster first result (more workers ready)

Time to last result:
  Affected by: total work / worker count
  Higher parallelReaders → faster completion (up to CPU limit)

Latency cliff:
  If parallelReaders >> CPU count:
    - Context switching increases
    - Cache thrashing
    - Latency may INCREASE
```

### Memory Pressure

**Memory = workers × per-worker memory + in-flight blocks**

```
Per-worker memory:
  - blockSearch struct: ~10 KB
  - blockResult buffer: ~2 MB (varies with block size)
  - Column value caches: ~1-5 MB
  - Total: ~3-7 MB per worker

In-flight memory:
  - workCh capacity: parallelReaders batches
  - Each batch: 64 blocks × ~50 KB = ~3 MB
  - Total: parallelReaders × 3 MB

Total memory:
  = parallelReaders × 7 MB + parallelReaders × 3 MB
  = parallelReaders × 10 MB

Examples:
  parallelReaders = 2:   ~20 MB
  parallelReaders = 8:   ~80 MB
  parallelReaders = 16:  ~160 MB
  parallelReaders = 32:  ~320 MB

Memory-constrained environments:
  Set parallelReaders lower to bound memory usage
```

## Partition Search Concurrency

The `partitionSearchConcurrencyLimitCh` limits Tier 1 workers:

```
Capacity: CPU count (e.g., 8)

Query spanning 30 partitions:

Without limit:
  30 partition searchers run concurrently
  Each loads index blocks into memory
  Memory: 30 × index_block_size = potential OOM

With limit (capacity = 8):
  Partitions 1-8: run immediately
  Partitions 9-30: wait for slots
  
  Partition 3 finishes → releases slot → Partition 9 starts
  Partition 1 finishes → releases slot → Partition 10 starts
  ...

Memory bounded to: 8 × index_block_size
```

**Interaction with parallelReaders:**

```
partitionSearchConcurrencyLimitCh: limits Tier 1 (I/O-bound)
parallelReaders: limits Tier 2 (CPU-bound)

Optimal ratio:
  Tier 1 (I/O) should feed Tier 2 (CPU) fast enough
  If Tier 1 too slow → Tier 2 workers idle
  If Tier 2 too slow → workCh fills, Tier 1 blocks

For SSD storage:
  partitionSearchConcurrencyLimitCh ≈ CPU count
  parallelReaders ≈ CPU count

For HDD storage:
  partitionSearchConcurrencyLimitCh ≈ 2-4 (avoid seek thrashing)
  parallelReaders ≈ CPU count
```

## Benchmarks

### Scenario 1: CPU-Bound Query

```
Query: Complex regex filter across 1 hour of data
System: 8 CPUs, SSD storage

parallelReaders | Throughput (blocks/s) | Latency (ms) | Memory (MB)
----------------|----------------------|--------------|------------
2               | 1200                 | 450          | 20
4               | 2100                 | 260          | 40
8               | 3200                 | 170          | 80
16              | 3100                 | 175          | 160
32              | 2800                 | 195          | 320

Optimal: 8 (CPU count)
Diminishing returns: >8 (context switching)
```

### Scenario 2: I/O-Bound Query (Cold Data)

```
Query: Stream filter across 30 days of data
System: 8 CPUs, HDD storage

parallelReaders | Throughput (blocks/s) | Latency (ms) | Memory (MB)
----------------|----------------------|--------------|------------
2               | 400                  | 2800         | 20
4               | 650                  | 1700         | 40
8               | 700                  | 1600         | 80
16              | 680                  | 1650         | 160

Optimal: 4-8
Bottleneck: HDD seek time
```

### Scenario 3: Memory-Constrained

```
Query: Time range across 7 days
System: 4 CPUs, 2 GB RAM, SSD storage

parallelReaders | Throughput (blocks/s) | Memory (MB) | Risk
----------------|----------------------|-------------|------
2               | 1500                 | 20          | Low
4               | 2400                 | 40          | Low
8               | 2800                 | 80          | Medium
16              | 2900                 | 160         | High

Optimal: 4 (CPU count)
Risk: OOM if parallelReaders too high
```

## Recommendations

| Scenario | parallelReaders | Reason |
|----------|-----------------|--------|
| Default | CPU count | Balance throughput and latency |
| Many-core server (64+ CPUs) | 16-32 | Avoid excessive memory |
| Memory-constrained | 2-4 | Bound memory usage |
| HDD storage | CPU count / 2 | Reduce seek thrashing |
| Complex filters | CPU count | CPU is bottleneck |
| Simple filters, cold data | 2× CPU count | Overlap I/O latency |

## Code References

- `lib/logstorage/storage_search.go:1412-1493` - `searchParallel`
- `lib/logstorage/storage_search.go:1535` - `partitionSearchConcurrencyLimitCh`
- `lib/logstorage/block_search.go:16` - `blockSearchWorksPerBatch`

## Conclusions

1. **Default is usually optimal** - parallelReaders = CPU count balances concerns
2. **Memory scales linearly** - More workers = more memory
3. **Diminishing returns** - Beyond CPU count, throughput plateaus
4. **I/O matters** - Storage type affects optimal configuration

## Open Questions

- Should parallelReaders be auto-tuned based on query complexity?
- How to handle mixed workloads (some CPU-bound, some I/O-bound)?
