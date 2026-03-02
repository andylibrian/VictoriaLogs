# Level 8 - Query Path: Pruning, Scheduling, and Block Evaluation

## Objective
Understand end-to-end query execution and where each pruning stage saves I/O/CPU.

## Outcomes
By the end of this level, you can:
- trace query execution from `RunQuery` to block-level filtering
- explain pruning stages and their order
- explain worker scheduling and concurrency limits

## Query Execution in VictoriaLogs (Conceptual Background)

### The cost of not pruning

Consider a query: `_stream:{host="web-01"} AND level:error AND _time:[10:00, 10:05]`. Without pruning, the system would decompress and scan every column of every block of every part of every partition. On a system storing 1 TB of logs across 30 daily partitions, each with hundreds of parts, that's reading the entire dataset — terabytes of I/O for a query that might match 50 rows in 3 blocks.

The query path exists to avoid this. Its job is to narrow terabytes down to kilobytes through a cascade of increasingly fine-grained eliminations, where each stage is cheaper than the next and eliminates data before the expensive stage runs.

### The full query pipeline

A query passes through three phases: **setup**, **scheduling**, and **evaluation**. Here is the complete flow:

```
RunQuery(qctx, writeBlock)
│
│  SETUP PHASE
├── initSubqueries()
│     Materialize in(subquery) filters, build join maps, wire union hooks.
│     Subqueries execute as full queries themselves before the main scan begins.
│
├── getSearchOptions()
│     Extract from the parsed query:
│       - minTimestamp, maxTimestamp (time range)
│       - streamFilter ({host="web-01"})
│       - filter (level:error — the remaining LogsQL filter tree)
│       - fieldsFilter (which columns the pipe chain actually needs)
│
│  SCHEDULING PHASE
├── searchParallel(workersCount, sso, ...)
│   │
│   │  STAGE 1: Partition pruning
│   ├── getPartitionsForTimeRange(minTs, maxTs)
│   │     Binary search on the sorted partition list.
│   │     Each partition covers one day. A 5-minute query on March 2
│   │     skips all 29 other partitions with zero I/O.
│   │
│   │  STAGE 2: Stream ID resolution (per partition)
│   ├── idb.searchStreamIDs(tenantIDs, streamFilter)
│   │     Resolves {host="web-01"} → concrete stream IDs using indexdb.
│   │     Result is cached with a generation counter. Cache invalidation
│   │     is coalesced/asynchronous after new streams are flushed into indexdb.
│   │
│   │  STAGE 3: Part pruning (per partition)
│   ├── getPartsForTimeRange(minTs, maxTs)
│   │     Scans all three tiers (inmemory, small, big).
│   │     Checks each part's [MinTimestamp, MaxTimestamp] from its header.
│   │     Calls incRef() on matching parts (under partsLock), then releases lock.
│   │
│   │  STAGE 4: Metaindex pruning (per part, in memory)
│   ├── scan indexBlockHeaders[]
│   │     Each entry covers a group of blocks with a (streamID, time range).
│   │     For streamID-targeted queries: binary search or early exit
│   │       (metaindex is sorted by streamID).
│   │     For time-range queries: skip if [ih.minTs, ih.maxTs] doesn't overlap.
│   │     ~400 entries for 1 billion rows, ~20 KB total. One comparison
│   │     can skip ~2.5 million rows.
│   │
│   │  STAGE 5: Block header pruning (per index block, one disk read)
│   ├── mustReadBlockHeaders(indexBlock)
│   │     ZSTD-decompress the index block from index.bin.
│   │     For each blockHeader:
│   │       - Check streamID match
│   │       - Check [bh.minTimestamp, bh.maxTimestamp] overlap
│   │     Each block covers ~10,000 rows. Skipping one block avoids
│   │     reading timestamps, columns, blooms, and values for those rows.
│   │
│   │  STAGE 6: Batch into work items
│   └── Send matching blocks to workCh in batches of 64
│       (blockSearchWorkBatch). Batching amortizes channel overhead.
│
│  EVALUATION PHASE (per block, in worker pool)
├── Worker pool: N goroutines drain workCh
│   │
│   ├── blockSearch.search(bsw, bm)
│   │   │
│   │   │  STAGE 7: Bitmap initialization
│   │   ├── bm.init(rowsCount); bm.setBits()
│   │   │     All rows start as candidates. The bitmap has one bit per row.
│   │   │
│   │   │  STAGE 8: Bloom precheck (AND filter fast path)
│   │   ├── filterAnd.matchBloomFilters(bs)
│   │   │     For each child filter, collect query tokens per column.
│   │   │     For each column:
│   │   │       1. Check const column value (free — already in block header)
│   │   │       2. Check dict column dictionary (free — inline in column header)
│   │   │       3. Check bloom filter: bf.containsAll(tokenHashes)
│   │   │          6 hash probes per token, ~1.5% false positive rate
│   │   │     If ANY token is definitely absent → bm.resetBits(), return
│   │   │     Entire block skipped without reading values.
│   │   │
│   │   │  STAGE 9: Per-filter row evaluation
│   │   ├── for each filter in AND chain:
│   │   │     filter.applyToBlockSearch(bs, bm)
│   │   │       Loads column values lazily (cached for this block)
│   │   │       Clears bitmap bits for non-matching rows
│   │   │     if bm.isZero() → short-circuit, skip remaining filters
│   │   │
│   │   │  STAGE 10: Result assembly
│   │   └── if bm has set bits:
│   │         br.mustInit(bs, bm)        — fetch matching rows
│   │         br.initColumns(fieldsFilter) — load only needed columns
│   │         writeBlock(workerID, &br)  — push to pipe chain
│   │
│   └── Merge per-worker QueryStats atomically at goroutine exit
│
└── Pipe chain processes results (sort, limit, stats, etc.)
```

### Why pruning order matters

Each stage is ordered by cost — cheapest first:

| Stage | What it checks | Cost per check | Typical data eliminated |
|-------|---------------|----------------|------------------------|
| 1. Partition | Day-level time range | 1 comparison | Entire days (billions of rows) |
| 2. Stream IDs | Stream label filter | Cached lookup | All non-matching streams |
| 3. Part | Part-level time range | 1 comparison per part | Entire parts (millions of rows) |
| 4. Metaindex | Index block time/stream range | ~400 comparisons in RAM | Groups of ~250 blocks |
| 5. Block header | Per-block time/stream match | ~100 comparisons + 1 decompress | Individual blocks (~10K rows) |
| 6-7. (batching/init) | — | Channel send + bitmap alloc | — |
| 8. Bloom | Token presence in column | 6 hash probes per token | Blocks where token is absent |
| 9. Values | Actual row content | Decompress + scan per row | Individual rows |

If the stages were reversed — scanning values first, then checking bloom filters — the system would decompress every column of every block before discovering that most blocks don't contain the query tokens. The current order ensures that the expensive decompression in Stage 9 only happens for blocks that survived every cheaper check.

### `searchByStreamIDs` and `searchByTenantIDs`: sorted metadata enables binary search

The metaindex (`indexBlockHeaders`) is sorted by `streamID`. When the query targets specific stream IDs (resolved in Stage 2), the search uses `sort.Search` (binary search) to find the first relevant index block header, then scans forward:

```
indexBlockHeaders sorted by streamID:
  [S1..S5] [S6..S10] [S11..S15] [S16..S20] [S21..S25]

Query targets streamID = S14:
  Binary search → jump to [S11..S15]
  Check: does S14 fall in [S11, S15]? Yes → load this index block
  Next: [S16..S20] — S16 > S14, all remaining are higher → stop
```

For tenant-scoped queries (no specific stream filter), `searchByTenantIDs` scans all index block headers but still prunes by time range. The two search paths (`searchByStreamIDs` and `searchByTenantIDs`) share the same downstream logic — the difference is only in how they traverse the metaindex.

Within each loaded index block, `blockHeaders` are sorted by `(streamID, minTimestamp)`. A targeted query can again binary-search for the first matching block and scan forward, skipping blocks for other streams without examining them.

### The bitmap lifecycle: from all-ones to filtered rows

The bitmap is the mechanism that connects block-level pruning (Stages 1-8) with row-level filtering (Stage 9). Its lifecycle within one block:

```
blockSearch.search():
  bm = [1111111111...]     10,000 bits, all set (every row is a candidate)

  filterAnd.matchBloomFilters():
    bloom check passes     → bitmap unchanged (block survives bloom check)

  filterPhrase("level", "error").applyToBlockSearch():
    loads level column values (lazy, cached)
    scans each row where bm bit is set:
      row 0: level="info"  → clear bit 0
      row 1: level="error" → keep bit 1
      row 2: level="error" → keep bit 2
      row 3: level="warn"  → clear bit 3
      ...
  bm = [0110010001...]     3,200 bits remaining

  filterPhrase("msg", "timeout").applyToBlockSearch():
    loads msg column values (lazy, cached)
    scans only rows where bm bit is still set (3,200 rows, not 10,000):
      row 1: msg="timeout"  → keep bit 1
      row 2: msg="started"  → clear bit 2
      ...
  bm = [0100010000...]     1,800 bits remaining

  bm is not zero → assemble result from 1,800 matching rows
```

Key observations:
- Each filter only examines rows that survived all previous filters. The second filter scans 3,200 rows, not 10,000.
- If any filter reduces the bitmap to all-zeros, remaining filters are skipped entirely (short-circuit).
- Column values are loaded lazily and cached per block. Bloom prechecks can reuse cached column metadata and bloom data, while value decoding still happens on first row-level use.

### Work batching and the two-tier worker model

The scheduling phase (Stages 4-5) and the evaluation phase (Stages 7-9) run in different goroutines connected by a channel:

```
Partition searchers (Tier 1)          Block search workers (Tier 2)
─────────────────────────────         ──────────────────────────────
One goroutine per partition           N goroutines (parallelReaders)
Bounded by CPU count                  Drain workCh until closed

Walk metaindex in memory              For each batch of 64 blocks:
Load index blocks from disk             Run bloom precheck
Batch matching blocks into              Run per-filter evaluation
  blockSearchWorkBatch (64 blocks)      Assemble results
Send batch to workCh ────────────────→  Push to pipe chain
```

Why two tiers? Tier 1 is I/O-bound (reading index blocks from disk). Tier 2 is CPU-bound (decompressing columns, evaluating filters, computing hashes). Separating them allows the system to overlap I/O and compute: while Tier 2 workers evaluate blocks from the current index block, Tier 1 can be loading the next index block.

The batch size of 64 blocks amortizes channel send/receive overhead. Without batching, each block would require a separate channel operation — at ~100K blocks per query, that's 100K channel operations vs ~1,600 batched sends.

### Per-worker statistics: avoiding contention

Each Tier 2 worker accumulates query statistics (blocks processed, rows processed, bytes read) in a local `QueryStats` struct. At goroutine exit, the local stats are merged into the shared `QueryStats` using `atomic.AddUint64`:

```
Worker 0: local qsLocal {BlocksProcessed: 450, RowsProcessed: 4500000, ...}
Worker 1: local qsLocal {BlocksProcessed: 380, RowsProcessed: 3800000, ...}
Worker 2: local qsLocal {BlocksProcessed: 420, RowsProcessed: 4200000, ...}
  ...
All workers finished:
  qs.BlocksProcessed.Add(450)    // atomic
  qs.BlocksProcessed.Add(380)    // atomic
  qs.BlocksProcessed.Add(420)    // atomic
  → qs.BlocksProcessed = 1250
```

If all workers updated shared counters on every block, the atomic operations would contend — each worker would be hammering the same cache lines. Accumulating locally and merging once eliminates this contention entirely. The trade-off is that real-time stats during query execution are approximate (only updated at goroutine exit), but this is acceptable for metrics and diagnostics.

### The `partitionSearchConcurrencyLimitCh`: preventing memory explosion

A query spanning 30 days would launch 30 partition searcher goroutines simultaneously. Each searcher loads index blocks, decompresses them, and creates `blockSearchWork` items — all consuming memory. Without a limit, 30 concurrent searchers could allocate enough index block buffers to exhaust RAM.

The `partitionSearchConcurrencyLimitCh` (capacity = CPU count) gates how many partition searchers run simultaneously:

```
30-day query on an 8-CPU machine:

  Partitions 1-8:  searching concurrently (8 slots occupied)
  Partitions 9-30: queued, waiting for a slot

  Partition 3 finishes → releases slot → Partition 9 starts
  Partition 1 finishes → releases slot → Partition 10 starts
  ...
```

This bounds memory usage to O(CPUs × index_block_size) regardless of how many partitions the query spans. The query takes longer (serialized over partitions) but doesn't risk OOM.

### Lazy loading: the key to I/O efficiency

Within `blockSearch`, every piece of data is loaded on demand and cached for the block's duration:

```
Query: level:error AND msg:timeout

Block evaluation:
  1. matchBloomFilters() needs bloom filter for "level" column
     → getColumnHeader("level")
       → getColumnsHeaderIndex()          first access: read from disk, cache
       → decode columnHeader for "level"  first access: decode, cache
     → getBloomFilterForColumn("level")   first access: read from disk, cache
     → bf.containsAll(["error"])          check 6 bits

  2. matchBloomFilters() needs bloom filter for "msg" column
     → getColumnHeader("msg")
       → getColumnsHeaderIndex()          cached (same block)
       → decode columnHeader for "msg"    first access: decode, cache
     → getBloomFilterForColumn("msg")     first access: read from disk, cache
     → bf.containsAll(["timeout"])        check 6 bits

  3. Bloom passes → per-row evaluation
     → getValuesForColumn("level")        first access: read from disk, cache
     → scan rows against "error"
     → getValuesForColumn("msg")          first access: read from disk, cache
     → scan remaining rows against "timeout"

  4. Block done → caches reset for next block
```

If the bloom check at step 1 had returned "definitely absent," steps 2-3 would never execute. The bloom filter for "msg" would never be loaded, and no column values would be read. The lazy loading ensures that I/O is proportional to how deep into the evaluation the block reaches — rejected blocks cost only a bloom filter read.

The caches (`bloomFilterCache`, `valuesCache`, `cshIndexCache`) are per-block maps, reset between blocks. There is no cross-block cache for column data — the OS page cache provides that implicitly for hot data, and in-memory parts provide it for the most recent data.

## Source Anchors
- `lib/logstorage/storage_search.go`
  - `RunQuery`
  - `searchParallel`
  - `getPartitionsForTimeRange`
  - `getPartsForTimeRange`
  - `searchByTenantIDs`
  - `searchByStreamIDs`
- `lib/logstorage/block_search.go`
  - `search`
  - `getColumnHeader`
  - `getBloomFilterForColumn`
  - `getValuesForColumn`
- `lib/logstorage/filter_and.go`
  - `matchBloomFilters`

## Core Concepts
1. Stage-wise pruning pipeline:
   - partition time pruning
   - part time pruning
   - index block pruning
   - block header pruning
   - bloom pruning
   - value scan and row bitmap evaluation
2. Work batching (`blockSearchWorkBatch`) and worker channels.
3. Why query stats are accumulated atomically across workers.

## Guided Reading Tasks
1. In `searchParallel`, map:
   - partition-level scheduling
   - work channel fan-out
   - finalizer flow
2. In `searchByTenantIDs` and `searchByStreamIDs`, explain how sorted metadata and `sort.Search` reduce block reads.
3. In `blockSearch.search`, explain bitmap lifecycle from all-ones to filtered rows.
4. In `filterAnd.matchBloomFilters`, explain why Bloom precheck can terminate early.

## Hands-On Lab
### Lab 1: Query Trace
Pick a realistic query and produce a function-by-function trace with pruning counters.

### Lab 2: Skip Effectiveness
For one query class, estimate relative reduction at each stage:
- partitions skipped
- parts skipped
- blocks skipped by metadata
- blocks skipped by bloom

### Lab 3: Parallelism Reasoning
Explain effect of changing `parallel readers` on:
- throughput
- latency
- memory pressure

## Checkpoint Questions
1. Why is pruning order important?
2. Why does Bloom sit before value decoding?
3. Why do we still need row-level bitmap evaluation after all pruning?

## Deliverables
- `level8/query-trace.md`
- `level8/skip-effectiveness.md`
- `level8/parallelism-analysis.md`
- `level8/checkpoint.md`

## Pass Criteria
- You can reconstruct query execution without skipping stages.
- You can explain observed performance through pruning mechanics.
