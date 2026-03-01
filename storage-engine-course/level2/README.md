# Level 2 - Ingestion Skeleton: Rows, Streams, Buffers

## Objective
Understand how inserted logs enter VictoriaLogs and become structured, buffered data.

## Outcomes
By the end of this level, you can:
- trace insertion path from partition to `datadb`
- explain stream registration and caches
- explain why buffering and sorting are required before part creation

## Source Anchors (Read In Order)
1. `lib/logstorage/partition.go`
   - `mustAddRows`
   - stream cache usage (`hasStreamIDInCache`, `putStreamIDToCache`)
2. `lib/logstorage/indexdb.go`
   - `hasStreamID`
   - `mustRegisterStream`
3. `lib/logstorage/datadb.go`
   - `mustAddRows`
   - `rowsBuffer` and `rowsBufferShard`
   - `mustFlushLogRows`
4. `lib/logstorage/log_rows.go`
   - `needFlush`
   - `Less`
   - `sortFieldsInRows`

## Core Concepts
1. Stream identity and metadata lifecycle.
2. Write amortization through sharded row buffering.
3. CPU-friendly sorting before block serialization.
4. Visibility timing: recently ingested rows are not immediately in final on-disk parts.

## From HTTP Request to Sorted Buffer (Conceptual Background)

### Why not write each log entry directly to disk?

A log database at scale receives thousands to millions of entries per second from many concurrent producers. If each entry were written to disk individually, every write would involve:
- a disk seek to find the right location
- a metadata update (inode, directory entry)
- an fsync to guarantee durability

At 100K entries/second, that is 100K disk operations per second — far beyond what a single disk can sustain. Even SSDs would bottleneck on the write amplification from many tiny, random writes.

The solution is **buffering**: accumulate many entries in memory, then flush them together as one large sequential write. This converts random I/O into sequential I/O and amortizes per-write overhead across thousands of entries.

### Streams: grouping logs by identity

Not all log entries are alike. Entries from different sources (hosts, containers, applications) have different label sets. VictoriaLogs calls each unique label combination a **stream**. For example:

```
{app="nginx", host="web-01"}     → stream A
{app="nginx", host="web-02"}     → stream B
{app="postgres", host="db-01"}   → stream C
```

Each stream gets a unique `streamID` — a 128-bit hash of its canonical (sorted) tag set. Stream identity matters because blocks are organized by stream: all rows in one block belong to the same stream. This means the storage engine needs to know each stream's identity before it can route entries to the right block.

### Stream registration: cache before disk

When a log entry arrives, VictoriaLogs must determine whether its stream already exists. The naive approach — query the on-disk index for every entry — would be prohibitively expensive. Instead, the system uses a two-tier lookup:

```
Entry arrives with {app="nginx", host="web-01"}
  → compute streamID = hash(canonical tags)

Tier 1: In-memory cache (shared across partitions)
  ├─ HIT  → stream is known, skip to buffering
  └─ MISS → proceed to Tier 2

Tier 2: On-disk indexdb
  ├─ EXISTS → add to cache, skip to buffering
  └─ NOT FOUND → register stream in indexdb, add to cache
```

This is the path through `partition.mustAddRows`. The in-memory cache (`hasStreamIDInCache`) handles the vast majority of lookups because most entries belong to streams that have been seen recently. Only genuinely new streams — a cold-start or a new container spinning up — trigger the expensive indexdb write.

The code also batches stream lookups: it collects all entries with unknown streams, sorts them by `streamID`, and deduplicates before querying indexdb. If 500 entries arrive from the same new stream, only one indexdb registration happens instead of 500.

### Sharded buffering: reducing lock contention

Once streams are resolved, entries move to `datadb.mustAddRows`, which appends them to a `rowsBuffer`. In a multi-core system, a single buffer with one lock would become a serialization bottleneck — every ingestion goroutine would contend for the same lock.

VictoriaLogs solves this with **sharding**: it creates one `rowsBufferShard` per available CPU. Each incoming batch is routed to a shard via round-robin (`nextIdx`), and each shard has its own independent lock:

```
Ingestion goroutines:    G1    G2    G3    G4    G5    G6
                          │     │     │     │     │     │
                          ▼     ▼     ▼     ▼     ▼     ▼
rowsBuffer shards:     [shard0] [shard1] [shard2] [shard3]
                        (lock0)  (lock1)  (lock2)  (lock3)
```

Each shard also has cache-line padding (`[atomicutil.CacheLineSize]byte`) to prevent false sharing — without this, adjacent shards on the same CPU cache line would cause unnecessary cache invalidations even when accessed by different cores.

### Two flush triggers: time and size

Each shard flushes its buffered rows under two conditions:

1. **Size threshold**: when the shard's arena buffer exceeds ~87.5% of `maxUncompressedBlockSize` (2 MB), the shard flushes immediately. This is checked in `logRows.needFlush()`: `len(lr.a.b) > (maxUncompressedBlockSize/8)*7`. The threshold is set below the maximum to leave room for the last batch of rows that pushed it over.

2. **Time threshold**: a 1-second timer starts when the first row enters an empty shard. If the size threshold is not reached within that second, the shard flushes anyway. This ensures that low-throughput streams do not sit in memory indefinitely, keeping tail latency bounded.

```
Shard receives first row → start 1-second timer
  │
  ├─ More rows arrive → buffer grows
  │   └─ Buffer exceeds ~1.75 MB → flush immediately (cancel timer)
  │
  └─ 1 second elapses → flush whatever is buffered (even if small)
```

### Sorting before block creation

When a shard flushes, its rows are not yet organized for storage. The flush target (`mustFlushLogRows`) converts the buffer into an in-memory part. Before writing blocks, two sorts happen:

1. **Inter-row sort by `(streamID, timestamp)`**: Rows are sorted so that all entries for the same stream are contiguous, ordered by time. This is the `logRows.Less` comparator. The sort ensures that block boundaries align with stream boundaries — each block contains rows from exactly one stream.

2. **Intra-row sort of field names**: Within each row, fields are sorted alphabetically by name (`sortFieldsInRows`). This ensures that when rows are rotated into columns (Level 3), field names appear in a consistent order. Consistent ordering improves compression (identical field names are adjacent) and enables binary search during queries.

Consider 4 unsorted rows from 2 streams:

```
Before sorting:
  row 1: streamB, t=3, {msg="ok", level="info"}
  row 2: streamA, t=1, {level="error", msg="fail"}
  row 3: streamA, t=2, {msg="retry", level="warn"}
  row 4: streamB, t=1, {msg="start", level="info"}

After inter-row sort (by streamID, then timestamp):
  row 2: streamA, t=1, {level="error", msg="fail"}
  row 3: streamA, t=2, {msg="retry", level="warn"}
  row 4: streamB, t=1, {msg="start", level="info"}
  row 1: streamB, t=3, {msg="ok", level="info"}

After intra-row field sort (alphabetical):
  row 2: streamA, t=1, {level="error", msg="fail"}     ← already sorted
  row 3: streamA, t=2, {level="warn", msg="retry"}
  row 4: streamB, t=1, {level="info", msg="start"}
  row 1: streamB, t=3, {level="info", msg="ok"}
```

Now the block writer can iterate sequentially: emit a block for streamA (rows 2–3), then a block for streamB (rows 4, 1). Within each block, columns are aligned by field name, ready for the encoding pipeline described in Level 3.

### Visibility gap

An important consequence of buffering: recently ingested rows are not immediately visible to queries. They exist only in the shard's memory buffer (up to 1 second) or as an in-memory part (until flushed to disk). Queries against on-disk parts will not see them until the flush completes. This is a deliberate tradeoff — the ingestion throughput gained from buffering far outweighs the brief delay in query visibility.

## Guided Reading Tasks
1. In `partition.mustAddRows`, list the exact order of operations from stream check to data append.
2. In `rowsBuffer.mustAddRows`, explain:
   - shard selection strategy
   - timer-driven flush path
   - size-driven flush path
3. In `logRows.needFlush`, explain threshold meaning relative to `maxUncompressedBlockSize`.
4. In `logRows.Less`, explain why `(streamID, timestamp)` ordering matters for later merges.

## Hands-On Lab
### Lab 1: Write Path Trace
Pick one synthetic row and create a step-by-step trace with function names and state transitions:
- stream cache miss/hit
- index registration path
- buffer append
- flush trigger

### Lab 2: Concurrency Reasoning
Explain how sharding in `rowsBuffer` reduces lock contention compared to one global lock.
Provide one edge case where contention still exists.

## Checkpoint Questions
1. Why does stream registration happen before writing row data to `datadb`?
2. Why does VictoriaLogs maintain both timer-based and threshold-based flushing?
3. Why is sorting fields in rows useful before column construction?

## Deliverables
- `level2/write-path-trace.md`
- `level2/concurrency-analysis.md`
- `level2/checkpoint.md`

## Pass Criteria
- You can narrate write path end-to-end from memory.
- You can justify buffering and ordering decisions in operational terms.
