# Level 5G - Concurrency, Backpressure, and Observability

## Objective
Harden the engine under concurrent load and operational stress.

## Outcomes
By the end of this phase, you can:
- define lock/channel ownership model
- prevent unsafe lifecycle races
- expose actionable metrics for tuning and incidents

## Core Concepts
1. Reader/writer/compactor concurrency boundaries.
2. Reference counting or epoch-based resource safety.
3. Backpressure/stall thresholds.
4. Metrics and diagnostics:
   - pending writes
   - compaction debt
   - read path probes

## Concurrency in VictoriaLogs (Conceptual Background)

### The three concurrent actors

A storage engine has three actors that run simultaneously and compete for access to the same data structures:

- **Writers** (ingestion): add new rows to in-memory buffers, create in-memory parts.
- **Readers** (queries): scan parts across all tiers, hold references to parts while scanning.
- **Compactors** (merge workers): select parts, merge them into new parts, replace old parts with new ones.

The fundamental challenge is: a compactor wants to delete a part that a reader is still scanning. Or a writer is adding a new in-memory part while a compactor is deciding which parts to merge. Every concurrent storage engine must solve these coordination problems.

VictoriaLogs solves them with three mechanisms: short-held mutexes for list operations, reference counting for safe deletion, and channel semaphores for concurrency limits.

### The lock hierarchy

VictoriaLogs uses four lock domains with short critical sections. Most hot paths stay within one domain; a few maintenance paths intentionally nest locks in fixed order (for example, `snapshotLock` then `partsLock` during snapshot creation):

```
Domain 1 — Storage level
  partitionsLock (sync.Mutex)
    Protects: partition list, hot-partition pointer
    Held for: microseconds (incRef, list scan, pointer swap)

  deleteTasksLock (sync.Mutex)
    Protects: pending delete task list
    Independent of partitionsLock

Domain 2 — Datadb level (one per partition)
  partsLock (sync.Mutex)
    Protects: inmemoryParts, smallParts, bigParts lists
    Also protects: isInMerge flags, parts.json writes
    Held for: microseconds (list manipulation, never during I/O)

Domain 3 — Partition snapshot
  snapshotLock (sync.Mutex)
    Protects: snapshot creation/deletion
    May be held while snapshot code briefly acquires datadb `partsLock`

Domain 4 — Row buffer shards (one per CPU)
  shard.mu (sync.Mutex)
    Protects: buffered rows, flush timer
    One per shard — shards never communicate under their own locks
```

The critical design rule: **locks are never held during expensive operations**. The pattern is always: acquire lock → read/modify shared state → release lock → do the I/O-heavy work. A merge worker acquires `partsLock` to select parts (microseconds), then releases the lock and spends seconds or minutes performing the actual merge. Readers and writers are never blocked by a long-running merge.

### Reference counting: safe deletion under concurrent reads

The core problem: a merge finishes and wants to delete the source parts, but a query started 2 seconds ago is still scanning one of those parts. Deleting it would crash the query.

VictoriaLogs solves this with reference counting on `partWrapper`:

```
                    refCount lifecycle
                    ─────────────────

  Part created:     refCount = 1  (the list's own reference)

  Reader starts:    partsLock.Lock()
                    find matching parts
                    pw.incRef() for each    → refCount = 2
                    partsLock.Unlock()
                    (reader now owns a reference)

  Merge completes:  partsLock.Lock()
                    remove old parts from list
                    add new part to list
                    partsLock.Unlock()
                    pw.mustDrop = true      (flag: delete when safe)
                    pw.decRef()             → refCount = 1
                    (part is gone from list but reader still holds it)

  Reader finishes:  pw.decRef()             → refCount = 0
                    mustDrop is true
                    → fs.MustRemoveDir(part.path)
                    (now the part is actually deleted)
```

The same pattern is used at the partition level: `partitionWrapper` has its own `refCount`, `mustDrop`, and a `doneCh` channel that closes when the count reaches zero. `PartitionDetach` blocks on `<-doneCh`, waiting for all readers to finish before allowing external backup tools to touch the files.

**The key invariant**: once `partsLock` is released, no lock protects the part's data files. Only the reference count prevents premature deletion. This is why `incRef` must happen while holding the lock — it's the only moment where the caller can guarantee the part still exists in the list.

### Channel semaphores: concurrency limits

VictoriaLogs uses buffered channels as counting semaphores. This is a Go idiom that integrates naturally with `select` for cancellation:

```go
// Package-level: shared across all partitions
inmemoryPartsConcurrencyCh = make(chan struct{}, AvailableCPUs())
smallPartsConcurrencyCh    = make(chan struct{}, AvailableCPUs())
bigPartsConcurrencyCh      = make(chan struct{}, AvailableCPUs())
```

To acquire a slot, send to the channel (blocks if full). To release, receive:

```
Worker wants to merge:
  inmemoryPartsConcurrencyCh <- struct{}{}  // blocks if all CPU slots taken
  ... perform merge ...
  <-inmemoryPartsConcurrencyCh              // release slot
```

Each tier has its own semaphore. This prevents a burst of big merges from starving in-memory merges, which would block ingestion. On an 8-CPU machine, at most 8 in-memory merges, 8 small merges, and 8 big merges run concurrently — across all partitions.

The query path has its own limiter:

```
partitionSearchConcurrencyLimitCh = make(chan struct{}, AvailableCPUs())
```

And the HTTP layer has a configurable concurrency limit that returns `503 Service Unavailable` when exceeded:

```
concurrencyLimitCh = make(chan struct{}, maxConcurrentRequests)
```

### The stopCh/WaitGroup pattern: lifecycle management

Every long-lived component uses the same shutdown pattern: a `stopCh` channel (closed to signal "stop") and a `sync.WaitGroup` (tracks running goroutines):

```
Start:
  ddb.stopCh = make(chan struct{})
  ddb.wg.Add(1)
  go ddb.inmemoryPartsFlusher()    // background goroutine

Background goroutine:
  for {
      select {
      case <-ddb.stopCh:
          return        // signal received, exit
      case <-ticker.C:
          // ... do work ...
      }
  }

Shutdown:
  close(ddb.stopCh)    // all goroutines see this
  ddb.wg.Wait()        // block until all exit
```

There is a subtle race to be aware of: `close(stopCh)` must not happen between a goroutine calling `wg.Add(1)` and actually starting. VictoriaLogs solves this by closing `stopCh` **under `partsLock`**, and checking `needStop(stopCh)` inside `startXxxMergerLocked` (also under `partsLock`). The lock serializes the close with any pending goroutine launches.

### Backpressure: how the system slows down gracefully

VictoriaLogs has no explicit "write stall" mechanism like RocksDB's Level 0 slowdown. Instead, backpressure emerges naturally from the concurrency limiters:

```
Ingestion rate exceeds merge capacity:

  In-memory parts accumulate
    → more merge workers try to acquire inmemoryPartsConcurrencyCh
      → all CPU slots occupied
        → mustFlushLogRows blocks on channel send
          → shard flushLocked blocks
            → rowsBuffer.mustAddRows blocks
              → HTTP handler blocks
                → client sees increased latency
```

This chain is entirely automatic. There's no threshold to tune — the CPU count itself is the throttle. The system degrades gracefully: ingestion slows to the pace at which merges can keep up, then resumes at full speed when the backlog clears.

Additional backpressure mechanisms:

| Mechanism | Trigger | Effect |
|-----------|---------|--------|
| Merge concurrency channels | All CPU slots busy | Merge/flush goroutines block |
| Read-only mode | Free disk < `minFreeDiskSpaceBytes` | Ingestion returns 429 |
| Disk space reservation | Merge would exceed available disk | Merge skipped, retried later |
| Query concurrency limit | Active queries ≥ `maxConcurrentRequests` | New queries return 503 |
| Merge ratio threshold | No merge has ratio ≥ 7.5× | Merge workers exit, await new parts |

### The parallel search architecture

Queries use a two-tier worker model to maximize CPU utilization:

```
Query arrives
│
├── Tier 1: Partition searchers (one goroutine per partition)
│   Throttled by partitionSearchConcurrencyLimitCh (CPU count)
│   Each partition:
│     - Walks metaindex (in memory)
│     - Loads index blocks on demand
│     - Batches candidate blocks into blockSearchWorkBatch (64 blocks each)
│     - Sends batches to workCh
│
└── Tier 2: Block search workers (N goroutines)
    N = query's parallelReaders setting
    Each worker:
      - Reads batches from workCh
      - For each block: bitmap init → filter cascade → result collection
      - Per-worker QueryStats (no contention)
      - Merges stats atomically once at goroutine exit
```

The two tiers decouple I/O (index block reads in Tier 1) from compute (filter evaluation in Tier 2). Batching into groups of 64 blocks amortizes the channel send overhead.

Per-worker statistics avoid contention: each worker accumulates into a local `QueryStats` struct and merges into the shared stats once via `atomic.AddUint64` when the goroutine exits. This eliminates lock contention that would occur if all workers updated shared counters on every block.

### What the metrics tell you

VictoriaLogs exposes metrics via Prometheus-compatible `/metrics` endpoint. Here are the key signals for diagnosing operational issues:

**Is merge keeping up?**
```
vl_active_merges{type="storage/inmemory"}   # current in-memory merges running
vl_active_merges{type="storage/small"}      # current small-part merges
vl_active_merges{type="storage/big"}        # current big-part merges
vl_storage_parts{type="storage/inmemory"}   # in-memory part count (high = merge lagging)
vl_storage_parts{type="storage/small"}      # small part count
vl_storage_parts{type="storage/big"}        # big part count
vl_pending_rows{type="storage"}             # rows in rowsBuffer, not yet a part
```

If `vl_storage_parts{type="storage/inmemory"}` is consistently high (approaching 20), merge workers are not keeping up with ingestion. If `vl_pending_rows` is growing, even the row buffer flush is backing up.

**Is disk pressure a concern?**
```
vl_free_disk_space_bytes{path="..."}        # current free space
vl_storage_is_read_only{path="..."}         # 1 = ingestion stopped
vl_compressed_data_size_bytes{type="..."}   # per-tier physical sizes
```

**Are queries healthy?**
```
vl_concurrent_select_current                # queries running now
vl_concurrent_select_capacity               # max allowed
vl_concurrent_select_limit_reached_total    # times queries had to wait
vl_concurrent_select_limit_timeout_total    # times queries timed out (503s)
vl_slow_queries_total                       # queries exceeding slow threshold
```

**Merge efficiency:**
```
vl_merge_duration_seconds{type="..."}       # how long merges take
vl_merge_bytes{type="..."}                  # how many bytes per merge
vl_rows_merged_total{type="..."}            # cumulative rows merged
vl_merges_total{type="..."}                 # cumulative merge count
```

Rising `vl_merge_duration_seconds` for big merges with flat `vl_merges_total` means individual merges are getting larger — expected as data accumulates, but worth monitoring for I/O saturation.

**Per-query diagnostics** (available via `| query_stats` pipe in LogsQL):
```
BytesReadBloomFilters      # bloom data read (high = many blocks checked)
BytesReadValues            # values data read (high = bloom not filtering well)
BlocksProcessed            # total blocks examined
RowsProcessed / RowsFound  # selectivity (low ratio = efficient filter)
```

A query reading many bloom filter bytes but few value bytes is working efficiently — bloom filters are eliminating most blocks. A query where `BytesReadValues ≈ BytesReadBloomFilters` suggests the bloom filters are not selective for this query pattern.

### Design lessons

**Why no read-write lock?** Many storage engines use `sync.RWMutex` to allow concurrent readers while blocking writers. VictoriaLogs avoids this because its locks are held for microseconds (list manipulation only). The overhead of RWMutex's reader counting would likely exceed the benefit. A simple mutex with fast critical sections performs better under high contention.

**Why per-tier merge workers instead of a unified pool?** A unified pool would be simpler, but a single long-running big merge could occupy all workers, starving in-memory merges and blocking ingestion. Separate pools ensure each tier makes independent progress.

**Why jitter in background watchers?** Retention checks, disk space checks, and snapshot cleanup all use `timeutil.AddJitterToDuration` to randomize their intervals. In a cluster with many VictoriaLogs instances, this prevents all instances from running maintenance tasks simultaneously, which would cause correlated I/O spikes.

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: locks, ref-count wrappers, merge workers
- `lib/logstorage/storage_search.go`: parallel search workers
- `lib/logstorage/storage.go`: background watchers

## Labs
### Lab 1: Concurrency Model Doc
Produce a lock-order and ownership table.

### Lab 2: Race/Deadlock Test
Add stress tests with parallel writes/reads/compactions and failure injections.

### Lab 3: Metrics Dashboard
Define and emit core metrics; add alerting thresholds.

## Deliverables
- `level5g/concurrency-model.md`
- `level5g/stress-results.md`
- `level5g/metrics-spec.md`
- `level5g/checkpoint.md`

## Pass Criteria
- No known data races/deadlocks in stress tests and metrics support diagnosis.
