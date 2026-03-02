# Level 9 - IndexDB and `mergeset` Deep Dive

## Objective
Understand stream metadata indexing and the second LSM-style subsystem behind VictoriaLogs.

## Outcomes
By the end of this level, you can:
- explain index key spaces and row shapes in `indexdb`
- explain how `mergeset.Table` is used for indexing
- explain merge-time consolidation for tag->streamIDs rows

## IndexDB and Mergeset in VictoriaLogs (Conceptual Background)

### Why a separate index engine?

VictoriaLogs stores two fundamentally different kinds of data:

- **Log payloads** (in `datadb`): the actual log entries — timestamps, message text, field values. Organized in columnar blocks, optimized for time-range scans and column-selective reads.
- **Stream metadata** (in `indexdb`): which streams exist, what tags they have, and how to map a tag filter like `{host="web-01"}` to concrete stream IDs. Organized as sorted key-value items, optimized for prefix seeks and point lookups.

These have different access patterns. Log payloads are written in large batches and read in broad scans. Stream metadata is written one entry at a time and read via targeted lookups. Mixing them in the same storage engine would force one access pattern to compromise for the other.

VictoriaLogs solves this by running two independent LSM-style engines per partition: `datadb` for payloads and `indexdb` (backed by `mergeset.Table`) for metadata. They share the same architectural DNA — immutable parts, background merge, atomic swap, reference counting — but differ in data format and merge semantics.

### The key schema: three namespaces

Every item in the mergeset table starts with a 9-byte common prefix: 1 byte for namespace, 8 bytes for tenant ID. The namespace byte determines how the remaining bytes are interpreted:

```
Namespace 0 — Stream existence
  Key:   [0x00] [tenantID 8B] [streamID 16B]
  Value: (none — key presence is the signal)

  Purpose: "does this stream exist?"
  Access pattern: exact-match seek

Namespace 1 — Stream ID → tags
  Key:   [0x01] [tenantID 8B] [streamID 16B]
  Value: streamTagsCanonical (e.g. {app="nginx",host="server1"})

  Purpose: given a stream ID, retrieve its human-readable tags
  Access pattern: exact-match seek

Namespace 2 — Tag → stream IDs (inverted index)
  Key:   [0x02] [tenantID 8B] [tagName] [separator] [tagValue] [streamID₁ 16B] [streamID₂ 16B] ...
  Value: (embedded in the key — stream IDs are the suffix)

  Purpose: given a tag like host="web-01", find all matching stream IDs
  Access pattern: prefix seek on [0x02][tenantID][tagName][sep][tagValue]
```

Namespace 2 is the workhorse. Because mergeset items are stored in sorted order, all entries sharing the same `(tenantID, tagName, tagValue)` prefix are physically adjacent on disk. A prefix seek jumps directly to the right position, then a forward scan collects all matching stream IDs until the prefix changes.

### How stream registration builds the inverted index

When a new log stream arrives (e.g., `{app="nginx", host="server1"}`), `mustRegisterStream` emits multiple items to the mergeset in a single batch:

```
Stream: {app="nginx", host="server1"}  streamID = X

Items emitted:
  1. [0x00][tenantID][X]                               ← existence marker
  2. [0x01][tenantID][X]{app="nginx",host="server1"}   ← ID→tags lookup
  3. [0x02][tenantID][app][sep][nginx][X]               ← tag→streamID
  4. [0x02][tenantID][host][sep][server1][X]             ← tag→streamID
```

Each tag produces one Namespace 2 item with a single stream ID in the suffix. If 1,000 streams have `host="server1"`, there are initially 1,000 separate items, each containing one 16-byte stream ID. These get consolidated into fewer, larger items during compaction (see merge callbacks below).

After registration, cache invalidation is scheduled via generation bump callbacks. The bump is coalesced and asynchronous, so visibility of newly registered streams through cached filter results can lag briefly.

### How `searchStreamIDs` resolves a filter

A stream filter like `{host="web-01", app="nginx"}` must be resolved to concrete stream IDs before the main log scan begins. This is the bridge between the index and the data: the query path uses stream IDs to prune blocks in `datadb`.

**Step 1 — Cache check.** The filter is marshaled into a cache key that includes the current generation counter, partition name, and tenant IDs. If the cache contains an entry for this exact key, the result is returned immediately with no index I/O.

```
Cache key: [generation=42] [partition="20260302"] [tenantIDs] [filter bytes]
Cache hit → return cached streamIDs
```

**Step 2 — Index scan (on cache miss).** The filter is decomposed into OR-of-AND form. For each AND clause, each tag filter is resolved independently:

```
Filter: {host="web-01", app="nginx"}

  Tag filter: host="web-01"
    Seek to prefix [0x02][tenantID][host][sep][web-01]
    Scan forward, collecting stream IDs from suffix bytes
    Result: {S1, S5, S12, S99}

  Tag filter: app="nginx"
    Seek to prefix [0x02][tenantID][app][sep][nginx]
    Scan forward, collecting stream IDs
    Result: {S1, S5, S42, S77}

  AND intersection: {S1, S5}
```

Different filter operators use different strategies:

| Operator | Strategy |
|----------|----------|
| `=value` | Prefix seek on `[tag][sep][value]`, collect stream IDs |
| `=""` | All streams for tenant minus streams that have this tag |
| `!=value` | All streams for tenant minus streams matching `=value` |
| `!=""` | All streams that have any value for this tag |
| `=~regex` | Iterate all values under the tag prefix, regex-test each, collect matches |
| `!~regex` | Complement of regex matches |

**Step 3 — Cache store.** The result is sorted and stored in the cache with the current generation as part of the key.

### Merge callbacks: semantic compaction

In `datadb`, merging is purely mechanical — blocks from source parts are combined into output blocks in sorted order. The merge doesn't understand what the data means.

In `indexdb`, merging has semantic awareness. The `mergeTagToStreamIDsRows` callback is invoked during every merge (both in-memory and file-based) and consolidates Namespace 2 items:

```
Before merge (4 separate items, one streamID each):
  [0x02][tenantID][host][sep][web-01][S1]
  [0x02][tenantID][host][sep][web-01][S5]
  [0x02][tenantID][host][sep][web-01][S12]
  [0x02][tenantID][host][sep][web-01][S99]

After merge (1 consolidated item, 4 streamIDs):
  [0x02][tenantID][host][sep][web-01][S1][S5][S12][S99]
```

The callback receives a block of sorted items and:

1. Identifies consecutive items with the same `(tenantID, tagName, tagValue)` prefix.
2. Collects their stream IDs, deduplicates, and sorts them.
3. Emits a single item with all stream IDs concatenated in the suffix.
4. Caps each item at 32 stream IDs (`maxStreamIDsPerRow`). If more exist, multiple items share the same prefix.

This consolidation reduces the number of items in the index and speeds up `searchStreamIDs` — instead of reading 1,000 single-ID items for a popular tag value, the query reads ~32 consolidated items.

**The boundary constraint:** The callback must preserve the sort order of the block relative to adjacent blocks. It does this by passing through the first and last items of each block unchanged — they act as sort-order anchors. Only interior items are consolidated.

**The unsorted fallback:** In rare cases, consolidation can produce an item that sorts after an unconsolidated item that follows it (because deduplication changes the suffix). When this happens, the callback detects the sort violation and falls back to returning the original unmodified items. The consolidation will be retried in a future merge when the items are arranged differently.

### `mergeset.Table`: the second LSM engine

`mergeset.Table` follows the same architectural pattern as `datadb` but with its own three-stage pipeline:

```
Stage 0: rawItems (sharded buffers, NOT visible to search)
  │  Incoming items land in per-CPU shards, each with its own mutex.
  │  Flushed when: shard fills up (256 blocks) or 1-second timer fires.
  │
  ▼
Stage 1: inmemoryParts (visible to search, in RAM)
  │  Raw items are sorted, merged (with prepareBlock callback),
  │  and formed into in-memory parts.
  │  Backpressure: max 30 in-memory parts (channel semaphore).
  │  Flushed to disk when: flush deadline passes.
  │
  ▼
Stage 2: fileParts (visible to search, on disk)
     File parts are merged by background workers.
     Tracked in parts.json (atomic write-then-rename).
     prepareBlock callback runs during every merge.
```

The parallels with `datadb` are deliberate — both are LSM engines from the same codebase (`mergeset` lives in the VictoriaMetrics shared library). Key differences:

| Aspect | `datadb` | `mergeset.Table` |
|--------|----------|-----------------|
| Data format | Columnar blocks with timestamps, bloom filters, column headers | Flat sorted byte-string items in blocks |
| Tiers | 3 (inmemory, small, big) | 2 (inmemory, file) |
| Merge semantics | Pure mechanical merge-sort | Semantic merge via `prepareBlock` callback |
| Block structure | Per-column encoding (dict, uint, string, etc.) | Simple sorted item list |
| Flush trigger | `flushInterval` from storage config | Same `flushInterval`, plus per-shard deadlines |
| Primary consumer | Log query scan path | Stream ID resolution (`searchStreamIDs`) |

### Cache invalidation via generation counters

The stream filter cache maps `(filter, tenantIDs)` → `streamIDs[]`. Stream registrations are continuous, so a cached result that was correct moments ago may miss a newly registered stream until the next generation bump.

VictoriaLogs solves this with a **generation counter** embedded in the cache key:

```
Stream registered:
  mustRegisterStream()
  → items added to mergeset
  → data is flushed into searchable parts
  → needFlushCallbackCall is set
  → flush callback worker runs (coalesced, ~10s cadence)
  → filterStreamCacheGeneration.Add(1)  (generation: 42 → 43)

Next query:
  Cache key: [generation=43][partition][tenantIDs][filter]
  → no entry for generation 43 (old entries were at generation 42)
  → cache miss → re-scan index → finds new stream
  → cache result at generation 43
```

The old cache entries (at generation 42) are never explicitly evicted. They simply become unreachable because no new query constructs a key with generation 42. The cache's natural eviction policy (LRU/size-based) eventually reclaims the memory.

This is **lazy invalidation** — simpler than tracking which cache entries are affected by each new stream. The trade-offs are: invalidation is coarse (partition-wide) and not immediate (coalesced callback cadence). Stream registrations are usually much less frequent than queries, so cache hit rate remains high in practice.

The generation counter is per-partition (each `indexdb` has its own counter). Cache keys also include the partition name, so results from different partitions never collide.

### Why merge callbacks matter

Without the `mergeTagToStreamIDsRows` callback, the index would accumulate one item per (tag, streamID) pair indefinitely. For a system with 100,000 streams and 10 tags each, that's 1 million items — each requiring a separate read during `searchStreamIDs`.

With the callback, these consolidate during compaction into ~31,250 items (1M / 32 IDs per item). A query that reads 100 items instead of 3,200 completes faster and reads less data from disk.

This is **semantic compaction** — the merge understands the meaning of the data and can combine entries that a purely mechanical merge-sort would keep separate. It's the same idea as a "compaction filter" in RocksDB, but applied to the inverted index structure rather than to user-facing key-value pairs.

## Source Anchors
- `lib/logstorage/indexdb.go`
  - namespace prefixes (`nsPrefix*`)
  - `mustRegisterStream`
  - `searchStreamIDs`
  - `mergeTagToStreamIDsRows`
- `vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset/table.go`
  - `Table` structure
  - ingestion and merge workers
- `vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset/merge.go`
  - block stream merge mechanics

## Core Concepts
1. Separation of concerns:
   - `datadb`: row payload blocks
   - `indexdb`: stream/tag discovery
2. Prefix-structured keys for efficient seeks and range scans.
3. Merge callbacks for semantic consolidation (`mergeTagToStreamIDsRows`).
4. Cache invalidation strategy via generation counters.

## Guided Reading Tasks
1. In `mustRegisterStream`, list exact emitted items for one stream registration.
2. In `searchStreamIDs`, trace cache lookup -> miss path -> store path.
3. In `mergeTagToStreamIDsRows`, explain why merged output can temporarily become unsorted and how fallback is handled.
4. In `mergeset.Table`, map key background workers and their analogous roles to `datadb` workers.

## Hands-On Lab
### Lab 1: Index Item Reconstruction
Given a tenant + stream tags, manually construct index rows for all namespace prefixes.

### Lab 2: Merge Consolidation Case
Construct a toy example with duplicate streamIDs across rows and show expected merged output.

### Lab 3: Cache Correctness
Explain why `filterStreamCacheGeneration` is embedded in cache key.

## Checkpoint Questions
1. Why is index metadata maintained in a dedicated engine?
2. Why are merge callbacks useful for semantic compaction?
3. What correctness issue appears if cache generation is ignored?

## Deliverables
- `level9/index-item-reconstruction.md`
- `level9/merge-consolidation.md`
- `level9/cache-correctness.md`
- `level9/checkpoint.md`

## Pass Criteria
- You can explain index query execution independent of row payload scans.
- You can reason about index merge correctness and cache validity.
