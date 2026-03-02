# Level 5D - Read Path, Bloom Filters, and Block Cache

## Objective
Build an efficient read path across memtable + immutable tables with Bloom-based skipping.

## Outcomes
By the end of this phase, you can:
- implement merged read semantics with sequence visibility
- integrate Bloom filters at table/block scope
- add block cache and measure win/loss conditions

## Core Concepts
1. Read merge order across levels/runs.
2. Bloom as prefilter (never final truth).
3. Cache policy tradeoffs (LRU/clock/tinyLFU optional).
4. Tombstone handling in point and range reads.

## The Read Path in VictoriaLogs (Conceptual Background)

### Reading across an LSM: the fundamental problem

A query like `_stream:{host="web-01"} AND level:error AND _time:[10:00, 10:05]` could match data in any part — in-memory, small, or big. The read path must search them all, merge the results, and return only matching rows. The challenge is doing this without reading every byte on disk.

VictoriaLogs solves this through a **pruning cascade**: each level of the storage hierarchy eliminates candidates before the next, more expensive level is consulted. By the time actual column values are decoded, the vast majority of data has already been skipped.

### The pruning cascade

A query passes through six levels of increasingly fine-grained filtering. Each level is cheaper to check and eliminates data before the next level runs:

```
Level 0 — Partition pruning
  Check: does this partition's time range overlap the query?
  Cost: one comparison per partition
  Skips: entire day/week ranges

Level 1 — Part pruning
  Check: does this part's [MinTimestamp, MaxTimestamp] overlap?
  Cost: one comparison per part (from metadata.json, loaded at startup)
  Skips: entire parts (millions of blocks)

Level 2 — Metaindex pruning (in memory)
  Check: does this index block's time range and streamID overlap?
  Cost: scan ~400 entries in RAM
  Skips: ~250 blocks per entry (~2.5M rows per entry)

Level 3 — Block header pruning (one disk read per index block)
  Check: does this block's streamID and time range match?
  Cost: decompress + scan block headers
  Skips: individual blocks (~10,000 rows each)

Level 4 — Bloom filter check (one disk read per column per block)
  Check: could this block contain the query tokens?
  Cost: read bloom filter bytes, compute 6 hash probes per token
  Skips: blocks where a token is definitely absent

Level 5 — Value scan (one disk read per column per block)
  Check: does each row actually match the filter?
  Cost: decompress + decode column values, scan row by row
  Returns: final matching rows
```

A well-targeted query on a large dataset might check 100 parts at Level 1, load 5 index blocks at Level 3, check bloom filters on 20 blocks at Level 4, and decode values for only 3 blocks at Level 5 — reading a few megabytes to answer a query over terabytes.

### Bloom filters: the prefilter

A bloom filter is a compact probabilistic data structure that answers one question: "is this token **definitely absent** or **possibly present**?" It never produces false negatives (if the token is there, the filter says yes), but it can produce false positives (sometimes says yes when the token isn't there).

VictoriaLogs builds one bloom filter per column per block at write time. At query time, before decoding any column values, the filter is checked. If it reports "definitely absent," the entire block is skipped for that column — no values are read from disk.

**How the bloom filter works in VictoriaLogs:**

Each token is hashed using a chain of 6 xxhash calls (a double-hashing technique). Each hash maps to a bit position in a bit array. At write time, those bits are set to 1. At query time, the same bits are checked — if any bit is 0, the token was never inserted.

```
Write time — inserting token "error":
  hash₀("error") → bit 42   → set to 1
  hash₁("error") → bit 187  → set to 1
  hash₂("error") → bit 903  → set to 1
  hash₃("error") → bit 51   → set to 1
  hash₄("error") → bit 672  → set to 1
  hash₅("error") → bit 334  → set to 1

Query time — checking for "error":
  hash₀("error") → bit 42   → is it 1? yes
  hash₁("error") → bit 187  → is it 1? yes
  hash₂("error") → bit 903  → is it 1? no → DEFINITELY ABSENT, skip block
```

The filter is sized at 16 bits per token, which with 6 hash probes gives a false positive rate of approximately 1.5%. This means ~98.5% of blocks that don't contain a query token are correctly skipped.

**Important constraint — token symmetry:** The tokenizer used at query time must be identical to the one used at write time. If "error-connection" is tokenized as `["error", "connection"]` at write time, the query must split it the same way. A mismatch would cause the bloom filter to miss tokens that are actually present — a false negative, which violates the bloom filter's guarantee.

### Dict columns: better than bloom

For columns with 8 or fewer unique values (like `level` with `{error, info, warn, debug}`), VictoriaLogs uses dictionary encoding. The complete dictionary is stored inline in the column header. The reader can check whether the filter value exists in the dictionary without touching any bloom filter or values file:

```
Query: level:critical

Column header for "level":
  valueType: dict
  valuesDict: {0: "error", 1: "info", 2: "warn", 3: "debug"}

  → "critical" not in dictionary → skip block immediately
```

This is a perfect filter (zero false positives) and costs only a handful of string comparisons. Dict columns don't need bloom filters at all — the dictionary is both smaller and more precise.

### Const columns: free pruning

When every row in a block has the same value for a column (e.g., `host = "web-01"` across all entries), VictoriaLogs stores it as a **const column** — the value appears once in the block header. Checking a const column requires no disk I/O beyond what was already loaded for the block header. If the const value doesn't match the filter, the entire block is skipped at zero additional cost.

The filter evaluation checks const columns first, before bloom filters, because they are the cheapest possible check.

### How filters compose: AND and OR

**AND filters** use a two-phase approach:

Phase 1 — **Bloom precheck** (whole-block granularity): Before examining any individual row, the AND filter collects all query tokens across all child filters and checks them against the bloom filter. If any token is definitely absent from the block, the entire block is rejected. Tokens are deduplicated per column — `word:"error" AND phrase:"error connection"` checks `"error"` against the bloom filter once, not twice.

Phase 2 — **Per-row evaluation** (row granularity): Each child filter operates on a bitmap where bit N represents row N. Every row starts as a candidate (all bits set). Each filter clears bits for non-matching rows. If the bitmap becomes all-zeros after any filter, evaluation short-circuits — remaining filters are skipped.

```
Block with 10,000 rows, query: level:error AND msg:timeout

  Start:       bitmap = [1111111111...] (10,000 bits set)

  Phase 1 — bloom precheck:
    tokens for "level": ["error"]    → bloom says possibly present ✓
    tokens for "msg":   ["timeout"]  → bloom says possibly present ✓
    → proceed to Phase 2

  Phase 2 — per-row evaluation:
    level:error  → bitmap = [1010100010...] (3,200 bits set)
    msg:timeout  → bitmap = [1010000010...] (1,800 bits set)
    → return 1,800 matching rows
```

**OR filters** use a complement approach: track which rows have **not yet** been matched. Each OR branch is tried against the unmatched set. Once a row matches any branch, it's removed from the unmatched set. If the unmatched set empties, remaining branches are skipped.

### Lazy loading: read only what you need

Every piece of data in the block search is loaded **lazily and cached** for the duration of one block:

| Data | Loaded when | Cached in |
|------|------------|-----------|
| Column header index | First `getColumnHeader()` call | `cshIndexCache` |
| Individual column header | Filter asks about that column | `cshCache` |
| Bloom filter | Bloom check requested | `bloomFilterCache[columnName]` |
| Column values | Values scan needed (bloom passed) | `valuesCache[columnName]` |
| Timestamps | Timestamp filter applied | `timestampsCache` |

If a block is rejected by a bloom filter check, column values are never loaded. If a query only touches columns `level` and `msg`, columns `host`, `service`, and `trace_id` are never read from disk. The cache is reset between blocks — there is no cross-block cache for values or bloom filters.

### No block cache — by design

VictoriaLogs does not implement an application-level block cache. This is a deliberate choice:

- **In-memory parts** serve as the hot-data tier. Recent data (within the flush interval) is already in RAM as fully formed parts.
- **The OS page cache** provides file-level caching transparently. Frequently accessed bloom filters and index blocks stay in the page cache without VictoriaLogs managing eviction.
- **Log queries are scan-heavy.** Unlike key-value stores where the same block is accessed repeatedly, log queries typically scan broad time ranges. A block cache would churn rapidly, caching blocks that are read once and never again.

This keeps the codebase simpler and avoids the double-caching problem (application cache duplicating the OS page cache), at the cost of less control over what stays hot.

### No tombstones — deletion by merge

Traditional LSM stores use **tombstone records** (markers that say "this key has been deleted") that must be checked during every read. A point query must scan past tombstones until it finds a live value or reaches the bottom level. This adds overhead to every read, even when no deletions have occurred.

VictoriaLogs takes a different approach: deletions are implemented as **merge-time filters**. When a delete request arrives:

1. The system identifies which parts contain matching rows (using the same bloom filter and block search machinery as queries).
2. Those parts are merged, with the delete filter applied during the merge — matching rows are simply omitted from the output.
3. The new parts (without the deleted rows) atomically replace the old parts.

This means the normal read path has **zero deletion overhead**. There are no tombstone records to check, no visibility logic to apply, no dead rows to skip. The trade-off is that deletion is not instant — queries running concurrently with a delete may still see the rows until the merge completes.

### Parallel search across parts

The top-level search fans out across all partitions and parts in parallel:

```
Storage.RunQuery()
  │
  ├── Resolve stream filter → stream IDs (cached per generation)
  │
  ├── Select partitions by time range (binary search)
  │
  └── For each partition (bounded by CPU count):
      │
      └── For each part (in-memory + small + big):
          │
          ├── Metaindex scan (in memory)
          ├── Index block reads (on demand)
          └── Block search work → worker pool
              │
              └── N workers process blocks in batches of 64
                  │
                  └── bitmap-based filter evaluation per block
                      │
                      └── matching rows → pipe chain → output
```

In-memory parts participate in the same search path as disk parts. They implement the same `ReadAt` interface but read from a memory buffer instead of a file. This means the same filter code — bloom checks, column scans, bitmap operations — works identically regardless of whether the data is in RAM or on disk.

Stream ID resolution is cached with a **generation counter**: the cache is invalidated whenever a new stream is registered, ensuring that newly ingested streams are immediately visible to queries.

## VictoriaLogs Anchors
- `lib/logstorage/bloomfilter.go`
- `lib/logstorage/block_search.go`
- `lib/logstorage/filter_and.go`
- `lib/logstorage/filter_phrase.go`

## Labs
### Lab 1: Visibility Rules
Implement and test newest-wins + tombstone masking.

### Lab 2: Bloom Integration
Measure false positives and I/O savings on synthetic and skewed workloads.

### Lab 3: Cache Benchmark
Run read benchmarks with cache off/on and multiple cache sizes.

## Deliverables
- `level5d/read-path-trace.md`
- `level5d/bloom-results.md`
- `level5d/cache-benchmark.md`
- `level5d/checkpoint.md`

## Pass Criteria
- Read correctness and measurable pruning/caching gains are demonstrated.
