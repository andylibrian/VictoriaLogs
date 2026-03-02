# Level 5A - LSM Mental Model, Invariants, and Simulator

## Objective
Establish rigorous LSM fundamentals and build a simulator that predicts behavior before writing storage code.

## Outcomes
By the end of this phase, you can:
- define LSM invariants precisely
- quantify write amplification/read amplification trends
- explain why/when compaction should happen

## Core Concepts
1. Immutable run model.
2. Memtable -> immutable run flush transition.
3. Cost dimensions:
   - write amplification
   - read amplification
   - space amplification
4. Policy knobs:
   - flush size
   - parts-to-merge
   - compaction trigger thresholds

## Why an LSM? (Conceptual Background)

### The core trade-off: writes vs reads

Databases face a fundamental choice. You can write data exactly where readers expect it (sorted order), paying the cost at write time. Or you can write data wherever is fastest (append order), paying the cost at read time when you must search unsorted data.

An LSM tree picks the second option and then claws back read performance through background reorganization. Incoming logs land in an in-memory buffer (memtable) and get flushed to disk as immutable, sorted files called **parts**. Reads must check every part because any part might contain relevant data. Compaction merges parts together, reducing the number a reader must check.

This makes LSM trees a natural fit for log storage: logs arrive at high throughput, are mostly written once, and are queried far less often than they are ingested.

### Immutable parts: the building block

Once a part is flushed to disk, it is never modified — only replaced by a merged successor. This is the **immutable run model**. It simplifies concurrency (readers never conflict with writers), crash recovery (a part is either fully written or absent), and compression (data is written once, optimally encoded).

In VictoriaLogs, a part on disk is a directory containing:
```
part/
  metaindex.bin      ← in-memory index of block header groups
  index.bin          ← block headers (time ranges, stream IDs, offsets)
  timestamps.bin     ← compressed timestamp columns
  columns_header.bin ← column metadata
  field_values.bin   ← compressed column data
  field_bloom.bin    ← bloom filters for filtering
  message_bloom.bin  ← bloom filters for _msg field
```

Every file is written once during flush or merge, then only read. Deletion happens atomically when a merge replaces the source parts.

### Three tiers, three speeds

VictoriaLogs organizes parts into three tiers based on size:

| Tier | Location | Size limit | Purpose |
|------|----------|------------|---------|
| In-memory | RAM | ~10% of allowed memory / `maxInmemoryPartsPerPartition` (20), min 1 MB per part target | Absorb burst ingestion |
| Small | Disk (page cache friendly) | Scales with free RAM (min 10 MB) | Fast merge of recent data |
| Big | Disk (sequential I/O) | 1 TB (or available disk space) | Long-term storage |

Each tier has its own background merge worker and concurrency limit (bounded by CPU count). This prevents a slow big-part merge from blocking fast in-memory merges, keeping ingestion latency stable.

### The data flow

```
Log entries arrive
       │
       ▼
  ┌──────────┐    timer (1s) or
  │  Shard   │    buffer full
  │  Buffer  │ ──────────────────┐
  └──────────┘                   │
                                 ▼
                        ┌─────────────────┐
                        │  In-memory Part  │  (immutable, sorted)
                        └────────┬────────┘
                                 │  merge when count > threshold
                                 ▼
                        ┌─────────────────┐
                        │   Small Part    │  (on disk)
                        └────────┬────────┘
                                 │  merge when size exceeds threshold
                                 ▼
                        ┌─────────────────┐
                        │    Big Part     │  (on disk)
                        └────────┬────────┘
                                 │  merge continues up to 1 TB
                                 ▼
                            final form
```

Backpressure appears when merge and flush workers are saturated: shard flushes block, which eventually slows ingestion. `maxInmemoryPartsPerPartition` is used for in-memory part sizing, not as a hard 20-part stall threshold in `datadb`.

### Write amplification: the cost of merging

Every merge rewrites data. **Write amplification** is the ratio of total bytes written to disk over the lifetime of the data vs the original data size. If a 1 MB part gets merged into a 2 MB part, then that 2 MB part merges into a 4 MB part, and so on, the original 1 MB was rewritten at every level.

Naive binary merging (always merge 2 parts) produces write amplification of O(log₂(N)), where N is the final data size. For 100 GB of data starting from 1 MB parts, that is ~17 rewrites per byte.

VictoriaLogs uses `minMergeMultiplier = 1.7` as a floor and size guard, but with `defaultPartsToMerge = 15` the effective acceptance threshold is higher: a candidate merge must have ratio `output_size / largest_input_size >= 7.5` to run.

```
Scenario: three candidate merges

  Option A: merge [10 MB x 8 parts]       → 80 MB output, ratio = 80/10 = 8.0×  ✓
  Option B: merge [10 MB x 4 parts]       → 40 MB output, ratio = 40/10 = 4.0×  ✗ rejected
  Option C: merge [1 MB, 100 MB]          → 101 MB output, ratio = 101/100 = 1.01× ✗ rejected
```

Option C rewrites 100 MB to absorb just 1 MB of new data — almost all wasted I/O. The ratio threshold blocks this class of merge and favors larger, more balanced fan-in windows.

### Read amplification: the cost of not merging

While merging reduces part count, **not** merging enough increases **read amplification** — the number of parts a query must examine. A point query for a specific log entry must check the bloom filter or scan the index of every part that could contain it.

```
After ingesting 1 GB of logs with different policies:

  Eager merge (merge any 2 parts):
    Part count: ~3 parts       ← great for reads
    Write amp:  ~17×           ← terrible for writes

  Never merge:
    Part count: ~1000 parts    ← terrible for reads
    Write amp:  1×             ← great for writes

  Effective 7.5× threshold (with default fan-in), up to 15 parts per merge:
    Part count: ~10-20 parts   ← good for reads
    Write amp:  ~5-8×          ← reasonable for writes
```

This is the fundamental LSM tension: every merge dollar spent reducing read amplification increases write amplification, and vice versa. The merge policy is the knob that controls this trade-off.

### Space amplification: the temporary cost

During a merge, both the source parts and the new destination part exist simultaneously on disk. If you are merging 5 parts of 100 MB each into one 500 MB part, you temporarily need 1 GB (500 MB old + 500 MB new). The old parts are deleted only after the new part is fully written and fsynced.

Space amplification = (total disk used) / (logical data size). During a merge, old and new bytes coexist temporarily; for a full rewrite of a merge set, transient usage is close to 2× for that set (old + new). After source parts are removed, usage returns near steady-state.

### The merge selection algorithm

VictoriaLogs uses `appendPartsToMerge` to pick which parts to merge. The algorithm:

1. **Sort** parts by size (ascending), then by time (descending for same-size parts).
2. **Try all windows** of `ceil(maxSrcParts/2)` to `maxSrcParts` consecutive parts (`maxSrcParts` is up to 15).
3. **Score each window** by merge ratio (output size / largest input size).
4. **Reject** all windows unless the best ratio meets `max(defaultPartsToMerge/2, minMergeMultiplier)` (7.5× with defaults).
5. **Pick the window** with the highest ratio — the most efficient merge.

Why consecutive parts after sorting by size? Parts of similar size are adjacent, and merging similar-sized parts maximizes the merge ratio. Merging a 1 KB part with a 1 GB part wastes almost all I/O rewriting the 1 GB part.

Why up to 15 parts at once (`defaultPartsToMerge = 15`)? Higher fan-in produces larger output parts in fewer merge steps, reducing total write amplification. But it also uses more memory and I/O bandwidth during the merge. 15 is the chosen balance point.

### Policy knobs in VictoriaLogs

| Knob | Value | Effect |
|------|-------|--------|
| `minMergeMultiplier` | 1.7 | Floor used in merge gating and max input-part size filtering |
| `defaultPartsToMerge` | 15 | Maximum parts merged in one operation; implies 7.5× minimum accepted ratio with current code |
| `maxInmemoryPartsPerPartition` | 20 | Used to size target in-memory part size (`~10% RAM / 20`) |
| `maxBigPartSize` | 1 TB | Upper bound on a single part |
| Flush interval | ~1 second per shard | How often in-memory data becomes a part |

Changing any of these shifts the balance between write amplification, read amplification, and space amplification. The labs below let you explore these trade-offs with a simulator before touching storage code.

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: constants and merge constraints
- `lib/logstorage/datadb.go`: `appendPartsToMerge`

## Labs
### Lab 1: Event-Driven LSM Simulator
Simulate operations:
- `PUT`, `FLUSH`, `COMPACT`

Track per step:
- run count
- bytes written due to compaction
- estimated point-read probes

### Lab 2: Policy Sweep
Run multiple policies and compare:
- eager merge
- multiplier-threshold merge
- bounded fan-in merge

### Lab 3: Invariant Spec
Write machine-checkable invariants for simulator state.

## Deliverables
- `level5a/invariants.md`
- `level5a/simulator-v1.md`
- `level5a/policy-sweep.md`
- `level5a/checkpoint.md`

## Pass Criteria
- You can predict LSM behavior under policy changes before coding storage files.
