# Level 5E - Compaction Engine and Policy Control

## Objective
Implement compaction picker/executor and analyze policy tradeoffs deeply.

## Outcomes
By the end of this phase, you can:
- implement compaction planning and execution
- support at least one tiered and one leveled policy
- quantify amplification and latency effects per policy

## Core Concepts
1. Candidate selection constraints.
2. Output run generation and atomic install.
3. Overlap management (leveled) vs fan-in grouping (tiered).
4. Compaction debt and scheduling fairness.

## Compaction in VictoriaLogs (Conceptual Background)

### Tiered vs leveled: two schools of compaction

The two classic compaction strategies offer opposite trade-offs:

**Leveled compaction** (used by LevelDB, RocksDB): each level has a strict size limit (e.g., level 1 = 10 MB, level 2 = 100 MB). When a level exceeds its limit, one file from that level is merged with all overlapping files from the next level. This keeps each level sorted and non-overlapping, giving excellent read performance (at most one file per level to check). The cost is high write amplification — every byte may be rewritten multiple times as it cascades through levels.

**Size-tiered compaction** (used by Cassandra, VictoriaLogs): parts of similar size are merged together, producing a larger part. There are no strict level boundaries — only size-based grouping. This produces lower write amplification because each merge consolidates similarly-sized parts. The cost is higher read amplification — multiple parts at each size tier may overlap, requiring queries to check all of them.

VictoriaLogs uses **size-tiered compaction** with three fixed tiers (in-memory, small, big). This choice aligns with log workloads: logs are write-heavy and scan-heavy, making low write amplification more valuable than minimizing per-query part lookups.

### The three tiers as a promotion path

Parts flow through the tiers based on size, not age:

```
In-memory parts (RAM)
  │  merge when parts accumulate
  │  output > maxInmemoryPartSize?
  ▼
Small parts (disk, page-cache friendly)
  │  merge when similar-sized parts accumulate
  │  output > maxSmallPartSize?
  ▼
Big parts (disk, sequential I/O)
  │  merge continues up to 1 TB
  ▼
Final form
```

The tier transition is decided by `getDstPartType` after each merge:

| Condition | Output tier |
|-----------|-------------|
| Output size > `maxSmallPartSize` | Big |
| `isFinal` (shutdown) or output size > `maxInmemoryPartSize` | Small |
| Any source part is on disk | Small (preserving durability) |
| All sources in memory and output is small | In-memory |

The durability rule is important: once data reaches disk, it never goes back to memory. Merging two small parts produces a small or big part, never an in-memory part. This prevents a merge from silently making durable data volatile.

### The merge candidate selection algorithm

The core question is: given N available parts, which subset should be merged next? A bad choice wastes I/O rewriting data for little benefit. VictoriaLogs answers this with `appendPartsToMerge`, which finds the most efficient merge through exhaustive search.

**Step 1 — Filter.** Remove parts too large to participate. A part can only be in the merge if the output would be at least 1.7× its size. So any part larger than `maxOutputSize / 1.7` is excluded.

**Step 2 — Sort by size.** Parts are sorted ascending by compressed size, with ties broken by newer timestamp first. This places similarly-sized parts adjacent to each other.

**Step 3 — Try all consecutive windows.** The algorithm tries every window of 2 to 15 consecutive parts in the sorted list. For each window, it computes the merge ratio: total output size divided by the largest input part.

```
Sorted parts (by size):
  [1 MB] [1 MB] [2 MB] [3 MB] [5 MB] [8 MB] [50 MB] [200 MB]

Window [1 MB, 1 MB, 2 MB, 3 MB, 5 MB]:
  output = 12 MB, largest input = 5 MB, ratio = 2.4×

Window [1 MB, 2 MB, 3 MB, 5 MB, 8 MB]:
  output = 19 MB, largest input = 8 MB, ratio = 2.4×

Window [1 MB, 1 MB, 2 MB, 3 MB, 5 MB, 8 MB]:
  output = 20 MB, largest input = 8 MB, ratio = 2.5×  ← better

Window [1 MB, 1 MB]:
  output = 2 MB, largest input = 1 MB, ratio = 2.0×

Window [50 MB, 200 MB]:
  output = 250 MB, largest input = 200 MB, ratio = 1.25× ← rejected
```

**Step 4 — Reject unbalanced windows.** A window is skipped if the smallest part multiplied by the part count is less than the largest part. This prevents merging 14 tiny parts with one huge part — the huge part would be rewritten for almost no benefit.

**Step 5 — Apply threshold.** The winning window must have a ratio of at least `defaultPartsToMerge / 2 = 7.5×`. This is a very conservative threshold: VictoriaLogs only merges when the total output is at least 7.5× the largest input. The `minMergeMultiplier = 1.7` constant acts as a floor for this calculation but in practice the effective threshold is 7.5×.

**Why consecutive windows?** After sorting by size, similarly-sized parts are adjacent. Merging similar-sized parts maximizes the ratio because every part contributes meaningfully to the output. Merging a 1 KB part with a 1 GB part yields a ratio of ~1.0× — almost all the I/O goes to rewriting the 1 GB part.

**Why exhaustive search?** With typical part counts under 100, the O(N²) search is fast enough to be invisible. It finds the globally optimal merge rather than settling for a greedy local choice.

### The merge execution: heap-based block stream merging

Once parts are selected, the actual merge is a k-way merge-sort of sorted block streams. Each source part provides a stream of blocks sorted by `(streamID, minTimestamp)`. A min-heap produces blocks in global sorted order:

```
Source readers (one per part being merged):
  Part A: [block₁(stream=S1,t=1)] [block₂(stream=S1,t=5)] [block₃(stream=S2,t=3)]
  Part B: [block₁(stream=S1,t=2)] [block₂(stream=S1,t=4)] [block₃(stream=S3,t=1)]
  Part C: [block₁(stream=S1,t=3)] [block₂(stream=S2,t=1)]

Min-heap pops blocks in order:
  (S1,t=1) from A → (S1,t=2) from B → (S1,t=3) from C → (S1,t=4) from B → ...
```

The merger has a critical optimization: **full blocks pass through without decompression**. If a block is already at the target size (≥ 2 MB uncompressed), has no rows matching a delete filter, and the accumulator is empty, the block's raw compressed bytes are written directly to the output. No decode, no re-encode — just a byte copy. This saves significant CPU during compaction of large parts.

Small blocks are decompressed and accumulated. When the accumulated data reaches the target block size, it's flushed as a new block. This ensures the output has uniformly sized blocks even when merging many small parts with undersized blocks.

```
Block processing decision tree:

  Is this block from a different stream than the accumulator?
    → Flush accumulator, start new stream

  Is the accumulator empty AND block ≥ 2 MB AND no delete filter match?
    → FAST PATH: write block bytes directly (zero copy)

  Would accumulator + this block exceed 2× target size?
    → Flush accumulator, then handle this block

  Otherwise:
    → Decompress block, merge-sort rows into accumulator
    → If accumulator ≥ target size, flush
```

The two-buffer swap technique avoids allocation during row merging: `rows` and `rowsTmp` alternate roles, so the merge result is always written into a pre-allocated buffer.

### Preventing concurrent merge conflicts

Two concurrent merges must never select the same part. VictoriaLogs uses a simple mechanism: a boolean `isInMerge` flag on each part wrapper, set under the global `partsLock` mutex.

```
Merge Worker A                         Merge Worker B
─────────────                          ─────────────
partsLock.Lock()                       (waiting for lock)
  scan parts where isInMerge=false
  select [P1, P2, P3]
  set P1.isInMerge = true
  set P2.isInMerge = true
  set P3.isInMerge = true
partsLock.Unlock()
                                       partsLock.Lock()
                                         scan parts where isInMerge=false
                                         P1, P2, P3 are excluded
                                         select [P4, P5, P6]
                                         set isInMerge = true for each
                                       partsLock.Unlock()

(merge P1+P2+P3 in parallel)          (merge P4+P5+P6 in parallel)
```

On merge completion (or failure), `releasePartsToMerge` resets the flags under the lock. A `defer` ensures cleanup even if the merge panics.

### The atomic swap

After the merge produces a new part, the old parts must be replaced atomically. No query should ever see a state where both old and new parts exist (double-counting) or neither exists (missing data).

`swapSrcWithDstParts` does this under `partsLock`:

1. **Remove** all source parts from the tier lists (in-memory, small, big).
2. **Insert** the new part into the appropriate tier list.
3. **Persist** the updated file-part list to `parts.json` (atomic write-then-rename).
4. **Trigger** the next merge check for the destination tier.

All four steps happen while holding the lock. Queries acquire the lock to snapshot the part list, so they see either all old parts or the new part — never a mix.

Source parts are not deleted immediately. Each part has a reference counter incremented by active queries. `mustDrop` is set to `true`, and `decRef()` is called. If no query holds a reference, the part directory is deleted immediately. If a query is mid-scan, the part stays alive until the query releases its reference, then is deleted.

### Event-driven scheduling

VictoriaLogs does not use a compaction scheduler, priority queue, or debt calculation. Instead, merge workers are spawned on demand:

1. New in-memory part created → `startInmemoryPartsMergerLocked()` spawns a goroutine.
2. Merge completes, new part inserted into small tier → `startSmallPartsMergerLocked()` spawns a goroutine.
3. Small merge completes, new part inserted into big tier → `startBigPartsMergerLocked()` spawns a goroutine.

Each goroutine loops: pick parts, merge, repeat. When `appendPartsToMerge` finds nothing worth merging, the goroutine exits. The next part arrival triggers a new goroutine.

Concurrency is bounded per tier by buffered channels sized to the CPU count. With 8 CPUs, at most 8 in-memory merges, 8 small merges, and 8 big merges run simultaneously — across all partitions.

```
Per-tier concurrency (8-CPU machine):

  inmemoryPartsConcurrencyCh: [slot][slot][slot][slot][slot][slot][slot][slot]
  smallPartsConcurrencyCh:    [slot][slot][slot][slot][slot][slot][slot][slot]
  bigPartsConcurrencyCh:      [slot][slot][slot][slot][slot][slot][slot][slot]

  Each merge acquires one slot before starting, releases it when done.
  If all slots are full, the merge goroutine blocks until one opens.
```

This design has no starvation prevention — a burst of big merges can occupy all big-tier slots, delaying other big merges. But the tier separation means big merges never block in-memory or small merges, keeping the ingestion pipeline responsive.

### Disk space reservation

Before starting a merge, the system reserves disk space by atomically incrementing a global counter. If the reservation would exceed available disk space, the merge is skipped (unless it's a final shutdown flush, which proceeds regardless). This prevents compaction from filling the disk and crashing the system.

```
Available disk: 100 GB
Current reservation: 60 GB
Merge wants: 50 GB
  → 60 + 50 = 110 > 100 → skip this merge, try again later

Merge wants: 30 GB
  → 60 + 30 = 90 ≤ 100 → reserve, proceed with merge
```

The reservation is released when the merge completes and old parts are deleted, freeing actual disk space.

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: `appendPartsToMerge`, `mustMergePartsInternal`
- `lib/logstorage/block_stream_merger.go`

## Labs
### Lab 1: Picker Design
Implement compaction picker with deterministic tie-breaking.

### Lab 2: Tiered vs Leveled
Replay same workload under both policies and compare:
- write amp
- read amp
- space amp

### Lab 3: Stall Control
Add simple throttling or write stalls when compaction debt is high.

## Deliverables
- `level5e/compaction-picker.md`
- `level5e/policy-comparison.md`
- `level5e/stall-control.md`
- `level5e/checkpoint.md`

## Pass Criteria
- Compaction is correct under overlap constraints and policy outcomes are quantified.
