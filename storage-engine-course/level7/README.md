# Level 7 - Merge Selection Heuristics and K-Way Merge

## Objective
Understand the algorithms that choose what to merge and how merge execution works.

## Outcomes
By the end of this level, you can:
- explain `appendPartsToMerge` heuristic
- explain `minMergeMultiplier` tradeoff
- explain heap-based block stream merging

## Merge Selection and Execution in Depth (Conceptual Background)

### Why not merge everything immediately?

The simplest compaction policy is: whenever two parts exist, merge them. This minimizes read amplification (fewer parts to check per query) but maximizes write amplification (every byte is rewritten on every merge). For an ingestion rate of 1 GB/hour with constant pairwise merging, the system would rewrite ~17 GB for every 1 GB of new data (log₂ of accumulated size).

The opposite extreme — never merge — gives perfect write amplification (1×) but terrible read amplification. Every part ever flushed would need to be checked by every query.

Merge selection is the problem of finding the sweet spot: merge *enough* to keep read costs manageable, but *not so much* that write costs dominate. VictoriaLogs frames this as a constrained optimization: among all possible subsets of parts, find the subset where merging produces the most "useful work" per byte written.

### The `appendPartsToMerge` algorithm in detail

The algorithm has five steps. Each step narrows the search space or evaluates candidates:

**Step 1 — Size filter.** Remove parts too large to benefit from merging. A part can only participate if the merge output would be at least 1.7× its size (the `minMergeMultiplier`). This means any part larger than `maxOutBytes / 1.7` is excluded upfront.

```
maxOutBytes = 500 MB, minMergeMultiplier = 1.7
maxInPartBytes = 500 / 1.7 ≈ 294 MB

Parts: [2 MB] [5 MB] [10 MB] [50 MB] [300 MB] [400 MB]
                                        ↑          ↑
                                     excluded    excluded
Remaining: [2 MB] [5 MB] [10 MB] [50 MB]
```

**Step 2 — Sort for adjacency.** Parts are sorted ascending by compressed size. Ties are broken by descending timestamp (newer first). This places similarly-sized parts next to each other, which is critical for Step 4.

```
After sort: [2 MB] [5 MB] [10 MB] [50 MB]
              ↑      ↑       ↑       ↑
            similar sizes are adjacent
```

Why descending timestamp on ties? When two parts have the same compressed size, the newer one is placed first. During merge, this means newer data (more likely to be queried) ends up in the same output block as other similarly-aged data, improving temporal locality.

**Step 3 — Window bounds.** The algorithm considers windows of `minSrcParts` to `maxSrcParts` consecutive parts. `maxSrcParts = min(defaultPartsToMerge, len(src))` where `defaultPartsToMerge = 15`. `minSrcParts = (maxSrcParts + 1) / 2`, with a floor of 2. This means:
- With 15+ parts available: try windows of 8 to 15 parts
- With 6 parts available: try windows of 4 to 6 parts
- With 3 parts available: try windows of 2 to 3 parts

**Step 4 — Exhaustive search.** Try every consecutive window within the bounds. For each window:

```
For each window size i from minSrcParts to maxSrcParts:
  For each starting position j from 0 to len(src)-i:
    window = src[j : j+i]

    Balance check: smallest × count < largest?
      If yes → skip (too lopsided)

    Total output = sum of all part sizes in window
      If output > maxOutBytes → break (further windows only get bigger)

    Merge ratio = output / largest part in window
      If ratio > best so far → save this window as the best candidate
```

The **balance check** rejects windows where tiny parts would be merged with a disproportionately large one. For example, `[1 KB, 1 KB, 1 KB, 1 KB, 100 MB]`: the smallest (1 KB) × count (5) = 5 KB, which is far less than the largest (100 MB). This merge would rewrite 100 MB to absorb 4 KB of new data — almost entirely wasted I/O.

Why **consecutive** windows? After sorting by size, the parts in any consecutive window are the most similar in size. Similar-sized parts produce the best merge ratio because each part contributes a significant fraction of the output. A window like `[10 MB, 10 MB, 10 MB]` has ratio 3.0×, while `[1 KB, 10 MB, 10 MB]` has ratio ~2.0× — the 1 KB part contributes nothing but the 10 MB parts still get fully rewritten.

**Step 5 — Threshold gate.** The winning window must have a merge ratio of at least `max(minMergeMultiplier, defaultPartsToMerge / 2)`. Since `15 / 2 = 7.5` and `7.5 > 1.7`, the effective threshold is **7.5×**. If no window meets this bar, no merge happens — the function returns nothing.

```
Best candidate: [5 MB, 5 MB, 5 MB, 5 MB, 5 MB]
  Output = 25 MB, largest = 5 MB, ratio = 5.0×
  Threshold = 7.5×
  5.0 < 7.5 → rejected, no merge

Best candidate: [2 MB, 2 MB, 2 MB, 2 MB, 2 MB, 2 MB, 2 MB, 2 MB, 2 MB, 2 MB]
  Output = 20 MB, largest = 2 MB, ratio = 10.0×
  10.0 ≥ 7.5 → accepted
```

This high threshold is why VictoriaLogs merges conservatively. It waits for a substantial improvement before spending I/O. The trade-off: more parts exist at any given time (higher read amplification), but each byte is rewritten fewer times over its lifetime (lower write amplification).

### Three examples where no merge is selected

Understanding when the algorithm says "no" is as important as understanding when it says "yes":

**Example 1 — All parts are large and similarly sized:**
```
Parts: [100 MB] [110 MB] [105 MB]
maxOutBytes = 500 MB → maxInPartBytes = 294 MB → all pass size filter
Best window: [100 MB, 105 MB, 110 MB]
  Output = 315 MB, ratio = 315/110 = 2.86×
  2.86 < 7.5 → no merge
```
Three 100 MB parts would produce a 315 MB part. The ratio is only 2.86× — not enough improvement to justify rewriting 315 MB.

**Example 2 — One huge part dominates:**
```
Parts: [1 MB] [2 MB] [500 MB]
maxOutBytes = 1 TB → maxInPartBytes = 588 GB → all pass size filter
Window [1 MB, 2 MB]: output = 3 MB, ratio = 1.5× < 7.5 → rejected
Window [1 MB, 2 MB, 500 MB]: balance check: 1 × 3 = 3 < 500 → skip (lopsided)
Window [2 MB, 500 MB]: balance check: 2 × 2 = 4 < 500 → skip (lopsided)
→ no merge
```
The 500 MB part is too large relative to the others. No balanced window exists.

**Example 3 — Only one part:**
```
Parts: [50 MB]
len(src) < 2 → return immediately, no merge
```
A merge requires at least 2 input parts.

### The `isInMerge` protocol: preventing double-selection

`getPartsToMergeLocked` wraps the selection algorithm with a critical safety protocol:

```
partsLock.Lock()

  1. Filter: collect only parts where isInMerge == false
  2. Call appendPartsToMerge on the filtered list
  3. Set isInMerge = true on every selected part

partsLock.Unlock()
```

All three steps happen atomically under the lock. If two merge workers run concurrently:

```
Worker A (under lock):              Worker B (waiting for lock):
  sees [P1, P2, P3, P4, P5]
  selects [P1, P2, P3]
  P1.isInMerge = true
  P2.isInMerge = true
  P3.isInMerge = true
  releases lock
                                    Worker B (acquires lock):
                                      sees [P4, P5] (P1-P3 filtered out)
                                      selects [P4, P5] or nothing
                                      releases lock
```

No part can appear in two concurrent merges. On merge completion (success or failure), `releasePartsToMerge` resets the flags under the lock, making the parts available again.

### K-way merge: the heap-based block stream merger

Once parts are selected, the merger must combine their sorted block streams into a single sorted output. This is the classic k-way merge problem, solved with a min-heap.

**Setup:** Each source part provides a `blockStreamReader`. The reader is positioned on its first block. All readers with data are pushed into a min-heap ordered by `(streamID, minTimestamp)`:

```
Source Part A: blocks sorted by (streamID, timestamp)
  [S1:t=1] [S1:t=5] [S2:t=3]

Source Part B:
  [S1:t=2] [S1:t=4] [S3:t=1]

Source Part C:
  [S1:t=3] [S2:t=1]

Initial heap (min at top):
  ┌──────────┐
  │ A: S1:t=1│  ← minimum
  ├──────────┤
  │ B: S1:t=2│
  ├──────────┤
  │ C: S1:t=3│
  └──────────┘
```

**Main loop:** Pop the minimum block, write it to the output, advance that reader, re-sift the heap:

```
Iteration 1: pop A(S1:t=1), write it, advance A to S1:t=5, heap.Fix(0)
  Heap: [B:S1:t=2, C:S1:t=3, A:S1:t=5]

Iteration 2: pop B(S1:t=2), write it, advance B to S1:t=4, heap.Fix(0)
  Heap: [C:S1:t=3, B:S1:t=4, A:S1:t=5]

Iteration 3: pop C(S1:t=3), write it, advance C to S2:t=1, heap.Fix(0)
  Heap: [B:S1:t=4, A:S1:t=5, C:S2:t=1]
  ...
```

`heap.Fix(0)` is O(log k) where k is the number of source parts (at most 15). This is the optimal approach — pairwise repeated merge would be O(N log² k) instead of O(N log k).

**The heap comparator** sorts by `streamID` first, then by `minTimestamp`. This is critical: all blocks for the same stream are emitted contiguously in timestamp order, which is the ordering invariant the index requires. If the comparator were timestamp-first, blocks from different streams would interleave, breaking the `(streamID, timestamp)` sort order of the output.

### `mustWriteBlock`: the fast path vs merge path decision

Not every block needs to be decompressed during a merge. The merger makes a per-block decision:

```
Block arrives from heap
│
├── Different streamID than accumulator?
│     → Flush accumulated rows for old stream
│       Start new stream context
│       Fall through to write/accumulate this block
│
├── Accumulator empty AND block ≥ 2 MB AND no delete filter match?
│     → FAST PATH: write raw compressed bytes directly to output
│       No decompression, no re-encoding, no re-compression.
│       Just a byte copy from source file to destination file.
│
├── Accumulator + this block would exceed 4 MB?
│     → Flush accumulator first, then handle this block
│
└── Otherwise:
      → MERGE PATH: decompress block into rows
        Merge-sort with accumulated rows (two-buffer swap technique)
        If accumulated data ≥ 2 MB → flush as new output block
```

The **fast path** is the key optimization. During compaction of large parts with well-formed blocks, most blocks are already at target size and pass through as raw bytes. A merge of two 100 MB parts where 90% of blocks are full-size copies only ~10% of the data through the decompress/re-compress path, saving significant CPU.

The **two-buffer swap** in the merge path avoids allocation:
```go
bsm.rowsTmp.mergeRows(existing[:n], new[n:])
bsm.rows, bsm.rowsTmp = bsm.rowsTmp, bsm.rows  // swap, no allocation
bsm.rowsTmp.reset()                               // reuse old buffer next time
```

### Delete-aware merge: how rows are physically removed

Log deletion in VictoriaLogs doesn't use tombstone records. Instead, a delete task triggers a targeted merge with a `dropFilter` — the same filter type used by queries. During the merge, `mustWriteBlockData` checks each block:

```
mustWriteBlockData(bd)
│
├── Does dropFilter match this block's streamID?
│     │
│     ├── Is the filter a simple stream-only filter (filterNoop)?
│     │     → Drop entire block without decompression
│     │       (all rows in the block belong to the deleted stream)
│     │
│     └── Filter needs per-row evaluation?
│           → Decompress block into rows
│             Apply filter to each row
│             Keep non-matching rows, drop matching ones
│             Re-block and write survivors
│
└── No match → write block normally (fast path or accumulate)
```

The stream-only fast path is important: if deleting all logs for a specific stream, entire blocks can be dropped by checking the stream ID in the block header — no decompression needed. Only filters that require examining row contents (e.g., `level:error AND msg:timeout`) force the expensive per-row path.

After the merge completes, the old parts (containing the deleted rows) are replaced by the new part (without them). The rows are physically gone — no tombstone tracking, no future garbage collection pass. But until the merge finishes, queries may still return the soon-to-be-deleted rows. The `DeleteTask.StartTime` field bounds this: rows ingested after the delete request was issued are never deleted, even if they match the filter.

### Why heap-based merge over pairwise repeated merge?

Consider merging 15 parts of 10 MB each (150 MB total):

**Pairwise repeated merge:** Merge pairs recursively — (P1+P2)→T1, (P3+P4)→T2, ..., then (T1+T2)→U1, ..., and so on. This requires ⌈log₂(15)⌉ = 4 rounds. Each round rewrites the entire dataset. Total bytes written: 150 MB × 4 = 600 MB. Temporary space needed: up to 150 MB for intermediate results.

**K-way heap merge:** All 15 parts are merged in a single pass. Each block is read once and written once. Total bytes written: 150 MB. No intermediate files needed.

The k-way merge writes 4× less data for this example, and the advantage grows with part count. The heap adds O(log 15) ≈ 4 comparisons per block, but this is negligible compared to the I/O savings. The only downside is memory: all 15 readers must be open simultaneously, each holding one decompressed index block. At ~128 KB per reader, this is ~2 MB — trivial.

## Source Anchors
- `lib/logstorage/datadb.go`
  - `appendPartsToMerge`
  - `sortPartsForOptimalMerge`
  - `getPartsToMergeLocked`
- `lib/logstorage/block_stream_merger.go`
  - `mustMergeBlockStreams`
  - `blockStreamReadersHeap`
  - `mustWriteBlock`
  - `mustMergeRows`

## Core Concepts
1. Candidate selection as constrained optimization.
2. Merge rejection to avoid expensive low-value compactions.
3. K-way merge over sorted block streams using heap.
4. Row-drop integration during merge for delete tasks.

## Guided Reading Tasks
1. In `appendPartsToMerge`, explain:
   - pre-filtering by size
   - exhaustive search window
   - multiplier threshold checks
2. In heap comparator (`Less`), explain sorting key order and why it is correct.
3. In `mustWriteBlock`, explain fast path vs merge path decisions.
4. In `mustUnmarshalRows`, explain where drop filter gets applied.

## Hands-On Lab
### Lab 1: Heuristic Reimplementation
Re-implement merge candidate selection for synthetic part sizes.
Provide at least 3 examples where no merge is selected.

### Lab 2: Heap Merge Walkthrough
Create a manual trace of heap state transitions for 3 input streams.

### Lab 3: Delete-Aware Merge
Explain how deleted rows become physically removed only through merge.

## Checkpoint Questions
1. Why not merge any available pair of parts immediately?
2. Why heap-based merge over pairwise repeated merge?
3. Why can delete tasks require post-delete force merge for space reclaim?

## Deliverables
- `level7/heuristic-reimplementation.md`
- `level7/heap-merge-trace.md`
- `level7/delete-aware-merge.md`
- `level7/checkpoint.md`

## Pass Criteria
- You can predict when a merge will or will not happen.
- You can explain merge correctness invariants under stream/time ordering.
