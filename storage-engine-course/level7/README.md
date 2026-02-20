# Level 7 - Merge Selection Heuristics and K-Way Merge

## Objective
Understand the algorithms that choose what to merge and how merge execution works.

## Outcomes
By the end of this level, you can:
- explain `appendPartsToMerge` heuristic
- explain `minMergeMultiplier` tradeoff
- explain heap-based block stream merging

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
