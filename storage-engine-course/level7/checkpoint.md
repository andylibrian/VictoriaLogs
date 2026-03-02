# Level 7 Checkpoint

## Context
- Level: 7 - Merge Selection Heuristics and K-Way Merge
- Date: 2026-03-02

## Checkpoint Questions and Answers

### Question 1: Why not merge any available pair of parts immediately?

**Answer:**

Immediate pairwise merging causes **excessive write amplification**.

**Example:** With constant pairwise merging at 1 GB/hour ingestion:
- Each byte is rewritten ~17 times (log₂ of accumulated size)
- Total I/O: ~17 GB written for every 1 GB of new data

**VictoriaLogs approach:**
- Wait for merge ratio ≥ 7.5x (threshold)
- Only merge when output is significantly larger than largest input
- Result: Each byte rewritten O(log_{1.7}(total_size)) times instead of O(log₂(total_size))

**Trade-off:**
- More parts exist at any time (higher read amplification)
- But each byte rewritten fewer times (lower write amplification)
- Better overall I/O efficiency

### Question 2: Why heap-based merge over pairwise repeated merge?

**Answer:**

**Pairwise repeated merge** for 15 parts × 10 MB (150 MB total):
- Requires ⌈log₂(15)⌉ = 4 rounds
- Each round rewrites entire dataset
- Total bytes written: 150 MB × 4 = 600 MB
- Temporary space: up to 150 MB for intermediate results

**K-way heap merge:**
- All 15 parts merged in single pass
- Each block read once, written once
- Total bytes written: 150 MB
- No intermediate files needed

**Result:** 4× less I/O for this example

**Heap overhead:**
- O(log k) comparisons per block (k ≤ 15, so ~4 comparisons)
- Memory: ~2 MB for 15 readers (128 KB each)
- Negligible compared to I/O savings

### Question 3: Why can delete tasks require post-delete force merge for space reclaim?

**Answer:**

**Parts are immutable** - cannot delete rows from existing files.

Space reclamation requires:
1. Creating NEW part without deleted rows
2. Merge is the ONLY mechanism that creates new parts
3. Old parts replaced atomically after merge

**Without force merge:**
- Delete request marks rows for deletion
- Normal merge cycles eventually process parts
- Space reclaimed when merge runs (could be minutes/hours later)

**With force merge:**
- `/delete/force_merge` API triggers immediate merge
- All parts processed in one operation
- Space reclaimed immediately

**Why not delete immediately?**
- Random-access deletion would require rewriting entire files
- Merge-based deletion amortizes cost across normal compaction
- Force merge available when immediate reclamation needed

## Summary

| Concept | Key Insight |
|---------|-------------|
| Merge selection | Conservative (7.5x threshold) to minimize write amplification |
| K-way merge | Single-pass, O(N log k) vs multi-round O(N log² k) |
| Delete mechanism | No tombstones; merge rewrites parts without deleted rows |
| Space reclamation | Requires merge; force merge available for immediate needs |

## Evidence

All three labs demonstrate these concepts:
- Lab 1: Heuristic algorithm with no-merge examples
- Lab 2: Heap merge trace with complexity analysis
- Lab 3: Delete timeline and space reclamation

## Conclusions

1. **Merge selection is optimization** - balance read vs write amplification
2. **K-way merge is efficient** - single pass with minimal overhead
3. **Deletion is merge-driven** - no tombstones, physical removal on merge
4. **Force merge is escape hatch** - immediate space reclamation when needed

## Open Questions

- Should the 7.5x threshold be configurable?
- How to optimize delete latency for compliance requirements?
- Impact of k-way merge with thousands of source parts?
