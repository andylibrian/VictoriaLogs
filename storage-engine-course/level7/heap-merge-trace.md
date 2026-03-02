# Level 7 Heap Merge Trace

## Context
- Level: 7 - Merge Selection Heuristics and K-Way Merge
- Date: 2026-03-02

## Input Streams

### Part A Blocks (sorted by streamID, timestamp)

```
[A1] S1:t=1-5, rows=100
[A2] S1:t=5-10, rows=100
[A3] S2:t=3-8, rows=50
```

### Part B Blocks

```
[B1] S1:t=2-4, rows=80
[B2] S1:t=4-7, rows=90
[B3] S3:t=1-3, rows=60
```

### Part C Blocks

```
[C1] S1:t=3-6, rows=70
[C2] S2:t=1-5, rows=40
```

## Heap Merge Trace

### Initial State

```
Heap (min-heap ordered by (streamID, minTimestamp)):
  [0] A(S1:t=1-5)  ← minimum
  [1] B(S1:t=2-4)
  [2] C(S1:t=3-6)
```

### Iteration 1

```
Action: Pop A, write block A1
Output: Part A → write S1:t=1-5

Advance A to next block (S1:t=5-10)
Heap.Fix(0) reorders heap

Heap:
  [0] B(S1:t=2-4)  ← new minimum
  [1] A(S1:t=5-10)
  [2] C(S1:t=3-6)
```

### Iteration 2

```
Action: Pop B, write block B1
Output: Part B → write S1:t=2-4

Advance B to next block (S1:t=4-7)
Heap.Fix(0) reorders heap

Heap:
  [0] C(S1:t=3-6)  ← new minimum
  [1] B(S1:t=4-7)
  [2] A(S1:t=5-10)
```

### Iteration 3

```
Action: Pop C, write block C1
Output: Part C → write S1:t=3-6

Advance C to next block (S2:t=1-5)
Heap.Fix(0) reorders heap

Heap:
  [0] B(S1:t=4-7)  ← minimum (S1 before S2)
  [1] A(S1:t=5-10)
  [2] C(S2:t=1-5)
```

### Iteration 4

```
Action: Pop B, write block B2
Output: Part B → write S1:t=4-7

Advance B to next block (S3:t=1-3)
Heap.Fix(0) reorders heap

Heap:
  [0] A(S1:t=5-10)  ← minimum (S1 before S3)
  [1] B(S3:t=1-3)
  [2] C(S2:t=1-5)
```

### Iteration 5

```
Action: Pop A, write block A2
Output: Part A → write S1:t=5-10

Advance A: no more blocks
Remove A from heap

Heap:
  [0] C(S2:t=1-5)  ← minimum (S2 before S3)
  [1] B(S3:t=1-3)
```

### Iteration 6

```
Action: Pop C, write block C2
Output: Part C → write S2:t=1-5

Advance C: no more blocks
Remove C from heap

Heap:
  [0] B(S3:t=1-3)
```

### Iteration 7

```
Action: Pop B, write block B3
Output: Part B → write S3:t=1-3

Advance B: no more blocks
Remove B from heap

Heap: (empty)
```

## Output Block Order

```
1. Part A → write S1:t=1-5
2. Part B → write S1:t=2-4
3. Part C → write S1:t=3-6
4. Part B → write S1:t=4-7
5. Part A → write S1:t=5-10
6. Part C → write S2:t=1-5
7. Part B → write S3:t=1-3
```

## Heap Comparator Explanation

### Sorting Key Order

```go
func (h *blockStreamReadersHeap) Less(i, j int) bool {
    a := h[i].CurrentBlock()
    b := h[j].CurrentBlock()
    
    // PRIMARY: streamID
    if !a.streamID.equal(&b.streamID) {
        return a.streamID.less(&b.streamID)
    }
    
    // SECONDARY: minTimestamp
    return a.timestampsData.minTimestamp < b.timestampsData.minTimestamp
}
```

### Why streamID First?

**Correct ordering:**
```
S1:t=1, S1:t=3, S1:t=5, S2:t=2, S2:t=4, S3:t=1
  ↑ all S1 blocks   ↑ all S2 blocks  ↑ S3 block
```

All blocks for each stream are contiguous, enabling:
1. Efficient block-level filtering by stream
2. Correct index structure (streamID-grouped)
3. Block boundaries aligned with stream boundaries

**Incorrect (if timestamp-first):**
```
S1:t=1, S2:t=2, S1:t=3, S2:t=4, S1:t=5, S3:t=1
  ↑       ↑       ↑       ↑       ↑       ↑
  streams INTERLEAVED - index structure corrupted
```

### Why timestamp Second?

Within the same stream, blocks must be in chronological order:
- Log queries expect time-ordered results
- Time-based filters work efficiently
- Merge-sort correctness maintained

## Complexity Analysis

### K-Way vs Pairwise Merge

**Example:** 15 parts × 10 MB = 150 MB total

| Metric | Pairwise | K-Way |
|--------|----------|-------|
| Rounds | 4 (⌈log₂15⌉) | 1 |
| Bytes written | 600 MB | 150 MB |
| Write amplification | 4x | 1x |
| Memory | ~256 KB | ~2 MB |

**K-way advantage:** 4x less I/O for this example.

### Per-Block Cost

```
Heap operations per block: O(log k)
  - Pop: O(log k)
  - Push (if advancing): O(log k)
  
With k ≤ 15: log₂15 ≈ 4 comparisons per block
```

Compared to I/O (reading/writing MBs), heap operations are negligible.

## Evidence

Lab program `heap_merge.go` demonstrates:
- Complete heap merge trace
- Heap state transitions
- Output block order
- Heap comparator explanation
- Complexity analysis

## Conclusions

1. **Single-pass merge** - All parts merged in one pass
2. **O(log k) per block** - Heap operations are cheap
3. **Correct ordering** - streamID-first maintains index structure
4. **Memory efficient** - Only one block per reader in memory

## Open Questions

- How does performance scale with thousands of source parts?
- Should heap size be limited for very large merges?
