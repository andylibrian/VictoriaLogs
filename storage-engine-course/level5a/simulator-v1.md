# LSM Simulator V1

## Context
- Date: Level 5A
- Owner: Storage Engine Course

## Work

### Simulator Design

The event-driven LSM simulator models VictoriaLogs behavior with three operations:

| Operation | Description | Effect |
|-----------|-------------|--------|
| `PUT(size)` | Add data to memtable | `memtableSize += size` |
| `FLUSH` | Convert memtable to in-memory part | `memtable → inMemoryParts` |
| `COMPACT` | Merge parts according to policy | Merge eligible parts, rewrite bytes |

### State Tracked

```
type LSMState struct {
    MemtableSize   uint64   // Current unflushed data
    InMemoryParts  []Part   // RAM-based parts
    SmallParts     []Part   // Disk-based, cache-friendly
    BigParts       []Part   // Disk-based, large
    BytesWritten   uint64   // Cumulative bytes written
    TotalFlushes   int      // Flush count
    TotalMerges    int      // Merge count
}
```

### Policy Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `FlushThreshold` | 1 MB | When to flush memtable |
| `SmallThreshold` | 10 MB | In-memory → Small boundary |
| `BigThreshold` | 100 MB | Small → Big boundary |
| `PartsToMerge` | 15 | Maximum parts per merge |
| `MergeMultiplier` | 7.5x | Minimum merge ratio |

### Merge Selection Algorithm

The simulator implements the same algorithm as `appendPartsToMerge`:

1. Sort parts by size (ascending)
2. Try windows from `ceil(maxSrcParts/2)` to `maxSrcParts`
3. Score each window: `ratio = outputSize / largestInputSize`
4. Accept only if `ratio >= max(PartsToMerge/2, minMergeMultiplier)`
5. Pick window with highest ratio

## Evidence

### Simulation Run: 100 MB Ingestion

**Policy Settings:**
- Flush threshold: 1 MB
- Small threshold: 10 MB
- Big threshold: 100 MB
- Parts to merge: 15
- Merge ratio: 7.5x

**Metrics Over Time:**

| Ops | Data (MB) | Parts | Write Amp | Read Amp |
|-----|-----------|-------|-----------|----------|
| 100 | 9 | 2 | 1.85x | 2 |
| 250 | 24 | 8 | 1.67x | 8 |
| 500 | 48 | 10 | 1.87x | 10 |
| 750 | 73 | 2 | 2.81x | 2 |
| 1000 | 97 | 11 | 2.54x | 11 |

**Final State:**
- Total data size: 97 MB
- Total bytes written: 248 MB
- Final part count: 11
- Write amplification: 2.54x
- Read amplification: 11 parts to scan

### Key Observations

1. **Write amplification varies**: Starts low, spikes during active merges, settles at ~2.5x
2. **Part count fluctuates**: Merges reduce count, flushes increase it
3. **Burst behavior**: After 500 ops, merge triggers and reduces parts from 10 to 2
4. **Steady state**: System reaches equilibrium with ~10-15 parts

### Merge Trace Example

Input parts (before sorting):
```
Part 1: 1 MB, Part 2: 1 MB, Part 3: 1 MB, Part 4: 1 MB, Part 5: 1 MB
Part 6: 2 MB, Part 7: 2 MB, Part 8: 5 MB, Part 9: 10 MB, Part 10: 20 MB
```

After sorting by size (same order in this case).

Window search (minSrcParts = 8):
```
Window [1-8]: 14 MB / 5 MB = 2.80x → rejected (< 7.5x)
Window [2-9]: 23 MB / 10 MB = 2.30x → rejected
Window [3-10]: 42 MB / 20 MB = 2.10x → rejected
...
No merge candidate meets threshold → NO MERGE
```

Result: Parts remain unmerged until more accumulate or ratio improves.

## Conclusions

1. **Simulator predicts behavior**: Can test policy changes before modifying storage code
2. **7.5x threshold is selective**: Only merges when truly beneficial
3. **Write amp ~2.5x**: Reasonable for 100 MB data with default policy
4. **Read amp ~11 parts**: Acceptable for point queries with bloom filters
5. **Burst merges**: System alternates between accumulation and merge phases

### Simulator Limitations

1. Does not model compression (assumes 1:1 size)
2. Does not model concurrent merges
3. Does not model bloom filter effectiveness
4. Does not model query patterns
5. Single partition only

## Open Questions

1. How to extend simulator for multi-partition scenarios?
2. Should simulator model compression ratios?
3. How to correlate simulator predictions with production metrics?
4. What's the optimal policy for specific workload patterns?
