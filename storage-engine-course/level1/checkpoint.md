# Level 1 Checkpoint Answers

## Context
- Level: 1 - Algorithmic Foundations for Storage Engines
- Date: 2026-03-02

## Checkpoint Questions

### 1. Why does VictoriaLogs maintain sorted partition order globally?

**Answer:**

VictoriaLogs maintains `s.partitions` sorted by day number to enable O(log n) lookups at every level of the storage hierarchy:

1. **Write path efficiency:** When a log entry arrives with a timestamp, `getPartitionForWriting` uses binary search to find the correct day partition in O(log n) time instead of scanning all partitions.

2. **Read path efficiency:** Time range queries use two binary searches to find the left and right boundaries of overlapping partitions. With 365 daily partitions, this is ~18 comparisons vs 365 for a linear scan.

3. **Composability:** The sorted order property propagates down the storage hierarchy. Partitions contain sorted index block headers, which contain sorted block headers. Each level can apply binary search independently, creating a cascading pruning effect.

4. **No additional index needed:** The sorted data itself serves as an index. No separate index structure needs to be maintained, updated, or stored.

### 2. Why are two binary searches needed for a time range?

**Answer:**

A time range query `[minDay, maxDay]` requires finding a **slice** of partitions, not a single partition. This requires identifying both boundaries:

1. **Left boundary:** Find the first partition where `day >= minDay`. Partitions before this point definitely don't overlap the query range.

2. **Right boundary:** Find the first partition where `day > maxDay`. Partitions at or after this point definitely don't overlap.

The result is `partitions[left:right]` - exactly the partitions that might contain data in the query range.

**Why different operators?**
- Left uses `>=` (inclusive): if a partition's day equals minDay, it might contain relevant data
- Right uses `>` (exclusive): if a partition's day equals maxDay, it should be included; we stop when day exceeds maxDay

This is more efficient than searching for each boundary independently because:
- The second search operates on a smaller slice (after left boundary is found)
- Total cost: O(log n) + O(log n) = O(log n), not O(n)

### 3. Why is metadata pruning mandatory before value-level filters?

**Answer:**

Metadata pruning must come first because of the **cost differential** between operations:

| Level | Cost | What it checks |
|-------|------|----------------|
| Partition selection | Nanoseconds | Day range (in-memory integer compare) |
| Block header scan | Microseconds | Timestamp range, stream ID |
| Bloom filter check | Microseconds | Token existence |
| Value scan | Milliseconds | Decompress + decode + compare strings |

**Key insight:** Value-level filters require:
1. Reading compressed data from disk
2. Decompressing blocks
3. Decoding column values
4. String comparisons

Metadata pruning requires only:
1. In-memory numeric comparisons
2. Small, cached data structures

**The multiplier effect:**

Without pruning, a query might scan:
- 365 partitions × 1000 parts × 1000 blocks = 365 million blocks

With pruning at each level:
- Binary search → 1 partition (364 eliminated)
- Binary search → 10 parts (990 eliminated)  
- Binary search → 50 blocks (950 eliminated)
- Bloom filter → 5 blocks (45 eliminated)

Result: 5 blocks scanned instead of 365 million.

**Design principle:** Never read data you can prove is irrelevant using metadata alone. Each pruning level acts as a gate - only data surviving all cheaper checks reaches the expensive value scan.
