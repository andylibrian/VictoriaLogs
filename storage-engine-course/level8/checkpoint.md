# Level 8 Checkpoint

## Context
- Level: 8 - Query Path: Pruning, Scheduling, and Block Evaluation
- Date: 2026-03-02

## Checkpoint Questions and Answers

### Question 1: Why is pruning order important?

**Answer:**

Pruning order is critical because **each stage has different cost**, and ordering cheapest-first maximizes efficiency.

**Cost hierarchy (cheapest to most expensive):**

| Stage | Cost per check | What it eliminates |
|-------|---------------|-------------------|
| Partition | 1 comparison | Billions of rows |
| Part | 1 comparison per part | Millions of rows |
| Metaindex | ~400 comparisons in RAM | ~2.5M rows per skip |
| Block header | 1 decompress + comparisons | ~10K rows per skip |
| Bloom | 6 hash probes per token | ~10K rows per skip |
| Row values | Decompress + scan per row | Individual rows |

**If stages were reversed (expensive-first):**

```
Wrong order (values → bloom → header → metaindex → part → partition):

1. Read and decompress ALL blocks (850,000 blocks)
2. Load ALL column values for ALL rows (10 billion)
3. Apply bloom filter (already decompressed everything)
4. Check block headers (already processed blocks)
5. Check metaindex (already processed index blocks)
6. Check partition time range (already processed all partitions)

Result: Read 1 TB of data, then discard 99.99%
```

**Correct order (cheapest-first):**

```
Right order (partition → part → metaindex → header → bloom → values):

1. Check partition time range → skip 29 partitions (96.7% reduction)
2. Check part time range → skip 127 parts (91% reduction)
3. Check metaindex → skip 408 index blocks (97% reduction)
4. Check block headers → skip 262 blocks (93% reduction)
5. Check bloom filters → skip some blocks (0-20% reduction)
6. Load and scan values → process only survivors

Result: Read 3.6 MB of data (99.9996% reduction)
```

**Key insight:** Early stages are O(1) or O(log N) and eliminate O(billions). Late stages are O(rows) but only run on tiny survivor sets.

### Question 2: Why does Bloom sit before value decoding?

**Answer:**

Bloom filters sit before value decoding because they can **prove absence without reading values**.

**Bloom filter properties:**
- Probabilistic data structure
- "Definitely absent" = 100% accurate (no false negatives)
- "Maybe present" = ~1.5% false positive rate
- 6 hash probes per token check (~50 ns)

**Without bloom filter:**

```
Query: level:error
Block: 10,000 rows, level column = all "info"

Path:
  1. Read level column from values.bin (disk I/O)
  2. Decompress ZSTD block (CPU)
  3. Decode 10,000 values (CPU)
  4. Scan 10,000 rows, all fail (CPU)
  5. Result: 0 matches

Cost: ~2 MB read + ~10 ms CPU
```

**With bloom filter:**

```
Query: level:error
Block: 10,000 rows, level column = all "info"

Path:
  1. Get column header for "level" (cached in block header)
  2. Check dict values: ["debug", "info", "warn"] (no "error")
  3. "error" definitely not in block → skip
  4. Never read values.bin

Cost: ~50 ns (dict check in RAM)

Alternative (no dict):
  1. Read bloom filter from bloom.bin (~2 KB)
  2. Check bloom.containsAll(["error"])
  3. Bloom says "definitely absent" → skip
  4. Never read values.bin

Cost: ~2 KB read + ~50 ns bloom check
```

**Savings:**
- Value read: 2 MB → 0 B (or 2 KB for bloom)
- CPU: 10 ms → 50 ns
- Speedup: ~200,000× for non-matching blocks

**Why not after value decoding?**

If bloom check happened after decoding:
- Would already have read and decompressed values
- Bloom check would be redundant (already know values)
- No savings possible

**Why bloom before values is correct:**

Bloom has **no false negatives**:
- If bloom says "absent" → definitely absent → safe to skip
- If bloom says "maybe present" → might be present → must decode values

The ~1.5% false positive rate only causes unnecessary work, never missed results.

### Question 3: Why do we still need row-level bitmap evaluation after all pruning?

**Answer:**

Row-level evaluation is necessary because **pruning stages can only approximate; they cannot guarantee exact matches**.

**What each stage guarantees:**

| Stage | Guarantees | Cannot guarantee |
|-------|-----------|-----------------|
| Partition | Rows might be in time range | Exact timestamps |
| Part | Rows might be in time range | Exact timestamps |
| Metaindex | Blocks might contain streamID | Exact streamID per block |
| Block header | Block contains streamID + time range | Row-level matches |
| Bloom | Tokens might be present | Exact token positions, AND/OR logic |

**Why row-level is unavoidable:**

```
Query: level:error AND msg:contains("timeout")

Block survives all pruning:
  - Partition time range: ✓ (10:00-10:05)
  - Part time range: ✓ (10:00-10:05)
  - Metaindex: ✓ (streamID matches)
  - Block header: ✓ (streamID + time match)
  - Bloom: ✓ ("error" and "timeout" might be present)

But block contains:
  Row 1: level="error", msg="connection started"
  Row 2: level="info", msg="timeout occurred"
  Row 3: level="error", msg="request timeout"

Which rows match the query?
  - Pruning cannot tell
  - Need to examine actual row values
  - Row 3 matches (error AND timeout)
  - Rows 1, 2 don't match

Only row-level bitmap evaluation can determine this.
```

**Bitmap evaluation process:**

```
bm = [1111111111...]  // 10,000 bits, all set

filterPhrase("level", "error").applyToBlockSearch(bs, bm):
  Load level column values
  For each row where bm bit is set:
    if level[row] != "error":
      clear bit
  bm = [0100110001...]  // 3,200 bits remaining

filterSubstring("msg", "timeout").applyToBlockSearch(bs, bm):
  Load msg column values
  For each row where bm bit is STILL SET (only 3,200):
    if "timeout" not in msg[row]:
      clear bit
  bm = [0000100001...]  // 500 bits remaining

Result: 500 rows match
```

**Why bitmap is efficient:**

1. **Progressive narrowing:** Each filter only examines survivors
   - First filter: 10,000 rows
   - Second filter: 3,200 rows (not 10,000)
   - Third filter: even fewer

2. **Short-circuit:** If bitmap becomes zero, stop immediately
   - Save work on remaining filters

3. **Lazy column loading:** Only load columns when filter needs them
   - Query: `level:error AND msg:contains("timeout") AND user:contains("admin")`
   - If bloom check fails → never load any columns
   - If first two filters eliminate all rows → never load `user` column

**What if we skipped row-level?**

```
Wrong approach: Return all rows from blocks that pass bloom check

Query: level:error AND msg:contains("timeout")

Block passes bloom (both tokens present)
But only 5% of rows actually match

Result: 95% false positives in output
User sees: 19 wrong rows for every 1 correct row

This violates query semantics.
```

**Conclusion:**

Row-level bitmap evaluation is the **final arbiter of truth**:
- Pruning stages reduce work (eliminate entire blocks)
- Row-level ensures correctness (exact matches only)
- Both are necessary: pruning for performance, row-level for correctness

## Summary

| Question | Key Insight |
|----------|-------------|
| Pruning order | Cheapest-first maximizes elimination before expensive stages |
| Bloom before values | "Definitely absent" allows skipping value read entirely |
| Row-level needed | Pruning approximates; only row evaluation guarantees exact matches |

## Evidence

All three labs demonstrate these concepts:
- Lab 1: Query trace showing stage-by-stage reduction
- Lab 2: Skip effectiveness showing 99.99%+ elimination
- Lab 3: Parallelism showing throughput/latency/memory tradeoffs

## Conclusions

1. **Cascading pruning** - Each stage eliminates data before more expensive stages
2. **Bloom optimization** - Skip value reads for definitely-absent tokens
3. **Row-level correctness** - Final evaluation ensures exact query semantics
4. **Lazy loading** - Load column data only when survivors need it

## Open Questions

- Could machine learning predict optimal pruning strategies?
- How to handle queries with very low selectivity (most rows match)?
