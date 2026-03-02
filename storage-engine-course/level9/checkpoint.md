# Level 9 Checkpoint

## Context
- Level: 9 - IndexDB and `mergeset` Deep Dive
- Date: 2026-03-02

## Checkpoint Questions and Answers

### Question 1: Why is index metadata maintained in a dedicated engine?

**Answer:**

Index metadata and log payloads have **fundamentally different access patterns**, requiring different storage optimizations.

**Access Pattern Comparison:**

| Aspect | Log Payloads (`datadb`) | Stream Metadata (`indexdb`) |
|--------|------------------------|----------------------------|
| Write pattern | Large batches (thousands of rows) | Single items (one stream at a time) |
| Read pattern | Broad scans (time ranges, filters) | Targeted lookups (exact prefix seeks) |
| Data format | Columnar blocks (timestamps, values) | Sorted key-value items |
| Query type | "Find logs matching X" | "Find streams with tag=X" |
| Optimization | Sequential I/O, column pruning | Prefix seeks, range scans |

**Why separate engines:**

1. **Format mismatch:**
   - Payloads: Columnar encoding (dict, bloom, timestamps)
   - Metadata: Flat sorted byte strings
   - Mixing would force compromises

2. **Merge semantics:**
   - Payloads: Mechanical merge-sort of blocks
   - Metadata: Semantic consolidation (deduplicate streamIDs)
   - Different merge callbacks needed

3. **Access frequency:**
   - Payloads: Every query scans blocks
   - Metadata: Only needed for stream filter resolution
   - Different caching strategies

4. **Cardinality:**
   - Payloads: Billions of rows
   - Metadata: Thousands of streams × tags
   - Different scaling characteristics

**Result:** Two specialized LSM engines, each optimized for its workload.

### Question 2: Why are merge callbacks useful for semantic compaction?

**Answer:**

Merge callbacks enable **semantic understanding** during compaction, allowing data transformations that a purely mechanical merge-sort cannot perform.

**Mechanical merge (datadb):**
```
Input:  Block A [row1, row2] + Block B [row3, row4]
Output: Block C [row1, row2, row3, row4] (sorted)

No understanding of row content
Just combines and re-sorts
```

**Semantic merge (indexdb with callback):**
```
Input:  Item 1 [tag=app][nginx][S1]
        Item 2 [tag=app][nginx][S5]
        Item 3 [tag=app][nginx][S12]
        
Callback understands: "These are inverted index entries"

Output: Item 1 [tag=app][nginx][S1, S5, S12] (consolidated)

Transformations performed:
  1. Group by (tenant, tag, value)
  2. Collect streamIDs
  3. Sort and deduplicate
  4. Emit consolidated items
```

**Benefits:**

1. **Space reduction:**
   ```
   Without consolidation: 1000 streams × 3 tags = 3000 items
   With consolidation:    1000 streams × 3 tags / 32 IDs per item ≈ 94 items
   Reduction: 97%
   ```

2. **Query speed:**
   ```
   Without consolidation: Read 1000 items, collect streamIDs
   With consolidation:    Read ~32 items, streamIDs already grouped
   Speedup: 31× fewer reads
   ```

3. **Deduplication:**
   ```
   Concurrent insertions may create duplicate (tag, streamID) entries
   Merge callback removes duplicates during consolidation
   ```

**Why not do this at insert time?**
- Inserts are single-threaded per stream
- Cannot coordinate across concurrent stream registrations
- Consolidation requires global view of all items with same prefix
- Merge provides natural coordination point

### Question 3: What correctness issue appears if cache generation is ignored?

**Answer:**

Without generation in cache keys, **newly registered streams become invisible** to queries with cached filters.

**Scenario:**

```
T0: User queries for {app="nginx"}
    Result: Streams [S1, S5, S12]
    Cache stores: {app="nginx"} → [S1, S5, S12]

T1: Application starts new instance
    New stream: {app="nginx", host="web-02"}
    StreamID: S42
    Index updated with new stream

T2: User queries for {app="nginx"} (same filter)
    Without generation:
      Cache hit → return [S1, S5, S12]
      S42 is MISSING from result
    
    With generation:
      Cache key includes generation
      Generation incremented after T1
      Cache miss at new generation
      Search index → return [S1, S5, S12, S42]
      S42 correctly included
```

**Correctness violation:**
- User expects new data to be immediately visible
- Cached result returns stale (incomplete) stream list
- Queries silently miss data from newly registered streams

**Why generation fixes this:**
- Generation is part of cache key
- Generation increments when new streams are registered
- Old cache entries (at old generation) become unreachable
- New queries build cache at new generation with complete data

**Key insight:** Cache invalidation must be tied to index mutations, not just time-based expiry. The generation counter provides this coupling.

## Summary

| Question | Key Insight |
|----------|-------------|
| Separate engine | Different access patterns require different optimizations |
| Merge callbacks | Semantic compaction reduces items and speeds queries |
| Cache generation | Prevents stale results when new streams registered |

## Evidence

All three labs demonstrate these concepts:
- Lab 1: Index item structure and access patterns
- Lab 2: Merge consolidation with deduplication
- Lab 3: Cache key construction with generation counter

## Conclusions

1. **Separation of concerns** - Index and payload have different requirements
2. **Semantic awareness** - Merge callbacks understand data meaning
3. **Cache consistency** - Generation counter ties cache validity to index state

## Open Questions

- Could the index use a different data structure (e.g., bitmap) for very high cardinality tags?
- Should cache generation be per-tenant instead of per-partition for better isolation?
