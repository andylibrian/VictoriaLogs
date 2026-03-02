# Level 8 Skip Effectiveness

## Context
- Level: 8 - Query Path: Pruning, Scheduling, and Block Evaluation
- Date: 2026-03-02

## Query Class Analysis

### Query Class: Time-Range Stream Filter

```logsql
_stream:{app="api"} AND _time:[2026-03-02T00:00:00Z, 2026-03-02T01:00:00Z]
```

**Characteristics:**
- Specific stream filter (1 stream out of 1000)
- Narrow time range (1 hour out of 30 days)
- No additional row-level filters

### Baseline: Without Pruning

| Metric | Value |
|--------|-------|
| Total data | 1 TB |
| Total partitions | 30 |
| Total parts | 4,200 |
| Total index blocks | 12,000 |
| Total blocks | 850,000 |
| Total rows | 10 billion |

**Cost without pruning:**
- Read 1 TB of data
- Decompress 850,000 blocks
- Scan 10 billion rows

### Stage-by-Stage Reduction

#### Stage 1: Partition Pruning

```
Time range: 1 hour on March 2
Partitions: 30 days of data

Binary search on sorted partition list:
  minDay = 2026-03-02 → partition[28]
  maxDay = 2026-03-02 → partition[28]

Result:
  Partitions selected: 1
  Partitions skipped: 29

Reduction: 29/30 = 96.7%

Data remaining: 33 GB (from 1 TB)
Rows remaining: 333 million
```

**Why it works:** Partitions are organized by day. A 1-hour query on one day eliminates all other days with O(log N) binary search.

#### Stage 2: Stream ID Resolution

```
Stream filter: {app="api"}
Total streams: 1000
Matching streams: 1

indexdb.searchStreamIDs() returns: [S42]

Result:
  Streams selected: 1
  Streams skipped: 999

Reduction within partition: 99.9%

This doesn't reduce data yet, but constrains subsequent stages.
```

**Why it works:** Stream filter resolved to concrete stream IDs using indexdb cache.

#### Stage 3: Part Pruning

```
Selected partition has:
  inmemoryParts: 5
  smallParts: 140
  bigParts: 18
  Total: 163 parts

Time range check per part:
  inmemoryParts: 2 match [00:00, 01:00] (3 skipped)
  smallParts: 6 match (134 skipped)
  bigParts: 1 match (17 skipped)

Result:
  Parts selected: 9
  Parts skipped: 154

Reduction: 154/163 = 94.5%

Data remaining: 1.8 GB (from 33 GB)
Rows remaining: 18 million
```

**Why it works:** Each part has [MinTimestamp, MaxTimestamp] in header. Parts outside time range skipped with single comparison.

#### Stage 4: Metaindex Pruning

```
Selected parts have total:
  Index block headers: 420 (across 9 parts)

Query targets streamID = S42

Part 1 (big part):
  50 index block headers
  Binary search for S42 → ih[18]
  Check: S42 in [ih[18].minStreamID, ih[18].maxStreamID]? Yes
  Check time range: overlaps? Yes
  Next: ih[19] covers S43 → stop
  Selected: 1 index block (49 skipped)

Part 2-9 (small parts):
  Similar analysis
  Each part: 1-2 index blocks selected
  Total selected: 12 index blocks

Result:
  Index blocks selected: 12
  Index blocks skipped: 408

Reduction: 408/420 = 97.1%

Data remaining: ~50 MB (from 1.8 GB)
Rows remaining: ~500,000
```

**Why it works:** Metaindex sorted by streamID. Binary search jumps directly to relevant region. Each index block header covers ~2.5M rows; skipping one header skips all those rows.

#### Stage 5: Block Header Pruning

```
Selected 12 index blocks contain:
  Total block headers: 280

Per-block checks:
  - streamID match (must equal S42)
  - time range overlap [00:00, 01:00]

Block distribution:
  Index block 1: 25 headers, 3 match
  Index block 2: 22 headers, 2 match
  Index block 3: 24 headers, 4 match
  ...
  Total: 18 blocks match

Result:
  Blocks selected: 18
  Blocks skipped: 262

Reduction: 262/280 = 93.6%

Data remaining: ~3.6 MB (from ~50 MB)
Rows remaining: ~180,000
```

**Why it works:** Block headers contain streamID and time range. One ZSTD decompress per index block, then cheap comparisons per block header.

#### Stage 8: Bloom Filter Pruning

```
Query filter: (none beyond stream filter)

No bloom filter check needed.

If query were: _stream:{app="api"} AND level:error
  
  For each block:
    Check bloom filter for "level" column
    Query token: "error"
    
    Block A: bloom.containsAll(["error"]) → true (pass)
    Block B: bloom.containsAll(["error"]) → false (skip)
    Block C: bloom.containsAll(["error"]) → true (pass)
    
  Bloom false positive rate: ~1.5%
  
  Result (with level:error filter):
    Blocks passing bloom: ~17 (1 block false positive)
    Blocks skipped by bloom: ~1
```

**Why it works:** Bloom filters are ~1.5% false positive. A "definitely absent" result skips entire block without reading values.

#### Stage 9: Row-Level Evaluation

```
18 blocks selected, each with ~10,000 rows

For stream-only query: all rows match
  Rows found: 180,000
  Rows skipped: 0

If query had additional filter: AND msg:contains("timeout")
  
  Per-row bitmap evaluation:
    Load msg column values
    Scan each row where bm bit is set
    
    Block 1: 10000 rows → 850 match (9150 skipped)
    Block 2: 10000 rows → 720 match (9280 skipped)
    ...
    
  Total rows found: ~1,500
  Rows skipped: ~178,500
```

**Why it works:** Bitmap tracks candidates. Each filter clears non-matching bits. Only survivors passed to next filter.

## Cumulative Reduction Summary

| Stage | Input | Output | Skipped | Reduction | Cumulative |
|-------|-------|--------|---------|-----------|------------|
| 1. Partition | 30 partitions | 1 | 29 | 96.7% | 96.7% |
| 2. Stream ID | - | - | - | - | 96.7% |
| 3. Part | 163 parts | 9 | 154 | 94.5% | 99.8% |
| 4. Metaindex | 420 ibhs | 12 | 408 | 97.1% | 99.995% |
| 5. Block header | 280 blocks | 18 | 262 | 93.6% | 99.9998% |
| 8. Bloom | 18 blocks | 18 | 0 | 0% | 99.9998% |
| 9. Row eval | 180K rows | 180K | 0 | 0% | 99.9998% |

**Final reduction:** 10 billion rows → 180,000 rows (99.998% eliminated)

**I/O saved:** 1 TB → 3.6 MB (99.9996% reduction)

## Comparison: High-Selectivity vs Low-Selectivity

### High-Selectivity Query (stream + time + row filter)

```logsql
_stream:{app="api"} AND _time:[2026-03-02T00:00:00Z, 2026-03-02T01:00:00Z] AND level:error AND msg:contains("timeout")
```

| Stage | Output | Reduction at Stage |
|-------|--------|-------------------|
| Partition | 1 | 96.7% |
| Part | 9 | 94.5% |
| Metaindex | 12 | 97.1% |
| Block header | 18 | 93.6% |
| Bloom | 15 | 16.7% |
| Row eval | ~150 | 99.2% |

**Total rows found:** ~150 (from 10 billion)
**Final reduction:** 99.9999985%

### Low-Selectivity Query (broad time range, no filters)

```logsql
_time:[2026-02-01T00:00:00Z, 2026-03-02T23:59:59Z]
```

| Stage | Output | Reduction at Stage |
|-------|--------|-------------------|
| Partition | 30 | 0% |
| Part | 4200 | 0% |
| Metaindex | 12000 | 0% |
| Block header | 850000 | 0% |
| Bloom | 850000 | 0% |
| Row eval | 10B | 0% |

**Total rows found:** 10 billion
**Final reduction:** 0%

**Cost:** Full table scan, but still efficient due to:
- Sequential I/O patterns
- OS page cache for hot data
- Parallel workers

## Key Insights

1. **Early stages are most effective** - Partition and part pruning eliminate 99.8% of data
2. **Bloom filters target specific queries** - Only help when query has token-based filters
3. **Row-level is the last resort** - Most expensive, but necessary for final filtering
4. **Pruning order matters** - Cheapest checks first, expensive checks only on survivors

## Code References

- `lib/logstorage/storage_search.go:1495-1530` - Partition pruning
- `lib/logstorage/storage_search.go:1650-1671` - Part pruning
- `lib/logstorage/storage_search.go:1860-1961` - Metaindex/block pruning
- `lib/logstorage/filter_and.go:85-120` - Bloom filter check
- `lib/logstorage/block_search.go:217-238` - Row-level evaluation

## Conclusions

1. **99.99%+ reduction is common** - Most queries touch tiny fractions of data
2. **Time range is most powerful** - Partition pruning alone eliminates entire days
3. **Stream filters are highly selective** - Resolving to stream IDs constrains all subsequent stages
4. **Bloom filters prevent decompression** - Skip blocks without reading values

## Open Questions

- How to optimize for queries with low time selectivity?
- Should bloom filter false positive rate be configurable?
