# Level 9 Cache Correctness

## Context
- Level: 9 - IndexDB and `mergeset` Deep Dive
- Date: 2026-03-02

## Why Cache Generation Matters

### Problem: Stale Cache Results

```
Timeline:

T0: Query Q1 arrives
    Filter: {app="nginx"}
    
    Cache lookup: [partition="20260302"][tenantIDs][filter={app="nginx"}]
    Cache miss → search index → find [S1, S5, S12]
    Cache store: [partition="20260302"][tenantIDs][filter={app="nginx"}]
                 → [S1, S5, S12]

T1: New stream registered
    Stream: {app="nginx", host="web-02"}
    StreamID: S42
    
    Items added to index:
      [0x00][tenantID][S42]              // existence
      [0x01][tenantID][S42]{...}         // tags
      [0x02][tenantID][app][sep][nginx][S42]  // inverted index

T2: Query Q2 arrives (same filter)
    Filter: {app="nginx"}
    
    Cache lookup: [partition="20260302"][tenantIDs][filter={app="nginx"}]
    Cache hit → return [S1, S5, S12]
    
    BUG: S42 is missing from result!
    New stream not included in cached result.
```

### Solution: Generation Counter in Cache Key

```
Cache key includes generation counter:

T0: Generation = 42
    Cache key: [gen=42][partition][tenantIDs][filter]
    Cache store: [S1, S5, S12]

T1: New stream registered
    Generation incremented: 42 → 43
    (via invalidateStreamFilterCache callback)

T2: Generation = 43
    Cache key: [gen=43][partition][tenantIDs][filter]
    Cache miss (no entry at gen=43)
    Search index → find [S1, S5, S12, S42]
    Cache store at gen=43: [S1, S5, S12, S42]

Correctness: New stream included!
```

## Generation Counter Implementation

### Increment on Stream Registration

```go
func (idb *indexdb) invalidateStreamFilterCache() {
    // Called when new index data is flushed
    // This is coalesced and asynchronous
    idb.filterStreamCacheGeneration.Add(1)
}
```

### Cache Key Construction

```go
func (idb *indexdb) marshalStreamFilterCacheKey(dst []byte, tenantIDs []TenantID, sf *StreamFilter) []byte {
    dst = encoding.MarshalUint32(dst, idb.filterStreamCacheGeneration.Load())
    dst = encoding.MarshalBytes(dst, idb.partitionName)
    dst = encoding.MarshalVarUint64(dst, uint64(len(tenantIDs)))
    for i := range tenantIDs {
        dst = tenantIDs[i].marshal(dst)
    }
    dst = sf.marshalForCacheKey(dst)
    return dst
}
```

### Cache Key Format

```
[generation 4B][partitionLen 4B][partition][tenantCount][tenantIDs...][filterBytes]

Example:
  Generation:   43 (0x0000002B)
  Partition:    "20260302"
  TenantIDs:    [{AccountID: 12345, ProjectID: 0}]
  Filter:       {app="nginx"}

Full key (hex):
  00 00 00 2B                      // generation = 43
  00 00 00 08 32 30 32 36 30 33 30 32  // partition = "20260302"
  01                               // tenantCount = 1
  00 00 00 00 00 00 30 39 00 00 00 00  // tenantID = 12345:0
  [filter bytes]                   // {app="nginx"}
```

## Lazy Invalidation Strategy

### Why Lazy?

```
Eager invalidation (expensive):
  - Track which cache entries are affected by new stream
  - Find all matching filters
  - Explicitly delete those entries
  
  Problems:
    - Complex: need reverse index from stream → cached filters
    - Expensive: O(cache size) scan
    - Race conditions: new stream might match during scan

Lazy invalidation (simple):
  - Increment generation counter
  - Old entries become unreachable
  - Natural cache eviction reclaims memory
  
  Benefits:
    - O(1) invalidation
    - No tracking needed
    - No race conditions
```

### Trade-offs

```
Lazy invalidation characteristics:

1. Coarse granularity
   - All cached filters invalidated at once
   - Even filters unrelated to new stream

2. Asynchronous
   - Delay between stream registration and generation bump
   - ~10 second cadence (coalesced callback)

3. Memory overhead
   - Old cache entries linger until natural eviction
   - Multiple generations may coexist briefly

4. Simplicity
   - No complex invalidation logic
   - Easy to reason about correctness
```

## Generation Bump Timing

```
Stream registration flow:

1. mustRegisterStream()
   → items added to mergeset

2. Data sits in rawItems buffer
   (not yet visible to search)

3. Buffer flush (256 blocks or 1 second timer)
   → items moved to inmemoryParts
   (now visible to search)

4. needFlushCallbackCall set
   → signals callback worker

5. Callback worker runs (coalesced, ~10s cadence)
   → filterStreamCacheGeneration.Add(1)

Visibility timeline:
  T0: Stream registered
  T0+1s: Stream visible to search
  T0+11s: Cache generation bumped
```

## What Happens Without Generation

### Correctness Violation

```
Without generation in cache key:

Query 1 at T0: {app="nginx"} → [S1, S5, S12]
Stream registered at T1: {app="nginx"} → S42
Query 2 at T2: {app="nginx"} → [S1, S5, S12] (cached, missing S42)

Result: Query 2 doesn't see newly registered stream
This violates user expectation of immediate visibility
```

### With Generation Counter

```
With generation in cache key:

Query 1 at T0 (gen=42): {app="nginx"} → [S1, S5, S12]
Stream registered at T1 → gen bumps to 43
Query 2 at T2 (gen=43): {app="nginx"} → cache miss → [S1, S5, S12, S42]

Result: Query 2 sees newly registered stream
Correctness maintained!
```

## Per-Partition Isolation

```
Each partition has its own indexdb with its own generation:

Partition 20260301:
  indexdb.filterStreamCacheGeneration = 100

Partition 20260302:
  indexdb.filterStreamCacheGeneration = 43

Cache keys include partition name:
  [gen=100][20260301][...]
  [gen=43][20260302][...]

Result: No cross-partition cache collisions
        Independent invalidation per partition
```

## Code References

- `lib/logstorage/indexdb.go:164` - `filterStreamCacheGeneration` field
- `lib/logstorage/indexdb.go:740-744` - `invalidateStreamFilterCache`
- `lib/logstorage/indexdb.go:748-757` - `marshalStreamFilterCacheKey`
- `lib/logstorage/indexdb.go:761-789` - `loadStreamIDsFromCache`
- `lib/logstorage/indexdb.go:792-805` - `storeStreamIDsToCache`

## Conclusions

1. **Generation in key** - Prevents stale cache results
2. **Lazy invalidation** - Simple, O(1), correct
3. **Coarse granularity** - All filters invalidated together
4. **Asynchronous** - ~10s delay between registration and invalidation
5. **Per-partition** - Independent generation counters

## Open Questions

- Should the callback cadence be configurable?
- How much memory is wasted by old generation entries?
- Could read-your-writes consistency be guaranteed?
