# Level 1 Notes - Guided Reading Tasks

## Context
- Level: 1 - Algorithmic Foundations for Storage Engines
- Date: 2026-03-02

## Task 1: `getPartitionsForTimeRange` Analysis

### Sort Invariant for `s.partitions`
**Location:** `lib/logstorage/storage_search.go:1498-1530`

The invariant is: **`s.partitions` is always sorted by `day` (ascending)**.

This is maintained by:
- `getPartitionForWriting` inserting new partitions at the correct position using binary search
- Partitions are never reordered after creation

### How `minDay` and `maxDay` Become Binary Search Boundaries

```go
// Step 1: Find LEFT boundary (first partition with day >= minDay)
minDay := minTimestamp / nsecsPerDay
n := sort.Search(len(ptwsTmp), func(i int) bool {
    return ptwsTmp[i].day >= minDay
})
ptwsTmp = ptwsTmp[n:]  // Discard partitions before left boundary

// Step 2: Find RIGHT boundary (first partition with day > maxDay)
maxDay := maxTimestamp / nsecsPerDay
n = sort.Search(len(ptwsTmp), func(i int) bool {
    return ptwsTmp[i].day > maxDay
})
ptwsTmp = ptwsTmp[:n]  // Keep only partitions before right boundary
```

**Key insight:** Two different comparison operators:
- Left boundary uses `>=` (inclusive: include partition if day equals minDay)
- Right boundary uses `>` (exclusive: stop when day exceeds maxDay)

## Task 2: `searchByTenantIDs` and `searchByStreamIDs` Analysis

### Where `sort.Search` is Used

**Location:** `lib/logstorage/storage_search.go:1758-1837`

```go
// 1. Skip tenantIDs that are less than the first index block header's tenant
n := sort.Search(len(tenantIDs), func(i int) bool {
    return !tenantIDs[i].less(tenantID)
})

// 2. Skip index block headers that are less than the target tenant
n = sort.Search(len(ibhs), func(i int) bool {
    return !ibhs[i].streamID.tenantID.less(tenantID)
})

// 3. Within block headers, find blocks matching the tenant
n = sort.Search(len(bhs), func(i int) bool {
    return !bhs[i].streamID.tenantID.less(tenantID)
})
```

### Why This Avoids Full Block Enumeration

The algorithm performs a **two-pointer merge** on sorted sequences:

1. Both `tenantIDs` (query targets) and `ibhs` (index block headers) are sorted
2. Instead of checking every tenantID against every ibh (O(n×m)), we:
   - Compare current positions
   - Binary search to skip ahead in the lagging sequence
   - Process only matching entries

**Example:** With 100 tenantIDs and 1000 index blocks:
- Brute force: 100 × 1000 = 100,000 comparisons
- Two-pointer with binary search: ~log₂(100) + log₂(1000) ≈ 17 comparisons per match

## Task 3: `getPartitionForWriting` Analysis

### Binary Search for Write Routing

**Location:** `lib/logstorage/storage.go:1409-1463`

```go
// Binary search for the partition with this day
ptws := s.partitions
n := sort.Search(len(ptws), func(i int) bool {
    return ptws[i].day >= day
})

var ptw *partitionWrapper
if n < len(ptws) {
    ptw = ptws[n]
    if ptw.day != day {
        ptw = nil  // Partition exists but not for this day
    }
}
```

### Insertion Maintains Sorted Order

When a new partition is created:
```go
// Insert at position n to maintain sorted order
if n == len(ptws) {
    ptws = append(ptws, ptw)       // Append at end
} else {
    ptws = append(ptws[:n+1], ptws[n:]...)  // Insert in middle
    ptws[n] = ptw
}
```

The position `n` returned by binary search is exactly where the new element belongs.

## Evidence

The lab programs demonstrate:
1. `range_lookup.go`: Working implementation of the two-binary-search algorithm
2. `cost_estimation.go`: Quantified speedup from O(log n) vs O(n)

## Conclusions

1. **Sorted data is a primitive index structure** - No additional index needed for single-key lookups
2. **Binary search boundaries are precise** - Two searches with different operators define exact range
3. **Write path benefits equally** - Same O(log n) lookup for routing writes

## Open Questions

- How does VictoriaLogs handle concurrent modifications to the partition list?
- What happens when a partition is deleted while a query is in progress?
