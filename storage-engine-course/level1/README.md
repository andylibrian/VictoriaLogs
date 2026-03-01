# Level 1 - Algorithmic Foundations For Storage Engines

## Objective
Build the minimum algorithm toolkit needed to reason about VictoriaLogs internals.

## Outcomes
By the end of this level, you can:
- explain why sorted metadata enables cheap pruning
- use binary search to locate overlap ranges
- reason about asymptotic behavior under write-heavy and read-heavy workloads

## Prerequisites
- Go basics: slices, structs, maps
- `sort.Search` familiarity (or willingness to learn quickly)

## Source Anchors (Read First)
- `lib/logstorage/storage.go`: `getPartitionForWriting`
- `lib/logstorage/storage_search.go`: `getPartitionsForTimeRange`
- `lib/logstorage/storage_search.go`: `searchByTenantIDs`
- `lib/logstorage/storage_search.go`: `searchByStreamIDs`

## Core Concepts
1. Sorted sequences as index structures.
2. Binary search as a range boundary finder.
3. Two-phase filtering:
   - metadata pruning
   - data scan only for remaining candidates
4. Cost model:
   - linear scan: `O(n)`
   - binary boundary search: `O(log n)`

## Why Sorted Data Changes Everything (Conceptual Background)

### The fundamental problem: finding a needle in a haystack

A log database might hold 365 days of data. When a user queries "show me errors from March 5th", the system must figure out *where* that data lives. If the data is stored in random order, every piece of data must be examined — a **linear scan**. With 365 partitions, that means 365 checks.

This seems fast for 365, but the same pattern repeats at every level of the storage hierarchy. Inside each partition there are thousands of parts, and inside each part there are thousands of block headers. If every level requires a linear scan, the costs multiply:

```
365 partitions × 1000 parts × 1000 blocks = 365,000,000 checks
```

Even if each check is nanosecond-fast, this adds up quickly under concurrent queries.

### Sorted order enables binary search

The fix is simple: keep things sorted. If partitions are sorted by day, you do not need to examine all 365 to find March 5th. You can use **binary search**: jump to the middle, compare, and eliminate half the candidates. Repeat until you find the target.

```
Linear scan of 365 partitions:   365 comparisons (worst case)
Binary search of 365 partitions:  ~9 comparisons  (log₂ 365 ≈ 8.5)
```

This is the difference between `O(n)` and `O(log n)`. As the data grows, the gap becomes dramatic:

| Partitions | Linear scan | Binary search |
|------------|-------------|---------------|
| 10         | 10          | 4             |
| 365        | 365         | 9             |
| 1,000      | 1,000       | 10            |
| 1,000,000  | 1,000,000   | 20            |

VictoriaLogs maintains `s.partitions` sorted by day number. This is not an optimization added later — the entire storage hierarchy depends on it.

### Range queries need two boundaries

Most log queries are not "find exactly day X" but "find everything between day X and day Y" — a **range query**. A range query on a sorted list needs two binary searches:

1. **Left boundary**: find the first partition where `day >= minDay`
2. **Right boundary**: find the first partition where `day > maxDay`

The slice between these two positions is the complete set of matching partitions. This is exactly what `getPartitionsForTimeRange` does:

```
Sorted partitions by day:

  [Jan1] [Jan2] [Jan3] ... [Mar4] [Mar5] [Mar6] [Mar7] ... [Dec31]
                             ↑                    ↑
                         left boundary        right boundary
                      (first day >= Mar4)   (first day > Mar7)

Result: partitions[left:right] = [Mar4, Mar5, Mar6, Mar7]
```

Two `sort.Search` calls — each `O(log n)` — replace a full scan and return the exact slice of relevant partitions. Everything outside that slice is never touched.

### The same pattern repeats at every level

This is not a one-time trick. VictoriaLogs applies sorted-order pruning at multiple levels of the storage hierarchy:

```
Query: "errors from March 5th for stream {app=nginx}"

Level 1: Partitions (sorted by day)
  → binary search → select [Mar5] partition only
  → skipped 364 out of 365 partitions

Level 2: Index block headers within the part (sorted by streamID)
  → binary search → jump to {app=nginx} stream's blocks
  → skipped thousands of unrelated streams

Level 3: Block headers (sorted by streamID, then time range)
  → binary search → select blocks overlapping March 5th
  → skipped blocks outside the time range

Level 4: Block-level bloom filter
  → check if "error" could exist in block
  → skipped blocks that definitely don't contain the term
```

Each level eliminates a large fraction of candidates before the next level even begins. By the time actual log values are read from disk, the search space has been reduced by orders of magnitude.

### Two-pointer merge on sorted sequences

`searchByTenantIDs` and `searchByStreamIDs` take this further. They must intersect two sorted sequences: the query's list of target IDs and the part's sorted index block headers. Instead of checking every combination (`O(n × m)`), they advance through both sequences in lockstep, using `sort.Search` to skip ahead when one sequence falls behind the other.

Consider searching for tenants `[T2, T5]` in index block headers that cover tenants `[T1, T2, T3, T4, T5, T6]`:

```
Step 1: tenantIDs=[T2,T5], ibhs=[T1,T2,T3,T4,T5,T6]
  → T2 >= T1? Yes → binary search ibhs for T2 → found at index 1
  → process ibh[1] (covers T2)

Step 2: tenantIDs=[T5], ibhs=[T3,T4,T5,T6]
  → T5 >= T3? Yes → binary search ibhs for T5 → found at index 2
  → process ibh[2+offset] (covers T5)

Done: processed 2 index blocks out of 6, skipped 4
```

This merge-style traversal works because *both* sequences are sorted. If either were unsorted, every element would require a full scan of the other.

### Writes benefit too

Sorted order is not just a read optimization. When a log entry arrives, `getPartitionForWriting` must route it to the correct day partition. A single `sort.Search` call finds the partition in `O(log n)` instead of scanning all partitions:

```
Incoming row: timestamp = March 5th, 14:32:07
  → day = timestamp / nsecsPerDay
  → sort.Search(partitions, day >= target) → partition[63]
  → partition[63].day == target day? Yes → write here
  → partition[63].day != target day? → create new partition, insert at position 63
```

The insert maintains sorted order by placing the new partition at the position returned by binary search, so future lookups remain `O(log n)`.

### Why metadata pruning must come before data scanning

Reading actual log values from disk is expensive: it requires decompressing blocks, decoding columns, and comparing strings. Metadata pruning — using sorted indices, time ranges, and stream IDs — is cheap: it operates on small, in-memory structures with simple numeric comparisons.

The design principle is: **never read data you can prove is irrelevant using metadata alone**. Each pruning level is progressively more expensive but also more precise:

| Level | Cost | What it checks | What it eliminates |
|-------|------|----------------|--------------------|
| Partition selection | Nanoseconds | Day range | Entire days of data |
| Index block headers | Microseconds | Stream ID, time range | Groups of blocks |
| Block headers | Microseconds | Min/max timestamp | Individual blocks |
| Bloom filter | Microseconds | Token membership | Blocks without search terms |
| Value scan | Milliseconds | Actual column values | Individual rows |

Each level acts as a gate: only data that survives all cheaper checks reaches the expensive value scan. This is why VictoriaLogs can query billions of log entries in milliseconds — the vast majority are eliminated before any decompression happens.

## Guided Reading Tasks
1. In `getPartitionsForTimeRange`, identify:
   - the sort invariant for `s.partitions`
   - how `minDay` and `maxDay` become binary search boundaries
2. In `searchByTenantIDs` and `searchByStreamIDs`, identify:
   - where `sort.Search` is used repeatedly on sorted IDs
   - why this avoids full block enumeration
3. In `getPartitionForWriting`, identify:
   - where binary search routes a write to the correct day partition

## Hands-On Lab
### Lab 1: Range Lookup Mini Program
Implement a tiny program that stores sorted day numbers and returns overlap range for a `[minDay, maxDay]` query using two `sort.Search` calls.

Acceptance:
- no full scan in the lookup path
- tests cover:
  - full overlap
  - no overlap
  - exact boundary match

### Lab 2: Cost Estimation Exercise
Create a table comparing rough operations count for 10, 1000, and 1,000,000 partitions:
- linear scan
- binary search boundaries + slice

## Checkpoint Questions
1. Why does VictoriaLogs maintain sorted partition order globally?
2. Why are two binary searches needed for a time range?
3. Why is metadata pruning mandatory before value-level filters?

## Deliverables
- `level1/notes.md`: answers to guided reading tasks
- `level1/lab-results.md`: outputs and observations from labs
- `level1/checkpoint.md`: checkpoint answers

## Pass Criteria
- You can explain range boundary logic without looking at source.
- You can justify algorithm choices with time complexity and workload context.
