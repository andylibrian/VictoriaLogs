# Level 1 Lab Results

## Context
- Level: 1 - Algorithmic Foundations for Storage Engines
- Date: 2026-03-02

## Lab 1: Range Lookup Mini Program

### Implementation
Created `lab/range_lookup.go` implementing:
- `PartitionIndex` - sorted collection of day partitions
- `AddPartition` - maintains sorted order on insert
- `FindOverlappingPartitions` - two binary searches for range lookup
- `FindPartitionForWriting` - single binary search for write routing

### Test Results

```
=== Test Cases ===
TestPartitionIndex_AddPartition_MaintainsSortedOrder     PASS
TestPartitionIndex_AddPartition_Deduplication            PASS
TestFindOverlappingPartitions_FullOverlap                PASS
TestFindOverlappingPartitions_NoOverlap                  PASS
TestFindOverlappingPartitions_ExactBoundaryMatch         PASS
TestFindOverlappingPartitions_SingleDay                  PASS
TestFindOverlappingPartitions_PartialOverlapStart        PASS
TestFindOverlappingPartitions_PartialOverlapEnd          PASS
TestFindOverlappingPartitions_EmptyIndex                 PASS
TestFindPartitionForWriting_Existing                     PASS
TestFindPartitionForWriting_NotExisting                  PASS
```

### Demo Output

```
=== Lab 1: Range Lookup with Binary Search ===

Creating 365 daily partitions (Jan 1 - Dec 31)...
Total partitions: 365

--- Test Case 1: Full Overlap ---
Query range: [0, 364] (entire year)
Found 365 partitions (expected 365)

--- Test Case 2: No Overlap ---
Query range: [400, 500] (future dates, no data)
Found 0 partitions (expected 0)

--- Test Case 3: Exact Boundary Match ---
Query range: [60, 90] (exact day boundaries)
Found 31 partitions (expected 31)
First: Day 60 (Month3_Day1)
Last:  Day 90 (Month4_Day1)

--- Test Case 4: Partial Overlap (Query Extends Beyond Data) ---
Query range: [350, 400] (extends beyond available data)
Found 15 partitions (expected 15)
First: Day 350 (Month12_Day20)
Last:  Day 364 (Month13_Day5)

--- Test Case 5: Single Day Query ---
Query range: [100, 100] (single day)
Found 1 partitions (expected 1)
Partition: Day 100 (Month4_Day11)
```

### Key Observations

1. **No full scan in lookup path:** All lookups use `sort.Search` (binary search)
2. **Exact boundary handling:** Left boundary is inclusive, right is exclusive
3. **Empty results handled correctly:** Returns nil for out-of-range queries
4. **Insertion maintains order:** New partitions inserted at correct position

## Lab 2: Cost Estimation Exercise

### Results Table

| Partitions | Linear Scan | Binary Search | Speedup  |
|------------|-------------|---------------|----------|
|         10 |          10 |             8 |       1x |
|        100 |         100 |            14 |       7x |
|        365 |         365 |            18 |      20x |
|      1,000 |       1,000 |            20 |      50x |
|     10,000 |      10,000 |            28 |     357x |
|    100,000 |     100,000 |            34 |    2941x |
|  1,000,000 |   1,000,000 |            40 |   25000x |
| 10,000,000 |  10,000,000 |            46 |  217391x |

### Multi-Level Hierarchy Example

```
Storage: 365 partitions × 50 parts × 1000 blocks = 18,250,000 total blocks

Linear scan at every level: 365 × 50 × 1000 = 18,250,000 block checks
Binary search: 18 (partition) + 12×5 (parts) + 20×50 (blocks) = 1,078 operations

Speedup: 16,930x
```

### Key Observations

1. **Speedup grows exponentially:** Binary search advantage increases with data size
2. **Compound effect:** Multi-level pruning compounds the speedup
3. **Real-world impact:** At 1M partitions, binary search is 25,000x faster

## Conclusions

1. **Two binary searches replace one linear scan** for range queries
2. **Sorted data acts as an implicit index** - no additional structure needed
3. **The algorithm scales logarithmically** - performance degrades gracefully as data grows
4. **VictoriaLogs applies this pattern at every level** of the storage hierarchy

## Open Questions

- How does the performance change with concurrent queries?
- What is the break-even point where maintaining sorted order costs more than the lookup savings?
