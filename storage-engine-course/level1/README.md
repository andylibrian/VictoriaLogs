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
