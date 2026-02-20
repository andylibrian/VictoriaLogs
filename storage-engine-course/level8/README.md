# Level 8 - Query Path: Pruning, Scheduling, and Block Evaluation

## Objective
Understand end-to-end query execution and where each pruning stage saves I/O/CPU.

## Outcomes
By the end of this level, you can:
- trace query execution from `RunQuery` to block-level filtering
- explain pruning stages and their order
- explain worker scheduling and concurrency limits

## Source Anchors
- `lib/logstorage/storage_search.go`
  - `RunQuery`
  - `searchParallel`
  - `getPartitionsForTimeRange`
  - `getPartsForTimeRange`
  - `searchByTenantIDs`
  - `searchByStreamIDs`
- `lib/logstorage/block_search.go`
  - `search`
  - `getColumnHeader`
  - `getBloomFilterForColumn`
  - `getValuesForColumn`
- `lib/logstorage/filter_and.go`
  - `matchBloomFilters`

## Core Concepts
1. Stage-wise pruning pipeline:
   - partition time pruning
   - part time pruning
   - index block pruning
   - block header pruning
   - bloom pruning
   - value scan and row bitmap evaluation
2. Work batching (`blockSearchWorkBatch`) and worker channels.
3. Why query stats are accumulated atomically across workers.

## Guided Reading Tasks
1. In `searchParallel`, map:
   - partition-level scheduling
   - work channel fan-out
   - finalizer flow
2. In `searchByTenantIDs` and `searchByStreamIDs`, explain how sorted metadata and `sort.Search` reduce block reads.
3. In `blockSearch.search`, explain bitmap lifecycle from all-ones to filtered rows.
4. In `filterAnd.matchBloomFilters`, explain why Bloom precheck can terminate early.

## Hands-On Lab
### Lab 1: Query Trace
Pick a realistic query and produce a function-by-function trace with pruning counters.

### Lab 2: Skip Effectiveness
For one query class, estimate relative reduction at each stage:
- partitions skipped
- parts skipped
- blocks skipped by metadata
- blocks skipped by bloom

### Lab 3: Parallelism Reasoning
Explain effect of changing `parallel readers` on:
- throughput
- latency
- memory pressure

## Checkpoint Questions
1. Why is pruning order important?
2. Why does Bloom sit before value decoding?
3. Why do we still need row-level bitmap evaluation after all pruning?

## Deliverables
- `level8/query-trace.md`
- `level8/skip-effectiveness.md`
- `level8/parallelism-analysis.md`
- `level8/checkpoint.md`

## Pass Criteria
- You can reconstruct query execution without skipping stages.
- You can explain observed performance through pruning mechanics.
