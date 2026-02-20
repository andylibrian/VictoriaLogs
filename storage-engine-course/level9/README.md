# Level 9 - IndexDB and `mergeset` Deep Dive

## Objective
Understand stream metadata indexing and the second LSM-style subsystem behind VictoriaLogs.

## Outcomes
By the end of this level, you can:
- explain index key spaces and row shapes in `indexdb`
- explain how `mergeset.Table` is used for indexing
- explain merge-time consolidation for tag->streamIDs rows

## Source Anchors
- `lib/logstorage/indexdb.go`
  - namespace prefixes (`nsPrefix*`)
  - `mustRegisterStream`
  - `searchStreamIDs`
  - `mergeTagToStreamIDsRows`
- `vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset/table.go`
  - `Table` structure
  - ingestion and merge workers
- `vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset/merge.go`
  - block stream merge mechanics

## Core Concepts
1. Separation of concerns:
   - `datadb`: row payload blocks
   - `indexdb`: stream/tag discovery
2. Prefix-structured keys for efficient seeks and range scans.
3. Merge callbacks for semantic consolidation (`mergeTagToStreamIDsRows`).
4. Cache invalidation strategy via generation counters.

## Guided Reading Tasks
1. In `mustRegisterStream`, list exact emitted items for one stream registration.
2. In `searchStreamIDs`, trace cache lookup -> miss path -> store path.
3. In `mergeTagToStreamIDsRows`, explain why merged output can temporarily become unsorted and how fallback is handled.
4. In `mergeset.Table`, map key background workers and their analogous roles to `datadb` workers.

## Hands-On Lab
### Lab 1: Index Item Reconstruction
Given a tenant + stream tags, manually construct index rows for all namespace prefixes.

### Lab 2: Merge Consolidation Case
Construct a toy example with duplicate streamIDs across rows and show expected merged output.

### Lab 3: Cache Correctness
Explain why `filterStreamCacheGeneration` is embedded in cache key.

## Checkpoint Questions
1. Why is index metadata maintained in a dedicated engine?
2. Why are merge callbacks useful for semantic compaction?
3. What correctness issue appears if cache generation is ignored?

## Deliverables
- `level9/index-item-reconstruction.md`
- `level9/merge-consolidation.md`
- `level9/cache-correctness.md`
- `level9/checkpoint.md`

## Pass Criteria
- You can explain index query execution independent of row payload scans.
- You can reason about index merge correctness and cache validity.
