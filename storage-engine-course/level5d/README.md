# Level 5D - Read Path, Bloom Filters, and Block Cache

## Objective
Build an efficient read path across memtable + immutable tables with Bloom-based skipping.

## Outcomes
By the end of this phase, you can:
- implement merged read semantics with sequence visibility
- integrate Bloom filters at table/block scope
- add block cache and measure win/loss conditions

## Core Concepts
1. Read merge order across levels/runs.
2. Bloom as prefilter (never final truth).
3. Cache policy tradeoffs (LRU/clock/tinyLFU optional).
4. Tombstone handling in point and range reads.

## VictoriaLogs Anchors
- `lib/logstorage/bloomfilter.go`
- `lib/logstorage/block_search.go`
- `lib/logstorage/filter_and.go`
- `lib/logstorage/filter_phrase.go`

## Labs
### Lab 1: Visibility Rules
Implement and test newest-wins + tombstone masking.

### Lab 2: Bloom Integration
Measure false positives and I/O savings on synthetic and skewed workloads.

### Lab 3: Cache Benchmark
Run read benchmarks with cache off/on and multiple cache sizes.

## Deliverables
- `level5d/read-path-trace.md`
- `level5d/bloom-results.md`
- `level5d/cache-benchmark.md`
- `level5d/checkpoint.md`

## Pass Criteria
- Read correctness and measurable pruning/caching gains are demonstrated.
