# Level 5H - Full-Featured LSM Implementation Capstone

## Objective
Integrate all prior phases into one coherent LSM engine and validate it with correctness and performance tests.

## Outcomes
By the end of this phase, you can:
- run a full LSM implementation with WAL, SSTables, compaction, recovery, and observability
- explain design tradeoffs with measured evidence
- compare your design against VictoriaLogs choices

## Required Features
1. Durable write path (WAL + memtable).
2. Immutable table files with metadata indexes.
3. Bloom-assisted read path.
4. Compaction (at least one mature policy).
5. Manifest/recovery correctness.
6. Concurrency-safe background workers.
7. Metrics + debug tooling.

## Recommended Project Layout
- `storage-engine-course/level5h/mini-lsm/`
  - `cmd/`
  - `pkg/wal/`
  - `pkg/memtable/`
  - `pkg/sstable/`
  - `pkg/compaction/`
  - `pkg/manifest/`
  - `pkg/engine/`
  - `tests/`

## VictoriaLogs Comparison Prompts
- Why VictoriaLogs uses per-day partitioning on top of LSM-like parts.
- Why VictoriaLogs applies merge heuristics like `minMergeMultiplier`.
- Why Bloom is placed at block-column granularity in the read path.

## Labs
### Lab 1: End-to-End Correctness Suite
Run deterministic tests for:
- crash recovery
- tombstones
- read-your-write semantics
- compaction equivalence

### Lab 2: Benchmark Suite
Collect baseline metrics:
- write throughput
- point-read latency
- range-read throughput
- compaction debt behavior

### Lab 3: Design Defense
Write a tradeoff memo against at least 3 alternative design choices.

## Deliverables
- `level5h/architecture.md`
- `level5h/implementation-notes.md`
- `level5h/correctness-report.md`
- `level5h/benchmark-report.md`
- `level5h/tradeoff-memo.md`
- `level5h/checkpoint.md`

## Graduation Criteria
- Implementation passes correctness suite.
- Benchmarks are reproducible.
- Tradeoff memo is concrete and source-informed.
