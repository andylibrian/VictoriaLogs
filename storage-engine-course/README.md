# VictoriaLogs Storage Engine Course (Level 1 -> Level 10)

This directory contains a full, code-anchored learning path for VictoriaLogs storage internals.

## How To Use
1. Complete levels in order.
2. At each level, produce the requested deliverables inside that level directory.
3. Do not skip checkpoints; they are designed to ensure conceptual continuity.
4. Keep a running engineering journal of tradeoffs you observe.

## Recommended Weekly Rhythm
- 3 sessions/week, 90-120 minutes/session
- Session A: theory + code reading
- Session B: labs + experiments
- Session C: recap + write-up + checkpoint

## Level Map
- `level1`: Core DS/algorithms for storage systems
- `level2`: VictoriaLogs ingestion skeleton and buffering
- `level3`: Block model, columnar layout, value encoding
- `level4`: Bloom filter theory and VictoriaLogs implementation
- `level5`: LSM deep-dive overview and phase plan
- `level5a`: LSM mental model, invariants, and simulator
- `level5b`: Memtable + WAL + flush pipeline
- `level5c`: SSTable format + metadata indexes
- `level5d`: Read path + Bloom + block cache
- `level5e`: Compaction policy engine (tiered/leveled)
- `level5f`: Recovery, manifest/versioning, snapshots
- `level5g`: Concurrency, backpressure, and observability
- `level5h`: Full-featured LSM implementation capstone
- `level6`: VictoriaLogs LSM details in `datadb`
- `level7`: Merge heuristics and k-way merge internals
- `level8`: Query path pruning and parallel search execution
- `level9`: `indexdb` and `mergeset` internals
- `level10`: Lifecycle ops, retention/delete behavior, capstone

## Common Environment
- Workspace root: `/Users/andy/Workspace/VictoriaMetrics/VictoriaLogs`
- Key package: `lib/logstorage`
- Supporting engine: `vendor/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset`

## Visual Aids
Reference diagrams for architecture and concepts:
- `assets/architecture-scaffold.md`: High-level architecture, write/query paths
- `assets/lsm-state-diagram.md`: Part lifecycle, merge state machine
- `assets/pruning-pipeline.md`: Query pruning stages and Bloom decision tree

## External Resources
Foundational readings organized by topic: `external-resources.md`

## Global Deliverables
By the end of Level 10, you should have:
- a complete architecture diagram
- a write path trace (row -> block -> part -> merge)
- a query path trace (query -> prune -> block scan)
- one validated optimization proposal with explicit tradeoff analysis

## Rules For This Course
- Always map concept -> concrete function(s) in source.
- Always state invariants before and after any operation.
- When you cannot explain "why this tradeoff", stop and dig deeper before proceeding.
