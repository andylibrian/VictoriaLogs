# Level 5 - LSM Deep Dive Overview

## Why This Exists
The previous Level 5 was too shallow for mastering LSM design. This upgraded Level 5 is split into phases (`level5a` .. `level5h`) so you can build a complete implementation gradually.

## Target Outcome
After finishing Level 5 phases, you should be able to:
- design and implement a production-style LSM tree end-to-end
- explain correctness invariants under crash/concurrency pressure
- reason about compaction strategy tradeoffs quantitatively
- map your LSM choices to VictoriaLogs internals

## Phase Sequence
1. `level5a`: Mental model, invariants, simulator
2. `level5b`: Memtable + WAL + flush
3. `level5c`: SSTable and metadata indexes
4. `level5d`: Read path, Bloom, cache
5. `level5e`: Compaction engine and policy
6. `level5f`: Recovery and manifest/versioning
7. `level5g`: Concurrency and observability
8. `level5h`: Full-featured LSM capstone

## Build Artifact Across Phases
Use one cumulative codebase for all phases:
- recommended path: `storage-engine-course/level5h/mini-lsm/`

Each phase should extend this implementation, not restart from scratch.

## Mapping Back To VictoriaLogs
- Write/flush/merge lifecycle: `lib/logstorage/datadb.go`
- Block/SSTable-like structures: `lib/logstorage/block_stream_writer.go`, `lib/logstorage/part.go`, `lib/logstorage/part_header.go`
- Query pruning/read path: `lib/logstorage/storage_search.go`, `lib/logstorage/block_search.go`
- Merge selection/compaction policy: `lib/logstorage/datadb.go` (`appendPartsToMerge`)

## Gate To Continue
Do not proceed to `level6` until `level5h/checkpoint.md` is complete.
