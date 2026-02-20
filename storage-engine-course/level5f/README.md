# Level 5F - Recovery, Manifest Versioning, and Snapshots

## Objective
Make the engine operationally safe across crashes and restarts.

## Outcomes
By the end of this phase, you can:
- recover from WAL + manifest state
- apply atomic version edits
- support snapshot/checkpoint semantics for consistent backup

## Core Concepts
1. Manifest as source of truth for active runs.
2. Version edit log and atomic commit points.
3. Startup replay ordering and idempotence.
4. Snapshot consistency model.

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: `parts.json` read/write paths
- `lib/logstorage/partition.go`: snapshot lifecycle
- `lib/logstorage/storage.go`: startup/open lifecycle

## Labs
### Lab 1: Manifest Format
Define and implement version edits with checksum.

### Lab 2: Recovery Scenarios
Test restarts at key failure points:
- partial flush
- partial compaction
- missing temp files

### Lab 3: Snapshot Semantics
Design snapshot/backup flow and verify consistency under concurrent writes.

## Deliverables
- `level5f/manifest-format.md`
- `level5f/recovery-scenarios.md`
- `level5f/snapshot-model.md`
- `level5f/checkpoint.md`

## Pass Criteria
- Restart is deterministic and data-consistent under tested crash scenarios.
