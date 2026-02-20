# Level 5B - Memtable, WAL, and Flush Pipeline

## Objective
Implement the crash-safe write path: WAL append, memtable mutation, flush to immutable run.

## Outcomes
By the end of this phase, you can:
- implement WAL record framing + checks
- explain durability semantics (`fsync` policy)
- flush sorted memtable output into first immutable table format

## Core Concepts
1. Write ordering invariant: WAL durable before ACK.
2. Memtable mutability and flush handoff.
3. Flush trigger design (size/time/manual).
4. Backpressure when too many immutable runs exist.

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: `rowsBuffer`, flush timers
- `lib/logstorage/datadb.go`: `mustFlushLogRows`
- `lib/logstorage/partition.go`: ingestion path context

## Labs
### Lab 1: WAL Format
Define WAL record fields:
- sequence number
- op type
- key/value lengths
- checksum

### Lab 2: Crash Matrix
Test crash points:
- before WAL append
- after WAL append before memtable apply
- after memtable apply before ACK

### Lab 3: Flush Handoff
Implement immutable memtable freeze and asynchronous flush.

## Deliverables
- `level5b/wal-format.md`
- `level5b/memtable-flush.md`
- `level5b/crash-matrix.md`
- `level5b/checkpoint.md`

## Pass Criteria
- Recovery from WAL reproduces acknowledged writes exactly.
