# Level 2 - Ingestion Skeleton: Rows, Streams, Buffers

## Objective
Understand how inserted logs enter VictoriaLogs and become structured, buffered data.

## Outcomes
By the end of this level, you can:
- trace insertion path from partition to `datadb`
- explain stream registration and caches
- explain why buffering and sorting are required before part creation

## Source Anchors (Read In Order)
1. `lib/logstorage/partition.go`
   - `mustAddRows`
   - stream cache usage (`hasStreamIDInCache`, `putStreamIDToCache`)
2. `lib/logstorage/indexdb.go`
   - `hasStreamID`
   - `mustRegisterStream`
3. `lib/logstorage/datadb.go`
   - `mustAddRows`
   - `rowsBuffer` and `rowsBufferShard`
   - `mustFlushLogRows`
4. `lib/logstorage/log_rows.go`
   - `needFlush`
   - `Less`
   - `sortFieldsInRows`

## Core Concepts
1. Stream identity and metadata lifecycle.
2. Write amortization through sharded row buffering.
3. CPU-friendly sorting before block serialization.
4. Visibility timing: recently ingested rows are not immediately in final on-disk parts.

## Guided Reading Tasks
1. In `partition.mustAddRows`, list the exact order of operations from stream check to data append.
2. In `rowsBuffer.mustAddRows`, explain:
   - shard selection strategy
   - timer-driven flush path
   - size-driven flush path
3. In `logRows.needFlush`, explain threshold meaning relative to `maxUncompressedBlockSize`.
4. In `logRows.Less`, explain why `(streamID, timestamp)` ordering matters for later merges.

## Hands-On Lab
### Lab 1: Write Path Trace
Pick one synthetic row and create a step-by-step trace with function names and state transitions:
- stream cache miss/hit
- index registration path
- buffer append
- flush trigger

### Lab 2: Concurrency Reasoning
Explain how sharding in `rowsBuffer` reduces lock contention compared to one global lock.
Provide one edge case where contention still exists.

## Checkpoint Questions
1. Why does stream registration happen before writing row data to `datadb`?
2. Why does VictoriaLogs maintain both timer-based and threshold-based flushing?
3. Why is sorting fields in rows useful before column construction?

## Deliverables
- `level2/write-path-trace.md`
- `level2/concurrency-analysis.md`
- `level2/checkpoint.md`

## Pass Criteria
- You can narrate write path end-to-end from memory.
- You can justify buffering and ordering decisions in operational terms.
