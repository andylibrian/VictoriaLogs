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

## The Write Path in VictoriaLogs (Conceptual Background)

### No WAL — and why

A traditional database writes every mutation to a Write-Ahead Log before acknowledging it. The WAL is a sequential, append-only file that survives crashes. On recovery, the database replays the WAL to reconstruct any data that hadn't yet been flushed to its final location.

VictoriaLogs does **not** use a WAL. Instead, incoming log rows accumulate in an in-memory buffer and are flushed directly to immutable parts on disk. This means acknowledged writes can be lost if the process crashes before the flush completes.

Why make this trade-off? Logs are different from transactional data:

- **Logs are append-only.** There are no updates or deletes to coordinate, so recovery doesn't need to reconstruct a sequence of mutations — only the most recent unflushed batch matters.
- **Sources can retry.** Log collectors (Filebeat, Fluentd, vlagent) track their read position. If VictoriaLogs crashes, the collector resends from its last checkpoint. The database doesn't need to guarantee durability because the source does.
- **Throughput matters more than per-row durability.** A WAL doubles disk I/O (write to WAL, then write to part). Skipping it halves the write path cost, enabling higher ingestion rates.
- **The loss window is small.** At most one configured flush interval of in-memory parts (default 5 seconds) can be lost, plus rows still in shard buffers.

This is an important design lesson: the "right" durability guarantee depends on the system's role in the larger architecture. A WAL is essential for a primary transactional database. For a log sink with upstream retry, it's unnecessary overhead.

### From HTTP request to in-memory part

The write path has four stages. Follow a batch of log rows through each one:

```
  HTTP POST /insert/jsonline
       │
       ▼
  ┌─────────────────────────────────┐
  │  vlinsert: parse, validate,     │
  │  assign streamID + timestamp    │
  └──────────────┬──────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────┐
  │  partition.mustAddRows()        │
  │  register streams in indexdb,   │
  │  forward rows to datadb         │
  └──────────────┬──────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────┐
  │  rowsBuffer: sharded append     │  ← Stage 1: buffer
  │  one shard per CPU, per-shard   │
  │  mutex, round-robin assignment  │
  └──────────────┬──────────────────┘
                 │  trigger: 1s timer
                 │  OR buffer ≥ ~1.75 MB
                 ▼
  ┌─────────────────────────────────┐
  │  mustFlushLogRows()             │  ← Stage 2: freeze + convert
  │  sort by (streamID, timestamp), │
  │  encode into columnar blocks,   │
  │  produce inmemoryPart           │
  └──────────────┬──────────────────┘
                 │
                 ▼
  ┌─────────────────────────────────┐
  │  append to ddb.inmemoryParts    │  ← Stage 3: queryable
  │  data is now visible to readers │
  │  but NOT yet on disk            │
  └──────────────┬──────────────────┘
                 │  trigger: flushInterval (default 5s)
                 │  OR merge fills up
                 ▼
  ┌─────────────────────────────────┐
  │  MustStoreToDisk()              │  ← Stage 4: durable
  │  write part files in parallel,  │
  │  write metadata.json last,      │
  │  fsync files + directory        │
  └─────────────────────────────────┘
```

Notice the gap between Stage 3 (queryable) and Stage 4 (durable). A query sees the data immediately, but a crash before Stage 4 loses it. This is the **durability window**.

### Stage 1: sharded buffering

The `rowsBuffer` distributes incoming rows across shards using a round-robin counter. Each shard is a separate buffer protected by its own mutex. This is a classic sharding technique to reduce lock contention on multi-core machines:

```
  CPU 0         CPU 1         CPU 2         CPU 3
    │             │             │             │
    ▼             ▼             ▼             ▼
┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐
│ shard 0│  │ shard 1│  │ shard 2│  │ shard 3│
│  mutex │  │  mutex │  │  mutex │  │  mutex │
│  timer │  │  timer │  │  timer │  │  timer │
└────────┘  └────────┘  └────────┘  └────────┘
```

Each shard has two flush triggers:

- **Size trigger:** fires immediately when the buffer reaches ~1.75 MB (7/8 of `maxUncompressedBlockSize`). This handles burst ingestion — don't wait for the timer if data is arriving fast.
- **Time trigger:** starts when the first row lands in an empty shard and fires ~1 second later if the shard hasn't already flushed by size. This handles trickle ingestion without waiting for a full buffer.

The combination ensures that data moves through the buffer within at most 1 second, regardless of ingestion rate.

### Stage 2: the freeze-and-convert handoff

When a shard flushes, it takes its accumulated rows and hands them to `mustFlushLogRows`. This is the **memtable freeze** moment — the mutable buffer is swapped out and replaced with a fresh empty buffer, while the old data is converted to an immutable in-memory part.

The conversion does real work:

1. **Sort** all rows by `(streamID, timestamp)` — this groups entries from the same log stream together and orders them chronologically.
2. **Group** sorted rows into blocks (up to ~64 KB uncompressed per block).
3. **Encode** each block into columnar format — timestamps, field values, bloom filters — exactly as they will appear on disk.

The result is a fully formed `inmemoryPart` — identical in structure to a disk part, but held in RAM. This means the same code path can read from in-memory parts and disk parts, simplifying the query engine.

Concurrency is bounded: at most one in-memory part creation per CPU core. If all slots are busy, the shard flush blocks until a slot opens. This prevents runaway memory usage during ingestion spikes.

### Stage 3: queryable but volatile

The new `inmemoryPart` is appended to the partition's part list under a lock. From this moment, queries can find and read it. But the data exists only in RAM — a crash or power failure loses it entirely.

Each in-memory part carries a **flush deadline**: the current time plus the configured flush interval (default 5 seconds). A background ticker (`inmemoryPartsFlusher`) runs every flush interval and collects all parts past their deadline for disk flush.

### Stage 4: durable on disk

When an in-memory part is flushed to disk (either by the periodic flusher or as part of a merge), `MustStoreToDisk` writes the part directory:

```
part_XXXX/
  column_names.bin
  column_idxs.bin
  metaindex.bin
  index.bin
  columns_header_index.bin
  columns_header.bin
  timestamps.bin
  message_bloom.bin
  message_values.bin
  field_bloom.bin
  field_values.bin
  metadata.json           ← written LAST
```

The ordering matters for crash safety:

1. All data files are written **in parallel** for speed.
2. `metadata.json` is written **last** — it contains the part header (row count, time range, size).
3. `fsync` is called on all files and the parent directory.

If a crash happens before `metadata.json` is written, the part directory is an orphan. On startup, VictoriaLogs discovers it has no matching entry in the parts list and deletes it. No corruption, no partial reads — just a clean loss of that batch.

### Backpressure: what happens when flushes can't keep up

If ingestion outpaces merging, in-memory parts accumulate. In `datadb`, there is no direct hard-cap stall at exactly 20 parts; backpressure comes from merge/flush worker saturation. The concurrency channel (`inmemoryPartsConcurrencyCh`, sized to CPU count) throttles part creation: if all slots are occupied, shard flushes block, which slows ingestion.

This creates a natural **backpressure chain**:

```
Too many in-memory parts
  → merge slots full
    → mustFlushLogRows blocks
      → shard flush blocks
        → ingestion slows down
```

The system degrades gracefully rather than running out of memory. Ingestion resumes at full speed once merges catch up.

### Crash matrix: where data can be lost

| Crash point | Data state | Outcome |
|-------------|------------|---------|
| During shard buffering (Stage 1) | Rows in RAM only | **Lost.** Up to 1 second of data per shard. |
| During in-memory part creation (Stage 2) | Rows being converted | **Lost.** Conversion hadn't completed. |
| After in-memory part created (Stage 3) | Queryable, not on disk | **Lost.** Data was only in RAM. |
| During disk write, before metadata.json (Stage 4a) | Partial files on disk | **Lost but clean.** Orphan directory deleted on startup. |
| After metadata.json + fsync (Stage 4b) | Fully written and synced | **Safe.** Part survives restart. |
| During merge (replacing old parts with new) | Old parts still valid | **Safe.** Old parts remain; incomplete new part is orphaned. |
| Clean shutdown | All in-memory parts force-flushed | **Safe.** Shutdown calls `mustFlushInmemoryPartsToFiles(isFinal=true)`. |

The maximum data loss window is roughly one in-memory-part flush interval (default 5 seconds) plus rows still waiting in shard buffers. For a log analytics system with upstream retry, this is an acceptable trade-off.

### Shutdown: the controlled flush

On graceful shutdown, VictoriaLogs ensures zero data loss:

1. **Flush all shard buffers** — any rows still in `rowsBuffer` become in-memory parts.
2. **Signal background workers to stop** — close the stop channel.
3. **Wait for workers to finish** — all in-progress merges complete.
4. **Force-flush all in-memory parts** — `mustFlushInmemoryPartsToFiles(isFinal=true)` bypasses size and time limits, writing everything to disk immediately.

This guarantees that a clean `SIGTERM` loses no data. Only unclean termination (crash, `SIGKILL`, power failure) can cause loss.

### Design lesson: durability is a spectrum

The textbook WAL model — "nothing acknowledged until durable" — is one end of a spectrum. VictoriaLogs sits at a different point: "acknowledge immediately, make durable soon, rely on upstream retry for the gap." Both are valid; the right choice depends on the system's guarantees and its position in the data pipeline.

The labs below ask you to implement the WAL end of the spectrum. Understanding VictoriaLogs' WAL-free design helps you reason about *why* a WAL exists — what guarantees it provides and what it costs — rather than treating it as an unquestioned requirement.

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
