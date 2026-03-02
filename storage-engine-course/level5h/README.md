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

## How VictoriaLogs Diverges from a Textbook LSM (Conceptual Background)

### The textbook LSM vs VictoriaLogs

A textbook LSM engine has a well-known shape: a Write-Ahead Log for durability, a memtable for buffering, SSTables for persistent storage, a MANIFEST for tracking which files are active, and a compaction strategy (leveled or size-tiered) for merging files. This is the engine you'll build in this capstone.

VictoriaLogs implements the same conceptual architecture but makes different choices at every layer. Understanding *why* it diverges — not just *how* — is the goal of this section. Each divergence reflects a deliberate trade-off informed by log workload characteristics.

### Divergence 1: No WAL

**Textbook:** Every write goes to a WAL before the memtable. The WAL is a sequential, append-only file. On crash, the WAL is replayed to recover writes that hadn't been flushed to SSTables yet. This guarantees zero data loss.

**VictoriaLogs:** No WAL at all. Rows land in a sharded in-memory buffer, become in-memory parts within ~1 second, and reach disk on the configured flush interval (default 5 seconds). A crash can lose recent data still in shard buffers or not yet flushed to disk.

**Why this works for logs:** Log collectors (Filebeat, Fluentd, vlagent) maintain their own read position. After a crash, the collector re-sends from its last checkpoint. The durability guarantee is pushed upstream to the collector, not duplicated in the storage engine. Skipping the WAL halves write I/O and simplifies recovery — there's no replay step at startup.

**What this means for your capstone:** Your engine should implement a WAL. This forces you to solve problems VictoriaLogs avoids: WAL record framing, crash-consistent replay, memtable/WAL coordination, and WAL garbage collection. Understanding these problems makes VictoriaLogs' decision to skip the WAL more meaningful.

### Divergence 2: Columnar blocks instead of key-value SSTables

**Textbook:** An SSTable stores sorted key-value pairs. Each entry has a key, a value, and a sequence number. The index maps key ranges to block offsets. Point reads binary-search the index, then scan the block.

**VictoriaLogs:** A part stores blocks of log entries in columnar format. Within each block, every field (timestamp, host, level, msg, etc.) is stored as a separate column with its own encoding, compression, bloom filter, and metadata. The index is two-level (metaindex → index blocks), sorted by `(streamID, timestamp)`.

**Why columnar for logs:** Log queries are almost always column-selective. A query filtering on `level:error` touches only the `level` column — `host`, `msg`, and every other field are skipped entirely. In a row-oriented SSTable, finding `level` values requires decompressing and parsing every field of every row. The columnar layout turns a full-row scan into a single-column scan, often reading 10-50× less data.

The block boundary enables per-column optimizations impossible in a row store:
- **Const columns**: if `host = "web-01"` for every row in a block, store it once in the header.
- **Dict encoding**: if `level` has only 4 unique values, store a 4-entry dictionary and represent each row as a single byte.
- **Per-column bloom filters**: a bloom filter on the `level` column alone is smaller and more accurate than a bloom filter on the entire row.

### Divergence 3: Per-day partitioning

**Textbook:** All data lives in one LSM tree. Compaction merges files across the entire key space. Deleting old data requires either tombstones that propagate through compaction or range-delete markers.

**VictoriaLogs:** Data is partitioned by day. Each day is an independent LSM-like structure with its own `parts.json`, part files, merge workers, and index database. Retention is enforced by dropping entire partition directories — no per-row deletion needed.

```
storage/
  partitions/
    20260301/     ← yesterday: one full LSM
      datadb/
        parts.json
        part_001/
        part_002/
    20260302/     ← today: another full LSM, independent merge
      datadb/
        parts.json
        part_003/
        part_004/
```

**Why partition by day:** Logs have a natural time axis. Queries almost always include a time range (`_time:last 1h`). Per-day partitioning makes time-range pruning trivial — entire partitions are skipped by checking one timestamp comparison. Retention becomes a directory delete instead of a compaction-driven tombstone cascade. And merge workers for today's hot partition don't compete with yesterday's cold partition.

**The cost:** Cross-partition queries must fan out to multiple independent LSMs. But log queries rarely span more than a few days, so the fan-out is small. The simplicity of per-day lifecycle management (create, fill, compact, retain, drop) outweighs the cost.

### Divergence 4: Size-tiered compaction with a 7.5× threshold

**Textbook:** Leveled compaction (LevelDB/RocksDB) maintains strict per-level size limits and non-overlapping key ranges within each level. This minimizes read amplification but produces high write amplification as data cascades through levels.

**VictoriaLogs:** Size-tiered compaction with three tiers (in-memory, small, big). The merge picker tries windows of consecutive similarly-sized parts (from roughly half of the max fan-in up to 15 parts) and selects the window with the highest merge ratio. The effective minimum threshold is 7.5× — the output must be at least 7.5× the largest input part.

**Why this is more conservative than most LSMs:** The 7.5× threshold means VictoriaLogs merges less frequently than a typical size-tiered system (which might merge at 2-4×). This reduces write amplification at the cost of more parts to scan during queries. For log workloads — write-heavy, scan-heavy, rarely point-queried — this is the right side of the trade-off.

**Why not leveled:** Leveled compaction shines for point reads (at most one file per level to check) but produces 10-30× write amplification. Log systems care more about ingestion throughput and time-range scans than point reads. Size-tiered compaction with a high threshold keeps write amplification low while accepting a modest part count.

### Divergence 5: Full-snapshot manifest instead of version edit log

**Textbook:** A MANIFEST file records version edits (file additions and deletions) as an append-only log. Recovery replays edits from a checkpoint. The MANIFEST itself must be compacted when it grows too large.

**VictoriaLogs:** A `parts.json` file lists every active part. It is overwritten atomically (write-temp-file, fsync, rename) on every change. Recovery reads one file — no replay needed.

**Why the simpler approach works:** VictoriaLogs has tens to hundreds of parts, not millions of files. Rewriting the full list costs microseconds. The version edit log's advantages (efficient appends, minimal I/O per change) matter at scale — millions of small files where listing them all on every change would be expensive. At VictoriaLogs' part count, the simpler approach eliminates manifest compaction, checkpoint management, and replay ordering — an entire class of potential bugs.

### Divergence 6: Deletion by merge, not tombstones

**Textbook:** Deletes insert a tombstone record with the target key and a sequence number. During reads, tombstones mask earlier versions of the same key. During compaction, tombstones and their masked entries are garbage-collected.

**VictoriaLogs:** Deletes trigger a targeted merge. The system identifies parts containing matching rows (using the same bloom/filter machinery as queries), then merges those parts with a delete filter applied — matching rows are simply omitted from the output. No tombstone records exist in the storage format.

**Why this works for logs:** Log data is append-only — there are no updates, so there's no need for sequence-based version resolution. Deletes are rare (GDPR compliance, error cleanup) and can tolerate the latency of a merge operation. The payoff is zero overhead on the read path: no tombstone checks, no visibility logic, no dead-row scanning on every query.

### Divergence 7: Emergent backpressure, not explicit write stalls

**Textbook:** RocksDB implements explicit write stalls: when Level 0 has too many files, writes are artificially slowed or stopped until compaction catches up. Stall thresholds are configurable and trigger warnings in logs.

**VictoriaLogs:** No explicit write stall. Backpressure emerges naturally from channel-based concurrency limiters. If merge workers can't keep up, all CPU merge slots fill, flush goroutines block on channel sends, shard buffers stop flushing, and HTTP handlers block on buffer inserts. Ingestion slows to the pace merges can sustain, then resumes when the backlog clears.

**Why emergent is enough:** The three-tier merge worker separation (in-memory, small, big) prevents cascading stalls — a slow big merge never blocks in-memory merges. The CPU-count-sized channels provide a natural feedback loop without tunable thresholds. For operators, this means fewer knobs to configure and fewer stall-related alerts to manage.

### Putting it together: your engine vs VictoriaLogs

| Feature | Your capstone engine | VictoriaLogs |
|---------|---------------------|--------------|
| Durability | WAL + memtable | No WAL, upstream retry |
| Data format | Row-oriented key-value SSTables | Columnar blocks with per-column encoding |
| Partitioning | Single LSM tree | Per-day partitions |
| Compaction | At least one mature policy | Size-tiered, 7.5× threshold, 3 tiers |
| Manifest | Version edit log | Full-snapshot `parts.json` |
| Deletion | Tombstone records | Merge-time filter |
| Backpressure | Explicit write stalls | Channel-based emergent throttling |
| Bloom filters | Per-SSTable or per-block | Per-column per-block |

Your engine implements the textbook approach. VictoriaLogs departs from it at every layer, each departure justified by the specific characteristics of log workloads: append-only, time-ordered, write-heavy, column-selective reads, and upstream durability guarantees.

The capstone labs ask you to measure the consequences of these choices. Your benchmark results will give you concrete numbers — write amplification, read latency, recovery time, space overhead — that make the comparison tangible rather than theoretical.

## VictoriaLogs Comparison Prompts
- Why does VictoriaLogs use per-day partitioning on top of LSM-like parts?
- Why does VictoriaLogs apply merge heuristics like `minMergeMultiplier`?
- Why is Bloom placed at block-column granularity in the read path?

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
