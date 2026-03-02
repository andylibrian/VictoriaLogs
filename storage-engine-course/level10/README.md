# Level 10 - Lifecycle Ops, Retention/Delete Semantics, and Capstone

## Objective
Connect algorithmic internals to operational behavior, then produce a full architecture-level synthesis.

## Outcomes
By the end of this level, you can:
- explain snapshot/retention/delete behavior using underlying data structures
- explain why some operations rely on merge for physical effects
- produce a defendable engineering improvement proposal

## Lifecycle Operations in VictoriaLogs (Conceptual Background)

### Three kinds of data removal

VictoriaLogs removes data in three fundamentally different ways, each with its own mechanism, latency, and guarantees:

| Operation | Mechanism | Latency | Space reclaimed |
|-----------|-----------|---------|-----------------|
| Retention expiry | Drop entire partition directory | Watcher cadence (~1h), then immediate removal from active list | When partition `refCount` reaches 0 |
| Disk pressure | Drop oldest partition directory | Watcher cadence (~10s), then immediate removal from active list | When partition `refCount` reaches 0 |
| Logical delete | Merge with drop filter | Minutes to hours | After merge completes and old parts are deleted |

Understanding which mechanism applies — and why — is the core of this level.

### Retention: the simplest removal

VictoriaLogs partitions data by day. Each partition is a self-contained directory with its own `datadb`, `indexdb`, and snapshot subdirectories. Retention is enforced by the `watchRetention` background loop, which runs hourly (with jitter):

```
watchRetention() — runs every ~1 hour
│
├── For each partition:
│     Is partition.day + retentionPeriod < today?
│       Yes → mark partition for removal
│
├── Under partitionsLock:
│     Remove expired partitions from the list
│     Record their day values in deletedPartitions[]
│
└── For each removed partition:
      ptw.mustDrop = true
      ptw.decRef()
        → when refCount hits 0: mustClosePartition + mustDeletePartition
        → mustDeletePartition calls fs.MustRemoveDir on the entire directory
```

Because data is organized by day, retention never needs to examine individual rows. Dropping a partition deletes all its files — `parts.json`, every part directory, the indexdb, and any snapshots — in one operation. No merge, no tombstone, no compaction pass.

**The `deletedPartitions` guard:** After a partition is removed from the active list, concurrent writers might still try to insert rows destined for that day (e.g., late-arriving logs with old timestamps). The `deletedPartitions` list (a set of day values) prevents re-creating a partition that was just deleted. Without this guard, a burst of late logs could resurrect an expired partition, violating the retention guarantee.

### Disk pressure: emergency retention

The `watchMaxDiskSpaceUsage` watcher runs every ~10 seconds and checks whether the total storage size exceeds `maxDiskSpaceUsageBytes`. If so, it drops the oldest partitions first (while keeping the newest two day partitions) until usage falls below the limit or only those two remain:

```
watchMaxDiskSpaceUsage() — runs every ~10 seconds
│
├── Calculate totalSize across all partitions
│
├── While totalSize > maxDiskSpaceUsageBytes:
│     Find oldest partition (smallest day value)
│     Remove it (same mechanism as retention)
│     Subtract its size from totalSize
│
└── If free disk < minFreeDiskSpaceBytes:
      Storage enters read-only mode (IsReadOnly = true)
      Ingestion returns 429 until space is freed
```

This is a safety valve. Even if the retention period is set to 30 days, if the disk fills up, the system drops older partitions to survive. The read-only mode is the last resort — it prevents the system from crashing due to disk exhaustion, giving operators time to add capacity or adjust retention.

### Logical delete: the slow path through merge

When an operator issues `POST /delete/execute?filter=level:error AND _time:[10:00,10:05]`, the data isn't immediately removed. Instead:

```
Delete request arrives
│
├── 1. Create DeleteTask with filter + StartTime (= now)
│      StartTime bounds by event timestamp (`_time <= StartTime`):
│      rows with newer `_time` are excluded.
│
├── 2. Persist to delete_tasks.json (crash-safe)
│
├── 3. watchDeleteTasks picks it up (~1 second)
│
├── 4. For each partition in the time range:
│      │
│      ├── 4a. Identify parts with matching rows
│      │       Uses the same bloom filter + block search machinery
│      │       as queries. Parts with zero matches are skipped entirely.
│      │
│      ├── 4b. Merge matching parts with dropFilter applied
│      │       The merge writer omits rows that match the filter.
│      │       Full blocks with no matching rows pass through as raw bytes.
│      │       Blocks with some matching rows are decompressed,
│      │       filtered row-by-row, and re-blocked.
│      │
│      └── 4c. Atomic swap: old parts → new parts (without deleted rows)
│             Old parts are reference-counted and deleted when safe.
│
└── 5. Delete task removed from delete_tasks.json
```

**Why this is slow:** The merge must read, decompress, filter, re-compress, and write every block that contains matching rows. For a delete touching 1% of data across a 100 GB partition, the system might rewrite 10-20 GB of part files. This takes minutes, not milliseconds.

**Why queries see stale results:** Between steps 3 and 4c, queries still see the not-yet-deleted rows. There are no tombstone records — the rows are physically present in the old parts until the merge replaces them. This is acceptable for log systems where deletes are rare operations (GDPR compliance, error cleanup), not a regular part of the query path.

**The `StartTime` bound:** A delete request adds a time predicate ending at `StartTime` (`_time <= StartTime`). This bounds deletion by log event time, not by ingestion arrival order.

### `mustForceMergeAllParts`: reclaiming space on demand

After a delete, there may still be many file parts. `mustForceMergeAllParts` runs an aggressive merge pass over currently known file parts:

```
mustForceMergeAllParts()
│
├── Flush all in-memory parts to files
├── Collect current small + big file parts
└── Repeatedly pick merge groups (or single-part fallback) and execute merges
    with normal size constraints
```

This is useful after bulk deletions or when an operator wants a consolidation pass. It is expensive and may rewrite large amounts of data; it does not guarantee "one part per tier" in a single call.

### Snapshots: consistent backup via hard links

A snapshot captures a point-in-time view of a partition that can be copied for backup without stopping ingestion. The mechanism relies on Unix hard links and reference counting:

```
mustCreateSnapshot()
│
├── 1. Acquire snapshotLock (serializes snapshots for this partition)
│
├── 2. Force-flush all in-memory parts to disk
│      isFinal=true: every in-memory part becomes a file part.
│      Rows still in shard buffers (`rowsBuffer`) are not part of this step.
│
├── 3. Snapshot indexdb
│      Hard-link all mergeset table files into the snapshot directory.
│
├── 4. Snapshot datadb
│      Under partsLock:
│        Collect all small + big parts
│        incRef() on each (prevent deletion during snapshot)
│      Release partsLock
│
│      Write parts.json for the snapshot (lists the same parts)
│
│      For each part:
│        fs.MustHardLinkFiles(live/part/, snapshot/part/)
│        Creates hard links for every file in the part directory:
│          timestamps.bin, values.bin0..N, bloom.bin0..N,
│          index.bin, metaindex.bin, metadata.json, etc.
│
│      decRef() on each part (release references)
│
├── 5. fsync the snapshot directory
│
└── 6. Release snapshotLock
```

**Why hard links work:** A hard link creates a second directory entry pointing to the same inode — the same physical disk blocks. The snapshot directory and the live part directory share the same data with zero copy overhead. The snapshot uses no additional disk space at creation time.

**What happens when a merge replaces a live part after the snapshot:**

```
Time 0: Live and snapshot both point to part_ABC's inode
  Live:     datadb/part_ABC/timestamps.bin  → inode #42
  Snapshot: snapshot/part_ABC/timestamps.bin → inode #42
  Inode #42 link count = 2

Time 1: Merge replaces part_ABC with part_DEF in live
  Live:     datadb/part_ABC deleted (link count: 2 → 1)
            datadb/part_DEF created
  Snapshot: snapshot/part_ABC/timestamps.bin → inode #42
  Inode #42 link count = 1 (still alive — snapshot holds the link)

Time 2: Snapshot deleted
  Snapshot: snapshot/part_ABC deleted (link count: 1 → 0)
  Inode #42 freed — disk space reclaimed
```

The Unix filesystem guarantees that a file's blocks are freed only when its link count reaches zero. The snapshot's hard links keep the data alive regardless of what happens in the live storage.

**Consistency guarantee:** Step 2 ensures current in-memory parts are file-backed before linking. The `incRef` in step 4 prevents selected parts from being deleted during hard-linking. The result is a consistent cut of selected file parts, while rows still in shard buffers or arriving concurrently may be outside the snapshot.

**Concurrent ingestion is not paused.** This is a deliberate trade-off: the snapshot may miss rows still buffered in shards (typically up to ~1 second per shard) and rows arriving during snapshot execution, but ingestion throughput is unaffected.

### Stale snapshot cleanup

The `watchSnapshotsMaxAge` watcher runs every ~1 minute and deletes snapshots older than `snapshotsMaxAge`:

```
For each partition:
  For each snapshot directory:
    If stat(snapshotDir).ModTime + maxAge < now:
      fs.MustRemoveDir(snapshotDir)
```

This prevents forgotten snapshots from holding hard links to old data indefinitely, which would prevent disk space reclamation even after the live parts are merged and deleted.

### Partition detach/attach: safe external backup

For backup workflows that need to copy files to external storage (S3, NFS, etc.), snapshots may not be sufficient — the operator may need direct file access without any concurrent I/O. The detach/attach API provides this:

```
PartitionDetach(name)
│
├── Under partitionsLock:
│     Remove partition from active list
│     Clear ptwHot if it points to this partition
│
├── ptw.decRef() — drop Storage's own reference
│
└── <-ptw.doneCh — BLOCK until refCount reaches 0
      All in-flight queries and merges must finish first.
      No goroutine holds any file handle into the partition.

  → Operator can now safely: cp -r, rsync, tar, upload to S3

PartitionAttach(name)
│
├── Re-open the partition (mustOpenPartition)
└── Add to active list under partitionsLock
```

The key difference from snapshots: detach **fully quiesces** the partition. No concurrent readers, no background merges, no flush timers. The partition is completely idle. The cost is that queries against this partition's time range will return no results while it's detached.

### How reference counting connects everything

Every lifecycle operation — retention, deletion, snapshots, detach — depends on the same reference counting mechanism:

```
Operation              Uses incRef/decRef on    Reason
────────────────────── ─────────────────────── ──────────────────────────
Query scan              partWrapper              Prevent part deletion mid-scan
Merge execution         partWrapper              Prevent source part deletion mid-merge
Snapshot creation       partWrapper              Prevent part deletion during hard-linking
Retention removal       partitionWrapper         Wait for queries to finish before delete
Disk pressure removal   partitionWrapper         Same as retention
Partition detach        partitionWrapper         Block until all activity ceases
```

Without reference counting, every operation would need its own coordination mechanism — read-write locks, condition variables, quiesce protocols. The unified `incRef`/`decRef` pattern replaces all of these with a single primitive: "increment before use, decrement after use, delete when zero."

### The lifecycle of a log entry: end to end

Putting it all together, a single log entry's lifecycle touches every mechanism covered in this course:

```
1. INGESTION
   HTTP POST → vlinsert parses → partition.mustAddRows()
   → rowsBuffer shard (per-CPU mutex, ~1 second buffer)
   → mustFlushLogRows() → inmemoryPart (columnar, sorted, queryable)

2. DURABILITY
   → inmemoryPartsFlusher (configured interval; default 5s) → MustStoreToDisk()
   → part directory on disk (timestamps.bin, values.bin, bloom.bin, ...)
   → metadata.json written last, fsync, parts.json updated atomically

3. COMPACTION
   → appendPartsToMerge selects similarly-sized parts
   → mustMergeBlockStreams: k-way heap merge-sort
   → full blocks pass through as raw bytes (fast path)
   → small blocks accumulated and re-blocked
   → swapSrcWithDstParts: atomic replacement under partsLock
   → old parts deleted when refCount reaches 0

4. QUERY
   → partition pruning → part pruning → metaindex pruning
   → index block pruning → bloom precheck → value scan
   → bitmap-based row filtering → result assembly → pipe chain

5. BACKUP
   → snapshot: force flush + hard-link all part files
   → or detach: quiesce partition + external copy + reattach

6. DELETION (if requested)
   → deleteRows: identify matching parts via bloom/filter
   → merge with dropFilter: matching rows omitted from output
   → atomic swap: old parts replaced, space reclaimed after old refs drain

7. EXPIRY
   → watchRetention: partition day + retention < today
   → remove partition from list, record in deletedPartitions
   → decRef → mustClosePartition → mustDeletePartition
   → entire directory tree removed
```

Every stage builds on the primitives from earlier levels: immutable parts (Level 5A), flush pipeline (Level 5B), SSTable format (Level 5C), bloom-assisted reads (Level 5D), merge execution (Level 5E), crash recovery (Level 5F), and concurrency coordination (Level 5G). The capstone deliverable asks you to synthesize these into a coherent system model and propose an improvement — demonstrating that you can reason about the system as a whole, not just individual components.

## Source Anchors
- `lib/logstorage/partition.go`
  - `mustCreateSnapshot`
- `lib/logstorage/datadb.go`
  - `mustCreateSnapshotAt`
  - `deleteRows`
  - `mustForceMergeAllParts`
- `lib/logstorage/storage.go`
  - retention/disk watchers
  - partition detach/attach lifecycle
- `onboarding/onboarding-partition-lifecycle.md`

## Core Concepts
1. Snapshot via hard links for point-in-time consistency.
2. Time/disk retention via partition lifecycle operations.
3. Logical delete vs physical reclamation through merge.
4. Safety and correctness from ref counting and atomicity.

## Guided Reading Tasks
1. Trace snapshot creation path and identify where in-memory data is forced to file-backed parts.
2. Trace delete task effect chain:
   - match rows
   - merge with drop filter
   - eventual disk reclaim
3. Trace retention deletion path and explain `deletedPartitions` guard implications.

## Hands-On Lab
### Lab 1: Snapshot Consistency Story
Write a precise explanation of why snapshot+hard-links remain consistent while background merges continue.

### Lab 2: Delete-Reclaim Experiment Design
Design an experiment that demonstrates:
- rows disappear from query results before space is reclaimed
- force merge changes on-disk space usage

### Lab 3: Retention Incident Drill
Simulate an ops scenario where disk pressure triggers old partition removal.
Document expected behavior and safeguards.

## Capstone
Produce `level10/capstone.md` covering:
1. End-to-end write path (row -> block -> part -> merge)
2. End-to-end query path (query -> prune -> block scan -> rows)
3. Bloom filter role and false-positive implications
4. Merge heuristic tradeoff analysis
5. One optimization proposal with:
   - expected benefit
   - explicit risks
   - validation plan

## Final Review Rubric
- Technical correctness: concepts map accurately to code.
- Completeness: both write and query paths are covered.
- Tradeoff quality: proposal discusses cost/benefit/risk.
- Verifiability: contains measurable validation plan.

## Deliverables
- `level10/snapshot-consistency.md`
- `level10/delete-reclaim-design.md`
- `level10/retention-incident-drill.md`
- `level10/capstone.md`
- `level10/final-checkpoint.md`

## Pass Criteria
- You can defend your system model under detailed engineering questioning.
- You can propose changes responsibly with explicit operational tradeoffs.
