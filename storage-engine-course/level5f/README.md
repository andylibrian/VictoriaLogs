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

## Recovery and Snapshots in VictoriaLogs (Conceptual Background)

### The manifest: `parts.json`

Every database engine needs an authoritative answer to the question: "which files belong to the current state?" Without this, a crash could leave the system unable to distinguish complete parts from half-written remnants.

In traditional LSM stores (LevelDB, RocksDB), a **MANIFEST** file records version edits — append-only log entries describing which files were added or removed. Recovery replays the MANIFEST from a checkpoint to reconstruct the current file set.

VictoriaLogs uses a simpler approach: a single `parts.json` file that lists every active file-based part by name. It is not an append-only log — it is **overwritten atomically** on every change. The current file set is read in one step, not reconstructed from a sequence of edits.

```json
["0001A2B3C4D5E6F7", "0001A2B3C4D5E6F8", "0001A2B3C4D5E6F9"]
```

Each entry is a part directory name (a 16-character hex string derived from an atomic counter). In-memory parts are never listed — they exist only in RAM and are either flushed to disk before shutdown or lost on crash.

**Why a full snapshot instead of a version edit log?** The part count is small (typically tens to low hundreds). Writing the full list is cheap, and it eliminates the complexity of edit log compaction, checkpoint management, and replay ordering. The trade-off is that each `parts.json` write replaces the entire file, but at a few hundred bytes per entry, this is negligible.

### Atomic writes: the foundation

Every `parts.json` update follows a strict protocol to ensure crash safety:

```
Step 1: Write the full content to a temporary file (parts.json.tmp.N)
Step 2: fsync the temporary file (data is durable on disk)
Step 3: rename(parts.json.tmp.N → parts.json)  (atomic on POSIX)
Step 4: fsync the parent directory (rename is durable)
```

At every point during this sequence, `parts.json` is in a consistent state:

| Crash point | `parts.json` state |
|-------------|-------------------|
| Before Step 1 | Old version (unchanged) |
| During Step 1 | Old version; temp file is partial |
| After Step 2, before Step 3 | Old version; temp file is complete |
| After Step 3 | New version |

The `rename` system call is the commit point. It is atomic on POSIX filesystems — the directory entry either points to the old file or the new file, never to a torn state. The orphaned temp file (if crash happens before rename) is harmless — it's not a directory, so `mustRemoveUnusedDirs` ignores it.

This is a fundamental pattern in storage systems: **write-then-rename**. It converts the problem of "atomically updating file contents" into the simpler problem of "atomically updating a directory entry," which the filesystem already guarantees.

### The startup sequence

When VictoriaLogs opens, it reconstructs its state from disk in a strict order:

```
MustOpenStorage(path)
│
├── 1. Acquire flock.lock
│      Exclusive OS-level file lock prevents two processes from
│      opening the same storage simultaneously. If the lock is held,
│      the process panics immediately.
│
├── 2. Load delete_tasks.json
│      Pending delete tasks from previous run are restored.
│
├── 3. Scan partitions/ directory
│      Each subdirectory (named YYYYMMDD) is a partition.
│      Directories with a .delete-this-dir sentinel are partially
│      removed — deletion is completed now.
│
├── 4. Open partitions in parallel (bounded by CPU count)
│      For each partition:
│      │
│      ├── 4a. Open indexdb (stream metadata)
│      │       If missing but datadb exists → FATAL (corruption)
│      │       If missing and datadb missing → auto-create
│      │
│      └── 4b. Open datadb (log data)
│              │
│              ├── Read parts.json → list of part names
│              │
│              ├── Remove orphan directories
│              │   Any directory in datadb/ not listed in parts.json
│              │   is deleted. These are remnants of crashed merges.
│              │
│              ├── Validate listed parts exist on disk
│              │   If a part is listed but missing → FATAL
│              │
│              ├── Open each part, categorize as small or big
│              │   based on CompressedSizeBytes
│              │
│              └── Start background workers
│                  (merge workers, flush timer, etc.)
│
├── 5. Drop partitions beyond retention
│
└── 6. Start background loops
       (retention watcher, delete task watcher,
        snapshot age watcher)
```

The ordering is intentional. The file lock (Step 1) must be acquired before any state is read. Partition opening (Step 4) is parallelized for fast startup on systems with many partitions. Within each partition, indexdb must open before datadb because datadb queries indexdb during stream resolution.

### Orphan cleanup: the safety net

The key crash recovery mechanism is **orphan cleanup** — the removal of part directories that exist on disk but aren't listed in `parts.json`. This handles the most common crash scenario: a merge that completed writing a new part but crashed before `parts.json` was updated.

Consider a merge of parts A, B, C into part D:

```
Timeline of a normal merge:

  1. Write part D's files (timestamps.bin, values.bin, bloom.bin, ...)
  2. Write metadata.json for part D
  3. fsync part D's directory
  4. Under partsLock:
     a. Remove A, B, C from in-memory part lists
     b. Add D to in-memory part lists
     c. Write parts.json = [..., D, ...]   (atomically)
  5. After lock:
     a. Mark A, B, C as mustDrop
     b. decRef A, B, C (delete when no readers remain)
```

Crash scenarios and their recovery:

| Crash point | parts.json says | On disk | Recovery |
|-------------|----------------|---------|----------|
| During Step 1 | [A, B, C] | A, B, C, partial D | D is orphan → deleted |
| After Step 3, before Step 4c | [A, B, C] | A, B, C, D | D is orphan → deleted |
| After Step 4c, before Step 5 | [D] | A, B, C, D | A, B, C are orphans → deleted |
| After Step 5 | [D] | D | Clean state |

In every case, `parts.json` is the source of truth. Directories not in the manifest are orphans. Directories in the manifest must exist — if they don't, it's an unrecoverable error (FATAL panic).

### Safe directory deletion

Even directory deletion can be interrupted by a crash. VictoriaLogs uses a **sentinel file pattern** to make deletion restartable:

```
Deleting directory "part_XYZ/":

  1. Create sentinel file: part_XYZ/.delete-this-dir
  2. fsync part_XYZ/
  3. Delete all files inside part_XYZ/ (except sentinel)
  4. fsync part_XYZ/
  5. Delete sentinel file
  6. Delete the now-empty directory
```

If the process crashes at any point after Step 1, the sentinel file remains. On next startup, any directory containing `.delete-this-dir` is recognized as "partially removed" and the deletion is re-run from the beginning.

This pattern converts a non-atomic operation (deleting a directory with many files) into a restartable one. The sentinel is the "intent record" — its presence means "I was trying to delete this, please finish."

### Snapshot: consistent backup via hard links

VictoriaLogs supports creating snapshots of individual partitions. A snapshot is a point-in-time copy that can be used for backup without stopping ingestion.

The snapshot mechanism:

```
mustCreateSnapshot()
│
├── 1. Force-flush all in-memory parts to disk
│      isFinal=true: current in-memory parts become file-backed
│      (rows still in shard buffers are outside this step)
│
├── 2. Under partsLock: collect all file-based parts, incRef each
│      References prevent parts from being deleted during snapshot
│
├── 3. Write parts.json for the snapshot directory
│      Lists exactly the parts that exist at this moment
│
├── 4. Hard-link every file from each part into the snapshot
│      For each part directory:
│        For each file (timestamps.bin, values.bin, ...):
│          os.Link(live/part/file, snapshot/part/file)
│
├── 5. decRef all parts (release references)
│
└── 6. fsync the snapshot directory
```

**Hard links** are the key to making this efficient. A hard link creates a second directory entry pointing to the same inode — the same physical blocks on disk. No data is copied. The snapshot occupies zero additional space at creation time.

```
Live storage:                    Snapshot:
  datadb/                          snapshots/20260102120000/datadb/
    part_ABC/                        part_ABC/
      timestamps.bin ─────┐            timestamps.bin ─────┐
      values.bin ─────────┤            values.bin ─────────┤
                          │                                │
                          └── same inode ──────────────────┘
```

After the snapshot is created, live parts can be merged and deleted. Because the snapshot's hard links still reference the original inodes, the data stays on disk until both the live reference and the snapshot reference are removed. This is how Unix reference counting works — a file's blocks are freed only when its link count reaches zero.

**What about consistency?** The forced flush in Step 1 ensures current in-memory parts are on disk. The `incRef` in Step 2 prevents concurrent merges from deleting selected parts during snapshot linking. The result is a consistent file-level view without partial-merge artifacts.

**What about concurrent ingestion?** Ingestion is **not** paused during snapshot creation. Data arriving during snapshot creation can land in new buffers/parts and may be excluded from the snapshot.

### Partition detach: safe offline backup

For backup workflows that need to copy partition files externally (e.g., to cloud storage), VictoriaLogs provides a detach/attach API:

```
PartitionDetach(name)
  1. Remove partition from the active list (under lock)
  2. Drop the storage's reference
  3. Block until all in-flight queries and merges release their references
  4. Return — no goroutine holds any file handle into the partition

  → Caller can now safely copy/move the partition directory

PartitionAttach(name)
  → Re-open the partition and add it back to the active list
```

This is stronger than a snapshot: the partition is fully quiesced. No concurrent reader or writer touches it. The trade-off is that queries against that partition will fail while it's detached.

### Failure modes and manual intervention

VictoriaLogs takes a deliberate stance on unrecoverable errors: **panic rather than silently corrupt**. Some situations cannot be auto-recovered:

| Scenario | Response | Why |
|----------|----------|-----|
| `parts.json` missing, part dirs exist | FATAL panic | Ambiguous: which parts are current? Cannot guess. |
| Part listed in `parts.json` but missing from disk | FATAL panic | Data loss confirmed. Admin must decide what to do. |
| `indexdb` missing but `datadb` present | FATAL panic | Stream metadata lost. Queries would return wrong results. |
| `flock.lock` held by another process | FATAL panic | Concurrent access would corrupt data. |

The philosophy: it's better to stop and ask for help than to silently serve incomplete data. An operator can examine the situation — restore from backup, manually edit `parts.json`, or accept the loss — and make an informed decision.

### Design lesson: simplicity as a recovery strategy

Compare VictoriaLogs' recovery with a system using a version edit log (like LevelDB's MANIFEST):

| Aspect | Version edit log | Full-snapshot manifest |
|--------|-----------------|----------------------|
| Recovery complexity | Replay edits from checkpoint, handle partial edits | Read one file, delete orphans |
| File format | Binary log with checksums, requires careful framing | Plain JSON array |
| Compaction of manifest itself | Required (manifest grows over time) | Not needed (file is always current) |
| Crash during manifest update | Must handle partial appends | Handled by write-then-rename |
| Code complexity | ~500+ lines for manifest management | ~50 lines for read/write/cleanup |

VictoriaLogs' approach works because the part count is manageable (tens to hundreds, not millions). For a system with millions of small files, a version edit log would be essential to avoid rewriting a massive manifest on every change. But for VictoriaLogs' architecture, the simpler approach eliminates an entire category of bugs.

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
