# VictoriaLogs Partition Lifecycle - Developer Onboarding Guide

This document covers the operational lifecycle of [partitions](./glossary.md#partition) in VictoriaLogs: creation, attach/detach, snapshots, backup/restore, delete tasks, and automatic retention. It focuses on the runtime management APIs and background workers rather than the internal storage format (see [onboarding-storage-engine.md](./onboarding-storage-engine.md) for that).

## Table of Contents

- [Overview](#overview)
- [Partition Fundamentals](#partition-fundamentals)
  - [Naming Convention](#naming-convention)
  - [partitionWrapper and Reference Counting](#partitionwrapper-and-reference-counting)
  - [Partition Creation](#partition-creation)
- [HTTP API Endpoints](#http-api-endpoints)
- [Partition Attach & Detach](#partition-attach--detach)
  - [Attach](#attach)
  - [Detach](#detach)
  - [List](#list)
- [Snapshots](#snapshots)
  - [Creating Snapshots](#creating-snapshots)
  - [How Snapshots Work Internally](#how-snapshots-work-internally)
  - [Listing Snapshots](#listing-snapshots)
  - [Deleting Snapshots](#deleting-snapshots)
  - [Automatic Snapshot Cleanup](#automatic-snapshot-cleanup)
- [Backup & Restore](#backup--restore)
  - [Backup Procedure](#backup-procedure)
  - [Restore Procedure](#restore-procedure)
  - [Cross-Storage Migration](#cross-storage-migration)
- [Delete Tasks](#delete-tasks)
  - [DeleteTask Struct](#deletetask-struct)
  - [Starting a Delete Task](#starting-a-delete-task)
  - [Delete Task Processing](#delete-task-processing)
  - [Stopping a Delete Task](#stopping-a-delete-task)
  - [Persistence](#persistence)
- [Automatic Retention](#automatic-retention)
  - [Time-Based Retention](#time-based-retention)
  - [Disk Space Retention](#disk-space-retention)
  - [Read-Only Mode](#read-only-mode)
- [Force Merge & Force Flush](#force-merge--force-flush)
- [Storage Shutdown](#storage-shutdown)
- [Configuration Flags](#configuration-flags)
- [Key Design Patterns](#key-design-patterns)

---

## Overview

VictoriaLogs organizes log data into **per-day [partitions](./glossary.md#partition)**. Each partition is a self-contained directory that holds one calendar day's worth of logs. This design enables:

- **Cheap directory-level retention**: Drop an entire day by deleting its directory (no data rewrite)
- **Live attach/detach**: Add or remove partitions without restarting the server
- **Instant snapshots**: Create snapshots via filesystem hard links
- **Cross-storage migration**: Move partitions between NVMe and HDD storage tiers

All partition management operations are exposed through `/internal/partition/*` HTTP endpoints and can be protected by the `-partitionManageAuthKey` flag. These operations are available on storage nodes in local mode; for distributed routing context see [VictoriaLogs Cluster Architecture](./onboarding-cluster.md).

---

## Partition Fundamentals

### Naming Convention

Partitions are named in **YYYYMMDD** format, representing the calendar day they store data for.

**Key File**: [`lib/logstorage/partition.go`](../lib/logstorage/partition.go#L297)

```go
const partitionNameFormat = "20060102"  // Go time format for YYYYMMDD
```

Helper functions:
- [`getPartitionDayFromName(name)`](../lib/logstorage/partition.go#L283) — parses "20260101" to a unix day number
- [`getPartitionNameFromDay(day)`](../lib/logstorage/partition.go#L292) — formats a unix day number to "20260101"

### On-Disk Structure

Each partition directory contains two subdirectories:

```
	<storageDataPath>/partitions/
	└── 20260101/              # partition (YYYYMMDD)
	    ├── indexdb/            # stream metadata (mergeset tables)
	    ├── datadb/             # log data (parts, blocks, bloom filters)
	    └── snapshots/          # created on demand
	        └── 20260101120000-00000001/ # snapshot (YYYYMMDDhhmmss-hex)
	            ├── indexdb/    # hard-linked index snapshot
	            └── datadb/     # hard-linked data snapshot
```

### partitionWrapper and Reference Counting

**Key File**: [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L538)

Every partition is accessed through a `partitionWrapper` that provides safe concurrent access via [reference counting](./glossary.md#reference-counting):

```go
type partitionWrapper struct {
    refCount atomic.Int32   // number of active references              [L541]
    mustDrop atomic.Bool    // if true, delete partition at refCount=0  [L544]
    day      int64          // unix day number                          [L547]
    pt       *partition     // the wrapped partition                    [L550]
    doneCh   chan struct{}  // closed when refCount reaches zero        [L553]
}
```

The lifecycle of a `partitionWrapper`:

1. [`newPartitionWrapper(pt, day)`](../lib/logstorage/storage.go#L556) creates a wrapper with initial `refCount=1` — [L562](../lib/logstorage/storage.go#L562)
2. Callers use [`incRef()`](../lib/logstorage/storage.go#L566) before accessing the partition and [`decRef()`](../lib/logstorage/storage.go#L570) when done
3. When `refCount` reaches 0 ([L570-592](../lib/logstorage/storage.go#L570)):
   - The partition is closed via `mustClosePartition()` — [L582](../lib/logstorage/storage.go#L582)
   - If `mustDrop` is set, the partition directory is deleted — [L587](../lib/logstorage/storage.go#L587)
   - `doneCh` is closed to unblock anyone waiting on the partition — [L591](../lib/logstorage/storage.go#L591)

This pattern allows safe detach and retention deletion: set `mustDrop`, call `decRef()`, and the partition is automatically cleaned up when the last reader finishes.

### Partition Creation

Partitions are created automatically during data ingestion by [`Storage.getPartitionForWriting(day)`](../lib/logstorage/storage.go#L1244):

1. **Binary search** in the sorted `s.partitions` list — [L1250-1252](../lib/logstorage/storage.go#L1250)
2. If not found, check `deletedPartitions` to avoid re-creating dropped partitions — [L1262](../lib/logstorage/storage.go#L1262)
3. If the directory already exists on disk (e.g., manually placed) but isn't attached, return nil — the user must call `PartitionAttach()` — [L1269](../lib/logstorage/storage.go#L1269)
4. Otherwise, create a new partition via [`mustCreatePartition(path)`](../lib/logstorage/partition.go#L52) — which creates the directory with `indexdb/` and `datadb/` subdirectories

---

## HTTP API Endpoints

**Key File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L243)

All partition management endpoints are routed by [`RequestHandler()`](../app/vlstorage/main.go#L243) and are only available when running in **local storage mode** (not cluster mode with `-storageNode`). Each handler checks the `-partitionManageAuthKey` flag.

| Endpoint | Handler | Line | Query Params | Response |
|----------|---------|------|-------------|----------|
| `/internal/partition/attach` | [`processPartitionAttach`](../app/vlstorage/main.go#L332) | 332 | `name=YYYYMMDD` | 200 OK or error |
| `/internal/partition/detach` | [`processPartitionDetach`](../app/vlstorage/main.go#L351) | 351 | `name=YYYYMMDD` | 200 OK or error |
| `/internal/partition/list` | [`processPartitionList`](../app/vlstorage/main.go#L370) | 370 | (none) | JSON `["20260101","20260102"]` |
| `/internal/partition/snapshot/create` | [`processPartitionSnapshotCreate`](../app/vlstorage/main.go#L390) | 390 | `partition_prefix=...` | JSON `["/path/to/snap"]` |
| `/internal/partition/snapshot/list` | [`processPartitionSnapshotList`](../app/vlstorage/main.go#L429) | 429 | (none) | JSON `["/path/to/snap"]` |
| `/internal/partition/snapshot/delete` | [`processPartitionSnapshotDelete`](../app/vlstorage/main.go#L449) | 449 | `path=/path/to/snap` | 204 No Content |
| `/internal/partition/snapshot/delete_stale` | [`processPartitionSnapshotDeleteStale`](../app/vlstorage/main.go#L474) | 474 | `max_age=3d` (optional) | JSON `["/deleted/paths"]` |
| `/internal/force_merge` | [`processForceMerge`](../app/vlstorage/main.go#L293) | 293 | `partition_prefix=...` | 200 OK (runs in background) |
| `/internal/force_flush` | [`processForceFlush`](../app/vlstorage/main.go#L316) | 316 | (none) | 200 OK |

---

## Partition Attach & Detach

### Attach

**Key Function**: [`Storage.PartitionAttach(name)`](../lib/logstorage/storage.go#L217)

Attaches a partition that exists on disk but isn't currently loaded. This is used for:
- Restoring a partition from backup
- Re-attaching a previously detached partition
- Loading a partition copied from another instance

**Flow**:

```
HTTP: GET /internal/partition/attach?name=20260101
  │
  ▼
processPartitionAttach()                    [vlstorage/main.go:332]
  │
  ▼
Storage.PartitionAttach(name)               [storage.go:217]
  │
  ├─ 1. Parse name → day number             [storage.go:218]
  ├─ 2. Acquire partitionsLock               [storage.go:223]
  ├─ 3. Check deletedPartitions list         [storage.go:226]
  │     (reject if already deleted by retention)
  ├─ 4. Check for duplicate attach           [storage.go:231]
  ├─ 5. Verify directory exists on disk      [storage.go:240]
  ├─ 6. Open partition (indexdb + datadb)    [storage.go:244]
  ├─ 7. Wrap in partitionWrapper             [storage.go:245]
  ├─ 8. Append to s.partitions and re-sort   [storage.go:247-248]
  └─ 9. Log success                          [storage.go:250]
```

**Error cases**:
- Partition day is in `deletedPartitions` (was already dropped by retention) — [L226-228](../lib/logstorage/storage.go#L226)
- Partition already attached — [L231-234](../lib/logstorage/storage.go#L231)
- Partition directory doesn't exist on disk — [L240-241](../lib/logstorage/storage.go#L240)

### Detach

**Key Function**: [`Storage.PartitionDetach(name)`](../lib/logstorage/storage.go#L260)

Safely detaches a partition at runtime. The function **blocks** until all concurrent readers and writers finish their work on the partition.

**Flow**:

```
HTTP: GET /internal/partition/detach?name=20260101
  │
  ▼
processPartitionDetach()                    [vlstorage/main.go:351]
  │
  ▼
Storage.PartitionDetach(name)               [storage.go:260]
  │
  ├─ 1. Lock partitionsLock                  [storage.go:262]
  ├─ 2. Find partition by name               [storage.go:265-266]
  ├─ 3. Remove from s.partitions slice       [storage.go:271]
  ├─ 4. Clear s.ptwHot if it was the hot partition [storage.go:272-273]
  ├─ 5. Unlock partitionsLock                [storage.go:263 (defer)]
  ├─ 6. Decrement reference count            [storage.go:285]
  ├─ 7. BLOCK on <-ptw.doneCh               [storage.go:288]
  │     (waits for all readers/writers to finish)
  └─ 8. Log success                          [storage.go:290]
```

**Important**: The detach call blocks on `<-ptw.doneCh` at [L288](../lib/logstorage/storage.go#L288). This means the HTTP request will not return until every concurrent query or ingestion operation on that partition has completed. This guarantees the partition is fully quiesced and safe to move/delete/restore.

**Restart behavior**: Detach affects only the currently running process. If the detached partition directory remains under `<storageDataPath>/partitions/`, it is attached again on restart when `MustOpenStorage()` scans and opens partition directories — [L684-715](../lib/logstorage/storage.go#L684).

### List

**Key Function**: [`Storage.PartitionList()`](../lib/logstorage/storage.go#L298)

Returns the names of all currently attached partitions:

```go
func (s *Storage) PartitionList() []string {
    s.partitionsLock.Lock()
    ptNames := make([]string, len(s.partitions))
    for i, ptw := range s.partitions {
        ptNames[i] = ptw.pt.name
    }
    s.partitionsLock.Unlock()
    return ptNames
}
```

The HTTP handler at [`processPartitionList()`](../app/vlstorage/main.go#L370) ensures an empty list returns `[]` not `null` — [L381-383](../app/vlstorage/main.go#L381).

---

## Snapshots

### Creating Snapshots

**Storage-level function**: [`Storage.PartitionSnapshotMustCreate(partitionPrefix)`](../lib/logstorage/storage.go#L318)

The `partitionPrefix` parameter supports flexible matching:

| Prefix | Matches | Example |
|--------|---------|---------|
| `YYYYMMDD` | Single day | `20260101` matches only that partition |
| `YYYYMM` | Entire month | `202601` matches all January 2026 partitions |
| `YYYY` | Entire year | `2026` matches all 2026 partitions |
| `""` (empty) | All partitions | Creates snapshots for everything |

The function iterates all partitions and creates a snapshot for each one matching the prefix — [L324-329](../lib/logstorage/storage.go#L324).

**Client disconnect handling**: The HTTP handler [`processPartitionSnapshotCreate()`](../app/vlstorage/main.go#L390) checks `r.Context().Err()` after creation. If the client disconnected, all created snapshots are automatically deleted — [L412-422](../app/vlstorage/main.go#L412). This prevents orphaned snapshots.

### How Snapshots Work Internally

**Partition-level function**: [`partition.mustCreateSnapshot()`](../lib/logstorage/partition.go#L222)

```
mustCreateSnapshot()                        [partition.go:222]
  │
  ├─ 1. Acquire snapshotLock (prevents concurrent snapshots) [partition.go:226]
  ├─ 2. Generate unique name via snapshotutil.NewName()       [partition.go:229]
  │     Format: YYYYMMDDhhmmss-XXXXXXXX (timestamp + hex counter)
  ├─ 3. Create snapshot directory:
  │     <partition_path>/snapshots/<snapshot_name>/            [partition.go:230-231]
  │
  ├─ 4. Snapshot indexdb:
  │     idb.mustCreateSnapshotAt(dstIndexdbDir)                [partition.go:234]
  │     └── calls mergeset.Table.MustCreateSnapshotAt()
  │
  ├─ 5. Snapshot datadb:
  │     ddb.mustCreateSnapshotAt(dstDatadbDir)                 [partition.go:237]
  │     └── see below
  │
  └─ 6. Sync directory                                        [partition.go:239]
```

**DataDB snapshot** ([`ddb.mustCreateSnapshotAt()`](../lib/logstorage/datadb.go#L979)):

1. **Flush all in-memory parts** to disk first — [L983](../lib/logstorage/datadb.go#L983)
2. Collect all file-backed parts (small + big) with ref-count protection — [L986-993](../lib/logstorage/datadb.go#L986)
3. Write `parts.json` listing all part directories — [L996-997](../lib/logstorage/datadb.go#L996)
4. **Create hard links** for each part directory — [L1000-1004](../lib/logstorage/datadb.go#L1000)
5. Release reference counts — [L1007-1009](../lib/logstorage/datadb.go#L1007)
6. Sync the destination directory — [L1013](../lib/logstorage/datadb.go#L1013)

**Why hard links?** Snapshots use [`fs.MustHardLinkFiles()`](../lib/logstorage/datadb.go#L1003) instead of copying data. This makes snapshots:
- **Near-instant**: No data copying, just filesystem metadata operations
- **Space-efficient**: Hard-linked files share the same disk blocks
- **Safe with merging**: When [background merges](./onboarding-storage-engine.md#merge--compaction) replace parts, the snapshot's hard links still point to the original data (copy-on-write semantics at the filesystem level)

### Listing Snapshots

**Key Function**: [`Storage.PartitionSnapshotList()`](../lib/logstorage/storage.go#L335)

Scans the `snapshots/` subdirectory of each active partition, validates snapshot names against the expected format (`YYYYMMDDhhmmss-hex`), and returns sorted paths — via [`getSnapshotPaths()`](../lib/logstorage/storage.go#L345).

### Deleting Snapshots

**Key Function**: [`Storage.PartitionSnapshotDelete(snapshotPath)`](../lib/logstorage/storage.go#L371)

Validates the snapshot path structure:
1. Snapshot name must match the expected format — [L372-374](../lib/logstorage/storage.go#L372)
2. Must reside in a `snapshots/` directory — [L378-379](../lib/logstorage/storage.go#L378)
3. The parent partition must be among active partitions — [L386-396](../lib/logstorage/storage.go#L386)

Then delegates to [`partition.deleteSnapshot(snapshotName)`](../lib/logstorage/partition.go#L247), which acquires `snapshotLock` and removes the snapshot directory.

### Automatic Snapshot Cleanup

**Key Function**: [`Storage.watchSnapshotsMaxAge()`](../lib/logstorage/storage.go#L900)

A background goroutine that runs every ~1 minute (jittered) if `-snapshotsMaxAge` is set to a positive value:

```go
func (s *Storage) watchSnapshotsMaxAge() {
    if s.snapshotsMaxAge <= 0 {
        return  // no automatic cleanup
    }
    d := timeutil.AddJitterToDuration(time.Minute)
    ticker := time.NewTicker(d)
    // ...
    s.MustDeleteStalePartitionSnapshots(s.snapshotsMaxAge)
}
```

[`MustDeleteStalePartitionSnapshots(maxAge)`](../lib/logstorage/storage.go#L405) checks each snapshot's `ModTime()` against the current time and deletes those older than `maxAge` — [L414-428](../lib/logstorage/storage.go#L414).

The HTTP endpoint [`/internal/partition/snapshot/delete_stale`](../app/vlstorage/main.go#L474) also exposes this functionality, with an optional `max_age` query parameter that defaults to the `-snapshotsMaxAge` flag value — [L484-493](../app/vlstorage/main.go#L484).

---

## Backup & Restore

VictoriaLogs does **not** have a built-in restore API. Restore is performed using standard filesystem tools (`rsync`, `cp`) combined with the attach/detach APIs.

### Backup Procedure

```
1. Create snapshot:
   GET /internal/partition/snapshot/create?partition_prefix=20260101
   → Returns: ["/data/partitions/20260101/snapshots/20260101120000-0000001"]

2. Copy snapshot to backup location:
   rsync --delete /data/partitions/20260101/snapshots/20260101120000-0000001/ /backup/20260101/

3. Delete snapshot (free disk space):
   GET /internal/partition/snapshot/delete?path=/data/partitions/20260101/snapshots/20260101120000-0000001
```

**Why use snapshots instead of copying directly?** Without a snapshot, [background merges](./onboarding-storage-engine.md#merge--compaction) could modify or delete part files mid-copy, resulting in a corrupted backup. Snapshots provide a consistent, point-in-time view via hard links.

### Restore Procedure

**Option A: While VictoriaLogs is running** (live restore):

```
1. Detach the partition (blocks until all readers finish):
   GET /internal/partition/detach?name=20260101

2. Copy backup data to the partition directory:
   rsync --delete /backup/20260101/ /data/partitions/20260101/

3. Re-attach the partition:
   GET /internal/partition/attach?name=20260101
```

**Option B: While VictoriaLogs is stopped**:

```
1. Stop VictoriaLogs

2. Copy backup data:
   rsync --delete /backup/20260101/ /data/partitions/20260101/

3. Start VictoriaLogs (partition will be auto-loaded)
```

**Cache invalidation note**: At shutdown, VictoriaLogs intentionally does **not** persist its `streamIDCache` and `filterStreamCache`, precisely because partitions may be restored or modified between restarts — [`MustClose()` L1092-1094](../lib/logstorage/storage.go#L1092).

### Cross-Storage Migration

Partitions can be moved between storage tiers (e.g., NVMe → HDD) across separate VictoriaLogs instances:

```
1. Create snapshot on source (NVMe) instance

2. rsync the snapshot to destination (HDD) instance's partition directory:
   rsync /nvme/partitions/20260101/snapshots/<name>/ /hdd/partitions/20260101/

3. Attach on destination (HDD):
   GET http://hdd-instance/internal/partition/attach?name=20260101

4. Detach from source (NVMe):
   GET http://nvme-instance/internal/partition/detach?name=20260101

5. Remove the directory from source after detach completes
```

---

## Delete Tasks

Delete tasks allow deleting log entries matching a filter, processed as background tasks that survive application restarts.

API availability prerequisites:
- `/delete/*` endpoints are handled by `vlselect` and require `-delete.enable` — [`app/vlselect/main.go` L35, L94](../app/vlselect/main.go#L35)
- In cluster mode, `vlstorage` nodes must also enable `/internal/delete/*` via `-internaldelete.enable` — [`app/vlselect/main.go` L36, L113](../app/vlselect/main.go#L36)

### DeleteTask Struct

**Key File**: [`lib/logstorage/delete_task.go`](../lib/logstorage/delete_task.go#L14)

```go
type DeleteTask struct {
    TaskID    string      `json:"task_id"`     // unique task identifier     [L16]
    TenantIDs []TenantID  `json:"tenant_ids"`  // target tenants             [L19]
    Filter    string      `json:"filter"`      // LogsQL filter expression   [L22]
    StartTime time.Time   `json:"start_time"`  // task creation time         [L25]

    ctx    context.Context  // non-nil during execution (pending tasks have nil) [L28]
    cancel func()           // cancellation function                            [L31]
    doneCh chan struct{}     // closed when task completes                       [L34]
}
```

### Starting a Delete Task

**Key Function**: [`Storage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, filter)`](../lib/logstorage/storage.go#L437)

1. Create a new `DeleteTask` via [`newDeleteTask()`](../lib/logstorage/delete_task.go#L46) — [L439](../lib/logstorage/storage.go#L439)
2. Acquire `deleteTasksLock` — [L441](../lib/logstorage/storage.go#L441)
3. Verify uniqueness by `taskID` — [L445-448](../lib/logstorage/storage.go#L445)
4. Append to `s.deleteTasks` — [L452](../lib/logstorage/storage.go#L452)
5. Persist to `delete_tasks.json` via [`mustSaveDeleteTasksLocked()`](../lib/logstorage/storage.go#L461) — [L453](../lib/logstorage/storage.go#L453)

### Delete Task Processing

**Key Function**: [`Storage.watchDeleteTasks()`](../lib/logstorage/storage.go#L920)

A background goroutine that polls every ~1 second:

```
watchDeleteTasks()                          [storage.go:920]
  │
  ├─ Poll loop (every ~1 second)
  │
  ├─ 1. Pick s.deleteTasks[0] (FIFO queue)  [storage.go:936]
  ├─ 2. Set up dt.ctx, dt.cancel, dt.doneCh [storage.go:940-941]
  │
  ├─ 3. processDeleteTask(dt.ctx, dt)       [storage.go:952]
  │     │
  │     ├─ Parse filter string               [storage.go:983]
  │     ├─ Create Query with time bound      [storage.go:988-997]
  │     │   (only deletes rows before dt.StartTime)
  │     ├─ Initialize subqueries             [storage.go:1003]
  │     ├─ deleteRows(sso, stopCh)           [storage.go:1018]
  │     │   └─ For each partition in time range:
  │     │       ptw.pt.deleteRows(sso, stopCh) [storage.go:1046]
  │     │       └─ datadb.deleteRows()
  │     └─ Return true (success) or false (retry later)
  │
  ├─ 4. close(dt.doneCh)                    [storage.go:953]
  ├─ 5. Remove from list if ok=true          [storage.go:964]
  │     Or move to end if ok=false           [storage.go:967]
  └─ 6. Persist updated list                 [storage.go:969]
```

**Key design decisions**:
- Tasks are processed **sequentially** (one at a time) to limit resource usage — [L950](../lib/logstorage/storage.go#L950)
- Failed tasks are moved to the **end of the queue** for later retry — [L965-967](../lib/logstorage/storage.go#L965)
- The query is time-bounded to `dt.StartTime` to prevent deleting logs ingested after the delete request — [L993-997](../lib/logstorage/storage.go#L993)

### Stopping a Delete Task

**Key Function**: [`Storage.DeleteStopTask(ctx, taskID)`](../lib/logstorage/storage.go#L470)

Two cases:
- **Task is currently executing** (`dt.cancel != nil`): cancel via `dt.cancel()` and wait on `dt.doneCh` — [L480-483](../lib/logstorage/storage.go#L480)
- **Task is pending** (not yet started): remove directly from the list — [L484-487](../lib/logstorage/storage.go#L484)

**Listing active tasks**: [`Storage.DeleteActiveTasks(ctx)`](../lib/logstorage/storage.go#L508) returns a copy of the current task list.

### Persistence

Delete tasks are persisted to `delete_tasks.json` at the storage root directory:

| File | Constant | Location |
|------|----------|----------|
| `delete_tasks.json` | [`deleteTasksFilename`](../lib/logstorage/filenames.go#L21) | `<storageDataPath>/delete_tasks.json` |

- **Loaded on startup**: [`MustOpenStorage()` L655-656](../lib/logstorage/storage.go#L655) calls [`mustReadDeleteTasksFromFile()`](../lib/logstorage/delete_task.go#L73)
- **Saved atomically** after every modification via [`mustSaveDeleteTasksLocked()`](../lib/logstorage/storage.go#L461), which calls [`mustWriteDeleteTasksToFile()`](../lib/logstorage/delete_task.go#L88) using `fs.MustWriteAtomic()` — [L90](../lib/logstorage/delete_task.go#L90)
- **Format**: JSON array of `DeleteTask` objects — [`MarshalDeleteTasksToJSON()`](../lib/logstorage/delete_task.go#L56) / [`UnmarshalDeleteTasksFromJSON()`](../lib/logstorage/delete_task.go#L65)

---

## Automatic Retention

### Time-Based Retention

**Key Function**: [`Storage.watchRetention()`](../lib/logstorage/storage.go#L772)

Runs every ~1 hour (jittered). Deletes partitions older than the configured `-retentionPeriod`:

1. Calculate `minAllowedDay` from current time and retention — [L779](../lib/logstorage/storage.go#L779)
2. Since `s.partitions` is sorted by day (oldest first), iterate from the beginning — [L786](../lib/logstorage/storage.go#L786)
3. When the first non-expired partition is reached, all preceding partitions are scheduled for deletion — [L792-793](../lib/logstorage/storage.go#L792)
4. Add deleted days to `s.deletedPartitions` to prevent re-creation — [L794](../lib/logstorage/storage.go#L794)
5. Mark each with `ptw.mustDrop.Store(true)` and call `ptw.decRef()` — [L808-809](../lib/logstorage/storage.go#L808)
6. The partition is physically deleted when the last reference is released

Current implementation note: the deletable set is built from that prefix scan. If all attached partitions are expired in a given pass, no deletion prefix is selected in that iteration — [L786-802](../lib/logstorage/storage.go#L786).

**Startup cleanup**: [`MustOpenStorage()`](../lib/logstorage/storage.go#L618) also deletes future partitions beyond `-futureRetention` at [L719-737](../lib/logstorage/storage.go#L719).

### Disk Space Retention

**Key Function**: [`Storage.watchMaxDiskSpaceUsage()`](../lib/logstorage/storage.go#L821)

Runs every ~10 seconds (jittered). Drops the **oldest** partitions when disk usage exceeds either:
- `-retention.maxDiskSpaceUsageBytes` — absolute limit in bytes
- `-retention.maxDiskUsagePercent` — percentage of filesystem capacity

The watcher keeps at least the newest two per-day partitions attached — [L858-861](../lib/logstorage/storage.go#L858).

### Read-Only Mode

When free disk space drops below `-storage.minFreeDiskSpaceBytes` (default 10MB), the storage enters **read-only mode** and stops accepting new data. This is controlled by the `minFreeDiskSpaceBytes` field in [`StorageConfig`](../lib/logstorage/storage.go#L100).

---

## Force Merge & Force Flush

### Force Merge

**HTTP**: `GET /internal/force_merge?partition_prefix=...` (protected by `-forceMergeAuthKey`)

**Handler**: [`processForceMerge()`](../app/vlstorage/main.go#L293) — runs the merge **in a background goroutine** and returns `200 OK` immediately — [L305-312](../app/vlstorage/main.go#L305). Progress is logged: `"started/finished force merge for partition YYYYMMDD"`.

**Storage function**: [`Storage.MustForceMerge(partitionPrefix)`](../lib/logstorage/storage.go#L1113) iterates matching partitions and calls [`pt.mustForceMerge()`](../lib/logstorage/partition.go#L271) on each one **sequentially** (to limit system load) — [L1112](../lib/logstorage/storage.go#L1112).

**What it does** ([`ddb.mustForceMergeAllParts()`](../lib/logstorage/datadb.go#L1480)):

1. **Flush in-memory parts to disk** — [L1482](../lib/logstorage/datadb.go#L1482)
2. **Collect all small and big file parts** — [L1488-1489](../lib/logstorage/datadb.go#L1488)
3. **Merge everything** using the same selection algorithm as [background merge](./onboarding-storage-engine.md#merge--compaction), but with no size cap (`maxOutBytes = MaxUint64`) and no multiplier threshold — the loop continues until all parts are fully reduced — [L1497-1507](../lib/logstorage/datadb.go#L1497)
4. **Even a single part is merged** — this applies any pending [delete task](#delete-task-processing) drop filter, physically removing deleted rows from the output part — [L1492-1493](../lib/logstorage/datadb.go#L1492)

**When to use it**:

- **After a delete task completes**: deleted rows are filtered out during merge; without a subsequent force merge they may remain in un-merged parts and continue consuming disk space (they are not returned in queries either way, but they do occupy space).
- **To reduce read amplification**: if a partition has accumulated many small parts (because background merge's multiplier threshold was never met), force merge compacts them unconditionally into as few parts as possible, improving query scan performance on that partition.

### Force Flush

**HTTP**: `GET /internal/force_flush` (protected by `-forceFlushAuthKey`)

**Handler**: [`processForceFlush()`](../app/vlstorage/main.go#L316) — calls `localStorage.DebugFlush()` to make all pending in-memory data immediately searchable.

---

## Storage Shutdown

**Key Function**: [`Storage.MustClose()`](../lib/logstorage/storage.go#L1075)

1. **Stop background workers**: close `s.stopCh`, wait for all goroutines via `s.wg.Wait()` — [L1077-1078](../lib/logstorage/storage.go#L1077)
2. **Close all partitions**: call `decRef()` on each, verify refCount reaches 0 — [L1081-1086](../lib/logstorage/storage.go#L1081)
3. **Stop caches** (intentionally **not** persisted — see [L1092-1094](../lib/logstorage/storage.go#L1092)):

   ```go
   // Do not persist caches, since they may become out of sync with partitions
   // if partitions are deleted, restored from backups or copied from other sources
   // between VictoriaLogs restarts.
   ```

4. **Release file lock** — [L1104](../lib/logstorage/storage.go#L1104)

---

## Configuration Flags

**Key File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L27)

### Storage & Retention

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-storageDataPath` | `victoria-logs-data` | [46](../app/vlstorage/main.go#L46) | Root storage directory |
| `-retentionPeriod` | `7d` | [28](../app/vlstorage/main.go#L28) | Auto-delete partitions older than this |
| `-futureRetention` | `2d` | [39](../app/vlstorage/main.go#L39) | Reject logs with timestamps beyond now+futureRetention |
| `-maxBackfillAge` | `0` (= retention) | [41](../app/vlstorage/main.go#L41) | Reject logs older than now-maxBackfillAge |
| `-retention.maxDiskSpaceUsageBytes` | `0` (no limit) | [36](../app/vlstorage/main.go#L36) | Drop oldest partitions when total disk exceeds this |
| `-retention.maxDiskUsagePercent` | `0` (no limit) | [38](../app/vlstorage/main.go#L38) | Drop oldest partitions when disk usage % exceeds this |
| `-storage.minFreeDiskSpaceBytes` | `10MB` | [56](../app/vlstorage/main.go#L56) | Enter read-only mode below this free space |
| `-inmemoryDataFlushInterval` | `5s` | [48](../app/vlstorage/main.go#L48) | Interval for flushing in-memory data to disk |

### Snapshots

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-snapshotsMaxAge` | `3d` | [43](../app/vlstorage/main.go#L43) | Auto-delete snapshots older than this |

### Auth Keys

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-partitionManageAuthKey` | (empty) | [66](../app/vlstorage/main.go#L66) | Auth key for `/internal/partition/*` endpoints |
| `-forceMergeAuthKey` | (empty) | [61](../app/vlstorage/main.go#L61) | Auth key for `/internal/force_merge` |
| `-forceFlushAuthKey` | (empty) | [63](../app/vlstorage/main.go#L63) | Auth key for `/internal/force_flush` |

### Debugging

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-logNewStreams` | `false` | [52](../app/vlstorage/main.go#L52) | Log creation of new streams |
| `-logIngestedRows` | `false` | [54](../app/vlstorage/main.go#L54) | Log all ingested log entries |

---

## Key Design Patterns

### 1. Reference-Counted Partition Lifecycle

Every partition access (queries, ingestion, snapshots) goes through `incRef()`/`decRef()`. This allows safe concurrent access while enabling clean shutdown, detach, and retention deletion. A partition is only physically closed/deleted when the last reference is released. The `doneCh` channel provides a blocking wait mechanism for operations that need to ensure quiescence (like detach).

### 2. Hard-Link Snapshots

Snapshots use filesystem hard links rather than data copies. This makes them near-instant and space-efficient. Since parts are immutable once written (only replaced via merge), hard links remain valid even as [background merging](./onboarding-storage-engine.md#merge--compaction) creates new parts. In-memory data is flushed to disk before taking a snapshot to ensure completeness.

### 3. Persisted Delete Tasks

Delete tasks survive application restarts by persisting to `delete_tasks.json`. They are processed sequentially to limit resource usage, with failed tasks moved to the end of the queue for retry. Each task records a `StartTime` to ensure only logs that existed at the time of the delete request are affected.

### 4. Sorted Partition List with Binary Search

The `s.partitions` slice is always sorted by day (oldest first). This enables:
- O(log n) lookup during data ingestion (find the right partition) — [`getPartitionForWriting()`](../lib/logstorage/storage.go#L1250)
- Efficient prefix deletion of expired oldest partitions (O(k), where k is the number of leading expired partitions)
- Efficient time-range queries (binary search for overlapping partitions)

### 5. deletedPartitions Guard

When a partition is deleted by retention, its day is added to `deletedPartitions`. This prevents the partition from being accidentally re-created if late-arriving data references that day — [`getPartitionForWriting()` L1262](../lib/logstorage/storage.go#L1262). It also prevents reattach of retention-deleted partitions — [`PartitionAttach()` L226](../lib/logstorage/storage.go#L226).

### 6. Jittered Background Workers

All background watchers use `timeutil.AddJitterToDuration()` to add randomness to their polling intervals. This prevents all workers from waking up simultaneously and causing load spikes.

---

## See Also

- [VictoriaLogs Storage Engine & On-Disk Format](./onboarding-storage-engine.md)
- [VictoriaLogs Data Ingestion Flow](./onboarding-insert-flow.md)
- [VictoriaLogs Query/Select Flow](./onboarding-select-flow.md)
- [VictoriaLogs Cluster Architecture](./onboarding-cluster.md)
- [Glossary](./glossary.md)

---

## Document Maintenance

This document references specific source code line numbers. When modifying the referenced files, please verify that:
1. Struct definitions at referenced lines haven't moved significantly
2. Key function signatures match what's documented
3. HTTP endpoint routing matches the documented paths

Use the format `[description](../relative/path/to/file.go#LNNN)` for all source references so they work as clickable links in VS Code and GitHub.
