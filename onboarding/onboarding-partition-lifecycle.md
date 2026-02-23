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

**Key File**: [`lib/logstorage/partition.go`](../lib/logstorage/partition.go#L707)

```go
const partitionNameFormat = "20060102"  // Go time format for YYYYMMDD
```

Helper functions:
- [`getPartitionDayFromName(name)`](../lib/logstorage/partition.go#L685) — parses "20260101" to a unix day number
- [`getPartitionNameFromDay(day)`](../lib/logstorage/partition.go#L699) — formats a unix day number to "20260101"

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

**Key File**: [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L591)

Every partition is accessed through a `partitionWrapper` that provides safe concurrent access via [reference counting](./glossary.md#reference-counting):

```go
type partitionWrapper struct {
    refCount atomic.Int32   // number of active references              [L594]
    mustDrop atomic.Bool    // if true, delete partition at refCount=0  [L598]
    day      int64          // unix day number                          [L602]
    pt       *partition     // the wrapped partition                    [L605]
    doneCh   chan struct{}  // closed when refCount reaches zero        [L609]
}
```

The lifecycle of a `partitionWrapper`:

1. [`newPartitionWrapper(pt, day)`](../lib/logstorage/storage.go#L612) creates a wrapper with initial `refCount=1` — [L618](../lib/logstorage/storage.go#L618)
2. Callers use [`incRef()`](../lib/logstorage/storage.go#L622) before accessing the partition and [`decRef()`](../lib/logstorage/storage.go#L626) when done
3. When `refCount` reaches 0 ([L626-659](../lib/logstorage/storage.go#L626)):
   - The partition is closed via `mustClosePartition()` — [L648](../lib/logstorage/storage.go#L648)
   - If `mustDrop` is set, the partition directory is deleted — [L654](../lib/logstorage/storage.go#L654)
   - `doneCh` is closed to unblock anyone waiting on the partition — [L659](../lib/logstorage/storage.go#L659)

This pattern allows safe detach and retention deletion: set `mustDrop`, call `decRef()`, and the partition is automatically cleaned up when the last reader finishes.

### Partition Creation

Partitions are created automatically during data ingestion by [`Storage.getPartitionForWriting(day)`](../lib/logstorage/storage.go#L1409):

1. **Binary search** in the sorted `s.partitions` list — [L1413-1417](../lib/logstorage/storage.go#L1413)
2. If not found, check `deletedPartitions` to avoid re-creating dropped partitions — [L1429](../lib/logstorage/storage.go#L1429)
3. If the directory already exists on disk (e.g., manually placed) but isn't attached, return nil — the user must call `PartitionAttach()` — [L1436](../lib/logstorage/storage.go#L1436)
4. Otherwise, create a new partition via [`mustCreatePartition(path)`](../lib/logstorage/partition.go#L174) — which creates the directory with `indexdb/` and `datadb/` subdirectories

---

## HTTP API Endpoints

**Key File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L380)

All partition management endpoints are routed by [`RequestHandler()`](../app/vlstorage/main.go#L380) and are only available when running in **local storage mode** (not cluster mode with `-storageNode`). Each handler checks the `-partitionManageAuthKey` flag.

| Endpoint | Handler | Line | Query Params | Response |
|----------|---------|------|-------------|----------|
| `/internal/partition/attach` | [`processPartitionAttach`](../app/vlstorage/main.go#L476) | 476 | `name=YYYYMMDD` | 200 OK or error |
| `/internal/partition/detach` | [`processPartitionDetach`](../app/vlstorage/main.go#L495) | 495 | `name=YYYYMMDD` | 200 OK or error |
| `/internal/partition/list` | [`processPartitionList`](../app/vlstorage/main.go#L514) | 514 | (none) | JSON `["20260101","20260102"]` |
| `/internal/partition/snapshot/create` | [`processPartitionSnapshotCreate`](../app/vlstorage/main.go#L534) | 534 | `partition_prefix=...` | JSON `["/path/to/snap"]` |
| `/internal/partition/snapshot/list` | [`processPartitionSnapshotList`](../app/vlstorage/main.go#L581) | 581 | (none) | JSON `["/path/to/snap"]` |
| `/internal/partition/snapshot/delete` | [`processPartitionSnapshotDelete`](../app/vlstorage/main.go#L601) | 601 | `path=/path/to/snap` | 204 No Content |
| `/internal/partition/snapshot/delete_stale` | [`processPartitionSnapshotDeleteStale`](../app/vlstorage/main.go#L626) | 626 | `max_age=3d` (optional) | JSON `["/deleted/paths"]` |
| `/internal/force_merge` | [`processForceMerge`](../app/vlstorage/main.go#L430) | 430 | `partition_prefix=...` | 200 OK (runs in background) |
| `/internal/force_flush` | [`processForceFlush`](../app/vlstorage/main.go#L460) | 460 | (none) | 200 OK |

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
processPartitionAttach()                    [vlstorage/main.go:476]
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
  ├─ 6. Open partition (indexdb + datadb)    [storage.go:245]
  ├─ 7. Wrap in partitionWrapper             [storage.go:246]
  ├─ 8. Append to s.partitions and re-sort   [storage.go:248-250]
  └─ 9. Log success                          [storage.go:252]
```

**Error cases**:
- Partition day is in `deletedPartitions` (was already dropped by retention) — [L226-228](../lib/logstorage/storage.go#L226)
- Partition already attached — [L231-234](../lib/logstorage/storage.go#L231)
- Partition directory doesn't exist on disk — [L240-241](../lib/logstorage/storage.go#L240)

### Detach

**Key Function**: [`Storage.PartitionDetach(name)`](../lib/logstorage/storage.go#L267)

Safely detaches a partition at runtime. The function **blocks** until all concurrent readers and writers finish their work on the partition.

**Flow**:

```
HTTP: GET /internal/partition/detach?name=20260101
  │
  ▼
processPartitionDetach()                    [vlstorage/main.go:495]
  │
  ▼
Storage.PartitionDetach(name)               [storage.go:267]
  │
  ├─ 1. Lock partitionsLock                  [storage.go:271]
  ├─ 2. Find partition by name               [storage.go:274-275]
  ├─ 3. Remove from s.partitions slice       [storage.go:281]
  ├─ 4. Clear s.ptwHot if it was the hot partition [storage.go:282-285]
  ├─ 5. Unlock partitionsLock                [storage.go:272 (defer)]
  ├─ 6. Decrement reference count            [storage.go:300]
  ├─ 7. BLOCK on <-ptw.doneCh               [storage.go:306]
  │     (waits for all readers/writers to finish)
  └─ 8. Log success                          [storage.go:308]
```

**Important**: The detach call blocks on `<-ptw.doneCh` at [L306](../lib/logstorage/storage.go#L306). This means the HTTP request will not return until every concurrent query or ingestion operation on that partition has completed. This guarantees the partition is fully quiesced and safe to move/delete/restore.

**Restart behavior**: Detach affects only the currently running process. If the detached partition directory remains under `<storageDataPath>/partitions/`, it is attached again on restart when `MustOpenStorage()` scans and opens partition directories — [L760-792](../lib/logstorage/storage.go#L760).

### List

**Key Function**: [`Storage.PartitionList()`](../lib/logstorage/storage.go#L316)

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

The HTTP handler at [`processPartitionList()`](../app/vlstorage/main.go#L514) ensures an empty list returns `[]` not `null` — [L525-527](../app/vlstorage/main.go#L525).

---

## Snapshots

### Creating Snapshots

**Storage-level function**: [`Storage.PartitionSnapshotMustCreate(partitionPrefix)`](../lib/logstorage/storage.go#L336)

The `partitionPrefix` parameter supports flexible matching:

| Prefix | Matches | Example |
|--------|---------|---------|
| `YYYYMMDD` | Single day | `20260101` matches only that partition |
| `YYYYMM` | Entire month | `202601` matches all January 2026 partitions |
| `YYYY` | Entire year | `2026` matches all 2026 partitions |
| `""` (empty) | All partitions | Creates snapshots for everything |

The function iterates all partitions and creates a snapshot for each one matching the prefix — [L342-347](../lib/logstorage/storage.go#L342).

**Client disconnect handling**: The HTTP handler [`processPartitionSnapshotCreate()`](../app/vlstorage/main.go#L534) checks `r.Context().Err()` after creation. If the client disconnected, all created snapshots are automatically deleted — [L566-574](../app/vlstorage/main.go#L566). This prevents orphaned snapshots.

### How Snapshots Work Internally

**Partition-level function**: [`partition.mustCreateSnapshot()`](../lib/logstorage/partition.go#L560)

```
mustCreateSnapshot()                        [partition.go:560]
  │
  ├─ 1. Acquire snapshotLock (prevents concurrent snapshots) [partition.go:564]
  ├─ 2. Generate unique name via snapshotutil.NewName()       [partition.go:568]
  │     Format: YYYYMMDDhhmmss-XXXXXXXX (timestamp + hex counter)
  ├─ 3. Create snapshot directory:
  │     <partition_path>/snapshots/<snapshot_name>/            [partition.go:569-570]
  │
  ├─ 4. Snapshot indexdb:
  │     idb.mustCreateSnapshotAt(dstIndexdbDir)                [partition.go:574]
  │     └── calls mergeset.Table.MustCreateSnapshotAt()
  │
  ├─ 5. Snapshot datadb:
  │     ddb.mustCreateSnapshotAt(dstDatadbDir)                 [partition.go:577]
  │     └── see below
  │
  └─ 6. Sync directory                                        [partition.go:580]
```

**DataDB snapshot** ([`ddb.mustCreateSnapshotAt()`](../lib/logstorage/datadb.go#L1155)):

1. **Flush all in-memory parts** to disk first — [L1159](../lib/logstorage/datadb.go#L1159)
2. Collect all file-backed parts (small + big) with ref-count protection — [L1162-1169](../lib/logstorage/datadb.go#L1162)
3. Write `parts.json` listing all part directories — [L1171-1173](../lib/logstorage/datadb.go#L1171)
4. **Create hard links** for each part directory — [L1176-1179](../lib/logstorage/datadb.go#L1176)
5. Release reference counts — [L1182-1184](../lib/logstorage/datadb.go#L1182)
6. Sync the destination directory — [L1189](../lib/logstorage/datadb.go#L1189)

**Why hard links?** Snapshots use [`fs.MustHardLinkFiles()`](../lib/logstorage/datadb.go#L1179) instead of copying data. This makes snapshots:
- **Near-instant**: No data copying, just filesystem metadata operations
- **Space-efficient**: Hard-linked files share the same disk blocks
- **Safe with merging**: When [background merges](./onboarding-storage-engine.md#merge--compaction) replace parts, the snapshot's hard links still point to the original data (copy-on-write semantics at the filesystem level)

### Listing Snapshots

**Key Function**: [`Storage.PartitionSnapshotList()`](../lib/logstorage/storage.go#L353)

Scans the `snapshots/` subdirectory of each active partition, validates snapshot names against the expected format (`YYYYMMDDhhmmss-hex`), and returns sorted paths — via [`getSnapshotPaths()`](../lib/logstorage/storage.go#L363).

### Deleting Snapshots

**Key Function**: [`Storage.PartitionSnapshotDelete(snapshotPath)`](../lib/logstorage/storage.go#L389)

Validates the snapshot path structure:
1. Snapshot name must match the expected format — [L391-393](../lib/logstorage/storage.go#L391)
2. Must reside in a `snapshots/` directory — [L396-397](../lib/logstorage/storage.go#L396)
3. The parent partition must be among active partitions — [L404-414](../lib/logstorage/storage.go#L404)

Then delegates to [`partition.deleteSnapshot(snapshotName)`](../lib/logstorage/partition.go#L596), which acquires `snapshotLock` and removes the snapshot directory.

### Automatic Snapshot Cleanup

**Key Function**: [`Storage.watchSnapshotsMaxAge()`](../lib/logstorage/storage.go#L995)

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

[`MustDeleteStalePartitionSnapshots(maxAge)`](../lib/logstorage/storage.go#L423) checks each snapshot's `ModTime()` against the current time and deletes those older than `maxAge` — [L439-444](../lib/logstorage/storage.go#L439).

The HTTP endpoint [`/internal/partition/snapshot/delete_stale`](../app/vlstorage/main.go#L626) also exposes this functionality, with an optional `max_age` query parameter that defaults to the `-snapshotsMaxAge` flag value — [L636-645](../app/vlstorage/main.go#L636).

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

**Cache invalidation note**: At shutdown, VictoriaLogs intentionally does **not** persist its `streamIDCache` and `filterStreamCache`, precisely because partitions may be restored or modified between restarts — [`MustClose()` L1187-1189](../lib/logstorage/storage.go#L1187).

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

**Key File**: [`lib/logstorage/delete_task.go`](../lib/logstorage/delete_task.go#L72)

```go
type DeleteTask struct {
    TaskID    string      `json:"task_id"`     // unique task identifier     [L74]
    TenantIDs []TenantID  `json:"tenant_ids"`  // target tenants             [L77]
    Filter    string      `json:"filter"`      // LogsQL filter expression   [L80]
    StartTime time.Time   `json:"start_time"`  // task creation time         [L83]

    ctx    context.Context  // non-nil during execution (pending tasks have nil) [L87]
    cancel func()           // cancellation function                            [L91]
    doneCh chan struct{}     // closed when task completes                       [L96]
}
```

### Starting a Delete Task

**Key Function**: [`Storage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, filter)`](../lib/logstorage/storage.go#L455)

1. Create a new `DeleteTask` via [`newDeleteTask()`](../lib/logstorage/delete_task.go#L113) — [L457](../lib/logstorage/storage.go#L457)
2. Acquire `deleteTasksLock` — [L459](../lib/logstorage/storage.go#L459)
3. Verify uniqueness by `taskID` — [L463-467](../lib/logstorage/storage.go#L463)
4. Append to `s.deleteTasks` — [L470](../lib/logstorage/storage.go#L470)
5. Persist to `delete_tasks.json` via [`mustSaveDeleteTasksLocked()`](../lib/logstorage/storage.go#L480) — [L472](../lib/logstorage/storage.go#L472)

### Delete Task Processing

**Key Function**: [`Storage.watchDeleteTasks()`](../lib/logstorage/storage.go#L1015)

A background goroutine that polls every ~1 second:

```
watchDeleteTasks()                          [storage.go:1015]
  │
  ├─ Poll loop (every ~1 second)
  │
  ├─ 1. Pick s.deleteTasks[0] (FIFO queue)  [storage.go:1031]
  ├─ 2. Set up dt.ctx, dt.cancel, dt.doneCh [storage.go:1035-1036]
  │
  ├─ 3. processDeleteTask(dt.ctx, dt)       [storage.go:1047]
  │     │
  │     ├─ Parse filter string               [storage.go:1078]
  │     ├─ Create Query with time bound      [storage.go:1083-1092]
  │     │   (only deletes rows before dt.StartTime)
  │     ├─ Initialize subqueries             [storage.go:1098]
  │     ├─ deleteRows(sso, stopCh)           [storage.go:1113]
  │     │   └─ For each partition in time range:
  │     │       ptw.pt.deleteRows(sso, stopCh) [storage.go:1141]
  │     │       └─ datadb.deleteRows()
  │     └─ Return true (success) or false (retry later)
  │
  ├─ 4. close(dt.doneCh)                    [storage.go:1048]
  ├─ 5. Remove from list if ok=true          [storage.go:1059]
  │     Or move to end if ok=false           [storage.go:1061-1062]
  └─ 6. Persist updated list                 [storage.go:1064]
```

**Key design decisions**:
- Tasks are processed **sequentially** (one at a time) to limit resource usage — [L1045](../lib/logstorage/storage.go#L1045)
- Failed tasks are moved to the **end of the queue** for later retry — [L1061-1062](../lib/logstorage/storage.go#L1061)
- The query is time-bounded to `dt.StartTime` to prevent deleting logs ingested after the delete request — [L1088-1092](../lib/logstorage/storage.go#L1088)

### Stopping a Delete Task

**Key Function**: [`Storage.DeleteStopTask(ctx, taskID)`](../lib/logstorage/storage.go#L489)

Two cases:
- **Task is currently executing** (`dt.cancel != nil`): cancel via `dt.cancel()` and wait on `dt.doneCh` — [L499-503](../lib/logstorage/storage.go#L499)
- **Task is pending** (not yet started): remove directly from the list — [L503-507](../lib/logstorage/storage.go#L503)

**Listing active tasks**: [`Storage.DeleteActiveTasks(ctx)`](../lib/logstorage/storage.go#L528) returns a copy of the current task list.

### Persistence

Delete tasks are persisted to `delete_tasks.json` at the storage root directory:

| File | Constant | Location |
|------|----------|----------|
| `delete_tasks.json` | [`deleteTasksFilename`](../lib/logstorage/filenames.go#L69) | `<storageDataPath>/delete_tasks.json` |

- **Loaded on startup**: [`MustOpenStorage()` L730-732](../lib/logstorage/storage.go#L730) calls [`mustReadDeleteTasksFromFile()`](../lib/logstorage/delete_task.go#L145)
- **Saved atomically** after every modification via [`mustSaveDeleteTasksLocked()`](../lib/logstorage/storage.go#L480), which calls [`mustWriteDeleteTasksToFile()`](../lib/logstorage/delete_task.go#L163) using `fs.MustWriteAtomic()` — [L165](../lib/logstorage/delete_task.go#L165)
- **Format**: JSON array of `DeleteTask` objects — [`MarshalDeleteTasksToJSON()`](../lib/logstorage/delete_task.go#L124) / [`UnmarshalDeleteTasksFromJSON()`](../lib/logstorage/delete_task.go#L134)

---

## Automatic Retention

### Time-Based Retention

**Key Function**: [`Storage.watchRetention()`](../lib/logstorage/storage.go#L850)

Runs every ~1 hour (jittered). Deletes partitions older than the configured `-retentionPeriod`:

1. Calculate `minAllowedDay` from current time and retention — [L859](../lib/logstorage/storage.go#L859)
2. Since `s.partitions` is sorted by day (oldest first), iterate from the beginning — [L868](../lib/logstorage/storage.go#L868)
3. When the first non-expired partition is reached, all preceding partitions are scheduled for deletion — [L874-877](../lib/logstorage/storage.go#L874)
4. Add deleted days to `s.deletedPartitions` to prevent re-creation — [L882](../lib/logstorage/storage.go#L882)
5. Mark each with `ptw.mustDrop.Store(true)` and call `ptw.decRef()` — [L901-903](../lib/logstorage/storage.go#L901)
6. The partition is physically deleted when the last reference is released

Current implementation note: the deletable set is built from that prefix scan. If all attached partitions are expired in a given pass, no deletion prefix is selected in that iteration — [L868-890](../lib/logstorage/storage.go#L868).

**Startup cleanup**: [`MustOpenStorage()`](../lib/logstorage/storage.go#L691) also deletes future partitions beyond `-futureRetention` at [L796-809](../lib/logstorage/storage.go#L796).

### Disk Space Retention

**Key Function**: [`Storage.watchMaxDiskSpaceUsage()`](../lib/logstorage/storage.go#L915)

Runs every ~10 seconds (jittered). Drops the **oldest** partitions when disk usage exceeds either:
- `-retention.maxDiskSpaceUsageBytes` — absolute limit in bytes
- `-retention.maxDiskUsagePercent` — percentage of filesystem capacity

The watcher keeps at least the newest two per-day partitions attached — [L953-955](../lib/logstorage/storage.go#L953).

### Read-Only Mode

When free disk space drops below `-storage.minFreeDiskSpaceBytes` (default 10MB), the storage enters **read-only mode** and stops accepting new data. This is controlled by the `minFreeDiskSpaceBytes` field in [`StorageConfig`](../lib/logstorage/storage.go#L100).

---

## Force Merge & Force Flush

### Force Merge

**HTTP**: `GET /internal/force_merge?partition_prefix=...` (protected by `-forceMergeAuthKey`)

**Handler**: [`processForceMerge()`](../app/vlstorage/main.go#L430) — runs the merge **in a background goroutine** and returns `200 OK` immediately — [L443-457](../app/vlstorage/main.go#L443). Progress is logged: `"started/finished force merge for partition YYYYMMDD"`.

**Storage function**: [`Storage.MustForceMerge(partitionPrefix)`](../lib/logstorage/storage.go#L1208) iterates matching partitions and calls [`pt.mustForceMerge()`](../lib/logstorage/partition.go#L647) on each one **sequentially** (to limit system load) — [L1215](../lib/logstorage/storage.go#L1215).

**What it does** ([`ddb.mustForceMergeAllParts()`](../lib/logstorage/datadb.go#L1682)):

1. **Flush in-memory parts to disk** — [L1684](../lib/logstorage/datadb.go#L1684)
2. **Collect all small and big file parts** — [L1690-1691](../lib/logstorage/datadb.go#L1690)
3. **Merge everything** using the same selection algorithm as [background merge](./onboarding-storage-engine.md#merge--compaction), but with no size cap (`maxOutBytes = MaxUint64`) and no multiplier threshold — the loop continues until all parts are fully reduced — [L1699-1709](../lib/logstorage/datadb.go#L1699)
4. **Even a single part is merged** — this applies any pending [delete task](#delete-task-processing) drop filter, physically removing deleted rows from the output part — [L1694-1695](../lib/logstorage/datadb.go#L1694)

**When to use it**:

- **After a delete task completes**: deleted rows are filtered out during merge; without a subsequent force merge they may remain in un-merged parts and continue consuming disk space (they are not returned in queries either way, but they do occupy space).
- **To reduce read amplification**: if a partition has accumulated many small parts (because background merge's multiplier threshold was never met), force merge compacts them unconditionally into as few parts as possible, improving query scan performance on that partition.

### Force Flush

**HTTP**: `GET /internal/force_flush` (protected by `-forceFlushAuthKey`)

**Handler**: [`processForceFlush()`](../app/vlstorage/main.go#L460) — calls `localStorage.DebugFlush()` to make all pending in-memory data immediately searchable.

---

## Storage Shutdown

**Key Function**: [`Storage.MustClose()`](../lib/logstorage/storage.go#L1170)

1. **Stop background workers**: close `s.stopCh`, wait for all goroutines via `s.wg.Wait()` — [L1172-1173](../lib/logstorage/storage.go#L1172)
2. **Close all partitions**: call `decRef()` on each, verify refCount reaches 0 — [L1176-1180](../lib/logstorage/storage.go#L1176)
3. **Stop caches** (intentionally **not** persisted — see [L1187-1189](../lib/logstorage/storage.go#L1187)):

   ```go
   // Do not persist caches, since they may become out of sync with partitions
   // if partitions are deleted, restored from backups or copied from other sources
   // between VictoriaLogs restarts.
   ```

4. **Release file lock** — [L1199](../lib/logstorage/storage.go#L1199)

---

## Configuration Flags

**Key File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L73)

### Storage & Retention

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-storageDataPath` | `victoria-logs-data` | [92](../app/vlstorage/main.go#L92) | Root storage directory |
| `-retentionPeriod` | `7d` | [74](../app/vlstorage/main.go#L74) | Auto-delete partitions older than this |
| `-futureRetention` | `2d` | [85](../app/vlstorage/main.go#L85) | Reject logs with timestamps beyond now+futureRetention |
| `-maxBackfillAge` | `0` (= retention) | [87](../app/vlstorage/main.go#L87) | Reject logs older than now-maxBackfillAge |
| `-retention.maxDiskSpaceUsageBytes` | `0` (no limit) | [82](../app/vlstorage/main.go#L82) | Drop oldest partitions when total disk exceeds this |
| `-retention.maxDiskUsagePercent` | `0` (no limit) | [84](../app/vlstorage/main.go#L84) | Drop oldest partitions when disk usage % exceeds this |
| `-storage.minFreeDiskSpaceBytes` | `10MB` | [102](../app/vlstorage/main.go#L102) | Enter read-only mode below this free space |
| `-inmemoryDataFlushInterval` | `5s` | [94](../app/vlstorage/main.go#L94) | Interval for flushing in-memory data to disk |

### Snapshots

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-snapshotsMaxAge` | `3d` | [89](../app/vlstorage/main.go#L89) | Auto-delete snapshots older than this |

### Auth Keys

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-partitionManageAuthKey` | (empty) | [112](../app/vlstorage/main.go#L112) | Auth key for `/internal/partition/*` endpoints |
| `-forceMergeAuthKey` | (empty) | [107](../app/vlstorage/main.go#L107) | Auth key for `/internal/force_merge` |
| `-forceFlushAuthKey` | (empty) | [109](../app/vlstorage/main.go#L109) | Auth key for `/internal/force_flush` |

### Debugging

| Flag | Default | Line | Description |
|------|---------|------|-------------|
| `-logNewStreams` | `false` | [98](../app/vlstorage/main.go#L98) | Log creation of new streams |
| `-logIngestedRows` | `false` | [100](../app/vlstorage/main.go#L100) | Log all ingested log entries |

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
- O(log n) lookup during data ingestion (find the right partition) — [`getPartitionForWriting()`](../lib/logstorage/storage.go#L1413)
- Efficient prefix deletion of expired oldest partitions (O(k), where k is the number of leading expired partitions)
- Efficient time-range queries (binary search for overlapping partitions)

### 5. deletedPartitions Guard

When a partition is deleted by retention, its day is added to `deletedPartitions`. This prevents the partition from being accidentally re-created if late-arriving data references that day — [`getPartitionForWriting()` L1429](../lib/logstorage/storage.go#L1429). It also prevents reattach of retention-deleted partitions — [`PartitionAttach()` L226](../lib/logstorage/storage.go#L226).

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
