# Level 10 Snapshot Consistency

## Context
- Level: 10 - Lifecycle Ops, Retention/Delete Semantics, and Capstone
- Date: 2026-03-02

## Why Snapshot + Hard-Links Remain Consistent During Merges

### The Problem

When creating a snapshot, we need a consistent point-in-time view of the data while:
- Ingestion continues (new rows arrive)
- Background merges run (parts are combined)
- Queries execute (data is read)

How can we capture a consistent snapshot without pausing the entire system?

### The Solution: Hard Links + Reference Counting

**Hard links** create additional directory entries pointing to the same physical disk blocks (inodes). **Reference counting** tracks when files can be safely deleted.

### Step-by-Step: Snapshot Creation

```
mustCreateSnapshot()
│
├── 1. Acquire snapshotLock
│     (Serializes concurrent snapshots for this partition)
│
├── 2. Force-flush in-memory parts to disk
│     isFinal=true: every in-memory part becomes a file part
│     Now all data is in file-backed parts
│
├── 3. Snapshot indexdb
│     Hard-link all mergeset table files
│
├── 4. Snapshot datadb
│     Under partsLock:
│       Collect all small + big parts
│       incRef() on each part (prevent deletion)
│     Release partsLock
│
│     Write parts.json for snapshot
│
│     For each part:
│       fs.MustHardLinkFiles(live/part/, snapshot/part/)
│       Creates hard links for every file
│
│     decRef() on each part (release references)
│
├── 5. fsync snapshot directory
│
└── 6. Release snapshotLock
```

### Why It Remains Consistent

#### Scenario 1: Merge Replaces a Part During Snapshot

```
Timeline:

T0: Snapshot starts
    Live:     datadb/part_ABC/timestamps.bin → inode #42
    Snapshot: (not yet created)
    
T1: Snapshot collects parts under partsLock
    partsLock acquired
    part_ABC.incRef() → refCount = 1
    partsLock released
    
T2: Merge completes, replaces part_ABC with part_DEF
    Under partsLock:
      Remove part_ABC from parts list
      part_ABC.mustDrop = true
      part_ABC.decRef() → refCount: 1 → 0? NO!
      
    part_ABC still has refCount = 1 (from snapshot's incRef)
    part_ABC is NOT deleted yet!
    
T3: Snapshot hard-links part_ABC
    fs.MustHardLinkFiles(part_ABC, snapshot/part_ABC)
    
    Live:     datadb/part_ABC/timestamps.bin → inode #42 (refCount: 1)
    Snapshot: snapshot/part_ABC/timestamps.bin → inode #42 (refCount: 2)
    
T4: Snapshot releases reference
    part_ABC.decRef() → refCount: 2 → 1
    
    Live still holds reference, part_ABC still alive
    
T5: Merge continues, live eventually releases
    Live parts are replaced
    Old live parts' refCount → 0 → deleted
    
    Snapshot's hard links keep inode #42 alive
```

**Key insight:** The `incRef()` at T1 prevents part_ABC from being deleted until the snapshot finishes hard-linking. Even if the merge completes and removes part_ABC from the live parts list, the file remains on disk because refCount > 0.

#### Scenario 2: Query Reads During Snapshot

```
T0: Query starts
    Query acquires references on parts it will scan
    part_ABC.incRef() → refCount: 1 → 2 (snapshot also holds ref)
    
T1: Snapshot creates hard links
    Both query and snapshot hold references
    
T2: Query completes
    part_ABC.decRef() → refCount: 2 → 1
    
T3: Snapshot completes
    part_ABC.decRef() → refCount: 1 → 0? NO!
    
    If merge replaced part_ABC:
      Live holds no reference
      Snapshot holds reference via hard link
      refCount: 1 (snapshot's hard link)
```

**Key insight:** Multiple goroutines can hold references simultaneously. The part is deleted only when ALL references are released AND the part is marked for deletion.

### Unix Hard Link Semantics

```
Filesystem state:

Before snapshot:
  datadb/part_ABC/timestamps.bin → inode #42
  Inode #42:
    link_count = 1
    ref_count = 0 (no open file handles)
    blocks = [block1, block2, block3, ...]

After hard link:
  datadb/part_ABC/timestamps.bin → inode #42
  snapshot/part_ABC/timestamps.bin → inode #42
  Inode #42:
    link_count = 2 (two directory entries)
    ref_count = 0
    blocks = [block1, block2, block3, ...] (same blocks)

After merge deletes live part:
  datadb/part_ABC/timestamps.bin → DELETED
  snapshot/part_ABC/timestamps.bin → inode #42
  Inode #42:
    link_count = 1 (one directory entry)
    ref_count = 0
    blocks = [block1, block2, block3, ...] (still alive)

After snapshot deleted:
  snapshot/part_ABC/timestamps.bin → DELETED
  Inode #42:
    link_count = 0
    ref_count = 0
    blocks = FREED (disk space reclaimed)
```

**Unix guarantee:** A file's blocks are freed only when:
1. `link_count = 0` (no directory entries)
2. `ref_count = 0` (no open file handles)

### Consistency Properties

| Property | How It's Achieved |
|----------|-------------------|
| Point-in-time | Flush all in-memory parts before snapshot |
| Atomic | Hard links are atomic at filesystem level |
| Concurrent-safe | incRef prevents deletion during snapshot |
| Space-efficient | Hard links share blocks, zero copy |
| Crash-safe | fsync ensures directory entries persisted |

### What's NOT in the Snapshot

1. **Rows in shard buffers** (rowsBuffer)
   - Still in per-CPU memory, not yet in parts
   - Typically ~1 second of data

2. **Rows arriving during snapshot**
   - New rows ingested after flush completes
   - These go to new parts not in snapshot

3. **In-memory parts created after flush**
   - Flush happens at step 2
   - New in-memory parts created after are not included

**Trade-off:** Snapshot may miss up to ~1 second of data, but ingestion throughput is unaffected. No global pause needed.

### Why Not Just Copy Files?

```
Copy approach:
  1. Stop all ingestion, merges, queries (global pause)
  2. Copy all files
  3. Resume operations
  
  Cost: Minutes of downtime for large datasets

Hard link approach:
  1. Brief flush (milliseconds)
  2. Hard link (instant, no data copy)
  3. Continue operations
  
  Cost: ~1 second of data may be missed
```

### Code References

- `lib/logstorage/partition.go` - `mustCreateSnapshot`
- `lib/logstorage/datadb.go` - `mustCreateSnapshotAt`
- `lib/fs/fs.go` - `MustHardLinkFiles`

## Conclusions

1. **Hard links** enable zero-copy snapshots
2. **Reference counting** prevents premature deletion
3. **Atomic flush** ensures point-in-time consistency
4. **Concurrent operations** continue without pause
5. **Unix semantics** guarantee data remains until last link removed

## Open Questions

- How to handle snapshots that span multiple partitions atomically?
- Should snapshot creation be rate-limited to prevent disk fragmentation?
