# Level 6 Checkpoint Answers

## Context
- Level: 6 - VictoriaLogs LSM Internals In `datadb`
- Date: 2026-03-02

## Checkpoint Questions

### 1. Why is immutable-part + atomic-swap a robust design?

**Answer:**

The combination of immutable parts and atomic swap provides several critical guarantees:

**1. No Concurrent Modification**

```
Traditional mutable design:
  - Query reads P1
  - Merge modifies P1 in-place
  - Query sees corrupted/partial data

Immutable design:
  - Query reads P1
  - Merge creates new P2, doesn't touch P1
  - Query always sees consistent P1
```

Parts are never modified after creation. A merge creates a NEW part with merged data, leaving old parts untouched.

**2. Atomic Visibility**

```go
// swapSrcWithDstParts in datadb.go:1192-1246
partsLock.Lock()
  // Remove old parts
  // Add new part
  // Update manifest
partsLock.Unlock()
// Mark old parts for deletion
```

All observers see either:
- The old state (before swap)
- The new state (after swap)
- Never an intermediate state (old parts removed, new part not added)

**3. Safe Deletion with Reference Counting**

```
Without ref counting:
  - Merge deletes P1
  - Query still reading P1 → crash/corruption

With ref counting:
  - Merge marks P1.mustDrop = true
  - Query holds P1.refCount = 1
  - P1 not deleted until query calls decRef()
```

**4. Crash Recovery Simplicity**

```
Crash during merge:
  - Old parts still exist
  - New part is orphan (not in manifest)
  - Recovery: delete orphan, old parts intact

Crash after swap:
  - New part in manifest
  - Old parts are orphans
  - Recovery: delete orphans, new part intact
```

No complex transaction rollback needed. The manifest (parts.json) is the source of truth.

**5. Snapshot Isolation**

```
Query at time T1:
  - Acquires references to [P1, P2, P3]
  - Parts list changes to [P4] at time T2
  - Query continues reading [P1, P2, P3]
  - Query sees consistent snapshot from T1
```

**Source reference:** `datadb.go:1192-1246` (swapSrcWithDstParts), `datadb.go:198-259` (partWrapper)

### 2. Why keep `inmemory`, `small`, and `big` separate?

**Answer:**

The three tiers have distinct operational characteristics that justify separate management:

**1. In-memory Tier**

```
Storage:    RAM (chunkedBuffer)
Size limit: ~10% of RAM / 20 partitions (min 1 MB)
Durability: None — lost on crash
Purpose:    Absorb ingestion bursts, immediate queryability
```

Benefits:
- **Zero disk I/O** for ingestion
- **Immediate visibility** - data queryable before disk write
- **Burst absorption** - handles traffic spikes without disk bottleneck

Cost:
- **Volatile** - lost on crash
- **Memory pressure** - limited by available RAM

**2. Small Tier**

```
Storage:    Disk, NO nocache flag (stays in OS page cache)
Size limit: Scales with free RAM (min 10 MB)
Durability: Full (fsynced)
Purpose:    Recent data, page-cache-friendly
```

Benefits:
- **Durable** - survives crash
- **Fast reads** - stays in OS page cache
- **Optimal for recent queries** - most queries hit recent data

Key distinction: **NO nocache flag** means OS keeps these files in page cache.

**3. Big Tier**

```
Storage:    Disk, nocache flag SET (bypasses page cache)
Size limit: 1 TB or available disk
Durability: Full (fsynced)
Purpose:    Long-term storage, sequential I/O optimized
```

Benefits:
- **Large capacity** - limited only by disk
- **Page cache protection** - doesn't evict hot small parts
- **Sequential I/O** - optimized for cold data scans

Key distinction: **nocache flag** prevents cold big parts from evicting hot small parts from page cache.

**Why Not Just Two Tiers (Memory + Disk)?**

```
Problem with two tiers:
  - Recent data (small) and historical data (big) treated same
  - Reading big parts evicts small parts from page cache
  - Query performance degrades for recent data

With three tiers:
  - Small parts stay in page cache (frequently accessed)
  - Big parts bypass page cache (rarely accessed)
  - Recent data queries remain fast even with large historical data
```

**Why Not Just One Tier (All Disk)?**

```
Problem with one tier:
  - Every write goes to disk immediately
  - High disk I/O for ingestion
  - Cannot absorb bursts
  - Latency for newly ingested data visibility

With in-memory tier:
  - Buffer writes in RAM
  - Batch flush to disk
  - Immediate queryability
```

**Source reference:** README.md:49-73 (tier characteristics)

### 3. Why can non-final merge be skipped when disk reservation fails?

**Answer:**

Non-final merges are optimizations, not requirements. Final merges (shutdown) cannot be skipped because they ensure durability.

**Non-Final Merge**

```go
// datadb.go:656-666
if tryReserveDiskSpace(ddb.path, partsSize) {
    defer releaseDiskSpace(partsSize)
} else {
    if !isFinal {
        // Skip the merge - not enough disk space
        return false
    }
    // Final merge proceeds regardless
}
```

**Why Skipping is Safe:**

1. **Data Already Durable**

```
Non-final merge sources:
  - In-memory parts: volatile, but...
  - Small parts: already fsynced to disk
  - Big parts: already fsynced to disk

If merging small→small or big→big:
  - Source data is already durable
  - Skipping merge doesn't risk data loss
  - Merge is just optimization (fewer files)
```

2. **Merge Will Be Retried**

```
When disk space becomes available:
  - Merge worker runs again
  - Same parts selected
  - Merge completes

Nothing is lost by waiting.
```

3. **System Remains Functional**

```
Without merge:
  - More small parts than optimal
  - Slightly slower queries (more files to read)
  - But still correct and durable

With merge (if space available):
  - Fewer, larger parts
  - Faster queries
  - Better disk space utilization
```

**Why Final Merge Cannot Be Skipped:**

```go
// datadb.go:659-665
if !isFinal {
    return false  // Skip non-final merge
}
// Try performing final merge even if there is no enough disk space
// in order to persist in-memory data to disk.
// It is better to crash on out of memory error in this case.
```

Final merge happens during shutdown:
- In-memory parts MUST be flushed to disk
- If skipped, in-memory data is LOST on shutdown
- Data durability > disk space concerns
- "Better to crash on OOM" than lose data

**Trade-off:**

| Merge Type | Skip Allowed | Reason |
|------------|--------------|--------|
| In-memory → In-memory | Yes | Still in RAM, no durability change |
| In-memory → Small | No (final only) | Must persist to disk |
| Small → Small | Yes | Already durable on disk |
| Small → Big | Yes | Already durable on disk |
| Final (shutdown) | No | Must persist all in-memory data |

**Source reference:** `datadb.go:656-666` (disk reservation logic)
