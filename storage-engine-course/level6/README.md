# Level 6 - VictoriaLogs LSM Internals In `datadb`

## Prerequisite
- Complete `level5h` checkpoint first.

## Objective
Map generic LSM ideas to exact VictoriaLogs code paths and invariants.

## Outcomes
By the end of this level, you can:
- explain the three-tier part model
- explain how merge destination type is selected
- explain part lifecycle and atomic swap behavior

## From Generic LSM to VictoriaLogs `datadb` (Conceptual Background)

### What `datadb` is

In Level 5 you built or studied a generic LSM engine with a memtable, SSTables, compaction, and a manifest. `datadb` is VictoriaLogs' concrete implementation of those same ideas, tuned for log data. It lives inside a single day-partition and manages all three tiers of parts — in-memory, small, and big — plus their merge workers, flush timers, and lifecycle coordination.

Think of `datadb` as the answer to: "What does an LSM engine look like when you remove the WAL, make the SSTables columnar, and optimize everything for append-only time-series log data?"

### The `partWrapper` struct: what each field does

Every part in the system is wrapped in a `partWrapper` that tracks its lifecycle state:

```
partWrapper {
    p             *part           The underlying part (file handles, metaindex, headers)
    mp            *inmemoryPart   Non-nil only for in-memory parts (nil for disk parts)
    refCount      atomic.Int32    Active references (readers, the list itself, snapshots)
    mustDrop      atomic.Bool     "Delete me when refCount hits 0"
    isInMerge     bool            "I'm currently being merged — don't select me again"
    flushDeadline time.Time       When this in-memory part must be written to disk
}
```

Each field exists to solve a specific concurrency problem:

- **`refCount`** solves safe deletion. A merge can't delete a part while a query is reading it. Instead, the merge sets `mustDrop = true` and calls `decRef()`. If a query holds a reference, the part survives until the query calls `decRef()` and the count reaches zero.

- **`isInMerge`** solves double-selection. Two concurrent merge workers must never pick the same part. `isInMerge` is checked and set under `partsLock`, so the selection is atomic. When a merge completes (or fails), `releasePartsToMerge` clears the flag.

- **`flushDeadline`** solves the durability window. In-memory parts are volatile — a crash loses them. The deadline (current time + flush interval) tells the background flusher "write this to disk by this time." The periodic `inmemoryPartsFlusher` collects parts past their deadline and merges them to files.

- **`mp`** distinguishes part types without a separate type field. If `mp != nil`, it's an in-memory part backed by a `chunkedBuffer`. If `mp == nil`, it's a file-based part backed by `fs.MustReadAtCloser` file handles. Both implement the same `ReadAt` interface, so queries use identical code for both.

### The three-tier model in practice

The tiers aren't just size categories — they have distinct operational characteristics:

```
Tier: In-memory
  Storage:    RAM (chunkedBuffer)
  Size limit: ~10% of allowed RAM / 20 partitions (min 1 MB)
  Durability: None — lost on crash
  Merge pool: inmemoryPartsConcurrencyCh (per-CPU slots)
  Purpose:    Absorb ingestion bursts, make data queryable immediately

Tier: Small
  Storage:    Disk, no nocache flag (stays in OS page cache)
  Size limit: Scales with free RAM (min 10 MB)
  Durability: Full (fsynced)
  Merge pool: smallPartsConcurrencyCh (per-CPU slots)
  Purpose:    Hold recently flushed data in page-cache-friendly sizes

Tier: Big
  Storage:    Disk, nocache flag set (bypasses page cache)
  Size limit: 1 TB or available disk
  Durability: Full (fsynced)
  Merge pool: bigPartsConcurrencyCh (per-CPU slots)
  Purpose:    Long-term storage, sequential I/O optimized
```

The `nocache` distinction matters: small parts are expected to be re-read soon (by queries on recent data), so they stay in the OS page cache. Big parts are cold — reading them through the page cache would evict hot small parts. Setting `nocache` tells the block stream writer to use `O_DIRECT`-style flags, bypassing the cache.

### `getDstPartType`: the promotion decision

After a merge completes, the system must decide which tier the output part belongs to. This is not a fixed rule but a four-branch decision tree, evaluated in order:

```
Branch 1: output size > maxSmallPartSize?
  → partBig
  Rationale: too large for page cache. Force to big tier.

Branch 2: isFinal (shutdown flush) OR output size > maxInmemoryPartSize?
  → partSmall
  Rationale: data must reach disk. Either the system is shutting down
  (isFinal=true, can't keep anything in memory) or the part is too large
  for RAM.

Branch 3: any source part is a file-based part?
  → partSmall
  Rationale: once data is durable on disk, never move it back to RAM.
  A merge of [small + small] produces small, not inmemory.
  This prevents silently making durable data volatile.

Branch 4: all sources are in-memory AND output is small enough
  → partInmemory
  Rationale: small in-memory merges stay in memory to reduce disk I/O.
  Two 500 KB in-memory parts merge into one 1 MB in-memory part.
```

Branch 3 is the most subtle. Without it, merging two small parts could produce an in-memory part if the output were small enough — but this would mean crash-safe data became crash-vulnerable. The "never regress durability" invariant is enforced by this single check.

### `mustMergePartsInternal`: the full merge control flow

The merge function is the most complex operation in `datadb`. Here is its control flow, annotated with what each step achieves:

```
mustMergePartsInternal(pws, isFinal, dropFilter, stopCh)
│
├── 1. Assert all parts have isInMerge=true
│      Safety: confirms the caller properly reserved these parts.
│      Defer: releasePartsToMerge(pws) — always clear isInMerge on exit.
│
├── 2. getDstPartType(pws, isFinal)
│      Decides output tier based on size and source types.
│
├── 3. tryReserveDiskSpace(totalCompressedSize)
│      Atomically increments a global counter. If the reservation would
│      exceed available disk space and !isFinal, the merge is skipped.
│      Final merges (shutdown) proceed regardless — data must be persisted.
│
├── 4. Increment active merge counter for the destination tier
│      ddb.smallPartActiveMerges.Add(1) or equivalent.
│      Used by Prometheus metrics to show current merge load.
│
├── 5. Generate destination path: mergeIdx (atomic uint64) → hex string
│      Unique directory name for the output part.
│
├── 6. Fast path: single in-memory part flush
│      If isFinal && len(pws)==1 && pws[0].mp != nil:
│        → mp.MustStoreToDisk(path) directly, skip stream merge.
│      Avoids the overhead of opening a block stream reader/writer
│      for a single part that just needs to be written out.
│
├── 7. Normal path: k-way merge
│      a. Open one blockStreamReader per source part
│      b. Open one blockStreamWriter for the output
│         - In-memory output: write to chunkedBuffer
│         - File output: write to disk files
│         - Big parts: set nocache=true (bypass page cache)
│      c. mustMergeBlockStreams(bsw, bsrs, dropFilter, stopCh)
│         Heap-based merge-sort of all source block streams.
│         Full blocks pass through without decompression.
│         Small blocks are accumulated and re-blocked.
│         Rows matching dropFilter are omitted (deletion).
│      d. Finalize writer (writes metaindex, column names, part header)
│      e. If stopCh fired: delete incomplete output, return false
│
├── 8. Write metadata
│      - In-memory: set mpNew.ph = ph
│      - File: ph.mustWriteMetadata(path) + fsync
│
├── 9. swapSrcWithDstParts(pws, pwNew, dstPartType)
│      Atomic replacement (see below).
│
└── 10. Log stats if merge took > 1 minute
        Includes: duration, rows merged, source/output sizes, tier.
```

### `swapSrcWithDstParts`: why the lock covers everything

The atomic swap must ensure no observer ever sees an intermediate state — neither "old parts removed but new part not yet added" nor "both old and new parts present." It does this by performing all mutations under a single `partsLock` acquisition:

```
partsLock.Lock()
│
├── Remove source parts from all three tier lists
│   removeParts(inmemoryParts, partsToRemove)
│   removeParts(smallParts, partsToRemove)
│   removeParts(bigParts, partsToRemove)
│
├── Add the new part to the correct tier list
│   ddb.smallParts = append(ddb.smallParts, pwNew)
│
├── Write parts.json (atomically: temp + fsync + rename)
│   Lists all current small + big parts.
│   Must be under partsLock because concurrent merges could
│   also be updating the part lists. Without the lock, two
│   goroutines could each snapshot a stale list and write it.
│
├── Start a merge worker for the destination tier
│   startSmallPartsMergerLocked() — spawns a goroutine to check
│   if the new part enables another merge.
│
partsLock.Unlock()
│
├── Mark old parts: pw.mustDrop.Store(true)
├── Release references: pw.decRef()
│   If refCount reaches 0 → fs.MustRemoveDir(part.path)
│   If a query holds a reference → part survives until query finishes
```

The `parts.json` write inside the lock is critical. Consider what happens without it: goroutine A removes parts [P1, P2] and adds P5, while goroutine B removes parts [P3, P4] and adds P6. If both snapshot the part list before writing `parts.json`, one write would overwrite the other's changes. Under the lock, the writes are serialized: A writes `[P3, P4, P5]`, then B writes `[P5, P6]`.

### How a reader coexists with a merge

The interaction between a query and a concurrent merge illustrates all the mechanisms working together:

```
Time 0: Parts list = [P1, P2, P3, P4, P5]

Time 1: Query starts
  partsLock.Lock()
  Select P2, P3 (time range match)
  P2.incRef() → refCount=2
  P3.incRef() → refCount=2
  partsLock.Unlock()
  → Query begins scanning P2 and P3

Time 2: Merge worker starts
  partsLock.Lock()
  Select P2, P3, P4 (best merge ratio)
  P2.isInMerge = true
  P3.isInMerge = true
  P4.isInMerge = true
  partsLock.Unlock()
  → Merge begins reading P2, P3, P4 and writing P6

Time 3: Merge completes
  partsLock.Lock()
  Remove P2, P3, P4 from list
  Add P6 to list
  Write parts.json = [P1, P5, P6]
  partsLock.Unlock()
  P2.mustDrop = true; P2.decRef() → refCount=1 (query still holds it)
  P3.mustDrop = true; P3.decRef() → refCount=1 (query still holds it)
  P4.mustDrop = true; P4.decRef() → refCount=0 → fs.MustRemoveDir(P4)

Time 4: Query finishes
  P2.decRef() → refCount=0, mustDrop=true → fs.MustRemoveDir(P2)
  P3.decRef() → refCount=0, mustDrop=true → fs.MustRemoveDir(P3)

Time 4: Parts list = [P1, P5, P6], all old parts deleted
```

At no point does the query see corrupted data. P2 and P3's files remain on disk (their directory entries exist, the OS keeps the inodes alive) until the query releases its references. P4 had no active readers, so it was deleted immediately.

### Crash safety through ordering

The merge's crash safety relies on the ordering of steps, not on any transactional mechanism:

| Crash point | parts.json says | On disk | Recovery |
|-------------|----------------|---------|----------|
| During merge (Step 7) | [P1, P2, P3, P4, P5] | P1-P5 + partial P6 | P6 is orphan → deleted on startup |
| After metadata write (Step 8), before swap | [P1, P2, P3, P4, P5] | P1-P5 + complete P6 | P6 is orphan → deleted on startup |
| After parts.json update (Step 9) | [P1, P5, P6] | P1-P6 all present | P2, P3, P4 are orphans → deleted on startup |
| After old part deletion | [P1, P5, P6] | P1, P5, P6 | Clean state |

In every case, `parts.json` is the source of truth. Directories not in the manifest are orphans and are safely deleted. The only unrecoverable scenario — a part listed in `parts.json` but missing from disk — cannot occur through normal operation because `parts.json` is only updated after the new part is fully written and fsynced.

## Source Anchors
- `lib/logstorage/datadb.go`
  - `datadb` struct
  - `partWrapper`
  - `mustFlushInmemoryPartsToFiles`
  - `mustMergePartsInternal`
  - `getDstPartType`
  - `swapSrcWithDstParts`
- `lib/logstorage/part.go`
  - part open/close

## Core Concepts
1. Part tiers:
   - `partInmemory`
   - `partSmall`
   - `partBig`
2. Durability and flush deadlines.
3. Reference counting for safe concurrent reads/writes/merges.
4. Atomic source->destination part swap to avoid inconsistent visible state.

## Guided Reading Tasks
1. Explain all fields in `partWrapper` and their lifecycle role.
2. In `mustMergePartsInternal`, map full control flow:
   - destination type
   - disk reservation
   - stream merge
   - destination materialization
   - source replacement
3. In `getDstPartType`, explain each branch with operational rationale.
4. In `swapSrcWithDstParts`, explain why `parts.json` update must be under lock.

## Hands-On Lab
### Lab 1: Merge Trace
Create a detailed trace of one merge cycle with state snapshots:
- pre-merge part lists
- in-merge flags
- post-swap part lists
- old part cleanup path

### Lab 2: Failure Scenario Reasoning
Explain behavior if merge is interrupted before swap vs after swap.

## Checkpoint Questions
1. Why is immutable-part + atomic-swap a robust design?
2. Why keep `inmemory`, `small`, and `big` separate?
3. Why can non-final merge be skipped when disk reservation fails?

## Deliverables
- `level6/merge-trace.md`
- `level6/failure-scenarios.md`
- `level6/checkpoint.md`

## Pass Criteria
- You can reason about correctness and durability under concurrent load.
- You can explain exact responsibilities of locks, refs, and swap logic.
