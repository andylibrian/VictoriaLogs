# Level 6 Merge Trace

## Context
- Level: 6 - VictoriaLogs LSM Internals In `datadb`
- Date: 2026-03-02

## Complete Merge Cycle Trace

### Pre-Merge State

```
Parts List:
  inmemory: [P1, P2, P3]  (3 parts, 600 KB total)
  small:    []             (0 parts)
  big:      []             (0 parts)

Part Details:
  P1: inmemory, 200 KB, 1000 rows, refCount=1, isInMerge=false
  P2: inmemory, 200 KB, 1000 rows, refCount=1, isInMerge=false
  P3: inmemory, 200 KB, 1000 rows, refCount=1, isInMerge=false
```

### Step 1: Part Selection

```
Action: Select parts for merge
  partsLock.Lock()
  Evaluate: Which parts to merge?
  Decision: [P1, P2, P3] - good merge ratio
  Set: P1.isInMerge = true
  Set: P2.isInMerge = true
  Set: P3.isInMerge = true
  Increment: P1.incRef(), P2.incRef(), P3.incRef()
  partsLock.Unlock()
```

### Step 2: Destination Type Decision

```
Action: getDstPartType([P1, P2, P3], isFinal=false)

Branch evaluation:
  1. dstPartSize (600 KB) > maxSmallPartSize (100 MB)? NO
  2. isFinal OR dstPartSize > maxInmemoryPartSize (1 MB)? NO
  3. Any source is file-based? NO (all inmemory)
  4. All sources inmemory AND size < 1 MB? YES

Result: partInmemory
Reason: Small in-memory merge stays in-memory to reduce disk I/O
```

### Step 3: Disk Reservation

```
Action: Reserve disk space (skipped for inmemory destination)
  dstPartType == partInmemory → skip reservation
```

### Step 4: Merge Counter Update

```
Action: Update merge metrics
  ddb.inmemoryMergesTotal.Add(1)
  ddb.inmemoryActiveMerges.Add(1)
```

### Step 5: Destination Path Generation

```
Action: Generate unique destination path
  mergeIdx = ddb.nextMergeIdx() = 42
  dstPartPath = ""  (empty for inmemory parts)
```

### Step 6: Block Stream Merge

```
Action: Open readers and writer
  Open: blockStreamReader for P1
  Open: blockStreamReader for P2
  Open: blockStreamReader for P3
  Open: blockStreamWriter for inmemory destination

Action: Execute k-way merge
  mustMergeBlockStreams(writer, readers, dropFilter, stopCh)
  - Heap-based merge-sort of block streams
  - Full blocks pass through without decompression
  - Small blocks accumulated and re-blocked
  - Result: partHeader with rows=3000, size=600 KB
```

### Step 7: Destination Part Creation

```
Action: Create new part wrapper
  mpNew.ph = partHeader{RowsCount: 3000, CompressedSize: 600 KB}
  pwNew = newPartWrapper(p, mpNew, flushDeadline)
  
New Part:
  P4: inmemory, 600 KB, 3000 rows, refCount=1, isInMerge=false
```

### Step 8: Atomic Swap

```
Action: swapSrcWithDstParts([P1, P2, P3], P4, partInmemory)

Inside partsLock:
  partsLock.Lock()
  
  Remove P1, P2, P3 from inmemoryParts:
    ddb.inmemoryParts = []  (now empty)
  
  Add P4 to inmemoryParts:
    ddb.inmemoryParts = [P4]
  
  Write parts.json (skipped - no file-based parts changed)
  
  partsLock.Unlock()

After lock release:
  P1.mustDrop = true; P1.decRef() → refCount=0 → return to pool
  P2.mustDrop = true; P2.decRef() → refCount=0 → return to pool
  P3.mustDrop = true; P3.decRef() → refCount=0 → return to pool
```

### Post-Merge State

```
Parts List:
  inmemory: [P4]           (1 part, 600 KB)
  small:    []             (0 parts)
  big:      []             (0 parts)

Part Details:
  P4: inmemory, 600 KB, 3000 rows, refCount=1, isInMerge=false

Old Parts:
  P1, P2, P3: deleted (returned to pool)
```

## Different Merge Scenarios

### Scenario 1: In-memory Merge (Stays In-memory)

```
Sources: [inmemory P1, inmemory P2, inmemory P3]
Total Size: 600 KB
isFinal: false

getDstPartType decision:
  Branch 4: All inmemory AND size < 1 MB → partInmemory

Result: Output stays in-memory
```

### Scenario 2: Final Flush to Disk

```
Sources: [inmemory P4, inmemory P5]
Total Size: 1 MB
isFinal: true

getDstPartType decision:
  Branch 2: isFinal=true → partSmall

Result: Force to disk for durability
```

### Scenario 3: Small to Big Promotion

```
Sources: [small P6 (40 MB), small P7 (40 MB), small P8 (40 MB)]
Total Size: 120 MB
isFinal: false

getDstPartType decision:
  Branch 1: 120 MB > maxSmallPartSize (100 MB) → partBig

Result: Promote to big tier
```

### Scenario 4: Durability Preservation

```
Sources: [small P9 (100 KB), small P10 (100 KB)]
Total Size: 200 KB
isFinal: false

getDstPartType decision:
  Branch 1: 200 KB > 100 MB? NO
  Branch 2: isFinal? NO, 200 KB > 1 MB? NO
  Branch 3: Any source file-based? YES → partSmall

Result: Output is small (file), NOT inmemory
Reason: Never regress from durable (file) to volatile (memory)
```

## Evidence

Lab program `merge_trace.go` demonstrates:
- Complete merge trace with state snapshots
- getDstPartType decision tree
- Atomic swap operation
- Reference counting for cleanup

## Conclusions

1. **Merge is atomic** - swap happens under single lock acquisition
2. **Destination type is deterministic** - four-branch decision tree
3. **Durability is never regressed** - file inputs always produce file output
4. **Cleanup is safe** - reference counting prevents premature deletion

## Open Questions

- How does merge prioritization work when multiple merges are possible?
- What happens if merge is interrupted by shutdown?
