# Level 6 Failure Scenarios

## Context
- Level: 6 - VictoriaLogs LSM Internals In `datadb`
- Date: 2026-03-02

## Crash Safety Through Ordering

VictoriaLogs achieves crash safety through careful ordering of operations, not through transactions. The key insight: **`parts.json` is always the source of truth**.

## Scenario 1: Crash During Merge (Step 7)

### Timeline

```
1. Merge starts reading parts [P1, P2, P3]
2. Creating output part P4...
3. Writing blocks to P4...
4. CRASH!
```

### State at Crash

```
parts.json: [P1, P2, P3]
On disk:    [P1, P2, P3, partial-P4]

P1: complete, in manifest
P2: complete, in manifest
P3: complete, in manifest
P4: incomplete, NOT in manifest
```

### Recovery

```
1. Read parts.json → [P1, P2, P3]
2. Scan data directory → [P1, P2, P3, P4]
3. Identify orphans → [P4]
4. Delete orphans → remove P4
5. Result: [P1, P2, P3]
```

### Why Safe

- `parts.json` not yet updated
- P4 not in manifest → treated as orphan
- Original data (P1, P2, P3) intact
- **No data loss**

## Scenario 2: Crash After Metadata Write (Step 8), Before Swap

### Timeline

```
1. Merge completes
2. P4 fully written
3. P4 metadata fsynced
4. About to update parts.json...
5. CRASH!
```

### State at Crash

```
parts.json: [P1, P2, P3]
On disk:    [P1, P2, P3, P4]

P1: complete, in manifest
P2: complete, in manifest
P3: complete, in manifest
P4: complete, NOT in manifest
```

### Recovery

```
1. Read parts.json → [P1, P2, P3]
2. Scan data directory → [P1, P2, P3, P4]
3. Identify orphans → [P4]
4. Delete orphans → remove P4
5. Result: [P1, P2, P3]
```

### Why Safe

- Same as Scenario 1
- Complete but unreferenced P4 is orphan
- Original data intact
- **No data loss** (but merge work is lost)

## Scenario 3: Crash After parts.json Update (Step 9)

### Timeline

```
1. Merge completes
2. P4 fully written and fsynced
3. parts.json updated to [P1, P4]
4. About to delete P2, P3...
5. CRASH!
```

### State at Crash

```
parts.json: [P1, P4]
On disk:    [P1, P2, P3, P4]

P1: complete, in manifest
P2: complete, NOT in manifest (orphan)
P3: complete, NOT in manifest (orphan)
P4: complete, in manifest
```

### Recovery

```
1. Read parts.json → [P1, P4]
2. Scan data directory → [P1, P2, P3, P4]
3. Identify orphans → [P2, P3]
4. Delete orphans → remove P2, P3
5. Result: [P1, P4]
```

### Why Safe

- `parts.json` updated atomically (temp + fsync + rename)
- Orphan cleanup removes old parts
- All referenced parts exist
- **Clean state** - merge completed successfully

## Scenario 4: Crash During parts.json Write

### Timeline

```
1. Writing parts.json.temp...
2. CRASH!
```

### State at Crash

```
parts.json:      [P1, P2, P3]  (intact)
parts.json.temp: incomplete
On disk:         [P1, P2, P3, P4]
```

### Recovery

```
1. Read parts.json → [P1, P2, P3]
2. parts.json.temp exists → delete it
3. Scan data directory → [P1, P2, P3, P4]
4. Identify orphans → [P4]
5. Delete orphans → remove P4
6. Result: [P1, P2, P3]
```

### Why Safe

- Atomic rename not completed
- Original `parts.json` intact
- Temp file cleaned up on startup
- **No data loss**

## Scenario 5: Crash After Complete Cleanup

### Timeline

```
1. Merge completes
2. P4 written and fsynced
3. parts.json updated
4. P2, P3 deleted
5. CRASH!
```

### State at Crash

```
parts.json: [P1, P4]
On disk:    [P1, P4]

P1: complete, in manifest
P4: complete, in manifest
```

### Recovery

```
1. Read parts.json → [P1, P4]
2. Scan data directory → [P1, P4]
3. No orphans
4. Result: [P1, P4]
```

### Why Safe

- All operations completed
- Clean state
- **Perfect recovery**

## Failure Scenario Summary

| Crash Point | parts.json | On Disk | Orphans | Recovery |
|-------------|------------|---------|---------|----------|
| During merge (step 7) | [P1,P2,P3] | [P1,P2,P3,P4'] | P4' | Delete P4' |
| After metadata (step 8) | [P1,P2,P3] | [P1,P2,P3,P4] | P4 | Delete P4 |
| After parts.json (step 9) | [P1,P4] | [P1,P2,P3,P4] | P2,P3 | Delete P2,P3 |
| During parts.json write | [P1,P2,P3] | [P1,P2,P3,P4] | P4 | Delete temp, P4 |
| After cleanup | [P1,P4] | [P1,P4] | None | Clean state |

P4' = incomplete P4

## Critical Invariant

**The only unrecoverable scenario** cannot occur through normal operation:

```
Scenario: parts.json lists P4, but P4 is missing from disk

Why this cannot happen:
  1. P4 is fully written and fsynced BEFORE parts.json update
  2. parts.json is only updated AFTER P4 is durable
  3. Therefore: if P4 is in parts.json, it must exist on disk
```

## Evidence

Lab program `main.go` demonstrates:
- All failure scenarios with state diagrams
- Recovery actions for each scenario
- Why parts.json is the source of truth

## Conclusions

1. **Ordering guarantees safety** - write data, then manifest
2. **Orphan cleanup handles everything** - any unreferenced directory is deleted
3. **No transactions needed** - careful ordering is sufficient
4. **parts.json is the manifest** - source of truth for recovery

## Open Questions

- What happens if parts.json itself is corrupted?
- How does replication affect crash recovery?
