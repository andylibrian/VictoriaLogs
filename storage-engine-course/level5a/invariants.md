# LSM Invariants

## Context
- Date: Level 5A
- Owner: Storage Engine Course

## Work

### Core LSM Invariants

These invariants must always hold true in VictoriaLogs. Violations indicate bugs or corruption.

| ID | Invariant | Description | Check Predicate |
|----|-----------|-------------|-----------------|
| I1 | Immutability | Once a part is flushed to disk, its content never changes | `partHash(before) == partHash(after)` |
| I2 | No Data Loss | All rows written must be readable until explicitly deleted | `sum(parts.rows) + memtable.rows == ingested - deleted` |
| I3 | Single Writer | Each part is owned by exactly one tier at any time | `part ∈ at most one of {inmemory, small, big}` |
| I4 | Reference Safety | A part with refCount > 0 is never deleted | `refCount > 0 ⇒ deletion blocked` |
| I5 | Merge Atomicity | Source parts remain visible until merge completes | `atomic swap under partsLock` |
| I6 | Size Monotonicity | Merged part size >= sum of source sizes | `output.Size >= sum(input.Sizes)` |
| I7 | Partition Ordering | Parts within partition sorted by timestamp | `part[i].maxTime <= part[i+1].minTime` |
| I8 | Tier Boundaries | Parts respect tier size limits | `inmemory < smallThresh, small < bigThresh` |

### Enforcement Points

**Part Creation (Flush)**
- Invariants: I1, I2, I6
- Implementation: `fsync` before adding to active list
- Code: `lib/logstorage/datadb.go:mustCreateDatadb`

**Merge Completion**
- Invariants: I3, I5, I6
- Implementation: `partsLock` held for atomic swap
- Code: `lib/logstorage/datadb.go:registerMergedParts`

**Reference Counting**
- Invariants: I4
- Implementation: `incRef` before use, `decRef` after use
- Code: `lib/logstorage/datadb.go:partWrapper.incRef/decRef`

**Part Deletion**
- Invariants: I4
- Implementation: Delete only when `refCount == 0 AND mustDrop == true`
- Code: `lib/logstorage/datadb.go:partWrapper.decRef`

**Tier Transitions**
- Invariants: I8
- Implementation: Check size before adding to tier
- Code: `lib/logstorage/datadb.go:registerMergedParts`

## Evidence

### Violation Scenarios

**I1 (Immutability) Violation**
- Symptom: Query returns different results for same query
- Root cause: Memory corruption, disk corruption, or concurrent write bug
- Detection: Hash verification on read

**I2 (No Data Loss) Violation**
- Symptom: Count queries return fewer results over time
- Root cause: Merge bug dropping rows, or premature part deletion
- Detection: Row count checksums

**I3 (Single Writer) Violation**
- Symptom: Double counting in metrics, duplicate query results
- Root cause: Missing lock or race condition in tier assignment
- Detection: Set membership assertion

**I4 (Reference Safety) Violation**
- Symptom: Panic, segfault, or garbage data in results
- Root cause: `decRef` without checking, or missing `incRef`
- Detection: Reference count assertions in debug builds

**I5 (Merge Atomicity) Violation**
- Symptom: Data disappears during merge, partial results
- Root cause: Lock not held during part list modification
- Detection: Before/after state verification

**I6 (Size Monotonicity) Violation**
- Symptom: Unexpected data loss, compression issues
- Root cause: Bug in merge logic or compression
- Detection: Size assertions after merge

### Testing Strategy

1. **Unit tests with assertions**: Verify invariants after each operation
2. **Fuzz testing**: Random operations with invariant checks
3. **Integration tests**: Before/after verification
4. **Runtime assertions**: Debug builds with extra checks
5. **Production monitoring**: Metrics and alerts for anomalies

## Conclusions

- LSM invariants are the contract that ensures data integrity
- Reference counting (I4) is the key mechanism for safe concurrent access
- Immutability (I1) simplifies crash recovery and concurrent reads
- Merge atomicity (I5) ensures queries never see partial state
- Testing invariants is critical - violations cause data loss or corruption

## Open Questions

1. How to efficiently verify I2 (No Data Loss) in production without full scans?
2. Should I7 (Partition Ordering) be relaxed for out-of-order ingestion?
3. What's the recovery procedure when I1 (Immutability) is violated due to disk corruption?
