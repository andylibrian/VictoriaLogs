# Level 7 Delete-Aware Merge

## Context
- Level: 7 - Merge Selection Heuristics and K-Way Merge
- Date: 2026-03-02

## How Deletion Works in VictoriaLogs

### Key Insight: No Tombstones

**Traditional LSM deletion:**
1. Write tombstone record
2. Queries check for tombstones
3. Compaction removes tombstones + deleted data

**VictoriaLogs deletion:**
1. Delete request triggers targeted merge
2. Merge uses dropFilter to skip matching rows
3. New part created WITHOUT deleted rows
4. Old parts replaced atomically
5. Deleted rows are PHYSICALLY GONE

### Advantages
- No tombstone tracking overhead
- No garbage collection pass needed
- Immediate space reclamation (after merge)

## Block Decision Tree During Merge

```
mustWriteBlock(block):

  Step 1: Check stream change
    IF block.streamID != currentStream:
      → Flush accumulated rows
      → Start new stream context

  Step 2: Check dropFilter (if present)
    IF dropFilter matches block.streamID:

      Sub-check: Is filter stream-only (filterNoop)?
        YES → DROP entire block (no decompression)
              → All rows belong to deleted stream
        NO  → FILTER: decompress, apply per-row
              → Keep non-matching rows

  Step 3: Check fast path eligibility
    IF accumulator empty AND block ≥ 2 MB:
      AND no dropFilter match:
        → FAST PATH: copy raw bytes
        → No decompression needed

  Step 4: Merge path
    OTHERWISE:
      → Decompress block
      → Merge with accumulated rows
      → Flush when ≥ 2 MB accumulated
```

## Delete Examples

### Example 1: Delete Entire Stream

```
Delete request: DELETE FROM logs WHERE _stream='{app="old-app"}'

Block decisions:
Stream              Rows       Decision    Reason
----------------------------------------------------------------------
app=old-app         1000       DROP        Stream matches, filterNoop
app=old-app         500        DROP        Stream matches, filterNoop
app=new-app         800        WRITE       Stream doesn't match
app=old-app         200        DROP        Stream matches, filterNoop
app=other           300        WRITE       Stream doesn't match

Result:
  - 3 blocks from 'app=old-app' dropped (1700 rows)
  - 2 blocks from other streams written (1100 rows)
  - No decompression needed (stream-only filter)
```

### Example 2: Delete with Row Filter

```
Delete request: DELETE FROM logs WHERE level='error' AND _msg:contains('timeout')

Block decisions:
  Stream app=api: FILTER (decompress, filter per-row)
  Stream app=api: FILTER (decompress, filter per-row)
  Stream app=web: FILTER (decompress, filter per-row)

Result:
  - All blocks decompressed
  - Per-row filter applied
  - Only rows matching (level=error AND msg:contains('timeout')) dropped
  - Survivors re-blocked and written
```

## Delete Timeline

```
T0: Data ingestion
  Parts: [P1, P2, P3]
  P1 contains rows for stream S1

T1: Delete request received
  DELETE FROM logs WHERE _stream_id=S1
  DeleteTask.StartTime = T1

T2: New rows arrive for S1
  Parts: [P1, P2, P3, P4]
  P4 contains rows for stream S1 (ingested AFTER T1)
  → These rows are PROTECTED (StartTime bound)

T3: Delete-aware merge triggered
  Merge P1, P2, P3 with dropFilter for S1
  dropFilter only matches rows ingested BEFORE T1

T4: Merge completes
  Old parts: [P1, P2, P3]
  New parts: [P5, P4]
  P5 = merged data WITHOUT deleted S1 rows from P1
  P4 = UNTOUCHED (rows ingested after StartTime)

T5: Query sees consistent state
  - P5 contains S1 rows from P4 (after T1)
  - P1, P2, P3 S1 rows are GONE
```

### Key Timing Constraint

DeleteTask.StartTime bounds the delete:
- Rows ingested BEFORE StartTime: eligible for deletion
- Rows ingested AFTER StartTime: PROTECTED

**Why?**
- Without this, newly ingested rows could be deleted
- Race between ingestion and delete
- StartTime ensures clean cut-off

## Space Reclamation Analysis

**Why can delete tasks require post-delete force merge?**

### Scenario:
- Delete request issued at T0
- Merge scheduled, but not immediately executed
- Space NOT reclaimed until merge completes

### Why merge is required for space reclamation:

**1. No in-place deletion**
- Parts are immutable
- Cannot delete rows from existing files
- Must create NEW part without deleted rows

**2. Merge is the only rewrite mechanism**
- Only merge creates new parts
- Only merge can exclude rows
- Old parts replaced atomically

**3. Force merge option**
- API: /delete/force_merge
- Triggers immediate merge of all parts
- Guarantees space reclamation
- But: expensive operation

### Timeline without force merge:
```
T0: Delete request
T1-T10: Normal merge cycles
T11: Parts with deleted data finally merged
→ Space reclaimed at T11, not T0
```

### Timeline with force merge:
```
T0: Delete request
T1: Force merge triggered
T2: All parts merged, deleted rows removed
→ Space reclaimed at T2
```

## Code References

- `lib/logstorage/datadb.go:1714-1738` - `deleteRows` function
- `lib/logstorage/block_stream_merger.go:330-354` - `mustWriteBlockData` with dropFilter
- `lib/logstorage/block_stream_merger.go:382-404` - `mustUnmarshalRows` with row filtering
- `lib/logstorage/block_stream_merger.go:406-409` - `needDropRows` precheck

## Evidence

Lab program `delete_aware_merge.go` demonstrates:
- Delete concept explanation
- Block decision tree
- Stream-level vs row-level delete examples
- Delete timeline with StartTime bound
- Space reclamation analysis

## Conclusions

1. **No tombstones** - Deletion happens through merge, not separate records
2. **Two paths** - Stream-only filter drops blocks; row filter requires decompression
3. **StartTime bound** - Protects rows ingested after delete request
4. **Merge required** - Space reclamation only happens when merge runs

## Open Questions

- How to balance delete latency vs merge cost?
- Should deletes trigger immediate merge for small parts?
