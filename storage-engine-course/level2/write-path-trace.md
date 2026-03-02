# Level 2 Write Path Trace

## Context
- Level: 2 - Ingestion Skeleton: Rows, Streams, Buffers
- Date: 2026-03-02

## Complete Write Path (End-to-End)

### Entry Point: `partition.mustAddRows`

```
HTTP Request with log entries
    │
    ▼
Storage.MustAddRows (storage.go)
    │ → Routes to correct day partition using binary search
    ▼
partition.mustAddRows (partition.go:347-430)
```

### Phase 1: Stream Registration

```
partition.mustAddRows
    │
    ├─► First Pass: Cache Check (partition.go:361-374)
    │   │
    │   ├─► For each row in batch:
    │   │   │
    │   │   ├─► hasStreamIDInCache(streamID)
    │   │   │   │
    │   │   │   ├─► HIT  → Skip registration
    │   │   │   └─► MISS → Add to pendingRows
    │   │   │
    │   │   └─► Deduplicate consecutive same streamIDs
    │   │
    │   └─► pendingRows contains indices of rows with unknown streams
    │
    ├─► If pendingRows not empty (partition.go:377-419):
    │   │
    │   ├─► Sort pendingRows by streamID
    │   │   │   WHY: Groups same streamIDs together for batched lookup
    │   │   │
    │   ├─► For each unique streamID in sorted pendingRows:
    │   │   │
    │   │   ├─► Double-check cache (another goroutine may have registered)
    │   │   │   │
    │   │   │   └─► If still not in cache:
    │   │   │       │
    │   │   │       ├─► idb.hasStreamID(streamID) (indexdb.go:306-325)
    │   │   │       │   │
    │   │   │       │   └─► Check mergeset index for stream existence
    │   │   │       │
    │   │   │       ├─► If NOT FOUND:
    │   │   │       │   │
    │   │   │       │   └─► idb.mustRegisterStream(streamID, tags) (indexdb.go:693-734)
    │   │   │       │       │
    │   │   │       │       ├─► Register: tenantID:streamID → existence marker
    │   │   │       │       ├─► Register: tenantID:streamID → streamTags
    │   │   │       │       └─► Register: tenantID:tag → streamID for each tag
    │   │   │       │
    │   │   │       └─► putStreamIDToCache(streamID)
    │   │   │           │
    │   │   │           └─► Add to Storage.streamIDCache for O(1) future lookups
    │   │   │
    │   └─► All streams now registered (or were already)
    │
    └─► Phase 1 Complete: All streams are known
```

### Phase 2: Data Insertion

```
partition.mustAddRows (partition.go:424)
    │
    └─► ddb.mustAddRows(lr) (datadb.go:867-869)
        │
        └─► rb.mustAddRows(lr) (datadb.go:932-962)
            │
            ├─► Round-robin shard selection:
            │   │   idx = nextIdx.Add(1) % numShards
            │   │   shard = &shards[idx]
            │   │
            │   └─► Distributes writes across shards to reduce contention
            │
            ├─► shard.mu.Lock()
            │
            ├─► If shard empty, start 1-second flush timer:
            │   │   shard.flushTimer = time.AfterFunc(time.Second, ...)
            │   │
            │   └─► Ensures low-throughput data doesn't wait forever
            │
            ├─► Append rows to shard.lr (logRows):
            │   │   shard.lr.mustAddRows(lr)
            │   │
            │   └─► Copies field data into arena allocator
            │
            ├─► Check flush condition:
            │   │   if shard.lr.needFlush() (log_rows.go:221-223)
            │   │   │   return len(lr.a.b) > (maxUncompressedBlockSize/8)*7
            │   │   │   // ~87.5% of 2MB = ~1.75MB
            │   │   │
            │   │   └─► If true: shard.flushLocked()
            │   │
            │   └─► Otherwise: keep buffering, timer will flush
            │
            └─► shard.mu.Unlock()
```

### Flush Path

```
shard.flushLocked() (datadb.go:966-979)
    │
    ├─► Stop timer if not yet fired
    │
    └─► If shard.lr not empty:
        │
        └─► flushFunc(shard.lr) → ddb.mustFlushLogRows(lr) (datadb.go:982-996)
            │
            ├─► Sort rows by (streamID, timestamp):
            │   │   sort.Sort(lr) using logRows.Less (log_rows.go:309-316)
            │   │
            │   └─► Groups rows by stream, orders chronologically
            │
            ├─► Sort fields within each row:
            │   │   lr.sortFieldsInRows() (log_rows.go:333-338)
            │   │
            │   └─► Alphabetical by field name
            │
            ├─► Create in-memory part:
            │   │   mp.mustInitFromRows(lr)
            │   │
            │   └─► Converts sorted rows into compressed blocks
            │
            └─► Add to inmemoryParts list:
                │   ddb.inmemoryParts = append(ddb.inmemoryParts, pw)
                │
                └─► Later merged to disk by background process
```

## State Transitions for a Single Row

```
1. HTTP Request
   State: Raw JSON/logfmt bytes
   
2. Parser (vlinsert/*)
   State: LogRows struct with streamIDs, timestamps, fields
   
3. partition.mustAddRows
   State: Stream registration complete
   
4. ddb.mustAddRows
   State: Added to shard's logRows buffer
   
5. Timer OR size threshold triggers flush
   State: Row in shard being flushed
   
6. sort.Sort(logRows)
   State: Row sorted by (streamID, timestamp)
   
7. sortFieldsInRows
   State: Row's fields sorted alphabetically
   
8. mp.mustInitFromRows
   State: Row encoded into compressed block
   
9. In-memory part
   State: Row visible to queries (via in-memory part)
   
10. Background merge to disk
    State: Row in persistent on-disk part
```

## Evidence

Lab program `write_path_trace.go` demonstrates:
- Stream cache hit/miss paths
- Deduplication of stream lookups in batches
- Buffering in sharded rowsBuffer
- Flush triggers (size-based)

## Conclusions

1. **Stream registration is idempotent**: Multiple rows with same streamID only cause one registration
2. **Two-tier caching works**: Cache handles hot paths, indexdb for cold starts
3. **Buffering amortizes I/O**: Many small writes become fewer large writes
4. **Sorting enables block organization**: Blocks are stream-contiguous and time-ordered

## Open Questions

- How does the background merge process work?
- What happens when a partition is deleted during active ingestion?
