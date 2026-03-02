# Level 2 Checkpoint Answers

## Context
- Level: 2 - Ingestion Skeleton: Rows, Streams, Buffers
- Date: 2026-03-02

## Checkpoint Questions

### 1. Why does stream registration happen before writing row data to `datadb`?

**Answer:**

Stream registration must happen first because **block organization depends on stream identity**. Here's the dependency chain:

```
Row data → Block creation → Block organization by stream
                         ↑
                    Requires streamID to exist
```

**Why this order matters:**

1. **Block organization**: All rows in a block belong to the same stream. The storage engine must know which stream a row belongs to before it can route it to the correct block.

2. **Index consistency**: When queries filter by stream tags (e.g., `{app="nginx"}`), they look up streamIDs in indexdb first. If the stream isn't registered, the query can't find it.

3. **Stream tag storage**: The `mustRegisterStream` function stores:
   - `tenantID:streamID` → existence marker
   - `tenantID:streamID` → stream tags (for tag-based queries)
   - `tenantID:tag:value` → streamID (reverse index for stream discovery)

   Without this registration, queries like `{app="nginx"}` couldn't discover matching streams.

4. **Idempotency**: The registration path handles both new streams (write to indexdb) and existing streams (skip write). Doing this first ensures the stream is known before any data references it.

**Source reference:** `partition.go:347-430` - Phase 1 is stream registration, Phase 2 is data insertion.

### 2. Why does VictoriaLogs maintain both timer-based and threshold-based flushing?

**Answer:**

The two mechanisms solve different problems:

**Threshold-based flush (size trigger):**
```go
// log_rows.go:221-223
func (lr *logRows) needFlush() bool {
    return len(lr.a.b) > (maxUncompressedBlockSize/8)*7  // ~87.5% of 2MB
}
```

- **Purpose:** Optimize for high-throughput workloads
- **When triggers:** Buffer reaches ~1.75MB
- **Benefit:** Creates optimally-sized blocks (~2MB) for compression and query efficiency
- **Without this:** Blocks would be too large (memory pressure) or too small (inefficient I/O)

**Timer-based flush (time trigger):**
```go
// datadb.go:946-952
shard.flushTimer = time.AfterFunc(time.Second, func() {
    shard.mu.Lock()
    shard.flushLocked()
    shard.mu.Unlock()
})
```

- **Purpose:** Bound tail latency for low-throughput workloads
- **When triggers:** 1 second after first row enters empty shard
- **Benefit:** Ensures recent data is visible to queries within 1 second
- **Without this:** Low-volume streams would sit in memory indefinitely, invisible to queries

**Why both are needed:**

| Scenario | Without Size Trigger | Without Time Trigger |
|----------|---------------------|---------------------|
| High throughput | Memory exhaustion | N/A (size triggers) |
| Low throughput | N/A (timer triggers) | Data invisible for hours |
| Burst traffic | Suboptimal block sizes | Inconsistent visibility |

**Real-world example:**

```
Stream A: 10,000 rows/second → Size trigger fires every ~0.2 seconds
Stream B: 5 rows/hour → Time trigger fires every second (mostly empty flushes)
```

Without the timer, Stream B's 5 rows would accumulate for hours before a size trigger.

### 3. Why is sorting fields in rows useful before column construction?

**Answer:**

Sorting fields alphabetically within each row enables multiple downstream optimizations:

**1. Column alignment during row-to-column transformation**

```
Before field sort:
  row 1: {message: "a", level: "info"}
  row 2: {level: "warn", message: "b"}
  row 3: {host: "web01", message: "c", level: "error"}

After field sort:
  row 1: {level: "info", message: "a"}
  row 2: {level: "warn", message: "b"}
  row 3: {host: "web01", level: "error", message: "c"}
```

When converting to columnar format:
- All `level` values are adjacent: ["info", "warn", "error"]
- All `message` values are adjacent: ["a", "b", "c"]
- Enables efficient column encoding (see Level 3)

**2. Compression improvement**

Sorted field names create patterns:
```
Unsorted field names: message, level, host, message, host, level, ...
Sorted field names:    host, level, message, host, level, message, ...
                         ↑ pattern repeats ↑
```

Repeated patterns compress better with LZ4/ZSTD.

**3. Binary search during queries**

When searching for a specific field in a row:
```go
// With sorted fields, can use binary search
sort.Search(len(fields), func(i int) bool {
    return fields[i].Name >= targetField
})
```

This is O(log n) instead of O(n) for unsorted fields.

**4. Deduplication opportunity**

When consecutive rows have the same field names, the arena allocator can reuse strings:
```go
// log_rows.go:277-282
if len(fieldsBuf) >= len(fields) {
    fPrev := &fieldsBuf[len(fieldsBuf)-len(fields)]
    if fPrev.Name == fieldName {
        dstField.Name = fPrev.Name  // Reuse, don't copy
    }
}
```

This only works reliably when fields are in consistent order.

**Source reference:** `log_rows.go:333-338` - `sortFieldsInRows` sorts each row's fields alphabetically.
