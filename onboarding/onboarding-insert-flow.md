# VictoriaLogs Data Ingestion Flow - Developer Onboarding Guide

This document provides a comprehensive overview of how log data flows through VictoriaLogs from HTTP ingestion endpoints to persistent storage on disk.

## Table of Contents

- [Overview](#overview)
- [Complete Data Flow](#complete-data-flow)
- [Architecture Layers](#architecture-layers)
  - [1. HTTP Endpoint Layer](#1-http-endpoint-layer)
  - [2. Common Parameters Extraction](#2-common-parameters-extraction)
  - [3. Log Message Processor](#3-log-message-processor)
  - [4. In-Memory Batching](#4-in-memory-batching)
  - [5. Storage Interface](#5-storage-interface)
  - [6. Storage Router](#6-storage-router)
  - [7. Day-Based Partitioning (logstorage.Storage)](#7-day-based-partitioning-logstoragestorage)
  - [8. Partition Layer](#8-partition-layer)
- [Deployment Modes](#deployment-modes)
  - [Local Storage Mode](#local-storage-mode)
  - [Distributed Storage Mode](#distributed-storage-mode)
- [Internal Insert Endpoint](#internal-insert-endpoint)
- [Key Design Patterns](#key-design-patterns)
- [Configuration Flags](#configuration-flags)

---

## Overview

VictoriaLogs ingests log data through multiple HTTP endpoints (e.g., `/insert/native`, `/insert/jsonline`, `/insert/elasticsearch/*`, etc.). This document focuses on the `/insert/native` endpoint as the primary example, as it demonstrates the core ingestion flow that other endpoints follow.

### What is "Native Insert"?

"Native" refers to VictoriaLogs' own internal binary protocol — as opposed to standard text/JSON formats from other ecosystems. When a client sends data to `/insert/native`, it sends pre-serialized `InsertRow` objects in VictoriaLogs' binary encoding. The server decodes these rows and applies limited common ingestion options (for example, tenant override from headers, ignore/extra/debug handling). It does not perform JSON parsing or timestamp-format detection.

### How Does It Differ from Other Formats?

The other ingestion endpoints accept **industry-standard formats** that require parsing and transformation before storage:

| Endpoint | Client sends | Parsing work |
|----------|-------------|-------------|
| `/insert/native` | VictoriaLogs binary (`InsertRow` objects) | Binary decode + limited common-params processing (e.g., tenant override, ignore/extra/debug); no JSON parsing |
| `/insert/jsonline` | Newline-delimited JSON (`{"msg":"foo","ts":"..."}`) | JSON parsing per line, field extraction, timestamp detection |
| `/insert/elasticsearch/_bulk` | Elasticsearch bulk format (alternating command + data JSON lines) | JSON parsing, Elasticsearch timestamp format handling (ISO8601, Unix ms/s) |
| `/insert/loki/api/v1/push` | Loki protobuf or JSON (Grafana's push format) | Protobuf/JSON decoding, stream label extraction |
| `/insert/opentelemetry/v1/logs` | OTLP protobuf (OpenTelemetry standard) | Protobuf decoding, resource/scope attribute extraction |
| `/insert/datadog/api/v2/logs` | Datadog JSON array | JSON parsing, nested Lambda format detection |
| Syslog (TCP/UDP socket) | RFC 3164/5424 syslog | Text parsing, priority/facility extraction, timezone handling |
| `/insert/journald/upload` | systemd journal binary export | Binary format parsing, field extraction |

All formats eventually converge on the same storage path. Most text protocols go through field/timestamp extraction and then `AddRow()`. Native/internal protocols decode pre-marshaled `InsertRow` objects and use `AddInsertRow()` instead.

**Who uses native insert?** Primarily `vlagent` (VictoriaLogs' own log collection agent). Cluster communication uses `/internal/insert`, which shares the same binary row format but has a different trust/parameter model.

### Deployment Modes

The system supports two deployment modes:
- **Local Mode**: Single node storing data on local disk
- **Distributed Mode**: Multiple nodes with data distributed across a cluster

---

## Complete Data Flow

**Key Files**:
- [`app/vlinsert/nativeinsert/nativeinsert.go`](../app/vlinsert/nativeinsert/nativeinsert.go#L28) - Native insert endpoint
- [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L49) - Common parameters and LogMessageProcessor
- [`app/vlstorage/main.go`](../app/vlstorage/main.go#L109) - Storage router
- [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L375) - Distributed storage network layer
- [`app/vlinsert/internalinsert/internalinsert.go`](../app/vlinsert/internalinsert/internalinsert.go#L24) - Internal insert endpoint
- [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L1139) - Local storage implementation
- [`lib/logstorage/partition.go`](../lib/logstorage/partition.go#L135) - Partition management

```
HTTP Request (POST /insert/native)
    ↓
nativeinsert.RequestHandler               [nativeinsert.go:28](../app/vlinsert/nativeinsert/nativeinsert.go#L28)
    ↓
Common Parameters Extraction              [common_params.go:49](../app/vlinsert/insertutil/common_params.go#L49)
    ↓
LogMessageProcessor Creation              [common_params.go:348](../app/vlinsert/insertutil/common_params.go#L348)
    ↓
Parse & Add Rows                          [nativeinsert.go:94](../app/vlinsert/nativeinsert/nativeinsert.go#L94)
    ↓
In-Memory Buffer Accumulation             [common_params.go:290](../app/vlinsert/insertutil/common_params.go#L290)
    ↓
Flush Trigger (buffer full / request end) [common_params.go:320](../app/vlinsert/insertutil/common_params.go#L320)
    ↓
logRowsStorage.MustAddRows(lr)            [common_params.go:323](../app/vlinsert/insertutil/common_params.go#L323)
    ↓
vlstorage.Storage.MustAddRows(lr)         [main.go:543](../app/vlstorage/main.go#L543)
    ↓
    ├─→ [LOCAL MODE]
    │   localStorage.MustAddRows(lr)      [storage.go:1139](../lib/logstorage/storage.go#L1139)
    │       ↓
    │   Partition by Day                  [storage.go:1204](../lib/logstorage/storage.go#L1204)
    │       ↓
    │   partition.mustAddRows(lr)         [partition.go:135](../lib/logstorage/partition.go#L135)
    │       ↓
    │   indexdb + datadb writes           [partition.go:162](../lib/logstorage/partition.go#L162)
    │       ↓
    │   DISK: victoria-logs-data/partitions/YYYYMMDD/{indexdb,datadb}
    │
    └─→ [DISTRIBUTED MODE]
        lr.ForEachRow(netstorageInsert.AddRow)  [main.go:549](../app/vlstorage/main.go#L549)
            ↓
        Adaptive Stream Routing            [netinsert.go:376](../app/vlstorage/netinsert/netinsert.go#L376)
            ↓
        Marshal & Batch (2MB blocks)      [netinsert.go:157](../app/vlstorage/netinsert/netinsert.go#L157)
            ↓
        Compress (zstd)                   [netinsert.go:241](../app/vlstorage/netinsert/netinsert.go#L241)
            ↓
        POST http://storageNode/internal/insert?version=v1
            ↓
        [See "Internal Insert Endpoint" section]
```

---

## Architecture Layers

### 1. HTTP Endpoint Layer

**File**: [`app/vlinsert/nativeinsert/nativeinsert.go`](../app/vlinsert/nativeinsert/nativeinsert.go#L28)

The `/insert/native` endpoint is the entry point for log ingestion using VictoriaLogs' native protocol.

**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlinsert/nativeinsert/nativeinsert.go#L28) - Main HTTP handler
- [`parseData(irp, data, tenantID)`](../app/vlinsert/nativeinsert/nativeinsert.go#L94) - Parse and add rows

```go
func RequestHandler(w http.ResponseWriter, r *http.Request) {
    startTime := time.Now()

    // Validate HTTP method
    if r.Method != "POST" {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    // Verify protocol version
    version := r.FormValue("version")
    if version != netinsert.ProtocolVersion {
        httpserver.Errorf(w, r, "unsupported protocol version=%q; want %q",
            version, netinsert.ProtocolVersion)
        return
    }

    // ... continue processing
}
```

**Key Responsibilities**:
- Validate HTTP method (must be POST)
- Verify protocol version
- Extract request encoding (Content-Encoding header)
- Read and decompress request body

**Location**: [`app/vlinsert/nativeinsert/nativeinsert.go:28`](../app/vlinsert/nativeinsert/nativeinsert.go#L28)

---

### 2. Common Parameters Extraction

**File**: [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L49)

Extracts ingestion parameters from HTTP headers and query arguments.

**Key Functions**:
- [`GetCommonParams(r)`](../app/vlinsert/insertutil/common_params.go#L49) - Extract parameters from HTTP request
- [`getArray(r, argKey, headerKey)`](../app/vlinsert/insertutil/common_params.go#L128) - Get array values from query/header
- [`getExtraFields(r)`](../app/vlinsert/insertutil/common_params.go#L108) - Parse extra_fields parameter

```go
type CommonParams struct {
    TenantID         logstorage.TenantID  // From AccountID/ProjectID headers
    TimeFields       []string             // Fields to parse as timestamp
    MsgFields        []string             // Fields to treat as message
    StreamFields     []string             // Fields defining log stream
    IgnoreFields     []string             // Fields to ignore
    DecolorizeFields []string             // Fields to strip ANSI colors from
    ExtraFields      []logstorage.Field   // Additional fields to add
    Debug            bool                 // Debug mode flag
}
```

The struct shown above is a focused subset for ingestion flow explanation. The actual `CommonParams` in code also includes `PreserveJSONKeys`, `IsTimeFieldSet`, `DebugRequestURI`, and `DebugRemoteAddr`.

**HTTP Parameters Mapping**:
- Headers `AccountID` / `ProjectID` → `TenantID`
- Query arg `_time_field` or Header `VL-Time-Field` → `TimeFields`
- Query arg `_msg_field` or Header `VL-Msg-Field` → `MsgFields`
- Query arg `_stream_fields` or Header `VL-Stream-Fields` → `StreamFields`
- Query arg `ignore_fields` or Header `VL-Ignore-Fields` → `IgnoreFields`
- Query arg `decolorize_fields` or Header `VL-Decolorize-Fields` → `DecolorizeFields`
- Query arg `preserve_json_keys` or Header `VL-Preserve-JSON-Keys` → `PreserveJSONKeys`
- Query arg `extra_fields` or Header `VL-Extra-Fields` → `ExtraFields`
- Query arg `debug` or Header `VL-Debug` → `Debug`

**Location**: [`app/vlinsert/insertutil/common_params.go:49-106`](../app/vlinsert/insertutil/common_params.go#L49)

---

### 3. Log Message Processor

**File**: [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L210)

The `logMessageProcessor` is the concurrency-safe orchestration layer that wraps a `*logstorage.LogRows` buffer. It is responsible for:
- Thread-safe accumulation of log rows into the in-memory `LogRows` buffer
- Deciding **when** to flush (buffer full, periodic timer, or request end)
- Tracking ingestion metrics (rows count, bytes count, flush duration)
- Enforcing per-row limits (max fields) and debug mode

#### `logMessageProcessor` vs `*logstorage.LogRows` — why both?

These two types serve different layers:

| | `logMessageProcessor` | `*logstorage.LogRows` |
|---|---|---|
| **Layer** | Ingestion orchestration (`app/vlinsert/`) | Storage data structure (`lib/logstorage/`) |
| **Responsibility** | Mutex locking, flush scheduling, metrics, debug mode, field-count limits | Holding raw row data (fields, timestamps, stream IDs) in memory with arena allocation |
| **Analogy** | A batching writer that decides *when* to flush | The batch buffer itself that holds *what* to flush |
| **Lifecycle** | Created per HTTP request (or per stream connection), closed via `MustClose()` | Obtained from a `sync.Pool` via `GetLogRows()`, returned via `PutLogRows()` |
| **Concurrency** | Thread-safe (mutex-protected) | NOT thread-safe — relies on the processor's mutex |

In short: `logMessageProcessor` owns a `LogRows` and adds concurrency control, batching policy, and metrics on top of it. `LogRows` is a dumb buffer that knows how to store rows efficiently but has no flushing or concurrency logic.

**Key Functions**:
- [`NewLogMessageProcessor(protocolName, isStreamMode)`](../app/vlinsert/insertutil/common_params.go#L348) - Create new processor
- [`AddRow(timestamp, fields, streamFieldsLen)`](../app/vlinsert/insertutil/common_params.go#L254) - Add a parsed log row to the in-memory `LogRows` buffer (`lmp.lr`)
- [`AddInsertRow(r)`](../app/vlinsert/insertutil/common_params.go#L290) - Add a pre-marshaled `InsertRow` to the in-memory `LogRows` buffer (`lmp.lr`)
- [`MustClose()`](../app/vlinsert/insertutil/common_params.go#L335) - Flush remaining buffered rows to storage and release resources
- [`flushLocked()`](../app/vlinsert/insertutil/common_params.go#L320) - Send buffered `LogRows` to `logRowsStorage.MustAddRows()`, then reset the buffer

```go
type logMessageProcessor struct {
    mu            sync.Mutex       // Protects all fields below; LogRows is not thread-safe on its own
    wg            sync.WaitGroup   // Tracks the background periodic-flush goroutine (stream mode only)
    stopCh        chan struct{}     // Signals the periodic-flush goroutine to stop
    lastFlushTime time.Time        // When the last flush happened; used by periodic flush to avoid redundant flushes

    cp *CommonParams               // Ingestion parameters (tenant, stream fields, debug flags, etc.)
    lr *logstorage.LogRows         // The actual in-memory row buffer; obtained from sync.Pool via GetLogRows()

    rowsIngestedTotal  *metrics.Counter  // Prometheus counter: total rows flushed to storage
    bytesIngestedTotal *metrics.Counter  // Prometheus counter: total estimated bytes flushed
    flushDuration      *metrics.Summary  // Prometheus summary: time spent in each flush call

    unflushedRows  int             // Rows added since last flush (for metrics; reset on flush)
    unflushedBytes int             // Estimated bytes added since last flush (for metrics; reset on flush)
}
```

**Creation**:
```go
lmp := cp.NewLogMessageProcessor("nativeinsert", false)
```

**Location**: [`app/vlinsert/insertutil/common_params.go:210-226`](../app/vlinsert/insertutil/common_params.go#L210)

---

### 4. In-Memory Batching

**File**: [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L290)

Rows are accumulated in memory to reduce disk I/O. Flushing occurs when:
1. Buffer is full (`lmp.lr.NeedFlush()` returns true)
2. Periodic timer (every ~1 second in stream mode — see below)
3. Request completion (`MustClose()` called)

#### What is "stream mode"?

The `isStreamMode` parameter passed to [`NewLogMessageProcessor`](../app/vlinsert/insertutil/common_params.go#L348) distinguishes two connection patterns:

| | Stream mode (`true`) | Non-stream mode (`false`) |
|---|---|---|
| **Connection lifetime** | Long-lived — stays open indefinitely | Short-lived — one HTTP request/response cycle |
| **When data arrives** | Continuously, possibly with idle gaps | All at once in the request body |
| **Natural flush point** | None — connection may stay open for hours | `MustClose()` at end of HTTP handler |
| **Periodic flush needed?** | Yes — otherwise rows could sit in buffer indefinitely during idle periods | No — `MustClose()` guarantees a flush when the request ends |

**Protocols by mode**:
- **Stream mode**: `syslog_tcp`, `syslog_udp`, `syslog_unix`, `journald`, `jsonline`, `elasticsearch_bulk`
- **Non-stream mode**: `loki_json`, `loki_protobuf`, `datadog`, `opentelemetry_protobuf`, `nativeinsert`, `internalinsert`

When stream mode is enabled, [`initPeriodicFlush()`](../app/vlinsert/insertutil/common_params.go#L227) spawns a background goroutine with a ~1-second ticker. On each tick, if at least 1 second has elapsed since the last flush, it calls `flushLocked()`. This bounds the maximum latency for data to reach storage, even when logs trickle in slowly.

#### How the buffer works internally

The in-memory buffer is [`*logstorage.LogRows`](../lib/logstorage/log_rows.go#L21) — a pool-allocated data structure from `lib/logstorage/`. Its internal layout:

```
LogRows
├── a (arena)                   // Contiguous byte slice for all string data (field names, values, stream tags).
│                                // Grows as rows are added; reset to [:0] on flush (memory reused, not freed).
├── fieldsBuf []Field           // Flat buffer of all Field structs across all rows.
├── rows [][]Field              // Each element is a sub-slice of fieldsBuf for one log entry's fields.
├── streamIDs []streamID        // One per row: tenantID + 128-bit hash of stream tags.
├── timestamps []int64          // One per row.
├── streamTagsCanonicals []string  // One per row: canonical serialized stream tags.
└── (settings)                  // streamFields, ignoreFields, decolorizeFields, extraFields, defaultMsgValue
                                 // — preserved across flushes via ResetKeepSettings().
```

**Flush threshold** ([`NeedFlush()`](../lib/logstorage/log_rows.go#L298)):
```go
func (lr *LogRows) NeedFlush() bool {
    return len(lr.a.b) > (maxUncompressedBlockSize/8)*7 || len(lr.rows) > maxUncompressedBlockSize/100
}
```
Where `maxUncompressedBlockSize` = 2 MB. This means flush when **either**:
- Arena exceeds ~1.75 MB of accumulated byte data, **or**
- Row count exceeds ~20,971 entries

After flush, [`ResetKeepSettings()`](../lib/logstorage/log_rows.go#L272) truncates all data slices to `[:0]` (reusing allocated capacity) while preserving field configuration.

#### Ownership flow: who owns what?

The relationship between the three participants is unintuitive at first glance, so here it is explicitly:

```
logMessageProcessor (app/vlinsert/insertutil/)
│
│  owns ──→  *logstorage.LogRows          ← the buffer (lib/logstorage/)
│              created via GetLogRows()     ← obtained from sync.Pool
│              returned via PutLogRows()    ← returned to pool in MustClose()
│
│  calls ──→  logRowsStorage.MustAddRows(lmp.lr)   ← the flush target
│              ↑
│              a package-level singleton (LogRowsStorage interface)
│              injected at startup via SetLogRowsStorage(&vlstorage.Storage{})
```

- **`logMessageProcessor`** (in `app/vlinsert/`) owns and manages a `LogRows` instance. It decides *when* to flush.
- **`LogRows`** (in `lib/logstorage/`) is the buffer. It holds row data and knows its own capacity limits (`NeedFlush`), but has no idea where to send data.
- **`logRowsStorage`** (the `LogRowsStorage` interface singleton) is the flush target. The processor passes its `LogRows` pointer directly to `logRowsStorage.MustAddRows(lmp.lr)`, which reads the rows out of the buffer and writes them to storage.

Note that `LogRows` is defined in `lib/logstorage/` (the storage library), but it is *owned by* `logMessageProcessor` in `app/vlinsert/` (the ingestion layer). This is because `LogRows` is a data-transfer structure — it's designed to be filled by ingestion code and consumed by storage code. The storage package defines it because it knows the internal format needed for writing.

```go
func (lmp *logMessageProcessor) AddInsertRow(r *logstorage.InsertRow) {
    lmp.mu.Lock()
    defer lmp.mu.Unlock()

    lmp.unflushedRows++
    n := logstorage.EstimatedJSONRowLen(r.Fields)
    lmp.unflushedBytes += n

    // Add to the LogRows buffer
    lmp.lr.MustAddInsertRow(r)

    // Flush the buffer to storage if it has accumulated enough data
    if lmp.lr.NeedFlush() {
        lmp.flushLocked()
    }
}
```

**Location**: [`app/vlinsert/insertutil/common_params.go:290-317`](../app/vlinsert/insertutil/common_params.go#L290)

---

### 5. Storage Interface

**File**: [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L174)

The `LogRowsStorage` interface provides a clean abstraction between the ingestion layer and storage layer.

**Key Functions**:
- [`SetLogRowsStorage(storage)`](../app/vlinsert/insertutil/common_params.go#L188) - Inject storage implementation
- [`CanWriteData()`](../app/vlinsert/insertutil/common_params.go#L193) - Check if storage can accept writes

```go
// LogRowsStorage is an interface for ingesting logs into the storage.
type LogRowsStorage interface {
    // MustAddRows must add lr to the underlying storage.
    MustAddRows(lr *logstorage.LogRows)

    // CanWriteData returns non-nil error if logs cannot be added.
    CanWriteData() error
}

var logRowsStorage LogRowsStorage  // Package-level singleton
```

**Dependency Injection at Startup**:

**File**: [`app/victoria-logs/main.go`](../app/victoria-logs/main.go#L46) (lines 46-50):
```go
func main() {
    vlstorage.Init()                                    // Initialize storage
    vlselect.Init()
    insertutil.SetLogRowsStorage(&vlstorage.Storage{}) // Inject implementation
    vlinsert.Init()                                    // Initialize endpoints
}
```

**Flush Implementation**:

```go
func (lmp *logMessageProcessor) flushLocked() {
    start := time.Now()
    lmp.lastFlushTime = start

    // THE HANDOFF POINT: Interface call
    logRowsStorage.MustAddRows(lmp.lr)

    lmp.lr.ResetKeepSettings()
    lmp.flushDuration.UpdateDuration(start)
    lmp.rowsIngestedTotal.Add(lmp.unflushedRows)
    lmp.bytesIngestedTotal.Add(lmp.unflushedBytes)

    lmp.unflushedRows = 0
    lmp.unflushedBytes = 0
}
```

**Location**: [`app/vlinsert/insertutil/common_params.go:174-195, 320-332`](../app/vlinsert/insertutil/common_params.go#L174)

---

### 6. Storage Router

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L109)

The `vlstorage.Storage` struct implements the `LogRowsStorage` interface and routes data to either local or distributed storage. This is a thin routing layer — it does not process data itself.

**Key Functions**:
- [`Init()`](../app/vlstorage/main.go#L109) - Initialize storage mode
- [`initLocalStorage()`](../app/vlstorage/main.go#L117) - Initialize local storage
- [`initNetworkStorage()`](../app/vlstorage/main.go#L164) - Initialize distributed storage
- [`Storage.MustAddRows(lr)`](../app/vlstorage/main.go#L543) - Route data to storage
- [`Storage.CanWriteData()`](../app/vlstorage/main.go#L524) - Check write readiness

**Important naming clarification**: There are **two different types both named `Storage`** in the call chain:
- `vlstorage.Storage` (this section) — an empty struct in `app/vlstorage/` that acts as a router. It has no fields.
- `logstorage.Storage` (next section) — the actual storage engine in `lib/logstorage/` that manages partitions and disk I/O.

The router delegates to the engine: `vlstorage.Storage.MustAddRows()` → `localStorage.MustAddRows()` (which is `*logstorage.Storage`).

```go
// vlstorage.Storage implements insertutil.LogRowsStorage interface.
// It is an empty struct — just a namespace for the routing methods.
type Storage struct{}

func (*Storage) MustAddRows(lr *logstorage.LogRows) {
    if localStorage != nil {
        // LOCAL MODE: delegate to logstorage.Storage (the actual storage engine)
        localStorage.MustAddRows(lr)
    } else {
        // DISTRIBUTED MODE: fan out rows to remote storage nodes
        lr.ForEachRow(netstorageInsert.AddRow)
    }
}

func (*Storage) CanWriteData() error {
    if localStorage == nil {
        // Distributed mode - data can always be written
        return nil
    }

    if localStorage.IsReadOnly() {
        return &httpserver.ErrorWithStatusCode{
            Err: fmt.Errorf("cannot add rows into storage in read-only mode"),
            StatusCode: http.StatusTooManyRequests,
        }
    }
    return nil
}
```

**Mode Selection**:

```go
var localStorage *logstorage.Storage       // non-nil in local mode
var netstorageInsert *netinsert.Storage    // non-nil in distributed mode

func Init() {
    if len(*storageNodeAddrs) == 0 {
        initLocalStorage()     // No -storageNode flag → Local mode
    } else {
        initNetworkStorage()   // -storageNode flag present → Distributed mode
    }
}
```

**Location**: [`app/vlstorage/main.go:99-115, 520-551`](../app/vlstorage/main.go#L99)

---

### 7. Day-Based Partitioning (logstorage.Storage)

**File**: [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L1139)

This is where `localStorage.MustAddRows(lr)` lands in local mode. The `logstorage.Storage` engine is responsible for:
- Splitting incoming rows by day (each day gets its own partition)
- Validating timestamps against retention bounds
- Managing partition lifecycle (creation, lookup, reference counting)
- Optimizing the common case where all rows belong to the same day ("hot partition")

**Key Functions**:
- `MustOpenStorage(path, cfg)` - Open storage (called during initialization)
- [`MustAddRows(lr)`](../lib/logstorage/storage.go#L1139) - Split rows by day and dispatch to partitions
- [`getPartitionForWriting(day)`](../lib/logstorage/storage.go#L1244) - Look up or create partition for a given day
- `IsReadOnly()` - Check if storage is read-only
- `MustClose()` - Close storage

#### How rows get from `logstorage.Storage` to `partition`

The connection between the storage engine and partitions is **not direct** — it goes through a `partitionWrapper`:

```
logstorage.Storage
│
│  s.partitions []*partitionWrapper      ← sorted slice, one per day
│       │
│       ├── partitionWrapper { day: 20260211, pt: *partition, refCount: ... }
│       ├── partitionWrapper { day: 20260212, pt: *partition, refCount: ... }  ← s.ptwHot
│       └── ...
│
│  s.ptwHot *partitionWrapper            ← cached pointer to most recently used partition
```

**`partitionWrapper`** ([`storage.go:538`](../lib/logstorage/storage.go#L538)) is a reference-counted handle around `*partition`:

```go
type partitionWrapper struct {
    refCount atomic.Int32    // Active readers/writers; partition closes when this reaches zero
    mustDrop atomic.Bool     // If true, delete partition directory after close
    day      int64           // Day number (unix timestamp / nsecsPerDay)
    pt       *partition      // The actual partition
    doneCh   chan struct{}   // Closed when refCount reaches zero
}
```

Reference counting ensures a partition is not closed while a concurrent write or read is using it. Callers `incRef()` before using a partition and `decRef()` when done.

#### Fast path vs slow path

```go
func (s *Storage) MustAddRows(lr *LogRows) {
    // ── FAST PATH ──────────────────────────────────────────────
    // Most batches contain rows from a single day (e.g., "now").
    // If the hot partition can accept ALL rows, skip day-splitting entirely.
    s.partitionsLock.Lock()
    ptwHot := s.ptwHot
    if ptwHot != nil {
        ptwHot.incRef()
    }
    s.partitionsLock.Unlock()

    if ptwHot != nil {
        if ptwHot.canAddAllRows(lr) {       // Do all timestamps fall within this day?
            ptwHot.pt.mustAddRows(lr)        // Yes → write directly to partition
            ptwHot.decRef()
            return
        }
        ptwHot.decRef()
    }

    // ── SLOW PATH ──────────────────────────────────────────────
    // Rows span multiple days, or there is no hot partition yet.
    // Split rows by day into separate LogRows, then dispatch each.
    now := time.Now().UnixNano()
    minAllowedDay := s.getMinAllowedDay(now)    // Based on -retentionPeriod
    maxAllowedDay := s.getMaxAllowedDay(now)    // Based on -futureRetention

    m := make(map[int64]*LogRows)               // day → rows for that day
    for i, ts := range lr.timestamps {
        day := ts / nsecsPerDay                 // nsecsPerDay = 86400 * 1e9

        if day < minAllowedDay {                // Too old → drop
            s.rowsDroppedTooSmallTimestamp.Add(1)
            continue
        }
        if day > maxAllowedDay {                // Too far in future → drop
            s.rowsDroppedTooBigTimestamp.Add(1)
            continue
        }

        lrPart := m[day]
        if lrPart == nil {
            lrPart = GetLogRows(nil, nil, nil, nil, "")
            m[day] = lrPart
        }
        lrPart.mustAddInternal(lr.streamIDs[i], ts, lr.rows[i], lr.streamTagsCanonicals[i])
    }

    for day, lrPart := range m {
        ptw := s.getPartitionForWriting(day)    // Binary search + create if missing
        if ptw != nil {
            ptw.pt.mustAddRows(lrPart)           // Dispatch to partition
            ptw.decRef()
        }
        PutLogRows(lrPart)                       // Return temporary LogRows to pool
    }
}
```

**`canAddAllRows`** ([`storage.go:594`](../lib/logstorage/storage.go#L594)) checks if every timestamp in the batch falls within the hot partition's day boundary (`[day * nsecsPerDay, (day+1) * nsecsPerDay - 1]`).

#### Partition lookup and creation

**`getPartitionForWriting(day)`** ([`storage.go:1244`](../lib/logstorage/storage.go#L1244)):
1. **Binary search** `s.partitions` (sorted by day) for the requested day
2. **If found**: increment ref count and return
3. **If missing**: check if it was previously deleted or detached → return `nil` (rows dropped)
4. **Otherwise**: create the partition on demand (`mustCreatePartition` + `mustOpenPartition`), insert into the sorted slice, and return
5. **Always updates `s.ptwHot`** to the returned partition (so the next call hits the fast path)

**Storage Configuration**:
```go
type StorageConfig struct {
    Retention              time.Duration  // Default: 7 days
    FlushInterval          time.Duration  // Default: 5 seconds
    FutureRetention        time.Duration  // Default: 2 days
    MaxBackfillAge         time.Duration  // Default: 0 (disabled)
    MinFreeDiskSpaceBytes  int64          // Minimum free space
    MaxDiskSpaceUsageBytes int64          // Maximum total usage
    MaxDiskUsagePercent    int            // Maximum usage percentage
}
```

The struct above is intentionally trimmed to fields most relevant to this insert-path walkthrough. The actual `StorageConfig` also contains `DefaultParallelReaders`, `SnapshotsMaxAge`, `LogNewStreams`, and `LogIngestedRows`.

Important behavior detail: although the flag default for `-maxBackfillAge` is `0`, storage normalizes non-positive values to `retention`, so the effective default is "bounded by retention", not "disabled".

**Location**: [`lib/logstorage/storage.go:1139-1293`](../lib/logstorage/storage.go#L1139)

---

### 8. Partition Layer

**File**: [`lib/logstorage/partition.go`](../lib/logstorage/partition.go#L74)

A partition holds all log data for a single day. It has two sub-databases:
- **indexdb**: Maps stream IDs to stream tag metadata. Used during queries to find which streams match a filter.
- **datadb**: Stores the actual log rows in a columnar format.

**Key Functions**:
- [`mustOpenPartition(s, path)`](../lib/logstorage/partition.go#L74) - Open existing partition from disk
- [`mustCreatePartition(path)`](../lib/logstorage/partition.go#L52) - Create new partition directory
- [`mustAddRows(lr)`](../lib/logstorage/partition.go#L135) - Register streams + write rows
- [`mustClosePartition(pt)`](../lib/logstorage/partition.go#L121) - Close partition

```go
type partition struct {
    s    *Storage        // Parent storage (for accessing shared config like logNewStreams)
    path string          // Absolute path to partition directory on disk
    name string          // Partition name (YYYYMMDD format)
    idb  *indexdb        // Stream index — maps streamID → stream tag metadata
    ddb  *datadb         // Data storage — log rows in columnar format
}
```

#### What `mustAddRows` does

By the time rows reach `partition.mustAddRows()`, they have already been split by day (section 7), so all rows in the batch belong to this partition's day. The method does two things:

```go
func (pt *partition) mustAddRows(lr *LogRows) {
    // ── PHASE 1: Stream registration ─────────────────────────
    // Identify streamIDs that are not yet known to this partition's indexdb.
    // Uses a fast in-memory cache (pt.hasStreamIDInCache) to avoid hitting
    // indexdb for every row. Only truly new streams get registered.
    var pendingRows []int
    streamIDs := lr.streamIDs
    for i := range lr.timestamps {
        streamID := &streamIDs[i]
        if pt.hasStreamIDInCache(streamID) {
            continue
        }
        if len(pendingRows) == 0 || !streamIDs[pendingRows[len(pendingRows)-1]].equal(streamID) {
            pendingRows = append(pendingRows, i)
        }
    }
    if len(pendingRows) > 0 {
        // Sort and deduplicate, then register each new stream in indexdb
        for _, rowIdx := range pendingRows {
            streamID := &streamIDs[rowIdx]
            if !pt.idb.hasStreamID(streamID) {
                pt.idb.mustRegisterStream(streamID, lr.streamTagsCanonicals[rowIdx])
            }
            pt.putStreamIDToCache(streamID)
        }
    }

    // ── PHASE 2: Data storage ────────────────────────────────
    // Write all rows to the data database (columnar on-disk format).
    pt.ddb.mustAddRows(lr)
}
```

**Phase 1 (stream registration)** ensures the indexdb knows about every log stream before data is written. A stream is identified by its `streamID` (tenant + 128-bit hash of stream tag values). The in-memory cache (`hasStreamIDInCache`) makes this cheap for streams that have already been seen in this partition.

**Phase 2 (data storage)** writes the actual log row data to `datadb`, which manages the columnar on-disk format.

#### What happens next? (Handoff to the Storage Engine)

This is where the insert flow document ends and the **storage engine** takes over. The call `pt.ddb.mustAddRows(lr)` enters the LSM-tree pipeline inside `datadb`:

```
pt.ddb.mustAddRows(lr)                          ← YOU ARE HERE (end of insert flow)
    ↓
rowsBuffer (sharded per-CPU lock-free buffer)    ← Amortizes locking cost
    ↓  flush (buffer full or 1-second timer)
inmemoryPart.mustInitFromRows(lr)                ← Sort by stream+time, encode into columnar blocks
    ↓                                               DATA IS NOW QUERYABLE
mustOpenInmemoryPart(pt, mp)                     ← Wrap as searchable part
    ↓
ddb.inmemoryParts list                           ← Added to in-memory tier
    ↓  background merge workers
Small file parts → Big file parts                ← Three-tier LSM compaction
    ↓
DISK: partitions/YYYYMMDD/datadb/<mergeIdx>/     ← Final resting place
```

Similarly, `pt.idb.mustRegisterStream()` writes stream metadata into the `indexdb`, which is backed by VictoriaMetrics' `mergeset` library (a sorted key-value store with its own merge/compaction).

**For the full details of everything below `ddb.mustAddRows()`** — the sharded buffer, in-memory parts, block encoding, column compression, bloom filters, merge/compaction, and on-disk file format — see:

> **[Storage Engine & On-Disk Format Guide](./onboarding-storage-engine.md)** — sections [3. DataDB Layer](./onboarding-storage-engine.md#3-datadb-layer) through [10. IndexDB](./onboarding-storage-engine.md#10-indexdb-stream-metadata)

**Partition Directory Structure**:
```
victoria-logs-data/
└── partitions/
    ├── 20260211/      # Partition for Feb 11, 2026
    │   ├── indexdb/   # Stream indexes (streamID → stream tag metadata)
    │   └── datadb/    # Log data (columnar format: timestamps, fields, etc.)
    ├── 20260212/      # Partition for Feb 12, 2026
    │   ├── indexdb/
    │   └── datadb/
    └── ...
```

**Location**: [`lib/logstorage/partition.go:23-178`](../lib/logstorage/partition.go#L23)

---

## Deployment Modes

### Local Storage Mode

**Activation**: Start VictoriaLogs without `-storageNode` flag.

```bash
./victoria-logs -storageDataPath=/data/victoria-logs-data
```

**Initialization**:

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L117) (lines 117-162):
```go
func initLocalStorage() {
    cfg := &logstorage.StorageConfig{
        Retention:              retentionPeriod.Duration(),
        FlushInterval:          *inmemoryDataFlushInterval,
        MinFreeDiskSpaceBytes:  minFreeDiskSpaceBytes.N,
    }

    localStorage = logstorage.MustOpenStorage(*storageDataPath, cfg)
}
```

This snippet is abbreviated for readability. The actual initialization also sets additional `StorageConfig` fields such as `DefaultParallelReaders`, `MaxDiskSpaceUsageBytes`, `MaxDiskUsagePercent`, `FutureRetention`, `MaxBackfillAge`, `SnapshotsMaxAge`, `LogNewStreams`, and `LogIngestedRows`.

**Data Flow**:
```
HTTP Request
    ↓
nativeinsert.RequestHandler
    ↓
LogMessageProcessor (buffer)
    ↓
logRowsStorage.MustAddRows(lr)
    ↓
vlstorage.Storage.MustAddRows(lr)
    ↓
localStorage.MustAddRows(lr)    [if localStorage != nil]
    ↓
Partition by day
    ↓
indexdb + datadb
    ↓
Disk: /data/victoria-logs-data/partitions/YYYYMMDD/
```

**Characteristics**:
- Single node
- Data stored locally on disk
- Simple deployment
- No network overhead
- Limited by single machine resources

---

### Distributed Storage Mode

**Activation**: Start VictoriaLogs with `-storageNode` flag.

```bash
# Frontend node (accepts HTTP requests)
./victoria-logs \
    -storageNode=storage-1:9428,storage-2:9428,storage-3:9428

# Storage nodes (store data)
./victoria-logs -storageDataPath=/data/victoria-logs-data
```

**Initialization**:

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L164) (lines 164-183):
```go
func initNetworkStorage() {
    netstorageInsert = netinsert.NewStorage(
        *storageNodeAddrs,          // ["storage-1:9428", "storage-2:9428"]
        authCfgs,                   // Optional auth
        isTLSs,                     // HTTP or HTTPS
        *insertConcurrency,         // Default: 2
        *insertDisableCompression,  // Default: false (compression enabled)
    )
}
```

**Stream-Based Routing**:

**File**: [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L375) (lines 375-379):

**Key Functions**:
- [`NewStorage(addrs, authCfgs, isTLSs, concurrency, disableCompression)`](../app/vlstorage/netinsert/netinsert.go#L322) - Create network storage
- [`AddRow(streamHash, r)`](../app/vlstorage/netinsert/netinsert.go#L375) - Add row to remote storage
- [`sendInsertRequestToAnyNode(pendingData)`](../app/vlstorage/netinsert/netinsert.go#L381) - Retry logic
- [`DebugFlush()`](../app/vlstorage/netinsert/netinsert.go#L366) - Force flush for debugging
```go
func (s *Storage) AddRow(streamHash uint64, r *logstorage.InsertRow) {
    idx := s.srt.getNodeIdx(streamHash)  // Adaptive stream routing policy
    sn := s.sns[idx]                     // Select storage node
    sn.addRow(r)                         // Send to that node
}
```

`sn.addRow(r)` appends to a per-node pending buffer. Network send can happen immediately when the buffer hits 2MB, or later via the background flusher (about once per second).

**Adaptive Stream Routing**:
- For the first 1000 rows of a stream, routing is `streamHash % nodesCount` for locality.
- After 1000 rows, rows from that stream are randomly spread across nodes for better parallel query performance.
- This balances locality for small streams with fan-out for high-volume streams.

**Batching**:

**File**: [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L157) (lines 157-183):
```go
func (sn *storageNode) addRow(r *logstorage.InsertRow) {
    b := r.Marshal(b)  // Serialize row

    sn.pendingDataMu.Lock()
    if sn.pendingData.Len() + len(b) > maxInsertBlockSize {  // 2MB limit
        pendingData = sn.grabPendingDataForFlushLocked()
    }
    sn.pendingData.MustWrite(b)  // Accumulate
    sn.pendingDataMu.Unlock()

    if pendingData != nil {
        sn.mustSendInsertRequest(pendingData)  // Send when full
    }
}
```

**Compression and Transmission**:

**File**: [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L224) (lines 224-252):
```go
func (sn *storageNode) sendInsertRequest(pendingData *bytesutil.ByteBuffer) error {
    var body io.Reader
    if !sn.s.disableCompression {
        bb.B = zstd.CompressLevel(bb.B[:0], pendingData.B, 1)
        body = bb.NewReader()
    } else {
        body = pendingData.NewReader()
    }

    // POST to remote storage node
    if err := sn.doRequest("/internal/insert", body); err != nil {
        return err
    }
    return nil
}
```

**HTTP Request Format**:
```
POST http://storage-1:9428/internal/insert?version=v1
Content-Type: application/octet-stream
Content-Encoding: zstd

[compressed binary data of marshaled InsertRow objects]
```

**Retry and Failover**:

**File**: [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L195) (lines 195-222):
```go
func (sn *storageNode) mustSendInsertRequest(pendingData *bytesutil.ByteBuffer) {
    err := sn.sendInsertRequest(pendingData)
    if err == nil {
        return
    }

    // Retry with other nodes
    for !sn.s.sendInsertRequestToAnyNode(pendingData) {
        logger.Errorf("all storage nodes unavailable; retrying in 1s")
        time.Sleep(time.Second)
    }
}
```

**Failure Handling**:
- Failed node temporarily disabled for 10 seconds
- Data automatically re-routed to healthy nodes
- Retries until successful or shutdown
- If all nodes stay unavailable and shutdown happens, pending data can be dropped
- Metrics track node reachability

**Characteristics**:
- Horizontal scalability
- Data distributed across nodes
- Higher availability
- Network overhead for inter-node communication
- More complex deployment

---

## Internal Insert Endpoint

The `/internal/insert` endpoint is used for inter-node communication in distributed mode.

### Endpoint Registration

**File**: [`app/vlinsert/main.go`](../app/vlinsert/main.go#L50) (lines 50-57)

**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlinsert/main.go#L38) - Main router for all /insert/* endpoints
- [`insertHandler(w, r, path)`](../app/vlinsert/main.go#L62) - Route specific insert endpoints

```go
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
    if path == "/internal/insert" {
        if *disableInternalInsert || *disableInsert {
            httpserver.Errorf(w, r, "requests disabled")
            return true
        }
        internalinsert.RequestHandler(w, r)
        return true
    }
    return false
}
```

### Request Handler

**File**: [`app/vlinsert/internalinsert/internalinsert.go`](../app/vlinsert/internalinsert/internalinsert.go#L23) (lines 23-92)

**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlinsert/internalinsert/internalinsert.go#L24) - Main handler for /internal/insert
- [`parseData(irp, data)`](../app/vlinsert/internalinsert/internalinsert.go#L96) - Unmarshal and add rows

```go
func RequestHandler(w http.ResponseWriter, r *http.Request) {
    // Verify POST method
    if r.Method != "POST" {
        w.WriteHeader(http.StatusMethodNotAllowed)
        return
    }

    // Verify protocol version
    version := r.FormValue("version")
    if version != netinsert.ProtocolVersion {  // Must be "v1"
        httpserver.Errorf(w, r, "unsupported version")
        return
    }

    // Get common params (mostly ignored for /internal/insert)
    cp, err := insertutil.GetCommonParams(r)

    // Reset params unsupported by /internal/insert
    cp.TenantID = logstorage.TenantID{}
    cp.TimeFields = nil
    cp.MsgFields = nil
    cp.StreamFields = nil

    // Read and decompress
    encoding := r.Header.Get("Content-Encoding")  // "zstd"
    err = protoparserutil.ReadUncompressedData(r.Body, encoding, maxRequestSize,
        func(data []byte) error {
            lmp := cp.NewLogMessageProcessor("internalinsert", false)
            irp := lmp.(insertutil.InsertRowProcessor)
            err := parseData(irp, data)
            lmp.MustClose()  // Flush to storage
            return err
        })
}
```

### Data Parsing

**File**: [`app/vlinsert/internalinsert/internalinsert.go`](../app/vlinsert/internalinsert/internalinsert.go#L96) (lines 96-114)

```go
func parseData(irp insertutil.InsertRowProcessor, data []byte) error {
    r := logstorage.GetInsertRow()
    defer logstorage.PutInsertRow(r)

    src := data
    for len(src) > 0 {
        // Unmarshal pre-serialized InsertRow
        tail, err := r.UnmarshalInplace(src)
        if err != nil {
            return fmt.Errorf("cannot parse row: %s", err)
        }
        src = tail

        irp.AddInsertRow(r)  // Add to buffer
    }
    return nil
}
```

### Complete Distributed Flow

The diagram below is a logical end-to-end path. In practice, forwarding from Node A to Node B is buffered and may be asynchronous (periodic flush or full-buffer flush), so it is not necessarily a synchronous send per ingested row.

```
┌─────────────────────────────────────────────────────────────────┐
│ CLIENT → Node A (Frontend/Ingestion Node)                       │
│                                                                  │
│  POST /insert/native                                            │
│    ↓                                                            │
│  Decode native InsertRow binary payload                         │
│    ↓                                                            │
│  Override row tenant with AccountID/ProjectID headers           │
│    ↓                                                            │
│  AddInsertRow() to ingestion buffer                             │
│    ↓                                                            │
│  Apply stream routing policy → Select Node B                    │
│    ↓                                                            │
│  Batch & Compress                                               │
│    ↓                                                            │
│  POST http://nodeB:9428/internal/insert?version=v1             │
└────────────────────────┬────────────────────────────────────────┘
                         │ NETWORK (zstd compressed)
┌────────────────────────┴────────────────────────────────────────┐
│ Node B (Storage Node)                                           │
│                                                                  │
│  POST /internal/insert                                          │
│    ↓                                                            │
│  Verify version                                                 │
│    ↓                                                            │
│  Decompress (zstd)                                              │
│    ↓                                                            │
│  Unmarshal InsertRow objects                                    │
│    ↓                                                            │
│  NO field/timestamp re-parsing (already marshaled)              │
│    ↓                                                            │
│  LogMessageProcessor buffer                                     │
│    ↓                                                            │
│  Flush → localStorage.MustAddRows(lr)                           │
│    ↓                                                            │
│  Partition by day                                               │
│    ↓                                                            │
│  indexdb + datadb                                               │
│    ↓                                                            │
│  DISK: victoria-logs-data/partitions/YYYYMMDD/{indexdb,datadb} │
└─────────────────────────────────────────────────────────────────┘
```

### Key Differences: `/insert/native` vs `/internal/insert`

| Aspect | `/insert/native` | `/internal/insert` |
|--------|------------------|-------------------|
| **Purpose** | Accept logs from external clients | Accept logs from other VictoriaLogs nodes |
| **Data Format** | Pre-marshaled `InsertRow` objects (same encoding as `/internal/insert`) | Pre-marshaled `InsertRow` objects |
| **Tenant ID** | Header tenant is authoritative; row tenant in payload is overwritten | Row tenant in payload is used; AccountID/ProjectID headers are not used for row tenant (if present and valid, they are parsed then reset/ignored) |
| **Stream/Time/Msg/Decolorize params** | `_stream_fields`, `_time_field`, `_msg_field`, `decolorize_fields` are ignored with warning | Same params are ignored with warning |
| **Field Parsing** | No JSON/text field parsing; binary row decode only | No parsing (already marshaled) |
| **Compression** | Optional (client-dependent) | Usually zstd from sender |
| **Usage** | External native clients (for example, `vlagent`) | Internal cluster communication |
| **Security Flag** | `-insert.disable` | `-internalinsert.disable` (and also disabled by `-insert.disable`) |
| **Performance** | Low overhead binary decode + limited parameter handling | Minimal overhead (pre-processed) |

### Why Two Different Endpoints?

1. **Separation of Concerns**: `/insert/native` is externally exposed and enforces header-based tenant override; `/internal/insert` is for trusted inter-node traffic and keeps tenant from payload

2. **Efficiency**: both endpoints use the same binary `InsertRow` format, so neither needs JSON/text parsing

3. **Security**: Can disable `/internal/insert` with `-internalinsert.disable` (or globally with `-insert.disable`) to prevent external abuse

4. **Avoid Double-Processing**: `/internal/insert` skips external-API semantics and ingests already-marshaled rows directly

---

## Key Design Patterns

### 1. Dependency Injection

The `insertutil` package uses dependency injection to decouple ingestion from storage:

```go
// Interface definition
type LogRowsStorage interface {
    MustAddRows(lr *logstorage.LogRows)
    CanWriteData() error
}

// Singleton instance
var logRowsStorage LogRowsStorage

// Injection at startup
insertutil.SetLogRowsStorage(&vlstorage.Storage{})
```

**Benefits**:
- Clean separation of concerns
- Easy to test (can inject mock storage)
- Storage implementation can change without affecting ingestion

### 2. Strategy Pattern

The `vlstorage.Storage` uses strategy pattern to route to local or distributed storage:

```go
func (*Storage) MustAddRows(lr *logstorage.LogRows) {
    if localStorage != nil {
        localStorage.MustAddRows(lr)  // Strategy: Local
    } else {
        lr.ForEachRow(netstorageInsert.AddRow)  // Strategy: Distributed
    }
}
```

### 3. Object Pooling

Reusable objects are pooled to reduce GC pressure:

```go
r := logstorage.GetInsertRow()  // Get from pool
defer logstorage.PutInsertRow(r)  // Return to pool
```

### 4. Batching

Multiple levels of batching reduce I/O:
- **Level 1**: `LogMessageProcessor` batches rows in memory
- **Level 2**: `storageNode` batches data before HTTP send (2MB blocks)
- **Level 3**: Partitions batch writes to disk

### 5. Background Flushing

Periodic background flushing reduces the in-memory window and improves durability:

```go
func (lmp *logMessageProcessor) initPeriodicFlush() {
    lmp.wg.Go(func() {
        ticker := time.NewTicker(time.Second)
        for {
            select {
            case <-lmp.stopCh:
                return
            case <-ticker.C:
                lmp.mu.Lock()
                if time.Since(lmp.lastFlushTime) >= time.Second {
                    lmp.flushLocked()
                }
                lmp.mu.Unlock()
            }
        }
    })
}
```

### 6. Ingestion Limits and Drop Conditions

Important guardrails that affect correctness and troubleshooting:

- `-insert.maxLineSizeBytes` (default 256KB): overlong lines are skipped by `LineReader` (`vl_too_long_lines_skipped_total`).
- `-insert.maxFieldsPerLine` (default 1000): rows with too many fields are dropped (`vl_rows_dropped_total{reason="too_many_fields"}`).
- Distributed `netinsert` has a hard 2MB per-row transfer limit (`maxInsertBlockSize`); oversized rows are skipped.
- In distributed mode, pending blocks can be dropped on shutdown if all storage nodes are unavailable.

**Key references**:
- [`app/vlinsert/insertutil/line_reader.go#L105`](../app/vlinsert/insertutil/line_reader.go#L105)
- [`app/vlinsert/insertutil/common_params.go#L263`](../app/vlinsert/insertutil/common_params.go#L263)
- [`app/vlstorage/netinsert/netinsert.go#L163`](../app/vlstorage/netinsert/netinsert.go#L163)
- [`app/vlstorage/netinsert/netinsert.go#L216`](../app/vlstorage/netinsert/netinsert.go#L216)

---

## Configuration Flags

### Storage Mode Selection

```bash
# Local mode (default)
-storageDataPath string
    Path to storage directory (default: "victoria-logs-data")

# Distributed mode
-storageNode array
    Comma-separated list of storage node addresses
    Example: -storageNode=node1:9428,node2:9428
```

### Local Storage Configuration

```bash
-retentionPeriod duration
    Data retention period (default: 7d, minimum: 1d)

-futureRetention duration
    Allow data this far into future (default: 2d)

-maxBackfillAge duration
    Maximum age for historical data (flag default: 0; effective behavior: values <= 0 are normalized to retention)

-inmemoryDataFlushInterval duration
    Flush interval for in-memory data (default: 5s)

-storage.minFreeDiskSpaceBytes bytes
    Minimum free disk space before read-only mode (default: 10MB)

-retention.maxDiskSpaceUsageBytes bytes
    Maximum disk usage before dropping old partitions (default: 0 - unlimited)

-retention.maxDiskUsagePercent int
    Maximum disk usage percentage 1-100 (default: 0 - unlimited)
```

### Distributed Mode Configuration

```bash
-insert.concurrency int
    Concurrent connections per storage node (default: 2)

-insert.disableCompression
    Disable compression to storage nodes (default: false)

-storageNode.username array
    Basic auth username for storage nodes

-storageNode.password array
    Basic auth password for storage nodes

-storageNode.bearerToken array
    Bearer token for storage nodes

-storageNode.tls array
    Use TLS (HTTPS) for storage nodes (default: false)

-storageNode.tlsCAFile array
    TLS CA file for storage nodes

-storageNode.tlsCertFile array
    TLS cert file for storage nodes

-storageNode.tlsKeyFile array
    TLS key file for storage nodes
```

### Endpoint Control

```bash
-insert.disable
    Disable all /insert/* endpoints (default: false)

-internalinsert.disable
    Disable /internal/insert endpoint (default: false)
```

### Request Limits

```bash
-nativeinsert.maxRequestSize bytes
    Max request size for /insert/native (default: 64MB)

-internalinsert.maxRequestSize bytes
    Max request size for /internal/insert (default: 64MB)
```

### Debugging

```bash
-logNewStreams
    Log creation of new log streams (default: false)

-logIngestedRows
    Log all ingested log entries (default: false)
```

---

## Summary

The VictoriaLogs ingestion flow is designed for:

1. **Performance**: Multi-level batching and background flushing minimize I/O
2. **Scalability**: Distributed mode enables horizontal scaling
3. **Reliability**: Retry logic and failover improve availability, but do not provide hard durability guarantees during prolonged full-cluster outage + shutdown
4. **Flexibility**: Support for both local and distributed deployments
5. **Efficiency**: Object pooling and compression reduce resource usage
6. **Simplicity**: Clean interfaces and separation of concerns

The key insight is the **dual-path architecture**:
- Simple path: Single node, direct to disk
- Complex path: Distributed nodes with adaptive stream routing and network transport

Both paths converge at the `LogRowsStorage` interface, making the system modular and testable.

---

## Document Maintenance Guidelines

### File Linking Syntax and Policy

This document uses VS Code-compatible markdown links to enable quick navigation from documentation to source code. All file references should follow these guidelines to maintain clickability.

#### Required Format

All file and line number references must use the following format:

```markdown
[DisplayText](path/to/file.go#LLineNumber)
```

**Key requirements**:
- Use capital `L` prefix before the line number (e.g., `#L28`, not `#28`)
- Use consistent relative paths from this document location (this file currently uses `../` to reach repo root)
- Include line numbers wherever possible

#### Standard Patterns

**1. Section Headers - File References**

```markdown
**File**: [`app/vlinsert/nativeinsert/nativeinsert.go`](../app/vlinsert/nativeinsert/nativeinsert.go#L28)
```

**2. Function References in Key Functions Lists**

```markdown
**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlinsert/nativeinsert/nativeinsert.go#L28) - Main HTTP handler
- [`parseData(irp, data, tenantID)`](../app/vlinsert/nativeinsert/nativeinsert.go#L94) - Parse and add rows
```

**3. Flow Diagram References**

```markdown
nativeinsert.RequestHandler               [nativeinsert.go:28](../app/vlinsert/nativeinsert/nativeinsert.go#L28)
```

**4. Location References**

```markdown
**Location**: [`app/vlinsert/nativeinsert/nativeinsert.go:28-90`](../app/vlinsert/nativeinsert/nativeinsert.go#L28)
```

#### Display Text Options

Choose the appropriate display text based on context:

- **Full path with backticks**: `` [`app/vlinsert/nativeinsert/nativeinsert.go`](path#L28) `` - Use in section headers
- **Filename only**: `[nativeinsert.go:28](path#L28)` - Use in flow diagrams for brevity
- **Function signature**: `[RequestHandler(w, r)](path#L28)` - Use in Key Functions lists
- **Description**: `[Main HTTP handler](path#L28)` - Use when context is clear

#### Line Number Selection

When linking to code:

- **Single function**: Link to the function definition line
- **Struct definition**: Link to the struct type declaration
- **Interface**: Link to the interface declaration
- **Code section**: Link to the first line of the section
- **Range (lines X-Y)**: Link to the start line (X)

#### Examples

✅ **Correct**:
```markdown
- [`GetCommonParams(r)`](../app/vlinsert/insertutil/common_params.go#L49) - Extract parameters
- **File**: [`storage.go`](../lib/logstorage/storage.go#L1139)
- [common_params.go:323](../app/vlinsert/insertutil/common_params.go#L323)
```

❌ **Incorrect**:
```markdown
- `GetCommonParams(r)` - Extract parameters (line 49)  # Not clickable
- **File**: `storage.go` (line 1139)  # Not clickable
- [common_params.go:323](../app/vlinsert/insertutil/common_params.go#323)  # Missing L prefix
```

#### Updating File References

When adding new file references to this document:

1. **Always include line numbers** - Even if referencing a general concept, link to the most relevant line
2. **Use the #L prefix** - Required for VS Code compatibility
3. **Test the links** - Cmd/Ctrl + Click in VS Code to verify they navigate correctly
4. **Keep display text concise** - Balance readability with information
5. **Update stale references** - If code moves, update line numbers accordingly

#### Verification Checklist

Before committing changes to this document:

- [ ] All file paths use a consistent relative base (as used throughout this file)
- [ ] All line number anchors use `#L` prefix (capital L)
- [ ] All links tested and navigate correctly in VS Code
- [ ] No plain text file references (should be clickable markdown links)
- [ ] Display text is appropriate for context (full path, filename, or function)

#### Tools for Validation

You can verify all links have the correct format using:

```bash
# Check for links missing the #L prefix
grep -n '\](.*\.go#[0-9]' onboarding-insert-flow.md

# Count total clickable line references
grep -o "#L[0-9]\+" onboarding-insert-flow.md | wc -l

# Find non-clickable file references
grep -n '`app/.*\.go`' onboarding-insert-flow.md | grep -v '\]('
```

#### Why This Matters

Clickable file references significantly improve the onboarding experience by:
- Reducing friction when exploring the codebase
- Enabling instant navigation from concept to implementation
- Making the documentation a living, interactive guide
- Helping developers quickly verify documented behavior against actual code

**Remember**: Every file reference is an opportunity to help a developer learn faster. Make them all clickable!
