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
  - [7. Partition Layer](#7-partition-layer)
  - [8. Disk Storage](#8-disk-storage)
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

"Native" refers to VictoriaLogs' own internal binary protocol — as opposed to standard text/JSON formats from other ecosystems. When a client sends data to `/insert/native`, it sends pre-serialized `InsertRow` objects in VictoriaLogs' binary encoding (tenant ID, stream tags, timestamp, and fields already marshaled into a compact binary representation). The server only needs to decode the binary data — no JSON parsing, no field mapping, no timestamp format detection.

### How Does It Differ from Other Formats?

The other ingestion endpoints accept **industry-standard formats** that require parsing and transformation before storage:

| Endpoint | Client sends | Parsing work |
|----------|-------------|-------------|
| `/insert/native` | VictoriaLogs binary (`InsertRow` objects) | Minimal — binary decode only |
| `/insert/jsonline` | Newline-delimited JSON (`{"msg":"foo","ts":"..."}`) | JSON parsing per line, field extraction, timestamp detection |
| `/insert/elasticsearch/_bulk` | Elasticsearch bulk format (alternating command + data JSON lines) | JSON parsing, Elasticsearch timestamp format handling (ISO8601, Unix ms/s) |
| `/insert/loki/api/v1/push` | Loki protobuf or JSON (Grafana's push format) | Protobuf/JSON decoding, stream label extraction |
| `/insert/opentelemetry/v1/logs` | OTLP protobuf (OpenTelemetry standard) | Protobuf decoding, resource/scope attribute extraction |
| `/insert/datadog/api/v2/logs` | Datadog JSON array | JSON parsing, nested Lambda format detection |
| Syslog (TCP/UDP socket) | RFC 3164/5424 syslog | Text parsing, priority/facility extraction, timezone handling |
| `/insert/journald/upload` | systemd journal binary export | Binary format parsing, field extraction |

All formats eventually converge on the same internal path: fields + timestamp → `AddRow()` → storage. The difference is how much work happens before that point. Native insert skips essentially all of it because the data is already in VictoriaLogs' internal format.

**Who uses native insert?** Primarily `vlagent` (VictoriaLogs' own log collection agent) and inter-node cluster communication (via `/internal/insert`). The standard-format endpoints exist for ecosystem compatibility — letting users send logs from fluent-bit, Promtail, OpenTelemetry Collector, Datadog Agent, rsyslog, and other tools without a format conversion layer.

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
Flush Trigger (buffer full/periodic)      [common_params.go:320](../app/vlinsert/insertutil/common_params.go#L320)
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
    │   DISK: victoria-logs-data/YYYYMMDD/{indexdb,datadb}
    │
    └─→ [DISTRIBUTED MODE]
        lr.ForEachRow(netstorageInsert.AddRow)  [main.go:549](../app/vlstorage/main.go#L549)
            ↓
        Hash-based Node Selection         [netinsert.go:376](../app/vlstorage/netinsert/netinsert.go#L376)
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

**HTTP Parameters Mapping**:
- Query arg `_time_field` or Header `VL-Time-Field` → `TimeFields`
- Query arg `_msg_field` or Header `VL-Msg-Field` → `MsgFields`
- Query arg `_stream_fields` or Header `VL-Stream-Fields` → `StreamFields`
- Query arg `extra_fields` or Header `VL-Extra-Fields` → `ExtraFields`

**Location**: [`app/vlinsert/insertutil/common_params.go:49-106`](../app/vlinsert/insertutil/common_params.go#L49)

---

### 3. Log Message Processor

**File**: [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L210)

The `LogMessageProcessor` is responsible for accumulating log rows in memory before flushing to storage.

**Key Functions**:
- [`NewLogMessageProcessor(protocolName, isStreamMode)`](../app/vlinsert/insertutil/common_params.go#L348) - Create new processor
- [`AddRow(timestamp, fields, streamFieldsLen)`](../app/vlinsert/insertutil/common_params.go#L254) - Add log row
- [`AddInsertRow(r)`](../app/vlinsert/insertutil/common_params.go#L290) - Add pre-marshaled row
- [`MustClose()`](../app/vlinsert/insertutil/common_params.go#L335) - Flush and close
- [`flushLocked()`](../app/vlinsert/insertutil/common_params.go#L320) - Flush to storage

```go
type logMessageProcessor struct {
    mu            sync.Mutex
    wg            sync.WaitGroup
    stopCh        chan struct{}
    lastFlushTime time.Time

    cp *CommonParams
    lr *logstorage.LogRows  // In-memory buffer

    rowsIngestedTotal  *metrics.Counter
    bytesIngestedTotal *metrics.Counter
    flushDuration      *metrics.Summary

    unflushedRows  int
    unflushedBytes int
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
2. Periodic timer (every ~1 second in stream mode)
3. Request completion (`MustClose()` called)

```go
func (lmp *logMessageProcessor) AddInsertRow(r *logstorage.InsertRow) {
    lmp.mu.Lock()
    defer lmp.mu.Unlock()

    lmp.unflushedRows++
    n := logstorage.EstimatedJSONRowLen(r.Fields)
    lmp.unflushedBytes += n

    // Add to buffer
    lmp.lr.MustAddInsertRow(r)

    // Check if flush needed
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

The `vlstorage.Storage` struct implements the `LogRowsStorage` interface and routes data to either local or distributed storage.

**Key Functions**:
- [`Init()`](../app/vlstorage/main.go#L109) - Initialize storage mode
- [`initLocalStorage()`](../app/vlstorage/main.go#L117) - Initialize local storage
- [`initNetworkStorage()`](../app/vlstorage/main.go#L164) - Initialize distributed storage
- [`Storage.MustAddRows(lr)`](../app/vlstorage/main.go#L543) - Route data to storage
- [`Storage.CanWriteData()`](../app/vlstorage/main.go#L524) - Check write readiness

**Key Functions**:
- `Init()` - Initialize storage mode (line 109)
- `initLocalStorage()` - Initialize local storage (line 117)
- `initNetworkStorage()` - Initialize distributed storage (line 164)
- `Storage.MustAddRows(lr)` - Route data to storage (line 543)
- `Storage.CanWriteData()` - Check write readiness (line 524)

```go
// Storage implements insertutil.LogRowsStorage interface
type Storage struct{}  // Empty struct - just a namespace

func (*Storage) MustAddRows(lr *logstorage.LogRows) {
    if localStorage != nil {
        // LOCAL MODE: Store data on disk
        localStorage.MustAddRows(lr)
    } else {
        // DISTRIBUTED MODE: Send to remote nodes
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
var localStorage *logstorage.Storage
var netstorageInsert *netinsert.Storage

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

### 7. Partition Layer

**File**: [`lib/logstorage/partition.go`](../lib/logstorage/partition.go#L74)

Data is organized into day-based partitions. Each partition has two components:

**Key Functions**:
- [`mustOpenPartition(s, path)`](../lib/logstorage/partition.go#L74) - Open existing partition
- [`mustCreatePartition(path)`](../lib/logstorage/partition.go#L52) - Create new partition
- [`mustAddRows(lr)`](../lib/logstorage/partition.go#L135) - Add rows to partition
- [`mustClosePartition(pt)`](../lib/logstorage/partition.go#L121) - Close partition
- **indexdb**: Stream registration and indexing
- **datadb**: Actual log data storage

```go
type partition struct {
    s    *Storage        // Parent storage
    path string          // Path to partition directory
    name string          // Partition name (YYYYMMDD)
    idb  *indexdb        // Index database
    ddb  *datadb         // Data database
}

func (pt *partition) mustAddRows(lr *LogRows) {
    // Register new streams in indexdb
    for i, rowIdx := range pendingRows {
        streamID := &streamIDs[rowIdx]
        if !pt.idb.hasStreamID(streamID) {
            pt.idb.mustRegisterStream(streamID, streamTagsCanonical)
        }
    }

    // Add rows to datadb
    pt.ddb.mustAddRows(lr)
}
```

**Partition Directory Structure**:
```
victoria-logs-data/
├── 20260211/          # Partition for Feb 11, 2026
│   ├── indexdb/       # Stream indexes
│   └── datadb/        # Log data (columnar format)
├── 20260212/          # Partition for Feb 12, 2026
│   ├── indexdb/
│   └── datadb/
└── ...
```

**Location**: [`lib/logstorage/partition.go:23-178`](../lib/logstorage/partition.go#L23)

---

### 8. Disk Storage

**File**: [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L1139)

The `logstorage.Storage` manages partitions and handles data retention.

**Key Functions**:
- `MustOpenStorage(path, cfg)` - Open storage (called during initialization)
- [`MustAddRows(lr)`](../lib/logstorage/storage.go#L1139) - Add rows to storage
- [`getPartitionForWriting(day)`](../lib/logstorage/storage.go#L1233) - Get partition for specific day
- `IsReadOnly()` - Check if storage is read-only
- `MustClose()` - Close storage

```go
func (s *Storage) MustAddRows(lr *LogRows) {
    // Fast path - try adding all rows to hot partition
    if ptwHot != nil && ptwHot.canAddAllRows(lr) {
        ptwHot.pt.mustAddRows(lr)
        return
    }

    // Slow path - split rows among partitions by day
    now := time.Now().UnixNano()
    minAllowedDay := s.getMinAllowedDay(now)
    maxAllowedDay := s.getMaxAllowedDay(now)

    m := make(map[int64]*LogRows)
    for i, ts := range lr.timestamps {
        day := ts / nsecsPerDay

        // Validate timestamp against retention
        if day < minAllowedDay {
            s.rowsDroppedTooSmallTimestamp.Add(1)
            continue
        }
        if day > maxAllowedDay {
            s.rowsDroppedTooBigTimestamp.Add(1)
            continue
        }

        // Add to appropriate day partition
        lrPart := m[day]
        if lrPart == nil {
            lrPart = GetLogRows(nil, nil, nil, nil, "")
            m[day] = lrPart
        }
        lrPart.mustAddInternal(lr.streamIDs[i], ts, lr.rows[i], lr.streamTagsCanonicals[i])
    }

    // Write each day's data to its partition
    for day, lrPart := range m {
        ptw := s.getPartitionForWriting(day)
        if ptw != nil {
            ptw.pt.mustAddRows(lrPart)
        }
        PutLogRows(lrPart)
    }
}
```

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

**Location**: [`lib/logstorage/storage.go:1139-1217`](../lib/logstorage/storage.go#L1139)

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
Disk: /data/victoria-logs-data/YYYYMMDD/
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
- [`DebugFlush()`](../app/vlstorage/netinsert/netinsert.go#L147) - Force flush for debugging
```go
func (s *Storage) AddRow(streamHash uint64, r *logstorage.InsertRow) {
    idx := s.srt.getNodeIdx(streamHash)  // Hash-based routing
    sn := s.sns[idx]                     // Select storage node
    sn.addRow(r)                         // Send to that node
}
```

**Stream Affinity**: All logs from the same stream (identified by stream fields) are routed to the same storage node. This ensures:
- Efficient querying (stream data is co-located)
- Consistent hashing for balanced distribution
- Reduced cross-node queries

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

    // Reset params since they're already embedded in marshaled data
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

```
┌─────────────────────────────────────────────────────────────────┐
│ CLIENT → Node A (Frontend/Ingestion Node)                       │
│                                                                  │
│  POST /insert/native                                            │
│    ↓                                                            │
│  Parse JSON/Native format                                       │
│    ↓                                                            │
│  Extract fields, parse timestamps                               │
│    ↓                                                            │
│  Marshal to InsertRow                                           │
│    ↓                                                            │
│  Hash stream → Select Node B                                    │
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
│  NO re-parsing (already processed)                              │
│    ↓                                                            │
│  LogMessageProcessor buffer                                     │
│    ↓                                                            │
│  Flush → localStorage.MustAddRows(lr)                           │
│    ↓                                                            │
│  Partition by day                                               │
│    ↓                                                            │
│  indexdb + datadb                                               │
│    ↓                                                            │
│  DISK: victoria-logs-data/YYYYMMDD/{indexdb,datadb}            │
└─────────────────────────────────────────────────────────────────┘
```

### Key Differences: `/insert/native` vs `/internal/insert`

| Aspect | `/insert/native` | `/internal/insert` |
|--------|------------------|-------------------|
| **Purpose** | Accept logs from external clients | Accept logs from other VictoriaLogs nodes |
| **Data Format** | Client-specific protocol | Pre-marshaled `InsertRow` objects |
| **Tenant ID** | Extracted from HTTP headers | Embedded in data (headers ignored) |
| **Stream/Time Fields** | Configurable via query params | Already processed (ignored) |
| **Field Parsing** | Full parsing and validation | No parsing (already done) |
| **Compression** | Optional (client-dependent) | Usually zstd from sender |
| **Usage** | External (vlagent, fluent-bit, etc.) | Internal cluster communication |
| **Security Flag** | `-insert.disable` | `-internalinsert.disable` |
| **Performance** | Full processing overhead | Minimal overhead (pre-processed) |

### Why Two Different Endpoints?

1. **Separation of Concerns**: `/insert/native` handles external clients with full parameter processing; `/internal/insert` handles trusted pre-processed data

2. **Efficiency**: `/internal/insert` skips parsing/validation since data is already in internal format

3. **Security**: Can disable `/internal/insert` with `-internalinsert.disable` to prevent external abuse

4. **Avoid Double-Processing**: Data sent to `/internal/insert` has already been through field mapping, time parsing, etc.

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

Periodic background flushing ensures data durability:

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
    Maximum age for historical data (default: 0 - disabled)

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
3. **Reliability**: Retry logic and failover ensure data durability
4. **Flexibility**: Support for both local and distributed deployments
5. **Efficiency**: Object pooling and compression reduce resource usage
6. **Simplicity**: Clean interfaces and separation of concerns

The key insight is the **dual-path architecture**:
- Simple path: Single node, direct to disk
- Complex path: Distributed nodes with hash-based routing and network transport

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
- Use relative paths from repository root
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

- [ ] All file paths use relative paths from repository root
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
