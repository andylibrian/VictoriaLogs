# VictoriaLogs Cluster Architecture - Developer Onboarding Guide

This document provides a comprehensive overview of how VictoriaLogs operates in cluster mode, covering the three core components (vlinsert, vlselect, vlstorage), their communication protocols, data sharding, query fan-out, high availability, and security.

## Table of Contents

- [Overview](#overview)
- [Complete Data Flows](#complete-data-flows)
  - [Ingestion Flow](#ingestion-flow)
  - [Query Flow](#query-flow)
- [Architecture Layers](#architecture-layers)
  - [1. Mode Selection — Single-Node vs Cluster](#1-mode-selection--single-node-vs-cluster)
  - [2. vlinsert — Data Ingestion](#2-vlinsert--data-ingestion)
  - [3. vlstorage (as vlinsert backend) — Network Insert](#3-vlstorage-as-vlinsert-backend--network-insert)
  - [4. Internal Insert Endpoint](#4-internal-insert-endpoint)
  - [5. vlselect — Distributed Querying](#5-vlselect--distributed-querying)
  - [6. Query Splitting — Remote vs Local Pipes](#6-query-splitting--remote-vs-local-pipes)
  - [7. Internal Select Endpoint](#7-internal-select-endpoint)
  - [8. Storage Router — Dual-Path Dispatch](#8-storage-router--dual-path-dispatch)
- [Data Sharding Strategy](#data-sharding-strategy)
- [High Availability](#high-availability)
  - [Ingestion Path HA](#ingestion-path-ha)
  - [Query Path Behavior](#query-path-behavior)
  - [Partial Response Support](#partial-response-support)
- [Inter-Node Communication Protocol](#inter-node-communication-protocol)
  - [Insert Protocol](#insert-protocol)
  - [Select Query Protocol](#select-query-protocol)
  - [Select Metadata Protocol](#select-metadata-protocol)
  - [Protocol Versioning](#protocol-versioning)
- [Security](#security)
  - [Endpoint Isolation](#endpoint-isolation)
  - [TLS and Authentication](#tls-and-authentication)
- [Key Design Patterns](#key-design-patterns)
- [Configuration Flags](#configuration-flags)

---

## Overview

VictoriaLogs cluster mode distributes log storage and querying across multiple nodes. All three roles (vlinsert, vlselect, vlstorage) share the **same executable** — their behavior is determined by the presence or absence of the `-storageNode` command-line flag.

### Component Roles at a Glance

| Component | Role | Activated When | Default Port |
|-----------|------|----------------|--------------|
| **vlstorage** | Stores logs on disk, serves internal queries | `-storageNode` is **not** set | 9428 |
| **vlinsert** | Accepts logs from external clients, shards to vlstorage nodes | `-storageNode` **is** set | (custom via `-httpListenAddr`) |
| **vlselect** | Accepts queries from external clients, fans out to vlstorage nodes | `-storageNode` **is** set | (custom via `-httpListenAddr`) |

When `-storageNode` is set, the process runs **both** vlinsert and vlselect simultaneously — it accepts logs on `/insert/*` and queries on `/select/*`, forwarding both to the configured storage nodes.

### Single-Node / Cluster Duality

Every vlstorage node is a fully functional single-node VictoriaLogs instance. It accepts logs and queries directly on its own port. In cluster mode, vlinsert and vlselect communicate with vlstorage via internal HTTP endpoints (`/internal/insert` and `/internal/select/*`), but vlstorage continues to serve its own `/insert/*` and `/select/*` endpoints independently.

**Public vs Internal Select Endpoints**:
- `/select/*` is the external query API for users and tools (JSON/NDJSON responses, user-facing query args). See [`app/vlselect/main.go`](../app/vlselect/main.go#L90) and [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1149).
- `/internal/select/*` is the cluster-internal API used by `vlselect` to query `vlstorage` (versioned form args, binary responses, optional zstd compression). See [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L29) and [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L431).

### Minimal Cluster Setup

```
vlinsert + vlselect (one node with -storageNode flag)
      ↓
vlstorage-1        vlstorage-2
(port 9491)        (port 9492)
```

---

## Complete Data Flows

**Key Files**:
- [`app/victoria-logs/main.go`](../app/victoria-logs/main.go#L31) - Entry point, request routing
- [`app/vlstorage/main.go`](../app/vlstorage/main.go#L109) - Mode selection (local vs network storage)
- [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L36) - Network insert storage
- [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L82) - Network select storage
- [`app/vlinsert/internalinsert/internalinsert.go`](../app/vlinsert/internalinsert/internalinsert.go#L24) - Internal insert endpoint handler
- [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L31) - Internal select endpoint handler
- [`lib/logstorage/net_query_runner.go`](../lib/logstorage/net_query_runner.go#L12) - Distributed query splitting and execution

### Ingestion Flow

```
Log Source (e.g. curl POST /insert/jsonline)
    ↓
victoria-logs-prod (vlinsert + vlselect node, -storageNode=...)
    ↓
requestHandler                               [main.go:76](../app/victoria-logs/main.go#L76)
    ↓
vlinsert.RequestHandler                      [main.go:38](../app/vlinsert/main.go#L38)
    ↓
insertHandler (e.g. jsonline.RequestHandler) [main.go:62](../app/vlinsert/main.go#L62)
    ↓
cp.NewLogMessageProcessor("jsonline", ...)   [common_params.go:348](../app/vlinsert/insertutil/common_params.go#L348)
    ↓
lmp.AddRow(timestamp, fields, ...)           [common_params.go:254](../app/vlinsert/insertutil/common_params.go#L254)
    ↓
logRowsStorage.MustAddRows(lr)               [common_params.go:323](../app/vlinsert/insertutil/common_params.go#L323)
    ↓
vlstorage.Storage.MustAddRows(lr)            [main.go:543](../app/vlstorage/main.go#L543)
    ↓
lr.ForEachRow(netstorageInsert.AddRow)       [main.go:549](../app/vlstorage/main.go#L549)
    ↓
netstorageInsert.AddRow(streamHash, r)       [netinsert.go:375](../app/vlstorage/netinsert/netinsert.go#L375)
    ↓
srt.getNodeIdx(streamHash)                   [netinsert.go:413](../app/vlstorage/netinsert/netinsert.go#L413)
    ↓
sn.addRow(r)                                 [netinsert.go:157](../app/vlstorage/netinsert/netinsert.go#L157)
    ↓ (buffer until ≥ 2MB or 1s timeout)
sn.mustSendInsertRequest(pendingData)        [netinsert.go:195](../app/vlstorage/netinsert/netinsert.go#L195)
    ↓
POST http://storageNode/internal/insert?version=v1  (zstd compressed)
    ↓
    ╔════════════════════════════════════════════════╗
    ║  vlstorage node                                ║
    ║                                                ║
    ║  internalinsert.RequestHandler                 ║
    ║      ↓                                        ║
    ║  Verify protocol version (v1)                  ║
    ║      ↓                                        ║
    ║  Decompress zstd body                          ║
    ║      ↓                                        ║
    ║  parseData → InsertRow.UnmarshalInplace         ║
    ║      ↓                                        ║
    ║  lmp.AddInsertRow(r) → localStorage.MustAddRows ║
    ╚════════════════════════════════════════════════╝
```

### Query Flow

```
Query Client (e.g. curl GET /select/logsql/query?query=...)
    ↓
victoria-logs-prod (vlinsert + vlselect node, -storageNode=...)
    ↓
requestHandler                                 [main.go:76](../app/victoria-logs/main.go#L76)
    ↓
vlselect.RequestHandler                        [main.go:90](../app/vlselect/main.go#L90)
    ↓
selectHandler → Timeout + Concurrency Control  [main.go:138](../app/vlselect/main.go#L138)
    ↓
logsql.ProcessQueryRequest                     [logsql.go:1149](../app/vlselect/logsql/logsql.go#L1149)
    ↓
parseCommonArgs → logstorage.ParseQueryAtTimestamp
    ↓
vlstorage.RunQuery(qctx, writeBlock)           [main.go:554](../app/vlstorage/main.go#L554)
    ↓
netstorageSelect.RunQuery(qctx, writeBlock)    [netselect.go:385](../app/vlstorage/netselect/netselect.go#L385)
    ↓
NewNetQueryRunner(qctx, ...)                   [net_query_runner.go:31](../lib/logstorage/net_query_runner.go#L31)
splitQueryToRemoteAndLocal(q)                  [net_query_runner.go:72](../lib/logstorage/net_query_runner.go#L72)
    → qRemote = filter + remotable pipes
    → pipesLocal = remaining pipes (merge, sort, limit)
    ↓
Fan out qRemote to ALL vlstorage nodes         [netselect.go:400](../app/vlstorage/netselect/netselect.go#L400)
    ↓
POST http://storageNode-1/internal/select/query (form body includes version=v4, query, tenant_ids, ...)
POST http://storageNode-2/internal/select/query (form body includes version=v4, query, tenant_ids, ...)
    ↓
    ╔════════════════════════════════════════════════╗
    ║  Each vlstorage node (in parallel)             ║
    ║                                                ║
    ║  internalselect.RequestHandler                 ║
    ║      ↓                                        ║
    ║  Concurrency control (channel semaphore)       ║
    ║      ↓                                        ║
    ║  processQueryRequest                           ║
    ║      ↓                                        ║
    ║  getCommonParams → verify protocol version     ║
    ║      ↓                                        ║
    ║  vlstorage.RunQuery → localStorage.RunQuery    ║
    ║      ↓                                        ║
    ║  searchParallel across partitions              ║
    ║      ↓                                        ║
    ║  Stream DataBlocks (binary + zstd) back        ║
    ║      ↓                                        ║
    ║  Send query stats block (last)                 ║
    ╚════════════════════════════════════════════════╝
    ↓
Receive + decompress DataBlocks from all nodes
    ↓
Apply pipesLocal (merge, sort, limit, etc.)
    ↓
writeBlock → Stream JSON to client
```

---

## Architecture Layers

### 1. Mode Selection — Single-Node vs Cluster

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L109)

The entire cluster vs single-node decision is made in `vlstorage.Init()` based on whether `-storageNode` is set.

**Key Functions**:
- [`Init()`](../app/vlstorage/main.go#L109) - Initializes either local or network storage
- [`initLocalStorage()`](../app/vlstorage/main.go#L117) - Opens on-disk storage at `-storageDataPath`
- [`initNetworkStorage()`](../app/vlstorage/main.go#L164) - Creates `netinsert.Storage` and `netselect.Storage` for remote nodes

```go
func Init() {
    if len(*storageNodeAddrs) == 0 {
        initLocalStorage()      // Single-node mode: store data locally
    } else {
        initNetworkStorage()    // Cluster mode: route to remote storage nodes
    }
}
```

The mode selection sets package-level variables that all routing functions check:

```go
var localStorage *logstorage.Storage      // non-nil in single-node mode
var netstorageInsert *netinsert.Storage    // non-nil in cluster mode
var netstorageSelect *netselect.Storage    // non-nil in cluster mode
```

**Location**: [`app/vlstorage/main.go:99-183`](../app/vlstorage/main.go#L99)

---

### 2. vlinsert — Data Ingestion

**File**: [`app/vlinsert/main.go`](../app/vlinsert/main.go#L38)

vlinsert handles incoming log data from all supported protocols. For HTTP-based ingestion, it routes requests to protocol-specific handlers, which parse logs and pass them to the storage layer.

**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlinsert/main.go#L38) - Routes `/insert/*` and `/internal/insert` requests
- [`insertHandler(w, r, path)`](../app/vlinsert/main.go#L62) - Dispatches to protocol handlers

**Supported HTTP Ingestion Protocols**:

| Path | Protocol | Handler Package |
|------|----------|-----------------|
| `/insert/jsonline` | JSON Lines | [`app/vlinsert/jsonline`](../app/vlinsert/jsonline/) |
| `/insert/elasticsearch/*` | Elasticsearch Bulk | [`app/vlinsert/elasticsearch`](../app/vlinsert/elasticsearch/) |
| `/insert/loki/*` | Loki Push | [`app/vlinsert/loki`](../app/vlinsert/loki/) |
| `/insert/opentelemetry/*` | OpenTelemetry OTLP | [`app/vlinsert/opentelemetry`](../app/vlinsert/opentelemetry/) |
| `/insert/datadog/*` | Datadog | [`app/vlinsert/datadog`](../app/vlinsert/datadog/) |
| `/insert/journald/*` | Journald | [`app/vlinsert/journald`](../app/vlinsert/journald/) |
| `/insert/native` | Native binary | [`app/vlinsert/nativeinsert`](../app/vlinsert/nativeinsert/) |
| `/internal/insert` | Cluster-internal | [`app/vlinsert/internalinsert`](../app/vlinsert/internalinsert/) |

**Syslog ingestion is separate from `/insert/*` HTTP routing**: syslog listeners are initialized via [`syslog.MustInit()`](../app/vlinsert/main.go#L29) and configured with flags such as [`-syslog.listenAddr.tcp`, `-syslog.listenAddr.udp`, `-syslog.listenAddr.unix`](../app/vlinsert/syslog/syslog.go#L38).

Every protocol handler follows the same pattern:
1. Parse the request using protocol-specific logic
2. Create a `LogMessageProcessor` via `cp.NewLogMessageProcessor()`
3. Add rows via `lmp.AddRow()`, which buffers and flushes to `logRowsStorage.MustAddRows()`
4. `MustAddRows()` either stores locally or shards across network storage nodes

```go
// In cluster mode (vlstorage.Storage.MustAddRows):
func (*Storage) MustAddRows(lr *logstorage.LogRows) {
    if localStorage != nil {
        localStorage.MustAddRows(lr)     // Single-node: store locally
    } else {
        lr.ForEachRow(netstorageInsert.AddRow)  // Cluster: shard to storage nodes
    }
}
```

**Location**: [`app/vlinsert/main.go:38-93`](../app/vlinsert/main.go#L38)

---

### 3. vlstorage (as vlinsert backend) — Network Insert

**File**: [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L36)

The `netinsert.Storage` manages connections to remote vlstorage nodes and handles buffering, compression, and delivery of log rows.

**Key Types**:
- [`Storage`](../app/vlstorage/netinsert/netinsert.go#L36) - Holds storage nodes, buffer pool, stream tracker
- [`storageNode`](../app/vlstorage/netinsert/netinsert.go#L49) - Represents a single vlstorage node with pending data buffer and HTTP client

**Key Functions**:
- [`NewStorage(addrs, authCfgs, isTLSs, concurrency, disableCompression)`](../app/vlstorage/netinsert/netinsert.go#L322) - Create network insert storage
- [`AddRow(streamHash, r)`](../app/vlstorage/netinsert/netinsert.go#L375) - Route a row to a storage node based on stream hash
- [`storageNode.addRow(r)`](../app/vlstorage/netinsert/netinsert.go#L157) - Buffer a row for sending
- [`storageNode.mustSendInsertRequest(pendingData)`](../app/vlstorage/netinsert/netinsert.go#L195) - Send buffered data with HA re-routing
- [`storageNode.sendInsertRequest(pendingData)`](../app/vlstorage/netinsert/netinsert.go#L224) - HTTP POST to `/internal/insert`

**Buffering and Flushing**:

Each storage node has a pending data buffer. Rows are serialized and appended to the buffer. The buffer is flushed when:
- It exceeds `maxInsertBlockSize` (2 MB) — triggered inline during `addRow()`
- A background flusher fires every 1 second — [`backgroundFlusher()`](../app/vlstorage/netinsert/netinsert.go#L118)
- The storage is being stopped — flush with `force=true`

```go
func (sn *storageNode) addRow(r *logstorage.InsertRow) {
    bb := bbPool.Get()
    b := r.Marshal(bb.B)

    sn.pendingDataMu.Lock()
    if sn.pendingData.Len()+len(b) > maxInsertBlockSize {
        pendingData = sn.grabPendingDataForFlushLocked()  // Swap buffer
    }
    sn.pendingData.MustWrite(b)
    sn.pendingDataMu.Unlock()

    if pendingData != nil {
        sn.mustSendInsertRequest(pendingData)  // Send full buffer
    }
}
```

**Concurrency Control**:

The `pendingDataBuffers` channel limits the number of concurrent in-flight buffers. Its capacity is `concurrency * len(storageNodes)`, where `concurrency` defaults to 2 (`-insert.concurrency`). When a buffer is grabbed for flushing, a new one is taken from this channel; after sending, the buffer is returned.

```go
pendingDataBuffers := make(chan *bytesutil.ByteBuffer, concurrency*len(addrs))
```

**Location**: [`app/vlstorage/netinsert/netinsert.go:36-440`](../app/vlstorage/netinsert/netinsert.go#L36)

---

### 4. Internal Insert Endpoint

**File**: [`app/vlinsert/internalinsert/internalinsert.go`](../app/vlinsert/internalinsert/internalinsert.go#L24)

The `/internal/insert` endpoint is the receiving side on each vlstorage node. It accepts binary-serialized log rows from vlinsert nodes.

**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlinsert/internalinsert/internalinsert.go#L24) - Main handler
- [`parseData(irp, data)`](../app/vlinsert/internalinsert/internalinsert.go#L96) - Deserialize rows from binary format

**Processing Steps**:
1. Verify the protocol version matches `netinsert.ProtocolVersion` (`"v1"`)
2. Parse common params (tenant ID is ignored — tenancy is embedded in the serialized row)
3. Decompress the request body (zstd by default)
4. Parse rows via `InsertRow.UnmarshalInplace()` in a loop
5. Add each row via `lmp.AddInsertRow(r)` → `logRowsStorage.MustAddRows()` → `localStorage.MustAddRows()`

```go
func parseData(irp insertutil.InsertRowProcessor, data []byte) error {
    r := logstorage.GetInsertRow()
    defer logstorage.PutInsertRow(r)

    src := data
    for len(src) > 0 {
        tail, err := r.UnmarshalInplace(src)
        src = tail
        irp.AddInsertRow(r)
    }
    return nil
}
```

**Location**: [`app/vlinsert/internalinsert/internalinsert.go:24-121`](../app/vlinsert/internalinsert/internalinsert.go#L24)

---

### 5. vlselect — Distributed Querying

**File**: [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L82)

The `netselect.Storage` handles distributed querying by fanning out queries to all vlstorage nodes in parallel and merging results.

This section describes the **internal** distributed query transport (`vlselect` → `/internal/select/*` on `vlstorage`). Public endpoint routing, timeouts, and user argument parsing for `/select/*` are handled in [`app/vlselect/main.go`](../app/vlselect/main.go#L138) and [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1362).

**Key Types**:
- [`Storage`](../app/vlstorage/netselect/netselect.go#L82) - Holds storage nodes and compression config
- [`storageNode`](../app/vlstorage/netselect/netselect.go#L88) - Represents a single vlstorage node with HTTP client

**Key Functions**:
- [`NewStorage(addrs, authCfgs, isTLSs, disableCompression)`](../app/vlstorage/netselect/netselect.go#L365) - Create network select storage
- [`Storage.RunQuery(qctx, writeBlock)`](../app/vlstorage/netselect/netselect.go#L385) - Entry point for distributed query
- [`Storage.runQuery(stopCh, qctx, writeBlock)`](../app/vlstorage/netselect/netselect.go#L400) - Fan-out to all storage nodes
- [`storageNode.runQuery(qctx, processBlock)`](../app/vlstorage/netselect/netselect.go#L132) - Query a single storage node

**RunQuery** orchestrates the full distributed query:

```go
func (s *Storage) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
    // 1. Split query into remote and local pipes
    nqr, _ := logstorage.NewNetQueryRunner(qctx, s.RunQuery, writeBlock)

    // 2. Define the network search function
    search := func(stopCh <-chan struct{}, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
        qctxLocal := qctx.WithQuery(q)
        return s.runQuery(stopCh, qctxLocal, writeBlock)
    }

    // 3. Execute: fan out remote query, merge results, apply local pipes
    concurrency := qctx.Query.GetConcurrency()
    return nqr.Run(qctx.Context, concurrency, search)
}
```

**runQuery** fans out to all nodes in parallel:

```go
func (s *Storage) runQuery(stopCh <-chan struct{}, qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
    errs := make([]error, len(s.sns))
    var wg sync.WaitGroup
    for nodeIdx := range s.sns {
        wg.Go(func() {
            sn := s.sns[nodeIdx]
            err := sn.runQuery(qctxLocal, func(db *logstorage.DataBlock) {
                writeBlock(uint(nodeIdx), db)
            })
            errs[nodeIdx] = sn.handleError(ctxWithCancel, cancel, err, qctx.AllowPartialResponse)
        })
    }
    wg.Wait()
    return getFirstError(errs, qctx.AllowPartialResponse)
}
```

**Metadata Queries** (GetFieldNames, GetStreams, etc.) follow the same fan-out-and-merge pattern:

```go
func (s *Storage) GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
    return s.getValuesWithHits(qctx, 0, false, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
        return sn.getFieldNames(qctx.WithContext(ctx))
    })
}
```

All node results are merged via `logstorage.MergeValuesWithHits()`.

**Location**: [`app/vlstorage/netselect/netselect.go:82-837`](../app/vlstorage/netselect/netselect.go#L82)

---

### 6. Query Splitting — Remote vs Local Pipes

**File**: [`lib/logstorage/net_query_runner.go`](../lib/logstorage/net_query_runner.go#L72)

In distributed mode, a LogsQL query is automatically split into pipes that run on storage nodes (remote) and pipes that run on the frontend after merging (local).

**Key Functions**:
- [`NewNetQueryRunner(qctx, runNetQuery, writeNetBlock)`](../lib/logstorage/net_query_runner.go#L31) - Create distributed query runner
- [`splitQueryToRemoteAndLocal(q)`](../lib/logstorage/net_query_runner.go#L72) - Split the query
- [`Run(ctx, concurrency, netSearch)`](../lib/logstorage/net_query_runner.go#L61) - Execute the distributed query

```go
func splitQueryToRemoteAndLocal(q *Query) (*Query, []pipe) {
    qRemote := q.Clone(timestamp)
    qRemote.DropAllPipes()

    pipesRemote, pipesLocal := getRemoteAndLocalPipes(q)
    qRemote.pipes = pipesRemote

    // Limit fields selected at remote storage to only those needed by local pipes
    pf := getNeededColumns(pipesLocal)
    qRemote.addFieldsFilters(pf)

    return qRemote, pipesLocal
}
```

Each pipe type implements `splitToRemoteAndLocal()` to declare how it can be distributed. The splitting walks the pipe chain and stops at the first pipe that cannot be fully executed remotely. Example:

- **Remotable**: filter, stats (partial), fields, delete, limit
- **Local-only**: sort, uniq (final merge), join, union

After receiving results from all storage nodes, the frontend executes `pipesLocal` via `runPipes()`, which merges and post-processes the data.

**Location**: [`lib/logstorage/net_query_runner.go:12-112`](../lib/logstorage/net_query_runner.go#L12)

---

### 7. Internal Select Endpoint

**File**: [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L31)

The `/internal/select/*` endpoints run on each vlstorage node and handle queries from vlselect frontend nodes.

Unlike `/select/*`, these endpoints are not client-facing:
- They expect internal transport parameters such as `tenant_ids`, `timestamp`, `hidden_fields_filters`, and `version` via form values.
- They return compact binary payloads (`application/octet-stream`) for most select operations.
- They enforce protocol version compatibility between cluster components.

**Key Functions**:
- [`RequestHandler(ctx, w, r)`](../app/vlselect/internalselect/internalselect.go#L31) - Concurrency-limited dispatcher
- [`processQueryRequest(ctx, w, r)`](../app/vlselect/internalselect/internalselect.go#L94) - Runs query and streams binary results
- [`getCommonParams(r, version)`](../app/vlselect/internalselect/internalselect.go#L431) - Parse internal query params

**Registered Endpoints**:

`/internal/delete/*` handlers are routed through the same internal dispatcher, but they are accessible only when [`-internaldelete.enable`](../app/vlselect/main.go#L36) is set at the receiving node.

```go
var requestHandlers = map[string]func(...) error{
    "/internal/select/query":               processQueryRequest,
    "/internal/select/field_names":         processFieldNamesRequest,
    "/internal/select/field_values":        processFieldValuesRequest,
    "/internal/select/stream_field_names":  processStreamFieldNamesRequest,
    "/internal/select/stream_field_values": processStreamFieldValuesRequest,
    "/internal/select/streams":             processStreamsRequest,
    "/internal/select/stream_ids":          processStreamIDsRequest,
    "/internal/select/tenant_ids":          processTenantIDsRequest,

    "/internal/delete/run_task":     processDeleteRunTask,
    "/internal/delete/stop_task":    processDeleteStopTask,
    "/internal/delete/active_tasks": processDeleteActiveTasks,
}
```

**Concurrency Control**: Uses a channel semaphore with `-internalselect.maxConcurrentRequests` (default: 100):

```go
func RequestHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
    select {
    case concurrencyLimitCh <- struct{}{}:
        requestHandler(ctx, w, r, startTime)
        <-concurrencyLimitCh
    case <-ctx.Done():
        // Request canceled while waiting
    }
}
```

**Location**: [`app/vlselect/internalselect/internalselect.go:31-558`](../app/vlselect/internalselect/internalselect.go#L31)

---

### 8. Storage Router — Dual-Path Dispatch

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L554)

The `vlstorage` package-level functions transparently route all operations to either local storage or network storage. Every function follows the same pattern:

```go
func RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
    // Try Last-N optimization first (works in both modes)
    qOpt, offset, limit := qctx.Query.GetLastNResultsQuery()
    if qOpt != nil {
        return runOptimizedLastNResultsQuery(...)
    }
    // Route to appropriate backend
    if localStorage != nil {
        return localStorage.RunQuery(qctx, writeBlock)
    }
    return netstorageSelect.RunQuery(qctx, writeBlock)
}
```

**Routed Operations**:

| Function | Local Mode | Cluster Mode |
|----------|-----------|-------------|
| `MustAddRows(lr)` | `localStorage.MustAddRows(lr)` | `lr.ForEachRow(netstorageInsert.AddRow)` |
| `RunQuery(qctx, writeBlock)` | `localStorage.RunQuery(...)` | `netstorageSelect.RunQuery(...)` |
| `GetFieldNames(qctx)` | `localStorage.GetFieldNames(...)` | `netstorageSelect.GetFieldNames(...)` |
| `GetFieldValues(qctx, ...)` | `localStorage.GetFieldValues(...)` | `netstorageSelect.GetFieldValues(...)` |
| `GetStreams(qctx, limit)` | `localStorage.GetStreams(...)` | `netstorageSelect.GetStreams(...)` |
| `DeleteRunTask(ctx, ...)` | `localStorage.DeleteRunTask(...)` | `netstorageSelect.DeleteRunTask(...)` |
| `GetTenantIDs(ctx, ...)` | `localStorage.GetTenantIDs(...)` | `netstorageSelect.GetTenantIDs(...)` |

**Location**: [`app/vlstorage/main.go:520-665`](../app/vlstorage/main.go#L520)

---

## Data Sharding Strategy

**File**: [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L399)

vlinsert distributes logs among vlstorage nodes using a two-phase sharding strategy based on stream hash and row count.

**Key Types**:
- [`streamRowsTracker`](../app/vlstorage/netinsert/netinsert.go#L399) - Tracks per-stream row counts for sharding decisions

**Key Functions**:
- [`getNodeIdx(streamHash)`](../app/vlstorage/netinsert/netinsert.go#L413) - Determine which storage node receives a row

```go
func (srt *streamRowsTracker) getNodeIdx(streamHash uint64) uint64 {
    if srt.nodesCount == 1 {
        return 0  // Fast path for single node
    }

    srt.mu.Lock()
    defer srt.mu.Unlock()

    streamRows := srt.rowsPerStream[streamHash] + 1
    srt.rowsPerStream[streamHash] = streamRows

    if streamRows <= 1000 {
        // Phase 1: First 1000 rows per stream go to a deterministic node
        // based on streamHash. This ensures locality for small streams.
        return streamHash % uint64(srt.nodesCount)
    }

    // Phase 2: After 1000 rows, distribute randomly across nodes.
    // This improves parallel query performance for high-volume streams.
    return uint64(fastrand.Uint32n(uint32(srt.nodesCount)))
}
```

**Design Rationale**:

- **Phase 1 (first 1000 rows)**: Uses consistent hashing (`streamHash % nodesCount`) for data locality. Most log streams have fewer than 1000 entries, so they end up entirely on one node. Since different streams have different hashes, they are still spread evenly across nodes overall.

- **Phase 2 (after 1000 rows)**: Switches to random distribution. High-volume streams are spread across all nodes, enabling parallel scanning during queries. Random distribution is preferred over round-robin to avoid correlations between ingestion order and node count.

**Stream Hash Computation**: The stream hash is computed from the stream ID's 128-bit identifier at [`log_rows.go:233`](../lib/logstorage/log_rows.go#L233):

```go
streamHash := sid.id.lo ^ sid.id.hi
```

**Location**: [`app/vlstorage/netinsert/netinsert.go:399-440`](../app/vlstorage/netinsert/netinsert.go#L399)

---

## High Availability

### Ingestion Path HA

**File**: [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L195)

The ingestion path provides high availability by re-routing data when a storage node becomes unavailable.

**Key Functions**:
- [`mustSendInsertRequest(pendingData)`](../app/vlstorage/netinsert/netinsert.go#L195) - Send with HA re-routing
- [`sendInsertRequestToAnyNode(pendingData)`](../app/vlstorage/netinsert/netinsert.go#L381) - Try sending to any available node
- [`setDisableTemporarily()`](../app/vlstorage/netinsert/netinsert.go#L305) - Mark a node as temporarily unavailable

**Re-routing Mechanism**:

```go
func (sn *storageNode) mustSendInsertRequest(pendingData *bytesutil.ByteBuffer) {
    err := sn.sendInsertRequest(pendingData)
    if err == nil {
        return
    }

    // Primary node failed — try any other available node
    for !sn.s.sendInsertRequestToAnyNode(pendingData) {
        // All nodes unavailable — retry every second until stop signal
        select {
        case <-sn.s.stopCh:
            logger.Errorf("dropping %d bytes of data", pendingData.Len())
            return
        case <-time.After(time.Second):
        }
    }
}
```

**Temporary Disable**: When a node fails, it is disabled for 10 seconds via `disabledUntil` timestamp. During this period, the node is skipped for both primary routing and re-routing attempts, avoiding repeated connection failures.

```go
func (sn *storageNode) setDisableTemporarily() {
    sn.disabledUntil.Store(fasttime.UnixTimestamp() + 10)
    sn.sendErrors.Inc()
    sn.isReachable.Store(false)
}
```

**Reachability Metric**: Each storage node exposes `vl_insert_remote_is_reachable{addr="..."}` (1 = reachable, 0 = unreachable) for monitoring.

### Query Path Behavior

**File**: [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L729)

By default, queries return a **502 Bad Gateway** error if any vlstorage node is unavailable. This guarantees query completeness — all stored data is considered.

**Key Functions**:
- [`handleError(ctx, cancel, err, allowPartialResponse)`](../app/vlstorage/netselect/netselect.go#L729) - Handle per-node errors
- [`getFirstError(errs, allowPartialResponse)`](../app/vlstorage/netselect/netselect.go#L751) - Determine final error

```go
func getFirstError(errs []error, allowPartialResponse bool) error {
    if !allowPartialResponse {
        // Return the first error from any node
        for _, err := range errs {
            if err != nil {
                return err
            }
        }
        return nil
    }

    // allowPartialResponse == true
    // Return error only if ALL nodes are unavailable
    // or if any node has a configuration error
    for _, err := range errs {
        if err == nil {
            return nil  // At least one node responded
        }
        if !isUnavailableBackendError(err) {
            return err  // Configuration error — always propagate
        }
    }
    return fmt.Errorf("all the vlstorage nodes are unavailable")
}
```

### Partial Response Support

When `-search.allowPartialResponse` is enabled (or `allow_partial_response=true` query parameter):
- Queries succeed as long as at least one vlstorage node is available
- Results from unavailable nodes are silently omitted
- Configuration errors (non-network errors) are always returned regardless of this flag

Unavailable backend errors are identified by checking for `httpserver.ErrorWithStatusCode` wrapping (connection errors produce 502 status codes):

```go
func isUnavailableBackendError(err error) bool {
    var es *httpserver.ErrorWithStatusCode
    return errors.As(err, &es)
}
```

**Location**: [`app/vlstorage/netselect/netselect.go:729-787`](../app/vlstorage/netselect/netselect.go#L729)

---

## Inter-Node Communication Protocol

### Insert Protocol

**Direction**: vlinsert → vlstorage

| Aspect | Details |
|--------|---------|
| **Endpoint** | `POST /internal/insert?version=v1` |
| **Content-Type** | `application/octet-stream` |
| **Compression** | zstd level 1 (unless `-insert.disableCompression`) |
| **Body Format** | Concatenated `InsertRow.Marshal()` binary records |
| **Max Block Size** | 2 MB (`maxInsertBlockSize`) |
| **Flush Interval** | 1 second (background flusher) |

Each `InsertRow` is a self-describing binary record containing tenant ID, stream tags, timestamp, and fields. The receiver deserializes rows via `InsertRow.UnmarshalInplace()`.

### Select Query Protocol

**Direction**: vlselect → vlstorage

This is the wire protocol for `/internal/select/query`. It is different from the public `/select/logsql/query` API, which returns NDJSON to external clients via [`logsql.ProcessQueryRequest`](../app/vlselect/logsql/logsql.go#L1149).

| Aspect | Details |
|--------|---------|
| **Endpoint** | `POST /internal/select/query` |
| **Request Content-Type** | `application/x-www-form-urlencoded` |
| **Protocol Version Transport** | `version=v4` form field in request body |
| **Response Content-Type** | `application/octet-stream` |
| **Response Format** | Streaming `[8-byte length][compressed block]...` |

**Response Block Structure**:

```
┌──────────────────┐
│ 8 bytes: length  │  → encoding.UnmarshalUint64()
├──────────────────┤
│ compressed data  │  → zstd decompress
│   ┌─────────┐    │
│   │ 0x00    │    │  → Marker: regular DataBlock
│   │ DataBlock│   │  → db.UnmarshalInplace()
│   ├─────────┤    │
│   │ 0x00    │    │  → Marker: another DataBlock
│   │ DataBlock│   │
│   ├─────────┤    │
│   │ ...     │    │
│   └─────────┘    │
└──────────────────┘
... (more blocks until EOF)
┌──────────────────┐
│ 8 bytes: length  │  → Last block
├──────────────────┤
│ compressed data  │
│   ┌─────────┐    │
│   │ 0x01    │    │  → Marker: query stats block (always last)
│   │ QStats  │    │  → unmarshalQueryStats()
│   └─────────┘    │
└──────────────────┘
```

Each compressed block is flushed to the client when the buffer reaches 1 MB at the vlstorage side.

### Select Metadata Protocol

**Direction**: vlselect → vlstorage

For ValueWithHits metadata queries (`field_names`, `field_values`, `stream_field_names`, `stream_field_values`, `streams`, `stream_ids`), the response is a single compressed binary blob:

```
[8 bytes: count of ValueWithHits entries]
[ValueWithHits #1]
[ValueWithHits #2]
...
[QueryStats DataBlock]    → always appended last
```

The entire blob is zstd-compressed (unless `-select.disableCompression`).

`/internal/select/tenant_ids` is an exception: it returns JSON and doesn't use the ValueWithHits+QueryStats binary payload.

### Protocol Versioning

Most internal cluster endpoints include a `version` parameter. Both sides verify it matches the expected constant. A mismatch returns an error suggesting a version mismatch between cluster components.

Exception: `/internal/select/tenant_ids` currently doesn't use protocol versioning.

| Endpoint | Version Constant | Current Value |
|----------|-----------------|---------------|
| `/internal/insert` | `netinsert.ProtocolVersion` | `"v1"` |
| `/internal/select/query` | `netselect.QueryProtocolVersion` | `"v4"` |
| `/internal/select/field_names` | `netselect.FieldNamesProtocolVersion` | `"v4"` |
| `/internal/select/streams` | `netselect.StreamsProtocolVersion` | `"v4"` |
| `/internal/delete/run_task` | `netselect.DeleteRunTaskProtocolVersion` | `"v1"` |
| `/internal/select/tenant_ids` | _no version parameter_ | _n/a_ |

**Location**: [`app/vlstorage/netinsert/netinsert.go:33`](../app/vlstorage/netinsert/netinsert.go#L33), [`app/vlstorage/netselect/netselect.go:29-78`](../app/vlstorage/netselect/netselect.go#L29)

---

## Security

### Endpoint Isolation

In a production cluster, it is recommended to disable endpoints that should not be exposed on each component:

Route external query traffic only to `/select/*` via your auth proxy / load balancer. Do not expose `/internal/select/*` to untrusted clients; these endpoints are intended only for cluster inter-node communication.

```bash
# vlinsert node: disable select endpoints to prevent query traffic
./victoria-logs-prod -storageNode=... -select.disable

# vlselect node: disable insert endpoints to prevent ingestion traffic
./victoria-logs-prod -storageNode=... -insert.disable
```

vlstorage nodes can disable their internal endpoints to prevent direct external access (useful when running a single-node that should not accept cluster traffic):

```bash
./victoria-logs-prod -storageDataPath=... -internalinsert.disable -internalselect.disable
```

**Endpoint Control Flags**:

| Flag | Effect |
|------|--------|
| `-insert.disable` | Disables `/insert/*` and `/internal/insert` endpoints |
| `-select.disable` | Disables `/select/*` and `/internal/select/*` endpoints |
| `-internalinsert.disable` | Disables `/internal/insert` endpoint |
| `-internalselect.disable` | Disables `/internal/select/*` endpoints |
| `-internaldelete.enable` | Enables `/internal/delete/*` endpoints |

### TLS and Authentication

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L185)

Per-storage-node TLS and authentication is configured via command-line flags. Each storage node can have independent auth configuration.

**Key Functions**:
- [`newAuthConfigForStorageNode(argIdx)`](../app/vlstorage/main.go#L185) - Build auth config for a specific storage node

The auth config supports:
- **Basic Auth**: `-storageNode.username`, `-storageNode.password` (or file variants)
- **Bearer Token**: `-storageNode.bearerToken` (or file variant)
- **TLS**: `-storageNode.tls` enables HTTPS; additional flags for CA, client cert, key, server name, skip verify

```go
func newAuthConfigForStorageNode(argIdx int) *promauth.Config {
    // Build BasicAuthConfig from username/password/files
    // Build TLSConfig from CA/cert/key/serverName/insecureSkipVerify
    // Build Options with basicAuth, bearerToken, TLS
    ac, _ := opts.NewConfig()
    return ac
}
```

On the vlstorage side, HTTPS is enabled via `-tls`, `-tlsCertFile`, `-tlsKeyFile`, and basic auth via `-httpAuth.username`, `-httpAuth.password`.

**Location**: [`app/vlstorage/main.go:185-223`](../app/vlstorage/main.go#L185)

---

## Key Design Patterns

### 1. Single Executable, Multiple Roles

All cluster components share the same binary (`victoria-logs-prod`). The role is determined at runtime by the presence of `-storageNode`:
- Without `-storageNode`: vlstorage mode (accepts all endpoints, stores locally)
- With `-storageNode`: vlinsert + vlselect mode (forwards to remote nodes)

This eliminates version mismatch risks between different component binaries and simplifies deployment.

### 2. Dual-Path Routing

Every storage operation (`MustAddRows`, `RunQuery`, `GetFieldNames`, etc.) checks `localStorage != nil` to decide whether to route locally or to network storage. This makes the cluster mode completely transparent to all higher-level code (protocol handlers, query engine, etc.).

### 3. Buffered Insert with Background Flushing

vlinsert batches rows into 2 MB blocks before sending to vlstorage. A background goroutine flushes pending data every second to bound latency. The `pendingDataBuffers` channel acts as both a buffer pool and a concurrency limiter.

### 4. Stream-Aware Sharding

The sharding strategy balances data locality (small streams on one node) with parallel query performance (large streams spread across nodes). The 1000-row threshold provides a simple heuristic that works well for typical log workloads.

### 5. HA Re-routing for Ingestion

When a vlstorage node becomes unavailable, vlinsert re-routes pending data to any other available node. This keeps ingestion available while the process is running, at the cost of temporary data imbalance until the node returns. If all nodes remain unavailable until shutdown, buffered data can still be dropped.

### 6. Query Fan-out with Pipe Splitting

Distributed queries are automatically split into remote pipes (executed in parallel on all storage nodes) and local pipes (executed on the frontend after merging). Each pipe type declares its own splitting strategy via `splitToRemoteAndLocal()`, making the system extensible.

### 7. Binary Protocol with zstd Compression

Inter-node communication uses custom binary serialization with zstd compression (level 1). This is significantly more efficient than JSON for both CPU and bandwidth. Compression can be disabled per-direction (`-insert.disableCompression`, `-select.disableCompression`) when nodes are on a fast network.

### 8. Protocol Version Guards

Versioned internal endpoints verify protocol compatibility between sender and receiver. This prevents silent data corruption when cluster components are at different versions, providing a clear error message instead.

---

## Configuration Flags

### Cluster Mode

```bash
-storageNode array
    Comma-separated list of vlstorage node addresses.
    Setting this flag activates cluster mode (vlinsert + vlselect).
    Example: -storageNode=storage-1:9428,storage-2:9428
```

### Insert Performance

```bash
-insert.concurrency int
    Average number of concurrent data ingestion requests per -storageNode (default: 2)
    Controls the pendingDataBuffers channel capacity = concurrency * len(storageNodes)

-insert.disableCompression
    Disable zstd compression when sending data to -storageNode nodes (default: false)
    Reduces CPU at the cost of higher network bandwidth

-internalinsert.maxRequestSize bytes
    Maximum request body size for /internal/insert (default: 64MB)
```

### Select Performance

```bash
-select.disableCompression
    Disable zstd compression for query responses from -storageNode nodes (default: false)

-search.allowPartialResponse
    Allow partial responses when some storage nodes are unavailable (default: false)

-internalselect.maxConcurrentRequests int
    Concurrent request limit for /internal/select/* endpoints at vlstorage (default: 100)
```

### Endpoint Control

```bash
-insert.disable
    Disable all /insert/* and /internal/insert HTTP endpoints (default: false)

-select.disable
    Disable all /select/* and /internal/select/* HTTP endpoints (default: false)

-internalinsert.disable
    Disable /internal/insert HTTP endpoint (default: false)

-internalselect.disable
    Disable /internal/select/* HTTP endpoints (default: false)

-internaldelete.enable
    Enable /internal/delete/* HTTP endpoints (default: false)
```

### Authentication (per storage node)

```bash
-storageNode.username array        Basic auth username
-storageNode.usernameFile array    Path to basic auth username (re-read every second)
-storageNode.password array        Basic auth password
-storageNode.passwordFile array    Path to basic auth password (re-read every second)
-storageNode.bearerToken array     Bearer token
-storageNode.bearerTokenFile array Path to bearer token (re-read every second)
```

### TLS (per storage node)

```bash
-storageNode.tls array                     Enable HTTPS for storage node communication
-storageNode.tlsCAFile array               Path to TLS CA file for verifying storage nodes
-storageNode.tlsCertFile array             Path to client TLS certificate (for mTLS)
-storageNode.tlsKeyFile array              Path to client TLS key (for mTLS)
-storageNode.tlsServerName array           TLS server name override
-storageNode.tlsInsecureSkipVerify array   Skip TLS verification (not recommended for production)
```

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
- Use relative paths that are valid from this document's location (`onboarding/`)
- Include line numbers wherever possible

#### Standard Patterns

**1. Section Headers - File References**

```markdown
**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L109)
```

**2. Function References in Key Functions Lists**

```markdown
**Key Functions**:
- [`Init()`](../app/vlstorage/main.go#L109) - Initializes storage mode
- [`initNetworkStorage()`](../app/vlstorage/main.go#L164) - Creates network storage
```

**3. Flow Diagram References**

```markdown
vlstorage.RunQuery                    [main.go:554](../app/vlstorage/main.go#L554)
```

**4. Location References**

```markdown
**Location**: [`app/vlstorage/main.go:109-183`](../app/vlstorage/main.go#L109)
```

#### Verification Checklist

Before committing changes to this document:

- [ ] All file paths are valid relative to this document location
- [ ] All line number anchors use `#L` prefix (capital L)
- [ ] All links tested and navigate correctly in VS Code
- [ ] No plain text file references (should be clickable markdown links)
- [ ] Display text is appropriate for context (full path, filename, or function)

#### Tools for Validation

You can verify all links have the correct format using:

```bash
# Check for links missing the #L prefix
grep -n '\](.*\.go#[0-9]' onboarding/onboarding-cluster.md

# Count total clickable line references
grep -o "#L[0-9]\+" onboarding/onboarding-cluster.md | wc -l

# Find non-clickable file references
grep -n '`app/.*\.go`' onboarding/onboarding-cluster.md | grep -v '\]('
```
