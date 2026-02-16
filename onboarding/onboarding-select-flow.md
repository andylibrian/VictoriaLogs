# VictoriaLogs Query/Select Flow - Developer Onboarding Guide

This document provides a comprehensive overview of how log queries flow through VictoriaLogs from HTTP select endpoints through query parsing, [pipe](./glossary.md#pipe) execution, and result delivery back to the client. For parser internals, see [VictoriaLogs LogsQL Parser & Pipe Execution](./onboarding-logsql-parser-pipes.md).

## Table of Contents

- [Overview](#overview)
- [Complete Data Flow](#complete-data-flow)
- [Architecture Layers](#architecture-layers)
  - [1. HTTP Endpoint Layer](#1-http-endpoint-layer)
  - [2. Common Query Arguments](#2-common-query-arguments)
  - [3. Concurrency Control](#3-concurrency-control)
  - [4. Query Endpoint Handlers](#4-query-endpoint-handlers)
  - [5. Storage Router](#5-storage-router)
  - [6. Local Query Execution](#6-local-query-execution)
  - [7. Result Delivery](#7-result-delivery)
- [Deployment Modes](#deployment-modes)
  - [Local Storage Mode](#local-storage-mode)
  - [Distributed Storage Mode](#distributed-storage-mode)
- [Internal Select Endpoint](#internal-select-endpoint)
- [Specialized Query Types](#specialized-query-types)
  - [Live Tailing](#live-tailing)
  - [Last N Results Optimization](#last-n-results-optimization)
  - [Delete Operations](#delete-operations)
- [Key Design Patterns](#key-design-patterns)
- [Configuration Flags](#configuration-flags)

---

## Overview

VictoriaLogs exposes multiple HTTP endpoints under `/select/logsql/*` for querying log data. Most of these endpoints accept a LogsQL `query` string, parse it into an internal query representation, execute it against either local or distributed storage, and return JSON results.

Important exceptions:
- `/select/tenant_ids` does not accept a LogsQL query. It scans [tenant](./glossary.md#tenant--tenantid) IDs over a time range.
- `/select/logsql/query_time_range` parses query time bounds but does not execute a storage scan.

### Query Endpoints at a Glance

| Endpoint | Purpose | Response Format |
|----------|---------|----------------|
| `/select/logsql/query` | Full log query with streaming results | `application/stream+json` ([NDJSON](./glossary.md#ndjson)) |
| `/select/logsql/hits` | Count log hits over time buckets | `application/json` |
| `/select/logsql/stats_query` | Aggregate statistics (instant) | `application/json` (Prometheus-style) |
| `/select/logsql/stats_query_range` | Aggregate statistics over time | `application/json` (Prometheus-style) |
| `/select/logsql/facets` | Field value facets with hit counts | `application/json` |
| `/select/logsql/field_names` | List field names | `application/json` |
| `/select/logsql/field_values` | List values for a specific field | `application/json` |
| `/select/logsql/streams` | List log streams | `application/json` |
| `/select/logsql/stream_ids` | List stream IDs | `application/json` |
| `/select/logsql/stream_field_names` | List stream field names | `application/json` |
| `/select/logsql/stream_field_values` | List values for a stream field | `application/json` |
| `/select/logsql/tail` | Live tailing (long-lived [NDJSON](./glossary.md#ndjson) stream) | `application/x-ndjson` |
| `/select/logsql/query_time_range` | Return the effective time range for a query | `application/json` |
| `/select/tenant_ids` | List tenant IDs (requires empty `AccountID` header) | `application/json` |

### Deployment Modes

The system supports two deployment modes — the same dual-path architecture used for ingestion:
- **Local Mode**: Single node querying data from local disk
- **Distributed Mode**: Frontend (vlselect) fans out queries to storage nodes (vlstorage) via `/internal/select/*` in [cluster mode](./onboarding-cluster.md)

### Stream Endpoint Prerequisite

`/select/logsql/streams`, `/select/logsql/stream_ids`, `/select/logsql/stream_field_names`, and `/select/logsql/stream_field_values` are most useful when [stream](./glossary.md#stream--streamid)-level fields are configured at ingestion via `_stream_fields`. Otherwise `_stream` defaults to `{}`, which limits stream-level query value and can hurt selectivity.

---

## Complete Data Flow

**Key Files**:
- [`app/vlselect/main.go`](../app/vlselect/main.go#L90) - HTTP routing and concurrency control
- [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1149) - Query endpoint handlers
- [`app/vlstorage/main.go`](../app/vlstorage/main.go#L554) - Storage router
- [`app/vlstorage/lastnoptimization.go`](../app/vlstorage/lastnoptimization.go#L15) - Last N results optimization
- [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L31) - Internal select endpoint
- [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L385) - Distributed query network layer
- [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L208) - Local storage query execution
- [`lib/logstorage/net_query_runner.go`](../lib/logstorage/net_query_runner.go#L12) - Distributed query runner

```
HTTP Request (GET/POST /select/logsql/query?query=...)
    ↓
vlselect.RequestHandler                   [main.go:90](../app/vlselect/main.go#L90)
    ↓
selectHandler                             [main.go:138](../app/vlselect/main.go#L138)
    ↓
Timeout + Concurrency Control             [main.go:192-204](../app/vlselect/main.go#L192)
    ↓
processSelectRequest (route by path)      [main.go:286](../app/vlselect/main.go#L286)
    ↓
logsql.ProcessQueryRequest                [logsql.go:1149](../app/vlselect/logsql/logsql.go#L1149)
    ↓
parseCommonArgs (query, tenant, time)     [logsql.go:1358](../app/vlselect/logsql/logsql.go#L1358)
    ↓
ca.newQueryContext(ctx)                   [logsql.go:1350](../app/vlselect/logsql/logsql.go#L1350)
    ↓
vlstorage.RunQuery(qctx, writeBlock)     [main.go:554](../app/vlstorage/main.go#L554)
    ↓
    ├─→ [LAST-N OPTIMIZATION]
    │   runOptimizedLastNResultsQuery     [lastnoptimization.go:15](../app/vlstorage/lastnoptimization.go#L15)
    │       ↓
    │   Binary search over time range
    │       ↓
    │   writeBlock(0, &db)
    │
    ├─→ [LOCAL MODE]
    │   localStorage.RunQuery(qctx, writeBlock)  [storage_search.go:208](../lib/logstorage/storage_search.go#L208)
    │       ↓
    │   initSubqueries + getSearchOptions [storage_search.go:216](../lib/logstorage/storage_search.go#L216)
    │       ↓
    │   runPipes(qctx, pipes, search, writeBlock)
    │       ↓
    │   searchParallel across partitions  [storage_search.go:1274](../lib/logstorage/storage_search.go#L1274)
    │       ↓
    │   Block scanning + filter matching
    │       ↓
    │   writeBlock(workerID, &db)         → streamed to client as JSON
    │
    └─→ [DISTRIBUTED MODE]
        netstorageSelect.RunQuery(qctx, writeBlock)  [netselect.go:385](../app/vlstorage/netselect/netselect.go#L385)
            ↓
        NewNetQueryRunner (split remote/local pipes) [net_query_runner.go:31](../lib/logstorage/net_query_runner.go#L31)
            ↓
        Fan out to all storage nodes in parallel     [netselect.go:400](../app/vlstorage/netselect/netselect.go#L400)
            ↓
        POST http://storageNode/internal/select/query?version=v4
            ↓
        [See "Internal Select Endpoint" section]
            ↓
        Merge results + apply local pipes
            ↓
        writeBlock(workerID, &db)         → streamed to client as JSON
```

---

## Architecture Layers

### 1. HTTP Endpoint Layer

**File**: [`app/vlselect/main.go`](../app/vlselect/main.go#L90)

The top-level router dispatches incoming requests to the appropriate handler based on the URL path.

**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlselect/main.go#L90) - Main HTTP router for all `/select/*`, `/delete/*`, and `/internal/select/*` paths
- [`selectHandler(w, r, path)`](../app/vlselect/main.go#L138) - Handles `/select/*` paths with timeout and concurrency control
- [`processSelectRequest(ctx, w, r, path)`](../app/vlselect/main.go#L286) - Routes to specific logsql handler by path

```go
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
    path := strings.ReplaceAll(r.URL.Path, "//", "/")

    if strings.HasPrefix(path, "/delete/") {
        if !*enableDelete {
            httpserver.Errorf(w, r, "requests to /delete/* are disabled")
            return true
        }
        deleteHandler(w, r, path)
        return true
    }
    if strings.HasPrefix(path, "/select/") {
        if *disableSelect {
            httpserver.Errorf(w, r, "requests to /select/* are disabled")
            return true
        }
        return selectHandler(w, r, path)
    }
    if strings.HasPrefix(path, "/internal/delete/") {
        if !*enableInternalDelete {
            httpserver.Errorf(w, r, "requests to /internal/delete/* are disabled")
            return true
        }
        internalselect.RequestHandler(r.Context(), w, r)
        return true
    }
    if strings.HasPrefix(path, "/internal/select/") {
        if *disableInternalSelect || *disableSelect {
            httpserver.Errorf(w, r, "requests to /internal/select/* are disabled")
            return true
        }
        internalselect.RequestHandler(r.Context(), w, r)
        return true
    }
    return false
}
```

**Location**: [`app/vlselect/main.go:90-136`](../app/vlselect/main.go#L90)

---

### 2. Common Query Arguments

**File**: [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1326)

Most `/select/logsql/*` endpoints parse a shared set of arguments via `parseCommonArgs*` before executing the query.

**Key Functions**:
- [`parseCommonArgs(r)`](../app/vlselect/logsql/logsql.go#L1358) - Parse common query arguments from HTTP request
- [`parseCommonArgsWithConfig(r, skipMaxRangeCheck)`](../app/vlselect/logsql/logsql.go#L1362) - Full implementation

```go
type commonArgs struct {
    // The parsed query, including optional extra_filters, extra_stream_filters,
    // and (start, end) time range filter.
    q *logstorage.Query

    // tenantIDs is the list of tenantIDs to query.
    tenantIDs []logstorage.TenantID

    // Whether to allow partial response when some vlstorage nodes are unavailable.
    allowPartialResponse bool

    // Optional fields and field prefixes to hide during query execution.
    hiddenFieldsFilters []string

    // qs contains query execution statistics.
    qs logstorage.QueryStats

    // startAligned and endAligned are the time range aligned to step.
    startAligned int64
    endAligned   int64
}
```

**HTTP Parameters Mapping**:
- `query` → LogsQL query string, parsed via `logstorage.ParseQueryAtTimestamp()`
- `start` / `end` → Time range boundaries (RFC3339, Unix timestamp, or relative like `5m`)
- `time` → Evaluation timestamp (for `now()` in LogsQL)
- `timeout` → Per-request execution timeout, handled in `selectHandler` (capped by `-search.maxQueryDuration`)
- `extra_filters` → Additional LogsQL filters to AND with the query
- `extra_stream_filters` → Additional stream-level filters
- `allow_partial_response` → Allow partial results in cluster mode
- `hidden_fields_filters` → Fields/prefixes to hide from results
- AccountID/ProjectID headers → Tenant identification

**Query Parsing Flow**:
1. Extract `tenantID` from HTTP headers
2. Parse optional `start`, `end`, `time` args
3. Parse the `query` string via `logstorage.ParseQueryAtTimestamp(qStr, timestamp)`
4. Convert HTTP `end` from exclusive `[start, end)` to inclusive bound (`end-1ns`) for internal filtering
5. Apply `start`/`end` as a `_time` filter if provided
6. Align `start`/`end` to `step` (if `step` is set and parseable)
7. Apply `extra_filters` and `extra_stream_filters`
8. Parse `allow_partial_response` and `hidden_fields_filters`
9. Enforce `-search.maxQueryTimeRange` if set (unless explicitly skipped by handler)
10. Create `QueryContext` with parsed query + tenant context

Notes:
- If `time` is provided, parsing uses `time-1ns` internally to avoid boundary spillover into the next period.
- `/select/tenant_ids` does not use `parseCommonArgs*`; it has its own parsing and security checks.

**Location**: [`app/vlselect/logsql/logsql.go:1326-1500`](../app/vlselect/logsql/logsql.go#L1326)

---

### 3. Concurrency Control

**File**: [`app/vlselect/main.go`](../app/vlselect/main.go#L246)

VictoriaLogs limits concurrent query execution to prevent resource exhaustion. A single query can saturate all CPU cores, so the default limit is CPU-based and capped at 16 (`2*CPUs` only when `CPUs <= 4`).

**Key Functions**:
- [`incRequestConcurrency(ctx, w, r)`](../app/vlselect/main.go#L246) - Acquire concurrency slot (blocks until available or timeout)
- [`decRequestConcurrency()`](../app/vlselect/main.go#L282) - Release concurrency slot
- [`getMaxQueryDuration(r)`](../app/vlselect/main.go#L435) - Resolve query timeout

```go
func selectHandler(w http.ResponseWriter, r *http.Request, path string) bool {
    // Live tailing bypasses concurrency limit (runs indefinitely)
    if path == "/select/logsql/tail" {
        logsql.ProcessLiveTailRequest(ctx, w, r)
        return true
    }

    // All other queries: apply timeout + concurrency limit
    d, _ := getMaxQueryDuration(r)
    ctxWithTimeout, cancel := context.WithTimeout(ctx, d)
    defer cancel()

    if !incRequestConcurrency(ctxWithTimeout, w, r) {
        return true  // Timed out waiting for a slot
    }
    defer decRequestConcurrency()

    processSelectRequest(ctxWithTimeout, w, r, path)
    return true
}
```

The concurrency limiter uses a buffered channel as a semaphore:
```go
concurrencyLimitCh = make(chan struct{}, *maxConcurrentRequests)
```

If the channel is full, the request waits until a slot opens or the request context is canceled/deadline-exceeded. In this path, the deadline comes from `timeout` / `-search.maxQueryDuration`.

**Location**: [`app/vlselect/main.go:138-284`](../app/vlselect/main.go#L138)

---

### 4. Query Endpoint Handlers

**File**: [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1149)

Each query endpoint follows a common pattern:
1. Parse common args (`parseCommonArgs`)
2. Parse endpoint-specific args
3. Optionally modify the query (add pipes, drop pipes)
4. Create a `writeBlock` callback to collect/stream results
5. Execute via `vlstorage.RunQuery(qctx, writeBlock)` or a metadata function
6. Format and write the JSON response

#### `/select/logsql/query` — Full Log Query

**Key Functions**:
- [`ProcessQueryRequest(ctx, w, r)`](../app/vlselect/logsql/logsql.go#L1149) - Main query handler

```go
func ProcessQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
    ca, _ := parseCommonArgs(r)

    offset, _ := getPositiveInt(r, "offset")
    limit, _ := getPositiveInt(r, "limit")

    // If limit > 0, add sorting and pagination pipes
    if limit > 0 {
        if ca.q.CanReturnLastNResults() {
            ca.q.AddPipeSortByTimeDesc()
        }
        ca.q.AddPipeOffsetLimit(uint64(offset), uint64(limit))
    }

    // Stream results as NDJSON (one JSON object per line)
    writeBlock := func(workerID uint, db *logstorage.DataBlock) {
        for i := 0; i < db.RowsCount(); i++ {
            WriteJSONRow(bw, db.Columns, i)
        }
    }

    qctx := ca.newQueryContext(ctx)
    vlstorage.RunQuery(qctx, writeBlock)
}
```

This endpoint streams results as `application/stream+json` — each log row is written as a separate JSON object on a single line. This allows the client to start processing results before the query is complete.

**Location**: [`app/vlselect/logsql/logsql.go:1149-1231`](../app/vlselect/logsql/logsql.go#L1149)

#### `/select/logsql/hits` — Histogram Over Time

**Key Functions**:
- [`ProcessHitsRequest(ctx, w, r)`](../app/vlselect/logsql/logsql.go#L215) - Hit count handler

Adds a time-bucketed stats pipeline via `AddCountByTimePipe(step, offset, fields)`, which appends:
- `| stats by (_time:<step> [offset ...], <fields...>) count() hits`
- `| sort by (_time, <fields...>)`

Before this, unsafe trailing pipes that can alter/remove `_time` are dropped so bucketing remains correct.

**Location**: [`app/vlselect/logsql/logsql.go:215-316`](../app/vlselect/logsql/logsql.go#L215)

#### `/select/logsql/stats_query` — Instant Statistics

**Key Functions**:
- [`ProcessStatsQueryRequest(ctx, w, r)`](../app/vlselect/logsql/logsql.go#L1028) - Stats query handler

Expects the query to end with a stats pipe (e.g., `| stats count() as total`). Returns Prometheus-compatible instant vector results.

**Location**: [`app/vlselect/logsql/logsql.go:1028-1132`](../app/vlselect/logsql/logsql.go#L1028)

#### `/select/logsql/stats_query_range` — Range Statistics

**Key Functions**:
- [`ProcessStatsQueryRangeRequest(ctx, w, r)`](../app/vlselect/logsql/logsql.go#L854) - Range stats handler

Like `stats_query`, but it augments the final `| stats ...` grouping with `_time:<step> offset <offset>` via `GetStatsLabelsAddGroupingByTime(step, offset)`.

It does not add a separate `group_by_time` pipe.

**Location**: [`app/vlselect/logsql/logsql.go:854-1010`](../app/vlselect/logsql/logsql.go#L854)

#### `/select/logsql/facets` — Field Facets

**Key Functions**:
- [`ProcessFacetsRequest(ctx, w, r)`](../app/vlselect/logsql/logsql.go#L114) - Facets handler

Drops all pipes from the query and adds a facets pipe. Returns JSON in the form `{"facets":[...]}` with per-field top values and hits.

**Location**: [`app/vlselect/logsql/logsql.go:114-205`](../app/vlselect/logsql/logsql.go#L114)

#### Metadata Endpoints

These endpoints query for metadata rather than log rows:

- [`ProcessFieldNamesRequest`](../app/vlselect/logsql/logsql.go#L427) → calls `vlstorage.GetFieldNames(qctx)`
- [`ProcessFieldValuesRequest`](../app/vlselect/logsql/logsql.go#L458) → calls `vlstorage.GetFieldValues(qctx, fieldName, limit)`
- [`ProcessStreamFieldNamesRequest`](../app/vlselect/logsql/logsql.go#L503) → calls `vlstorage.GetStreamFieldNames(qctx)`
- [`ProcessStreamFieldValuesRequest`](../app/vlselect/logsql/logsql.go#L534) → calls `vlstorage.GetStreamFieldValues(qctx, fieldName, limit)`
- [`ProcessStreamIDsRequest`](../app/vlselect/logsql/logsql.go#L579) → calls `vlstorage.GetStreamIDs(qctx, limit)`
- [`ProcessStreamsRequest`](../app/vlselect/logsql/logsql.go#L617) → calls `vlstorage.GetStreams(qctx, limit)`
- [`ProcessTenantIDsRequest`](../app/vlselect/logsql/logsql.go#L1234) → calls `vlstorage.GetTenantIDs(ctx, start, end)`

External metadata responses are wrapped as `{"values":[{"value":"...","hits":N}, ...]}`.

Special case:
- `/select/tenant_ids` returns tenant objects (`[{ "account_id": ..., "project_id": ... }]`) and is forbidden when `AccountID` header is non-empty.

---

### 5. Storage Router

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L554)

The `vlstorage` package-level functions route queries to either local or distributed storage, mirroring the insert flow's dual-path pattern.

**Key Functions**:
- [`RunQuery(qctx, writeBlock)`](../app/vlstorage/main.go#L554) - Route query execution
- [`GetFieldNames(qctx)`](../app/vlstorage/main.go#L568) - Route field names query
- [`GetFieldValues(qctx, fieldName, limit)`](../app/vlstorage/main.go#L578) - Route field values query
- [`GetStreamFieldNames(qctx)`](../app/vlstorage/main.go#L586) - Route stream field names query
- [`GetStreamFieldValues(qctx, fieldName, limit)`](../app/vlstorage/main.go#L596) - Route stream field values query
- [`GetStreams(qctx, limit)`](../app/vlstorage/main.go#L606) - Route streams query
- [`GetStreamIDs(qctx, limit)`](../app/vlstorage/main.go#L616) - Route stream IDs query
- [`GetTenantIDs(ctx, start, end)`](../app/vlstorage/main.go#L660) - Route tenant IDs query

```go
// Mode selection variables (set at Init() time)
var localStorage *logstorage.Storage      // non-nil in local mode
var netstorageSelect *netselect.Storage   // non-nil in distributed mode

func RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
    // First: try the Last-N optimization (applies in both modes)
    qOpt, offset, limit := qctx.Query.GetLastNResultsQuery()
    if qOpt != nil {
        qctxOpt := qctx.WithQuery(qOpt)
        return runOptimizedLastNResultsQuery(qctxOpt, offset, limit, writeBlock)
    }

    // Route to appropriate backend
    if localStorage != nil {
        return localStorage.RunQuery(qctx, writeBlock)
    }
    return netstorageSelect.RunQuery(qctx, writeBlock)
}
```

All the `Get*` functions follow the same routing pattern:
```go
func GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
    if localStorage != nil {
        return localStorage.GetFieldNames(qctx)
    }
    return netstorageSelect.GetFieldNames(qctx)
}
```

**Location**: [`app/vlstorage/main.go:554-665`](../app/vlstorage/main.go#L554)

---

### 6. Local Query Execution

**File**: [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L208)

The core of VictoriaLogs' query engine. When running in local mode, queries execute directly against on-disk partitions.

**Key Types**:
- [`QueryContext`](../lib/logstorage/storage_search.go#L25) - Holds query, tenant IDs, context, stats
- [`WriteDataBlockFunc`](../lib/logstorage/storage_search.go#L176) - Callback for result blocks
- [`DataBlock`](../lib/logstorage/storage_search.go#L1093) - Columnar block of result rows (see [DataBlock](./glossary.md#datablock))
- [`BlockColumn`](../lib/logstorage/storage_search.go#L1084) - A single named column with string values

**Key Functions**:
- [`Storage.RunQuery(qctx, writeBlock)`](../lib/logstorage/storage_search.go#L208) - Entry point for local query
- [`Storage.runQuery(qctx, writeBlock)`](../lib/logstorage/storage_search.go#L216) - Internal query execution
- [`Storage.searchParallel(workersCount, sso, qs, stopCh, writeBlock)`](../lib/logstorage/storage_search.go#L1274) - Parallel partition scanning

```go
func (s *Storage) RunQuery(qctx *QueryContext, writeBlock WriteDataBlockFunc) error {
    // Convert public WriteDataBlockFunc to internal writeBlockResultFunc
    writeBlockResult := writeBlock.newBlockResultWriter()
    return s.runQuery(qctx, writeBlockResult)
}

func (s *Storage) runQuery(qctx *QueryContext, writeBlock writeBlockResultFunc) error {
    // 1. Initialize subqueries (nested queries in the LogsQL)
    qNew, err := initSubqueries(qctx, s.runQuery, true)

    // 2. Build search options (tenant filter, time range, stream filter, etc.)
    sso := s.getSearchOptions(qctx.TenantIDs, q, qctx.HiddenFieldsFilters)

    // 3. Define the search function
    search := func(stopCh <-chan struct{}, writeBlockToPipes writeBlockResultFunc) error {
        workersCount := q.GetParallelReaders(s.defaultParallelReaders)
        s.searchParallel(workersCount, sso, qctx.QueryStats, stopCh, writeBlockToPipes)
        return nil
    }

    // 4. Execute pipes (filter → transform → aggregate → output)
    concurrency := q.GetConcurrency()
    return runPipes(qctx, q.pipes, search, writeBlock, concurrency)
}
```

**Parallel Search Architecture**:

```go
func (s *Storage) searchParallel(workersCount int, sso *storageSearchOptions, ...) {
    // 1. Spin up worker goroutines
    workCh := make(chan *blockSearchWorkBatch, workersCount)
    for workerID := range workersCount {
        // Each worker: reads blocks from workCh, applies filter, writes matching rows
        go func() {
            for bswb := range workCh {
                for _, bsw := range bswb.bsws {
                    bs.search(qsLocal, bsw, bm)  // scan block, apply filter
                    if bs.br.rowsLen > 0 {
                        writeBlock(uint(workerID), &bs.br)  // send matching rows
                    }
                }
            }
        }()
    }

    // 2. Select partitions by time range
    ptws := s.getPartitionsForTimeRange(sso.minTimestamp, sso.maxTimestamp)

    // 3. Search partitions concurrently
    for _, ptw := range ptws {
        ptw.pt.search(sso, qsLocal, workCh, stopCh)  // enqueues blocks to workCh
    }

    // 4. Close workCh to signal completion
    close(workCh)
}
```

The search works in a producer-consumer model:
- **Producers**: Partition searchers scan indexdb to find matching stream IDs and block headers, then enqueue block search work items.
- **Consumers**: Worker goroutines read blocks from disk (datadb), decompress them, apply the row-level filter, and invoke `writeBlock` for matching rows.

**Location**: [`lib/logstorage/storage_search.go:208-1340`](../lib/logstorage/storage_search.go#L208)

---

### 7. Result Delivery

**File**: [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L1093)

Query results flow through `DataBlock` objects — a columnar representation of log rows.

```go
type BlockColumn struct {
    Name   string    // Column name (e.g., "_time", "_msg", "host")
    Values []string  // Column values (one per row)
}

type DataBlock struct {
    Columns []BlockColumn
}
```

**Key Methods**:
- [`RowsCount()`](../lib/logstorage/storage_search.go#L1105) - Number of rows in the block
- [`GetTimestamps(dst)`](../lib/logstorage/storage_search.go#L1116) - Extract `_time` column as int64 nanoseconds
- [`GetColumnByName(name)`](../lib/logstorage/storage_search.go#L1127) - Find column by name
- [`Marshal(dst)`](../lib/logstorage/storage_search.go#L1139) - Serialize for network transfer
- [`UnmarshalInplace(src, valuesBuf)`](../lib/logstorage/storage_search.go#L1177) - Deserialize from network transfer

For the `/select/logsql/query` endpoint, each `DataBlock` is immediately serialized as NDJSON rows and streamed to the client. For aggregation endpoints (`hits`, `stats_query`, etc.), results are accumulated in memory, post-processed (sorted, merged), and then sent as a complete JSON response.

**Location**: [`lib/logstorage/storage_search.go:1084-1240`](../lib/logstorage/storage_search.go#L1084)

---

## Deployment Modes

### Local Storage Mode

**Activation**: Start VictoriaLogs without `-storageNode` flag.

```bash
./victoria-logs -storageDataPath=/data/victoria-logs-data
```

**Data Flow**:
```
HTTP Request (GET /select/logsql/query?query=...)
    ↓
vlselect.RequestHandler
    ↓
Concurrency control (channel semaphore)
    ↓
logsql.ProcessQueryRequest
    ↓
parseCommonArgs → logstorage.ParseQueryAtTimestamp
    ↓
vlstorage.RunQuery(qctx, writeBlock)
    ↓
localStorage.RunQuery(qctx, writeBlock)
    ↓
initSubqueries + getSearchOptions
    ↓
searchParallel (across day partitions)
    ↓
For each partition:
    indexdb → find matching streams and block headers
    datadb → read + decompress blocks
    Apply row-level filter
    ↓
runPipes (execute query pipes: transform, aggregate, sort, limit)
    ↓
writeBlock(workerID, &DataBlock)
    ↓
Stream JSON to client
```

**Characteristics**:
- Single node, direct disk access
- Full pipe execution locally
- No network overhead for queries
- Limited by single machine resources

---

### Distributed Storage Mode

**Activation**: Start VictoriaLogs with `-storageNode` flag.

```bash
# Frontend node (vlselect - accepts HTTP requests, executes queries)
./victoria-logs \
    -storageNode=storage-1:9428,storage-2:9428,storage-3:9428

# Storage nodes (vlstorage - store and scan data)
./victoria-logs -storageDataPath=/data/victoria-logs-data
```

**Initialization**:

**File**: [`app/vlstorage/main.go`](../app/vlstorage/main.go#L164) (lines 164-183):
```go
func initNetworkStorage() {
    netstorageSelect = netselect.NewStorage(
        *storageNodeAddrs,          // ["storage-1:9428", "storage-2:9428"]
        authCfgs,                   // Optional auth
        isTLSs,                     // HTTP or HTTPS
        *selectDisableCompression,  // Default: false (compression enabled)
    )
}
```

**Query Splitting**:

**File**: [`lib/logstorage/net_query_runner.go`](../lib/logstorage/net_query_runner.go#L31)

The distributed query engine splits the LogsQL query into **remote pipes** (executed on each storage node) and **local pipes** (executed on the frontend after merging results).

**Key Functions**:
- [`NewNetQueryRunner(qctx, runNetQuery, writeNetBlock)`](../lib/logstorage/net_query_runner.go#L31) - Create distributed query runner
- [`Run(ctx, concurrency, netSearch)`](../lib/logstorage/net_query_runner.go#L61) - Execute distributed query
- [`splitQueryToRemoteAndLocal(q)`](../lib/logstorage/net_query_runner.go#L72) - Split query pipes

```go
func NewNetQueryRunner(qctx *QueryContext, runNetQuery RunNetQueryFunc,
    writeNetBlock WriteDataBlockFunc) (*NetQueryRunner, error) {

    // Initialize subqueries
    qNew, _ := initSubqueries(qctx, runQuery, false)

    // Split: some pipes run on storage nodes, others run locally after merge
    qRemote, pipesLocal := splitQueryToRemoteAndLocal(q)

    return &NetQueryRunner{
        qctx:       qctx,
        qRemote:    qRemote,
        pipesLocal: pipesLocal,
        writeBlock: writeBlock,
    }, nil
}

func (nqr *NetQueryRunner) Run(ctx context.Context, concurrency int,
    netSearch func(stopCh <-chan struct{}, q *Query, writeBlock WriteDataBlockFunc) error) error {

    search := func(stopCh <-chan struct{}, writeBlockToPipes writeBlockResultFunc) error {
        writeNetBlock := writeBlockToPipes.newDataBlockWriter()
        return netSearch(stopCh, nqr.qRemote, writeNetBlock)
    }
    return runPipes(qctxLocal, nqr.pipesLocal, search, nqr.writeBlock, concurrency)
}
```

**Fan-out to Storage Nodes**:

**File**: [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L385)

**Key Functions**:
- [`Storage.RunQuery(qctx, writeBlock)`](../app/vlstorage/netselect/netselect.go#L385) - Entry point for distributed query
- [`Storage.runQuery(stopCh, qctx, writeBlock)`](../app/vlstorage/netselect/netselect.go#L400) - Fan-out implementation
- [`storageNode.runQuery(qctx, processBlock)`](../app/vlstorage/netselect/netselect.go#L132) - Query a single storage node

```go
func (s *Storage) RunQuery(qctx *logstorage.QueryContext,
    writeBlock logstorage.WriteDataBlockFunc) error {
    nqr, _ := logstorage.NewNetQueryRunner(qctx, s.RunQuery, writeBlock)

    search := func(stopCh <-chan struct{}, q *logstorage.Query,
        writeBlock logstorage.WriteDataBlockFunc) error {
        qctxLocal := qctx.WithQuery(q)
        return s.runQuery(stopCh, qctxLocal, writeBlock)
    }

    concurrency := qctx.Query.GetConcurrency()
    return nqr.Run(qctx.Context, concurrency, search)
}

func (s *Storage) runQuery(stopCh <-chan struct{}, qctx *logstorage.QueryContext,
    writeBlock logstorage.WriteDataBlockFunc) error {
    // Fan out to ALL storage nodes in parallel
    var wg sync.WaitGroup
    for nodeIdx := range s.sns {
        wg.Go(func() {
            sn := s.sns[nodeIdx]
            sn.runQuery(qctxLocal, func(db *logstorage.DataBlock) {
                writeBlock(uint(nodeIdx), db)
            })
        })
    }
    wg.Wait()
    return getFirstError(errs, qctx.AllowPartialResponse)
}
```

**Binary Streaming Protocol**:

Each storage node receives the query via HTTP POST and streams results back as a binary protocol:

```go
func (sn *storageNode) runQuery(qctx *logstorage.QueryContext,
    processBlock func(db *logstorage.DataBlock)) error {
    // Send query to storage node
    args := sn.getCommonArgs(QueryProtocolVersion, qctx)
    responseBody, _, _ := sn.getResponseBodyForPathAndArgs(ctx, "/internal/select/query", args)

    // Read streaming response: [8-byte length][compressed data block]...
    for {
        io.ReadFull(responseBody, dataLenBuf[:])  // Read block size
        blockLen := encoding.UnmarshalUint64(dataLenBuf[:])

        io.ReadFull(responseBody, buf[:blockLen])  // Read block data
        src := decompress(buf)                     // zstd decompress

        for len(src) > 0 {
            if src[0] == 1 {
                // Query stats block (sent last)
                unmarshalQueryStats(qsLocal, src[1:])
            } else {
                // Regular data block
                db.UnmarshalInplace(src[1:], valuesBuf)
                processBlock(&db)
            }
        }
    }
}
```

**Metadata Queries (GetFieldNames, GetStreams, etc.)**:

For metadata queries, all storage nodes are queried in parallel and results are merged:

```go
func (s *Storage) GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
    return s.getValuesWithHits(qctx, 0, false, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
        return sn.getFieldNames(qctx.WithContext(ctx))
    })
}

func (s *Storage) getValuesWithHits(qctx, limit, resetHits, callback) ([]logstorage.ValueWithHits, error) {
    // Fan-out to all nodes
    for nodeIdx := range s.sns {
        results[nodeIdx], errs[nodeIdx] = callback(ctx, s.sns[nodeIdx])
    }
    // Merge results from all nodes
    return logstorage.MergeValuesWithHits(results, limit, resetHitsOnLimitExceeded), nil
}
```

**Partial Response Handling**:

When `-search.allowPartialResponse` is enabled and some storage nodes are unavailable:
- The query proceeds with available nodes
- If all nodes are unavailable, an error is returned
- Configuration errors (non-network) always return errors regardless of the flag

**Location**: [`app/vlstorage/netselect/netselect.go:81-837`](../app/vlstorage/netselect/netselect.go#L81)

---

## Internal Select Endpoint

The `/internal/select/*` endpoints are used for inter-node communication in distributed mode. Storage nodes expose these endpoints for the frontend (vlselect) to query.

### Endpoint Registration

**File**: [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L79)

```go
var requestHandlers = map[string]func(ctx context.Context, w http.ResponseWriter, r *http.Request) error{
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

### Request Handler

**File**: [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L31)

**Key Functions**:
- [`RequestHandler(ctx, w, r)`](../app/vlselect/internalselect/internalselect.go#L31) - Concurrency-limited dispatcher
- [`requestHandler(ctx, w, r, startTime)`](../app/vlselect/internalselect/internalselect.go#L62) - Route to handler by path
- [`getCommonParams(r, version)`](../app/vlselect/internalselect/internalselect.go#L431) - Parse internal query params
- [`processQueryRequest(ctx, w, r)`](../app/vlselect/internalselect/internalselect.go#L94) - Query handler

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

### Query Response Format

The `/internal/select/query` handler streams results as a binary protocol:

```go
func processQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
    cp, _ := getCommonParams(r, netselect.QueryProtocolVersion)

    writeBlock := func(workerID uint, db *logstorage.DataBlock) {
        bb.B = append(bb.B, 0)       // Marker: regular data block
        bb.B = db.Marshal(bb.B)       // Serialize DataBlock

        if len(bb.B) >= 1024*1024 {   // Flush when buffer >= 1MB
            sendBuf(bb)               // Compress + send with length prefix
        }
    }

    vlstorage.RunQuery(qctx, writeBlock)

    // Send remaining data + query stats block (marker byte = 1)
    bb.B = append(bb.B, 1)           // Marker: query stats block
    bb.B = marshalQueryStatsBlock(bb.B, qctx)
    sendBuf(bb)
}
```

### Metadata Response Format

For metadata queries (`field_names`, `streams`, etc.), the response is a single compressed binary blob:

```go
func writeValuesWithHits(w http.ResponseWriter, qctx *logstorage.QueryContext, vhs []logstorage.ValueWithHits, disableCompression bool) error {
    var b []byte
    b = encoding.MarshalUint64(b, uint64(len(vhs)))  // Number of items
    for i := range vhs {
        b = vhs[i].Marshal(b)                         // Serialize each ValueWithHits
    }
    b = marshalQueryStatsBlock(b, qctx)               // Append query stats
    if !disableCompression {
        b = zstd.CompressLevel(nil, b, 1)             // zstd compress
    }
    w.Write(b)
}
```

### Key Differences: `/select/logsql/*` vs `/internal/select/*`

| Aspect | `/select/logsql/*` | `/internal/select/*` |
|--------|-------------------|---------------------|
| **Purpose** | Accept queries from external clients | Accept queries from vlselect frontend nodes |
| **Response Format** | JSON (streaming NDJSON or complete JSON) | Binary protocol (`DataBlock` stream for query, compact values/hits blob for metadata) |
| **Tenant ID** | Extracted from HTTP headers | Passed as `tenant_ids` query param |
| **Query Parameters** | User-friendly (`start`, `end`, `query`, `extra_filters`, etc.) | Pre-processed (`tenant_ids`, `timestamp`, `query`, `hidden_fields_filters`, etc.) |
| **Pipe Processing** | Full query with all pipes | For `/internal/select/query`, receives already split remote query from frontend; metadata internal endpoints execute their own storage-side query helpers |
| **Compression** | Not applicable (JSON text) | zstd compression (configurable) |
| **Security Flag** | `-select.disable` | `-internalselect.disable` |
| **Concurrency Limit** | `-search.maxConcurrentRequests` (default: CPU-based, capped at 16) | `-internalselect.maxConcurrentRequests` (default: 100) |

### Why Two Different Endpoints?

1. **Response Format**: External endpoints return human-readable JSON; internal endpoints use efficient binary serialization with zstd compression

2. **Query Splitting**: In distributed mode, `/internal/select/query` receives the pre-split remote query; the frontend merges and runs remaining local pipes. Internal metadata endpoints use dedicated storage-side metadata execution paths.

3. **Security**: Internal endpoints can be disabled separately to prevent external access to cluster internals

4. **Protocol Versioning**: Internal endpoints use version strings (e.g., `v4`) to detect version mismatches between cluster components

**Location**: [`app/vlselect/internalselect/internalselect.go:31-558`](../app/vlselect/internalselect/internalselect.go#L31)

---

## Specialized Query Types

### Live Tailing

**File**: [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L655)

**Key Functions**:
- [`ProcessLiveTailRequest(ctx, w, r)`](../app/vlselect/logsql/logsql.go#L655) - Live tail handler

Live tailing provides a streaming connection that periodically polls for new log entries:

```go
func ProcessLiveTailRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
    // Bypasses concurrency limit and timeout — runs indefinitely
    ca, _ := parseCommonArgsWithConfig(r, true)

    refreshInterval := parseDuration(r, "refresh_interval", "1s")
    startOffset := parseDuration(r, "start_offset", "5s")
    offset := parseDuration(r, "offset", "5s")

    w.Header().Set("Content-Type", "application/x-ndjson")

    for {
        // Poll for new entries in [start, end] time range
        q = qOrig.CloneWithTimeFilter(end, start, end)
        vlstorage.RunQuery(qctxLocal, tp.writeBlock)

        // De-duplicate using per-stream last-seen timestamps
        resultRows, _ := tp.getTailRows()
        WriteJSONRows(w, resultRows)
        flusher.Flush()

        // Wait for next poll interval
        select {
        case <-doneCh: return
        case <-ticker.C:
            start = end - tailOffsetNsecs
            end = time.Now().UnixNano() - offset
        }
    }
}
```

**Key Design Decisions**:
- **No concurrency limit**: Tail requests are long-lived and lightweight between polls
- **No timeout**: Runs until client disconnects
- **Per-stream deduplication**: Tracks last-seen timestamp per stream to avoid duplicates across poll intervals
- **5-second overlap**: Each poll looks back 5 seconds to catch late-arriving logs

**Location**: [`app/vlselect/logsql/logsql.go:655-849`](../app/vlselect/logsql/logsql.go#L655)

---

### Last N Results Optimization

**File**: [`app/vlstorage/lastnoptimization.go`](../app/vlstorage/lastnoptimization.go#L15)

When a query requests the last N results (detected by `query.GetLastNResultsQuery()`), VictoriaLogs uses a binary search over time ranges instead of scanning all data:

**Key Functions**:
- [`runOptimizedLastNResultsQuery(qctx, offset, limit, writeBlock)`](../app/vlstorage/lastnoptimization.go#L15) - Entry point
- [`getLastNQueryResults(qctx, limit)`](../app/vlstorage/lastnoptimization.go#L41) - Binary search implementation
- [`getLogRowsLastN(qctx, start, end, n)`](../app/vlstorage/lastnoptimization.go#L124) - Fallback for small ranges

```go
func getLastNQueryResults(qctx *logstorage.QueryContext, limit uint64) ([]logRow, error) {
    // First attempt: query with 2*limit to check if time range is small enough
    q := qctx.Query.Clone(timestamp)
    q.AddPipeOffsetLimit(0, 2*limit)
    rows, _ := getQueryResults(qctxLocal)

    if uint64(len(rows)) < 2*limit {
        // Fast path: time range has fewer than 2*limit rows
        return getLastNRows(rows, limit), nil
    }

    // Slow path: binary search over the time range
    // Repeatedly halve the time range until we find ~limit rows
    for {
        q = qctx.Query.CloneWithTimeFilter(timestamp, start, end)
        rows, _ := getQueryResults(qctxLocal)

        if uint64(len(rows)) >= 2*n {
            start += end/2 - start/2  // Narrow: search more recent half
        } else if uint64(len(rowsFound)+len(rows)) >= limit {
            return getLastNRows(rowsFound, limit), nil  // Found enough
        } else {
            // Expand: search older half
            end = start
            start -= d
        }
    }
}
```

This optimization is triggered when the query ends with `| sort by (_time) desc | limit N` (or the equivalent implicit pattern), which is the common case for "show me the latest logs" queries.

**Location**: [`app/vlstorage/lastnoptimization.go:15-209`](../app/vlstorage/lastnoptimization.go#L15)

---

### Delete Operations

**File**: [`app/vlselect/main.go`](../app/vlselect/main.go#L359)

Delete operations are exposed under `/delete/*` and execute asynchronously:

**Key Functions**:
- [`processDeleteRunTaskRequest(ctx, w, r)`](../app/vlselect/main.go#L377) - Start a delete task
- [`processDeleteStopTaskRequest(ctx, w, r)`](../app/vlselect/main.go#L405) - Stop a running delete task
- [`processDeleteActiveTasksRequest(ctx, w, r)`](../app/vlselect/main.go#L421) - List active delete tasks

```
POST /delete/run_task?filter=<LogsQL filter>
    ↓
Parse filter, generate taskID from timestamp
    ↓
vlstorage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f)
    ↓
├── [LOCAL]  localStorage.DeleteRunTask(...)
└── [DISTRIBUTED]  netstorageSelect.DeleteRunTask(...) → POST to all storage nodes
    ↓
Returns {"task_id":"..."}
```

Delete tasks run in the background and can be monitored via `/delete/active_tasks` and stopped via `/delete/stop_task`.

**Location**: [`app/vlselect/main.go:359-432`](../app/vlselect/main.go#L359)

---

## Key Design Patterns

### 1. Callback-Based Streaming

Results flow through the system via `WriteDataBlockFunc` callbacks rather than being accumulated in memory. This enables streaming large result sets without holding everything in memory:

```go
type WriteDataBlockFunc func(workerID uint, db *DataBlock)
```

Each layer passes a callback to the next layer down. The bottom layer (partition searcher) calls the callback for each matching block, and results propagate up through pipe processing to the HTTP response writer.

### 2. Query Splitting for Distributed Execution

In cluster mode, a LogsQL query is automatically split into remote and local portions:

```go
qRemote, pipesLocal := splitQueryToRemoteAndLocal(q)
```

- **Remote pipes**: Executed on each storage node (filtering, initial aggregation)
- **Local pipes**: Executed on the frontend after merging results (final aggregation, sorting, limiting)

Each pipe type implements `splitToRemoteAndLocal()` to declare how it can be distributed.

### 3. Columnar DataBlock Transfer

Results are transferred in columnar `DataBlock` format, not row-by-row:

```go
type DataBlock struct {
    Columns []BlockColumn  // Each column: Name + []string Values
}
```

This is efficient for both local processing (column-oriented access patterns) and network transfer (better compression of similar values).

### 4. Concurrency Channel Semaphore

Query concurrency is limited using Go's buffered channels as semaphores:

```go
concurrencyLimitCh = make(chan struct{}, *maxConcurrentRequests)

// Acquire
concurrencyLimitCh <- struct{}{}
// Release
<-concurrencyLimitCh
```

This is simpler than a mutex-based approach and naturally supports blocking with context cancellation via `select`.

### 5. Parallel Partition Search

The storage layer searches multiple day-partitions concurrently, with a pool of worker goroutines processing individual data blocks:

```
Partitions (by day) → [Partition Searcher goroutines] → workCh → [Block Worker goroutines] → writeBlock
```

### 6. Binary Protocol with zstd Compression

Inter-node communication uses a custom binary protocol (marshaled `DataBlock` objects) with zstd compression, providing significant bandwidth savings compared to JSON:

```
[8-byte block length][compressed data: marker byte + serialized DataBlock(s)]...
```

### 7. Partial Response Support

In cluster mode, the system can return partial results when some storage nodes are unavailable, controlled by `-search.allowPartialResponse`. This trades completeness for availability.

---

## Configuration Flags

### Query Execution

```bash
-search.maxConcurrentRequests int
    Maximum concurrent search requests (default: CPU-based, capped at 16;
    uses 2×CPUs only when CPUs <= 4)

-search.maxQueueDuration duration
    Configured queue wait hint (default: 10s)
    Note: current select path wait bound is effectively request timeout
    (`timeout` arg / `-search.maxQueryDuration`)

-search.maxQueryDuration duration
    Maximum query execution time (default: 30s)
    Can be overridden per-query via 'timeout' query arg

-search.maxQueryTimeRange duration
    Maximum allowed time range in queries (default: 0 - unlimited)

-search.logSlowQueryDuration duration
    Log queries slower than this (default: 5s; 0 disables)
```

### Distributed Mode

```bash
-storageNode array
    Comma-separated list of storage node addresses
    Example: -storageNode=node1:9428,node2:9428

-select.disableCompression
    Disable zstd compression for responses from storage nodes (default: false)

-search.allowPartialResponse
    Allow partial responses when some storage nodes are unavailable (default: false)

-internalselect.maxConcurrentRequests int
    Concurrent request limit for /internal/select/* endpoints (default: 100)
```

### Endpoint Control

```bash
-select.disable
    Disable all /select/* HTTP endpoints (default: false)

-internalselect.disable
    Disable /internal/select/* HTTP endpoints (default: false)

-delete.enable
    Enable /delete/* HTTP endpoints (default: false)

-internaldelete.enable
    Enable /internal/delete/* HTTP endpoints (default: false)
```

### Authentication (per storage node)

```bash
-storageNode.username array
    Basic auth username for storage nodes

-storageNode.password array
    Basic auth password for storage nodes

-storageNode.bearerToken array
    Bearer token for storage nodes

-storageNode.tls array
    Use TLS (HTTPS) for storage nodes (default: false)
```

---

## Summary

The VictoriaLogs query flow is designed for:

1. **Streaming**: Results are streamed via callbacks rather than accumulated, enabling low-latency delivery of large result sets
2. **Parallelism**: Partitions are searched concurrently, with worker pools processing blocks in parallel
3. **Distributed Execution**: Queries are automatically split into remote (storage node) and local (frontend) portions for efficient cluster execution
4. **Optimization**: Common patterns like "last N results" are detected and optimized with binary search over time ranges
5. **Resource Control**: Concurrency limits, query timeouts, and queue duration prevent resource exhaustion
6. **Resilience**: Partial response support allows degraded but functional service when storage nodes are unavailable

The key architectural insight is the **callback pipeline**: `HTTP handler → parseCommonArgs → vlstorage.RunQuery → localStorage/netselect → searchParallel → writeBlock → JSON response`. Each layer wraps the next layer's callback, enabling streaming, pipe processing, and transparent local/distributed routing.

---

## Complete Distributed Query Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ CLIENT → Node A (Frontend / vlselect Node)                       │
│                                                                  │
│  GET /select/logsql/query?query=error                           │
│    ↓                                                            │
│  vlselect.RequestHandler                                        │
│    ↓                                                            │
│  Concurrency control (channel semaphore)                        │
│    ↓                                                            │
│  logsql.ProcessQueryRequest                                     │
│    ↓                                                            │
│  parseCommonArgs → logstorage.ParseQueryAtTimestamp              │
│    ↓                                                            │
│  vlstorage.RunQuery                                             │
│    ↓                                                            │
│  netstorageSelect.RunQuery                                      │
│    ↓                                                            │
│  NewNetQueryRunner                                              │
│  splitQueryToRemoteAndLocal(q)                                  │
│    → qRemote = filter + some pipes                              │
│    → pipesLocal = merge + remaining pipes                       │
│    ↓                                                            │
│  Fan out qRemote to ALL storage nodes in parallel               │
│    ↓                                                            │
│  POST http://nodeB:9428/internal/select/query?version=v4        │
│  POST http://nodeC:9428/internal/select/query?version=v4        │
└────────────────────────┬───────────────────────┬────────────────┘
                         │ NETWORK               │ NETWORK
┌────────────────────────┴───────────┐  ┌────────┴───────────────────────────┐
│ Node B (Storage Node)               │  │ Node C (Storage Node)               │
│                                     │  │                                     │
│  POST /internal/select/query        │  │  POST /internal/select/query        │
│    ↓                                │  │    ↓                                │
│  Verify protocol version (v4)       │  │  Verify protocol version (v4)       │
│    ↓                                │  │    ↓                                │
│  getCommonParams (query, tenants)   │  │  getCommonParams (query, tenants)   │
│    ↓                                │  │    ↓                                │
│  localStorage.RunQuery(qctx)        │  │  localStorage.RunQuery(qctx)        │
│    ↓                                │  │    ↓                                │
│  searchParallel (partitions)        │  │  searchParallel (partitions)        │
│    ↓                                │  │    ↓                                │
│  Stream DataBlocks (binary+zstd)    │  │  Stream DataBlocks (binary+zstd)    │
│    ↓                                │  │    ↓                                │
│  Send query stats block (last)      │  │  Send query stats block (last)      │
└────────────────────────┬───────────┘  └────────┬───────────────────────────┘
                         │ NETWORK               │ NETWORK
┌────────────────────────┴───────────────────────┴────────────────┐
│ Node A (Frontend) — continued                                    │
│                                                                  │
│  Receive + decompress DataBlocks from all storage nodes         │
│    ↓                                                            │
│  Apply pipesLocal (merge, sort, limit, etc.)                    │
│    ↓                                                            │
│  writeBlock → Stream JSON to client                             │
│    ↓                                                            │
│  Update per-query stats metrics                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## See Also

- [VictoriaLogs System Overview](./onboarding-system-overview.md)
- [VictoriaLogs LogsQL Parser & Pipe Execution](./onboarding-logsql-parser-pipes.md)
- [VictoriaLogs Storage Engine & On-Disk Format](./onboarding-storage-engine.md)
- [VictoriaLogs Cluster Architecture](./onboarding-cluster.md)
- [Glossary](./glossary.md)

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
**File**: [`app/vlselect/main.go`](../app/vlselect/main.go#L90)
```

**2. Function References in Key Functions Lists**

```markdown
**Key Functions**:
- [`RequestHandler(w, r)`](../app/vlselect/main.go#L90) - Main HTTP router
- [`selectHandler(w, r, path)`](../app/vlselect/main.go#L138) - Select handler with concurrency control
```

**3. Flow Diagram References**

```markdown
vlselect.RequestHandler                   [main.go:90](../app/vlselect/main.go#L90)
```

**4. Location References**

```markdown
**Location**: [`app/vlselect/main.go:90-136`](../app/vlselect/main.go#L90)
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

- [ ] All file paths are valid relative to this document location
- [ ] All line number anchors use `#L` prefix (capital L)
- [ ] All links tested and navigate correctly in VS Code
- [ ] No plain text file references (should be clickable markdown links)
- [ ] Display text is appropriate for context (full path, filename, or function)

#### Tools for Validation

You can verify all links have the correct format using:

```bash
# Check for links missing the #L prefix
grep -n '\](.*\.go#[0-9]' onboarding/onboarding-select-flow.md

# Count total clickable line references
grep -o "#L[0-9]\+" onboarding/onboarding-select-flow.md | wc -l

# Find non-clickable file references
grep -n '`app/.*\.go`' onboarding/onboarding-select-flow.md | grep -v '\]('
```
