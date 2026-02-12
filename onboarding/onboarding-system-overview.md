# VictoriaLogs System Overview - Developer Onboarding Guide

This document is the high-level map of VictoriaLogs internals. It is meant to be read before deep dives into ingestion, query execution, storage engine internals, and partition lifecycle.

It answers:

- Which binaries and packages own which responsibilities
- How requests move between components in single-node and cluster modes
- Where startup, routing, flow control, and metrics are implemented
- Which files to open first when changing a specific subsystem

## Table of Contents

- [Overview](#overview)
- [Core Components](#core-components)
- [Deployment Modes](#deployment-modes)
  - [Single-Node](#single-node)
  - [Cluster](#cluster)
- [Startup and Shutdown Lifecycle](#startup-and-shutdown-lifecycle)
- [HTTP Routing Boundaries](#http-routing-boundaries)
- [Data Path Summary](#data-path-summary)
  - [Write Path (Insert)](#write-path-insert)
  - [Read Path (Select)](#read-path-select)
- [Internal Protocol Contracts](#internal-protocol-contracts)
- [Cross-Cutting Runtime Behavior](#cross-cutting-runtime-behavior)
  - [Tenant Propagation](#tenant-propagation)
  - [Flow Control and Backpressure](#flow-control-and-backpressure)
  - [Timeouts and Partial Responses](#timeouts-and-partial-responses)
  - [Observability and Metrics](#observability-and-metrics)
- [Change Map](#change-map)
- [Related Onboarding Docs](#related-onboarding-docs)

---

## Overview

In single-node mode, `victoria-logs` hosts ingestion, query, and storage in one process and one HTTP server.

In cluster mode, frontend components (`vlinsert`, `vlselect`) forward to storage nodes via internal APIs (`/internal/insert`, `/internal/select/*`, `/internal/delete/*`) with explicit protocol versions.

The top-level glue code lives in the `app/` tree, while durable data structures and query/storage algorithms live in `lib/logstorage`.

---

## Core Components

| Component | Responsibility | Key Entry Points |
|----------|----------------|------------------|
| `app/victoria-logs` | Single-node binary: wires insert, select, storage in one process | [`main()`](../app/victoria-logs/main.go#L31), [`requestHandler`](../app/victoria-logs/main.go#L76) |
| `app/vlinsert` | Public ingestion endpoint router for all supported protocols | [`RequestHandler`](../app/vlinsert/main.go#L38), [`insertHandler`](../app/vlinsert/main.go#L62) |
| `app/vlselect` | Public query API, tailing, concurrency/timeout controls, VMUI | [`RequestHandler`](../app/vlselect/main.go#L90), [`selectHandler`](../app/vlselect/main.go#L138), [`processSelectRequest`](../app/vlselect/main.go#L286) |
| `app/vlstorage` | Storage facade: local disk mode or network fanout mode | [`Init`](../app/vlstorage/main.go#L109), [`RunQuery`](../app/vlstorage/main.go#L554), [`Storage.MustAddRows`](../app/vlstorage/main.go#L543) |
| `app/vlstorage/netinsert` | Sends batched insert blocks to storage nodes | [`ProtocolVersion`](../app/vlstorage/netinsert/netinsert.go#L33), [`Storage.AddRow`](../app/vlstorage/netinsert/netinsert.go#L375) |
| `app/vlstorage/netselect` | Fans out queries to storage nodes and merges responses | [`QueryProtocolVersion`](../app/vlstorage/netselect/netselect.go#L63), [`Storage.RunQuery`](../app/vlstorage/netselect/netselect.go#L385) |
| `app/vlinsert/internalinsert` | Handler for `/internal/insert` on storage nodes | [`RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L24) |
| `app/vlselect/internalselect` | Handlers for `/internal/select/*` and `/internal/delete/*` on storage nodes | [`RequestHandler`](../app/vlselect/internalselect/internalselect.go#L31), [`requestHandlers` map](../app/vlselect/internalselect/internalselect.go#L79) |
| `app/vlinsert/insertutil` | Shared ingestion params, normalization, buffering, storage interface | [`GetCommonParams`](../app/vlinsert/insertutil/common_params.go#L50), [`LogRowsStorage`](../app/vlinsert/insertutil/common_params.go#L175), [`SetLogRowsStorage`](../app/vlinsert/insertutil/common_params.go#L188) |
| `lib/logstorage` | Local storage engine + query engine | [`Storage`](../lib/logstorage/storage.go#L115), [`MustOpenStorage`](../lib/logstorage/storage.go#L618), [`Storage.MustAddRows`](../lib/logstorage/storage.go#L1139), [`Storage.RunQuery`](../lib/logstorage/storage_search.go#L208) |

---

## Deployment Modes

### Single-Node

`victoria-logs` process:

```
HTTP
  -> vlinsert.RequestHandler (/insert/*)
  -> vlselect.RequestHandler (/select/*, /delete/*)
  -> vlstorage local mode
  -> lib/logstorage (disk-backed storage engine)
```

Boot wiring:

- [`vlstorage.Init()`](../app/victoria-logs/main.go#L46)
- [`vlselect.Init()`](../app/victoria-logs/main.go#L47)
- [`insertutil.SetLogRowsStorage(&vlstorage.Storage{})`](../app/victoria-logs/main.go#L49)
- [`vlinsert.Init()`](../app/victoria-logs/main.go#L50)

### Cluster

Frontend nodes:

- `vlinsert` receives `/insert/*` and forwards via `netinsert` to `/internal/insert` (unless disabled by `-insert.disable`)
- `vlselect` receives `/select/*` and forwards via `netselect` to `/internal/select/*` (unless disabled by `-select.disable`)

Storage nodes:

- Serve `/internal/insert` via [`internalinsert.RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L24) (disabled by `-internalinsert.disable` or `-insert.disable`)
- Serve `/internal/select/*` via [`internalselect.RequestHandler`](../app/vlselect/internalselect/internalselect.go#L31) (disabled by `-internalselect.disable` or `-select.disable`)
- Serve `/internal/delete/*` via [`internalselect.RequestHandler`](../app/vlselect/internalselect/internalselect.go#L31) only when `-internaldelete.enable` is set
- Execute against local `lib/logstorage`

`vlstorage` picks mode by `-storageNode` presence in [`Init`](../app/vlstorage/main.go#L109) and branches to local/network setup in [`initLocalStorage`](../app/vlstorage/main.go#L117) and [`initNetworkStorage`](../app/vlstorage/main.go#L164).

---

## Startup and Shutdown Lifecycle

In `victoria-logs`:

1. Parse flags and init logging/build info in [`main`](../app/victoria-logs/main.go#L31).
2. Initialize storage/select/insert subsystems in order ([`main.go#L46-L50`](../app/victoria-logs/main.go#L46)).
3. Start shared HTTP server ([`httpserver.Serve`](../app/victoria-logs/main.go#L52)).
4. On shutdown signal, stop HTTP server first, then stop insert/select/storage ([`main.go#L62-L71`](../app/victoria-logs/main.go#L62)).

In `vlstorage` stop path:

- Local mode closes storage via [`localStorage.MustClose()`](../app/vlstorage/main.go#L231), which stops background workers and closes partitions in [`Storage.MustClose`](../lib/logstorage/storage.go#L1075).
- Network mode stops remote insert/select clients via [`netstorageInsert.MustStop()`](../app/vlstorage/main.go#L234) and [`netstorageSelect.MustStop()`](../app/vlstorage/main.go#L237).

---

## HTTP Routing Boundaries

Top-level router in single-node:

- [`victoria-logs.requestHandler`](../app/victoria-logs/main.go#L76) delegates in order:
1. [`vlinsert.RequestHandler`](../app/victoria-logs/main.go#L93)
2. [`vlselect.RequestHandler`](../app/victoria-logs/main.go#L96)
3. [`vlstorage.RequestHandler`](../app/victoria-logs/main.go#L99)

Public ingestion routing:

- [`vlinsert.RequestHandler`](../app/vlinsert/main.go#L38) handles `/insert/*` and `/internal/insert`.
- Protocol-specific dispatch is in [`insertHandler`](../app/vlinsert/main.go#L62).

Public query routing:

- [`vlselect.RequestHandler`](../app/vlselect/main.go#L90) handles `/select/*`, `/delete/*`, `/internal/select/*`, `/internal/delete/*`.
- `/select/buildinfo`, `/select/vmui*`, and `/select/logsql/tail` are handled directly in [`selectHandler`](../app/vlselect/main.go#L138).
- Most `/select/logsql/*` and `/select/tenant_ids` endpoint dispatch is in [`processSelectRequest`](../app/vlselect/main.go#L286).

Storage internal ops routing:

- [`vlstorage.RequestHandler`](../app/vlstorage/main.go#L243) handles maintenance endpoints (`/internal/force_merge`, `/internal/partition/*`, etc.).

---

## Data Path Summary

### Write Path (Insert)

High-level path:

1. Public insert endpoint handled in [`app/vlinsert/main.go`](../app/vlinsert/main.go#L38).
2. Common params parsed in [`GetCommonParams`](../app/vlinsert/insertutil/common_params.go#L50).
3. Buffered ingestion pipeline via [`CommonParams.NewLogMessageProcessor`](../app/vlinsert/insertutil/common_params.go#L348).
4. Data sink resolved through `insertutil.LogRowsStorage` interface ([`common_params.go#L175`](../app/vlinsert/insertutil/common_params.go#L175)):
   - Local single-node path: [`vlstorage.Storage.MustAddRows`](../app/vlstorage/main.go#L543) -> [`logstorage.Storage.MustAddRows`](../lib/logstorage/storage.go#L1139).
   - Cluster frontend path: [`netinsert.Storage.AddRow`](../app/vlstorage/netinsert/netinsert.go#L375) -> `/internal/insert`.
5. Storage-node ingress validates protocol in [`internalinsert.RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L30).

### Read Path (Select)

High-level path:

1. Public select endpoint handled in [`vlselect.RequestHandler`](../app/vlselect/main.go#L90).
2. Query request parsed/executed through [`logsql.ProcessQueryRequest`](../app/vlselect/logsql/logsql.go#L1149), [`parseCommonArgs`](../app/vlselect/logsql/logsql.go#L1358), and [`newQueryContext`](../app/vlselect/logsql/logsql.go#L1350).
3. Storage facade execution via [`vlstorage.RunQuery`](../app/vlstorage/main.go#L554).
4. Execution mode split:
   - Local mode: [`logstorage.Storage.RunQuery`](../lib/logstorage/storage_search.go#L208).
   - Cluster mode: [`netselect.Storage.RunQuery`](../app/vlstorage/netselect/netselect.go#L385) -> `/internal/select/query`.
5. Storage-node internal select handler starts in [`internalselect.processQueryRequest`](../app/vlselect/internalselect/internalselect.go#L94).

---

## Internal Protocol Contracts

| Internal API | Version Source | Enforced At |
|--------------|----------------|-------------|
| `/internal/insert` | [`netinsert.ProtocolVersion = "v1"`](../app/vlstorage/netinsert/netinsert.go#L33) | [`internalinsert.RequestHandler` version check](../app/vlinsert/internalinsert/internalinsert.go#L30) |
| `/internal/select/query` | [`netselect.QueryProtocolVersion = "v4"`](../app/vlstorage/netselect/netselect.go#L63) | [`internalselect.processQueryRequest`](../app/vlselect/internalselect/internalselect.go#L95) |
| `/internal/select/{field_names,field_values,streams,...}` | [`netselect` protocol constants](../app/vlstorage/netselect/netselect.go#L29) | corresponding `getCommonParams(...version)` calls in [`internalselect`](../app/vlselect/internalselect/internalselect.go#L185) |
| `/internal/select/tenant_ids` | no dedicated protocol constant (`netselect` sends only `start`/`end`) in [`storageNode.getTenantIDs`](../app/vlstorage/netselect/netselect.go#L248) | no `checkProtocolVersion` in [`internalselect.processTenantIDsRequest`](../app/vlselect/internalselect/internalselect.go#L377) |
| `/internal/delete/*` | [`Delete*ProtocolVersion = "v1"`](../app/vlstorage/netselect/netselect.go#L65) | internal delete handlers in [`internalselect`](../app/vlselect/internalselect/internalselect.go#L89) |

Any wire format change must bump the corresponding protocol constant and both sender and receiver.

---

## Cross-Cutting Runtime Behavior

### Tenant Propagation

- Tenant identity is extracted from HTTP headers by [`GetTenantIDFromRequest`](../lib/logstorage/tenant_id.go#L73).
- Ingestion common params include `TenantID` in [`CommonParams`](../app/vlinsert/insertutil/common_params.go#L30).
- `/internal/insert` ignores non-zero tenant headers and resets tenant to zero tenant in [`internalinsert.RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L48).
- Query execution carries tenant scope through [`QueryContext.TenantIDs`](../lib/logstorage/storage_search.go#L32).

### Flow Control and Backpressure

- Query concurrency at public API:
  - Configured by [`-search.maxConcurrentRequests`](../app/vlselect/main.go#L25).
  - Enforced in [`incRequestConcurrency`](../app/vlselect/main.go#L246).
- Query concurrency at storage-node internal API:
  - Configured by [`-internalselect.maxConcurrentRequests`](../app/vlselect/internalselect/internalselect.go#L27).
  - Enforced by channel gate in [`internalselect.RequestHandler`](../app/vlselect/internalselect/internalselect.go#L35).
- Ingest write protection:
  - Local read-only check in [`vlstorage.Storage.CanWriteData`](../app/vlstorage/main.go#L524).
  - `insertutil` calls storage gate via [`CanWriteData`](../app/vlinsert/insertutil/common_params.go#L193).
- Cluster insert retry/reroute:
  - Node-local send path in [`mustSendInsertRequest`](../app/vlstorage/netinsert/netinsert.go#L195).
  - Reroute fallback in [`sendInsertRequestToAnyNode`](../app/vlstorage/netinsert/netinsert.go#L381).

### Timeouts and Partial Responses

- `/select/logsql/tail` bypasses both per-request timeout and public query concurrency limit in [`selectHandler`](../app/vlselect/main.go#L183).
- Per-request timeout creation in [`context.WithTimeout` usage](../app/vlselect/main.go#L198).
- Query-context-level partial response flag in [`QueryContext.AllowPartialResponse`](../lib/logstorage/storage_search.go#L38).
- Query options may override this behavior in [`newQueryContext`](../lib/logstorage/storage_search.go#L80).
- Cluster query fanout applies `allowPartialResponse` when handling node errors in [`netselect.runQuery`](../app/vlstorage/netselect/netselect.go#L415).

### Observability and Metrics

- Public select concurrency gauges/counters live in [`app/vlselect/main.go`](../app/vlselect/main.go#L73).
- Storage health/size/read-only gauges are written in [`writeStorageMetrics`](../app/vlstorage/main.go#L667).
- Per-query storage I/O histograms are updated in [`UpdatePerQueryStatsMetrics`](../app/vlstorage/query_stats.go#L28).

---

## Change Map

If you need to change:

- Public ingestion endpoint behavior:
  - [`app/vlinsert/main.go`](../app/vlinsert/main.go#L38)
  - Protocol handler under `app/vlinsert/<protocol>/`
- Common ingestion params or row buffering:
  - [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L50)
- Public select routing, limits, timeouts:
  - [`app/vlselect/main.go`](../app/vlselect/main.go#L90)
- LogsQL parse/exec glue in API layer:
  - [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1149)
- Cluster internal select/insert protocol behavior:
  - [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L31)
  - [`app/vlinsert/internalinsert/internalinsert.go`](../app/vlinsert/internalinsert/internalinsert.go#L24)
  - [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L29)
  - [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L30)
- Local storage write/read/retention behavior:
  - [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L115)
  - [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L208)
- LogsQL parser internals:
  - [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L1671)

---

## Related Onboarding Docs

- Ingestion deep dive: [`onboarding-insert-flow.md`](./onboarding-insert-flow.md)
- Query deep dive: [`onboarding-select-flow.md`](./onboarding-select-flow.md)
- LogsQL parser and pipe runtime deep dive: [`onboarding-logsql-parser-pipes.md`](./onboarding-logsql-parser-pipes.md)
- Storage internals deep dive: [`onboarding-storage-engine.md`](./onboarding-storage-engine.md)
- Partition operations deep dive: [`onboarding-partition-lifecycle.md`](./onboarding-partition-lifecycle.md)

Suggested reading order for new engineers:

1. This file (`onboarding-system-overview.md`)
2. `onboarding-insert-flow.md`
3. `onboarding-select-flow.md`
4. `onboarding-logsql-parser-pipes.md`
5. `onboarding-storage-engine.md`
6. `onboarding-partition-lifecycle.md`
