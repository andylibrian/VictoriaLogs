# VictoriaLogs System Overview - Developer Onboarding Guide

This document is the high-level map of VictoriaLogs internals. It is meant to be read before deep dives into ingestion, query execution, storage engine internals, and partition lifecycle.

It answers:

- Which binaries and packages own which responsibilities
- How requests move between components in single-node and cluster modes
- Where startup, routing, flow control, and metrics are implemented
- Which files to open first when changing a specific subsystem

For unfamiliar terms (LSM-tree, bloom filter, mergeset, zstd, etc.), see the [Glossary](./glossary.md).

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

In cluster mode, frontend components (`vlinsert`, `vlselect`) forward to storage nodes via internal APIs (`/internal/insert`, `/internal/select/*`, `/internal/delete/*`) that are mostly versioned (notably, `/internal/select/tenant_ids` is an exception).

The top-level glue code lives in the `app/` tree, while durable data structures and query/storage algorithms live in `lib/logstorage`.

---

## Core Components

| Component | Responsibility | Key Entry Points |
|----------|----------------|------------------|
| `app/victoria-logs` | Single-node binary: wires insert, select, storage in one process | [`main()`](../app/victoria-logs/main.go#L68), [`requestHandler`](../app/victoria-logs/main.go#L141) |
| `app/vlinsert` | Public HTTP ingestion router (`/insert/*`) and syslog listener bootstrap | [`RequestHandler`](../app/vlinsert/main.go#L91), [`insertHandler`](../app/vlinsert/main.go#L119), [`Init`](../app/vlinsert/main.go#L76) |
| `app/vlselect` | Public query API, tailing, concurrency/timeout controls, VMUI | [`RequestHandler`](../app/vlselect/main.go#L189), [`selectHandler`](../app/vlselect/main.go#L254), [`processSelectRequest`](../app/vlselect/main.go#L428) |
| `app/vlstorage` | Storage facade: local disk mode or network fanout mode | [`Init`](../app/vlstorage/main.go#L179), [`RunQuery`](../app/vlstorage/main.go#L741), [`Storage.MustAddRows`](../app/vlstorage/main.go#L718) |
| `app/vlstorage/netinsert` | Sends batched insert blocks to storage nodes | [`ProtocolVersion`](../app/vlstorage/netinsert/netinsert.go#L74), [`Storage.AddRow`](../app/vlstorage/netinsert/netinsert.go#L481) |
| `app/vlstorage/netselect` | Fans out queries to storage nodes and merges responses | [`QueryProtocolVersion`](../app/vlstorage/netselect/netselect.go#L94), [`Storage.RunQuery`](../app/vlstorage/netselect/netselect.go#L442) |
| `app/vlinsert/internalinsert` | Handler for `/internal/insert` on storage nodes | [`RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L70) |
| `app/vlselect/internalselect` | Handlers for `/internal/select/*` and `/internal/delete/*` on storage nodes | [`RequestHandler`](../app/vlselect/internalselect/internalselect.go#L84), [`requestHandlers` map](../app/vlselect/internalselect/internalselect.go#L136) |
| `app/vlinsert/insertutil` | Shared ingestion params, normalization, buffering, storage interface | [`GetCommonParams`](../app/vlinsert/insertutil/common_params.go#L121), [`LogRowsStorage`](../app/vlinsert/insertutil/common_params.go#L254), [`SetLogRowsStorage`](../app/vlinsert/insertutil/common_params.go#L272) |
| `lib/logstorage` | Local storage engine + query engine | [`Storage`](../lib/logstorage/storage.go#L115), [`MustOpenStorage`](../lib/logstorage/storage.go#L691), [`Storage.MustAddRows`](../lib/logstorage/storage.go#L1262), [`Storage.RunQuery`](../lib/logstorage/storage_search.go#L274) |

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

- [`vlstorage.Init()`](../app/victoria-logs/main.go#L86)
- [`vlselect.Init()`](../app/victoria-logs/main.go#L88)
- [`insertutil.SetLogRowsStorage(&vlstorage.Storage{})`](../app/victoria-logs/main.go#L93)
- [`vlinsert.Init()`](../app/victoria-logs/main.go#L95)

### Cluster

Frontend nodes:

- `vlinsert` receives `/insert/*` and forwards via `netinsert` to `/internal/insert` (unless disabled by `-insert.disable`)
- `vlselect` receives `/select/*` and forwards via `netselect` to `/internal/select/*` (unless disabled by `-select.disable`)
- Syslog ingestion is listener-based (`-syslog.listenAddr.*`) and initialized by [`vlinsert.Init`](../app/vlinsert/main.go#L76), not routed via `/insert/*`.

Storage nodes:

- Serve `/internal/insert` via [`internalinsert.RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L70) (disabled by `-internalinsert.disable` or `-insert.disable`)
- Serve `/internal/select/*` via [`internalselect.RequestHandler`](../app/vlselect/internalselect/internalselect.go#L84) (disabled by `-internalselect.disable` or `-select.disable`)
- Serve `/internal/delete/*` via [`internalselect.RequestHandler`](../app/vlselect/internalselect/internalselect.go#L84) only when `-internaldelete.enable` is set
- Execute against local `lib/logstorage`

`vlstorage` picks mode by `-storageNode` presence in [`Init`](../app/vlstorage/main.go#L179) and branches to local/network setup in [`initLocalStorage`](../app/vlstorage/main.go#L202) and [`initNetworkStorage`](../app/vlstorage/main.go#L266).

---

## Startup and Shutdown Lifecycle

In `victoria-logs`:

1. Parse flags and init logging/build info in [`main`](../app/victoria-logs/main.go#L68).
2. Initialize storage/select/insert subsystems in order ([`main.go#L86-L95`](../app/victoria-logs/main.go#L86)).
3. Start shared HTTP server ([`httpserver.Serve`](../app/victoria-logs/main.go#L99)).
4. On shutdown signal, stop HTTP server first, then stop insert/select/storage ([`main.go#L110-L123`](../app/victoria-logs/main.go#L110)).

In `vlstorage` stop path:

- Local mode closes storage via [`localStorage.MustClose()`](../app/vlstorage/main.go#L351), which stops background workers and closes partitions in [`Storage.MustClose`](../lib/logstorage/storage.go#L1170).
- Network mode stops remote insert/select clients via [`netstorageInsert.MustStop()`](../app/vlstorage/main.go#L354) and [`netstorageSelect.MustStop()`](../app/vlstorage/main.go#L357).

---

## HTTP Routing Boundaries

Top-level router in single-node:

- [`victoria-logs.requestHandler`](../app/victoria-logs/main.go#L141) delegates in order:
1. [`vlinsert.RequestHandler`](../app/victoria-logs/main.go#L158)
2. [`vlselect.RequestHandler`](../app/victoria-logs/main.go#L161)
3. [`vlstorage.RequestHandler`](../app/victoria-logs/main.go#L164)

Public ingestion routing:

- [`vlinsert.RequestHandler`](../app/vlinsert/main.go#L91) handles `/insert/*` and `/internal/insert`.
- Syslog listeners are started separately in [`vlinsert.Init`](../app/vlinsert/main.go#L76) via [`syslog.MustInit`](../app/vlinsert/main.go#L77).
- Protocol-specific dispatch is in [`insertHandler`](../app/vlinsert/main.go#L119).

Public query routing:

- [`vlselect.RequestHandler`](../app/vlselect/main.go#L189) handles `/select/*`, `/delete/*`, `/internal/select/*`, `/internal/delete/*`.
- `/internal/delete/*` requires [`-internaldelete.enable`](../app/vlselect/main.go#L91); otherwise requests are rejected by [`vlselect.RequestHandler`](../app/vlselect/main.go#L212).
- `/select/buildinfo`, `/select/vmui*`, and `/select/logsql/tail` are handled directly in [`selectHandler`](../app/vlselect/main.go#L254).
- Most `/select/logsql/*` and `/select/tenant_ids` endpoint dispatch is in [`processSelectRequest`](../app/vlselect/main.go#L428).

Storage internal ops routing:

- [`vlstorage.RequestHandler`](../app/vlstorage/main.go#L380) handles maintenance endpoints (`/internal/force_merge`, `/internal/partition/*`, etc.).

---

## Data Path Summary

### Write Path (Insert)

High-level path:

1. Public insert endpoint handled in [`app/vlinsert/main.go`](../app/vlinsert/main.go#L91).
2. Common params parsed in [`GetCommonParams`](../app/vlinsert/insertutil/common_params.go#L121).
3. Buffered ingestion pipeline via [`CommonParams.NewLogMessageProcessor`](../app/vlinsert/insertutil/common_params.go#L575).
4. Data sink resolved through `insertutil.LogRowsStorage` interface ([`common_params.go#L254`](../app/vlinsert/insertutil/common_params.go#L254)):
   - Local single-node path: [`vlstorage.Storage.MustAddRows`](../app/vlstorage/main.go#L718) -> [`logstorage.Storage.MustAddRows`](../lib/logstorage/storage.go#L1262).
   - Cluster frontend path: [`netinsert.Storage.AddRow`](../app/vlstorage/netinsert/netinsert.go#L481) -> `/internal/insert`.
5. Storage-node ingress validates protocol in [`internalinsert.RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L77).

### Read Path (Select)

High-level path:

1. Public select endpoint handled in [`vlselect.RequestHandler`](../app/vlselect/main.go#L189).
2. Query request parsed/executed through [`logsql.ProcessQueryRequest`](../app/vlselect/logsql/logsql.go#L1323), [`parseCommonArgs`](../app/vlselect/logsql/logsql.go#L1566), and [`newQueryContext`](../app/vlselect/logsql/logsql.go#L1548).
3. Storage facade execution via [`vlstorage.RunQuery`](../app/vlstorage/main.go#L741).
4. Execution mode split:
   - Local mode: [`logstorage.Storage.RunQuery`](../lib/logstorage/storage_search.go#L274).
   - Cluster mode: [`netselect.Storage.RunQuery`](../app/vlstorage/netselect/netselect.go#L442) -> `/internal/select/query`.
5. Storage-node internal select handler starts in [`internalselect.processQueryRequest`](../app/vlselect/internalselect/internalselect.go#L162).

---

## Internal Protocol Contracts

| Internal API | Version Source | Enforced At |
|--------------|----------------|-------------|
| `/internal/insert` | [`netinsert.ProtocolVersion = "v1"`](../app/vlstorage/netinsert/netinsert.go#L74) | [`internalinsert.RequestHandler` version check](../app/vlinsert/internalinsert/internalinsert.go#L77) |
| `/internal/select/query` | [`netselect.QueryProtocolVersion = "v4"`](../app/vlstorage/netselect/netselect.go#L94) | [`internalselect.processQueryRequest`](../app/vlselect/internalselect/internalselect.go#L163) |
| `/internal/select/{field_names,field_values,streams,...}` | [`netselect` protocol constants](../app/vlstorage/netselect/netselect.go#L73) | corresponding `getCommonParams(...version)` calls in [`internalselect`](../app/vlselect/internalselect/internalselect.go#L254) |
| `/internal/select/tenant_ids` | no dedicated protocol constant (`netselect` sends only `start`/`end`) in [`storageNode.getTenantIDs`](../app/vlstorage/netselect/netselect.go#L281) | no `checkProtocolVersion` in [`internalselect.processTenantIDsRequest`](../app/vlselect/internalselect/internalselect.go#L445) |
| `/internal/delete/*` | [`Delete*ProtocolVersion = "v1"`](../app/vlstorage/netselect/netselect.go#L97) | internal delete handlers in [`internalselect`](../app/vlselect/internalselect/internalselect.go#L146) |

Any wire format change must bump the corresponding protocol constant and both sender and receiver.

---

## Cross-Cutting Runtime Behavior

### Tenant Propagation

- Tenant identity is extracted from HTTP headers by [`GetTenantIDFromRequest`](../lib/logstorage/tenant_id.go#L73).
- Ingestion common params include `TenantID` in [`CommonParams`](../app/vlinsert/insertutil/common_params.go#L74).
- `/internal/insert` ignores non-zero tenant headers and resets tenant to zero tenant in [`internalinsert.RequestHandler`](../app/vlinsert/internalinsert/internalinsert.go#L94).
- Query execution carries tenant scope through [`QueryContext.TenantIDs`](../lib/logstorage/storage_search.go#L85).

### Flow Control and Backpressure

- Query concurrency at public API:
  - Configured by [`-search.maxConcurrentRequests`](../app/vlselect/main.go#L69).
  - Enforced in [`incRequestConcurrency`](../app/vlselect/main.go#L375).
- Query concurrency at storage-node internal API:
  - Configured by [`-internalselect.maxConcurrentRequests`](../app/vlselect/internalselect/internalselect.go#L78).
  - Enforced by channel gate in [`internalselect.RequestHandler`](../app/vlselect/internalselect/internalselect.go#L88).
- Ingest write protection:
  - Local read-only check in [`vlstorage.Storage.CanWriteData`](../app/vlstorage/main.go#L686).
  - `insertutil` calls storage gate via [`CanWriteData`](../app/vlinsert/insertutil/common_params.go#L278).
- Cluster insert retry/reroute:
  - Node-local send path in [`mustSendInsertRequest`](../app/vlstorage/netinsert/netinsert.go#L282).
  - Reroute fallback in [`sendInsertRequestToAnyNode`](../app/vlstorage/netinsert/netinsert.go#L491).
  - If all storage nodes stay unavailable until shutdown, pending buffered data can be dropped in [`mustSendInsertRequest`](../app/vlstorage/netinsert/netinsert.go#L303).

### Timeouts and Partial Responses

- `/select/logsql/tail` bypasses both per-request timeout and public query concurrency limit in [`selectHandler`](../app/vlselect/main.go#L297).
- Per-request timeout creation in [`context.WithTimeout` usage](../app/vlselect/main.go#L310).
- Query-context-level partial response flag in [`QueryContext.AllowPartialResponse`](../lib/logstorage/storage_search.go#L94).
- Query options may override this behavior in [`newQueryContext`](../lib/logstorage/storage_search.go#L135).
- Cluster query fanout applies `allowPartialResponse` when handling node errors in [`netselect.runQuery`](../app/vlstorage/netselect/netselect.go#L462).

### Observability and Metrics

- Public select concurrency gauges/counters live in [`app/vlselect/main.go`](../app/vlselect/main.go#L151).
- Storage health/size/read-only gauges are written in [`writeStorageMetrics`](../app/vlstorage/main.go#L872).
- Per-query storage I/O histograms are updated in [`UpdatePerQueryStatsMetrics`](../app/vlstorage/query_stats.go#L28).

---

## Change Map

If you need to change:

- Public ingestion endpoint behavior:
  - [`app/vlinsert/main.go`](../app/vlinsert/main.go#L91)
  - Protocol handler under `app/vlinsert/<protocol>/`
- Common ingestion params or row buffering:
  - [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L121)
- Public select routing, limits, timeouts:
  - [`app/vlselect/main.go`](../app/vlselect/main.go#L189)
- LogsQL parse/exec glue in API layer:
  - [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1323)
- Cluster internal select/insert protocol behavior:
  - [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L84)
  - [`app/vlinsert/internalinsert/internalinsert.go`](../app/vlinsert/internalinsert/internalinsert.go#L70)
  - [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L73)
  - [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L71)
- Local storage write/read/retention behavior:
  - [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L115)
  - [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L274)
- LogsQL parser internals:
  - [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L1806)

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
