# VictoriaLogs vmui Frontend Internals - Developer Onboarding Guide

This document provides a comprehensive overview of [vmui](./glossary.md#vmui), VictoriaLogs' built-in web UI. It covers the build system, Go embedding, component architecture, state management, API integration, chart visualization, and local development workflow.

## Table of Contents

- [Overview](#overview)
- [Complete Data Flow](#complete-data-flow)
- [Architecture Layers](#architecture-layers)
  - [1. Build System & Tooling](#1-build-system--tooling)
  - [2. Go Embedding & Serving](#2-go-embedding--serving)
  - [3. App Shell & Routing](#3-app-shell--routing)
  - [4. State Management](#4-state-management)
  - [5. API Layer & Data Fetching](#5-api-layer--data-fetching)
  - [6. Pages](#6-pages)
  - [7. Chart Visualization](#7-chart-visualization)
- [Key Design Patterns](#key-design-patterns)
- [Local Development Workflow](#local-development-workflow)
- [Document Maintenance Guidelines](#document-maintenance-guidelines)

---

## Overview

**[vmui](./glossary.md#vmui)** is a Preact+TypeScript single-page application that provides a browser-based query interface for VictoriaLogs. It is built with Vite, embedded into the Go binary via `//go:embed`, and served at `/select/vmui/`. The UI executes the same `/select/logsql/*` API paths documented in [VictoriaLogs Query/Select Flow](./onboarding-select-flow.md), with query text interpreted by the parser pipeline described in [VictoriaLogs LogsQL Parser & Pipe Execution](./onboarding-logsql-parser-pipes.md).

### Key characteristics

- **Preact SPA**: Uses Preact (`^10.28.2`) with `preact/compat` for React API compatibility, React Router DOM v7 for client-side routing.
- **Vite build**: Vite `^7.3.1` for development server (port 3000) and production builds.
- **Embedded in Go binary**: Built assets are committed to `app/vlselect/vmui/` and served via `http.FileServer` with `embed.FS`.
- **Four routes**: Query (log search + hits chart), Overview (aggregated stats + field exploration), Stream Context (surrounding log entries), and Icons (internal preview page).
- **uPlot charts**: Log hit histograms rendered with uPlot (`^1.6.32`), supporting stacked, cumulative, and bar modes.
- **URL-driven state**: Query text, time range, tenant, and display options are synced to URL search params via `HashRouter`.

### Source structure

The frontend source lives at `app/vmui/packages/vmui/src/`:

| Directory | Purpose |
|-----------|---------|
| [`api/`](../app/vmui/packages/vmui/src/api/logs.ts#L1) | API endpoint URL builders and response types |
| [`state/`](../app/vmui/packages/vmui/src/state/common/StateContext.tsx#L1) | Context+useReducer state management (5 domains) |
| [`pages/`](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L34) | Top-level page components (Query, Overview, StreamContext) |
| [`components/`](../app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx#L33) | Reusable UI components (Chart, Table, Configurators, Main) |
| [`hooks/`](../app/vmui/packages/vmui/src/hooks/useTenant.ts#L4) | Custom hooks (tenant, URL sync, uPlot interaction, fetch) |
| [`router/`](../app/vmui/packages/vmui/src/router/index.ts#L1) | Route definitions and per-route header options |
| [`layouts/`](../app/vmui/packages/vmui/src/layouts/LogsLayout/LogsLayout.tsx#L15) | Page shell (Header, Footer, LogsLayout) |
| [`contexts/`](../app/vmui/packages/vmui/src/contexts/AppContextProvider.tsx#L10) | Provider composition |
| [`utils/`](../app/vmui/packages/vmui/src/utils/time.ts#L1) | Utility functions (time, logs, storage, uPlot helpers) |
| [`styles/`](../app/vmui/packages/vmui/src/styles/style.scss) | SCSS stylesheets |
| [`constants/`](../app/vmui/packages/vmui/src/constants/logs.ts) | Configuration constants |
| [`types/`](../app/vmui/packages/vmui/src/types/index.ts) | TypeScript type definitions |

---

## Complete Data Flow

**Key Files**:
- [`app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx`](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L34) - Main query page
- [`app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L28) - Log fetch hook
- [`app/vlselect/main.go`](../app/vlselect/main.go#L84) - Go embedding and serving

### Query execution flow (QueryPage)

```
User clicks "Execute" or presses Enter
        │
        ▼
handleRunQuery()                         [QueryPage.tsx:118](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L118)
  ├── fetchQueryTime({query, period})    — check for _time filter in query
  ├── setPeriod(newPeriod)               — update effective time range
  └── debouncedFetchLogs(period, flags)  — debounce 300ms, then:
        │
        ├── fetchLogs({period, query, limit})
        │     │
        │     ▼
        │   useFetchLogs()               [useFetchLogs.ts:28](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L28)
        │     ├── buildBody()            — URLSearchParams: query, limit, start, end (ISO)
        │     ├── buildOptions()         — POST, headers: {AccountID, ProjectID, Accept: stream+json}
        │     ├── fetch(url, {body})     — POST /select/logsql/query
        │     ├── response.text()        — read full [NDJSON](./glossary.md#ndjson) response
        │     ├── text.split("\n")       — split into lines
        │     ├── JSON.parse(line)       — parse each line [useFetchLogs.ts:184](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L184)
        │     └── setLogs(data)          — update state → re-render QueryPageBody
        │
        └── fetchLogHits({period, barsCount, field, fieldsLimit})
              │
              ▼
            useFetchLogHits()            [useFetchLogHits.ts:33](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogHits.ts#L33)
              ├── getOptions()           — URLSearchParams: query, step, offset, start, end, fields_limit, field
              ├── fetch(url, options)    — POST /select/logsql/hits or /stats_query_range
              ├── response.json()        — parse JSON response
              ├── processHits(data)      — extract hits[], mark "other", sort by total
              └── setLogHits(hits)       — update state → re-render BarHitsChart
```

### Static asset serving flow

```
Browser requests /select/vmui/#/
        │
        ▼
vlselect.RequestHandler()                [main.go:90](../app/vlselect/main.go#L90)
  └── selectHandler()                    [main.go:138](../app/vlselect/main.go#L138)
        ├── path == "/select/vmui"       → redirect to "vmui/?" [main.go:162](../app/vlselect/main.go#L162)
        └── path starts "/select/vmui/"  → serve via embed.FS   [main.go:171](../app/vlselect/main.go#L171)
              ├── /select/vmui/static/*  → Cache-Control: max-age=31536000
              └── strip "/select" prefix → vmuiFileServer.ServeHTTP()
```

---

## Architecture Layers

### 1. Build System & Tooling

**Files**:
- [`app/vmui/packages/vmui/package.json`](../app/vmui/packages/vmui/package.json#L7) - npm scripts and dependencies
- [`app/vmui/packages/vmui/vite.config.ts`](../app/vmui/packages/vmui/vite.config.ts#L35) - Vite configuration
- [`app/vmui/Makefile`](../app/vmui/Makefile#L21) - Docker-based build targets

#### Tech stack

| Tool | Version | Purpose |
|------|---------|---------|
| Preact | `^10.28.2` | UI framework (React-compatible) |
| React Router DOM | `^7.12.0` | Client-side routing |
| Vite | `^7.3.1` | Dev server and bundler |
| TypeScript | `^5.9.3` | Type checking |
| uPlot | `^1.6.32` | Chart rendering |
| dayjs | `^1.11.19` | Date/time manipulation |
| Vitest | `^4.0.17` | Unit testing |
| SCSS | via `sass-embedded` | Styling |

#### npm scripts

```bash
npm start                 # Vite dev server on port 3000 (runs gen:logsql-pipes first)
npm run start:playground  # Dev server with PLAYGROUND=LOGS proxy
npm run build             # Production build to ./build
npm run gen:logsql-pipes  # Auto-generate LogsQL pipe definitions from source
npm run lint:local        # ESLint
npm run typecheck         # TypeScript type checking
npm run test              # Vitest unit tests
```

#### Vite configuration

The Vite config at [`vite.config.ts`](../app/vmui/packages/vmui/vite.config.ts#L35) defines:

- **Base URL**: Empty string (relative paths) — required for serving under `/select/vmui/`.
- **Plugins**: `@preact/preset-vite` for Preact JSX transform, `dynamicIndexHtml` for mode-specific index files.
- **Dev server**: Port 3000, auto-open browser, optional proxy for playground mode.
- **Build output**: `./build` directory with vendor chunk splitting (all `node_modules` in a separate `vendor` chunk).
- **Path alias**: `src` resolves to the source directory.

#### Makefile targets (Docker-based)

The [`Makefile`](../app/vmui/Makefile#L3) runs npm commands inside a Docker container (`vmui-builder-image`, Node 22.20 Alpine):

```bash
make vmui-build    # gen:logsql-pipes + npm run build
make vmui-update   # build + copy assets to app/vlselect/vmui/
make vmui-lint     # npm run lint
make vmui-typecheck # npm run typecheck
make vmui-test     # npm run test
```

The `vmui-update` target is the key production step — it builds the SPA and moves the output to [`app/vlselect/vmui/`](../app/vlselect/main.go#L84), where Go's `//go:embed` picks it up.

**Location**: [`app/vmui/Makefile:21-35`](../app/vmui/Makefile#L21)

---

### 2. Go Embedding & Serving

**File**: [`app/vlselect/main.go`](../app/vlselect/main.go#L84)

The built vmui assets are embedded into the Go binary at compile time and served over HTTP:

```go
//go:embed vmui
var vmuiFiles embed.FS

var vmuiFileServer = http.FileServer(http.FS(vmuiFiles))
```

#### Serving logic

The `selectHandler()` at [`main.go:138`](../app/vlselect/main.go#L138) handles two vmui URL patterns:

1. **Redirect** (`/select/vmui` without trailing slash) — redirects to `vmui/?` preserving query params. This is a relative redirect so it works behind reverse proxies like vmauth.

2. **File serving** (`/select/vmui/*`) — strips the `/select` prefix from the URL path and delegates to `vmuiFileServer`. The current cache header shortcut is applied only for `/select/vmui/static/*` paths; built assets are currently emitted under `/select/vmui/assets/*`.

```go
if strings.HasPrefix(path, "/select/vmui/") {
    if strings.HasPrefix(path, "/select/vmui/static/") {
        w.Header().Set("Cache-Control", "max-age=31536000")
    }
    r.URL.Path = strings.TrimPrefix(path, "/select")
    vmuiFileServer.ServeHTTP(w, r)
    return true
}
```

**Location**: [`app/vlselect/main.go:162-181`](../app/vlselect/main.go#L162)

---

### 3. App Shell & Routing

**Files**:
- [`app/vmui/packages/vmui/src/App.tsx`](../app/vmui/packages/vmui/src/App.tsx#L13) - Root component
- [`app/vmui/packages/vmui/src/router/index.ts`](../app/vmui/packages/vmui/src/router/index.ts#L1) - Route definitions
- [`app/vmui/packages/vmui/src/layouts/LogsLayout/LogsLayout.tsx`](../app/vmui/packages/vmui/src/layouts/LogsLayout/LogsLayout.tsx#L15) - Page layout

#### Entry point

[`index.tsx`](../app/vmui/packages/vmui/src/index.tsx#L7) renders the `<App/>` component into `#root`. It also loads dayjs plugins, SCSS styles, and web vitals reporting.

#### Root component (App.tsx)

The `App` component sets up the routing and context hierarchy:

```tsx
<HashRouter>
  <AppContextProvider>
    <ThemeProvider onLoaded={setLoadedTheme}/>
    {loadedTheme && (
      <Routes>
        <Route path={"/"} element={<LogsLayout/>}>
          <Route path={"/"} element={<QueryPage/>}/>
          <Route path={router.overview} element={<OverviewPage/>}/>
          <Route path={router.streamContext} element={<StreamContext/>}/>
          <Route path={"/icons"} element={<PreviewIcons/>}/>
        </Route>
      </Routes>
    )}
  </AppContextProvider>
</HashRouter>
```

Key design choices:
- **`HashRouter`** — Uses URL hash (`#/`, `#/overview`) rather than browser history. This is required because the Go file server serves all paths from the same `index.html`, and hash-based routing avoids 404s on page refresh.
- **Theme gate** — Routes are only rendered after the theme loads (`loadedTheme` state), preventing a flash of unstyled content.
- **Nested routes** — All pages are children of `LogsLayout`, which provides the shared Header, Footer, and `<Outlet/>`.

#### Route definitions

The [`router/index.ts`](../app/vmui/packages/vmui/src/router/index.ts#L1) file defines four routes:

| Route | Path | Component | Header Controls |
|-------|------|-----------|-----------------|
| Home | `/` | `QueryPage` | Tenant, time selector, execution |
| Overview | `/overview` | `OverviewPage` | Tenant, time selector, execution |
| Stream Context | `/stream-context/:_stream_id/:_time` | `StreamContext` | None |
| Icons | `/icons` | `PreviewIcons` | None |

The `routerOptions` map at [`router/index.ts:19`](../app/vmui/packages/vmui/src/router/index.ts#L19) controls which header controls are shown per route (tenant selector, time picker, run button).

#### LogsLayout

[`LogsLayout`](../app/vmui/packages/vmui/src/layouts/LogsLayout/LogsLayout.tsx#L15) is the shared page shell:
- Sets `document.title` based on the matched route's title.
- Renders the `Header` with route-specific controls via `ControlsLogsLayout`.
- Renders the active page via `<Outlet/>`.
- Renders `Footer` with documentation links (hidden in app-mode embedding).
- Runs a one-time localStorage key migration on mount.

**Location**: [`app/vmui/packages/vmui/src/layouts/LogsLayout/LogsLayout.tsx:15-56`](../app/vmui/packages/vmui/src/layouts/LogsLayout/LogsLayout.tsx#L15)

---

### 4. State Management

**Files**:
- [`app/vmui/packages/vmui/src/contexts/AppContextProvider.tsx`](../app/vmui/packages/vmui/src/contexts/AppContextProvider.tsx#L10) - Provider composition
- [`app/vmui/packages/vmui/src/state/common/StateContext.tsx`](../app/vmui/packages/vmui/src/state/common/StateContext.tsx#L1) - App state context
- [`app/vmui/packages/vmui/src/state/common/reducer.ts`](../app/vmui/packages/vmui/src/state/common/reducer.ts#L7) - App state reducer

vmui uses Preact's `createContext` + `useReducer` pattern (similar to Redux) for state management. There is no external state library.

#### Provider chain

[`AppContextProvider`](../app/vmui/packages/vmui/src/contexts/AppContextProvider.tsx#L10) composes six providers using a `combineComponents()` utility:

```typescript
const providers = [
  AppStateProvider,     // Global config: server URL, theme, flags
  TimeStateProvider,    // Time range: duration, period, relative time, timezone
  QueryStateProvider,   // Query: text, history, autocomplete
  SnackbarProvider,     // Notifications
  LogsStateProvider,    // Logs panel UI state
  OverviewStateProvider // Overview page state
];
```

Each provider follows the same pattern: `createContext` → `useReducer(reducer, initialState)` → `Provider`. Each exposes a state hook and a dispatch hook.

#### State domains

| Domain | State Hook | Dispatch Hook | Key State Fields |
|--------|-----------|---------------|------------------|
| App | `useAppState()` | `useAppDispatch()` | `serverUrl`, `theme`, `isDarkTheme`, `flags`, `appConfig` |
| Time | `useTimeState()` | `useTimeDispatch()` | `duration`, `period` (start/end), `relativeTime`, `timezone` |
| Query | `useQueryState()` | `useQueryDispatch()` | `queryHistory`, `autocomplete`, `queryHasTimeFilter` |
| Logs | `useLogsState()` | `useLogsDispatch()` | Logs panel display options |
| Overview | `useOverviewState()` | `useOverviewDispatch()` | Overview page filters and state |

#### App state reducer (example)

The [`reducer.ts`](../app/vmui/packages/vmui/src/state/common/reducer.ts#L30) handles five action types:

```typescript
export type Action =
  | { type: "SET_SERVER", payload: string }
  | { type: "SET_THEME", payload: Theme }
  | { type: "SET_FLAGS", payload: Record<string, string | null> }
  | { type: "SET_APP_CONFIG", payload: AppConfig }
  | { type: "SET_DARK_THEME" }
```

Initial state is prepopulated from URL query params via `getQueryStringValue()` at [`StateContext.tsx:12`](../app/vmui/packages/vmui/src/state/common/StateContext.tsx#L12), so bookmarked URLs restore the full UI state.

**Location**: [`app/vmui/packages/vmui/src/state/common/reducer.ts:7-61`](../app/vmui/packages/vmui/src/state/common/reducer.ts#L7)

---

### 5. API Layer & Data Fetching

**Files**:
- [`app/vmui/packages/vmui/src/api/logs.ts`](../app/vmui/packages/vmui/src/api/logs.ts#L1) - Endpoint URL builders
- [`app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L28) - Log query hook
- [`app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogHits.ts`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogHits.ts#L33) - Hits chart hook
- [`app/vmui/packages/vmui/src/hooks/useTenant.ts`](../app/vmui/packages/vmui/src/hooks/useTenant.ts#L4) - Tenant header hook

#### API endpoints

Three URL builders in [`api/logs.ts`](../app/vmui/packages/vmui/src/api/logs.ts#L1) construct the full endpoint URLs from the configured server:

| Function | Endpoint | Used By |
|----------|----------|---------|
| `getLogsUrl(server)` | `/select/logsql/query` | `useFetchLogs` |
| `getLogHitsUrl(server)` | `/select/logsql/hits` | `useFetchLogHits` (HITS mode) |
| `getStatsQueryRangeUrl(server)` | `/select/logsql/stats_query_range` | `useFetchLogHits` (STATS mode) |

All requests use `POST` method with `URLSearchParams` body.

#### useFetchLogs hook

[`useFetchLogs()`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L28) is the primary data fetching hook:

**Request construction** ([`useFetchLogs.ts:44-74`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L44)):
- Body params: `query` (trimmed), `limit`, `start` (ISO), `end` (ISO)
- Headers: tenant headers (`AccountID`, `ProjectID`) + `Accept: application/stream+json`
- Method: POST

**Response handling** ([`useFetchLogs.ts:115-159`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L115)):
1. Reads response headers for tenant ID changes (server may return different AccountID/ProjectID).
2. Extracts `vl-request-duration-seconds` header for query timing display.
3. Reads the full response as text, splits by newline, and parses each line as JSON (NDJSON format).
4. Sets `logs` state with the parsed `Logs[]` array.

**Abort handling**: Each fetch creates a new `AbortController`. For logs queries, callers abort in-flight requests before starting a new one (for example `QueryPage`); for hits queries, `useFetchLogHits` aborts previous requests internally. The component unmount effect also aborts any pending request.

**beforeFetch interceptor** ([`useFetchLogs.ts:99-106`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L99)): An optional callback that can abort the request or modify the body before it's sent. Used by `useLimitGuard` to warn about large result sets.

#### useFetchLogHits hook

[`useFetchLogHits()`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogHits.ts#L33) fetches histogram data for the hits chart:

**Request params** ([`useFetchLogHits.ts:57-84`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogHits.ts#L57)):
- `query`, `step` (ms), `offset` (UTC offset in minutes), `start` (ISO), `end` (ISO)
- `fields_limit` (max field values to show), `field` (group-by field)

**Two query modes** ([`useFetchLogHits.ts:134-141`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogHits.ts#L134)):
- `GRAPH_QUERY_MODE.hits` — uses `/select/logsql/hits`, returns `LogHits[]` grouped by field values
- `GRAPH_QUERY_MODE.stats` — uses `/select/logsql/stats_query_range`, returns time-series stats

**Response processing**: Hits are sorted with "other" (empty fields) first, then by total count descending, for better chart visibility.

#### Tenant headers

The [`useTenant()`](../app/vmui/packages/vmui/src/hooks/useTenant.ts#L4) hook extracts `AccountID` and `ProjectID` from URL search params (defaulting to `"0"`) and returns them as an object. All fetch hooks spread these into their request headers:

```typescript
headers: {
  ...tenant,   // { AccountID: "0", ProjectID: "0" }
  Accept: "application/stream+json",
}
```

**Location**: [`app/vmui/packages/vmui/src/hooks/useTenant.ts:4-14`](../app/vmui/packages/vmui/src/hooks/useTenant.ts#L4)

---

### 6. Pages

#### QueryPage

**File**: [`app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx`](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L34)

The main query interface. Composes three data-fetching hooks and four visual sections:

**Data hooks**:
- [`useFetchLogs(query, limit)`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L28) — log results
- [`useFetchLogHits(query)`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogHits.ts#L33) — histogram data
- [`useFetchQueryTime(query)`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchQueryTime.ts) — time range from query's `_time` filter

**Visual structure**:
```
┌─────────────────────────────────────────┐
│  QueryPageHeader                        │  Query editor, limit, run button
├─────────────────────────────────────────┤
│  Alert (error / time filter warning)    │  Conditional
├─────────────────────────────────────────┤
│  HitsChart (BarHitsChart)               │  Log hit histogram (hideable)
├─────────────────────────────────────────┤
│  QueryPageBody                          │  Log results table (hideable)
└─────────────────────────────────────────┘
```

**Query execution** ([`handleRunQuery()`](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L118)):
1. Validates query is non-empty.
2. Calls `fetchQueryTime()` to check if the query contains a `_time` filter (which overrides the UI time picker).
3. Aborts any in-flight requests.
4. Debounces (300ms) data loading: `fetchLogs()` runs first, then `fetchLogHits()` (if both are enabled).
5. Syncs query, time range, and relative time to URL search params.
6. Updates query history.

**URL params synced**: `query`, `g0.range_input`, `g0.end_input`, `g0.relative_time`, `limit`, `hide_chart`, `hide_logs`, `graph_mode`.

**Location**: [`app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx:34-267`](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L34)

#### OverviewPage

**File**: [`app/vmui/packages/vmui/src/pages/OverviewPage/OverviewPage.tsx`](../app/vmui/packages/vmui/src/pages/OverviewPage/OverviewPage.tsx#L12)

An exploratory dashboard that shows aggregated stats without requiring a specific query:

```
┌─────────────────────────────────────────┐
│  FiltersBar                             │  Extra filter controls
├─────────────────────────────────────────┤
│  TotalsSection                          │  Summary stats (total logs, bytes, etc.)
├─────────────────────────────────────────┤
│  OverviewHits                           │  Hit histogram (same BarHitsChart)
├─────────────────────────────────────────┤
│  OverviewFields                         │  Top field names, values, stream names
├─────────────────────────────────────────┤
│  FiltersBarPreview                      │  Active filter preview
├─────────────────────────────────────────┤
│  OverviewLogs                           │  Sample log entries
└─────────────────────────────────────────┘
```

Uses custom hooks under `OverviewPage/hooks/` and `OverviewPage/Totals/hooks/`:
- `useFetchTotals` reuses `useFetchLogs` with stats queries like `* | stats count() as total`.
- `useFetchFieldNames` and `useFetchStreamNames` call dedicated endpoints (`/select/logsql/field_names`, `/select/logsql/stream_field_names`) directly.

**Location**: [`app/vmui/packages/vmui/src/pages/OverviewPage/OverviewPage.tsx:12-36`](../app/vmui/packages/vmui/src/pages/OverviewPage/OverviewPage.tsx#L12)

#### StreamContext

**File**: [`app/vmui/packages/vmui/src/pages/StreamContext/StreamContext.tsx`](../app/vmui/packages/vmui/src/pages/StreamContext/StreamContext.tsx#L8)

Shows log entries surrounding a specific log line, identified by `_stream_id` and `_time` (extracted from route params). Renders a `StreamContextList` component that fetches and displays context before and after the target log entry.

**Location**: [`app/vmui/packages/vmui/src/pages/StreamContext/StreamContext.tsx:8-31`](../app/vmui/packages/vmui/src/pages/StreamContext/StreamContext.tsx#L8)

---

### 7. Chart Visualization

**Files**:
- [`app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx`](../app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx#L33) - Main chart component
- [`app/vmui/packages/vmui/src/hooks/uplot/usePlotScale.ts`](../app/vmui/packages/vmui/src/hooks/uplot/usePlotScale.ts) - Scale management
- [`app/vmui/packages/vmui/src/utils/uplot/stack.ts`](../app/vmui/packages/vmui/src/utils/uplot/stack.ts) - Stacking logic

The hits chart uses [uPlot](https://github.com/leeoniya/uPlot) for high-performance time-series rendering.

#### Component hierarchy

```
HitsChart
  └── BarHitsChart
        └── BarHitsPlot              — uPlot instance management
              ├── BarHitsTooltip     — Hover tooltip (desktop only)
              └── BarHitsLegend      — Interactive legend with filter actions
```

#### uPlot lifecycle ([`BarHitsPlot.tsx`](../app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx#L33))

1. **Creation** ([line 111](../app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx#L109)): A new `uPlot` instance is created when the container ref, dark theme, or timezone changes. The previous instance is destroyed via cleanup.

2. **Data transforms** ([lines 45-53](../app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx#L45)): Raw data is optionally transformed through cumulative (`cumulativeMatrix()`) and stacking (`stack()`) pipelines based on `graphOptions`.

3. **Series sync** ([lines 84-98](../app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx#L84)): When series change, old series are removed via `delSeries()`, new ones added via `addSeries()`, and bands recalculated. Show/hide state from the previous series is preserved.

4. **Reactive updates**: Separate effects handle:
   - Scale range changes (`xRange`) → `redraw()`
   - Container size changes → `setSize()` + `redraw()`
   - Data changes → `setData()` + `redraw()`
   - Band changes → `delBand()` + `addBand()` + `redraw()`

#### Custom hooks for uPlot interaction

| Hook | Purpose |
|------|---------|
| [`usePlotScale`](../app/vmui/packages/vmui/src/hooks/uplot/usePlotScale.ts) | Manages x-axis range, converts zoom selections to time period updates |
| [`useReadyChart`](../app/vmui/packages/vmui/src/hooks/uplot/useReadyChart.ts) | Tracks chart ready state, handles pan gesture detection |
| [`useZoomChart`](../app/vmui/packages/vmui/src/hooks/uplot/useZoomChart.ts) | Handles zoom interactions, updates plot scale on zoom |

#### Graph options

The chart supports these display modes (toggled via the chart header UI):
- **Stacked**: Series values are stacked vertically (area chart style)
- **Cumulative**: Running total across time buckets
- **Fill**: Filled bars vs outline-only
- **Hide chart**: Collapses the chart section entirely

**Location**: [`app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx:33-164`](../app/vmui/packages/vmui/src/components/Chart/BarHitsChart/BarHitsPlot/BarHitsPlot.tsx#L33)

---

## Key Design Patterns

### 1. URL-Driven State

All significant UI state (query, time range, tenant, display options) is synced to URL search params via `useStateSearchParams()` and `useSearchParamsFromObject()`. This means:
- Bookmarks and shared links restore the full UI state.
- Browser back/forward navigates through query history.
- The `HashRouter` keeps all state in the URL hash fragment, avoiding server round-trips.

### 2. Context Composition via combineComponents()

Instead of deeply nesting providers in JSX, `AppContextProvider` uses a `combineComponents()` utility that programmatically composes the provider chain from an array. This keeps the provider list declarative and easy to reorder or extend.

### 3. Abort-Previous-Request Pattern

Fetch hooks create a new `AbortController` per request. `useFetchLogHits` aborts previous requests internally, while logs-query callers (for example `QueryPage`) abort previous requests before refetching. This prevents stale responses from overwriting newer data when the user rapidly changes queries or time ranges.

### 4. NDJSON Response Parsing

The `/select/logsql/query` endpoint returns newline-delimited JSON (NDJSON) with `Accept: application/stream+json`. The `useFetchLogs` hook reads the full response as text and splits by newline, parsing each line as independent JSON. This is simpler than true streaming but compatible with the streaming response format.

### 5. Debounced Query Execution

Query execution is debounced by 300ms (`useDebounceCallback`) to avoid firing redundant requests when multiple state changes happen in quick succession (e.g., typing in the query editor while the time range auto-updates).

---

## Local Development Workflow

### Quick start

```bash
cd app/vmui/packages/vmui
npm ci                        # Install dependencies
npm start                     # Dev server at http://localhost:3000
```

The dev server serves the SPA on port 3000. You need a running VictoriaLogs instance for API calls, or use the playground proxy.

### Playground mode

To develop against the public VictoriaLogs playground without running a local server:

```bash
npm run start:playground
# Or equivalently:
PLAYGROUND=LOGS npm start
```

This configures Vite's dev proxy to forward `/select/*` and `/flags` requests to `https://play-vmlogs.victoriametrics.com`, stripping tenant headers before proxying. See [`vite.config.ts:7-33`](../app/vmui/packages/vmui/vite.config.ts#L7).

### Against a local VictoriaLogs instance

Run VictoriaLogs locally (default port 9428), then point the UI's server URL to it. The UI reads the server URL from the `serverUrl` URL param or the global settings panel.

### Build and embed cycle

To update the embedded UI in the Go binary:

```bash
# From repository root:
make vmui-update    # Build SPA + copy to app/vlselect/vmui/
make victoria-logs  # Rebuild Go binary with new UI assets
```

The `vmui-update` target:
1. Generates LogsQL pipe definitions (`gen:logsql-pipes`).
2. Runs `vite build` (output to `app/vmui/packages/vmui/build/`).
3. Copies built assets to `app/vlselect/vmui/` (removing old files first).
4. Removes `dashboards/` directory (not needed for VictoriaLogs).

### Code quality checks

```bash
npm run lint:local   # ESLint
npm run typecheck    # TypeScript type checking
npm run test         # Vitest unit tests
npm run precommit    # All three in sequence
```

Or via Docker (matches CI):

```bash
make vmui-lint
make vmui-typecheck
make vmui-test
```

---

## See Also

- [VictoriaLogs Query/Select Flow](./onboarding-select-flow.md)
- [VictoriaLogs LogsQL Parser & Pipe Execution](./onboarding-logsql-parser-pipes.md)
- [VictoriaLogs vlogscli Internals](./onboarding-vlogscli.md)
- [Glossary](./glossary.md)

---

## Document Maintenance Guidelines

### File Linking Syntax and Policy

This document uses VS Code-compatible markdown links to enable quick navigation from documentation to source code. All file references should follow these guidelines to maintain clickability.

#### Required Format

All file and line number references must use the following format:

```markdown
[DisplayText](path/to/file.ext#LLineNumber)
```

**Key requirements**:
- Use capital `L` prefix before the line number (e.g., `#L28`, not `#28`)
- Use consistent relative paths from this document location (this file currently uses `../` to reach repo root)
- Include line numbers wherever possible

#### Standard Patterns

**1. Section Headers - File References**

```markdown
**File**: [`app/vmui/packages/vmui/src/App.tsx`](../app/vmui/packages/vmui/src/App.tsx#L13)
```

**2. Function References in Key Functions Lists**

```markdown
- [`useFetchLogs()`](../app/vmui/packages/vmui/src/pages/QueryPage/hooks/useFetchLogs.ts#L28) - Log fetch hook
```

**3. Flow Diagram References**

```markdown
handleRunQuery()                         [QueryPage.tsx:118](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L118)
```

**4. Location References**

```markdown
**Location**: [`app/vmui/packages/vmui/src/App.tsx:13-53`](../app/vmui/packages/vmui/src/App.tsx#L13)
```

#### Line Number Selection

When linking to code:

- **Single function**: Link to the function definition line
- **Component**: Link to the component function declaration
- **Hook**: Link to the exported hook function
- **Code section**: Link to the first line of the section
- **Range (lines X-Y)**: Link to the start line (X)

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
# Check for links missing the #L prefix (TypeScript/TSX files)
grep -n '\](.*\.\(ts\|tsx\)#[0-9]' onboarding/onboarding-vmui.md

# Count total clickable line references
grep -o "#L[0-9]\+" onboarding/onboarding-vmui.md | wc -l

# Find non-clickable file references
grep -n '`app/.*\.\(ts\|tsx\|go\)`' onboarding/onboarding-vmui.md | grep -v '\]('
```

#### Why This Matters

Clickable file references significantly improve the onboarding experience by:
- Reducing friction when exploring the codebase
- Enabling instant navigation from concept to implementation
- Making the documentation a living, interactive guide
- Helping developers quickly verify documented behavior against actual code

**Remember**: Every file reference is an opportunity to help a developer learn faster. Make them all clickable!
