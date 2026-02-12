# VictoriaLogs LogsQL Parser & Pipe Execution - Developer Onboarding Guide

This document provides a comprehensive overview of how LogsQL queries are parsed, rewritten, and executed in VictoriaLogs, from query text at `/select/logsql/*` endpoints to parallel block scanning and pipe processing.

## Table of Contents

- [Overview](#overview)
- [Complete Data Flow](#complete-data-flow)
- [Architecture Layers](#architecture-layers)
  - [1. Query Model](#1-query-model)
  - [2. Query Parsing](#2-query-parsing)
  - [3. Filter Parsing](#3-filter-parsing)
  - [4. Pipe Parsing](#4-pipe-parsing)
  - [5. Query Rewrites & Optimizations](#5-query-rewrites--optimizations)
  - [6. QueryContext & Subquery Initialization](#6-querycontext--subquery-initialization)
  - [7. Pipe Execution Chain](#7-pipe-execution-chain)
  - [8. Storage Search Planning & Parallel Execution](#8-storage-search-planning--parallel-execution)
- [Execution Modes](#execution-modes)
  - [Local Storage Mode](#local-storage-mode)
  - [Distributed Storage Mode](#distributed-storage-mode)
- [Key Design Patterns](#key-design-patterns)
- [Query Options](#query-options)
- [Summary](#summary)

---

## Overview

VictoriaLogs parses LogsQL queries in `lib/logstorage/parser.go` and executes them through `app/vlstorage/main.go` (router) and `lib/logstorage/storage_search.go` using:

- A filter tree (`Query.f`)
- A pipe list (`Query.pipes`)
- Query options (`Query.opts`)

The parser first produces a semantic query structure, then applies rewrites (for example, filter merging and pipe optimizations). The executor then:

1. Materializes subqueries (`in(subquery)`, `join`, `union`, `stream_context`)
2. Builds storage search options
3. Executes parallel part/block scanning
4. Streams matching blocks through the pipe processor chain

### Where This Runs

- Public API parsing starts from select handlers in [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1149)
- Local execution runs in [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L208)
- Distributed execution additionally uses [`lib/logstorage/net_query_runner.go`](../lib/logstorage/net_query_runner.go#L31)

---

## Complete Data Flow

**Key Files**:
- [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1149) - Select endpoint query handling
- [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L1671) - LogsQL parsing and query rewrites
- [`lib/logstorage/pipe.go`](../lib/logstorage/pipe.go#L13) - Pipe and pipeProcessor contracts
- [`app/vlstorage/main.go`](../app/vlstorage/main.go#L554) - Storage router
- [`app/vlstorage/lastnoptimization.go`](../app/vlstorage/lastnoptimization.go#L15) - Last-N optimized execution path
- [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L208) - Local query execution
- [`lib/logstorage/net_query_runner.go`](../lib/logstorage/net_query_runner.go#L31) - Distributed query split (remote/local)

```
HTTP Request (GET/POST /select/logsql/query?query=...)
    ↓
logsql.ProcessQueryRequest               [logsql.go:1149](../app/vlselect/logsql/logsql.go#L1149)
    ↓
parseCommonArgsWithConfig                [logsql.go:1362](../app/vlselect/logsql/logsql.go#L1362)
    ↓
logstorage.ParseQueryAtTimestamp         [parser.go:1697](../lib/logstorage/parser.go#L1697)
    ↓
parseQuery + parseFilter + parsePipes    [parser.go:1779](../lib/logstorage/parser.go#L1779), [pipe.go:115](../lib/logstorage/pipe.go#L115)
    ↓
q.optimize()                             [parser.go:898](../lib/logstorage/parser.go#L898)
    ↓
optional AddTimeFilter / AddExtraFilters [logsql.go:1436](../app/vlselect/logsql/logsql.go#L1436), [parser.go:846](../lib/logstorage/parser.go#L846)
    ↓
ca.newQueryContext(ctx)                  [logsql.go:1350](../app/vlselect/logsql/logsql.go#L1350)
    ↓
vlstorage.RunQuery(qctx, writeBlock)     [main.go:554](../app/vlstorage/main.go#L554)
    ↓
    ├─→ [LAST-N OPTIMIZED PATH]
    │   GetLastNResultsQuery             [parser.go:649](../lib/logstorage/parser.go#L649)
    │       ↓
    │   runOptimizedLastNResultsQuery    [lastnoptimization.go:15](../app/vlstorage/lastnoptimization.go#L15)
    │
    ├─→ [LOCAL MODE]
    │   localStorage.RunQuery            [storage_search.go:208](../lib/logstorage/storage_search.go#L208)
    │       ↓
    │   initSubqueries                   [storage_search.go:756](../lib/logstorage/storage_search.go#L756)
    │       ↓
    │   getSearchOptions                 [storage_search.go:235](../lib/logstorage/storage_search.go#L235)
    │       ↓
    │   runPipes                         [storage_search.go:269](../lib/logstorage/storage_search.go#L269)
    │       ↓
    │   searchParallel                   [storage_search.go:1274](../lib/logstorage/storage_search.go#L1274)
    │       ↓
    │   partition/part/block scan → pipe processors → writeBlock
    │
    └─→ [DISTRIBUTED MODE]
        netselect.Storage.RunQuery       [netselect.go:385](../app/vlstorage/netselect/netselect.go#L385)
            ↓
        NewNetQueryRunner                [net_query_runner.go:31](../lib/logstorage/net_query_runner.go#L31)
            ↓
        initSubqueries                   [net_query_runner.go:37](../lib/logstorage/net_query_runner.go#L37)
            ↓
        splitQueryToRemoteAndLocal       [net_query_runner.go:72](../lib/logstorage/net_query_runner.go#L72)
            ↓
        remote /internal/select/query + local pipe tail
```

---

## Architecture Layers

### 1. Query Model

**File**: [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L367)

`Query` is the canonical parsed representation:

```go
type Query struct {
    opts      queryOptions
    f         filter
    pipes     []pipe
    timestamp int64
}
```

**Key Functions**:
- [`GetConcurrency()`](../lib/logstorage/parser.go#L467) - CPU worker concurrency
- [`GetParallelReaders(defaultParallelReaders)`](../lib/logstorage/parser.go#L447) - I/O parallelism
- [`GetFilterTimeRange()`](../lib/logstorage/parser.go#L759) - Effective time bounds
- [`Clone(timestamp)`](../lib/logstorage/parser.go#L625) - Re-parse clone for transformations
- [`DropAllPipes()`](../lib/logstorage/parser.go#L527) - Keep only filter stage

**Location**: [`lib/logstorage/parser.go:367-405`](../lib/logstorage/parser.go#L367)

---

### 2. Query Parsing

**File**: [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L1671)

**Key Functions**:
- [`ParseQuery(s)`](../lib/logstorage/parser.go#L1671) - Parse at current timestamp
- [`ParseQueryAtTimestamp(s, ts)`](../lib/logstorage/parser.go#L1697) - Parse at explicit timestamp context
- [`parseQuery(lex)`](../lib/logstorage/parser.go#L1779) - Parse options + filter + pipes
- [`parseQueryOptions(dstOpts, lex)`](../lib/logstorage/parser.go#L1845) - Parse `options(...)`

Parsing sequence:

1. Build lexer from query string
2. Parse options block (if present)
3. Parse root filter expression
4. Parse optional pipe chain
5. Verify no unparsed tail
6. Apply query rewrites and stats-time initialization

API request-level additions before execution:

- `/select/logsql/query` request flow parses at explicit timestamp via [`ParseQueryAtTimestamp`](../lib/logstorage/parser.go#L1697) from [`parseCommonArgsWithConfig`](../app/vlselect/logsql/logsql.go#L1362)
- HTTP `start`/`end` may inject global `_time` filter via [`AddTimeFilter`](../lib/logstorage/parser.go#L787) at [`logsql.go:1436`](../app/vlselect/logsql/logsql.go#L1436)
- `extra_filters` / `extra_stream_filters` are appended via [`AddExtraFilters`](../lib/logstorage/parser.go#L846) at [`logsql.go:1450`](../app/vlselect/logsql/logsql.go#L1450)

---

### 3. Filter Parsing

**File**: [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L1943)

Filter parsing handles precedence and dispatch:

- OR layer: [`parseFilterOr`](../lib/logstorage/parser.go#L1964)
- AND layer: [`parseFilterAnd`](../lib/logstorage/parser.go#L1987)
- operator/type dispatch: [`parseFilterGeneric`](../lib/logstorage/parser.go#L2010)

**Key Functions**:
- [`parseFilter`](../lib/logstorage/parser.go#L1943) - Root filter parse and guard rails
- [`parseFilterPhrase`](../lib/logstorage/parser.go#L2099) - Default token/field phrase handling
- [`parseInValues`](../lib/logstorage/parser.go#L2480) - `in(...)` literal vs subquery fallback
- [`parseInQuery`](../lib/logstorage/parser.go#L3741) - Parse `in(subquery)` and infer value field

Notable guard rails:

- Prevents starting query filter with pipe/stats keywords unless quoted ([`parser.go:1948-1954`](../lib/logstorage/parser.go#L1948))
- Rejects invalid syntactic forms such as missing `:` before field-specific operators

---

### 4. Pipe Parsing

**File**: [`lib/logstorage/pipe.go`](../lib/logstorage/pipe.go#L13)

Pipes are parsed by name and mapped to typed implementations.

**Key Functions**:
- [`parsePipes(lex)`](../lib/logstorage/pipe.go#L115) - Parse chain `| p1 | p2 | ...`
- [`parsePipe(lex)`](../lib/logstorage/pipe.go#L135) - Parse single pipe
- [`initPipeParsers()`](../lib/logstorage/pipe.go#L177) - Pipe parser registry
- [`isPipeName(s)`](../lib/logstorage/pipe.go#L242) - Pipe keyword detection

Core contracts:

- [`type pipe`](../lib/logstorage/pipe.go#L13) - parse/runtime metadata + processor factory
- [`type pipeProcessor`](../lib/logstorage/pipe.go#L65) - `writeBlock()` + `flush()` runtime stage

---

### 5. Query Rewrites & Optimizations

**File**: [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L898)

Rewrites are applied after parsing via [`Query.optimize()`](../lib/logstorage/parser.go#L898).

**Key Function**:
- [`optimizeNoSubqueries()`](../lib/logstorage/parser.go#L904)

Notable rewrites:

- Merge leading `| filter ...` into root filter
- Flatten nested AND/OR trees
- Remove no-op star filters
- Merge stream filters
- Optimize offset/limit and uniq/limit pipe forms
- Router-level fast path for eligible last-N queries via [`GetLastNResultsQuery`](../lib/logstorage/parser.go#L649) and [`runOptimizedLastNResultsQuery`](../app/vlstorage/lastnoptimization.go#L15)

Time-filter augmentation:

- [`AddTimeFilter(start, end)`](../lib/logstorage/parser.go#L787)
- Internal injection in [`addTimeFilter(...)`](../lib/logstorage/parser.go#L806)

---

### 6. QueryContext & Subquery Initialization

**File**: [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L25)

`QueryContext` binds query AST with runtime context, tenant scope, hidden field filters, and query stats collector.

**Key Functions**:
- [`NewQueryContext(...)`](../lib/logstorage/storage_search.go#L54)
- [`newQueryContext(...)`](../lib/logstorage/storage_search.go#L79) - applies option override for `allow_partial_response`
- [`initSubqueries(...)`](../lib/logstorage/storage_search.go#L756)

Subquery initialization steps:

1. Resolve `in(subquery)` values ([`initFilterInValues`](../lib/logstorage/storage_search.go#L811))
2. Build join maps for `join` pipes ([`initJoinMaps`](../lib/logstorage/storage_search.go#L869))
3. Initialize `union` execution hooks ([`initUnionQueries`](../lib/logstorage/storage_search.go#L839))
4. Validate/initialize `stream_context` ([`initStreamContextPipes`](../lib/logstorage/storage_search.go#L784))

---

### 7. Pipe Execution Chain

**File**: [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L269)

Pipe execution is orchestrated by [`runPipes(...)`](../lib/logstorage/storage_search.go#L269):

1. Build processor chain in reverse order (`last pipe` created first)
2. Execute search callback and stream blocks into the head processor
3. Flush processors in forward order
4. Propagate cancellation on search/flush error

**Key Mechanics**:
- Chain construction: [`storage_search.go:283-293`](../lib/logstorage/storage_search.go#L283)
- Search invocation: [`storage_search.go:295`](../lib/logstorage/storage_search.go#L295)
- Flush and error handling: [`storage_search.go:301-325`](../lib/logstorage/storage_search.go#L301)
- Query stats injection for `query_stats` pipes: [`storage_search.go:303-307`](../lib/logstorage/storage_search.go#L303)

---

### 8. Storage Search Planning & Parallel Execution

**File**: [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L1274)

Search planning starts with [`getSearchOptions(...)`](../lib/logstorage/storage_search.go#L235), then executes with [`searchParallel(...)`](../lib/logstorage/storage_search.go#L1274).

**Key Functions**:
- [`Storage.RunQuery`](../lib/logstorage/storage_search.go#L208)
- [`Storage.runQuery`](../lib/logstorage/storage_search.go#L216)
- [`getSearchOptions`](../lib/logstorage/storage_search.go#L235)
- [`searchParallel`](../lib/logstorage/storage_search.go#L1274)
- [`getPartitionsForTimeRange`](../lib/logstorage/storage_search.go#L1354)
- [`partition.search`](../lib/logstorage/storage_search.go#L1395)
- [`part.searchByTenantIDs`](../lib/logstorage/storage_search.go#L1614)
- [`part.searchByStreamIDs`](../lib/logstorage/storage_search.go#L1716)

Execution highlights:

- Partition selection uses binary search over sorted partitions
- Work is batched by block headers and processed by worker goroutines
- Query stats are gathered per worker and merged atomically
- Partition search concurrency is capped by [`partitionSearchConcurrencyLimitCh`](../lib/logstorage/storage_search.go#L1391)

---

## Execution Modes

### Local Storage Mode

Flow:

- Select API parses and augments query (`start/end`, `extra_filters`) -> `vlstorage.RunQuery` -> [`localStorage.RunQuery`](../lib/logstorage/storage_search.go#L208)
- `vlstorage.RunQuery` may use last-N fast path before local/distributed dispatch ([`main.go:554`](../app/vlstorage/main.go#L554), [`lastnoptimization.go:15`](../app/vlstorage/lastnoptimization.go#L15))
- Entire parse and pipe execution lifecycle runs in-process
- Pipes execute directly on locally scanned blocks

### Distributed Storage Mode

Flow:

- Frontend query execution starts at [`netselect.Storage.RunQuery`](../app/vlstorage/netselect/netselect.go#L385)
- [`NewNetQueryRunner`](../lib/logstorage/net_query_runner.go#L31) initializes subqueries before split ([`net_query_runner.go:37`](../lib/logstorage/net_query_runner.go#L37))
- Split logic:
  - [`splitQueryToRemoteAndLocal`](../lib/logstorage/net_query_runner.go#L72)
  - per-pipe split contract: [`splitToRemoteAndLocal`](../lib/logstorage/pipe.go#L24)

Special case:

- `query_stats` executes as remote + local tandem:
  - Remote: [`pipeQueryStats`](../lib/logstorage/pipe_query_stats.go#L11)
  - Local: [`pipeQueryStatsLocal`](../lib/logstorage/pipe_query_stats_local.go#L9)

---

## Key Design Patterns

### 1. AST + Runtime Separation

`Query`/`filter`/`pipe` types represent parsed semantics, while `pipeProcessor` instances represent runtime execution stages. This separation makes parsing deterministic and execution composable.

### 2. Reverse Chain Assembly

`runPipes` builds processors from tail to head, allowing each pipe to wrap downstream behavior in a pipeline style.

### 3. Two-Phase Subquery Evaluation

Complex subquery constructs are materialized before main search, reducing per-row work during block scanning.

### 4. Push-Based Block Streaming

Search workers emit matching blocks to processors immediately, enabling progressive processing and backpressure through cancellation.

### 5. Time-Range-First Pruning

Partition/part/block time bounds are used early to skip irrelevant data and reduce read amplification.

### 6. Cluster Pipe Splitting

Each pipe controls remote/local split behavior, enabling efficient distributed execution without hardcoding pipe-specific cluster logic into the runner.

---

## Query Options

**Key File**: [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L1845)

`options(...)` controls query runtime behavior:

| Option | Parse Line | Meaning |
|--------|------------|---------|
| `concurrency` | [1873](../lib/logstorage/parser.go#L1873) | Max CPU-bound workers for pipe processing |
| `parallel_readers` | [1880](../lib/logstorage/parser.go#L1880) | IO-bound readers for block scanning |
| `ignore_global_time_filter` | [1887](../lib/logstorage/parser.go#L1887) | Skip external/global time filter injection |
| `allow_partial_response` | [1894](../lib/logstorage/parser.go#L1894) | Allow partial results in cluster failures |
| `time_offset` | [1901](../lib/logstorage/parser.go#L1901) | Shift query time filters and output timestamps |

Related runtime application:

- `allow_partial_response` override in [`newQueryContext`](../lib/logstorage/storage_search.go#L79)
- output timestamp offset application in [`searchParallel`](../lib/logstorage/storage_search.go#L1298)

---

## Summary

The LogsQL parser and executor are designed for:

1. **Determinism**: Explicit parser stages produce a stable query model
2. **Performance**: Rewrite passes and needed-field projection reduce unnecessary work
3. **Scalability**: Parallel search and worker pools scale across cores and partitions
4. **Extensibility**: Pipe interface contracts make new pipe types pluggable
5. **Distribution**: Remote/local pipe split supports efficient cluster execution
6. **Observability**: Query stats capture read/process costs across execution stages

The key insight is the **two-level execution model**:

- Planning level: parse, optimize, and materialize subqueries
- Runtime level: scan blocks in parallel and stream through a cancellable pipe chain

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
- Use capital `L` prefix before the line number (e.g., `#L208`, not `#208`)
- Use consistent relative paths from this document location (this file currently uses `../` to reach repo root)
- Include line numbers wherever possible

#### Standard Patterns

**1. Section Headers - File References**

```markdown
**File**: [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L208)
```

**2. Function References in Key Functions Lists**

```markdown
**Key Functions**:
- [`ParseQuery(s)`](../lib/logstorage/parser.go#L1671) - Parse query at current timestamp
- [`runPipes(...)`](../lib/logstorage/storage_search.go#L269) - Execute pipe chain
```

**3. Flow Diagram References**

```markdown
logstorage.ParseQuery                    [parser.go:1671](../lib/logstorage/parser.go#L1671)
```

**4. Location References**

```markdown
**Location**: [`lib/logstorage/parser.go:1779-1805`](../lib/logstorage/parser.go#L1779)
```

#### Display Text Options

Choose the appropriate display text based on context:

- **Full path with backticks**: `` [`lib/logstorage/parser.go`](path#L1671) `` - Use in section headers
- **Filename only**: `[parser.go:1671](path#L1671)` - Use in flow diagrams for brevity
- **Function signature**: `[ParseQuery(s)](path#L1671)` - Use in Key Functions lists
- **Description**: `[Parse query at current timestamp](path#L1671)` - Use when context is clear

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
- [`ParseQuery(s)`](../lib/logstorage/parser.go#L1671) - Parse query
- **File**: [`storage_search.go`](../lib/logstorage/storage_search.go#L208)
- [pipe.go:115](../lib/logstorage/pipe.go#L115)
```

❌ **Incorrect**:
```markdown
- `ParseQuery(s)` - Parse query (line 1671)  # Not clickable
- **File**: `storage_search.go` (line 208)   # Not clickable
- [pipe.go:115](../lib/logstorage/pipe.go#115)  # Missing L prefix
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
grep -n '\](.*\.go#[0-9]' onboarding/onboarding-logsql-parser-pipes.md

# Count total clickable line references
grep -o "#L[0-9]\+" onboarding/onboarding-logsql-parser-pipes.md | wc -l

# Find non-clickable file references
grep -n '`lib/.*\.go`' onboarding/onboarding-logsql-parser-pipes.md | grep -v '\]('
```

#### Why This Matters

Clickable file references significantly improve the onboarding experience by:

- Reducing friction when exploring the codebase
- Enabling instant navigation from concept to implementation
- Making the documentation a living, interactive guide
- Helping developers quickly verify documented behavior against actual code

**Remember**: Every file reference is an opportunity to help a developer learn faster. Make them all clickable.
