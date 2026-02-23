# VictoriaLogs vlogscli Internals - Developer Onboarding Guide

This document provides a comprehensive overview of [vlogscli](./glossary.md#vlogscli), VictoriaLogs' interactive command-line query tool. It covers the REPL loop, query execution, output formatting, live tailing, paging with `less`, and connection handling.

## Table of Contents

- [Overview](#overview)
- [Complete Data Flow](#complete-data-flow)
- [Architecture Layers](#architecture-layers)
  - [1. Entry Point & Initialization](#1-entry-point--initialization)
  - [2. REPL Loop](#2-repl-loop)
  - [3. Query Execution](#3-query-execution)
  - [4. Output Formatting (JSON Prettifier)](#4-output-formatting-json-prettifier)
  - [5. Paging with Less](#5-paging-with-less)
  - [6. Live Tailing](#6-live-tailing)
  - [7. Connection & Authentication](#7-connection--authentication)
- [Key Design Patterns](#key-design-patterns)
- [Configuration Flags](#configuration-flags)
- [Document Maintenance Guidelines](#document-maintenance-guidelines)

---

## Overview

**[vlogscli](./glossary.md#vlogscli)** is a small (3-file, ~900 lines) interactive CLI for querying VictoriaLogs. It provides a readline-based REPL that sends LogsQL queries to VictoriaLogs `/select/logsql/*` endpoints (see [VictoriaLogs Query/Select Flow](./onboarding-select-flow.md)), formats the streaming [NDJSON](./glossary.md#ndjson) response into human-readable output, and pipes results through `less` for paging.

### Key characteristics

- **Interactive REPL**: Readline-based with history, multi-line query support, and special commands.
- **Streaming**: Processes and displays results as they arrive — no buffering of the entire response.
- **4 output modes**: Multiline JSON (default), singleline JSON, logfmt, and compact.
- **Paging**: Automatically pipes output through `less` when running in a terminal.
- **Live tailing**: `\tail <query>` streams live results via the `/select/logsql/tail` endpoint from the [Query/Select flow](./onboarding-select-flow.md#live-tailing).
- **Non-interactive mode**: Supports piped input (`echo "query;" | vlogscli`) for scripting.

### File structure

| File | Lines | Purpose |
|------|-------|---------|
| [`main.go`](../app/vlogscli/main.go#L101) | 719 | Entry point, REPL loop, query execution, HTTP client, history |
| [`json_prettifier.go`](../app/vlogscli/json_prettifier.go#L35) | 409 | Streaming JSON-to-format conversion with 4 output modes |
| [`less_wrapper.go`](../app/vlogscli/less_wrapper.go#L65) | 216 | Terminal detection, `less` paging, signal management |

---

## Complete Data Flow

**Key Files**:
- [`app/vlogscli/main.go`](../app/vlogscli/main.go#L101) - REPL and HTTP execution
- [`app/vlogscli/json_prettifier.go`](../app/vlogscli/json_prettifier.go#L113) - Output formatting
- [`app/vlogscli/less_wrapper.go`](../app/vlogscli/less_wrapper.go#L65) - Paging

```
User types query + ";"
    ↓
runReadlineLoop()                        [main.go:176](../app/vlogscli/main.go#L176)
    ↓
Multi-line accumulation (no ";" → continue)
    ↓
executeQuery(ctx, rl, qStr, ...)         [main.go:451](../app/vlogscli/main.go#L451)
    ↓
logstorage.ParseQuery(qStr)             [main.go:546](../app/vlogscli/main.go#L546)
    ↓
POST http://victorialogs:9428/select/logsql/query
    Content-Type: application/x-www-form-urlencoded
    Body: query=<canonical query>
    ↓
HTTP response (streaming [NDJSON](./glossary.md#ndjson))
    ↓
newJSONPrettifier(resp.Body, outputMode) [json_prettifier.go:113](../app/vlogscli/json_prettifier.go#L113)
    ↓
    ┌─ Background goroutine ─────────────────────────────────┐
    │  prettifyJSONLines()                                    │
    │    ↓                                                    │
    │  readNextJSONObject(decoder)  → []Field                │
    │    ↓                                                    │
    │  sort fields alphabetically                            │
    │    ↓                                                    │
    │  formatter(bw, fields)  → write to pipe                │
    │    ↓                                                    │
    │  bw.Flush()  → immediate display                       │
    └─────────────────────────────────────────────────────────┘
    ↓  (io.Pipe connects prettifier to pager)
readWithLess(jp, disableColors, wrapLongLines)  [less_wrapper.go:65](../app/vlogscli/less_wrapper.go#L65)
    ↓
    ├─ [TERMINAL]
    │   less -F -X [-R] [-S]
    │     stdin ← pipe ← prettified output
    │     stdout → user's terminal
    │
    └─ [NON-TERMINAL / PIPED]
        io.Copy(os.Stdout, r)
```

---

## Architecture Layers

### 1. Entry Point & Initialization

**File**: [`app/vlogscli/main.go`](../app/vlogscli/main.go#L101)

**Key Functions**:
- [`main()`](../app/vlogscli/main.go#L101) - Entry point
- [`newHTTPClient()`](../app/vlogscli/main.go#L625) - Create HTTP client with auth
- [`newAuthConfig()`](../app/vlogscli/main.go#L639) - Build authentication configuration
- [`parseHeaders(a)`](../app/vlogscli/main.go#L687) - Parse custom HTTP headers

```go
func main() {
    envflag.Parse()
    buildinfo.Init()
    logger.InitNoLogFlags()

    // Parse custom headers from -header flags
    hes, err := parseHeaders(*header)
    headers = hes

    // Create HTTP client with auth/TLS
    authConfig, httpClient = newHTTPClient()

    // Initialize readline with prompt and listener
    incompleteLine := ""
    cfg := &readline.Config{
        Prompt:                 firstLinePrompt,    // ";> "
        DisableAutoSaveHistory: true,
        Listener: func(line []rune, pos int, _ rune) ([]rune, int, bool) {
            incompleteLine = string(line)  // Track incomplete input for Ctrl+C
            return line, pos, false
        },
    }
    rl, _ := readline.NewFromConfig(cfg)

    fmt.Fprintf(rl, "sending queries to -datasource.url=%s\n", *datasourceURL)
    runReadlineLoop(rl, &incompleteLine)
}
```

The prompt is `;> ` — the semicolon reminds users that queries must end with `;` to execute.

**Location**: [`app/vlogscli/main.go:101-161`](../app/vlogscli/main.go#L101)

---

### 2. REPL Loop

**File**: [`app/vlogscli/main.go`](../app/vlogscli/main.go#L176)

**Key Functions**:
- [`runReadlineLoop(rl, incompleteLine)`](../app/vlogscli/main.go#L176) - Main REPL loop
- [`pushToHistory(rl, historyLines, s)`](../app/vlogscli/main.go#L337) - Save query to history
- [`loadFromHistory(filePath)`](../app/vlogscli/main.go#L365) - Load history from disk
- [`mustSaveToHistory(filePath, lines)`](../app/vlogscli/main.go#L393) - Persist history to disk
- [`printCommandsHelp(w)`](../app/vlogscli/main.go#L427) - Display command reference

#### Multi-line query handling

Queries accumulate across lines until a line ending with `;` is entered:

```go
s += line
// ...
if line != "" && !strings.HasSuffix(line, ";") {
    // Query is incomplete — allow continuation on next line
    s += "\n"
    rl.SetPrompt(nextLinePrompt)  // Empty prompt for continuation lines
    continue
}
// Line ends with ";" → execute the full query
```

Example session:
```
;> _time:1h
   | stats count() by (level)
   | sort by (count) desc;
executing [_time:1h | stats count() by (level) | sort by (count) desc]...
```

#### Special commands

All commands are checked before the `;`-terminated query logic:

| Command | Action |
|---------|--------|
| `\q`, `q`, `quit`, `exit` | Exit the REPL |
| `\h`, `h`, `help`, `?` | Print command reference |
| `\s` | Switch to singleline JSON output |
| `\m` | Switch to multiline JSON output (default) |
| `\c` | Switch to compact output |
| `\logfmt` | Switch to logfmt output |
| `\wrap_long_lines` | Toggle line wrapping in pager |
| `\enable_colors` | Enable ANSI colors in compact mode |
| `\disable_colors` | Disable ANSI colors in compact mode |
| `\tail <query>` | Live tail query results (see [Live Tailing](#6-live-tailing)) |

#### Signal handling (Ctrl+C)

Three contexts for Ctrl+C:

1. **Empty prompt**: Exit with code 130 (`128 + SIGINT`)
2. **Mid-line input**: Save the incomplete text to history, clear the prompt
3. **During query execution**: Cancel the HTTP request via `context.Context`

```go
// Create cancellable context for each query
ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
executeQuery(ctx, rl, s, outputMode, disableColors, wrapLongLines)
cancel()
```

#### History management

- **Format**: Quoted strings, one per line (using `strconv.Quote`/`Unquote` for safe escaping)
- **Limit**: 500 entries (oldest trimmed)
- **Deduplication**: Consecutive identical queries are not duplicated
- **Persistence**: Saved to disk after every query via `fs.MustWriteSync()`
- **Default path**: `vlogscli-history` in the current directory

**Location**: [`app/vlogscli/main.go:176-427`](../app/vlogscli/main.go#L176)

---

### 3. Query Execution

**File**: [`app/vlogscli/main.go`](../app/vlogscli/main.go#L451)

**Key Functions**:
- [`executeQuery(ctx, output, qStr, outputMode, disableColors, wrapLongLines)`](../app/vlogscli/main.go#L451) - Route to query or tail
- [`getQueryResponse(ctx, output, qStr, outputMode, qURL)`](../app/vlogscli/main.go#L541) - Send HTTP request and wrap response

#### Request pipeline

```go
func getQueryResponse(ctx context.Context, output io.Writer, qStr string, outputMode outputMode, qURL string) io.ReadCloser {
    // 1. Parse query into canonical form
    qStr = strings.TrimSuffix(qStr, ";")
    q, err := logstorage.ParseQuery(qStr)
    qStr = q.String()
    fmt.Fprintf(output, "executing [%s]...", qStr)

    // 2. Build POST request
    args := make(url.Values)
    args.Set("query", qStr)
    req, _ := http.NewRequestWithContext(ctx, "POST", qURL, strings.NewReader(args.Encode()))
    req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
    req.Header.Set("AccountID", strconv.Itoa(*accountID))
    req.Header.Set("ProjectID", strconv.Itoa(*projectID))

    // 3. Apply auth headers
    authConfig.SetHeaders(req, true)

    // 4. Execute and measure duration
    startTime := time.Now()
    resp, err := httpClient.Do(req)

    // Prefer server-reported duration if available
    queryDuration := fmt.Sprintf("client %.3f", time.Since(startTime).Seconds())
    if qd := resp.Header.Get("VL-Request-Duration-Seconds"); qd != "" {
        queryDuration = "server " + qd
    }
    fmt.Fprintf(output, "; duration: %ss\n", queryDuration)

    // 5. Wrap response in prettifier
    jp := newJSONPrettifier(resp.Body, outputMode)
    return jp
}
```

**Key details**:
- The query is **parsed and re-serialized** via `logstorage.ParseQuery()` → `q.String()`. This validates the query syntax client-side and converts it to canonical form before sending.
- The `VL-Request-Duration-Seconds` response header is used when available, giving the server-side execution time instead of the round-trip time.
- The response is an NDJSON stream (one JSON object per line), which the `jsonPrettifier` consumes incrementally.

**Location**: [`app/vlogscli/main.go:451-621`](../app/vlogscli/main.go#L451)

---

### 4. Output Formatting (JSON Prettifier)

**File**: [`app/vlogscli/json_prettifier.go`](../app/vlogscli/json_prettifier.go#L35)

**Key Functions**:
- [`newJSONPrettifier(r, outputMode)`](../app/vlogscli/json_prettifier.go#L113) - Create streaming formatter
- [`prettifyJSONLines()`](../app/vlogscli/json_prettifier.go#L163) - Background formatting loop
- [`readNextJSONObject(d)`](../app/vlogscli/json_prettifier.go#L221) - Parse one JSON object from stream
- [`getOutputFormatter(outputMode)`](../app/vlogscli/json_prettifier.go#L52) - Select formatter function
- [`writeJSONObject(w, fields, isMultiline)`](../app/vlogscli/json_prettifier.go#L340) - JSON output
- [`writeLogfmtObject(w, fields)`](../app/vlogscli/json_prettifier.go#L281) - Logfmt output
- [`writeCompactObject(w, fields)`](../app/vlogscli/json_prettifier.go#L301) - Compact output

#### Streaming architecture

The prettifier decouples response reading from output display using an `io.Pipe`:

```
HTTP response body (NDJSON)
    ↓
json.Decoder                           ← reads JSON objects one at a time
    ↓
prettifyJSONLines() [background goroutine]
    ↓
formatter(bw, fields)                  ← writes formatted output
    ↓
bufio.Writer → io.PipeWriter           ← buffered write into pipe
    ↓  (io.Pipe)
io.PipeReader                          ← readWithLess() reads from here
    ↓
less / stdout
```

```go
type jsonPrettifier struct {
    r         io.ReadCloser                              // Source: HTTP response body
    formatter func(w io.Writer, fields []logstorage.Field) error  // Selected output formatter
    d         *json.Decoder                              // Incremental JSON decoder
    pr        *io.PipeReader                             // Read end (consumed by less/stdout)
    pw        *io.PipeWriter                             // Write end (fed by formatter)
    bw        *bufio.Writer                              // Buffered writer on pw
    wg        sync.WaitGroup                             // Tracks background goroutine
}
```

The background goroutine reads JSON objects one at a time, sorts fields alphabetically, formats them, and flushes after each object. The flush-per-object ensures results appear immediately rather than being buffered.

#### Output modes

**Multiline JSON** (`\m`, default):
```json
{
  "_msg": "request completed",
  "_time": "2026-02-14T10:30:00Z",
  "level": "info"
}
```

**Singleline JSON** (`\s`):
```json
{"_msg":"request completed","_time":"2026-02-14T10:30:00Z","level":"info"}
```

**Logfmt** (`\logfmt`):
```
_msg="request completed" _time=2026-02-14T10:30:00Z level=info
```

**Compact** (`\c`):
- 1 field: just the value
- 2 fields with `_time`: `timestamp\tvalue`
- Otherwise: falls back to logfmt

#### JSON object parsing

[`readNextJSONObject()`](../app/vlogscli/json_prettifier.go#L221) uses `json.Decoder` token-level parsing to read one JSON object at a time without unmarshaling into a `map`. It only handles string values (which is all VictoriaLogs returns in query results). This is more efficient than full `json.Unmarshal`.

**Location**: [`app/vlogscli/json_prettifier.go:35-409`](../app/vlogscli/json_prettifier.go#L35)

---

### 5. Paging with Less

**File**: [`app/vlogscli/less_wrapper.go`](../app/vlogscli/less_wrapper.go#L65)

**Key Functions**:
- [`readWithLess(r, disableColors, wrapLongLines)`](../app/vlogscli/less_wrapper.go#L65) - Pipe output through `less`
- [`isTerminal()`](../app/vlogscli/less_wrapper.go#L44) - Check if stdout/stderr are TTYs
- [`ignoreSignals(sigs)`](../app/vlogscli/less_wrapper.go#L194) - Suppress signals in parent process

#### Terminal vs non-terminal

```go
func readWithLess(r io.Reader, disableColors, wrapLongLines bool) error {
    if !isTerminal() {
        // Non-terminal: write directly to stdout (for piping)
        io.Copy(os.Stdout, r)
        os.Stdout.Sync()
        return nil
    }
    // Terminal: pipe through less
    // ...
}
```

#### Less configuration

| Flag | Meaning | When used |
|------|---------|-----------|
| `-F` | Quit if output fits on one screen | Always |
| `-X` | Don't clear screen on exit | Always |
| `-R` | Preserve ANSI color codes | When colors are enabled |
| `-S` | Chop (don't wrap) long lines | When line wrapping is disabled |

The `LESSCHARSET=utf-8` environment variable is set to ensure proper Unicode display.

#### Signal handling during paging

When `less` is running, Ctrl+C must be handled by `less` (not vlogscli). The parent process ignores `SIGINT` while `less` is active:

```go
// Ignore Ctrl+C in parent process so less handles it
cancel := ignoreSignals(os.Interrupt)
defer cancel()

// Start less process with stdin connected to pipe
p, _ := os.StartProcess(path, opts, &os.ProcAttr{
    Env:   append(os.Environ(), "LESSCHARSET=utf-8"),
    Files: []*os.File{pr, os.Stdout, os.Stderr},
})
```

A background goroutine waits for `less` to exit and closes the pipe reader, which unblocks the `io.Copy` that feeds data to `less`.

**Location**: [`app/vlogscli/less_wrapper.go:44-216`](../app/vlogscli/less_wrapper.go#L44)

---

### 6. Live Tailing

**File**: [`app/vlogscli/main.go`](../app/vlogscli/main.go#L482)

**Key Functions**:
- [`tailQuery(ctx, output, qStr, outputMode)`](../app/vlogscli/main.go#L482) - Execute live tail query
- [`getTailURL()`](../app/vlogscli/main.go#L514) - Determine tail endpoint URL

Live tailing uses VictoriaLogs' `/select/logsql/tail` endpoint, which streams new log entries as they arrive.

```go
func tailQuery(ctx context.Context, output io.Writer, qStr string, outputMode outputMode) {
    qStr = strings.TrimPrefix(qStr, `\tail `)
    qURL, _ := getTailURL()

    // Use the same HTTP request path as regular queries
    respBody := getQueryResponse(ctx, output, qStr, outputMode, qURL)

    // Stream directly to output — no less paging
    io.Copy(output, respBody)
}
```

**Key differences from regular queries:**
- **No paging**: Output goes directly to the terminal (not through `less`), since tailing is indefinite.
- **Auto URL detection**: If `-tail.url` is not set, replaces `/query` with `/tail` in the datasource URL.
- **Cancellation**: User presses Ctrl+C to stop tailing (via context cancellation).

**Location**: [`app/vlogscli/main.go:482-529`](../app/vlogscli/main.go#L482)

---

### 7. Connection & Authentication

**File**: [`app/vlogscli/main.go`](../app/vlogscli/main.go#L625)

**Key Functions**:
- [`newHTTPClient()`](../app/vlogscli/main.go#L625) - Create HTTP client with auth transport
- [`newAuthConfig()`](../app/vlogscli/main.go#L639) - Build auth config from flags

vlogscli uses the same `promauth` library as vlagent and the VictoriaLogs server for authentication.

```go
func newHTTPClient() (*promauth.Config, *http.Client) {
    ac := newAuthConfig()
    tr := httputil.NewTransport(true, "vlogscli")
    c := &http.Client{
        Transport: ac.NewRoundTripper(tr),
    }
    return ac, c
}
```

Supported authentication methods:

| Method | Flags |
|--------|-------|
| Basic auth | `-username`, `-password` |
| Bearer token | `-bearerToken` |
| TLS client certs | `-tlsCertFile`, `-tlsKeyFile` |
| Custom CA | `-tlsCAFile` |
| Skip TLS verify | `-tlsInsecureSkipVerify` |
| Custom headers | `-header "Name: value"` |

Multi-tenancy is handled via `AccountID` and `ProjectID` HTTP headers, set from the `-accountID` and `-projectID` flags.

**Location**: [`app/vlogscli/main.go:625-702`](../app/vlogscli/main.go#L625)

---

## Key Design Patterns

### 1. Streaming Pipeline (no full buffering)

The response is never fully buffered in memory. Data flows through a three-stage pipeline:

```
HTTP response → jsonPrettifier (goroutine) → io.Pipe → less/stdout
```

Each JSON object is formatted and flushed immediately, so users see results as they arrive from the server. This is critical for large result sets and live tailing.

### 2. io.Pipe for Goroutine Decoupling

The `jsonPrettifier` uses `io.Pipe` to decouple the background formatting goroutine from the downstream consumer (`less` or `stdout`). The pipe provides natural backpressure — if `less` stops reading (user is viewing a page), the pipe write blocks, which pauses the formatter, which pauses the HTTP response read.

### 3. Client-Side Query Validation

Queries are parsed via `logstorage.ParseQuery()` before being sent to the server. This catches syntax errors locally and converts the query to canonical form, giving the user immediate feedback without a round-trip.

### 4. Signal Context Layering

Signal handling is context-aware:
- **Query execution**: `signal.NotifyContext()` creates a context cancelled on Ctrl+C
- **Less paging**: Parent process ignores SIGINT so `less` handles it
- **Tail streaming**: Context cancellation stops the HTTP request and stream

### 5. Non-Interactive Mode

The REPL handles `io.EOF` from readline (piped input) by executing the accumulated query. This enables scripting:

```bash
echo "_time:1h | stats count();" | vlogscli -datasource.url=http://localhost:9428/select/logsql/query
```

When not a terminal, `readWithLess()` writes directly to stdout without `less`.

---

## Configuration Flags

### Connection

```bash
-datasource.url string
    VictoriaLogs query URL (default: http://localhost:9428/select/logsql/query)

-tail.url string
    VictoriaLogs tail URL (default: auto-detected by replacing /query with /tail)

-header array
    Custom HTTP headers in format "HeaderName: value"
```

### Multi-Tenancy

```bash
-accountID int
    Account ID for tenant-scoped queries (default: 0)

-projectID int
    Project ID for tenant-scoped queries (default: 0)
```

### Authentication

```bash
-username string
    Basic auth username

-password string
    Basic auth password (secure flag, hidden from metrics)

-bearerToken string
    Bearer token for authentication (secure flag)
```

### TLS

```bash
-tlsCAFile string
    Path to TLS CA file for verifying server connections

-tlsCertFile string
    Path to client-side TLS certificate

-tlsKeyFile string
    Path to client-side TLS certificate key

-tlsServerName string
    Override TLS server name (default: from -datasource.url)

-tlsInsecureSkipVerify bool
    Skip TLS certificate verification (default: false)
```

### History

```bash
-historyFile string
    Path to query history file (default: vlogscli-history)
```

---

## See Also

- [VictoriaLogs Query/Select Flow](./onboarding-select-flow.md)
- [VictoriaLogs LogsQL Parser & Pipe Execution](./onboarding-logsql-parser-pipes.md)
- [VictoriaLogs vmui Frontend Internals](./onboarding-vmui.md)
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
- Use consistent relative paths from this document location (this file currently uses `../` to reach repo root)
- Include line numbers wherever possible

#### Standard Patterns

**1. Section Headers - File References**

```markdown
**File**: [`app/vlogscli/main.go`](../app/vlogscli/main.go#L101)
```

**2. Function References in Key Functions Lists**

```markdown
**Key Functions**:
- [`runReadlineLoop(rl, incompleteLine)`](../app/vlogscli/main.go#L176) - Main REPL loop
```

**3. Flow Diagram References**

```markdown
runReadlineLoop()                        [main.go:176](../app/vlogscli/main.go#L176)
```

**4. Location References**

```markdown
**Location**: [`app/vlogscli/main.go:176-427`](../app/vlogscli/main.go#L176)
```

#### Line Number Selection

When linking to code:

- **Single function**: Link to the function definition line
- **Struct definition**: Link to the struct type declaration
- **Interface**: Link to the interface declaration
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
# Check for links missing the #L prefix
grep -n '\](.*\.go#[0-9]' onboarding-vlogscli.md

# Count total clickable line references
grep -o "#L[0-9]\+" onboarding-vlogscli.md | wc -l

# Find non-clickable file references
grep -n '`app/.*\.go`' onboarding-vlogscli.md | grep -v '\]('
```

#### Why This Matters

Clickable file references significantly improve the onboarding experience by:
- Reducing friction when exploring the codebase
- Enabling instant navigation from concept to implementation
- Making the documentation a living, interactive guide
- Helping developers quickly verify documented behavior against actual code

**Remember**: Every file reference is an opportunity to help a developer learn faster. Make them all clickable!
