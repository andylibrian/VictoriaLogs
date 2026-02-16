# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

VictoriaLogs is an open-source, resource-efficient database for logs by VictoriaMetrics. It supports single-node and cluster architectures, with data ingestion via Elasticsearch, Loki, OpenTelemetry, Datadog, Journald, Syslog, and JSON lines formats. Query language is LogsQL.

Go module: `github.com/VictoriaMetrics/VictoriaLogs`

## Official Documentation Source (`docs/`)

`docs/victorialogs/` is the official product documentation source for VictoriaLogs and contains valuable user-facing information
(features, APIs, flags, examples, operations, and integrations). This content is published to docs.victoriametrics.com.

Structure overview:

- `docs/victorialogs/_index.md` is the section entry page and renders `docs/victorialogs/README.md`.
- `docs/victorialogs/data-ingestion/` contains ingestion docs (`_index.md` + `README.md` + collector-specific pages).
- `docs/victorialogs/querying/` contains querying docs (`_index.md` + `README.md` + `vlogscli` docs).
- `docs/victorialogs/integrations/` contains integration guides (e.g. Grafana, Perses).
- Generated flags references live in:
  - `docs/victorialogs/victoria_logs_common_flags.md`
  - `docs/victorialogs/victoria_logs_enterprise_flags.md`
  - `docs/victorialogs/vlagent_common_flags.md`
  - `docs/victorialogs/vlagent_enterprise_flags.md`
  - `docs/victorialogs/querying/vlogscli_common_flags.md`

Docs tooling:

- `docs/Makefile` contains targets for docs image/debug workflow and docs updates (`docs-debug`, `docs-update-version`, `docs-update-flags`, etc.).
- Root `Makefile` includes `docs/Makefile`, so docs targets are available from repo root.

Important distinction:

- `docs/victorialogs/` is official external product documentation.
- `onboarding/` is internal engineering guidance for contributors and should complement, not replace, official docs.

## Build Commands

```bash
make victoria-logs          # Build main server binary (CGO enabled)
make victoria-logs-pure     # Build without CGO
make vlagent                # Build log collection agent
make vlogscli               # Build CLI query tool
make vlogsgenerator         # Build log generator

make all                    # Build all production binaries via Docker
make clean                  # Remove bin/ directory
```

## Testing

```bash
make test                   # Unit tests: GOEXPERIMENT=synctest go test ./lib/... ./app/...
make test-race              # Race detector tests
make test-pure              # Tests without CGO
make test-full              # Tests with coverage profile

make apptest                # Integration tests (builds binaries first, then runs go test ./apptest/...)
```

Run a single test:
```bash
GOEXPERIMENT=synctest go test ./lib/logstorage/ -run TestFunctionName
go test ./app/vlinsert/ -run TestFunctionName
```

Note: `lib/` tests require `GOEXPERIMENT=synctest`. `app/` tests do not.

## Linting

```bash
make check-all              # fmt + vet + golangci-lint + govulncheck
make fmt                    # gofmt -l -w -s
make vet                    # go vet (lib/ uses GOEXPERIMENT=synctest)
make golangci-lint          # golangci-lint 2.7.2
```

## Architecture

The main server (`app/victoria-logs/main.go`) composes three subsystems:

- **vlinsert** (`app/vlinsert/`) — Data ingestion. Each subdirectory handles a different protocol (jsonline, elasticsearch, loki, opentelemetry, datadog, journald, syslog, native). Uses `insertutil` for common insertion logic.
- **vlselect** (`app/vlselect/`) — Query execution. Handles `/select/*` and `/delete/*` HTTP endpoints. Contains `logsql` query engine and `vmui` web UI serving.
- **vlstorage** (`app/vlstorage/`) — Storage backend. Manages on-disk storage, distributed queries, and network protocols for cluster mode (`netinsert`/`netselect`).

Request routing: `victoria-logs/main.go:requestHandler` chains `vlinsert.RequestHandler` → `vlselect.RequestHandler` → `vlstorage.RequestHandler`.

### Core Library

**`lib/logstorage/`** is the largest package (~320 files). It contains:
- Storage engine: block management, encoding/compression, bloom filters, arena allocation
- LogsQL implementation: `filter_*.go` (40+ filter types), `pipe_*.go` (query pipes), `stats_*.go` (aggregation functions)
- Parsers: logfmt, tokenizer, pattern matching

### Other Applications

- **vlagent** (`app/vlagent/`) — Log collection agent with Kubernetes collector. Default port 9429.
- **vlogscli** (`app/vlogscli/`) — Interactive CLI for querying VictoriaLogs.
- **vlogsgenerator** (`app/vlogsgenerator/`) — Synthetic log generator for testing/benchmarks.
- **vmui** (`app/vmui/`) — Web UI frontend (React/Node.js).

### Integration Tests

`apptest/` contains integration tests that start application binaries in separate processes. `make apptest` builds binaries first, then runs `go test ./apptest/...`. Test files are in `apptest/tests/*_test.go`. See `apptest/README.md` for details.

## Key Conventions

- Dependencies are vendored (`vendor/` directory). Update with `make vendor-update`.
- QuickTemplate (`.qtpl` files) generates Go code. Regenerate with `make quicktemplate-gen`.
- Default listen port: 9428 (victoria-logs), 9429 (vlagent).
- Heavily depends on `github.com/VictoriaMetrics/VictoriaMetrics` for shared infrastructure (httpserver, logger, buildinfo, flagutil, etc.).
