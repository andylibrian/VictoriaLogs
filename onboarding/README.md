# VictoriaLogs Onboarding Docs

This folder contains in-repo onboarding guides for engineers who need to understand and change VictoriaLogs internals quickly.

Use this file as the entry point and reading plan.

If you encounter unfamiliar terms (LSM-tree, bloom filter, mergeset, etc.), see the [Glossary](./glossary.md).

## Official Product Docs (`docs/`)

Before diving into internals, treat `../docs/victorialogs/` as the official VictoriaLogs documentation source with valuable product behavior details.
This is the source for published docs at docs.victoriametrics.com and should be the primary reference for user-facing APIs, flags, and examples.

How `docs/` is organized:

- `../docs/victorialogs/_index.md` renders `../docs/victorialogs/README.md` (top-level docs landing content).
- `../docs/victorialogs/data-ingestion/` contains ingestion docs (`_index.md`, `README.md`, and collector-specific pages).
- `../docs/victorialogs/querying/` contains querying docs (`_index.md`, `README.md`, `vlogscli` docs, query API behavior).
- `../docs/victorialogs/integrations/` contains integration guides.
- `../docs/Makefile` contains docs workflows (preview/debug image, version marker updates, flags regeneration).

Relationship to onboarding docs:

- `onboarding/` explains implementation internals and code navigation for contributors.
- `docs/victorialogs/` documents product-facing behavior and operational guidance.
- When changing behavior, update both when needed: official docs for users, onboarding docs for contributors.

## Recommended Order

1. [`onboarding-system-overview.md`](./onboarding-system-overview.md)
2. [`onboarding-cluster.md`](./onboarding-cluster.md)
3. [`onboarding-insert-flow.md`](./onboarding-insert-flow.md)
4. [`onboarding-select-flow.md`](./onboarding-select-flow.md)
5. [`onboarding-logsql-parser-pipes.md`](./onboarding-logsql-parser-pipes.md)
6. [`onboarding-storage-engine.md`](./onboarding-storage-engine.md)
7. [`onboarding-partition-lifecycle.md`](./onboarding-partition-lifecycle.md)
8. [`onboarding-vlagent.md`](./onboarding-vlagent.md)
9. [`onboarding-vlogscli.md`](./onboarding-vlogscli.md)
10. [`onboarding-vmui.md`](./onboarding-vmui.md)

Why this order:

- Start with topology and ownership boundaries.
- Then map cluster roles, internal APIs, protocol/version contracts, and endpoint flags.
- Then learn write path and read path.
- Then drill into LogsQL parser and pipe runtime mechanics.
- Then go deep on storage internals.
- Finish with operational lifecycle tasks (attach/detach/snapshot/retention/delete tasks).
- Then learn the log collection agent (vlagent) and how it ships logs to VictoriaLogs.
- Then learn the interactive CLI (vlogscli) and how it queries VictoriaLogs.
- Then learn the web UI (vmui) and how it calls VictoriaLogs query endpoints.

## Role-Based Shortcuts

If you mostly work on:

- Ingestion protocols/parsing:
  1. [`onboarding-system-overview.md`](./onboarding-system-overview.md)
  2. [`onboarding-cluster.md`](./onboarding-cluster.md)
  3. [`onboarding-insert-flow.md`](./onboarding-insert-flow.md)
  4. [`app/vlinsert/main.go`](../app/vlinsert/main.go#L38)
  5. [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L50)
- Query API/behavior:
  1. [`onboarding-system-overview.md`](./onboarding-system-overview.md)
  2. [`onboarding-cluster.md`](./onboarding-cluster.md)
  3. [`onboarding-select-flow.md`](./onboarding-select-flow.md)
  4. [`onboarding-logsql-parser-pipes.md`](./onboarding-logsql-parser-pipes.md)
  5. [`app/vlselect/main.go`](../app/vlselect/main.go#L90)
  6. [`app/vlselect/logsql/logsql.go`](../app/vlselect/logsql/logsql.go#L1149)
- Storage and retention:
  1. [`onboarding-system-overview.md`](./onboarding-system-overview.md)
  2. [`onboarding-cluster.md`](./onboarding-cluster.md)
  3. [`onboarding-storage-engine.md`](./onboarding-storage-engine.md)
  4. [`onboarding-partition-lifecycle.md`](./onboarding-partition-lifecycle.md)
  5. [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L115)
- vlagent / log collection:
  1. [`onboarding-system-overview.md`](./onboarding-system-overview.md)
  2. [`onboarding-insert-flow.md`](./onboarding-insert-flow.md)
  3. [`onboarding-vlagent.md`](./onboarding-vlagent.md)
  4. [`app/vlagent/main.go`](../app/vlagent/main.go#L34)
  5. [`app/vlagent/kubernetescollector/collector.go`](../app/vlagent/kubernetescollector/collector.go#L43)
- vlogscli / query CLI:
  1. [`onboarding-system-overview.md`](./onboarding-system-overview.md)
  2. [`onboarding-select-flow.md`](./onboarding-select-flow.md)
  3. [`onboarding-vlogscli.md`](./onboarding-vlogscli.md)
  4. [`app/vlogscli/main.go`](../app/vlogscli/main.go#L58)
- vmui / web UI:
  1. [`onboarding-system-overview.md`](./onboarding-system-overview.md)
  2. [`onboarding-select-flow.md`](./onboarding-select-flow.md)
  3. [`onboarding-vmui.md`](./onboarding-vmui.md)
  4. [`app/vmui/packages/vmui/src/App.tsx`](../app/vmui/packages/vmui/src/App.tsx#L13)
  5. [`app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx`](../app/vmui/packages/vmui/src/pages/QueryPage/QueryPage.tsx#L34)

## Local Dev Basics

Common commands and where they are defined:

- Build local binaries:
  - [`make victoria-logs`](../app/victoria-logs/Makefile#L3)
  - [`make vlagent`](../app/vlagent/Makefile#L3)
  - [`make vlogscli`](../app/vlogscli/Makefile#L3)
- Run tests:
  - [`make test`](../Makefile#L290)
  - [`make test-race`](../Makefile#L293)
  - [`make apptest`](../Makefile#L308)
- Integration test harness overview:
  - [`apptest/README.md`](../apptest/README.md)

## What To Learn Next After These Docs

After finishing the onboarding docs, new engineers should focus on the following areas in this order.

1. LogsQL parser and execution pipeline internals
   Start with [`onboarding-logsql-parser-pipes.md`](./onboarding-logsql-parser-pipes.md), then dive into [`lib/logstorage/parser.go`](../lib/logstorage/parser.go#L1671) and [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L216).
2. Cluster protocol and remote fanout details
   Read [`onboarding-cluster.md`](./onboarding-cluster.md) first, then [`app/vlstorage/netinsert/netinsert.go`](../app/vlstorage/netinsert/netinsert.go#L30), [`app/vlstorage/netselect/netselect.go`](../app/vlstorage/netselect/netselect.go#L29), and [`app/vlselect/internalselect/internalselect.go`](../app/vlselect/internalselect/internalselect.go#L79).
3. Multi-tenancy and request scoping
   Study [`lib/logstorage/tenant_id.go`](../lib/logstorage/tenant_id.go#L14), [`app/vlinsert/insertutil/common_params.go`](../app/vlinsert/insertutil/common_params.go#L30), and query context wiring in [`lib/logstorage/storage_search.go`](../lib/logstorage/storage_search.go#L25).
4. Operational behavior and production guardrails
   Go through flags and runtime checks in [`app/vlstorage/main.go`](../app/vlstorage/main.go#L27) and [`app/vlselect/main.go`](../app/vlselect/main.go#L25), then cross-check with docs in [`docs/victorialogs/victoria_logs_common_flags.md`](../docs/victorialogs/victoria_logs_common_flags.md) and [`docs/victorialogs/cluster.md`](../docs/victorialogs/cluster.md).
5. Testing strategy and performance work
   Learn integration testing in [`apptest/`](../apptest/README.md), unit tests in `lib/logstorage/*_test.go`, and perf tooling from [`make benchmark`](../Makefile#L312) / [`make benchmark-pure`](../Makefile#L316).
6. Adjacent binaries and ecosystem flow
   Understand collector/client edges via [`app/vlagent/main.go`](../app/vlagent/main.go#L34), [`app/vlogscli/main.go`](../app/vlogscli/main.go#L58), and how they connect to `/insert/*` and `/select/*`.

## First-Week Learning Checklist

1. Draw the single-node request path from memory (`/insert/native` and `/select/logsql/query`).
2. Explain when `vlstorage` runs local mode vs network mode.
3. Find where query concurrency is limited and where timeout errors are returned.
4. Run `make test` and `make apptest` once locally.
5. Pick one existing test in `apptest/tests/` and trace the production code it exercises.
6. Explain endpoint control behavior for `-insert.disable`, `-select.disable`, and `-internaldelete.enable` from code.
