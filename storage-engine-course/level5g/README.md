# Level 5G - Concurrency, Backpressure, and Observability

## Objective
Harden the engine under concurrent load and operational stress.

## Outcomes
By the end of this phase, you can:
- define lock/channel ownership model
- prevent unsafe lifecycle races
- expose actionable metrics for tuning and incidents

## Core Concepts
1. Reader/writer/compactor concurrency boundaries.
2. Reference counting or epoch-based resource safety.
3. Backpressure/stall thresholds.
4. Metrics and diagnostics:
   - pending writes
   - compaction debt
   - read path probes

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: locks, ref-count wrappers, merge workers
- `lib/logstorage/storage_search.go`: parallel search workers
- `lib/logstorage/storage.go`: background watchers

## Labs
### Lab 1: Concurrency Model Doc
Produce a lock-order and ownership table.

### Lab 2: Race/Deadlock Test
Add stress tests with parallel writes/reads/compactions and failure injections.

### Lab 3: Metrics Dashboard
Define and emit core metrics; add alerting thresholds.

## Deliverables
- `level5g/concurrency-model.md`
- `level5g/stress-results.md`
- `level5g/metrics-spec.md`
- `level5g/checkpoint.md`

## Pass Criteria
- No known data races/deadlocks in stress tests and metrics support diagnosis.
