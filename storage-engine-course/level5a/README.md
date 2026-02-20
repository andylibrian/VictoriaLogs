# Level 5A - LSM Mental Model, Invariants, and Simulator

## Objective
Establish rigorous LSM fundamentals and build a simulator that predicts behavior before writing storage code.

## Outcomes
By the end of this phase, you can:
- define LSM invariants precisely
- quantify write amplification/read amplification trends
- explain why/when compaction should happen

## Core Concepts
1. Immutable run model.
2. Memtable -> immutable run flush transition.
3. Cost dimensions:
   - write amplification
   - read amplification
   - space amplification
4. Policy knobs:
   - flush size
   - parts-to-merge
   - compaction trigger thresholds

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: constants and merge constraints
- `lib/logstorage/datadb.go`: `appendPartsToMerge`

## Labs
### Lab 1: Event-Driven LSM Simulator
Simulate operations:
- `PUT`, `FLUSH`, `COMPACT`

Track per step:
- run count
- bytes written due to compaction
- estimated point-read probes

### Lab 2: Policy Sweep
Run multiple policies and compare:
- eager merge
- multiplier-threshold merge
- bounded fan-in merge

### Lab 3: Invariant Spec
Write machine-checkable invariants for simulator state.

## Deliverables
- `level5a/invariants.md`
- `level5a/simulator-v1.md`
- `level5a/policy-sweep.md`
- `level5a/checkpoint.md`

## Pass Criteria
- You can predict LSM behavior under policy changes before coding storage files.
