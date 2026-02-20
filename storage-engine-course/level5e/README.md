# Level 5E - Compaction Engine and Policy Control

## Objective
Implement compaction picker/executor and analyze policy tradeoffs deeply.

## Outcomes
By the end of this phase, you can:
- implement compaction planning and execution
- support at least one tiered and one leveled policy
- quantify amplification and latency effects per policy

## Core Concepts
1. Candidate selection constraints.
2. Output run generation and atomic install.
3. Overlap management (leveled) vs fan-in grouping (tiered).
4. Compaction debt and scheduling fairness.

## VictoriaLogs Anchors
- `lib/logstorage/datadb.go`: `appendPartsToMerge`, `mustMergePartsInternal`
- `lib/logstorage/block_stream_merger.go`

## Labs
### Lab 1: Picker Design
Implement compaction picker with deterministic tie-breaking.

### Lab 2: Tiered vs Leveled
Replay same workload under both policies and compare:
- write amp
- read amp
- space amp

### Lab 3: Stall Control
Add simple throttling or write stalls when compaction debt is high.

## Deliverables
- `level5e/compaction-picker.md`
- `level5e/policy-comparison.md`
- `level5e/stall-control.md`
- `level5e/checkpoint.md`

## Pass Criteria
- Compaction is correct under overlap constraints and policy outcomes are quantified.
