# Level 3 - Blocks, Columns, and Type-Aware Encoding

## Objective
Understand how VictoriaLogs transforms rows into compact columnar blocks.

## Outcomes
By the end of this level, you can:
- describe `block` layout and serialization flow
- explain const-column optimization
- explain how value type selection impacts query path

## Source Anchors
- `lib/logstorage/block.go`
  - `MustInitFromRows`
  - `canStoreInConstColumn`
  - `mustWriteTo`
- `lib/logstorage/block_header.go`
  - `columnHeader`
  - values/bloom offsets and sizes
- `lib/logstorage/values_encoder.go`
  - `valueType` constants
  - `valuesEncoder.encode`
  - dict encoding behavior
- `lib/logstorage/consts.go`
  - `maxConstColumnValueSize`
  - block size constraints

## Core Concepts
1. Row-to-column transformation per block.
2. Const-column optimization to avoid repeated storage.
3. Type-aware encoding (`dict`, numeric, string, timestamp, etc.).
4. Column metadata as random-access pointers (offset/size) into values/bloom streams.

## Guided Reading Tasks
1. In `block.MustInitFromRows`, identify fast path vs slow path and when each is taken.
2. In `column.mustWriteTo`, map the sequence:
   - encode values
   - write values block
   - write bloom block
   - persist offsets/sizes in `columnHeader`
3. In `valuesEncoder.encode`, document fallback order:
   - dict -> uint -> int -> float -> ipv4 -> timestamp -> string
4. Explain why `valueTypeDict` can skip bloom storage.

## Hands-On Lab
### Lab 1: Block Decomposition
Given a small synthetic dataset, manually derive:
- const columns
- non-const columns
- probable value types
- expected query fast paths

### Lab 2: Encoding Impact Analysis
Compare two scenarios:
- low-cardinality column (dict likely)
- high-cardinality free-text column (string likely)

For each, explain expected impact on:
- storage size
- bloom usefulness
- filtering speed

## Checkpoint Questions
1. Why are block-level offsets important for query performance?
2. What makes dict columns special in VictoriaLogs filtering?
3. Why is column sorting in block initialization useful?

## Deliverables
- `level3/block-decomposition.md`
- `level3/encoding-analysis.md`
- `level3/checkpoint.md`

## Pass Criteria
- You can explain how one row becomes bytes in part files.
- You can reason about encoding choice effects without guesswork.
