# Level 6 - VictoriaLogs LSM Internals In `datadb`

## Prerequisite
- Complete `level5h` checkpoint first.

## Objective
Map generic LSM ideas to exact VictoriaLogs code paths and invariants.

## Outcomes
By the end of this level, you can:
- explain the three-tier part model
- explain how merge destination type is selected
- explain part lifecycle and atomic swap behavior

## Source Anchors
- `lib/logstorage/datadb.go`
  - `datadb` struct
  - `partWrapper`
  - `mustFlushInmemoryPartsToFiles`
  - `mustMergePartsInternal`
  - `getDstPartType`
  - `swapSrcWithDstParts`
- `lib/logstorage/part.go`
  - part open/close

## Core Concepts
1. Part tiers:
   - `partInmemory`
   - `partSmall`
   - `partBig`
2. Durability and flush deadlines.
3. Reference counting for safe concurrent reads/writes/merges.
4. Atomic source->destination part swap to avoid inconsistent visible state.

## Guided Reading Tasks
1. Explain all fields in `partWrapper` and their lifecycle role.
2. In `mustMergePartsInternal`, map full control flow:
   - destination type
   - disk reservation
   - stream merge
   - destination materialization
   - source replacement
3. In `getDstPartType`, explain each branch with operational rationale.
4. In `swapSrcWithDstParts`, explain why `parts.json` update must be under lock.

## Hands-On Lab
### Lab 1: Merge Trace
Create a detailed trace of one merge cycle with state snapshots:
- pre-merge part lists
- in-merge flags
- post-swap part lists
- old part cleanup path

### Lab 2: Failure Scenario Reasoning
Explain behavior if merge is interrupted before swap vs after swap.

## Checkpoint Questions
1. Why is immutable-part + atomic-swap a robust design?
2. Why keep `inmemory`, `small`, and `big` separate?
3. Why can non-final merge be skipped when disk reservation fails?

## Deliverables
- `level6/merge-trace.md`
- `level6/failure-scenarios.md`
- `level6/checkpoint.md`

## Pass Criteria
- You can reason about correctness and durability under concurrent load.
- You can explain exact responsibilities of locks, refs, and swap logic.
