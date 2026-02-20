# Level 10 - Lifecycle Ops, Retention/Delete Semantics, and Capstone

## Objective
Connect algorithmic internals to operational behavior, then produce a full architecture-level synthesis.

## Outcomes
By the end of this level, you can:
- explain snapshot/retention/delete behavior using underlying data structures
- explain why some operations rely on merge for physical effects
- produce a defendable engineering improvement proposal

## Source Anchors
- `lib/logstorage/partition.go`
  - `mustCreateSnapshot`
- `lib/logstorage/datadb.go`
  - `mustCreateSnapshotAt`
  - `deleteRows`
  - `mustForceMergeAllParts`
- `lib/logstorage/storage.go`
  - retention/disk watchers
  - partition detach/attach lifecycle
- `onboarding/onboarding-partition-lifecycle.md`

## Core Concepts
1. Snapshot via hard links for point-in-time consistency.
2. Time/disk retention via partition lifecycle operations.
3. Logical delete vs physical reclamation through merge.
4. Safety and correctness from ref counting and atomicity.

## Guided Reading Tasks
1. Trace snapshot creation path and identify where in-memory data is forced to file-backed parts.
2. Trace delete task effect chain:
   - match rows
   - merge with drop filter
   - eventual disk reclaim
3. Trace retention deletion path and explain `deletedPartitions` guard implications.

## Hands-On Lab
### Lab 1: Snapshot Consistency Story
Write a precise explanation of why snapshot+hard-links remain consistent while background merges continue.

### Lab 2: Delete-Reclaim Experiment Design
Design an experiment that demonstrates:
- rows disappear from query results before space is reclaimed
- force merge changes on-disk space usage

### Lab 3: Retention Incident Drill
Simulate an ops scenario where disk pressure triggers old partition removal.
Document expected behavior and safeguards.

## Capstone
Produce `level10/capstone.md` covering:
1. End-to-end write path (row -> block -> part -> merge)
2. End-to-end query path (query -> prune -> block scan -> rows)
3. Bloom filter role and false-positive implications
4. Merge heuristic tradeoff analysis
5. One optimization proposal with:
   - expected benefit
   - explicit risks
   - validation plan

## Final Review Rubric
- Technical correctness: concepts map accurately to code.
- Completeness: both write and query paths are covered.
- Tradeoff quality: proposal discusses cost/benefit/risk.
- Verifiability: contains measurable validation plan.

## Deliverables
- `level10/snapshot-consistency.md`
- `level10/delete-reclaim-design.md`
- `level10/retention-incident-drill.md`
- `level10/capstone.md`
- `level10/final-checkpoint.md`

## Pass Criteria
- You can defend your system model under detailed engineering questioning.
- You can propose changes responsibly with explicit operational tradeoffs.
