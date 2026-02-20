# Level 5C - SSTable Format and Metadata Indexes

## Objective
Design and implement immutable table files with metadata for efficient read pruning.

## Outcomes
By the end of this phase, you can:
- define SSTable physical format
- implement table builder and iterators
- add per-table metadata needed for fast reads

## Core Concepts
1. Data block layout and restart points (or equivalent index aid).
2. Table-level metadata:
   - min/max key
   - sequence bounds
   - block index
3. Optional fence pointers and sparse index.
4. Checksums and corruption detection.

## VictoriaLogs Anchors
- `lib/logstorage/block_stream_writer.go`
- `lib/logstorage/index_block_header.go`
- `lib/logstorage/part_header.go`
- `lib/logstorage/part.go`

## Labs
### Lab 1: SSTable Spec
Write complete binary format spec and compatibility rules.

### Lab 2: Builder + Reader
Implement table writer and point/range reader with tests.

### Lab 3: Corruption Tests
Inject truncation/bit flips and verify failures are detected.

## Deliverables
- `level5c/sstable-spec.md`
- `level5c/builder-reader-tests.md`
- `level5c/corruption-tests.md`
- `level5c/checkpoint.md`

## Pass Criteria
- Reads are correct and corruption detection is reliable.
