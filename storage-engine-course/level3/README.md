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

## Why Blocks? (Conceptual Background)

### Blocks enable columnar storage

You can only rotate rows into columns if you have a fixed set of rows to work with. You cannot know that `host` has the same value across all entries until you have collected those entries together. A block is that collection.

Consider 5 log entries:

```
{ time: 1, host: "web-01", level: "error", msg: "timeout" }
{ time: 2, host: "web-01", level: "info",  msg: "started" }
{ time: 3, host: "web-01", level: "error", msg: "timeout" }
{ time: 4, host: "web-01", level: "info",  msg: "ok"      }
{ time: 5, host: "web-01", level: "error", msg: "timeout" }
```

Row storage keeps each entry together as written. Columnar storage groups each field together across all entries:

```
time:  [1, 2, 3, 4, 5]
host:  [web-01, web-01, web-01, web-01, web-01]
level: [error, info, error, info, error]
msg:   [timeout, started, timeout, ok, timeout]
```

Once you have this layout within a block:

- **Compression improves**: values of the same field are adjacent, making patterns obvious to a compressor. `level` becomes just `[error, info, error, info, error]`.
- **Const column detection works**: because all values of `host` are now together, VictoriaLogs can detect "every value is web-01" and store it once in the block header instead of once per row.
- **Dictionary encoding works**: the `level` column has only 2 unique values. VictoriaLogs assigns `{0: "error", 1: "info"}` and stores each row as a single byte. This requires knowing all values upfront — which the block boundary provides.
- **Selective reads become possible**: a query asking for `level:error` reads only the `level` column's bytes from disk, skipping `host`, `msg`, and `time` entirely.

### Blocks reduce index size

To answer a query like "find logs between 10:00 and 10:05", the database must know where each entry is on disk without scanning the entire file. It uses an **index** — a smaller structure that maps "what's in here" to "where on disk to find it".

If every individual row had an index entry, the index would be enormous. At 100M logs/day with 16 bytes of metadata each, that is 1.6GB of index — too large to hold in memory.

Blocks solve this by indexing at the block level. One `blockHeader` covers thousands to millions of rows and stores:

```
minTimestamp, maxTimestamp   ← time range of the whole block
streamID                     ← which stream this block belongs to
offset into timestamps.bin   ← where to find the data on disk
offset into columns_header   ← where to find column metadata
```

A query for "10:00 to 10:05" checks each block header: does `[minTimestamp, maxTimestamp]` overlap my range? If not, the entire block is skipped with a single comparison — potentially skipping millions of rows at once.

VictoriaLogs adds a second index level to handle large parts:

```
metaindex.bin  →  index.bin  →  actual data files
```

`metaindex.bin` is loaded into memory when a part opens. Each entry covers a group of block headers with a combined time range. A query first scans the metaindex in memory to eliminate entire hour-sized ranges, then reads only the relevant sections of `index.bin` for individual block headers. For 1 billion rows:

| Level | Entries | Approximate size |
|-------|---------|-----------------|
| Rows | 1,000,000,000 | (the actual data) |
| Block headers (`index.bin`) | ~100,000 | ~5MB |
| Metaindex entries (`metaindex.bin`) | ~400 | ~20KB |

The metaindex fits in tens of kilobytes. One comparison can skip 2.5 million rows. This is the payoff of the block boundary.

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
