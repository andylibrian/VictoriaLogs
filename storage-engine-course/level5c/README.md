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

## Parts as SSTables (Conceptual Background)

### What is an SSTable?

An SSTable (Sorted String Table) is an immutable, on-disk file containing key-value pairs in sorted order. Once written, it is never modified — only replaced during compaction. The classic SSTable has three pieces: a data section containing sorted entries, an index mapping keys to offsets within the data, and metadata summarizing the file's contents (key range, entry count, etc.).

VictoriaLogs' **part** is its SSTable equivalent. A part is a directory of specialized files rather than a single monolithic file. This multi-file design separates concerns — timestamps, column values, bloom filters, and indexes each live in their own file, enabling the system to read only the bytes it needs.

### Anatomy of a part

A part directory contains these files:

```
part_20240315_093000.000_20240315_094500.000_ABC123/
  metadata.json              ← part header (row count, time range, sizes)
  column_names.bin           ← dictionary: column name → numeric ID
  column_idxs.bin            ← column ID → bloom/values shard index
  metaindex.bin              ← top-level index (loaded into memory at open)
  index.bin                  ← block headers grouped into index blocks
  columns_header_index.bin   ← per-block column lookup table
  columns_header.bin         ← per-block column metadata (type, offsets, min/max)
  timestamps.bin             ← compressed timestamp arrays, one array per block (not per column —
                                every row has exactly one timestamp; they are stored together as a
                                single array for the whole block)
  message_bloom.bin          ← bloom filters for the _msg column
  message_values.bin         ← encoded values for the _msg column
  bloom.bin0 ... bloom.binN  ← bloom filters for field columns (sharded by column, multiple blocks
                                per file — offset stored in columnHeader locates a specific
                                block's data for a specific column)
  values.bin0 ... values.binN← encoded values for field columns (same sharding: multiple blocks ×
                                multiple columns packed into each shard file)
```

Each file is append-only during construction. The writer streams blocks sequentially, recording offsets as it goes. The reader uses those offsets for random access.

### The two-level index

Finding a specific block in a part with millions of blocks requires an efficient lookup structure. VictoriaLogs uses a two-level index — the same idea as a B-tree's interior nodes, but optimized for immutable, append-only construction.

```
                    ┌───────────────────┐
                    │   metaindex.bin   │  Level 0: in memory
                    │   ~400 entries    │  one entry per index block
                    │   ~20 KB total    │
                    └────────┬──────────┘
                             │ offset + size
                             ▼
              ┌──────────────────────────────┐
              │          index.bin            │  Level 1: on disk
              │   ~100,000 block headers     │  grouped into index blocks
              │   ~5 MB total                │  each block compressed separately
              └──────────────┬───────────────┘
                             │ offsets into data files
                             ▼
    ┌──────────┬──────────────┬────────────────┬──────────────┐
    │timestamps│columns_header│ bloom.bin0..N   │ values.bin0..N│  Level 2: data
    │  .bin    │   .bin       │ message_bloom   │ message_values│
    └──────────┴──────────────┴────────────────┴──────────────┘
```

**Level 0 — metaindex.bin:** Loaded entirely into RAM when the part opens. Each entry is an `indexBlockHeader` covering a group of blocks:

```
indexBlockHeader {
    streamID          ← minimum streamID in this group
    minTimestamp       ← earliest timestamp across all blocks in the group
    maxTimestamp       ← latest timestamp across all blocks in the group
    indexBlockOffset   ← byte position in index.bin
    indexBlockSize     ← compressed size in index.bin
}
```

For a part with 1 billion rows (~100,000 blocks), the metaindex has ~400 entries and fits in ~20 KB. One comparison can eliminate ~2,500 blocks (2.5 million rows).

**Level 1 — index.bin:** Each index block is a ZSTD-compressed array of `blockHeader` entries. A new index block is flushed every 128 KB of uncompressed header data. The reader loads an index block only when the metaindex says it might contain relevant data.

```
blockHeader {
    streamID                   ← which log stream
    rowsCount                  ← rows in this block
    uncompressedSizeBytes      ← original data size
    timestampsHeader {
        blockOffset            ← byte position in timestamps.bin where THIS block's
                                  timestamp array begins (timestamps are per-block, not
                                  per-column; all rows in the block share one array)
        blockSize              ← compressed size of that array in timestamps.bin
        minTimestamp            ← earliest timestamp in block
        maxTimestamp            ← latest timestamp in block
    }
    columnsHeaderIndexOffset   ← position in columns_header_index.bin
    columnsHeaderIndexSize     ← size in columns_header_index.bin
    columnsHeaderOffset        ← position in columns_header.bin
    columnsHeaderSize          ← size in columns_header.bin
}
```

Block headers within an index block are sorted by `(streamID, minTimestamp)`. This ordering supports both stream-targeted queries ("logs from web-01") and time-range queries ("logs from 10:00–10:05").

### How a query finds its data

Consider a query: `_stream:{host="web-01"} AND _time:[10:00, 10:05] AND level:error`. The reader prunes at every level:

```
Step 1 — Part header (metadata.json)
  Check: does [part.MinTimestamp, part.MaxTimestamp] overlap [10:00, 10:05]?
  If no → skip entire part (millions of blocks avoided)

Step 2 — Metaindex (in memory, ~400 entries)
  For each indexBlockHeader:
    Check: does [ih.minTimestamp, ih.maxTimestamp] overlap [10:00, 10:05]?
    Check: could ih.streamID match web-01's stream ID?
    If no → skip this index block (~250 blocks avoided per entry)

Step 3 — Index block (one disk read + decompress)
  For each blockHeader in the loaded index block:
    Check: does bh.streamID match web-01?
    Check: does [bh.minTimestamp, bh.maxTimestamp] overlap [10:00, 10:05]?
    If no → skip this block (~10,000 rows avoided per block)

Step 4 — Column metadata (columns_header_index.bin + columns_header.bin)
  Resolve "level" → columnNameID via the column_names dictionary
  Load only the columnHeader for "level" from columns_header.bin
  Check: does columnHeader.minValue/maxValue overlap the filter?
  For valueTypeDict: does the inline dictionary contain "error"?
  If no → skip reading values entirely

Step 5 — Bloom filter (bloom.binN)
  Read bloom filter at ch.bloomFilterOffset for the "level" column
  Check: is token "error" definitely absent?
  If yes → skip values read

Step 6 — Values (values.binN)
  Only now read the actual column data at ch.valuesOffset
  Decode, apply filter, return matching rows
```

Each step is cheaper than the next. The metaindex scan (Step 2) touches ~20 KB in RAM. An index block read (Step 3) is one seek + ~128 KB decompression. Bloom filter checks (Step 5) read a few hundred bytes. The expensive values read (Step 6) happens only for blocks that passed every prior check.

### Column metadata as random-access pointers

Each block's column metadata lives in two files that work together:

**`columns_header_index.bin`** maps column names to offsets within the header data. When a query asks for column "level", the reader loads this small index, finds the offset for "level", and jumps directly to that column's metadata — without parsing headers for every other column in the block.

```
columnsHeaderIndex {
    columnHeadersRefs []  {columnNameID, offset}   ← non-const columns
    constColumnsRefs  []  {columnNameID, offset}   ← const columns
}
```

**`columns_header.bin`** stores the actual per-column metadata at those offsets:

```
columnHeader {
    valueType          ← string, dict, uint64, float64, ipv4, timestamp, etc.
    minValue, maxValue ← for numeric types: enables range pruning
    valuesDict         ← for dict type: the full dictionary (up to 8 values)
    valuesOffset       ← byte position in values.binN
    valuesSize         ← compressed size in values.binN
    bloomFilterOffset  ← byte position in bloom.binN
    bloomFilterSize    ← compressed size in bloom.binN
}
```

Concretely, for a block containing `{host="web-01", env="prod", level, status, _msg}` where
`host` and `env` are identical across all rows, the blob looks like:

```
columns_header.bin  (this block's blob, starting at bh.columnsHeaderOffset)
┌────────────────────────────────────────────────────────────────────────┐
│  columnHeader for "level"                           bytes [0 .. 63]    │
│    valueType    = valueTypeDict                                         │
│    valuesDict   = {"error", "info", "warn"}  ← inline, no bloom needed │
│    minValue     = 0  (unused for dict type)                             │
│    maxValue     = 0                                                     │
│    valuesOffset = 4096  ← seek here in values.bin0                     │
│    valuesSize   = 12                                                    │
│    bloomOffset  = 0    (dict type skips bloom filter)                   │
│    bloomSize    = 0                                                     │
├────────────────────────────────────────────────────────────────────────┤
│  columnHeader for "status"                          bytes [64 .. 127]  │
│    valueType    = valueTypeUint16                                       │
│    minValue     = 200  ← range pruning: skip block if query min > 503  │
│    maxValue     = 503                                                   │
│    valuesOffset = 512   ← seek here in values.bin1                     │
│    valuesSize   = 10                                                    │
│    bloomOffset  = 128  ← seek here in bloom.bin1                       │
│    bloomSize    = 32                                                    │
├────────────────────────────────────────────────────────────────────────┤
│  constColumn: host = "web-01"               bytes [128 .. 143]         │
│  constColumn: env  = "prod"                 bytes [144 .. 159]         │
│    (value stored once here; no entry in values.binN needed)            │
└────────────────────────────────────────────────────────────────────────┘

Note: _msg uses message_values.bin / message_bloom.bin — not in this blob.
```

`host` and `env` are **const columns**: all rows in the block share the same value, so the
value is stored once in the header itself rather than in a values file.

The `minValue`/`maxValue` fields are what make range queries efficient. A query like `status >= 400` can skip any block where `columnHeader.maxValue < 400` without reading the values file at all.

For `valueTypeDict` columns (up to 8 unique values), the dictionary is stored inline in the column header. The reader can check whether the filter value exists in the dictionary before touching any data file. If `level` has only `{error, info, warn, debug}` and the query asks for `level:critical`, the block is skipped by examining the header alone.

### The block stream writer: building a part

The `blockStreamWriter` constructs a part by streaming blocks in sorted order. Each block write appends data to multiple files simultaneously:

```
For each block:
  1. Compress timestamps → append to timestamps.bin
     Record (offset, size, minTimestamp, maxTimestamp) in timestampsHeader

  2. For each column in the block:
     a. Encode values → append to values.binN (or message_values.bin for _msg)
     b. Build bloom filter → append to bloom.binN (or message_bloom.bin)
     c. Record (valuesOffset, valuesSize, bloomOffset, bloomSize,
        valueType, minValue, maxValue) in columnHeader

  3. Marshal all columnHeaders → append to columns_header.bin
     Build columnHeaderIndex → append to columns_header_index.bin
     Record offsets in blockHeader

  4. Marshal blockHeader → append to current index block buffer

  5. If index block buffer > 128 KB:
     ZSTD-compress buffer → append to index.bin
     Record (offset, size, streamID, minTs, maxTs) in indexBlockHeader
     Append indexBlockHeader to metaindex buffer
```

At finalization:
1. Flush the last index block to `index.bin`.
2. Write the column name dictionary to `column_names.bin` (ZSTD-compressed).
3. Write column-to-shard mapping to `column_idxs.bin`.
4. ZSTD-compress the full metaindex buffer and write to `metaindex.bin`.
5. Write `metadata.json` with total counts and compressed sizes.

The writer enforces a strict ordering invariant: blocks must arrive sorted by `(streamID, minTimestamp)`. This ensures the index blocks and metaindex entries are naturally sorted, enabling the pruning logic at read time.

### Bloom/values sharding

A part can contain thousands of distinct column names. Storing all columns' bloom filters in a single `bloom.bin` file would create contention and require complex offset management. Instead, VictoriaLogs shards columns across up to 128 file pairs (`bloom.bin0`/`values.bin0` through `bloom.bin127`/`values.bin127`).

The `_msg` column (the log message) gets dedicated files (`message_bloom.bin`/`message_values.bin`) because it appears in nearly every query and benefits from isolation.

In the current format (v3), the column-to-shard mapping is explicitly stored in `column_idxs.bin`. Earlier formats used hash-based assignment (`xxhash(name) % shardCount`), which broke when merge changed the shard count. The explicit mapping makes each part self-describing — it carries its own routing table.

### Corruption detection without checksums

VictoriaLogs does not store explicit CRC or checksum fields in the storage format. Instead, it detects corruption through layered cross-validation:

**1. Decompression and decode validation.** Many blobs (`metaindex.bin`, each index block in `index.bin`, `column_names.bin`, etc.) are ZSTD-compressed. Corruption commonly surfaces as decompression or unmarshaling errors. This format does not add a dedicated per-block CRC field in part metadata.

**2. Size invariants.** The part header records four totals: `CompressedSizeBytes`, `UncompressedSizeBytes`, `RowsCount`, and `BlocksCount`. After reading every block, the reader verifies that the running totals match exactly:

```
if totalBytesRead != ph.CompressedSizeBytes   → panic  (file truncated or extended)
if totalUncompressed != ph.UncompressedSizeBytes → panic
if totalRows != ph.RowsCount                    → panic  (blocks missing or duplicated)
if totalBlocks != ph.BlocksCount                → panic
```

A truncated file produces a mismatch in `CompressedSizeBytes`. Extra garbage bytes at the end also produce a mismatch. Missing or duplicated blocks produce row count or block count mismatches.

**3. Ordering invariants.** Block headers within each index block must be sorted by `(streamID, minTimestamp)`. Index block headers in the metaindex must be sorted by `streamID`. The reader validates these properties during iteration. Index corruption that scrambles ordering is detected immediately.

**4. Bounds checks.** Deserialized fields are validated against known limits: `rowsCount <= maxRowsPerBlock`, `indexBlockSize <= 8 MB`, `columnsHeaderSize <= 8 MB`, `columnNameID < len(columnNames)`. Out-of-range values from corrupted data are caught before they cause memory allocation failures.

This layered approach detects practical corruption classes — decode failures, truncation (size invariants), and logical corruption (ordering) — without adding explicit per-block checksum fields.

### The iterator pattern

The block stream reader provides a simple sequential iterator:

```go
reader.MustInitFromFilePart(path)
for reader.NextBlock() {
    block := reader.blockData
    // process block
}
reader.MustClose()
```

Under the hood, `NextBlock` manages the two-level index transparently. When the current index block's blocks are exhausted, it loads the next index block from `index.bin` using the next metaindex entry. The caller sees a flat stream of blocks without managing index navigation.

This pattern is used during compaction: a merge opens multiple readers (one per source part), advances them in sorted order, and feeds blocks to a writer. The reader's sequential consumption is efficient because it reads `index.bin` in order, giving the OS straightforward read-ahead opportunities.

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
