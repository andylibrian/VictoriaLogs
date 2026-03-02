# Level 3 Checkpoint Answers

## Context
- Level: 3 - Blocks, Columns, and Type-Aware Encoding
- Date: 2026-03-02

## Checkpoint Questions

### 1. Why are block-level offsets important for query performance?

**Answer:**

Block-level offsets enable **random access** to data, allowing queries to skip irrelevant data without scanning.

**The offset structure:**

```go
// block_header.go:54-78
type blockHeader struct {
    streamID                 // Which stream
    timestampsHeader         // Time range info
    columnsHeaderIndexOffset uint64  // Where to find column index
    columnsHeaderIndexSize   uint64  // How big the index is
    columnsHeaderOffset      uint64  // Where to find column headers
    columnsHeaderSize        uint64  // How big the headers are
}

// block_header.go:819-850
type columnHeader struct {
    name              string
    valueType         valueType
    minValue, maxValue uint64
    valuesOffset      uint64  // Where values start in values.bin
    valuesSize        uint64  // How many bytes
    bloomFilterOffset uint64  // Where bloom filter starts
    bloomFilterSize   uint64  // How many bytes
}
```

**Query flow with offsets:**

```
Query: level = "error"

1. Read blockHeader (small, in index.bin)
   └─► Check timestampsHeader: does time range overlap? 
       └─► NO → Skip entire block (0 bytes of column data read)
       └─► YES → Continue

2. Read columnsHeaderIndex (using offset/size)
   └─► Find "level" column ID

3. Read specific columnHeader (using offset/size)
   └─► Check valueType (dict? string?)
   └─► Check bloom filter (using bloomFilterOffset/Size)
       └─► NO MATCH → Skip (0 bytes of values read)
       └─► MATCH → Continue

4. Read only the "level" column values (using valuesOffset/Size)
   └─► Skip all other columns entirely

5. Decode and filter
```

**Without offsets, you would need to:**
- Scan index.bin sequentially to find the block
- Scan columns sequentially to find "level"
- Potentially read bloom filters for wrong columns
- No ability to skip ahead

**Impact example:**

| Scenario | With Offsets | Without Offsets |
|----------|--------------|-----------------|
| Query one column | Read ~10KB | Read ~2MB (entire block) |
| Bloom filter rejects | Read ~1KB header | Read ~2MB block |
| Time range miss | Read ~100 bytes | Read ~2MB block |

**Source reference:** `block_header.go:54-78` (blockHeader), `block_header.go:819-850` (columnHeader)

### 2. What makes dict columns special in VictoriaLogs filtering?

**Answer:**

Dict columns have three unique properties that make them the fastest query path:

**1. No bloom filter needed**

```go
// block.go:180-188
if ch.valueType != valueTypeDict {
    // create and marshal bloom filter
    bb.B = bloomFilterMarshalHashes(bb.B[:0], hashesBuf.A)
} else {
    // there is no need in encoding bloom filter for dictionary type,
    // since it isn't used during querying - all the dictionary values 
    // are available in ch.valuesDict
    bb.B = bb.B[:0]
}
```

The dictionary itself provides **exact** membership testing - no false positives possible.

**2. Single-byte comparisons**

Dict-encoded values are stored as 1-byte indices:

```
Original: ["error", "info", "warn", "error", "info", ...]
Dict:     {0: "error", 1: "info", 2: "warn"}
Encoded:  [0, 1, 2, 0, 1, ...]
```

Query `level = "error"`:
1. Lookup "error" in dict → index 0
2. Compare each byte to 0 (CPU-friendly)
3. No string comparisons needed

**3. Direct header access**

The dictionary is stored in the column header:

```go
// block_header.go:836-837
type columnHeader struct {
    valuesDict valuesDict  // Unique values for valueTypeDict
    // ...
}
```

When reading a block:
- Dictionary is loaded immediately with the header
- No separate file read to get unique values
- Enables immediate filtering decision

**Query flow comparison:**

| Step | Dict Column | String Column |
|------|-------------|---------------|
| 1. Read header | Dict loaded | Bloom filter loaded |
| 2. Check membership | Exact dict lookup | Bloom check (may have false positives) |
| 3. Read values | 1 byte per row | Variable length per row |
| 4. Filter | Byte comparison | String comparison |

**Speedup: 5-10x for typical low-cardinality columns**

### 3. Why is column sorting in block initialization useful?

**Answer:**

Column sorting (alphabetically by name) provides several benefits:

**1. Deterministic binary format**

```go
// block.go:229
func (b *block) MustInitFromRows(timestamps []int64, rows [][]Field) {
    b.mustInitFromRows(timestamps, rows)
    b.sortColumnsByName()  // Always sort
}
```

Same data → Same binary layout → Identical files

Benefits:
- Reproducible builds
- Consistent hashing/checksums
- Easier debugging

**2. Efficient column lookup**

With sorted columns, finding a specific column is O(log n) instead of O(n):

```go
// Binary search for column name
sort.Search(len(columns), func(i int) bool {
    return columns[i].name >= targetName
})
```

For 100 columns:
- Linear search: 50 comparisons average
- Binary search: 7 comparisons

**3. Better compression**

Sorted column names create patterns:

```
Unsorted: [message, level, host, timestamp, app, ...]
Sorted:   [app, host, level, message, timestamp, ...]
```

When multiple blocks are written:
- Same column order in each block
- Column name index can reference by position
- Repeated patterns compress better

**4. Column name deduplication**

The columnsHeaderIndex stores unique column names once:

```
Block 1: [app, host, level, message]
Block 2: [app, host, level, message]  ← Same order
Block 3: [app, host, level, message]  ← Same order

Name index: {0: "app", 1: "host", 2: "level", 3: "message"}
Each block references by ID: [0, 1, 2, 3]
```

Without sorting:
```
Block 1: [message, level, host, app]
Block 2: [app, level, host, message]
...

Each block needs different index mapping
```

**5. Cache-friendly access**

When queries access multiple columns (e.g., `SELECT level, message`):
- Sorted columns are adjacent in memory
- Better CPU cache utilization
- Sequential reads are faster than random

**Source reference:** `block.go:229` (sortColumnsByName called in MustInitFromRows)
