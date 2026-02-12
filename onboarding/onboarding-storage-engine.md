# VictoriaLogs Storage Engine & On-Disk Format - Developer Onboarding Guide

This document provides a comprehensive overview of VictoriaLogs' storage engine architecture, from the top-level `Storage` object down to individual bytes on disk. It covers partitioning, the part-based LSM-tree design, block encoding, column compression, bloom filters, the index database, and background merge/compaction.

## Table of Contents

- [Overview](#overview)
- [On-Disk Directory Layout](#on-disk-directory-layout)
- [Architecture Layers](#architecture-layers)
  - [1. Storage Layer](#1-storage-layer)
  - [2. Partition Layer](#2-partition-layer)
  - [3. DataDB Layer](#3-datadb-layer)
  - [4. In-Memory Buffer (rowsBuffer)](#4-in-memory-buffer-rowsbuffer)
  - [5. In-Memory Parts](#5-in-memory-parts)
  - [6. File-Backed Parts](#6-file-backed-parts)
  - [7. Block Encoding](#7-block-encoding)
  - [8. Column Value Encoding](#8-column-value-encoding)
  - [9. Bloom Filters](#9-bloom-filters)
  - [10. IndexDB (Stream Metadata)](#10-indexdb-stream-metadata)
- [Data Ingestion Flow](#data-ingestion-flow)
- [Merge & Compaction](#merge--compaction)
  - [Three-Tier Merge Pipeline](#three-tier-merge-pipeline)
  - [Merge Selection Algorithm](#merge-selection-algorithm)
  - [Block Stream Merge Strategy](#block-stream-merge-strategy)
- [Retention & Lifecycle](#retention--lifecycle)
- [Stream Identification](#stream-identification)
- [Key Constants](#key-constants)
- [Key Design Patterns](#key-design-patterns)
- [Configuration Flags](#configuration-flags)

---

## Overview

VictoriaLogs stores log data in a **part-based LSM-tree** (Log-Structured Merge-tree) design. Data flows through several layers before reaching persistent storage:

1. **Incoming rows** arrive via `Storage.MustAddRows()` and are routed to the correct per-day **partition**
2. Within a partition, rows enter a **sharded in-memory buffer** (`rowsBuffer`) for amortized batching
3. Buffered rows are periodically converted to searchable **in-memory parts**
4. In-memory parts are **flushed to disk** as small file-backed parts
5. Background **merge workers** continuously compact small parts into larger ones

This guide is intentionally scoped to the local storage engine (`lib/logstorage`) used by `vlstorage` when local storage is enabled. For how data reaches `Storage.MustAddRows()` — HTTP endpoints, common parameters, batching, the storage router, and distributed mode — see the **[Data Ingestion Flow Guide](./onboarding-insert-flow.md)**.

Each partition contains two subsystems:
- **datadb** — stores the actual log data in parts (columnar blocks with bloom filters)
- **indexdb** — stores stream metadata as an inverted index (backed by VictoriaMetrics' `mergeset` library)

### What Makes This Design Efficient?

- **Columnar storage**: Each field is stored as a separate column within a block, enabling type-specific compression and selective column reads during queries
- **Bloom filters**: Per-column bloom filters allow skipping blocks that definitely don't contain queried tokens
- **Dictionary encoding**: Columns with few unique values (e.g., log levels) are encoded as single bytes, dramatically speeding up filtering
- **Day-based partitioning**: Entire partitions can be dropped for retention without rewriting data
- **Append-only with background merging**: Write path is sequential (no random I/O), while background merging reduces read amplification

---

## On-Disk Directory Layout

**Key File**: [`lib/logstorage/filenames.go`](../lib/logstorage/filenames.go#L1) — defines all filename constants

```
<storagePath>/
├── delete_tasks.json                        # Persisted delete tasks
└── partitions/
    ├── 20260101/                             # Partition (YYYYMMDD format)
    │   ├── indexdb/                           # Stream metadata (mergeset tables)
    │   │   └── (mergeset internal files)
    │   ├── datadb/                            # Log data
    │   │   ├── parts.json                     # List of active part directory names
    │   │   ├── <mergeIdx_hex>/                # Individual part
    │   │   │   ├── metadata.json              # partHeader (format version, rows, size, timestamps)
    │   │   │   ├── column_names.bin           # Column name ↔ internal ID mapping
    │   │   │   ├── column_idxs.bin            # Column name → bloom/values shard index
    │   │   │   ├── metaindex.bin              # ZSTD-compressed indexBlockHeaders
    │   │   │   ├── index.bin                  # ZSTD-compressed blocks of blockHeaders
    │   │   │   ├── columns_header_index.bin   # Per-block column header references
    │   │   │   ├── columns_header.bin         # Per-block column metadata
    │   │   │   ├── timestamps.bin             # Encoded timestamp data
    │   │   │   ├── message_bloom.bin          # Bloom filter for _msg column
    │   │   │   ├── message_values.bin         # Encoded values for _msg column
    │   │   │   ├── bloom.bin0 ... bloom.binN  # Bloom filters for other columns (sharded)
    │   │   │   └── values.bin0 ... values.binN # Encoded values for other columns (sharded)
    │   │   └── <another_mergeIdx>/
    │   └── snapshots/                         # Partition snapshots
    │       └── <snapshot_name>/
    ├── 20260102/
    └── ...
```

**Directory name constants** ([`filenames.go:23-26`](../lib/logstorage/filenames.go#L23)):
- `indexdbDirname = "indexdb"` — stream metadata
- `datadbDirname = "datadb"` — log data
- `partitionsDirname = "partitions"` — parent directory for all partitions
- `snapshotsDirname = "snapshots"` — per-partition snapshots

---

## Architecture Layers

### 1. Storage Layer

**Key File**: [`lib/logstorage/storage.go`](../lib/logstorage/storage.go#L114)

The `Storage` struct is the top-level object that owns all partitions and caches.

#### StorageConfig

[`StorageConfig`](../lib/logstorage/storage.go#L59) controls storage behavior:

| Field | Line | Description |
|-------|------|-------------|
| `Retention` | [63](../lib/logstorage/storage.go#L63) | How long to keep data; minimum 24h |
| `DefaultParallelReaders` | [68](../lib/logstorage/storage.go#L68) | Default parallel readers per query |
| `FlushInterval` | [80](../lib/logstorage/storage.go#L80) | Interval for flushing in-memory data to disk; minimum 1s |
| `FutureRetention` | [82](../lib/logstorage/storage.go#L82) | Maximum allowed future timestamp offset; minimum 24h |
| `MaxBackfillAge` | [87](../lib/logstorage/storage.go#L87) | Maximum age for historical log ingestion |
| `SnapshotsMaxAge` | [96](../lib/logstorage/storage.go#L96) | Automatic cleanup age for partition snapshots |
| `MaxDiskSpaceUsageBytes` | [73](../lib/logstorage/storage.go#L73) | Optional disk space limit |
| `MaxDiskUsagePercent` | [77](../lib/logstorage/storage.go#L77) | Optional disk usage percentage threshold |
| `MinFreeDiskSpaceBytes` | [100](../lib/logstorage/storage.go#L100) | Triggers read-only mode when free disk space drops below this |

#### Storage Struct

[`Storage`](../lib/logstorage/storage.go#L114) key fields:

```go
type Storage struct {
    path       string             // path to storage directory on disk         [L120]
    retention  time.Duration      // retention for stored data                 [L123]

    partitions     []*partitionWrapper  // sorted by time (oldest first)       [L171]
    ptwHot         *partitionWrapper    // "hot" partition for latest ingestion [L176]
    deletedPartitions []int64           // prevents re-creating deleted parts  [L183]
    partitionsLock sync.Mutex           // protects the above fields           [L186]

    streamIDCache     *cache    // caches (partition, streamID) during ingestion  [L198]
    filterStreamCache *cache    // caches streamIDs for query optimization        [L203]
    deleteTasks       []*DeleteTask // active and pending delete tasks            [L209]
}
```

#### Storage Initialization

[`MustOpenStorage(path, cfg)`](../lib/logstorage/storage.go#L618) performs these steps:

1. Validate and clamp config values (flush interval, retention, etc.) — [L619-641](../lib/logstorage/storage.go#L619)
2. Create storage directory if needed, acquire file lock — [L644-648](../lib/logstorage/storage.go#L644)
3. Initialize caches (`streamIDCache`, `filterStreamCache`) — [L651-652](../lib/logstorage/storage.go#L651)
4. Load persisted delete tasks — [L655-656](../lib/logstorage/storage.go#L655)
5. **Open all partitions in parallel** using CPU-limited concurrency — [L684-715](../lib/logstorage/storage.go#L684)
6. Sort partitions by time, delete future partitions — [L717-737](../lib/logstorage/storage.go#L717)
7. Start background workers — [L740-743](../lib/logstorage/storage.go#L740)

#### partitionWrapper

[`partitionWrapper`](../lib/logstorage/storage.go#L538) wraps a partition with reference counting:

```go
type partitionWrapper struct {
    refCount atomic.Int32     // reference count for safe concurrent access  [L540]
    mustDrop atomic.Bool      // set when partition should be deleted        [L543]
    day      int64            // unix day number                             [L547]
    pt       *partition       // the wrapped partition                       [L550]
    doneCh   chan struct{}    // closed when refCount reaches 0              [L553]
}
```

#### Background Workers

Started by `MustOpenStorage` at [L740-743](../lib/logstorage/storage.go#L740):

| Worker | Function | Purpose |
|--------|----------|---------|
| Retention watcher | [`watchRetention()`](../lib/logstorage/storage.go#L772) | Hourly check; deletes partitions older than `-retentionPeriod` |
| Disk space watcher | [`watchMaxDiskSpaceUsage()`](../lib/logstorage/storage.go#L821) | 10s check; drops oldest partitions when disk limit exceeded |
| Delete tasks watcher | [`watchDeleteTasks()`](../lib/logstorage/storage.go#L764) | Processes delete tasks sequentially |
| Snapshots watcher | [`watchSnapshotsMaxAge()`](../lib/logstorage/storage.go#L768) | Deletes stale partition snapshots |

---

### 2. Partition Layer

**Key File**: [`lib/logstorage/partition.go`](../lib/logstorage/partition.go#L23)

Each partition represents **one calendar day** of log data. Partitions are named in `YYYYMMDD` format ([`partitionNameFormat`](../lib/logstorage/partition.go#L297)).

#### partition Struct

```go
type partition struct {
    s    *Storage       // parent storage                [L25]
    path string         // path to partition directory    [L28]
    name string         // directory name (YYYYMMDD)      [L32]
    idb  *indexdb       // stream metadata index          [L35]
    ddb  *datadb        // actual log data                [L38]
    snapshotLock sync.Mutex // prevents concurrent snapshots [L44]
}
```

#### Partition Lifecycle

| Operation | Function | Line | Description |
|-----------|----------|------|-------------|
| Create | [`mustCreatePartition(path)`](../lib/logstorage/partition.go#L52) | 52 | Creates directory with `indexdb/` and `datadb/` subdirs |
| Open | [`mustOpenPartition(s, path)`](../lib/logstorage/partition.go#L74) | 74 | Opens indexdb and datadb; handles missing dirs from unclean shutdown |
| Close | [`mustClosePartition(pt)`](../lib/logstorage/partition.go#L121) | 121 | Closes indexdb and datadb |
| Delete | [`mustDeletePartition(path)`](../lib/logstorage/partition.go#L67) | 67 | Removes the entire partition directory |
| Add rows | [`pt.mustAddRows(lr)`](../lib/logstorage/partition.go#L135) | 135 | Registers streams in indexdb, adds rows to datadb |
| Snapshot | [`pt.mustCreateSnapshot()`](../lib/logstorage/partition.go#L222) | 222 | Creates snapshot in `<partition>/snapshots/<name>/` |
| Force merge | [`pt.mustForceMerge()`](../lib/logstorage/partition.go#L271) | 271 | Merges all parts in the partition |

#### Data Ingestion in Partition

[`partition.mustAddRows(lr)`](../lib/logstorage/partition.go#L135) does two things:

1. **Register streams** in indexdb ([L136-171](../lib/logstorage/partition.go#L136)):
   - Check each streamID against the in-memory `streamIDCache`
   - For uncached streams, check `idb.hasStreamID()` on disk
   - If stream is new, call `idb.mustRegisterStream()` to create inverted index entries
   - Cache the streamID for future lookups

2. **Add rows** to datadb ([L174](../lib/logstorage/partition.go#L174)):
   - Calls `ddb.mustAddRows(lr)` which enters the in-memory buffer

#### Naming Helpers

- [`getPartitionDayFromName(name)`](../lib/logstorage/partition.go#L283) — parses "20260101" to unix day number
- [`getPartitionNameFromDay(day)`](../lib/logstorage/partition.go#L292) — formats unix day number to "20260101"

---

### 3. DataDB Layer

**Key File**: [`lib/logstorage/datadb.go`](../lib/logstorage/datadb.go#L49)

The `datadb` manages the LSM-tree of parts within a partition. It maintains three tiers of parts (in-memory, small file, big file) and runs background workers to merge them.

#### datadb Struct

```go
type datadb struct {
    rb            rowsBuffer        // sharded in-memory row buffer             [L51]
    mergeIdx      atomic.Uint64     // generates unique part directory names     [L56]

    // Merge counters per tier
    inmemoryMergesTotal    atomic.Uint64  // [L58]
    smallPartMergesTotal   atomic.Uint64  // [L62]
    bigPartMergesTotal     atomic.Uint64  // [L66]

    pt            *partition         // parent partition                         [L79]
    path          string             // path to datadb directory                 [L82]
    flushInterval time.Duration      // interval for flushing to disk            [L85]

    inmemoryParts []*partWrapper     // in-memory parts                          [L88]
    smallParts    []*partWrapper     // small file-backed parts                  [L91]
    bigParts      []*partWrapper     // big file-backed parts                    [L94]
    partsLock     sync.Mutex         // protects all part lists                  [L97]
    stopCh        chan struct{}       // signals background workers to stop       [L109]
}
```

#### partWrapper

[`partWrapper`](../lib/logstorage/datadb.go#L113) wraps an opened part with lifecycle management:

```go
type partWrapper struct {
    refCount      atomic.Int32   // reference count                    [L117]
    mustDrop      atomic.Bool    // marks part for deletion            [L120]
    p             *part          // the opened part                    [L123]
    mp            *inmemoryPart  // non-nil for in-memory parts        [L126]
    isInMerge     bool           // true when part is being merged     [L129]
    flushDeadline time.Time      // when in-memory part must be flushed [L132]
}
```

#### Part Types

[`partType`](../lib/logstorage/datadb.go#L650) defines three tiers:

| Type | Value | Description |
|------|-------|-------------|
| `partInmemory` | 0 | In-memory only; fastest reads but not durable |
| `partSmall` | 1 | Small file on disk; cached in OS page cache |
| `partBig` | 2 | Large file on disk; may exceed available RAM |

**Size thresholds** determine the tier:

- **maxInmemoryPartSize** ([`getMaxInmemoryPartSize()`](../lib/logstorage/datadb.go#L1135)): 10% of allowed memory / `maxInmemoryPartsPerPartition` (min 1MB)
- **maxSmallPartSize** ([`getMaxSmallPartSize()`](../lib/logstorage/datadb.go#L1168)): remaining free RAM / `defaultPartsToMerge` (min 10MB)
- **maxBigPartSize**: [`1TB`](../lib/logstorage/datadb.go#L26)

#### DataDB Initialization

[`mustOpenDatadb(pt, path, flushInterval)`](../lib/logstorage/datadb.go#L170):

1. Read `parts.json` to get the list of active part directories — [L171](../lib/logstorage/datadb.go#L171)
2. Remove unused directories on disk — [L172](../lib/logstorage/datadb.go#L172)
3. Open each file part, classify as small or big based on compressed size — [L176-195](../lib/logstorage/datadb.go#L176)
4. Initialize the `rowsBuffer` with `ddb.mustFlushLogRows` as the flush callback — [L213](../lib/logstorage/datadb.go#L213)
5. Start background workers — [L216](../lib/logstorage/datadb.go#L216)

---

### 4. In-Memory Buffer (rowsBuffer)

**Key File**: [`lib/logstorage/datadb.go`](../lib/logstorage/datadb.go#L709)

The `rowsBuffer` is a **sharded, lock-per-shard** buffer that amortizes the cost of converting incoming log rows into searchable parts.

#### rowsBuffer Struct

```go
type rowsBuffer struct {
    shards  []rowsBufferShard  // one shard per CPU   [L710]
    nextIdx atomic.Uint64      // round-robin counter  [L711]
}
```

#### rowsBufferShard Struct

```go
type rowsBufferShard struct {
    wg        *sync.WaitGroup        // shared with datadb.wg     [L740]
    flushFunc func(lr *logRows)      // callback to create parts  [L741]
    mu        sync.Mutex             // per-shard lock             [L743]
    lr        *logRows               // accumulated rows           [L744]
    flushTimer *time.Timer           // auto-flush after 1 second  [L745]
    _         [CacheLineSize]byte    // padding to prevent false sharing [L748]
}
```

#### Data Flow Through rowsBuffer

[`rb.mustAddRows(lr)`](../lib/logstorage/datadb.go#L761) performs:

1. **Select shard** via round-robin: `idx := rb.nextIdx.Add(1) % len(shards)` — [L767](../lib/logstorage/datadb.go#L767)
2. **Set 1-second auto-flush timer** (if not already set) — [L771-780](../lib/logstorage/datadb.go#L771)
3. **Append rows** to shard's `logRows` buffer — [L784](../lib/logstorage/datadb.go#L784)
4. **Flush immediately** if buffer `needFlush()` returns true — [L785-786](../lib/logstorage/datadb.go#L785)

[`shard.flushLocked()`](../lib/logstorage/datadb.go#L791) calls the `flushFunc` callback which is [`ddb.mustFlushLogRows`](../lib/logstorage/datadb.go#L806).

---

### 5. In-Memory Parts

**Key File**: [`lib/logstorage/inmemory_part.go`](../lib/logstorage/inmemory_part.go#L14)

When the `rowsBuffer` flushes, rows are converted into a searchable **in-memory part**. This is the first point where data becomes queryable.

#### inmemoryPart Struct

```go
type inmemoryPart struct {
    ph partHeader                      // part metadata                     [L16]

    columnNames        chunkedbuffer.Buffer  // column name ↔ ID mapping   [L18]
    columnIdxs         chunkedbuffer.Buffer  // column → shard mapping      [L19]
    metaindex          chunkedbuffer.Buffer  // compressed indexBlockHeaders [L20]
    index              chunkedbuffer.Buffer  // compressed blockHeaders     [L21]
    columnsHeaderIndex chunkedbuffer.Buffer  // column header references    [L22]
    columnsHeader      chunkedbuffer.Buffer  // per-block column metadata   [L23]
    timestamps         chunkedbuffer.Buffer  // encoded timestamps          [L24]

    messageBloomValues bloomValuesBuffer  // bloom+values for _msg column  [L26]
    fieldBloomValues   bloomValuesBuffer  // bloom+values for other columns [L27]
}
```

#### Creating an In-Memory Part

[`inmemoryPart.mustInitFromRows(lr)`](../lib/logstorage/inmemory_part.go#L71):

1. **Sort rows** by streamID, then by timestamp within each stream — [L74](../lib/logstorage/inmemory_part.go#L74)
2. **Sort fields** within each row — [L75](../lib/logstorage/inmemory_part.go#L75)
3. **Write blocks** via `blockStreamWriter`: iterate rows, flush a block when the uncompressed size reaches `maxUncompressedBlockSize` (2MB) or the streamID changes — [L85-102](../lib/logstorage/inmemory_part.go#L85)
4. **Finalize** the writer to populate `partHeader` stats — [L105](../lib/logstorage/inmemory_part.go#L105)

#### Flushing to Disk

[`inmemoryPart.MustStoreToDisk(path)`](../lib/logstorage/inmemory_part.go#L110):

1. Create the part directory — [L111](../lib/logstorage/inmemory_part.go#L111)
2. Write all buffers to their respective files **in parallel** using `ParallelStreamWriter` — [L123-142](../lib/logstorage/inmemory_part.go#L123)
3. Write `metadata.json` — [L144](../lib/logstorage/inmemory_part.go#L144)
4. Sync the directory and its parent — [L148](../lib/logstorage/inmemory_part.go#L148)

#### The Flush Pipeline

[`ddb.mustFlushLogRows(lr)`](../lib/logstorage/datadb.go#L806):

1. Acquire concurrency semaphore — [L807](../lib/logstorage/datadb.go#L807)
2. Create `inmemoryPart` from log rows — [L808-809](../lib/logstorage/datadb.go#L808)
3. Open it as a searchable `part` via [`mustOpenInmemoryPart()`](../lib/logstorage/part.go#L63) — [L810](../lib/logstorage/datadb.go#L810)
4. Release concurrency semaphore — [L811](../lib/logstorage/datadb.go#L811)
5. Wrap in `partWrapper` with a `flushDeadline` — [L813-814](../lib/logstorage/datadb.go#L813)
6. Add to `ddb.inmemoryParts` list and start in-memory merger — [L816-818](../lib/logstorage/datadb.go#L816)

---

### 6. File-Backed Parts

**Key File**: [`lib/logstorage/part.go`](../lib/logstorage/part.go#L15)

A `part` is the searchable representation of a set of blocks, either in-memory or file-backed.

#### part Struct

```go
type part struct {
    pt   *partition                       // parent partition              [L17]
    path string                           // empty for in-memory parts     [L22]
    ph   partHeader                       // part metadata                 [L25]

    columnNameIDs map[string]uint64       // name → internal ID            [L29]
    columnNames   []string                // internal ID → name            [L33]
    columnIdxs    map[string]uint64       // name → bloom/values shard idx [L36]

    indexBlockHeaders []indexBlockHeader   // metaindex entries             [L39]

    // File handles for random access reads
    indexFile              fs.MustReadAtCloser  // [L41]
    columnsHeaderIndexFile fs.MustReadAtCloser  // [L42]
    columnsHeaderFile      fs.MustReadAtCloser  // [L43]
    timestampsFile         fs.MustReadAtCloser  // [L44]

    messageBloomValues bloomValuesReaderAt      // bloom+values for _msg   [L46]
    bloomValuesShards  []bloomValuesReaderAt    // bloom+values for others  [L49]
}
```

#### Opening a File Part

[`mustOpenFilePart(pt, path)`](../lib/logstorage/part.go#L106):

1. Read `metadata.json` into `partHeader` — [L110](../lib/logstorage/part.go#L110)
2. Read `column_names.bin` to build name↔ID mappings — [L121-124](../lib/logstorage/part.go#L121)
3. Read `column_idxs.bin` for bloom/values shard routing — [L126-129](../lib/logstorage/part.go#L126)
4. Read `metaindex.bin` (ZSTD-compressed) into `indexBlockHeaders` — [L132-137](../lib/logstorage/part.go#L132)
5. Open random-access file handles for `index.bin`, `columns_header_index.bin`, `columns_header.bin`, `timestamps.bin` — [L140-145](../lib/logstorage/part.go#L140)
6. Open `message_bloom.bin` and `message_values.bin` for the `_msg` column — [L148-152](../lib/logstorage/part.go#L148)
7. Open sharded `bloom.binN` and `values.binN` files for other columns — [L161-170](../lib/logstorage/part.go#L161)

#### partHeader (metadata.json)

[`partHeader`](../lib/logstorage/part_header.go#L16) — stored as JSON in each part directory:

```go
type partHeader struct {
    FormatVersion         uint    // current latest is 3            [L18]
    CompressedSizeBytes   uint64  // physical size on disk           [L21]
    UncompressedSizeBytes uint64  // original log entry size         [L24]
    RowsCount             uint64  // number of log entries           [L27]
    BlocksCount           uint64  // number of blocks                [L30]
    MinTimestamp           int64  // min timestamp in nanoseconds    [L33]
    MaxTimestamp           int64  // max timestamp in nanoseconds    [L36]
    BloomValuesShardsCount uint64 // number of bloom/values shards  [L39]
}
```

#### Bloom/Values Shard Routing

[`part.getBloomValuesFileForColumnName(name)`](../lib/logstorage/part.go#L202):
- Empty name (the `_msg` column) → uses `messageBloomValues` — [L203-204](../lib/logstorage/part.go#L203)
- FormatVersion < 1 → uses legacy `oldBloomValues` files — [L207-209](../lib/logstorage/part.go#L207)
- FormatVersion 1..2 → hash-based shard selection via xxhash — [L210-217](../lib/logstorage/part.go#L210)
- FormatVersion >= 3 → uses `columnIdxs` map for deterministic shard assignment — [L220-224](../lib/logstorage/part.go#L220)

---

### 7. Block Encoding

Blocks are the fundamental unit of storage. Each block contains log entries for a **single stream** within a part.

#### block Struct

**Key File**: [`lib/logstorage/block.go`](../lib/logstorage/block.go#L15)

```go
type block struct {
    timestamps   []int64    // sorted timestamps per entry    [L17]
    columns      []column   // variable-value columns          [L20]
    constColumns []Field    // constant-value columns          [L23]
}
```

#### column Struct

[`column`](../lib/logstorage/block.go#L94):

```go
type column struct {
    name   string     // field name       [L96]
    values []string   // one value per row [L99]
}
```

#### Const Column Optimization

If all values in a column are identical and fit within [`maxConstColumnValueSize`](../lib/logstorage/consts.go#L46) (256 bytes), the column is stored as a **const column** in the block header instead of as a full column. This is detected by [`column.canStoreInConstColumn()`](../lib/logstorage/block.go#L109) during block construction.

**Why this matters**: Fields like `hostname`, `datacenter`, or `log_level` often have the same value across an entire block. Storing them once in the header rather than N times (once per row) saves significant space and speeds up queries because the value is available without reading the values file.

#### blockData (Packed Block)

**Key File**: [`lib/logstorage/block_data.go`](../lib/logstorage/block_data.go#L15)

`blockData` is the packed, serialized form of a block used for I/O and merging:

```go
type blockData struct {
    streamID              streamID          // stream this block belongs to  [L17]
    uncompressedSizeBytes uint64            // original entry size           [L20]
    rowsCount             uint64            // number of entries             [L23]
    timestampsData        timestampsData    // encoded timestamps            [L26]
    columnsData           []columnData      // packed per-column data        [L29]
    constColumns          []Field           // constant-value columns        [L32]
}
```

#### blockHeader (Index Entry)

**Key File**: [`lib/logstorage/block_header.go`](../lib/logstorage/block_header.go#L17)

Each block has a header stored in `index.bin`:

```go
type blockHeader struct {
    streamID                 streamID         // stream ID for entries        [L19]
    uncompressedSizeBytes    uint64           // original size                [L22]
    rowsCount                uint64           // number of entries            [L25]
    timestampsHeader         timestampsHeader // timestamp block location     [L28]
    columnsHeaderIndexOffset uint64           // offset in columns_header_index.bin [L31]
    columnsHeaderIndexSize   uint64           // size in columns_header_index.bin   [L34]
    columnsHeaderOffset      uint64           // offset in columns_header.bin [L37]
    columnsHeaderSize        uint64           // size in columns_header.bin   [L40]
}
```

#### indexBlockHeader (Metaindex Entry)

**Key File**: [`lib/logstorage/index_block_header.go`](../lib/logstorage/index_block_header.go#L15)

The metaindex (`metaindex.bin`) contains entries that point to groups of `blockHeader` entries in `index.bin`:

```go
type indexBlockHeader struct {
    streamID         streamID  // minimum streamID in this index block [L17]
    minTimestamp      int64    // min timestamp across covered blocks  [L20]
    maxTimestamp      int64    // max timestamp across covered blocks  [L23]
    indexBlockOffset  uint64   // offset in index.bin                  [L26]
    indexBlockSize    uint64   // size in index.bin                    [L29]
}
```

Index blocks in `index.bin` are **ZSTD-compressed** ([`indexBlockHeader.mustWriteIndexBlock()`](../lib/logstorage/index_block_header.go#L42), [L48](../lib/logstorage/index_block_header.go#L48)).

#### columnsHeader & columnsHeaderIndex

[`columnsHeader`](../lib/logstorage/block_header.go#L367) — per-block column metadata:

```go
type columnsHeader struct {
    columnHeaders []columnHeader  // info about each variable column  [L369]
    constColumns  []Field         // constant-value columns           [L372]
}
```

[`columnsHeaderIndex`](../lib/logstorage/block_header.go#L234) — efficient column lookup without reading all column headers:

```go
type columnsHeaderIndex struct {
    columnHeadersRefs []columnHeaderRef  // refs to variable column headers [L236]
    constColumnsRefs  []columnHeaderRef  // refs to const columns           [L239]
}
```

[`columnHeaderRef`](../lib/logstorage/block_header.go#L225):

```go
type columnHeaderRef struct {
    columnNameID uint64  // maps to part.columnNames  [L227]
    offset       uint64  // offset within columnsHeader  [L230]
}
```

#### columnHeader

[`columnHeader`](../lib/logstorage/block_header.go#L584) — per-column metadata within a block:

```go
type columnHeader struct {
    name              string     // column name                          [L586]
    valueType         valueType  // encoding type (string, dict, uint, etc.) [L589]
    minValue          uint64     // min encoded value (for range queries) [L594]
    maxValue          uint64     // max encoded value (for range queries) [L599]
    valuesDict        valuesDict // dictionary for dict-encoded columns  [L602]
    valuesOffset      uint64     // location in values file              [L605]
    valuesSize        uint64     // size in values file                  [L608]
    bloomFilterOffset uint64     // location in bloom file               [L611]
    bloomFilterSize   uint64     // size in bloom file                   [L614]
}
```

#### timestampsHeader

[`timestampsHeader`](../lib/logstorage/block_header.go#L955) — locates the timestamp data within `timestamps.bin`:

```go
type timestampsHeader struct {
    blockOffset  uint64                // offset in timestamps.bin         [L957]
    blockSize    uint64                // size in timestamps.bin           [L960]
    minTimestamp  int64                // minimum timestamp (nanoseconds)  [L963]
    maxTimestamp  int64                // maximum timestamp (nanoseconds)  [L966]
    marshalType  encoding.MarshalType  // encoding method used             [L969]
}
```

#### Block Lookup Path (Query Time)

During a query, blocks are located through this multi-level index:

```
metaindex.bin (ZSTD-compressed indexBlockHeaders)
    → filter by streamID and time range
    → find matching indexBlockHeader entries
    → read index blocks from index.bin (ZSTD-compressed blockHeaders)
        → filter by streamID and time range
        → read columnsHeaderIndex from columns_header_index.bin
            → resolve column names via part.columnNames
            → check if needed columns exist
        → read columnsHeader from columns_header.bin
            → read bloom filter from bloom.binN or message_bloom.bin
            → check if queried tokens exist in bloom filter
            → read column values from values.binN or message_values.bin
        → read timestamps from timestamps.bin
```

---

### 8. Column Value Encoding

**Key File**: [`lib/logstorage/values_encoder.go`](../lib/logstorage/values_encoder.go#L20)

VictoriaLogs automatically detects the best encoding for each column within a block. The encoding priority in [`valuesEncoder.encode()`](../lib/logstorage/values_encoder.go#L109) is:

| Priority | Type | Constant | Value | Description |
|----------|------|----------|-------|-------------|
| 1 | Dictionary | `valueTypeDict` | 2 | Best for querying; ≤8 unique values, ≤256 bytes total |
| 2 | Unsigned int | `valueTypeUint8` / `16` / `32` / `64` | 3-6 | Width selected by max value |
| 3 | Signed int | `valueTypeInt64` | 10 | 8 bytes per value |
| 4 | Float | `valueTypeFloat64` | 7 | 8 bytes per value |
| 5 | IPv4 | `valueTypeIPv4` | 8 | 4 bytes per value |
| 6 | ISO8601 timestamp | `valueTypeTimestampISO8601` | 9 | 8 bytes per value |
| 7 | String (fallback) | `valueTypeString` | 1 | Stored as-is |

#### Dictionary Encoding

Dictionary encoding is tried **first** because it provides the highest speedup during querying — [L119-124](../lib/logstorage/values_encoder.go#L119).

[`valuesDict`](../lib/logstorage/values_encoder.go#L1249) constraints:
- Maximum [`maxDictLen = 8`](../lib/logstorage/consts.go#L75) unique values
- Maximum [`maxDictSizeBytes = 256`](../lib/logstorage/consts.go#L70) total bytes
- Each value encoded as a single byte (the dictionary index)
- The dictionary itself is stored in the `columnHeader`, so no bloom filter is needed

**Why dict is fastest**: When all values fit in a dict, the query engine can compare against the dict entries directly without reading the values file at all. For example, filtering `level:error` on a dict-encoded `level` column is an instant dict lookup.

---

### 9. Bloom Filters

**Key File**: [`lib/logstorage/bloomfilter.go`](../lib/logstorage/bloomfilter.go#L16)

Each non-dict column in a block has an associated bloom filter for fast token matching.

#### Constants

- [`bloomFilterHashesCount = 6`](../lib/logstorage/bloomfilter.go#L16) — number of hash functions
- [`bloomFilterBitsPerItem = 16`](../lib/logstorage/bloomfilter.go#L19) — bits allocated per token

#### bloomFilter Struct

```go
type bloomFilter struct {
    bits []uint64  // bit array stored as 64-bit words  [L40]
}
```

#### Key Functions

| Function | Line | Description |
|----------|------|-------------|
| [`bloomFilterMarshalTokens(dst, tokens)`](../lib/logstorage/bloomfilter.go#L22) | 22 | Creates and marshals bloom filter for string tokens |
| [`bloomFilterMarshalHashes(dst, hashes)`](../lib/logstorage/bloomfilter.go#L31) | 31 | Creates and marshals bloom filter for pre-hashed values |
| [`mustInitTokens(tokens)`](../lib/logstorage/bloomfilter.go#L74) | 74 | Initializes bloom filter from tokens |
| [`containsAll(hashes)`](../lib/logstorage/bloomfilter.go#L173) | 173 | Checks if all hash values are present (used at query time) |
| [`appendTokensHashes(dst, tokens)`](../lib/logstorage/bloomfilter.go#L126) | 126 | Generates bloom filter hashes using xxhash |
| [`initBloomFilter(bits, hashes)`](../lib/logstorage/bloomfilter.go#L109) | 109 | Sets bits in the filter |

#### How Bloom Filters are Used

**At ingestion time**: When writing a block, each non-dict column's values are tokenized, hashed, and stored as a bloom filter in the bloom file.

**At query time**: The query's search terms are tokenized and hashed. The bloom filter is checked first — if it says "no", the block is skipped entirely. If it says "maybe yes", the actual values are read and checked.

**Dict columns skip bloom filters entirely**: Since all unique values are stored in the `columnHeader`'s `valuesDict`, there's no need for a probabilistic structure.

---

### 10. IndexDB (Stream Metadata)

**Key File**: [`lib/logstorage/indexdb.go`](../lib/logstorage/indexdb.go#L72)

Each partition has an `indexdb` that stores stream metadata as an inverted index. It is backed by VictoriaMetrics' [`mergeset.Table`](../lib/logstorage/indexdb.go#L88) — a general-purpose sorted key-value store with background merging.

#### indexdb Struct

```go
type indexdb struct {
    streamsCreatedTotal         atomic.Uint64   // counter since initialization    [L74]
    filterStreamCacheGeneration atomic.Uint32   // invalidated on new entries      [L78]
    path                        string          // path to indexdb directory        [L81]
    partitionName               string          // e.g., "20260101"                [L84]
    tb                          *mergeset.Table // backing storage                 [L88]
    indexSearchPool             sync.Pool       // pooled search helpers            [L91]
    s                           *Storage        // parent storage                  [L93]
}
```

#### Namespace Prefixes

IndexDB stores three types of entries, distinguished by a prefix byte ([L20-31](../lib/logstorage/indexdb.go#L20)):

| Prefix | Value | Key Format | Purpose |
|--------|-------|------------|---------|
| `nsPrefixStreamID` | 0 | `tenantID:streamID` | Check if stream exists |
| `nsPrefixStreamIDToStreamTags` | 1 | `tenantID:streamID → streamTagsCanonical` | Map streamID to its labels |
| `nsPrefixTagToStreamIDs` | 2 | `tenantID:name:value → streamIDs` | Inverted index for `_stream:{...}` filter |

#### Stream Registration

[`idb.mustRegisterStream(streamID, streamTagsCanonical)`](../lib/logstorage/indexdb.go#L534) creates three types of entries for each new stream:

1. **Existence entry** (`nsPrefixStreamID`): `tenantID:streamID` — [L543-547](../lib/logstorage/indexdb.go#L543)
2. **Tags mapping** (`nsPrefixStreamIDToStreamTags`): `tenantID:streamID → tags` — [L549-554](../lib/logstorage/indexdb.go#L549)
3. **Inverted index** (`nsPrefixTagToStreamIDs`): one entry per tag: `tenantID:name:value → streamID` — [L557-564](../lib/logstorage/indexdb.go#L557)

All entries are added in a single batch via `idb.tb.AddItems(items)` — [L568](../lib/logstorage/indexdb.go#L568).

#### Stream Search

[`idb.searchStreamIDs(tenantIDs, sf)`](../lib/logstorage/indexdb.go#L235) finds streams matching a `StreamFilter`:

1. **Check cache** via `loadStreamIDsFromCache()` — [L237-241](../lib/logstorage/indexdb.go#L237)
2. **Search indexdb** for each tenant and each OR-filter — [L246-253](../lib/logstorage/indexdb.go#L246)
3. **Sort and cache** the result — [L256-263](../lib/logstorage/indexdb.go#L256)

The cache is invalidated every time a new entry is added to the mergeset table, via `filterStreamCacheGeneration` ([L578-581](../lib/logstorage/indexdb.go#L578)).

#### Stream Existence Check

[`idb.hasStreamID(sid)`](../lib/logstorage/indexdb.go#L189) — fast existence check using `nsPrefixStreamID` prefix scan.

---

## Data Ingestion Flow

Complete path from `Storage.MustAddRows()` to searchable data:

```
Storage.MustAddRows(lr *LogRows)                    [storage.go:1139]
│
├── Fast path: try hot partition                     [storage.go:1141-1155]
│   └── ptwHot.canAddAllRows(lr) → pt.mustAddRows(lr)
│
└── Slow path: split rows by day                     [storage.go:1157-1217]
    ├── Validate timestamps (retention, futureRetention, maxBackfillAge)
    ├── Group rows by day into map[int64]*LogRows
    └── For each day:
        └── getPartitionForWriting(day)              [storage.go:1244]
            └── Binary search in s.partitions, or create new partition
                └── pt.mustAddRows(lr)               [partition.go:135]
                    │
                    ├── Register streams in indexdb   [partition.go:136-171]
                    │   ├── Check streamIDCache
                    │   ├── idb.hasStreamID()         [indexdb.go:189]
                    │   └── idb.mustRegisterStream()  [indexdb.go:534]
                    │
                    └── ddb.mustAddRows(lr)           [datadb.go:705]
                        └── rb.mustAddRows(lr)        [datadb.go:761]
                            └── shard.flushLocked()   [datadb.go:791]
                                └── ddb.mustFlushLogRows(lr) [datadb.go:806]
                                    │
                                    ├── inmemoryPart.mustInitFromRows(lr) [inmemory_part.go:71]
                                    │   ├── Sort by streamID + timestamp
                                    │   └── Write blocks via blockStreamWriter
                                    │
                                    ├── mustOpenInmemoryPart(pt, mp) [part.go:63]
                                    │   (data is now searchable!)
                                    │
                                    └── Add to ddb.inmemoryParts     [datadb.go:817]
                                        └── Start inmemoryPartsMerger
```

**Key insight**: Data becomes searchable after `mustOpenInmemoryPart()` returns. The `flushInterval` controls how often in-memory parts are persisted to disk for durability, but they're queryable even before that.

---

## Merge & Compaction

### Three-Tier Merge Pipeline

Background merge workers run continuously per datadb instance, started at [`startBackgroundWorkers()`](../lib/logstorage/datadb.go#L221):

| Tier | Worker | Started | Concurrency |
|------|--------|---------|-------------|
| In-memory | [`inmemoryPartsMerger()`](../lib/logstorage/datadb.go#L363) | Dynamically, on new inmemory part | CPU count |
| Small file | [`smallPartsMerger()`](../lib/logstorage/datadb.go#L385) | At datadb open | CPU count |
| Big file | [`bigPartsMerger()`](../lib/logstorage/datadb.go#L407) | At datadb open | CPU count |

Additionally, [`inmemoryPartsFlusher()`](../lib/logstorage/datadb.go#L277) runs on a timer equal to `flushInterval`, flushing in-memory parts whose `flushDeadline` has passed.

### Merge Selection Algorithm

[`appendPartsToMerge(dst, src, maxOutBytes)`](../lib/logstorage/datadb.go#L1369) selects the optimal set of parts to merge:

1. **Filter out oversized parts**: Remove parts bigger than `maxOutBytes / minMergeMultiplier` — [L1377](../lib/logstorage/datadb.go#L1377)
2. **Sort by size** (smallest first, ties broken by reverse timestamp) — [L1387](../lib/logstorage/datadb.go#L1387)
3. **Exhaustive O(N²) search**: Try all sliding windows of size `minSrcParts..maxSrcParts` — [L1401-1421](../lib/logstorage/datadb.go#L1401)
4. **Select best window**: Choose the window with maximum merge multiplier (ratio of total size to largest part)
5. **Skip weak merges**: If the best multiplier < `max(defaultPartsToMerge/2, minMergeMultiplier)`, skip the merge to avoid write amplification — [L1423-1430](../lib/logstorage/datadb.go#L1423)

Key constants:
- [`defaultPartsToMerge = 15`](../lib/logstorage/datadb.go#L38) — maximum parts in a single merge
- [`minMergeMultiplier = 1.7`](../lib/logstorage/datadb.go#L46) — minimum ratio to justify a merge
- Effective merge floor is currently `max(15/2, 1.7) = 7.5` — [L1423-1427](../lib/logstorage/datadb.go#L1423)
- [`maxBigPartSize = 1e12`](../lib/logstorage/datadb.go#L26) — 1TB upper limit for big parts

### The Core Merge Function

[`ddb.mustMergePartsInternal(pws, isFinal, dropFilter, stopCh)`](../lib/logstorage/datadb.go#L489):

1. **Determine destination type** via [`getDstPartType()`](../lib/logstorage/datadb.go#L658) based on merged size — [L500](../lib/logstorage/datadb.go#L500)
2. **Reserve disk space** for file-backed destinations — [L502-515](../lib/logstorage/datadb.go#L502)
3. **Fast path for single in-memory flush**: Use `MustStoreToDisk()` directly — [L538-545](../lib/logstorage/datadb.go#L538)
4. **Open block stream readers** for all source parts — [L549](../lib/logstorage/datadb.go#L549)
5. **Open block stream writer** for destination — [L561-569](../lib/logstorage/datadb.go#L561)
6. **Call `mustMergeBlockStreams()`** — [L577](../lib/logstorage/datadb.go#L577)
7. **Atomically swap** source parts with destination — [L600-612](../lib/logstorage/datadb.go#L600)

### Block Stream Merge Strategy

**Key File**: [`lib/logstorage/block_stream_merger.go`](../lib/logstorage/block_stream_merger.go#L22)

[`mustMergeBlockStreams()`](../lib/logstorage/block_stream_merger.go#L22) uses a min-heap of readers ordered by (streamID, minTimestamp):

1. Initialize heap from all source readers — [L25](../lib/logstorage/block_stream_merger.go#L25)
2. Pop minimum block, write via `mustWriteBlock()` — [L29-30](../lib/logstorage/block_stream_merger.go#L29)
3. If the reader has more blocks, re-heap; otherwise remove it — [L31-35](../lib/logstorage/block_stream_merger.go#L31)
4. Finalize — [L37-41](../lib/logstorage/block_stream_merger.go#L37)

[`blockStreamMerger.mustWriteBlock(bd)`](../lib/logstorage/block_stream_merger.go#L183) decides how to handle each incoming block:

| Condition | Action | Line |
|-----------|--------|------|
| Different streamID | Flush pending rows, start new stream | [L186-191](../lib/logstorage/block_stream_merger.go#L186) |
| Same stream, bsm empty, block is full | **Fast path**: write directly without re-compression | [L192-194](../lib/logstorage/block_stream_merger.go#L192) |
| Same stream, combined too big | Flush pending, then process new block | [L195-199](../lib/logstorage/block_stream_merger.go#L195) |
| Same stream, fits | Merge rows together | [L200-204](../lib/logstorage/block_stream_merger.go#L200) |

The **fast path** (writing full blocks without re-compression) is critical for merge performance — it avoids redundant decode/encode cycles for blocks that are already well-formed.

---

## Retention & Lifecycle

### Retention Enforcement

[`Storage.watchRetention()`](../lib/logstorage/storage.go#L772) runs hourly:

1. Calculate `minAllowedDay` from current time and retention period — [L779](../lib/logstorage/storage.go#L779)
2. Since partitions are sorted by day, iterate from the beginning — [L786-801](../lib/logstorage/storage.go#L786)
3. Mark expired partitions for deletion via `ptw.mustDrop.Store(true)` — [L808](../lib/logstorage/storage.go#L808)
4. Decrement ref count; partition is deleted when refCount reaches 0

### Disk Space Enforcement

[`Storage.watchMaxDiskSpaceUsage()`](../lib/logstorage/storage.go#L821) runs every ~10 seconds:

- Drops the **oldest** partitions first when total disk usage exceeds `maxDiskSpaceUsageBytes` or `maxDiskUsagePercent`
- Keeps at least the newest two per-day partitions attached — [L858-861](../lib/logstorage/storage.go#L858)

### Free Disk Space Guard

When free disk space drops below `minFreeDiskSpaceBytes`, the storage enters **read-only mode** and stops accepting new data.

### Partition Creation

[`Storage.getPartitionForWriting(day)`](../lib/logstorage/storage.go#L1244):

1. **Binary search** in sorted `s.partitions` for the target day — [L1250-1252](../lib/logstorage/storage.go#L1250)
2. If not found, check `deletedPartitions` (prevents re-creating dropped partitions) — [L1262](../lib/logstorage/storage.go#L1262)
3. If the directory exists on disk but isn't loaded (manual addition), return nil — [L1269](../lib/logstorage/storage.go#L1269)
4. Otherwise, create new partition via `mustCreatePartition()` — [L1268](../lib/logstorage/storage.go#L1268)

---

## Stream Identification

### TenantID

**Key File**: [`lib/logstorage/tenant_id.go`](../lib/logstorage/tenant_id.go#L17)

```go
type TenantID struct {
    AccountID uint32  // account identifier  [L19]
    ProjectID uint32  // project identifier  [L22]
}
```

Serialized as 8 bytes (4+4). URL format: `accountID:projectID`.

### streamID

**Key File**: [`lib/logstorage/stream_id.go`](../lib/logstorage/stream_id.go#L11)

```go
type streamID struct {
    tenantID TenantID  // placed first for physical grouping of same-tenant blocks [L16]
    id       u128      // hash of canonically sorted stream labels                 [L21]
}
```

Blocks within parts are **sorted by streamID**. This means all blocks for the same tenant and stream are physically adjacent, enabling efficient range scans.

### StreamTags

**Key File**: [`lib/logstorage/stream_tags.go`](../lib/logstorage/stream_tags.go#L33)

```go
type StreamTags struct {
    buf  []byte        // backing storage for tag data  [L35]
    tags []streamTag   // added tags                    [L38]
}

type streamTag struct {
    Name  []byte  // [L191]
    Value []byte  // [L192]
}
```

Key methods:
- [`Add(name, value)`](../lib/logstorage/stream_tags.go#L75) — adds a tag; empty names become `_msg`; empty values are skipped
- [`MarshalCanonical(dst)`](../lib/logstorage/stream_tags.go#L103) — sorts tags alphabetically, marshals as `count + [name_bytes, value_bytes, ...]`

The canonical form ensures that identical sets of labels always produce the same `streamID` hash, regardless of insertion order.

---

## Key Constants

**Key File**: [`lib/logstorage/consts.go`](../lib/logstorage/consts.go#L1)

| Constant | Value | Line | Description |
|----------|-------|------|-------------|
| `partFormatLatestVersion` | 3 | [11](../lib/logstorage/consts.go#L11) | Current part format version |
| `bloomValuesMaxShardsCount` | 128 | [16](../lib/logstorage/consts.go#L16) | Max bloom/values shard files per part |
| `maxUncompressedIndexBlockSize` | 128KB | [21](../lib/logstorage/consts.go#L21) | Max uncompressed index block |
| `maxUncompressedBlockSize` | 2MB | [26](../lib/logstorage/consts.go#L26) | Max uncompressed data block |
| `maxRowsPerBlock` | 8M | [29](../lib/logstorage/consts.go#L29) | Max log entries per block |
| `maxColumnsPerBlock` | 2,000 | [35](../lib/logstorage/consts.go#L35) | Max columns per block |
| `maxFieldNameSize` | 128 bytes | [40](../lib/logstorage/consts.go#L40) | Max field name length |
| `maxConstColumnValueSize` | 256 bytes | [46](../lib/logstorage/consts.go#L46) | Max const column value size |
| `maxDictSizeBytes` | 256 bytes | [70](../lib/logstorage/consts.go#L70) | Max total dict value bytes |
| `maxDictLen` | 8 | [75](../lib/logstorage/consts.go#L75) | Max dictionary entries |
| `maxParallelReaders` | 2,000 | [6](../lib/logstorage/consts.go#L6) | Max parallel readers for queries |

**DataDB constants** ([`datadb.go`](../lib/logstorage/datadb.go#L22)):

| Constant | Value | Line | Description |
|----------|-------|------|-------------|
| `maxBigPartSize` | 1TB | [26](../lib/logstorage/datadb.go#L26) | Max file size for big parts |
| `maxInmemoryPartsPerPartition` | 20 | [32](../lib/logstorage/datadb.go#L32) | Target max in-memory parts |
| `defaultPartsToMerge` | 15 | [38](../lib/logstorage/datadb.go#L38) | Default merge batch size |
| `minMergeMultiplier` | 1.7 | [46](../lib/logstorage/datadb.go#L46) | Min ratio to justify a merge |

---

## Key Design Patterns

### 1. Part-Based LSM-Tree

Data flows through three tiers: **in-memory → small file → big file**. Each tier has background merge workers that compact parts to reduce read amplification. The merge selection algorithm optimizes for **minimum write amplification** by choosing parts that provide the best size ratio.

### 2. Columnar Storage with Type-Aware Encoding

Rather than storing each log entry as a monolithic row, VictoriaLogs stores each field as a **separate column** within a block. This enables:
- **Type-specific compression**: Integers as fixed-width values, repeated values as dict bytes
- **Selective column reads**: Queries only read the columns they need
- **Const column optimization**: Fields with identical values across a block are stored once in the header

### 3. Sharded In-Memory Buffer

The `rowsBuffer` uses **one shard per CPU** with per-shard locks and cache-line padding ([L748](../lib/logstorage/datadb.go#L748)). This eliminates lock contention during high-throughput ingestion. Each shard independently flushes to in-memory parts after 1 second or when the buffer is full.

### 4. Bloom Filter Skip Optimization

Per-column bloom filters allow queries to skip blocks that definitely don't contain the searched tokens. Combined with the metaindex's time range and streamID filtering, most blocks can be skipped without reading their data.

### 5. Reference Counting for Safe Concurrent Access

Both `partitionWrapper` and `partWrapper` use atomic reference counts (`incRef`/`decRef`) to allow concurrent readers while background workers merge and delete parts. A part is only closed/deleted when its reference count reaches zero.

### 6. Day-Based Partitioning for O(1) Retention

Since each partition is a self-contained directory for one calendar day, retention enforcement is simply a matter of deleting the directory. No data rewriting or compaction is needed. This also allows time-based queries to skip entire partitions that don't overlap the query's time range.

### 7. Stream Registration Caching

The `streamIDCache` ([`storage.go:198`](../lib/logstorage/storage.go#L198)) remembers recently seen streams to avoid redundant indexdb lookups during ingestion. The `filterStreamCache` ([`storage.go:203`](../lib/logstorage/storage.go#L203)) caches query results for `_stream:{...}` filters, invalidated via an atomic generation counter whenever new index entries are added.

### 8. Parallel File I/O for High-Latency Storage

File operations (opening/closing parts, flushing to disk) use parallel I/O where possible. For example, [`mustClosePart()`](../lib/logstorage/part.go#L176) closes all file handles in parallel via `fs.MustCloseParallel()`, and [`MustStoreToDisk()`](../lib/logstorage/inmemory_part.go#L110) writes all files in parallel via `ParallelStreamWriter`. This is particularly important for NFS and Ceph storage backends.

---

## Configuration Flags

Key storage-related flags for local `vlstorage` mode (set by the application layer, passed via `StorageConfig`):

| Flag | Config Field | Default | Description |
|------|-------------|---------|-------------|
| `-retentionPeriod` | `Retention` | 7d | How long to keep data |
| `-defaultParallelReaders` | `DefaultParallelReaders` | `2 * available CPUs` | Default parallel readers per query |
| `-futureRetention` | `FutureRetention` | 2d | Max future timestamp offset |
| `-maxBackfillAge` | `MaxBackfillAge` | (= retention) | Max age for backfilled logs |
| `-snapshotsMaxAge` | `SnapshotsMaxAge` | 3d | Auto-delete partition snapshots older than this age |
| `-inmemoryDataFlushInterval` | `FlushInterval` | 5s | Flush interval for in-memory data |
| `-retention.maxDiskSpaceUsageBytes` | `MaxDiskSpaceUsageBytes` | 0 (no limit) | Max disk usage in bytes |
| `-retention.maxDiskUsagePercent` | `MaxDiskUsagePercent` | 0 (no limit) | Max disk usage percent |
| `-storage.minFreeDiskSpaceBytes` | `MinFreeDiskSpaceBytes` | 10MB | Min free disk space before read-only mode |
| `-logNewStreams` | `LogNewStreams` | false | Log newly created streams (debug) |
| `-logIngestedRows` | `LogIngestedRows` | false | Log all ingested entries (debug) |

---

## Document Maintenance

This document references specific source code line numbers. When modifying the referenced files, please verify that:
1. Struct definitions at referenced lines haven't moved significantly
2. Key function signatures match what's documented
3. Constants and their values are still current

Use the format `[description](../relative/path/to/file.go#LNNN)` for all source references so they work as clickable links in VS Code and GitHub.
