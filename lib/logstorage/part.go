// Package logstorage provides the core storage engine for VictoriaLogs.
//
// ============== Part Overview ==============
//
// A "part" is a searchable unit of stored data within a partition's datadb.
// Parts are the building blocks of the LSM-tree storage architecture:
//
//   - In-memory parts: Data recently ingested, not yet flushed to disk
//   - Small file parts: Flushed to disk but small enough to be cached in RAM
//   - Big file parts: Large on-disk files optimized for sequential reads
//
// ============== Part Lifecycle ==============
//
//  1. Rows are ingested and buffered in rowsBuffer
//  2. Buffer flushes → creates inmemoryPart
//  3. inmemoryPart is opened as searchable part (data now queryable!)
//  4. After flushInterval, inmemoryPart is flushed to disk as file part
//  5. Background merge combines small parts into larger parts
//  6. Old parts are deleted after successful merge
//
// ============== Part Structure ==============
//
// Each part contains:
//   - Metadata (partHeader): Row count, timestamp range, compressed/uncompressed sizes
//   - Column name dictionary: Maps column names to internal IDs
//   - Metaindex: Top-level index pointing to block groups
//   - Index: Per-block headers with stream/timestamp/column locations
//   - Column data: Per-block bloom filters and encoded values
//   - Timestamps: Delta-encoded timestamp blocks
//
// ============== Query Path ==============
//
// When searching for logs:
//  1. Load partHeader to check if timestamp range overlaps query
//  2. Scan indexBlockHeaders (metaindex) to find candidate blocks
//  3. Read blockHeaders (index) for detailed stream/time filtering
//  4. Check bloom filters to skip blocks that can't contain matches
//  5. Read actual column values only for blocks that pass all filters
//
// This multi-level filtering minimizes I/O by eliminating irrelevant data
// at each stage before reading the next, more detailed index level.
package logstorage

import (
	"fmt"
	"path/filepath"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// part is a searchable collection of log data blocks.
//
// A part can be backed by either:
//   - Memory (inmemoryPart): Fast reads, volatile, counted as "in-memory" tier
//   - Disk (file part): Durable, counted as "small" or "big" tier based on size
//
// All parts share the same structure regardless of backing storage:
//   - Blocks are organized by stream ID, then by timestamp within each stream
//   - Each block contains rows for a single stream within a time range
//   - Column data is stored separately from metadata for efficient filtering
//
// The part provides the query interface that:
//   - Enumerates blocks via indexBlockHeaders (coarse) and blockHeaders (fine)
//   - Loads column bloom filters for fast negative lookups
//   - Reads actual column values only when bloom filters pass
//
// WHY NOT JUST ONE INDEX LEVEL?
// The two-level index (metaindex → index) allows:
//   - Loading only the metaindex for initial filtering
//   - Loading index blocks only for matching time ranges
//   - Avoiding loading the entire index into memory for large parts
type part struct {
	// pt is the partition that owns this part.
	// Used for accessing shared resources like indexdb during queries.
	pt *partition

	// path is the filesystem path to this part's directory.
	// Empty string for in-memory parts (they have no on-disk presence yet).
	// For file parts, this is the directory containing all the .bin files.
	path string

	// ph contains the part header with summary metadata.
	// This includes row count, timestamp range, and compressed size.
	// Loaded from metadata.json for file parts, or set during construction for in-memory.
	ph partHeader

	// columnNameIDs maps column names to internal numeric IDs.
	// This indirection saves space: column names appear once in the dictionary,
	// then column headers reference them by 8-byte ID instead of variable-length strings.
	// Built from column_names.bin during part opening.
	columnNameIDs map[string]uint64

	// columnNames is the reverse mapping: internal ID → column name.
	// Used to resolve columnHeaderRef.columnNameID back to the actual name.
	columnNames []string

	// columnIdxs maps column names to their bloom/values shard index.
	// For format v3+, this is stored in column_idxs.bin and provides
	// deterministic shard assignment (v1-v2 used hash-based assignment).
	// This mapping is needed to find the correct bloom.binN and values.binN
	// files for a given column during queries.
	columnIdxs map[string]uint64

	// indexBlockHeaders is the metaindex: top-level index entries.
	// Each entry describes a compressed block of blockHeaders in index.bin.
	// During queries, we scan these to find which index blocks to load
	// based on stream ID and timestamp range filtering.
	indexBlockHeaders []indexBlockHeader

	// File handles for random-access reads from various .bin files.
	// These are kept open for the lifetime of the part to avoid
	// repeated open/close overhead during queries.
	// For in-memory parts, these point to the underlying chunkedbuffer instead.

	indexFile              fs.MustReadAtCloser // Block headers (stream/time info per block)
	columnsHeaderIndexFile fs.MustReadAtCloser // Per-block column name references
	columnsHeaderFile      fs.MustReadAtCloser // Per-block column metadata
	timestampsFile         fs.MustReadAtCloser // Encoded timestamp data

	// messageBloomValues holds bloom filter and values for the _msg column.
	// The _msg column (empty name internally) is special-cased because:
	//   - It's present in virtually every log entry
	//   - It's commonly queried (full-text search)
	//   - Having it in dedicated files avoids sharding overhead
	messageBloomValues bloomValuesReaderAt

	// oldBloomValues is for format version < 1 compatibility.
	// Old formats stored all non-_msg columns in shared bloom/values files.
	// Only used when ph.FormatVersion < 1.
	oldBloomValues bloomValuesReaderAt

	// bloomValuesShards contains sharded bloom filter and value files.
	// Sharding improves parallelism and allows efficient updates during merge.
	// The number of shards is stored in ph.BloomValuesShardsCount.
	// Use getBloomValuesFileForColumnName() to find the correct shard.
	bloomValuesShards []bloomValuesReaderAt
}

// bloomValuesReaderAt pairs bloom filter and values file handles for a column shard.
// Both files are read via random-access (ReadAt) during queries.
type bloomValuesReaderAt struct {
	// bloom is the bloom filter file for this shard.
	// Contains token hashes that allow quick "definitely not present" checks.
	// Empty bloom means "cannot rule out" - must check actual values.
	bloom fs.MustReadAtCloser

	// values is the encoded column values file for this shard.
	// Contains the actual field values, compressed and encoded.
	// Format depends on columnHeader.valueType (string, dict, uint*, etc.)
	values fs.MustReadAtCloser
}

// appendClosers adds both bloom and values closers to the slice for batch closing.
// Used during part shutdown to close all file handles in parallel.
func (r *bloomValuesReaderAt) appendClosers(dst []fs.MustCloser) []fs.MustCloser {
	dst = append(dst, r.bloom)
	dst = append(dst, r.values)
	return dst
}

// mustOpenInmemoryPart creates a searchable part from an inmemoryPart.
//
// Unlike file-backed parts, in-memory parts read from the inmemoryPart's
// chunkedbuffer directly rather than from files. This provides:
//   - Zero disk I/O for recent data
//   - Immediate queryability after flush from rowsBuffer
//   - Data remains in-memory until flushed to disk by merge or timeout
//
// The returned part is fully functional for queries but has path="".
func mustOpenInmemoryPart(pt *partition, mp *inmemoryPart) *part {
	var p part
	p.pt = pt
	p.path = "" // In-memory parts have no disk path
	p.ph = mp.ph

	// Read column name dictionary from the in-memory buffer.
	// This establishes the name ↔ ID mapping used throughout the part.
	columnNamesReader := mp.columnNames.NewReader()
	p.columnNames, p.columnNameIDs = mustReadColumnNames(columnNamesReader)
	columnNamesReader.MustClose()

	// Read column → shard mapping (v3+ feature).
	// For in-memory parts, this is always a single shard.
	columnIdxsReader := mp.columnIdxs.NewReader()
	p.columnIdxs = mustReadColumnIdxs(columnIdxsReader, p.columnNames, p.ph.BloomValuesShardsCount)
	columnIdxsReader.MustClose()

	// Read the metaindex (indexBlockHeaders) from the compressed buffer.
	// This is the top-level index that points to blocks of blockHeaders.
	metaindexReader := mp.metaindex.NewReader()
	var mrs readerWithStats
	mrs.init(metaindexReader)
	p.indexBlockHeaders = mustReadIndexBlockHeaders(p.indexBlockHeaders[:0], &mrs)
	metaindexReader.MustClose()

	// For in-memory parts, file handles point to the chunkedbuffer.
	// This allows the same query code to work for both in-memory and file parts.
	p.indexFile = &mp.index
	p.columnsHeaderIndexFile = &mp.columnsHeaderIndex
	p.columnsHeaderFile = &mp.columnsHeader
	p.timestampsFile = &mp.timestamps

	// Open _msg column bloom/values from in-memory buffers.
	p.messageBloomValues.bloom = &mp.messageBloomValues.bloom
	p.messageBloomValues.values = &mp.messageBloomValues.values

	// In-memory parts always use a single shard for other columns.
	// Sharding is only meaningful for file-backed parts that may be large.
	p.bloomValuesShards = []bloomValuesReaderAt{
		{
			bloom:  &mp.fieldBloomValues.bloom,
			values: &mp.fieldBloomValues.values,
		},
	}

	return &p
}

// mustOpenFilePart loads a file-backed part from disk and makes it searchable.
//
// INITIALIZATION STEPS:
//  1. Read metadata.json to get partHeader (row count, timestamps, sizes)
//  2. Read column_names.bin to build name ↔ ID dictionary
//  3. Read column_idxs.bin to build column → shard mapping (v3+)
//  4. Read metaindex.bin (ZSTD-compressed) to get indexBlockHeaders
//  5. Open all data files for random-access reads
//
// FORMAT VERSION HANDLING:
//   - v0: Single shared bloom/values files for all columns (oldBloomValues)
//   - v1-v2: Sharded bloom/values with hash-based shard assignment
//   - v3+: Sharded bloom/values with deterministic column→shard mapping
//
// The part becomes queryable as soon as this function returns.
func mustOpenFilePart(pt *partition, path string) *part {
	var p part
	p.pt = pt
	p.path = path
	p.ph.mustReadMetadata(path)

	// Build file paths for all the component files
	columnNamesPath := filepath.Join(path, columnNamesFilename)
	columnIdxsPath := filepath.Join(path, columnIdxsFilename)
	metaindexPath := filepath.Join(path, metaindexFilename)
	indexPath := filepath.Join(path, indexFilename)
	columnsHeaderIndexPath := filepath.Join(path, columnsHeaderIndexFilename)
	columnsHeaderPath := filepath.Join(path, columnsHeaderFilename)
	timestampsPath := filepath.Join(path, timestampsFilename)

	// Load column name dictionary (v1+).
	// v0 format doesn't have this file - column names are embedded in each columnHeader.
	if p.ph.FormatVersion >= 1 {
		columnNamesReader := filestream.MustOpen(columnNamesPath, true)
		p.columnNames, p.columnNameIDs = mustReadColumnNames(columnNamesReader)
		columnNamesReader.MustClose()
	}

	// Load column → shard mapping (v3+).
	// This provides deterministic shard assignment instead of hash-based.
	if p.ph.FormatVersion >= 3 {
		columnIdxsReader := filestream.MustOpen(columnIdxsPath, true)
		p.columnIdxs = mustReadColumnIdxs(columnIdxsReader, p.columnNames, p.ph.BloomValuesShardsCount)
		columnIdxsReader.MustClose()
	}

	// Load and decompress the metaindex.
	// This is a ZSTD-compressed block of indexBlockHeader entries.
	metaindexReader := filestream.MustOpen(metaindexPath, true)
	var mrs readerWithStats
	mrs.init(metaindexReader)
	p.indexBlockHeaders = mustReadIndexBlockHeaders(p.indexBlockHeaders[:0], &mrs)
	mrs.MustClose()

	// Open data files for random-access reads.
	// These file handles are kept open for the part's lifetime.
	p.indexFile = fs.MustOpenReaderAt(indexPath)
	if p.ph.FormatVersion >= 1 {
		p.columnsHeaderIndexFile = fs.MustOpenReaderAt(columnsHeaderIndexPath)
	}
	p.columnsHeaderFile = fs.MustOpenReaderAt(columnsHeaderPath)
	p.timestampsFile = fs.MustOpenReaderAt(timestampsPath)

	// Open _msg column bloom/values files.
	// The _msg column always has dedicated files (message_bloom.bin, message_values.bin).
	messageBloomFilterPath := filepath.Join(path, messageBloomFilename)
	p.messageBloomValues.bloom = fs.MustOpenReaderAt(messageBloomFilterPath)

	messageValuesPath := filepath.Join(path, messageValuesFilename)
	p.messageBloomValues.values = fs.MustOpenReaderAt(messageValuesPath)

	// Open bloom/values files for other columns.
	if p.ph.FormatVersion < 1 {
		// v0 format: single shared files for all non-_msg columns.
		bloomPath := filepath.Join(path, oldBloomFilename)
		p.oldBloomValues.bloom = fs.MustOpenReaderAt(bloomPath)

		valuesPath := filepath.Join(path, oldValuesFilename)
		p.oldBloomValues.values = fs.MustOpenReaderAt(valuesPath)
	} else {
		// v1+ format: sharded files for parallelism and better organization.
		p.bloomValuesShards = make([]bloomValuesReaderAt, p.ph.BloomValuesShardsCount)
		for i := range p.bloomValuesShards {
			shard := &p.bloomValuesShards[i]

			bloomPath := getBloomFilePath(path, uint64(i))
			shard.bloom = fs.MustOpenReaderAt(bloomPath)

			valuesPath := getValuesFilePath(path, uint64(i))
			shard.values = fs.MustOpenReaderAt(valuesPath)
		}
	}

	return &p
}

// mustClosePart releases all resources held by a part.
//
// This closes all open file handles in parallel for efficiency,
// which is especially important on high-latency storage (NFS, S3).
// After this call, the part pointer must not be used.
func mustClosePart(p *part) {
	// Collect all closers for parallel close.
	// Parallel close reduces wall-clock time on high-latency storage.
	var cs []fs.MustCloser

	cs = append(cs, p.indexFile)
	if p.ph.FormatVersion >= 1 {
		cs = append(cs, p.columnsHeaderIndexFile)
	}
	cs = append(cs, p.columnsHeaderFile)
	cs = append(cs, p.timestampsFile)
	cs = p.messageBloomValues.appendClosers(cs)

	if p.ph.FormatVersion < 1 {
		// v0 format: close the shared bloom/values files
		cs = p.oldBloomValues.appendClosers(cs)
	} else {
		// v1+ format: close all shard file pairs
		for i := range p.bloomValuesShards {
			cs = p.bloomValuesShards[i].appendClosers(cs)
		}
	}

	fs.MustCloseParallel(cs)

	// Clear references to help GC and catch use-after-close bugs
	p.pt = nil
}

// getBloomValuesFileForColumnName returns the bloom/values reader for a column.
//
// COLUMN ROUTING LOGIC:
//   - Empty name (""): Returns messageBloomValues for the _msg column
//   - Format < v1: Returns oldBloomValues (single shared file)
//   - Format v1-v2: Hash-based shard selection (may be inconsistent)
//   - Format v3+: Deterministic lookup via columnIdxs map
//
// The v3+ deterministic mapping solves a problem from v1-v2 where the same
// column name could hash to different shards on different writes due to
// varying shard counts during merges. v3 stores the mapping explicitly.
func (p *part) getBloomValuesFileForColumnName(name string) *bloomValuesReaderAt {
	// Empty name maps to the special _msg column
	if name == "" {
		return &p.messageBloomValues
	}

	// Legacy format: all non-_msg columns share single bloom/values files
	if p.ph.FormatVersion < 1 {
		return &p.oldBloomValues
	}

	// v1-v2: Hash-based shard selection
	// This can cause issues when shard count changes during merges
	if p.ph.FormatVersion < 3 {
		n := len(p.bloomValuesShards)
		shardIdx := uint64(0)
		if n > 1 {
			// Hash the column name to pick a shard
			h := xxhash.Sum64(bytesutil.ToUnsafeBytes(name))
			shardIdx = h % uint64(n)
		}
		return &p.bloomValuesShards[shardIdx]
	}

	// v3+: Use persisted column→shard mapping
	// This is deterministic regardless of when the part was written
	shardIdx, ok := p.columnIdxs[name]
	if !ok {
		logger.Panicf("BUG: unknown shard index for column %q; columnIdxs=%v", name, p.columnIdxs)
	}
	return &p.bloomValuesShards[shardIdx]
}

// getBloomFilePath returns the path to a bloom filter shard file.
// Shard files are named bloom.bin0, bloom.bin1, etc.
func getBloomFilePath(partPath string, shardIdx uint64) string {
	return filepath.Join(partPath, bloomFilename) + fmt.Sprintf("%d", shardIdx)
}

// getValuesFilePath returns the path to a values shard file.
// Shard files are named values.bin0, values.bin1, etc.
func getValuesFilePath(partPath string, shardIdx uint64) string {
	return filepath.Join(partPath, valuesFilename) + fmt.Sprintf("%d", shardIdx)
}
