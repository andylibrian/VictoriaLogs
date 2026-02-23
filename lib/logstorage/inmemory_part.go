// Package logstorage provides the core storage engine for VictoriaLogs.
//
// ============== In-Memory Part Overview ==============
//
// An inmemoryPart is the first queryable form of ingested data. When rows are
// flushed from the rowsBuffer, they become an inmemoryPart which can be:
//   - Immediately searched (data is queryable before disk persistence!)
//   - Merged with other in-memory parts to reduce part count
//   - Flushed to disk as a file-backed part for durability
//
// ============== Why In-Memory Parts? ==============
//
// In-memory parts provide a crucial optimization:
//  1. Rows can be searched immediately after buffer flush
//  2. No waiting for disk I/O before query visibility
//  3. Small parts can be merged in-memory before disk write
//  4. Reduces write amplification by batching multiple flushes
//
// ============== Memory Management ==============
//
// In-memory parts use chunkedbuffer.Buffer which grows dynamically.
// getMaxInmemoryPartSize() defines a per-part target size.
// Total in-memory usage also depends on part count and merge/flush behavior.
// Parts are flushed/merged when they exceed in-memory thresholds.
//
// ============== Data Flow ==============
//
//  1. rowsBuffer accumulates incoming rows
//  2. mustFlushLogRows() creates inmemoryPart from rows
//  3. mustOpenInmemoryPart() wraps it as searchable part
//  4. Background merger may combine multiple in-memory parts
//  5. After flushInterval, MustStoreToDisk() persists to disk
package logstorage

import (
	"path/filepath"
	"sort"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/chunkedbuffer"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/filestream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// inmemoryPart holds log data in memory before it's flushed to disk.
//
// The structure mirrors a file-backed part but uses chunkedbuffer.Buffer
// instead of files. This allows the same query code to work for both
// in-memory and on-disk parts by providing Reader/Writer interfaces.
//
// MEMORY LAYOUT:
// Each buffer stores a specific type of data:
//   - columnNames: Dictionary of column names (deduplication)
//   - columnIdxs: Column → shard mapping for bloom/values lookup
//   - metaindex: Compressed indexBlockHeaders (top-level index)
//   - index: Compressed blockHeaders (per-block metadata)
//   - columnsHeaderIndex: Per-block column name references
//   - columnsHeader: Per-block column metadata (types, ranges)
//   - timestamps: Delta-encoded timestamps for all blocks
//
// The bloom/values buffers store per-column filter and value data.
type inmemoryPart struct {
	// ph contains summary metadata for this part.
	// Populated during mustInitFromRows() with row/block counts and sizes.
	ph partHeader

	// Buffers for structural data (indexes, dictionaries)
	columnNames        chunkedbuffer.Buffer // Column name dictionary
	columnIdxs         chunkedbuffer.Buffer // Column → shard mapping
	metaindex          chunkedbuffer.Buffer // ZSTD-compressed indexBlockHeaders
	index              chunkedbuffer.Buffer // ZSTD-compressed blockHeaders
	columnsHeaderIndex chunkedbuffer.Buffer // Per-block column name references
	columnsHeader      chunkedbuffer.Buffer // Per-block column metadata
	timestamps         chunkedbuffer.Buffer // Delta-encoded timestamps

	// Buffers for column bloom filters and values
	messageBloomValues bloomValuesBuffer // Special _msg column
	fieldBloomValues   bloomValuesBuffer // All other columns
}

// bloomValuesBuffer holds bloom filter and values data for a set of columns.
// Used during construction of in-memory parts; written to separate files on disk.
type bloomValuesBuffer struct {
	bloom  chunkedbuffer.Buffer // Bloom filter tokens/hashes
	values chunkedbuffer.Buffer // Encoded column values
}

// reset clears both bloom and values buffers for reuse.
func (b *bloomValuesBuffer) reset() {
	b.bloom.Reset()
	b.values.Reset()
}

// NewStreamReader creates a reader for accessing bloom and values data.
// Used when reading an in-memory part during queries or merges.
func (b *bloomValuesBuffer) NewStreamReader() bloomValuesStreamReader {
	return bloomValuesStreamReader{
		bloom:  b.bloom.NewReader(),
		values: b.values.NewReader(),
	}
}

// NewStreamWriter creates a writer for appending bloom and values data.
// Used during part construction to write column data.
func (b *bloomValuesBuffer) NewStreamWriter() bloomValuesStreamWriter {
	return bloomValuesStreamWriter{
		bloom:  &b.bloom,
		values: &b.values,
	}
}

// reset clears all buffers in mp, preparing it for reuse.
// Called both before initialization and when returning to pool.
func (mp *inmemoryPart) reset() {
	mp.ph.reset()

	mp.columnNames.Reset()
	mp.columnIdxs.Reset()
	mp.metaindex.Reset()
	mp.index.Reset()
	mp.columnsHeaderIndex.Reset()
	mp.columnsHeader.Reset()
	mp.timestamps.Reset()

	mp.messageBloomValues.reset()
	mp.fieldBloomValues.reset()
}

// mustInitFromRows converts a batch of sorted log rows into an in-memory part.
//
// This is the transformation from row-oriented to columnar storage:
//  1. Sort rows by streamID, then by timestamp (if not already sorted)
//  2. Normalize field order within each row for consistent encoding
//  3. Group rows into blocks by streamID and size
//  4. Write each block with its column data, bloom filters, timestamps
//  5. Build the metaindex and finalize the partHeader
//
// BLOCK BOUNDARIES:
// A new block is started when:
//   - The streamID changes (each block is single-stream)
//   - Uncompressed size reaches maxUncompressedBlockSize (2MB)
//
// WHY SORT FIRST?
// Sorting by streamID groups all rows for the same stream together,
// which is essential for:
//   - Efficient block-level filtering during queries
//   - Good compression (similar timestamps compress better)
//   - Merge efficiency (sorted inputs are easier to merge)
func (mp *inmemoryPart) mustInitFromRows(lr *logRows) {
	mp.reset()

	// Ensure deterministic ordering for stable encoding and block boundaries.
	// Rows are sorted by (streamID, timestamp), fields are sorted by name.
	sort.Sort(lr)
	lr.sortFieldsInRows()

	// Initialize the block stream writer to write into our buffers
	bsw := getBlockStreamWriter()
	bsw.MustInitForInmemoryPart(mp)

	// Temporary storage for accumulating rows within the current block
	trs := getTmpRows()
	var sidPrev *streamID
	uncompressedBlockSizeBytes := uint64(0)
	timestamps := lr.timestamps
	rows := lr.rows
	streamIDs := lr.streamIDs

	// Process each row, flushing blocks at stream boundaries or size limits
	for i := range timestamps {
		streamID := &streamIDs[i]
		if sidPrev == nil {
			sidPrev = streamID
		}

		// Flush current block when:
		// 1. Size limit reached (maxUncompressedBlockSize)
		// 2. Stream ID changed (each block is single-stream)
		if uncompressedBlockSizeBytes >= maxUncompressedBlockSize || !streamID.equal(sidPrev) {
			bsw.MustWriteRows(sidPrev, trs.timestamps, trs.rows)
			trs.reset()
			sidPrev = streamID
			uncompressedBlockSizeBytes = 0
		}

		// Accumulate this row into the current block
		fields := rows[i]
		trs.timestamps = append(trs.timestamps, timestamps[i])
		trs.rows = append(trs.rows, fields)
		uncompressedBlockSizeBytes += uint64(EstimatedJSONRowLen(fields))
	}

	// Write the final block (if any rows remain)
	bsw.MustWriteRows(sidPrev, trs.timestamps, trs.rows)
	putTmpRows(trs)

	// Finalize the part header with aggregate statistics
	bsw.Finalize(&mp.ph)
	putBlockStreamWriter(bsw)
}

// MustStoreToDisk writes the in-memory part to disk as a file-backed part.
//
// This is called when:
//   - flushInterval has elapsed since the part was created
//   - A snapshot is being created
//   - Memory pressure requires flushing in-memory data
//
// PERSISTENCE GUARANTEES:
//  1. All data files are written in parallel for speed
//  2. metadata.json is written LAST (it references the other files)
//  3. Directory and parent are synced to ensure durability
//
// After this call, the in-memory part should be discarded and the
// file-backed part opened in its place. The in-memory buffers remain
// valid until the caller releases them.
func (mp *inmemoryPart) MustStoreToDisk(path string) {
	fs.MustMkdirFailIfExist(path)

	// Build paths for all component files
	columnNamesPath := filepath.Join(path, columnNamesFilename)
	columnIdxsPath := filepath.Join(path, columnIdxsFilename)
	metaindexPath := filepath.Join(path, metaindexFilename)
	indexPath := filepath.Join(path, indexFilename)
	columnsHeaderIndexPath := filepath.Join(path, columnsHeaderIndexFilename)
	columnsHeaderPath := filepath.Join(path, columnsHeaderFilename)
	timestampsPath := filepath.Join(path, timestampsFilename)
	messageValuesPath := filepath.Join(path, messageValuesFilename)
	messageBloomFilterPath := filepath.Join(path, messageBloomFilename)

	// Use parallel stream writer to write all files concurrently.
	// This significantly reduces wall-clock time, especially on SSDs.
	var psw filestream.ParallelStreamWriter

	// Add structural data files
	psw.Add(columnNamesPath, &mp.columnNames)
	psw.Add(columnIdxsPath, &mp.columnIdxs)
	psw.Add(metaindexPath, &mp.metaindex)
	psw.Add(indexPath, &mp.index)
	psw.Add(columnsHeaderIndexPath, &mp.columnsHeaderIndex)
	psw.Add(columnsHeaderPath, &mp.columnsHeader)
	psw.Add(timestampsPath, &mp.timestamps)

	// Add _msg column files
	psw.Add(messageBloomFilterPath, &mp.messageBloomValues.bloom)
	psw.Add(messageValuesPath, &mp.messageBloomValues.values)

	// Add other column files (in-memory parts use single shard)
	bloomPath := getBloomFilePath(path, 0)
	psw.Add(bloomPath, &mp.fieldBloomValues.bloom)

	valuesPath := getValuesFilePath(path, 0)
	psw.Add(valuesPath, &mp.fieldBloomValues.values)

	// Execute all writes in parallel
	psw.Run()

	// Write metadata.json AFTER data files are complete.
	// If we crash before this, the part directory won't be in parts.json
	// and will be cleaned up on restart.
	mp.ph.mustWriteMetadata(path)

	// Sync the directory and its parent to ensure all metadata
	// is durable on disk. This prevents data loss on power failure.
	fs.MustSyncPathAndParentDir(path)
}

// tmpRows is used as a helper for inmemoryPart.mustInitFromRows()
type tmpRows struct {
	timestamps []int64

	rows [][]Field
}

func (trs *tmpRows) reset() {
	trs.timestamps = trs.timestamps[:0]

	rows := trs.rows
	for i := range rows {
		// Break references to field slices before putting back to the pool.
		rows[i] = nil
	}
	trs.rows = rows[:0]
}

func getTmpRows() *tmpRows {
	v := tmpRowsPool.Get()
	if v == nil {
		return &tmpRows{}
	}
	return v.(*tmpRows)
}

func putTmpRows(trs *tmpRows) {
	trs.reset()
	tmpRowsPool.Put(trs)
}

var tmpRowsPool sync.Pool

func getInmemoryPart() *inmemoryPart {
	v := inmemoryPartPool.Get()
	if v == nil {
		return &inmemoryPart{}
	}
	return v.(*inmemoryPart)
}

func putInmemoryPart(mp *inmemoryPart) {
	mp.reset()
	inmemoryPartPool.Put(mp)
}

var inmemoryPartPool sync.Pool
