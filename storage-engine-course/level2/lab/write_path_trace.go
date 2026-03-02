package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// ==================== Stream ID Types ====================

// streamID represents a unique identifier for a log stream.
// In VictoriaLogs, this is a 128-bit hash of the canonical (sorted) tag set.
type streamID struct {
	tenantID uint64
	id       uint64
}

func (s *streamID) equal(other *streamID) bool {
	return s.tenantID == other.tenantID && s.id == other.id
}

func (s *streamID) less(other *streamID) bool {
	if s.tenantID != other.tenantID {
		return s.tenantID < other.tenantID
	}
	return s.id < other.id
}

// ==================== Field and Log Row Types ====================

// Field represents a single key-value pair in a log entry.
type Field struct {
	Name  string
	Value string
}

// LogRows represents a batch of log entries to be ingested.
// This mirrors the LogRows type in VictoriaLogs.
type LogRows struct {
	streamIDs            []streamID
	timestamps           []int64
	rows                 [][]Field
	streamTagsCanonicals []string
}

// ==================== Stream Cache ====================

// StreamCache provides O(1) lookup for known stream IDs.
// This mirrors the partition's stream ID cache in partition.go:477
type StreamCache struct {
	mu    sync.RWMutex
	cache map[string]bool
}

func NewStreamCache() *StreamCache {
	return &StreamCache{
		cache: make(map[string]bool),
	}
}

func (sc *StreamCache) Has(sid *streamID, partitionName string) bool {
	key := fmt.Sprintf("%s:%d:%d", partitionName, sid.tenantID, sid.id)
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.cache[key]
}

func (sc *StreamCache) Put(sid *streamID, partitionName string) {
	key := fmt.Sprintf("%s:%d:%d", partitionName, sid.tenantID, sid.id)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.cache[key] = true
}

// ==================== IndexDB Simulation ====================

// IndexDB simulates the persistent storage for stream metadata.
type IndexDB struct {
	mu        sync.Mutex
	streams   map[string]string
	lookups   atomic.Int64
	registers atomic.Int64
}

func NewIndexDB() *IndexDB {
	return &IndexDB{
		streams: make(map[string]string),
	}
}

func (idb *IndexDB) HasStreamID(sid *streamID) bool {
	idb.lookups.Add(1)
	key := fmt.Sprintf("%d:%d", sid.tenantID, sid.id)
	idb.mu.Lock()
	defer idb.mu.Unlock()
	_, exists := idb.streams[key]
	return exists
}

func (idb *IndexDB) MustRegisterStream(sid *streamID, streamTagsCanonical string) {
	idb.registers.Add(1)
	key := fmt.Sprintf("%d:%d", sid.tenantID, sid.id)
	idb.mu.Lock()
	defer idb.mu.Unlock()
	idb.streams[key] = streamTagsCanonical
}

func (idb *IndexDB) Stats() (lookups, registers int64) {
	return idb.lookups.Load(), idb.registers.Load()
}

// ==================== Partition Simulation ====================

// Partition simulates a single day partition in VictoriaLogs.
type Partition struct {
	name         string
	streamCache  *StreamCache
	idb          *IndexDB
	ddb          *DataDB
	logNewStream bool
}

func NewPartition(name string, streamCache *StreamCache, idb *IndexDB) *Partition {
	pt := &Partition{
		name:         name,
		streamCache:  streamCache,
		idb:          idb,
		logNewStream: true,
	}
	pt.ddb = NewDataDB(pt)
	return pt
}

// MustAddRows is the main entry point for log ingestion.
// This mirrors partition.mustAddRows in partition.go:347-430
func (pt *Partition) MustAddRows(lr *LogRows) {
	// ==================== PHASE 1: Stream Registration ====================

	// pendingRows tracks indices for rows that MIGHT need stream registration
	var pendingRows []int
	streamIDs := lr.streamIDs

	// First pass: identify rows with streams not in the in-memory cache
	for i := range lr.timestamps {
		streamID := &streamIDs[i]
		if pt.hasStreamIDInCache(streamID) {
			// Stream already known - skip registration
			continue
		}
		// Batch consecutive rows with the same streamID
		if len(pendingRows) == 0 || !streamIDs[pendingRows[len(pendingRows)-1]].equal(streamID) {
			pendingRows = append(pendingRows, i)
		}
	}

	// Process pending rows that might need stream registration
	if len(pendingRows) > 0 {
		streamTagsCanonicals := lr.streamTagsCanonicals

		// Sort by streamID to group lookups for the same stream together
		sort.Slice(pendingRows, func(i, j int) bool {
			return streamIDs[pendingRows[i]].less(&streamIDs[pendingRows[j]])
		})

		// Process each unique stream
		for i, rowIdx := range pendingRows {
			streamID := &streamIDs[rowIdx]

			// Skip duplicate streamIDs
			if i > 0 && streamIDs[pendingRows[i-1]].equal(streamID) {
				continue
			}

			// Double-check cache - another goroutine might have registered it
			if pt.hasStreamIDInCache(streamID) {
				continue
			}

			// Check indexdb - the authoritative source
			if !pt.idb.HasStreamID(streamID) {
				// New stream! Register it in indexdb
				streamTagsCanonical := streamTagsCanonicals[rowIdx]
				pt.idb.MustRegisterStream(streamID, streamTagsCanonical)

				if pt.logNewStream {
					fmt.Printf("  [NEW STREAM] %s registered: %s\n", pt.name, streamTagsCanonical)
				}
			}

			// Add to cache for fast future lookups
			pt.putStreamIDToCache(streamID)
		}
	}

	// ==================== PHASE 2: Data Insertion ====================
	pt.ddb.MustAddRows(lr)
}

func (pt *Partition) hasStreamIDInCache(sid *streamID) bool {
	return pt.streamCache.Has(sid, pt.name)
}

func (pt *Partition) putStreamIDToCache(sid *streamID) {
	pt.streamCache.Put(sid, pt.name)
}

// ==================== DataDB Simulation ====================

const maxUncompressedBlockSize = 2 * 1024 * 1024 // 2MB

// logRows is the internal buffer for a single shard
type logRows struct {
	streamIDs  []streamID
	timestamps []int64
	rows       [][]Field
	bufferSize int
}

func (lr *logRows) Len() int {
	return len(lr.streamIDs)
}

func (lr *logRows) Less(i, j int) bool {
	a := &lr.streamIDs[i]
	b := &lr.streamIDs[j]
	if !a.equal(b) {
		return a.less(b)
	}
	return lr.timestamps[i] < lr.timestamps[j]
}

func (lr *logRows) Swap(i, j int) {
	lr.streamIDs[i], lr.streamIDs[j] = lr.streamIDs[j], lr.streamIDs[i]
	lr.timestamps[i], lr.timestamps[j] = lr.timestamps[j], lr.timestamps[i]
	lr.rows[i], lr.rows[j] = lr.rows[j], lr.rows[i]
}

// needFlush returns true when buffer exceeds ~87.5% of max block size
// This mirrors logRows.needFlush in log_rows.go:221-223
func (lr *logRows) needFlush() bool {
	return lr.bufferSize > (maxUncompressedBlockSize/8)*7
}

// rowsBufferShard is a single shard of the rows buffer
type rowsBufferShard struct {
	mu        sync.Mutex
	lr        *logRows
	rowsCount int
	flushes   int
}

// rowsBuffer is a sharded in-memory buffer for incoming log rows
type rowsBuffer struct {
	shards  []rowsBufferShard
	nextIdx atomic.Uint64
}

func NewRowsBuffer(numShards int) *rowsBuffer {
	rb := &rowsBuffer{
		shards: make([]rowsBufferShard, numShards),
	}
	return rb
}

func (rb *rowsBuffer) MustAddRows(lr *LogRows) {
	if len(lr.streamIDs) == 0 {
		return
	}

	// Round-robin shard selection
	idx := rb.nextIdx.Add(1) % uint64(len(rb.shards))
	shard := &rb.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.lr == nil {
		shard.lr = &logRows{}
	}

	// Append rows to shard buffer
	for i := range lr.rows {
		shard.lr.streamIDs = append(shard.lr.streamIDs, lr.streamIDs[i])
		shard.lr.timestamps = append(shard.lr.timestamps, lr.timestamps[i])
		shard.lr.rows = append(shard.lr.rows, lr.rows[i])
		shard.lr.bufferSize += 100 // Simulated row size
	}
	shard.rowsCount += len(lr.rows)

	// Check if flush is needed
	if shard.lr.needFlush() {
		shard.flushLocked()
	}
}

func (shard *rowsBufferShard) flushLocked() {
	if shard.lr == nil || shard.lr.Len() == 0 {
		return
	}

	// Sort rows by (streamID, timestamp) before flushing
	sort.Sort(shard.lr)

	// Sort fields within each row
	for _, row := range shard.lr.rows {
		sort.Slice(row, func(i, j int) bool {
			return row[i].Name < row[j].Name
		})
	}

	shard.flushes++
	fmt.Printf("  [FLUSH] Shard flushed %d rows (flush #%d)\n", shard.lr.Len(), shard.flushes)

	// Reset buffer
	shard.lr = &logRows{}
}

func (rb *rowsBuffer) FlushAll() {
	for i := range rb.shards {
		shard := &rb.shards[i]
		shard.mu.Lock()
		shard.flushLocked()
		shard.mu.Unlock()
	}
}

func (rb *rowsBuffer) Stats() (totalRows, totalFlushes int) {
	for i := range rb.shards {
		shard := &rb.shards[i]
		shard.mu.Lock()
		totalRows += shard.rowsCount
		totalFlushes += shard.flushes
		shard.mu.Unlock()
	}
	return
}

// DataDB manages the rows buffer and part creation
type DataDB struct {
	pt *Partition
	rb *rowsBuffer
}

func NewDataDB(pt *Partition) *DataDB {
	// Create one shard per "CPU" (simulated as 4)
	return &DataDB{
		pt: pt,
		rb: NewRowsBuffer(4),
	}
}

func (ddb *DataDB) MustAddRows(lr *LogRows) {
	ddb.rb.MustAddRows(lr)
}

func (ddb *DataDB) Flush() {
	ddb.rb.FlushAll()
}

func (ddb *DataDB) Stats() (int, int) {
	return ddb.rb.Stats()
}

// ==================== Write Path Trace Demo ====================

func WritePathTraceDemo() {
	fmt.Println("=== Level 2 Lab 1: Write Path Trace ===")
	fmt.Println()
	fmt.Println("This program traces the complete path of log ingestion,")
	fmt.Println("showing stream registration, buffering, and flushing.")
	fmt.Println()

	// Create shared infrastructure
	streamCache := NewStreamCache()
	idb := NewIndexDB()
	partition := NewPartition("2026-03-02", streamCache, idb)

	fmt.Println("=== Step-by-Step Write Path Trace ===")
	fmt.Println()

	// Scenario 1: First row from a new stream
	fmt.Println("--- Scenario 1: First row from a new stream ---")
	fmt.Println()
	lr1 := &LogRows{
		streamIDs: []streamID{
			{tenantID: 1, id: 100},
		},
		timestamps: []int64{1709337600000000000}, // 2024-03-02
		rows: [][]Field{
			{{Name: "message", Value: "Application started"}, {Name: "level", Value: "info"}},
		},
		streamTagsCanonicals: []string{`{app="nginx",host="web-01"}`},
	}
	fmt.Println("Input: Row with stream {app=\"nginx\",host=\"web-01\"}")
	fmt.Println("Path:")
	fmt.Println("  1. Check stream cache (partition.hasStreamIDInCache)")
	fmt.Println("     → MISS: Stream not in cache")
	fmt.Println("  2. Add to pendingRows for registration")
	fmt.Println("  3. Sort pendingRows by streamID")
	fmt.Println("  4. Check indexdb (idb.hasStreamID)")
	fmt.Println("     → NOT FOUND: New stream")
	fmt.Println("  5. Register stream in indexdb (idb.mustRegisterStream)")
	partition.MustAddRows(lr1)
	fmt.Println("  6. Add stream to cache (partition.putStreamIDToCache)")
	fmt.Println("  7. Append row to DataDB buffer (shard selected via round-robin)")
	fmt.Println()

	// Scenario 2: Another row from the same stream
	fmt.Println("--- Scenario 2: Row from known stream (cache hit) ---")
	fmt.Println()
	lr2 := &LogRows{
		streamIDs: []streamID{
			{tenantID: 1, id: 100},
		},
		timestamps: []int64{1709337660000000000},
		rows: [][]Field{
			{{Name: "message", Value: "Request processed"}, {Name: "level", Value: "info"}},
		},
		streamTagsCanonicals: []string{`{app="nginx",host="web-01"}`},
	}
	fmt.Println("Input: Row with same stream {app=\"nginx\",host=\"web-01\"}")
	fmt.Println("Path:")
	fmt.Println("  1. Check stream cache")
	fmt.Println("     → HIT: Stream already known")
	fmt.Println("  2. Skip registration (pendingRows empty)")
	partition.MustAddRows(lr2)
	fmt.Println("  3. Append row to DataDB buffer")
	fmt.Println()

	// Scenario 3: Batch with multiple streams
	fmt.Println("--- Scenario 3: Batch with mixed streams ---")
	fmt.Println()
	lr3 := &LogRows{
		streamIDs: []streamID{
			{tenantID: 1, id: 100}, // Known stream
			{tenantID: 1, id: 200}, // New stream
			{tenantID: 1, id: 100}, // Known stream again
			{tenantID: 1, id: 300}, // Another new stream
		},
		timestamps: []int64{
			1709337700000000000,
			1709337720000000000,
			1709337740000000000,
			1709337760000000000,
		},
		rows: [][]Field{
			{{Name: "message", Value: "Log 1"}},
			{{Name: "message", Value: "Log 2"}},
			{{Name: "message", Value: "Log 3"}},
			{{Name: "message", Value: "Log 4"}},
		},
		streamTagsCanonicals: []string{
			`{app="nginx",host="web-01"}`,
			`{app="nginx",host="web-02"}`,
			`{app="nginx",host="web-01"}`,
			`{app="postgres",host="db-01"}`,
		},
	}
	fmt.Println("Input: 4 rows with 3 unique streams (1 known, 2 new)")
	fmt.Println("Path:")
	fmt.Println("  1. Check stream cache for each unique stream")
	fmt.Println("     → streamID 100: HIT (already registered)")
	fmt.Println("     → streamID 200: MISS (pending)")
	fmt.Println("     → streamID 300: MISS (pending)")
	fmt.Println("  2. pendingRows = [1, 3] (indices of unknown streams)")
	fmt.Println("  3. Sort pendingRows by streamID")
	fmt.Println("  4. For each unique pending stream:")
	fmt.Println("     - Check indexdb")
	fmt.Println("     - Register if not found")
	fmt.Println("     - Add to cache")
	partition.MustAddRows(lr3)
	fmt.Println()

	// Flush remaining data
	fmt.Println("--- Flushing all buffers ---")
	partition.ddb.Flush()
	fmt.Println()

	// Print statistics
	fmt.Println("=== Statistics ===")
	lookups, registers := idb.Stats()
	totalRows, totalFlushes := partition.ddb.Stats()
	fmt.Printf("IndexDB lookups:    %d\n", lookups)
	fmt.Printf("Stream registrations: %d (3 unique streams)\n", registers)
	fmt.Printf("Total rows ingested: %d\n", totalRows)
	fmt.Printf("Total flushes:      %d\n", totalFlushes)
}
