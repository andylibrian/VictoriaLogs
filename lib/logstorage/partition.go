// Package logstorage provides the core storage engine for VictoriaLogs.
//
// ============== Partition Overview ==============
//
// A "partition" is a fundamental storage unit in VictoriaLogs that organizes log data by time.
// Each partition typically represents one day's worth of logs (e.g., "20240115" for January 15, 2024).
// This time-based sharding provides several benefits:
//
//  1. Efficient Time-Range Queries: When querying logs within a specific time range,
//     the system only needs to open and search relevant partitions, skipping others entirely.
//
//  2. Simplified Data Retention: Old data can be deleted by simply removing entire partition
//     directories without complex row-level deletion logic.
//
//  3. Isolated Compaction: Each partition has its own compaction process, preventing
//     cross-partition interference and allowing parallel processing.
//
//  4. Faster Recovery: If a partition becomes corrupted, only that day's data is affected,
//     not the entire dataset.
//
// ============== Partition Structure ==============
//
// On disk, a partition is a directory containing:
//
//	partition_path/
//	├── indexdb/     # Stream metadata (stream ID ↔ tags mapping, tag → stream index)
//	├── datadb/      # Actual log data (timestamps, messages, fields)
//	└── snapshots/   # Point-in-time backup snapshots (created on demand)
//
// The separation between indexdb and datadb allows:
//   - Fast stream lookups without scanning raw log data
//   - Independent compaction of metadata vs. log data
//   - Efficient filtering during queries
//
// ============== Key Concepts ==============
//
// Stream: A unique combination of labels/tags that identifies a log source (e.g., a specific
// container, host, or application). Each stream gets a unique streamID. Multiple log entries
// can belong to the same stream.
//
// StreamID: A hash-based identifier derived from stream tags. Used as the primary key for
// looking up stream metadata in indexdb.
//
// StreamID Cache: An in-memory cache (shared across all partitions via Storage.streamIDCache)
// that tracks which streamIDs are known to exist. This avoids expensive indexdb lookups for
// streams we've already seen.
//
// ============== Thread Safety ==============
//
// Partitions are accessed concurrently by:
//   - Ingestion goroutines (adding rows via mustAddRows)
//   - Query goroutines (searching via vlselect)
//   - Background compaction/maintenance goroutines
//   - Snapshot creation goroutines
//
// Most internal components (indexdb, datadb) handle their own synchronization.
// The snapshotLock specifically prevents overlapping snapshot creation for
// the same partition.
package logstorage

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/snapshot/snapshotutil"
)

// PartitionStats contains aggregated statistics for a partition.
// It combines metrics from both subsystems:
//   - DatadbStats: Metrics about stored log data (row counts, compressed sizes, etc.)
//   - IndexdbStats: Metrics about stream metadata (number of streams, index sizes, etc.)
//
// These stats are used for monitoring, capacity planning, and debugging.
// They can be exposed via metrics endpoints for observability tools.
type PartitionStats struct {
	DatadbStats
	IndexdbStats
}

// partition represents a time-partitioned storage unit containing log data.
//
// A partition is the primary organizational unit in VictoriaLogs, typically holding
// one day's worth of logs. It manages two key subsystems:
//
//   - indexdb: Stores stream metadata, enabling efficient lookup of stream tags
//     and field information. When a query filters by stream labels, indexdb
//     provides the mapping from label combinations to internal stream IDs.
//
//   - datadb: Stores the actual log entries (timestamps, messages, structured fields).
//     Data is organized by stream for efficient retrieval and compressed to
//     minimize disk usage.
//
// The partition acts as a coordinator between these subsystems during:
//   - Ingestion: Ensures new streams are registered in indexdb before data is
//     written to datadb
//   - Queries: Coordinates search across both metadata (indexdb) and raw data (datadb)
//   - Snapshots: Creates restorable copies of both subsystems
//   - Maintenance: Triggers compaction and cleanup in both subsystems
type partition struct {
	// s is the parent Storage that owns this partition.
	// The Storage provides shared resources like:
	//   - streamIDCache: Global cache for known stream IDs
	//   - Configuration flags (flushInterval, logNewStreams, etc.)
	//   - Coordination for cross-partition operations
	s *Storage

	// path is the absolute filesystem path to this partition's directory.
	// Example: /data/vlogs/20240115
	// This is where all partition data (indexdb, datadb, snapshots) is stored.
	path string

	// name is the partition name extracted from the directory name.
	// Format: YYYYMMDD (e.g., "20240115" for January 15, 2024).
	// Used for:
	//   - Generating unique cache keys (combined with streamID)
	//   - Logging and debugging
	//   - Time-based partition selection during queries
	name string

	// idb is the index database for this partition.
	// It stores:
	//   - Stream ID to stream tags (labels) mapping
	//   - Tag to stream ID reverse index used by stream filters
	//   - Stream existence markers used during ingestion deduplication
	//
	// When new log streams are encountered during ingestion, they must be
	// registered here first before data can be written to datadb.
	idb *indexdb

	// ddb is the data database for this partition.
	// It stores the actual log entries, organized by:
	//   - Stream ID (for locality of related logs)
	//   - Timestamp (for time-ordered retrieval)
	//
	// Data is stored in compressed blocks for efficiency, with background
	// compaction merging small blocks into larger, more optimized ones.
	ddb *datadb

	// snapshotLock prevents concurrent snapshot creation for this partition.
	// Snapshot creation involves multiple filesystem operations; serializing
	// them avoids overlap and keeps lifecycle behavior predictable.
	snapshotLock sync.Mutex
}

// mustCreatePartition creates a new empty partition on disk at the specified path.
//
// USAGE FLOW:
//  1. mustCreatePartition() - Create the partition directory structure
//  2. mustOpenPartition()   - Open it for reading/writing
//  3. (use the partition for ingestion/queries)
//  4. mustClosePartition()  - Close when done
//  5. mustDeletePartition() - Optionally delete if no longer needed
//
// WHAT IT CREATES:
//
//	partition_path/
//	├── indexdb/    # Stream metadata storage
//	└── datadb/     # Log data storage
//
// The function uses "Must" prefix because partition creation is fundamental
// to storage operation - if it fails, the database cannot function, so we
// panic rather than return an error.
//
// fs.MustSyncPathAndParentDir ensures the directory entries are durably
// written to disk, preventing data loss on power failure before the
// partition is used.
func mustCreatePartition(path string) {
	fs.MustMkdirFailIfExist(path)

	indexdbPath := filepath.Join(path, indexdbDirname)
	mustCreateIndexdb(indexdbPath)

	datadbPath := filepath.Join(path, datadbDirname)
	mustCreateDatadb(datadbPath)

	fs.MustSyncPathAndParentDir(path)
}

// mustDeletePartition permanently removes a partition from disk.
//
// IMPORTANT: The partition MUST be closed via mustClosePartition() before deletion.
// Deleting an open partition would cause undefined behavior and potential crashes
// as in-memory references would point to deleted files.
//
// This is typically called during:
//   - Data retention cleanup (removing old partitions beyond retention period)
//   - Manual partition deletion via administrative API
func mustDeletePartition(path string) {
	fs.MustRemoveDir(path)
}

// mustOpenPartition loads an existing partition from disk into memory.
//
// INITIALIZATION PROCESS:
//  1. Validate that required subdirectories (indexdb, datadb) exist
//  2. Handle partial/corrupt states from unclean shutdowns
//  3. Open indexdb (stream metadata) first
//  4. Create partition struct with indexdb reference
//  5. Open datadb (log data) with reference to partition
//
// CORRUPTION RECOVERY:
// If VictoriaLogs crashes during partition creation (e.g., OOM, SIGKILL, power loss),
// we might have a partially created partition. The recovery logic handles:
//
//   - Missing indexdb but existing datadb: PANIC - this indicates serious corruption
//     (indexdb should always be created before datadb during normal operation)
//
//   - Missing indexdb AND missing datadb: Create missing indexdb
//     (this was a partition in mid-creation when crash occurred)
//
//   - Missing datadb but existing indexdb: Create missing datadb
//     (crash occurred after indexdb creation but before datadb creation)
//
// The order matters: indexdb is opened first because datadb initialization
// may need to reference stream information from indexdb.
func mustOpenPartition(s *Storage, path string) *partition {
	name := filepath.Base(path)

	indexdbPath := filepath.Join(path, indexdbDirname)
	isIndexDBExist := fs.IsPathExist(indexdbPath)

	datadbPath := filepath.Join(path, datadbDirname)
	isDatadbExist := fs.IsPathExist(datadbPath)

	// Handle corruption case: datadb exists but indexdb is missing
	// This is a serious inconsistency that cannot be auto-recovered
	if !isIndexDBExist {
		if isDatadbExist {
			logger.Panicf("FATAL: indexdb directory %s is missing, but datadb directory %s exists. "+
				"This indicates corruption. Manually remove the %s partition to resolve it (partition data will be lost)",
				indexdbPath, datadbPath, path)
		}

		// Auto-recover: create missing indexdb (crash during partition creation)
		logger.Warnf("creating missing indexdb directory %s, this could happen if VictoriaLogs shuts down uncleanly "+
			"(via OOM crash, a panic, SIGKILL or hardware shutdown) while creating new per-day partition", indexdbPath)
		mustCreateIndexdb(indexdbPath)
	}

	idb := mustOpenIndexdb(indexdbPath, name, s)

	// Create the partition object with indexdb reference
	// Note: ddb is set later after we handle potential missing datadb
	pt := &partition{
		s:    s,
		path: path,
		name: name,
		idb:  idb,
	}

	// Auto-recover: create missing datadb if needed
	if !isDatadbExist {
		logger.Warnf("creating missing datadb directory %s, this could happen if VictoriaLogs shuts down uncleanly "+
			"(via OOM crash, a panic, SIGKILL or hardware shutdown) while creating new per-day partition", datadbPath)
		mustCreateDatadb(datadbPath)
	}

	// Open datadb with reference to partition (for accessing shared config)
	pt.ddb = mustOpenDatadb(pt, datadbPath, s.flushInterval)

	return pt
}

// mustClosePartition gracefully shuts down a partition, flushing pending data and releasing resources.
//
// SHUTDOWN ORDER:
//  1. Close indexdb first (flushes metadata, releases indexes)
//  2. Close datadb second (flushes log data, releases data files)
//  3. Clear partition struct fields (prevents accidental use after close)
//
// After calling this, the partition pointer should not be used. If the partition
// will be deleted, call mustDeletePartition() after this returns.
func mustClosePartition(pt *partition) {
	// Close indexdb first
	mustCloseIndexdb(pt.idb)
	pt.idb = nil

	// Close datadb
	mustCloseDatadb(pt.ddb)
	pt.ddb = nil

	// Clear references to prevent use-after-close bugs
	// These also help garbage collection
	pt.name = ""
	pt.path = ""
	pt.s = nil
}

// mustAddRows ingests a batch of log rows into this partition.
//
// This is the main entry point for data ingestion at the partition level.
// The function performs two critical operations in order:
//
// ============== PHASE 1: Stream Registration ==============
//
// Before adding data, we must ensure all streams referenced in this batch are
// registered in indexdb. A "stream" represents a unique log source (e.g., a
// specific container, application instance, or host). Each stream has:
//   - A unique streamID (hash of its labels)
//   - Associated labels/tags (e.g., {app="nginx", host="server1"})
//
// WHY REGISTER FIRST?
//   - Queries filter by stream labels, so we need the label→streamID mapping
//   - Data in datadb references streamIDs, not labels directly
//   - Without registration, queries couldn't find data by label
//
// REGISTRATION ALGORITHM:
//
//  1. Identify "pending rows" - rows belonging to streams we haven't seen.
//     We use a two-level cache strategy:
//     a) Check in-memory streamIDCache (fast, shared across all partitions)
//     b) If cache miss, we'll check indexdb later
//
//  2. Sort pending rows by streamID to batch lookups for the same stream.
//     This reduces indexdb contention when many rows belong to the same stream.
//
//  3. For each unique streamID among pending rows:
//     a) Double-check in-memory cache (might have been added by concurrent goroutine)
//     b) Query indexdb to see if stream already exists
//     c) If not in indexdb, register it (create streamID → labels mapping)
//     d) Add to in-memory cache for future fast lookups
//
//  4. Optionally log new streams (for debugging, controlled by -logNewStreams flag)
//
// ============== PHASE 2: Data Insertion ==============
//
// After all streams are registered, add the actual log data to datadb.
// The datadb handles:
//   - Organizing data by stream and timestamp
//   - Compression of log entries
//   - Flushing to disk in the background
//
// Optionally log all ingested rows (for debugging, controlled by -logIngestedRows flag).
//
// PERFORMANCE CONSIDERATIONS:
//   - The two-phase approach ensures we never write data for unregistered streams
//   - Cache checking is O(1) and prevents most indexdb lookups
//   - Sorting pending rows groups duplicate stream checks together
//   - Batch operations minimize lock contention
func (pt *partition) mustAddRows(lr *LogRows) {
	// ==================== PHASE 1: Stream Registration ====================

	// pendingRows tracks indices into lr for rows that MIGHT need stream registration.
	// We'll refine this list through multiple cache checks.
	// WHY track indices instead of streamIDs directly? Because we need to access
	// the full row data (streamTagsCanonicals, rows) for registration later.
	var pendingRows []int
	streamIDs := lr.streamIDs

	// First pass: identify rows with streams not in the in-memory cache.
	// This is fast (O(1) cache lookup) and filters out most rows.
	// WHY two passes? The first pass is cheap (in-memory cache check) and eliminates
	// most rows. Only rows that pass this check go through the expensive indexdb lookup.
	for i := range lr.timestamps {
		streamID := &streamIDs[i]
		if pt.hasStreamIDInCache(streamID) {
			// Stream already known - skip registration for this row
			continue
		}
		// Batch consecutive rows with the same streamID
		// (they'll be handled together in the sorted pass)
		// WHY only add first occurrence? After sorting, we'll process unique streamIDs.
		// Adding duplicates here would just mean more work to deduplicate later.
		if len(pendingRows) == 0 || !streamIDs[pendingRows[len(pendingRows)-1]].equal(streamID) {
			pendingRows = append(pendingRows, i)
		}
	}

	// Process pending rows that might need stream registration
	if len(pendingRows) > 0 {
		logNewStreams := pt.s.logNewStreams.Load()
		streamTagsCanonicals := lr.streamTagsCanonicals

		// Sort by streamID to group lookups for the same stream together.
		// This is more efficient than random access patterns.
		// WHY sort? When we query indexdb for stream existence, consecutive queries
		// for the same streamID are wasteful. Sorting ensures we only check each unique
		// streamID once, and also improves cache locality for the indexdb lookups.
		sort.Slice(pendingRows, func(i, j int) bool {
			return streamIDs[pendingRows[i]].less(&streamIDs[pendingRows[j]])
		})

		// Process each unique stream that might need registration
		for i, rowIdx := range pendingRows {
			streamID := &streamIDs[rowIdx]

			// Skip duplicate streamIDs (they're sorted, so check previous)
			if i > 0 && streamIDs[pendingRows[i-1]].equal(streamID) {
				continue
			}

			// Double-check cache - another goroutine might have registered it
			if pt.hasStreamIDInCache(streamID) {
				continue
			}

			// Check indexdb - the authoritative source for stream existence
			if !pt.idb.hasStreamID(streamID) {
				// New stream! Register it in indexdb.
				streamTagsCanonical := streamTagsCanonicals[rowIdx]
				pt.idb.mustRegisterStream(streamID, streamTagsCanonical)

				// Optional: log new stream discovery for debugging
				if logNewStreams {
					pt.logNewStream(streamTagsCanonical, lr.rows[rowIdx])
				}
			}

			// Add to cache for fast future lookups (even if already existed)
			pt.putStreamIDToCache(streamID)
		}
	}

	// ==================== PHASE 2: Data Insertion ====================

	// Add rows to datadb (handles compression, organization, persistence)
	pt.ddb.mustAddRows(lr)

	// Optional: log all ingested rows for debugging
	if pt.s.logIngestedRows {
		pt.logIngestedRows(lr)
	}
}

// logNewStream logs information about a newly discovered log stream.
// This is controlled by the -logNewStreams flag and is useful for:
//   - Debugging ingestion issues (seeing what streams are being created)
//   - Monitoring cardinality growth (detecting label explosion)
//   - Understanding data flow patterns
func (pt *partition) logNewStream(streamTagsCanonical string, fields []Field) {
	streamTags := getStreamTagsString(streamTagsCanonical)
	line := MarshalFieldsToJSON(nil, fields)
	logger.Infof("partition %s: new stream %s for log entry %s", pt.path, streamTags, line)
}

// logIngestedRows logs every ingested log entry.
// This is controlled by the -logIngestedRows flag and is useful for:
//   - Detailed debugging of data ingestion
//   - Verifying log content during development
//   - Tracing specific log entries through the system
//
// WARNING: This is very verbose and should only be enabled for debugging!
func (pt *partition) logIngestedRows(lr *LogRows) {
	for i := range lr.rows {
		s := lr.GetRowString(i)
		logger.Infof("partition %s: new log entry %s", pt.path, s)
	}
}

// ==================== Stream ID Cache Functions ====================
//
// These functions manage the in-memory cache that tracks known stream IDs.
// The cache provides O(1) lookup to avoid expensive indexdb queries for
// streams we've already seen.
//
// Cache Architecture:
//   - The cache lives in Storage.streamIDCache (shared across all partitions)
//   - Cache keys include partition name to distinguish streams across partitions
//   - This is a "seen" cache - we only track existence, not the full stream data
//
// Why partition name in the cache key?
//   - The same streamID hash could theoretically exist in different partitions
//   - Including partition name ensures correct isolation
//   - Allows independent cache management per partition

// hasStreamIDInCache checks if this stream ID is already known (in-memory).
// Returns true if the stream has been seen before in this partition.
//
// PERFORMANCE: This is an in-memory cache lookup that can avoid indexdb access.
func (pt *partition) hasStreamIDInCache(sid *streamID) bool {
	bb := bbPool.Get()
	bb.B = pt.marshalStreamIDCacheKey(bb.B, sid)
	_, ok := pt.s.streamIDCache.Get(bb.B)
	bbPool.Put(bb)

	return ok
}

// putStreamIDToCache marks a stream ID as known in the in-memory cache.
// After this call, hasStreamIDInCache will return true for this stream.
//
// This is called after:
//   - Registering a new stream in indexdb
//   - Confirming a stream exists in indexdb (on cache miss)
func (pt *partition) putStreamIDToCache(sid *streamID) {
	bb := bbPool.Get()
	bb.B = pt.marshalStreamIDCacheKey(bb.B, sid)
	pt.s.streamIDCache.Set(bb.B, nil)
	bbPool.Put(bb)
}

// marshalStreamIDCacheKey creates a unique cache key for a stream in this partition.
//
// Key format: [partition_name_bytes] + [stream_id_bytes]
//
// Using the partition name in the key ensures:
//   - Cache entries don't collide across partitions
//   - Each partition maintains its own stream namespace
func (pt *partition) marshalStreamIDCacheKey(dst []byte, sid *streamID) []byte {
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(pt.name))
	dst = sid.marshal(dst)
	return dst
}

// ==================== Debug and Testing Functions ====================

// debugFlush forces all pending data in memory to be flushed to disk and made searchable.
//
// This is primarily used for testing and debugging. In production, data is flushed
// automatically in the background based on time and size thresholds.
//
// What it flushes:
//   - datadb: Recently ingested log entries waiting in memory buffers
//   - indexdb: Recently registered streams and metadata
//
// After calling this, all data ingested before the call will be visible to queries.
// This is important for:
//   - Integration tests that need immediate query visibility
//   - Debugging query results that seem to be missing recent data
//   - Ensuring data durability before controlled shutdown
func (pt *partition) debugFlush() {
	pt.ddb.debugFlush()
	pt.idb.debugFlush()
}

// ==================== Snapshot Operations ====================
//
// Snapshots provide restorable backups of partition data.
// They are used for:
//   - Creating backups without stopping the service
//   - Replicating data to another VictoriaLogs instance
//   - Disaster recovery
//
// A snapshot contains both indexdb and datadb state for a partition and can be
// restored to recover that partition.

// mustCreateSnapshot creates a snapshot backup of this partition.
//
// SNAPSHOT PROCESS:
//  1. Acquire snapshotLock to prevent concurrent snapshots
//  2. Create snapshot directory under partition/snapshots/<timestamp>/
//  3. Create indexdb snapshot
//  4. Create datadb snapshot (this flushes in-memory datadb parts to files first)
//  5. Sync to disk for durability
//
// NOTES:
//   - Snapshot operations are serialized per partition by snapshotLock.
//   - Ingestion isn't globally paused while snapshotting.
//   - The result is intended to be restorable, but should not be interpreted
//     as a globally atomic cut across all in-flight ingestion.
//
// Returns: The absolute path to the created snapshot directory.
func (pt *partition) mustCreateSnapshot() string {
	logger.Infof("creating a snapshot for partition %q", pt.name)
	startTime := time.Now()

	pt.snapshotLock.Lock()
	defer pt.snapshotLock.Unlock()

	// Generate unique snapshot name based on timestamp
	snapshotName := snapshotutil.NewName()
	dstDir := filepath.Join(pt.path, snapshotsDirname, snapshotName)
	fs.MustMkdirFailIfExist(dstDir)

	// Create snapshots of both databases while holding snapshotLock
	dstIndexdbDir := filepath.Join(dstDir, indexdbDirname)
	pt.idb.mustCreateSnapshotAt(dstIndexdbDir)

	dstDatadbDir := filepath.Join(dstDir, datadbDirname)
	pt.ddb.mustCreateSnapshotAt(dstDatadbDir)

	// Ensure directory entries are durable on disk
	fs.MustSyncPathAndParentDir(dstDir)

	logger.Infof("created a snapshot for partition %q at %q in %.3f seconds", pt.name, dstDir, time.Since(startTime).Seconds())

	return dstDir
}

// deleteSnapshot removes a previously created snapshot from this partition.
//
// This is used for:
//   - Cleaning up old snapshots after they've been backed up elsewhere
//   - Freeing disk space
//   - Removing failed or corrupted snapshots
//
// Returns an error if the snapshot doesn't exist (non-fatal, allows cleanup scripts
// to be idempotent).
func (pt *partition) deleteSnapshot(snapshotName string) error {
	logger.Infof("deleting snapshot %q for partition %q", snapshotName, pt.name)

	pt.snapshotLock.Lock()
	defer pt.snapshotLock.Unlock()

	snapshotPath := filepath.Join(pt.path, snapshotsDirname, snapshotName)
	if !fs.IsPathExist(snapshotPath) {
		return fmt.Errorf("snapshot %q doesn't exist at %q", snapshotName, pt.path)
	}

	fs.MustRemoveDir(snapshotPath)

	logger.Infof("deleted snapshot %q for partition %q at %q", snapshotName, pt.name, snapshotPath)

	return nil
}

// ==================== Statistics and Maintenance ====================

// updateStats populates the provided PartitionStats with current metrics from this partition.
//
// This is called periodically to update monitoring metrics. The stats include:
//   - Number of rows stored
//   - Compressed and uncompressed data sizes
//   - Number of streams
//   - Index sizes and efficiency metrics
//
// These stats are typically exposed via the /metrics endpoint for Prometheus scraping.
func (pt *partition) updateStats(ps *PartitionStats) {
	pt.ddb.updateStats(&ps.DatadbStats)
	pt.idb.updateStats(&ps.IndexdbStats)
}

// mustForceMerge triggers aggressive compaction of all data in this partition.
//
// COMPACTION BACKGROUND:
// Log data is written in small chunks (parts) during ingestion. Over time, these
// accumulate and become inefficient for queries. Compaction merges small parts
// into larger, better-organized parts:
//   - Improves query performance (fewer files to read)
//   - Improves compression (more data to compress together)
//   - Removes deleted data (garbage collection)
//
// Normal compaction runs automatically in the background with rate limits.
// Force merge bypasses those limits to immediately compact everything.
//
// Use cases:
//   - Preparing a partition for read-heavy workloads
//   - Maximizing compression before archiving
//   - Reclaiming space after large deletions
func (pt *partition) mustForceMerge() {
	pt.ddb.mustForceMergeAllParts()
}

// deleteRows removes log entries matching the specified search criteria.
//
// DELETION PROCESS:
//  1. Flush all pending data to make it visible for the deletion query
//  2. Convert storage-level search options to partition-specific options
//  3. Execute deletion in datadb (which handles the actual row removal)
//
// Note: Deletion is not immediate - rows are marked as deleted and actually
// removed during the next compaction. This is standard LSM-tree behavior.
//
// The stopCh allows cancellation of long-running deletions.
// Returns true if deletion completed, false if it was stopped.
func (pt *partition) deleteRows(sso *storageSearchOptions, stopCh <-chan struct{}) bool {
	// Make recently ingested rows visible for search, so they could be deleted.
	// Without this, data still in memory buffers wouldn't be found by the deletion query.
	pt.debugFlush()

	pso := pt.getSearchOptions(sso)
	return pt.ddb.deleteRows(pso, stopCh)
}

// ==================== Partition Naming Utilities ====================
//
// Partitions are named by their date in YYYYMMDD format (e.g., "20240115").
// These utilities convert between partition names and internal day representations.
//
// The "day" representation is the number of days since Unix epoch (1970-01-01).
// This makes date arithmetic simple (add/subtract days by integer operations).

// getPartitionDayFromName parses a partition name into a day number (days since Unix epoch).
//
// Example: "20240115" -> day number for January 15, 2024
//
// Returns an error if the name doesn't match the YYYYMMDD format.
func getPartitionDayFromName(name string) (int64, error) {
	t, err := time.Parse(partitionNameFormat, name)
	if err != nil {
		return 0, fmt.Errorf("cannot parse partition name %q; it must have the format YYYYMMDD: %w", name, err)
	}
	day := t.UTC().UnixNano() / nsecsPerDay
	return day, nil
}

// getPartitionNameFromDay converts a day number to a partition name.
//
// Example: day number for January 15, 2024 -> "20240115"
//
// All times are in UTC to ensure consistent naming across timezones.
func getPartitionNameFromDay(day int64) string {
	name := time.Unix(0, day*nsecsPerDay).UTC().Format(partitionNameFormat)
	return name
}

// partitionNameFormat defines the expected format for partition directory names.
// This follows Go's reference time format: Mon Jan 2 15:04:05 MST 2006
// "20060102" means: 4-digit year, 2-digit month, 2-digit day
const partitionNameFormat = "20060102"
