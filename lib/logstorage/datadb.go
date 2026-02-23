// Package logstorage provides the core storage engine for VictoriaLogs.
//
// ============== Datadb Overview ==============
//
// datadb is the data storage layer for VictoriaLogs, responsible for:
//   - Storing log entries in compressed blocks
//   - Managing the lifecycle of storage parts (in-memory, small, big)
//   - Performing background merges to optimize storage efficiency
//   - Handling data snapshots for backups
//
// ============== Storage Architecture ==============
//
// datadb uses a Log-Structured Merge-tree (LSM) inspired architecture with three tiers:
//
// 1. IN-MEMORY PARTS (fastest, most volatile)
//   - Newly ingested data is first stored in memory
//   - Periodically flushed to disk based on time or size thresholds
//   - Fast to write, but data could be lost on power failure before flush
//
// 2. SMALL PARTS (on disk, cached)
//   - Flushed in-memory parts become small parts
//   - Kept small to fit in OS page cache for fast reads
//   - Merged together when multiple small parts accumulate
//
// 3. BIG PARTS (on disk, large)
//   - Small parts that grow beyond the small threshold become big parts
//   - Optimized for sequential reads of large time ranges
//   - May be cached differently by the OS
//
// ============== Merge Process ==============
//
// Background merges are crucial for storage efficiency:
//
// WHY MERGE?
//   - Reduce number of files (fewer seeks during queries)
//   - Improve compression (more data = better compression ratios)
//   - Remove deleted rows (garbage collection)
//   - Reorganize data for better query patterns
//
// MERGE STRATEGY:
//   - Parts are selected for merge based on size similarity
//   - The merge multiplier (minMergeMultiplier = 1.7) ensures output part
//     is at least 1.7x the size of the largest input, minimizing write amplification
//   - Multiple concurrent mergers run based on CPU count
//
// ============== Data Flow ==============
//
// Ingestion:
//  1. Log entries arrive via mustAddRows()
//  2. Data is buffered in rowsBuffer (sharded by CPU)
//  3. When buffer is full, data is converted to in-memory part
//  4. In-memory parts are eventually flushed to disk
//
// Merge:
//  1. Background workers detect merge opportunities
//  2. Parts are selected based on optimal merge algorithm
//  3. Selected parts are read, merged, and rewritten
//  4. Old parts are atomically replaced with merged part
//
// ============== Thread Safety ==============
//
// datadb uses several synchronization mechanisms:
//   - partsLock: Protects the part lists (inmemoryParts, smallParts, bigParts)
//   - refCount on partWrapper: Tracks active readers, enables safe deletion
//   - stopCh: Signals background workers to stop during shutdown
//   - wg: WaitGroup to wait for background workers during shutdown
package logstorage

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/metrics"
)

// ==================== Storage Constants ====================
//
// These constants control the behavior of the storage system.

// maxBigPartSize limits the maximum size of a "big" part (1TB).
// This prevents parts from growing indefinitely, which would:
//   - Make compaction slower
//   - Increase memory usage during merge
//   - Make recovery from corruption more painful
const maxBigPartSize = 1e12

// maxInmemoryPartsPerPartition limits how many in-memory parts can exist.
// Too many in-memory parts indicate the merger can't keep up, potentially
// leading to memory pressure. This limit acts as a backpressure mechanism.
const maxInmemoryPartsPerPartition = 20

// defaultPartsToMerge is the optimal number of parts to merge at once.
// This value was determined empirically to minimize overhead.
// Too few: high write amplification
// Too many: slower merges, more memory usage
const defaultPartsToMerge = 15

// minMergeMultiplier ensures the output of a merge is significantly larger
// than the inputs. This reduces write amplification by ensuring each piece
// of data isn't rewritten too many times during its lifetime.
// Value 1.7 means: output_size >= 1.7 * max(input_sizes)
const minMergeMultiplier = 1.7

// ==================== Datadb Structure ====================

// datadb represents the data storage database for a partition.
// It manages the lifecycle of log data from ingestion to persistence.
type datadb struct {
	// rb is an in-memory buffer for incoming log rows.
	// It's sharded by CPU to minimize lock contention.
	// Rows accumulate here until converted to in-memory parts.
	rb rowsBuffer

	// mergeIdx generates unique directory names for merged parts.
	// Incremented atomically for each new part.
	mergeIdx atomic.Uint64

	// ==================== Merge Statistics ====================
	// These counters track merge operations for monitoring.

	inmemoryMergesTotal    atomic.Uint64 // Total in-memory merges performed
	inmemoryActiveMerges   atomic.Int64  // Currently active in-memory merges
	inmemoryMergeRowsTotal atomic.Uint64 // Total rows merged in-memory

	smallPartMergesTotal    atomic.Uint64 // Total small part merges performed
	smallPartActiveMerges   atomic.Int64  // Currently active small part merges
	smallPartMergeRowsTotal atomic.Uint64 // Total rows merged in small parts

	bigPartMergesTotal    atomic.Uint64 // Total big part merges performed
	bigPartActiveMerges   atomic.Int64  // Currently active big part merges
	bigPartMergeRowsTotal atomic.Uint64 // Total rows merged in big parts

	// ==================== Merge Metrics ====================
	// Prometheus metrics for monitoring merge performance.

	inmemoryPartMergeDuration *metrics.Summary
	inmemoryPartMergeBytes    *metrics.Summary
	smallPartMergeDuration    *metrics.Summary
	smallPartMergeBytes       *metrics.Summary
	bigPartMergeDuration      *metrics.Summary
	bigPartMergeBytes         *metrics.Summary

	// ==================== Core References ====================

	// pt is the parent partition that owns this datadb.
	pt *partition

	// path is the filesystem path to the datadb directory.
	path string

	// flushInterval is how long in-memory parts can stay in memory
	// before being flushed to disk. Longer = better batching, more risk.
	flushInterval time.Duration

	// ==================== Part Storage ====================

	// inmemoryParts contains parts stored only in RAM.
	// These are the most recently ingested data, not yet persisted.
	inmemoryParts []*partWrapper

	// smallParts contains file-based parts that fit in OS cache.
	// These have been flushed to disk but are small enough for caching.
	smallParts []*partWrapper

	// bigParts contains large file-based parts.
	// These are optimized for sequential reads of large data ranges.
	bigParts []*partWrapper

	// partsLock protects all part lists and related state.
	// Must be held when reading or modifying inmemoryParts, smallParts, bigParts.
	partsLock sync.Mutex

	// ==================== Lifecycle Management ====================

	// wg tracks background worker goroutines.
	// Used during shutdown to wait for workers to finish.
	wg sync.WaitGroup

	// stopCh signals background workers to stop.
	// Closed during shutdown; workers should check this periodically.
	stopCh chan struct{}
}

// partWrapper wraps a part with reference counting and lifecycle management.
// It enables safe concurrent access and deferred deletion.
type partWrapper struct {
	// refCount tracks active references to this part.
	// When it reaches zero and mustDrop is true, the part is deleted.
	// Incremented when a query starts using the part.
	// Decremented when the query finishes.
	refCount atomic.Int32

	// mustDrop indicates the part should be deleted when refCount hits zero.
	// Set when a part is replaced by a merged part.
	mustDrop atomic.Bool

	// p is the actual part data (either in-memory or file-based).
	p *part

	// mp holds the in-memory part data if this is an in-memory part.
	// nil for file-based parts.
	mp *inmemoryPart

	// isInMerge indicates this part is currently being merged.
	// Prevents the part from being selected for another merge.
	isInMerge bool

	// flushDeadline is when this in-memory part should be flushed to disk.
	// Helps ensure data durability within the flushInterval.
	flushDeadline time.Time
}

// incRef increments the reference count.
// Called when a query starts using this part.
func (pw *partWrapper) incRef() {
	pw.refCount.Add(1)
}

// decRef decrements the reference count.
// When count reaches zero, the part may be closed and deleted.
// Called when a query finishes using this part.
func (pw *partWrapper) decRef() {
	n := pw.refCount.Add(-1)
	if n > 0 {
		return
	}

	// Reference count hit zero - clean up if needed
	deletePath := ""
	if pw.mp == nil {
		// File-based part: delete if marked for deletion
		if pw.mustDrop.Load() {
			deletePath = pw.p.path
		}
	} else {
		// In-memory part: return to pool
		putInmemoryPart(pw.mp)
		pw.mp = nil
	}

	mustClosePart(pw.p)
	pw.p = nil

	if deletePath != "" {
		fs.MustRemoveDir(deletePath)
	}
}

// ==================== Datadb Lifecycle ====================

// mustCreateDatadb creates a new datadb directory structure on disk.
// This is called when creating a new partition.
func mustCreateDatadb(path string) {
	fs.MustMkdirFailIfExist(path)
	mustWritePartNames(path, nil)
	fs.MustSyncPathAndParentDir(path)
}

// mustOpenDatadb loads an existing datadb from disk and starts background workers.
//
// INITIALIZATION STEPS:
//  1. Read the list of existing parts from parts.json
//  2. Remove any orphaned directories (left from unclean shutdown)
//  3. Open each part file and categorize as small or big
//  4. Initialize metrics and start background workers
//
// RECOVERY: The function handles unclean shutdown recovery:
//   - Missing parts.json and no part dirs: recreated with an empty list
//   - Missing parts.json with existing part dirs: treated as corruption (panic)
//   - Extra directories: Removed (they were being created during crash)
func mustOpenDatadb(pt *partition, path string, flushInterval time.Duration) *datadb {
	partNames := mustReadPartNames(path)
	mustRemoveUnusedDirs(path, partNames)

	var smallParts []*partWrapper
	var bigParts []*partWrapper
	for _, partName := range partNames {
		// Make sure the partName exists on disk.
		// If it is missing, then manual action from the user is needed,
		// since this is unexpected state, which cannot occur under normal operation,
		// including unclean shutdown.
		partPath := filepath.Join(path, partName)
		if !fs.IsPathExist(partPath) {
			partsFile := filepath.Join(path, partsFilename)
			logger.Panicf("FATAL: part %q is listed in %q, but is missing on disk; ensure %q contents is not corrupted; remove %q from %q in order to fix this error",
				partPath, partsFile, partsFile, partPath, partsFile)
		}

		p := mustOpenFilePart(pt, partPath)
		pw := newPartWrapper(p, nil, time.Time{})
		// Categorize by size: big parts are larger than what fits in memory
		if p.ph.CompressedSizeBytes > getMaxInmemoryPartSize() {
			bigParts = append(bigParts, pw)
		} else {
			smallParts = append(smallParts, pw)
		}
	}

	ddb := &datadb{
		pt:            pt,
		flushInterval: flushInterval,

		inmemoryPartMergeDuration: metrics.GetOrCreateSummary(`vl_merge_duration_seconds{type="storage/inmemory"}`),
		inmemoryPartMergeBytes:    metrics.GetOrCreateSummary(`vl_merge_bytes{type="storage/inmemory"}`),
		smallPartMergeDuration:    metrics.GetOrCreateSummary(`vl_merge_duration_seconds{type="storage/small"}`),
		smallPartMergeBytes:       metrics.GetOrCreateSummary(`vl_merge_bytes{type="storage/small"}`),
		bigPartMergeDuration:      metrics.GetOrCreateSummary(`vl_merge_duration_seconds{type="storage/big"}`),
		bigPartMergeBytes:         metrics.GetOrCreateSummary(`vl_merge_bytes{type="storage/big"}`),

		path:       path,
		smallParts: smallParts,
		bigParts:   bigParts,
		stopCh:     make(chan struct{}),
	}
	ddb.rb.init(&ddb.wg, ddb.mustFlushLogRows)
	ddb.mergeIdx.Store(uint64(time.Now().UnixNano()))

	ddb.startBackgroundWorkers()

	return ddb
}

// startBackgroundWorkers launches the background goroutines for merges and flushing.
func (ddb *datadb) startBackgroundWorkers() {
	// Start file parts mergers, so they could start merging unmerged parts if needed.
	// There is no need in starting in-memory parts mergers, since there are no in-memory parts yet.
	ddb.startSmallPartsMergers()
	ddb.startBigPartsMergers()

	ddb.startInmemoryPartsFlusher()
}

// ==================== Concurrency Control ====================
//
// These channels limit the number of concurrent merge operations to prevent
// resource exhaustion. The limit is based on CPU count.

var (
	inmemoryPartsConcurrencyCh = make(chan struct{}, cgroup.AvailableCPUs())
	smallPartsConcurrencyCh    = make(chan struct{}, cgroup.AvailableCPUs())
	bigPartsConcurrencyCh      = make(chan struct{}, cgroup.AvailableCPUs())
)

func (ddb *datadb) startSmallPartsMergers() {
	ddb.partsLock.Lock()
	for i := 0; i < cap(smallPartsConcurrencyCh); i++ {
		ddb.startSmallPartsMergerLocked()
	}
	ddb.partsLock.Unlock()
}

func (ddb *datadb) startBigPartsMergers() {
	ddb.partsLock.Lock()
	for i := 0; i < cap(bigPartsConcurrencyCh); i++ {
		ddb.startBigPartsMergerLocked()
	}
	ddb.partsLock.Unlock()
}

func (ddb *datadb) startInmemoryPartsMergerLocked() {
	if needStop(ddb.stopCh) {
		return
	}
	ddb.wg.Go(ddb.inmemoryPartsMerger)
}

func (ddb *datadb) startSmallPartsMergerLocked() {
	if needStop(ddb.stopCh) {
		return
	}
	ddb.wg.Go(ddb.smallPartsMerger)
}

func (ddb *datadb) startBigPartsMergerLocked() {
	if needStop(ddb.stopCh) {
		return
	}
	ddb.wg.Go(ddb.bigPartsMerger)
}

func (ddb *datadb) startInmemoryPartsFlusher() {
	ddb.wg.Go(ddb.inmemoryPartsFlusher)
}

// ==================== Background Workers ====================

// inmemoryPartsFlusher periodically flushes in-memory parts to disk.
// This ensures data durability even if the merge process is slow.
func (ddb *datadb) inmemoryPartsFlusher() {
	// Do not add jitter to d in order to guarantee the flush interval
	ticker := time.NewTicker(ddb.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ddb.stopCh:
			return
		case <-ticker.C:
			ddb.mustFlushInmemoryPartsToFiles(false)
		}
	}
}

// mustFlushInmemoryPartsToFiles flushes eligible in-memory parts to disk.
// If isFinal is true, all parts are flushed (used during shutdown/snapshot).
func (ddb *datadb) mustFlushInmemoryPartsToFiles(isFinal bool) {
	currentTime := time.Now()
	var pws []*partWrapper

	ddb.partsLock.Lock()
	for _, pw := range ddb.inmemoryParts {
		if !pw.isInMerge && (isFinal || pw.flushDeadline.Before(currentTime)) {
			pw.isInMerge = true
			pws = append(pws, pw)
		}
	}
	ddb.partsLock.Unlock()

	ddb.mustMergePartsToFiles(pws)
}

// mustMergePartsToFiles merges the given in-memory parts to file-based parts.
// Uses parallel merging for efficiency.
//
// WHY PARALLEL? When many in-memory parts need flushing (e.g., after high-throughput
// ingestion), processing them one at a time would be slow. Parallel merging uses
// all available CPUs to speed up the flush process.
func (ddb *datadb) mustMergePartsToFiles(pws []*partWrapper) {
	wg := getWaitGroup()
	for len(pws) > 0 {
		// Select optimal set of parts to merge together
		// This uses the same algorithm as background merges (see appendPartsToMerge)
		pwsToMerge, pwsRemaining := getPartsForOptimalMerge(pws)

		// Acquire a slot in the concurrency limiter
		// This blocks if all CPU slots are in use, preventing resource exhaustion
		inmemoryPartsConcurrencyCh <- struct{}{}

		wg.Go(func() {
			ddb.mustMergeParts(pwsToMerge, true)
			// Release the concurrency slot when done
			<-inmemoryPartsConcurrencyCh
		})

		pws = pwsRemaining
	}
	wg.Wait()
	putWaitGroup(wg)
}

// getPartsForOptimalMerge selects parts for an optimal merge operation.
// Returns the parts to merge and the remaining parts.
func getPartsForOptimalMerge(pws []*partWrapper) ([]*partWrapper, []*partWrapper) {
	pwsToMerge := appendPartsToMerge(nil, pws, math.MaxUint64)
	if len(pwsToMerge) == 0 {
		return pws, nil
	}

	m := partsToMap(pwsToMerge)
	pwsRemaining := make([]*partWrapper, 0, len(pws)-len(pwsToMerge))
	for _, pw := range pws {
		if _, ok := m[pw]; !ok {
			pwsRemaining = append(pwsRemaining, pw)
		}
	}

	// Clear references to pws items, so they could be reclaimed faster by Go GC.
	for i := range pws {
		pws[i] = nil
	}

	return pwsToMerge, pwsRemaining
}

// ==================== WaitGroup Pool ====================

func getWaitGroup() *sync.WaitGroup {
	v := wgPool.Get()
	if v == nil {
		return &sync.WaitGroup{}
	}
	return v.(*sync.WaitGroup)
}

func putWaitGroup(wg *sync.WaitGroup) {
	wgPool.Put(wg)
}

var wgPool sync.Pool

// ==================== Background Mergers ====================
//
// These functions run continuously, looking for merge opportunities.

// inmemoryPartsMerger merges in-memory parts together to reduce part count.
func (ddb *datadb) inmemoryPartsMerger() {
	for {
		if needStop(ddb.stopCh) {
			return
		}
		maxOutBytes := ddb.getMaxBigPartSize()

		ddb.partsLock.Lock()
		pws := getPartsToMergeLocked(ddb.inmemoryParts, maxOutBytes)
		ddb.partsLock.Unlock()

		if len(pws) == 0 {
			// Nothing to merge
			return
		}

		inmemoryPartsConcurrencyCh <- struct{}{}
		ddb.mustMergeParts(pws, false)
		<-inmemoryPartsConcurrencyCh
	}
}

// smallPartsMerger merges small file-based parts together.
func (ddb *datadb) smallPartsMerger() {
	for {
		if needStop(ddb.stopCh) {
			return
		}
		maxOutBytes := ddb.getMaxBigPartSize()

		ddb.partsLock.Lock()
		pws := getPartsToMergeLocked(ddb.smallParts, maxOutBytes)
		ddb.partsLock.Unlock()

		if len(pws) == 0 {
			// Nothing to merge
			return
		}

		smallPartsConcurrencyCh <- struct{}{}
		ddb.mustMergeParts(pws, false)
		<-smallPartsConcurrencyCh
	}
}

// bigPartsMerger merges big file-based parts together.
func (ddb *datadb) bigPartsMerger() {
	for {
		if needStop(ddb.stopCh) {
			return
		}
		maxOutBytes := ddb.getMaxBigPartSize()

		ddb.partsLock.Lock()
		pws := getPartsToMergeLocked(ddb.bigParts, maxOutBytes)
		ddb.partsLock.Unlock()

		if len(pws) == 0 {
			// Nothing to merge
			return
		}

		bigPartsConcurrencyCh <- struct{}{}
		ddb.mustMergeParts(pws, false)
		<-bigPartsConcurrencyCh
	}
}

// getPartsToMergeLocked selects parts for merging based on the optimal merge algorithm.
// Must be called with partsLock held.
func getPartsToMergeLocked(pws []*partWrapper, maxOutBytes uint64) []*partWrapper {
	pwsRemaining := make([]*partWrapper, 0, len(pws))
	for _, pw := range pws {
		if !pw.isInMerge {
			pwsRemaining = append(pwsRemaining, pw)
		}
	}

	pwsToMerge := appendPartsToMerge(nil, pwsRemaining, maxOutBytes)

	for _, pw := range pwsToMerge {
		if pw.isInMerge {
			logger.Panicf("BUG: partWrapper.isInMerge cannot be set")
		}
		pw.isInMerge = true
	}

	return pwsToMerge
}

// assertIsInMerge verifies that all parts are marked as being in a merge.
func assertIsInMerge(pws []*partWrapper) {
	for _, pw := range pws {
		if !pw.isInMerge {
			logger.Panicf("BUG: partWrapper.isInMerge unexpectedly set to false")
		}
	}
}

// ==================== Core Merge Logic ====================

// mustMergeParts merges multiple parts into a single resulting part.
// This is the main entry point for all merge operations.
//
// PARAMETERS:
//   - pws: Parts to merge (must have isInMerge set to true)
//   - isFinal: If true, merge cannot be interrupted and must complete
//
// The merge may not complete if:
//   - stopCh is closed (shutdown requested)
//   - Not enough disk space (unless isFinal)
func (ddb *datadb) mustMergeParts(pws []*partWrapper, isFinal bool) {
	_ = ddb.mustMergePartsInternal(pws, isFinal, nil, ddb.stopCh)
}

// mustMergePartsInternal performs the actual merge operation.
//
// MERGE PROCESS:
//  1. Determine the destination part type (inmemory, small, or big)
//  2. Reserve disk space if writing to file
//  3. Open stream readers for all source parts
//  4. Create stream writer for destination
//  5. Merge-sort all data from sources to destination
//  6. Write metadata and sync to disk
//  7. Atomically swap old parts with new part
//
// Returns false if merge was interrupted or couldn't complete.
//
// WHY ATOMIC SWAP? During a merge, we're reading from old parts and writing to a new part.
// If we crash mid-merge, we need to ensure either:
// - The old parts are still valid (merge didn't complete)
// - The new part is complete and old parts can be deleted
// The atomic swap (under partsLock) guarantees we never have a state where both
// old and new are partially visible.
func (ddb *datadb) mustMergePartsInternal(pws []*partWrapper, isFinal bool, dropFilter *partitionSearchOptions, stopCh <-chan struct{}) bool {
	if len(pws) == 0 {
		// Nothing to merge.
		return true
	}

	assertIsInMerge(pws)
	defer ddb.releasePartsToMerge(pws)

	startTime := time.Now()

	dstPartType := ddb.getDstPartType(pws, isFinal)
	if dstPartType != partInmemory {
		// Make sure there is enough disk space for performing the merge
		partsSize := getCompressedSize(pws)
		if tryReserveDiskSpace(ddb.path, partsSize) {
			defer releaseDiskSpace(partsSize)
		} else {
			if !isFinal {
				// There is no enough disk space for performing the non-final merge.
				return false
			}
			// Try performing final merge even if there is no enough disk space
			// in order to persist in-memory data to disk.
			// It is better to crash on out of memory error in this case.
		}
	}

	// Update merge statistics
	switch dstPartType {
	case partInmemory:
		ddb.inmemoryMergesTotal.Add(1)
		ddb.inmemoryActiveMerges.Add(1)
		defer ddb.inmemoryActiveMerges.Add(-1)
	case partSmall:
		ddb.smallPartMergesTotal.Add(1)
		ddb.smallPartActiveMerges.Add(1)
		defer ddb.smallPartActiveMerges.Add(-1)
	case partBig:
		ddb.bigPartMergesTotal.Add(1)
		ddb.bigPartActiveMerges.Add(1)
		defer ddb.bigPartActiveMerges.Add(-1)
	default:
		logger.Panicf("BUG: unknown partType=%d", dstPartType)
	}

	// Initialize destination paths.
	mergeIdx := ddb.nextMergeIdx()
	dstPartPath := ddb.getDstPartPath(dstPartType, mergeIdx)

	// Fast path: single in-memory part being flushed to disk
	if isFinal && len(pws) == 1 && pws[0].mp != nil {
		mp := pws[0].mp
		mp.MustStoreToDisk(dstPartPath)
		pwNew := ddb.openCreatedPart(&mp.ph, pws, nil, dstPartPath)
		ddb.swapSrcWithDstParts(pws, pwNew, dstPartType)
		ddb.updateMergeMetrics(dstPartType, mp.ph.RowsCount, startTime, mp.ph.CompressedSizeBytes)
		return true
	}

	// Prepare blockStreamReaders for source parts.
	bsrs := mustOpenBlockStreamReaders(pws)

	// Prepare BlockStreamWriter for destination part.
	srcSize := uint64(0)
	srcRowsCount := uint64(0)
	srcBlocksCount := uint64(0)
	for _, pw := range pws {
		ph := &pw.p.ph
		srcSize += ph.CompressedSizeBytes
		srcRowsCount += ph.RowsCount
		srcBlocksCount += ph.BlocksCount
	}
	bsw := getBlockStreamWriter()
	var mpNew *inmemoryPart
	if dstPartType == partInmemory {
		mpNew = getInmemoryPart()
		bsw.MustInitForInmemoryPart(mpNew)
	} else {
		nocache := dstPartType == partBig
		bsw.MustInitForFilePart(dstPartPath, nocache)
	}

	// Merge source parts to destination part.
	var ph partHeader
	if isFinal {
		// The final merge shouldn't be stopped even if stopCh is closed.
		stopCh = nil
	}
	mustMergeBlockStreams(&ph, ddb.pt.idb, bsw, bsrs, dropFilter, stopCh)
	putBlockStreamWriter(bsw)
	for _, bsr := range bsrs {
		putBlockStreamReader(bsr)
	}

	// Persist partHeader for destination part after the merge.
	if mpNew != nil {
		mpNew.ph = ph
	} else {
		ph.mustWriteMetadata(dstPartPath)
		// Make sure the created part directory contents is synced and visible in case of unclean shutdown.
		fs.MustSyncPathAndParentDir(dstPartPath)
	}
	if needStop(stopCh) {
		// Remove incomplete destination part
		if dstPartType != partInmemory {
			fs.MustRemoveDir(dstPartPath)
		}
		return false
	}

	// Atomically swap the source parts with the newly created part.
	pwNew := ddb.openCreatedPart(&ph, pws, mpNew, dstPartPath)

	dstSize := uint64(0)
	dstRowsCount := uint64(0)
	dstBlocksCount := uint64(0)
	if pwNew != nil {
		pDst := pwNew.p
		dstSize = pDst.ph.CompressedSizeBytes
		dstRowsCount = pDst.ph.RowsCount
		dstBlocksCount = pDst.ph.BlocksCount
	}

	ddb.swapSrcWithDstParts(pws, pwNew, dstPartType)
	ddb.updateMergeMetrics(dstPartType, srcRowsCount, startTime, dstSize)

	d := time.Since(startTime)
	if d <= time.Minute {
		return true
	}

	// Log stats for long merges.
	durationSecs := d.Seconds()
	rowsPerSec := int(float64(srcRowsCount) / durationSecs)
	logger.Infof("merged (%d parts, %d rows, %d blocks, %d bytes) into (1 part, %d rows, %d blocks, %d bytes) in %.3f seconds at %d rows/sec to %q",
		len(pws), srcRowsCount, srcBlocksCount, srcSize, dstRowsCount, dstBlocksCount, dstSize, durationSecs, rowsPerSec, dstPartPath)

	return true
}

// updateMergeMetrics records merge performance metrics.
func (ddb *datadb) updateMergeMetrics(partType partType, srcRowCount uint64, startTime time.Time, dstSize uint64) {
	switch partType {
	case partInmemory:
		ddb.inmemoryMergeRowsTotal.Add(srcRowCount)
		ddb.inmemoryPartMergeDuration.UpdateDuration(startTime)
		ddb.inmemoryPartMergeBytes.Update(float64(dstSize))
	case partSmall:
		ddb.smallPartMergeRowsTotal.Add(srcRowCount)
		ddb.smallPartMergeDuration.UpdateDuration(startTime)
		ddb.smallPartMergeBytes.Update(float64(dstSize))
	case partBig:
		ddb.bigPartMergeRowsTotal.Add(srcRowCount)
		ddb.bigPartMergeDuration.UpdateDuration(startTime)
		ddb.bigPartMergeBytes.Update(float64(dstSize))
	}
}

func (ddb *datadb) nextMergeIdx() uint64 {
	return ddb.mergeIdx.Add(1)
}

// partType identifies the storage tier for a part.
type partType int

var (
	partInmemory = partType(0) // Stored only in RAM
	partSmall    = partType(1) // File-based, small enough to cache
	partBig      = partType(2) // File-based, large
)

// getDstPartType determines where the merged result should be stored.
func (ddb *datadb) getDstPartType(pws []*partWrapper, isFinal bool) partType {
	dstPartSize := getCompressedSize(pws)
	if dstPartSize > ddb.getMaxSmallPartSize() {
		return partBig
	}
	if isFinal || dstPartSize > getMaxInmemoryPartSize() {
		return partSmall
	}
	if !areAllInmemoryParts(pws) {
		// If at least a single source part is located in file,
		// then the destination part must be in file for durability reasons.
		return partSmall
	}
	return partInmemory
}

// getDstPartPath returns the filesystem path for a new part.
func (ddb *datadb) getDstPartPath(dstPartType partType, mergeIdx uint64) string {
	ptPath := ddb.path
	dstPartPath := ""
	if dstPartType != partInmemory {
		dstPartPath = filepath.Join(ptPath, fmt.Sprintf("%016X", mergeIdx))
	}
	return dstPartPath
}

// openCreatedPart opens a newly created part after merge completion.
func (ddb *datadb) openCreatedPart(ph *partHeader, pws []*partWrapper, mpNew *inmemoryPart, dstPartPath string) *partWrapper {
	// Open the created part.
	if ph.RowsCount == 0 {
		// The created part is empty. Remove it
		if mpNew == nil {
			fs.MustRemoveDir(dstPartPath)
		}
		return nil
	}
	var p *part
	var flushDeadline time.Time
	if mpNew != nil {
		// Open the created part from memory.
		p = mustOpenInmemoryPart(ddb.pt, mpNew)
		flushDeadline = ddb.getFlushToDiskDeadline(pws)
	} else {
		// Open the created part from disk.
		p = mustOpenFilePart(ddb.pt, dstPartPath)
	}
	return newPartWrapper(p, mpNew, flushDeadline)
}

// ==================== Data Ingestion ====================

// mustAddRows adds log rows to the in-memory buffer.
// This is the main entry point for data ingestion at the datadb level.
func (ddb *datadb) mustAddRows(lr *LogRows) {
	ddb.rb.mustAddRows(lr)
}

// ==================== Rows Buffer ====================
//
// rowsBuffer is a sharded in-memory buffer for incoming log rows.
// Sharding by CPU reduces lock contention during high-throughput ingestion.

type rowsBuffer struct {
	shards  []rowsBufferShard
	nextIdx atomic.Uint64
}

// Len returns the total number of rows across all shards.
func (rb *rowsBuffer) Len() uint64 {
	shards := rb.shards
	n := uint64(0)
	for i := range shards {
		shard := &shards[i]
		shard.mu.Lock()
		if shard.lr != nil {
			n += uint64(shard.lr.Len())
		}
		shard.mu.Unlock()
	}

	return n
}

// init creates the sharded buffer with one shard per CPU.
func (rb *rowsBuffer) init(wg *sync.WaitGroup, flushFunc func(lr *logRows)) {
	shards := make([]rowsBufferShard, cgroup.AvailableCPUs())
	for i := range shards {
		shard := &shards[i]
		shard.wg = wg
		shard.flushFunc = flushFunc
	}
	rb.shards = shards
}

// rowsBufferShard is a single shard of the rows buffer.
type rowsBufferShard struct {
	wg        *sync.WaitGroup // Shared with datadb
	flushFunc func(lr *logRows)

	mu         sync.Mutex
	lr         *logRows
	flushTimer *time.Timer

	// padding for preventing false sharing between shards
	_ [atomicutil.CacheLineSize]byte
}

// flush forces all shards to flush their data.
func (rb *rowsBuffer) flush() {
	shards := rb.shards
	for i := range shards {
		shard := &shards[i]
		shard.mu.Lock()
		shard.flushLocked()
		shard.mu.Unlock()
	}
}

// mustAddRows adds rows to a shard using round-robin distribution.
func (rb *rowsBuffer) mustAddRows(lr *LogRows) {
	if len(lr.streamIDs) == 0 {
		return
	}

	shards := rb.shards
	idx := rb.nextIdx.Add(1) % uint64(len(shards))
	shard := &shards[idx]

	shard.mu.Lock()
	if shard.flushTimer == nil {
		// Set up a timer to flush this shard after 1 second
		shard.wg.Add(1)
		shard.flushTimer = time.AfterFunc(time.Second, func() {
			defer shard.wg.Done()

			shard.mu.Lock()
			shard.flushLocked()
			shard.mu.Unlock()
		})
	}
	if shard.lr == nil {
		shard.lr = getLogRows()
	}
	shard.lr.mustAddRows(lr)
	if shard.lr.needFlush() {
		shard.flushLocked()
	}
	shard.mu.Unlock()
}

// flushLocked flushes the shard's data to storage.
// Must be called with mu held.
func (shard *rowsBufferShard) flushLocked() {
	if shard.flushTimer != nil {
		if shard.flushTimer.Stop() {
			shard.wg.Done()
		}
		shard.flushTimer = nil
	}

	if shard.lr != nil {
		shard.flushFunc(shard.lr)
		putLogRows(shard.lr)
		shard.lr = nil
	}
}

// mustFlushLogRows converts accumulated log rows into an in-memory part.
func (ddb *datadb) mustFlushLogRows(lr *logRows) {
	inmemoryPartsConcurrencyCh <- struct{}{}
	mp := getInmemoryPart()
	mp.mustInitFromRows(lr)
	p := mustOpenInmemoryPart(ddb.pt, mp)
	<-inmemoryPartsConcurrencyCh

	flushDeadline := time.Now().Add(ddb.flushInterval)
	pw := newPartWrapper(p, mp, flushDeadline)

	ddb.partsLock.Lock()
	ddb.inmemoryParts = append(ddb.inmemoryParts, pw)
	ddb.startInmemoryPartsMergerLocked()
	ddb.partsLock.Unlock()
}

// DatadbStats contains various stats for datadb.
type DatadbStats struct {
	// InmemoryMergesCount is the number of inmemory merges performed in the given datadb.
	InmemoryMergesCount uint64

	// ActiveInmemoryMerges is the number of currently active inmemory merges performed by the given datadb.
	ActiveInmemoryMerges uint64

	// InmemoryRowsMerged is the number of rows merged to inmemory parts.
	InmemoryRowsMerged uint64

	// SmallMergesCount is the number of small file merges performed in the given datadb.
	SmallMergesCount uint64

	// ActiveSmallMerges is the number of currently active small file merges performed by the given datadb.
	ActiveSmallMerges uint64

	// SmallRowsMerged is the number of rows merged to small parts.
	SmallRowsMerged uint64

	// BigMergesCount is the number of big file merges performed in the given datadb.
	BigMergesCount uint64

	// ActiveBigMerges is the number of currently active big file merges performed by the given datadb.
	ActiveBigMerges uint64

	// BigRowsMerged is the number of rows merged to big parts.
	BigRowsMerged uint64

	// PendingRows is the number of rows, which weren't flushed to searchable part yet.
	PendingRows uint64

	// InmemoryRowsCount is the number of rows, which weren't flushed to disk yet.
	InmemoryRowsCount uint64

	// SmallPartRowsCount is the number of rows stored on disk in small parts.
	SmallPartRowsCount uint64

	// BigPartRowsCount is the number of rows stored on disk in big parts.
	BigPartRowsCount uint64

	// InmemoryParts is the number of in-memory parts, which weren't flushed to disk yet.
	InmemoryParts uint64

	// SmallParts is the number of file-based small parts stored on disk.
	SmallParts uint64

	// BigParts is the number of file-based big parts stored on disk.
	BigParts uint64

	// InmemoryBlocks is the number of in-memory blocks, which weren't flushed to disk yet.
	InmemoryBlocks uint64

	// SmallPartBlocks is the number of file-based small blocks stored on disk.
	SmallPartBlocks uint64

	// BigPartBlocks is the number of file-based big blocks stored on disk.
	BigPartBlocks uint64

	// CompressedInmemorySize is the size of compressed data stored in memory.
	CompressedInmemorySize uint64

	// CompressedSmallPartSize is the size of compressed small parts data stored on disk.
	CompressedSmallPartSize uint64

	// CompressedBigPartSize is the size of compressed big data stored on disk.
	CompressedBigPartSize uint64

	// UncompressedInmemorySize is the size of uncompressed data stored in memory.
	UncompressedInmemorySize uint64

	// UncompressedSmallPartSize is the size of uncompressed small data stored on disk.
	UncompressedSmallPartSize uint64

	// UncompressedBigPartSize is the size of uncompressed big data stored on disk.
	UncompressedBigPartSize uint64
}

func (s *DatadbStats) reset() {
	*s = DatadbStats{}
}

// RowsCount returns the number of rows stored in datadb.
func (s *DatadbStats) RowsCount() uint64 {
	return s.InmemoryRowsCount + s.SmallPartRowsCount + s.BigPartRowsCount
}

// updateStats updates s with ddb stats.
func (ddb *datadb) updateStats(s *DatadbStats) {
	s.InmemoryMergesCount += ddb.inmemoryMergesTotal.Load()
	s.ActiveInmemoryMerges += uint64(ddb.inmemoryActiveMerges.Load())
	s.InmemoryRowsMerged += ddb.inmemoryMergeRowsTotal.Load()
	s.SmallMergesCount += ddb.smallPartMergesTotal.Load()
	s.ActiveSmallMerges += uint64(ddb.smallPartActiveMerges.Load())
	s.SmallRowsMerged += ddb.smallPartMergeRowsTotal.Load()
	s.BigMergesCount += ddb.bigPartMergesTotal.Load()
	s.ActiveBigMerges += uint64(ddb.bigPartActiveMerges.Load())
	s.BigRowsMerged += ddb.bigPartMergeRowsTotal.Load()

	s.PendingRows = ddb.rb.Len()

	ddb.partsLock.Lock()

	s.InmemoryRowsCount += getRowsCount(ddb.inmemoryParts)
	s.SmallPartRowsCount += getRowsCount(ddb.smallParts)
	s.BigPartRowsCount += getRowsCount(ddb.bigParts)

	s.InmemoryParts += uint64(len(ddb.inmemoryParts))
	s.SmallParts += uint64(len(ddb.smallParts))
	s.BigParts += uint64(len(ddb.bigParts))

	s.InmemoryBlocks += getBlocksCount(ddb.inmemoryParts)
	s.SmallPartBlocks += getBlocksCount(ddb.smallParts)
	s.BigPartBlocks += getBlocksCount(ddb.bigParts)

	s.CompressedInmemorySize += getCompressedSize(ddb.inmemoryParts)
	s.CompressedSmallPartSize += getCompressedSize(ddb.smallParts)
	s.CompressedBigPartSize += getCompressedSize(ddb.bigParts)

	s.UncompressedInmemorySize += getUncompressedSize(ddb.inmemoryParts)
	s.UncompressedSmallPartSize += getUncompressedSize(ddb.smallParts)
	s.UncompressedBigPartSize += getUncompressedSize(ddb.bigParts)

	ddb.partsLock.Unlock()
}

// getMinMaxTimestampsFast returns min and max timestamps across parts in ddb.
func (ddb *datadb) getMinMaxTimestamps() (int64, int64) {
	minTs := int64(math.MaxInt64)
	maxTs := int64(math.MinInt64)

	updateMinMaxTimestamps := func(pws []*partWrapper) {
		for _, pw := range pws {
			ph := &pw.p.ph
			if ph.MinTimestamp < minTs {
				minTs = ph.MinTimestamp
			}
			if ph.MaxTimestamp > maxTs {
				maxTs = ph.MaxTimestamp
			}
		}
	}

	ddb.partsLock.Lock()
	updateMinMaxTimestamps(ddb.inmemoryParts)
	updateMinMaxTimestamps(ddb.smallParts)
	updateMinMaxTimestamps(ddb.bigParts)
	ddb.partsLock.Unlock()

	return minTs, maxTs
}

// debugFlush() makes sure that the recently ingested data is available for search.
func (ddb *datadb) debugFlush() {
	ddb.rb.flush()
}

func (ddb *datadb) mustCreateSnapshotAt(dstDir string) {
	fs.MustMkdirFailIfExist(dstDir)

	// flush in-memory parts before making a snapshot
	ddb.mustFlushInmemoryPartsToFiles(true)

	// Get all the file-based parts
	ddb.partsLock.Lock()
	pws := make([]*partWrapper, 0, len(ddb.smallParts)+len(ddb.bigParts))
	pws = append(pws, ddb.smallParts...)
	pws = append(pws, ddb.bigParts...)
	for _, pw := range pws {
		pw.incRef()
	}
	ddb.partsLock.Unlock()

	// Write parts.json file
	partNames := getPartNames(pws)
	mustWritePartNames(dstDir, partNames)

	// Make hardlinks for pws at dstDir
	for _, pw := range pws {
		srcPartPath := pw.p.path
		dstPartPath := filepath.Join(dstDir, filepath.Base(srcPartPath))
		fs.MustHardLinkFiles(srcPartPath, dstPartPath)
	}

	// Release all the file-based parts
	for _, pw := range pws {
		pw.decRef()
	}

	// Sync dstDir contents.
	// The parent dir for the dstDir must be synced by the caller.
	fs.MustSyncPath(dstDir)
}

func (ddb *datadb) swapSrcWithDstParts(pws []*partWrapper, pwNew *partWrapper, dstPartType partType) {
	// Atomically unregister old parts and add new part to pt.
	partsToRemove := partsToMap(pws)

	removedInmemoryParts := 0
	removedSmallParts := 0
	removedBigParts := 0

	func() {
		// Prevent from deadlock mentioned at https://github.com/VictoriaMetrics/VictoriaLogs/issues/1020#issuecomment-3763912067
		ddb.partsLock.Lock()
		defer ddb.partsLock.Unlock()

		ddb.inmemoryParts, removedInmemoryParts = removeParts(ddb.inmemoryParts, partsToRemove)
		ddb.smallParts, removedSmallParts = removeParts(ddb.smallParts, partsToRemove)
		ddb.bigParts, removedBigParts = removeParts(ddb.bigParts, partsToRemove)

		if pwNew != nil {
			switch dstPartType {
			case partInmemory:
				ddb.inmemoryParts = append(ddb.inmemoryParts, pwNew)
				ddb.startInmemoryPartsMergerLocked()
			case partSmall:
				ddb.smallParts = append(ddb.smallParts, pwNew)
				ddb.startSmallPartsMergerLocked()
			case partBig:
				ddb.bigParts = append(ddb.bigParts, pwNew)
				ddb.startBigPartsMergerLocked()
			default:
				logger.Panicf("BUG: unknown partType=%d", dstPartType)
			}
		}

		// Atomically store the updated list of file-based parts on disk.
		// This must be performed under partsLock in order to prevent from races
		// when multiple concurrently running goroutines update the list.
		if removedSmallParts > 0 || removedBigParts > 0 || (pwNew != nil && dstPartType != partInmemory) {
			smallPartNames := getPartNames(ddb.smallParts)
			bigPartNames := getPartNames(ddb.bigParts)
			partNames := append(smallPartNames, bigPartNames...)
			mustWritePartNames(ddb.path, partNames)
		}
	}()

	removedParts := removedInmemoryParts + removedSmallParts + removedBigParts
	if removedParts != len(partsToRemove) {
		logger.Panicf("BUG: unexpected number of parts removed; got %d, want %d", removedParts, len(partsToRemove))
	}

	// Mark old parts as must be deleted and decrement reference count, so they are eventually closed and deleted.
	for _, pw := range pws {
		pw.mustDrop.Store(true)
		pw.decRef()
	}
}

func partsToMap(pws []*partWrapper) map[*partWrapper]struct{} {
	m := make(map[*partWrapper]struct{}, len(pws))
	for _, pw := range pws {
		m[pw] = struct{}{}
	}
	if len(m) != len(pws) {
		logger.Panicf("BUG: %d duplicate parts found out of %d parts", len(pws)-len(m), len(pws))
	}
	return m
}

func removeParts(pws []*partWrapper, partsToRemove map[*partWrapper]struct{}) ([]*partWrapper, int) {
	dst := pws[:0]
	for _, pw := range pws {
		if _, ok := partsToRemove[pw]; !ok {
			dst = append(dst, pw)
		}
	}
	for i := len(dst); i < len(pws); i++ {
		pws[i] = nil
	}
	return dst, len(pws) - len(dst)
}

func mustOpenBlockStreamReaders(pws []*partWrapper) []*blockStreamReader {
	bsrs := make([]*blockStreamReader, 0, len(pws))
	for _, pw := range pws {
		bsr := getBlockStreamReader()
		if pw.mp != nil {
			bsr.MustInitFromInmemoryPart(pw.mp)
		} else {
			bsr.MustInitFromFilePart(pw.p.path)
		}
		bsrs = append(bsrs, bsr)
	}
	return bsrs
}

func newPartWrapper(p *part, mp *inmemoryPart, flushDeadline time.Time) *partWrapper {
	pw := &partWrapper{
		p:  p,
		mp: mp,

		flushDeadline: flushDeadline,
	}

	// Increase reference counter for newly created part - it is decreased when the part
	// is removed from the list of open parts.
	pw.incRef()

	return pw
}

func (ddb *datadb) getFlushToDiskDeadline(pws []*partWrapper) time.Time {
	d := time.Now().Add(ddb.flushInterval)
	for _, pw := range pws {
		if pw.mp != nil && pw.flushDeadline.Before(d) {
			d = pw.flushDeadline
		}
	}
	return d
}

func getMaxInmemoryPartSize() uint64 {
	// Allocate 10% of allowed memory for in-memory parts.
	n := uint64(0.1 * float64(memory.Allowed()) / maxInmemoryPartsPerPartition)
	if n < 1e6 {
		n = 1e6
	}
	return n
}

func areAllInmemoryParts(pws []*partWrapper) bool {
	for _, pw := range pws {
		if pw.mp == nil {
			return false
		}
	}
	return true
}

func (ddb *datadb) releasePartsToMerge(pws []*partWrapper) {
	ddb.partsLock.Lock()
	for _, pw := range pws {
		if !pw.isInMerge {
			logger.Panicf("BUG: missing isInMerge flag on the part %q", pw.p.path)
		}
		pw.isInMerge = false
	}
	ddb.partsLock.Unlock()
}

func (ddb *datadb) getMaxBigPartSize() uint64 {
	return getMaxOutBytes(ddb.path)
}

func (ddb *datadb) getMaxSmallPartSize() uint64 {
	// Small parts are cached in the OS page cache,
	// so limit their size by the remaining free RAM.
	mem := memory.Remaining()
	n := uint64(mem) / defaultPartsToMerge
	if n < 10e6 {
		n = 10e6
	}
	// Make sure the output part fits available disk space for small parts.
	sizeLimit := getMaxOutBytes(ddb.path)
	if n > sizeLimit {
		n = sizeLimit
	}
	return n
}

func getMaxOutBytes(path string) uint64 {
	n := availableDiskSpace(path)
	if n > maxBigPartSize {
		n = maxBigPartSize
	}
	return n
}

func availableDiskSpace(path string) uint64 {
	available := fs.MustGetFreeSpace(path)
	reserved := reservedDiskSpace.Load()
	if available < reserved {
		return 0
	}
	return available - reserved
}

func tryReserveDiskSpace(path string, n uint64) bool {
	available := fs.MustGetFreeSpace(path)
	reserved := reserveDiskSpace(n)
	if available >= reserved {
		return true
	}
	releaseDiskSpace(n)
	return false
}

func reserveDiskSpace(n uint64) uint64 {
	return reservedDiskSpace.Add(n)
}

func releaseDiskSpace(n uint64) {
	reservedDiskSpace.Add(^(n - 1))
}

// reservedDiskSpace tracks global reserved disk space for currently executed
// background merges across all the partitions.
//
// It should allow avoiding background merges when there is no free disk space.
var reservedDiskSpace atomicutil.Uint64

func needStop(stopCh <-chan struct{}) bool {
	select {
	case <-stopCh:
		return true
	default:
		return false
	}
}

// mustCloseDatadb can be called only when nobody accesses ddb.
func mustCloseDatadb(ddb *datadb) {
	// Flush ddb.rb for the last time
	ddb.rb.flush()

	// Notify background workers to stop.
	// Make it under ddb.partsLock in order to prevent from calling ddb.wg.Add()
	// after ddb.stopCh is closed and ddb.wg.Wait() is called.
	ddb.partsLock.Lock()
	close(ddb.stopCh)
	ddb.partsLock.Unlock()

	// Wait for background workers to stop.
	ddb.wg.Wait()

	// flush in-memory data to disk
	ddb.mustFlushInmemoryPartsToFiles(true)
	if len(ddb.inmemoryParts) > 0 {
		logger.Panicf("BUG: the number of in-memory parts must be zero after flushing them to disk; got %d", len(ddb.inmemoryParts))
	}
	ddb.inmemoryParts = nil

	// close small parts
	for _, pw := range ddb.smallParts {
		pw.decRef()
		if n := pw.refCount.Load(); n != 0 {
			logger.Panicf("BUG: there are %d references to smallPart", n)
		}
	}
	ddb.smallParts = nil

	// close big parts
	for _, pw := range ddb.bigParts {
		pw.decRef()
		if n := pw.refCount.Load(); n != 0 {
			logger.Panicf("BUG: there are %d references to bigPart", n)
		}
	}
	ddb.bigParts = nil

	ddb.path = ""
	ddb.pt = nil
}

func getPartNames(pws []*partWrapper) []string {
	partNames := make([]string, 0, len(pws))
	for _, pw := range pws {
		if pw.mp != nil {
			// Skip in-memory parts
			continue
		}
		partName := filepath.Base(pw.p.path)
		partNames = append(partNames, partName)
	}
	sort.Strings(partNames)
	return partNames
}

func mustWritePartNames(path string, partNames []string) {
	data, err := json.Marshal(partNames)
	if err != nil {
		logger.Panicf("BUG: cannot marshal partNames to JSON: %s", err)
	}
	partNamesPath := filepath.Join(path, partsFilename)
	fs.MustWriteAtomic(partNamesPath, data, true)
}

func mustReadPartNames(path string) []string {
	partNamesPath := filepath.Join(path, partsFilename)
	data, err := os.ReadFile(partNamesPath)
	if err != nil {
		if os.IsNotExist(err) {
			// The parts.json file is missing. This can happen if VictoriaLogs shuts down uncleanly
			// (via OOM crash, a panic, SIGKILL or hardware shutdown) in the middle of creating
			// new per-day partition inside the mustCreatePartition() function.
			// Check if there are any part directories in the datadb directory.
			des := fs.MustReadDir(path)
			var partDirs []string
			for _, de := range des {
				if !fs.IsDirOrSymlink(de) {
					continue
				}
				partDirs = append(partDirs, de.Name())
			}

			if len(partDirs) == 0 {
				logger.Warnf("creating missing %s with empty parts list, since no part directories found in %s", partNamesPath, path)
				mustWritePartNames(path, nil)
				return []string{}
			}

			// Parts exist but parts.json is missing - this is an unexpected state that requires manual intervention
			logger.Panicf("FATAL: cannot read %s: %s; found part directories %v in %s. "+
				"This indicates corruption. Manually remove the %s partition directory to resolve the corruption (the partition data will be lost)",
				partNamesPath, err, partDirs, path, path)
		}
		logger.Panicf("FATAL: cannot read %s: %s", partNamesPath, err)
	}
	var partNames []string
	if err := json.Unmarshal(data, &partNames); err != nil {
		logger.Panicf("FATAL: cannot parse %s: %s", partNamesPath, err)
	}
	return partNames
}

// mustRemoveUnusedDirs removes dirs at path, which are missing in partNames.
//
// These dirs may be left after unclean shutdown.
func mustRemoveUnusedDirs(path string, partNames []string) {
	des := fs.MustReadDir(path)
	m := make(map[string]struct{}, len(partNames))
	for _, partName := range partNames {
		m[partName] = struct{}{}
	}
	removedDirs := 0
	for _, de := range des {
		if !fs.IsDirOrSymlink(de) {
			// Skip non-directories.
			continue
		}
		fn := de.Name()
		if _, ok := m[fn]; !ok {
			deletePath := filepath.Join(path, fn)
			logger.Infof("removed unused directory %s (e.g. not listed in parts.json), which may have been left after an unclean shutdown", deletePath)
			fs.MustRemoveDir(deletePath)
			removedDirs++
		}
	}
	if removedDirs > 0 {
		fs.MustSyncPath(path)
	}
}

// appendPartsToMerge finds optimal parts to merge from src,
// appends them to dst and returns the result.
//
// THE OPTIMAL MERGE PROBLEM:
// We want to minimize write amplification while reducing part count.
// Write amplification occurs when the same data is rewritten multiple times.
// If we always merge 2 parts of size 1MB into a 2MB part, then merge that with
// another 2MB part to get 4MB, etc., data gets rewritten many times.
//
// THE SOLUTION (minMergeMultiplier):
// We only merge parts if the output will be at least minMergeMultiplier (1.7x)
// larger than the largest input. This ensures each piece of data is rewritten
// at most O(log_{1.7}(total_size)) times.
//
// THE ALGORITHM:
//  1. Filter out parts that are too large to fit in the output size limit
//  2. Sort remaining parts by size (smallest first)
//  3. Exhaustively search for the combination of 2-15 consecutive parts
//     that gives the best merge ratio (output_size / largest_input_size)
//  4. Only merge if the ratio exceeds minMergeMultiplier
//
// WHY CONSECUTIVE? After sorting by size, consecutive parts have similar sizes.
// Merging similarly-sized parts is more efficient than merging a tiny part with
// a huge one (which would rewrite the huge part for little benefit).
//
// WHY EXHAUSTIVE SEARCH? With typical part counts (< 100), the O(N^2) search
// is fast enough, and it finds the globally optimal merge. This is better than
// a greedy algorithm that might make locally optimal but globally suboptimal choices.
func appendPartsToMerge(dst, src []*partWrapper, maxOutBytes uint64) []*partWrapper {
	if len(src) < 2 {
		// There is no need in merging zero or one part :)
		return dst
	}

	// Filter out too big parts.
	// This should reduce N for O(N^2) algorithm below.
	maxInPartBytes := uint64(float64(maxOutBytes) / minMergeMultiplier)
	tmp := make([]*partWrapper, 0, len(src))
	for _, pw := range src {
		if pw.p.ph.CompressedSizeBytes > maxInPartBytes {
			continue
		}
		tmp = append(tmp, pw)
	}
	src = tmp

	sortPartsForOptimalMerge(src)

	maxSrcParts := defaultPartsToMerge
	if maxSrcParts > len(src) {
		maxSrcParts = len(src)
	}
	minSrcParts := (maxSrcParts + 1) / 2
	if minSrcParts < 2 {
		minSrcParts = 2
	}

	// Exhaustive search for parts giving the lowest write amplification when merged.
	var pws []*partWrapper
	maxM := float64(0)
	for i := minSrcParts; i <= maxSrcParts; i++ {
		for j := 0; j <= len(src)-i; j++ {
			a := src[j : j+i]
			if a[0].p.ph.CompressedSizeBytes*uint64(len(a)) < a[len(a)-1].p.ph.CompressedSizeBytes {
				// Do not merge parts with too big difference in size,
				// since this results in unbalanced merges.
				continue
			}
			outSize := getCompressedSize(a)
			if outSize > maxOutBytes {
				// There is no need in verifying remaining parts with bigger sizes.
				break
			}
			m := float64(outSize) / float64(a[len(a)-1].p.ph.CompressedSizeBytes)
			if m < maxM {
				continue
			}
			maxM = m
			pws = a
		}
	}

	minM := float64(defaultPartsToMerge) / 2
	if minM < minMergeMultiplier {
		minM = minMergeMultiplier
	}
	if maxM < minM {
		// There is no sense in merging parts with too small m,
		// since this leads to high disk write IO.
		return dst
	}
	return append(dst, pws...)
}

func sortPartsForOptimalMerge(pws []*partWrapper) {
	// Sort src parts by size and backwards timestamp.
	// This should improve adjanced points' locality in the merged parts.
	sort.Slice(pws, func(i, j int) bool {
		a := &pws[i].p.ph
		b := &pws[j].p.ph
		if a.CompressedSizeBytes == b.CompressedSizeBytes {
			return a.MinTimestamp > b.MinTimestamp
		}
		return a.CompressedSizeBytes < b.CompressedSizeBytes
	})
}

func getCompressedSize(pws []*partWrapper) uint64 {
	n := uint64(0)
	for _, pw := range pws {
		n += pw.p.ph.CompressedSizeBytes
	}
	return n
}

func getUncompressedSize(pws []*partWrapper) uint64 {
	n := uint64(0)
	for _, pw := range pws {
		n += pw.p.ph.UncompressedSizeBytes
	}
	return n
}

func getRowsCount(pws []*partWrapper) uint64 {
	n := uint64(0)
	for _, pw := range pws {
		n += pw.p.ph.RowsCount
	}
	return n
}

func getBlocksCount(pws []*partWrapper) uint64 {
	n := uint64(0)
	for _, pw := range pws {
		n += pw.p.ph.BlocksCount
	}
	return n
}

func (ddb *datadb) mustForceMergeAllParts() {
	// Flush inmemory parts to files before forced merge
	ddb.mustFlushInmemoryPartsToFiles(true)

	var pws []*partWrapper

	// Collect all the file parts for forced merge
	ddb.partsLock.Lock()
	pws = appendAllPartsForMergeLocked(pws, ddb.smallParts)
	pws = appendAllPartsForMergeLocked(pws, ddb.bigParts)
	ddb.partsLock.Unlock()

	// If len(pws) == 1, then the merge must run anyway.
	// This allows applying the configured retention, removing the deleted data, etc.

	// Merge pws optimally
	wg := getWaitGroup()
	for len(pws) > 0 {
		pwsToMerge, pwsRemaining := getPartsForOptimalMerge(pws)
		bigPartsConcurrencyCh <- struct{}{}

		wg.Go(func() {
			ddb.mustMergeParts(pwsToMerge, false)
			<-bigPartsConcurrencyCh
		})

		pws = pwsRemaining
	}
	wg.Wait()
	putWaitGroup(wg)
}

func (ddb *datadb) deleteRows(pso *partitionSearchOptions, stopCh <-chan struct{}) bool {
	// Get all the parts and make sure they are kept open.
	pws, pwsDecRef := ddb.getPartsForTimeRange(pso.minTimestamp, pso.maxTimestamp)
	defer pwsDecRef()

	// Search for parts, which contain logs matching pso for the deletion and which aren't in merge at the moment.
	var pwsToMerge []*partWrapper
	for _, pw := range pws {
		if !pw.p.hasMatchingRows(pso, stopCh) {
			continue
		}

		ddb.partsLock.Lock()
		ok := !pw.isInMerge
		if ok {
			pw.isInMerge = true
			pwsToMerge = append(pwsToMerge, pw)
		}
		ddb.partsLock.Unlock()

		if !ok {
			ddb.releasePartsToMerge(pwsToMerge)
			return false
		}
	}

	// merge pwsToMerge while dropping logs matching pso.
	return ddb.mustMergePartsInternal(pwsToMerge, false, pso, stopCh)
}

func appendAllPartsForMergeLocked(dst, src []*partWrapper) []*partWrapper {
	for _, pw := range src {
		if !pw.isInMerge {
			pw.isInMerge = true
			dst = append(dst, pw)
		}
	}
	return dst
}
