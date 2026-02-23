package logstorage

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/contextutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/snapshot/snapshotutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

// StorageStats represents stats for the storage. It may be obtained by calling Storage.UpdateStats().
type StorageStats struct {
	// RowsDroppedTooBigTimestamp is the number of rows dropped during data ingestion because their timestamp is bigger than the maximum allowed.
	RowsDroppedTooBigTimestamp uint64

	// RowsDroppedTooSmallTimestamp is the number of rows dropped during data ingestion because their timestamp is smaller than the minimum allowed.
	RowsDroppedTooSmallTimestamp uint64

	// PartitionsCount is the number of partitions in the storage.
	PartitionsCount uint64

	// MaxDiskSpaceUsageBytes is the maximum disk space logs can use.
	MaxDiskSpaceUsageBytes int64

	// IsReadOnly indicates whether the storage is read-only.
	IsReadOnly bool

	// PartitionStats contains partition stats.
	PartitionStats

	// MinTimestamp is the minimum event timestamp across the entire storage (in nanoseconds).
	// It is set to math.MinInt64 if there is no data.
	MinTimestamp int64

	// MaxTimestamp is the maximum event timestamp across the entire storage (in nanoseconds).
	// It is set to math.MaxInt64 if there is no data.
	MaxTimestamp int64
}

// Reset resets s.
func (s *StorageStats) Reset() {
	*s = StorageStats{}
}

// StorageConfig is the config for the Storage.
type StorageConfig struct {
	// Retention is the retention for the ingested data.
	//
	// Older data is automatically deleted.
	Retention time.Duration

	// DefaultParallelReaders is the default number of parallel readers to use per each query execution.
	//
	// Higher value can help improving query performance on storage with high disk read latency such as S3.
	DefaultParallelReaders int

	// MaxDiskSpaceUsageBytes is an optional maximum disk space logs can use.
	//
	// The oldest per-day partitions are automatically dropped if the total disk space usage exceeds this limit.
	MaxDiskSpaceUsageBytes int64

	// MaxDiskUsagePercent is an optional threshold in percentage (1-100) for disk usage of the filesystem holding the storage path.
	// When the current disk usage exceeds this percentage, the oldest per-day partitions are automatically dropped.
	MaxDiskUsagePercent int

	// FlushInterval is the interval for flushing the in-memory data to disk at the Storage.
	FlushInterval time.Duration

	// FutureRetention is the allowed retention from the current time to future for the ingested data.
	//
	// Log entries with timestamps bigger than now+FutureRetention are ignored.
	FutureRetention time.Duration

	// MaxBackfillAge is the maximum allowed age for the backfilled logs.
	//
	// Log entries with timestamps older than now-MaxBackfillAge are ignored.
	MaxBackfillAge time.Duration

	// SnapshotsMaxAge is the maximum age for the created partition snapshots.
	//
	// Snapshots are automatically dropped after that duration.
	// See https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle
	SnapshotsMaxAge time.Duration

	// MinFreeDiskSpaceBytes is the minimum free disk space at storage path after which the storage stops accepting new data
	// and enters read-only mode.
	MinFreeDiskSpaceBytes int64

	// LogNewStreams indicates whether to log newly created log streams.
	//
	// This can be useful for debugging of high cardinality issues.
	// https://docs.victoriametrics.com/victorialogs/keyconcepts/#high-cardinality
	LogNewStreams bool

	// LogIngestedRows indicates whether to log the ingested log entries.
	//
	// This can be useful for debugging of data ingestion.
	LogIngestedRows bool
}

// Storage is the storage for log entries.
type Storage struct {
	rowsDroppedTooBigTimestamp   atomic.Uint64
	rowsDroppedTooSmallTimestamp atomic.Uint64

	// path is the path to the Storage directory
	path string

	// retention is the retention for the stored data
	//
	// older data is automatically deleted
	retention time.Duration

	// defaultParallelReaders is the default number of parallel IO-bound readers to use for query execution.
	//
	// Higher number of readers may help increasing query performance on storage with high read latency such as S3.
	defaultParallelReaders int

	// maxDiskSpaceUsageBytes is an optional maximum disk space logs can use.
	//
	// The oldest per-day partitions are automatically dropped if the total disk space usage exceeds this limit.
	maxDiskSpaceUsageBytes int64

	// maxDiskUsagePercent is an optional threshold for disk usage percentage at which the oldest partitions are automatically dropped.
	maxDiskUsagePercent int

	// flushInterval is the interval for flushing in-memory data to disk
	flushInterval time.Duration

	// futureRetention is the maximum allowed interval to write data into the future
	futureRetention time.Duration

	// maxBackfillAge is the maximum age of logs with historical timestamps to accept
	maxBackfillAge time.Duration

	// snapshotsMaxAge is the maximum age for the created partition snapshots.
	//
	// Older snapshots are automatically deleted. See https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle
	snapshotsMaxAge time.Duration

	// minFreeDiskSpaceBytes is the minimum free disk space at path after which the storage stops accepting new data
	minFreeDiskSpaceBytes uint64

	// logNewStreams instructs to log new streams if it is set to true
	logNewStreams atomic.Bool

	// logIngestedRows instructs to log all the ingested log entries if it is set to true
	logIngestedRows bool

	// flockF is a file, which makes sure that the Storage is opened by a single process
	flockF *os.File

	// partitions is a list of partitions for the Storage.
	//
	// It must be accessed under partitionsLock.
	//
	// partitions are sorted by time, e.g. partitions[0] has the smallest time.
	partitions []*partitionWrapper

	// ptwHot is the "hot" partition, were the last rows were ingested.
	//
	// It must be accessed under partitionsLock.
	ptwHot *partitionWrapper

	// deletedPartitions contains days for the deleted partitions.
	//
	// It prevents from re-creating already deleted partitions.
	//
	// It must be accessed under partitionsLock.
	deletedPartitions []int64

	// partitionsLock protects partitions, ptwHot, deletedPartitions.
	partitionsLock sync.Mutex

	// stopCh is closed when the Storage must be stopped.
	stopCh chan struct{}

	// wg is used for waiting for background workers at MustClose().
	wg sync.WaitGroup

	// streamIDCache caches (partition, streamIDs) seen during data ingestion.
	//
	// It reduces the load on persistent storage during data ingestion by skipping
	// the check whether the given stream is already registered in the persistent storage.
	streamIDCache *cache

	// filterStreamCache caches streamIDs keyed by (partition, []TenanID, StreamFilter).
	//
	// It reduces the load on persistent storage during querying by _stream:{...} filter.
	filterStreamCache *cache

	// deleteTasksLock protects deleteTasks
	deleteTasksLock sync.Mutex

	// deleteTasks contains a list of active and pending delete tasks
	deleteTasks []*DeleteTask
}

// PartitionAttach attaches the partition with the given name to s.
//
// The name must have the YYYYMMDD format.
//
// The attached partition can be detached via PartitionDetach() call.
func (s *Storage) PartitionAttach(name string) error {
	day, err := getPartitionDayFromName(name)
	if err != nil {
		return err
	}

	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()

	if slices.Contains(s.deletedPartitions, day) {
		// Keep retention-deleted days permanently blocked from re-attach.
		return fmt.Errorf("cannot attach the partition %q, since it is automatically deleted because of retention; see https://docs.victoriametrics.com/victorialogs/#retention", name)
	}

	// Verify whether the given partition already exists in the attached partitions list.
	for _, ptw := range s.partitions {
		if ptw.pt.name == name {
			return fmt.Errorf("cannot attach the partition %q, because it is arleady attached", name)
		}
	}

	// Open the partition and add it to the s.partitions.
	partitionsPath := filepath.Join(s.path, partitionsDirname)
	partitionPath := filepath.Join(partitionsPath, name)
	if !fs.IsPathExist(partitionPath) {
		return fmt.Errorf("cannot attach the partition %q, because there is no the corresponding directory %q", name, partitionPath)
	}

	pt := mustOpenPartition(s, partitionPath)
	ptw := newPartitionWrapper(pt, day)

	s.partitions = append(s.partitions, ptw)
	// Keep invariant: s.partitions is ordered by day.
	sortPartitions(s.partitions)

	logger.Infof("successfully attached partition %q from %q", name, partitionPath)

	return nil
}

// PartitionDetach detaches the partition with the given name from s.
//
// The name must have the YYYYMMDD format.
//
// The detached partition can be attached again via PartitionAttach() call.
//
// IMPORTANT: This function BLOCKS until all active readers/writers finish.
// The blocking behavior is intentional - it guarantees the partition is in a
// consistent state before it can be moved, backed up, or deleted externally.
// This is essential for safe backup/restore workflows.
func (s *Storage) PartitionDetach(name string) error {
	// Use a closure to keep the lock scope minimal - we only need the lock
	// while modifying the partition list, not while waiting for refs to drop.
	ptw := func() *partitionWrapper {
		s.partitionsLock.Lock()
		defer s.partitionsLock.Unlock()

		for i, ptw := range s.partitions {
			if ptw.pt.name != name {
				continue
			}

			// Found the partition to detach. Detach it.
			// Remove from the slice - this prevents new references from being taken
			s.partitions = append(s.partitions[:i], s.partitions[i+1:]...)
			if ptw == s.ptwHot {
				// Force re-selection of hot partition on next write.
				// If we don't clear this, future writes would try to use the detached partition.
				s.ptwHot = nil
			}
			return ptw
		}
		return nil
	}()

	if ptw == nil {
		return fmt.Errorf("cannot detach the partition %q, because it isn't attached", name)
	}

	partitionPath := ptw.pt.path
	// Drop storage-owned ref. Remaining readers/writers keep their refs.
	// After this call, refCount will eventually reach 0 when all active
	// operations complete, triggering doneCh close.
	ptw.decRef()

	// BLOCKING: Wait until all concurrent readers/writers finish.
	// This is crucial for safety - the caller can be certain the partition
	// is quiesced before they touch its files on disk.
	logger.Infof("waiting until the partition %q isn't accessed", name)
	<-ptw.doneCh

	logger.Infof("successfully detached partition %q from %q", name, partitionPath)

	return nil
}

// PartitionList returns the list of the names for the currently attached partitions.
//
// Every partition name has YYYYMMDD format.
func (s *Storage) PartitionList() []string {
	s.partitionsLock.Lock()
	ptNames := make([]string, len(s.partitions))
	for i, ptw := range s.partitions {
		ptNames[i] = ptw.pt.name
	}
	s.partitionsLock.Unlock()

	return ptNames
}

// PartitionSnapshotMustCreate creates snapshots for partitions with the given partitionPrefix
//
// The partitionPrefix must match one of the following formats:
// - YYYYMMDD - matches partitions for the given day
// - YYYYMM - matches partitions for the given month
// - YYYY - matches partitions for the given year
// - an empty string - matches all the partitions
//
// The function returns paths to created snapshots
func (s *Storage) PartitionSnapshotMustCreate(partitionPrefix string) []string {
	ptws := s.getPartitions()
	defer s.putPartitions(ptws)

	var snapshotPaths []string

	for _, ptw := range ptws {
		if strings.HasPrefix(ptw.pt.name, partitionPrefix) {
			snapshotPath := ptw.pt.mustCreateSnapshot()
			snapshotPaths = append(snapshotPaths, snapshotPath)
		}
	}

	return snapshotPaths
}

// PartitionSnapshotList returns a list of paths to all the snapshots across active partitions.
func (s *Storage) PartitionSnapshotList() []string {
	ptws := s.getPartitions()
	defer s.putPartitions(ptws)

	snapshotPaths := getSnapshotPaths(ptws)
	sort.Strings(snapshotPaths)

	return snapshotPaths
}

func getSnapshotPaths(ptws []*partitionWrapper) []string {
	var snapshotPaths []string

	for _, ptw := range ptws {
		snapshotsPath := filepath.Join(ptw.pt.path, snapshotsDirname)
		if !fs.IsPathExist(snapshotsPath) {
			continue
		}

		des := fs.MustReadDir(snapshotsPath)
		for _, de := range des {
			name := de.Name()
			if err := snapshotutil.Validate(name); err != nil {
				logger.Warnf("unsupported snapshot name %q at %q: %s", name, snapshotsPath, err)
				continue
			}

			snapshotPath := filepath.Join(snapshotsPath, name)
			snapshotPaths = append(snapshotPaths, snapshotPath)
		}
	}

	return snapshotPaths
}

// PartitionSnapshotDelete removes the snapshot located at the given snapshotPath if it belongs to an active partition.
func (s *Storage) PartitionSnapshotDelete(snapshotPath string) error {
	snapshotName := filepath.Base(snapshotPath)
	if err := snapshotutil.Validate(snapshotName); err != nil {
		return fmt.Errorf("unsupported snapshot name %q at %q: %s", snapshotName, snapshotPath, err)
	}

	snapshotDir := filepath.Dir(snapshotPath)
	if filepath.Base(snapshotDir) != snapshotsDirname {
		return fmt.Errorf("snapshot path %q must point to a directory inside %q", snapshotPath, snapshotsDirname)
	}
	partitionPath := filepath.Dir(snapshotDir)

	ptws := s.getPartitions()
	defer s.putPartitions(ptws)

	ptw := func() *partitionWrapper {
		for _, ptw := range ptws {
			if ptw.pt.path == partitionPath {
				return ptw
			}
		}
		return nil
	}()

	if ptw == nil {
		return fmt.Errorf("partition path %q cannot be found across active partitions", partitionPath)
	}

	return ptw.pt.deleteSnapshot(snapshotName)
}

// MustDeleteStalePartitionSnapshots deletes snapshots older than maxAge.
//
// The list of paths to deleted snapshots is returned from this function.
func (s *Storage) MustDeleteStalePartitionSnapshots(maxAge time.Duration) []string {
	var deletedSnapshotPaths []string

	currentTime := time.Now()

	ptws := s.getPartitions()
	defer s.putPartitions(ptws)

	snapshotPaths := getSnapshotPaths(ptws)
	for _, snapshotPath := range snapshotPaths {
		fi, err := os.Stat(snapshotPath)
		if err != nil {
			logger.Warnf("skipping snapshot at %s since cannot access it: %s", snapshotPath, err)
			continue
		}

		creationTime := fi.ModTime()
		if currentTime.Sub(creationTime) > maxAge {
			logger.Infof("deleting snapshot at %s because it became older than maxAge=%s (snapshot creation time: %s)", snapshotPath, maxAge, creationTime)
			fs.MustRemoveDir(snapshotPath)
			deletedSnapshotPaths = append(deletedSnapshotPaths, snapshotPath)
			logger.Infof("deleted snapshot at %s", snapshotPath)
		}
	}

	return deletedSnapshotPaths
}

// DeleteRunTask starts deletion of logs according to the given filter f for the given tenantIDs.
//
// The taskID must contain an unique id of the task. It is used for tracking the task at the list returned by DeleteActiveTasks().
// The timestamp must contain the timestamp in seconds when the task is started.
func (s *Storage) DeleteRunTask(_ context.Context, taskID string, timestamp int64, tenantIDs []TenantID, f *Filter) error {
	// Register the task in the list of active delete tasks, so it survives application restarts and crashes.
	dt := newDeleteTask(taskID, tenantIDs, f.String(), timestamp)

	s.deleteTasksLock.Lock()
	defer s.deleteTasksLock.Unlock()

	// Verify that the task with the given taskID doesn't exist yet
	for _, dt := range s.deleteTasks {
		if dt.TaskID == taskID {
			return fmt.Errorf("the delete task with task_id=%q is already registered", taskID)
		}
	}

	// Register the task and persist it to the file.
	s.deleteTasks = append(s.deleteTasks, dt)
	// Persist immediately, so task survives crashes/restarts.
	s.mustSaveDeleteTasksLocked()

	return nil
}

// mustSaveDeleteTasksLocked saves s.deleteTasks to file
//
// The s.deleteTaskLock must be locked while calling this function.
func (s *Storage) mustSaveDeleteTasksLocked() {
	deleteTasksPath := filepath.Join(s.path, deleteTasksFilename)
	mustWriteDeleteTasksToFile(deleteTasksPath, s.deleteTasks)
}

// DeleteStopTask stops the delete task with the given taskID.
//
// It waits until the task is stopped before returning.
// If there is no a task with the given taskID, then the function returns immediately.
func (s *Storage) DeleteStopTask(ctx context.Context, taskID string) error {
	var doneCh <-chan struct{}

	s.deleteTasksLock.Lock()

	for i, dt := range s.deleteTasks {
		if dt.TaskID != taskID {
			continue
		}

		if dt.cancel != nil {
			// Cancel the currently executed task. The task executor will remove this task from s.deleteTasks
			dt.cancel()
			doneCh = dt.doneCh
		} else {
			// The task is waiting to be executed. Drop it.
			s.deleteTasks = append(s.deleteTasks[:i], s.deleteTasks[i+1:]...)
			s.mustSaveDeleteTasksLocked()
		}
		break
	}

	s.deleteTasksLock.Unlock()

	if doneCh == nil {
		return nil
	}

	// Wait until the task is canceled.
	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		// Caller can bound wait time for task cancellation.
		return ctx.Err()
	}
}

// DeleteActiveTasks returns currently running active delete tasks, which were started via DeleteRunTask().
func (s *Storage) DeleteActiveTasks(_ context.Context) ([]*DeleteTask, error) {
	s.deleteTasksLock.Lock()
	dts := append([]*DeleteTask{}, s.deleteTasks...)
	s.deleteTasksLock.Unlock()

	return dts, nil
}

// EnableLogNewStreams enables logging newly ingested streams during the given number of seconds
func (s *Storage) EnableLogNewStreams(seconds int) {
	if seconds <= 0 {
		// Do nothing.
		return
	}

	vPrev := s.logNewStreams.Swap(true)
	if vPrev {
		logger.Infof("logging of new streams is already enabled")
		return
	}

	logger.Infof("enabled logging of new streams for %d seconds", seconds)

	d := time.Second * time.Duration(seconds)
	time.AfterFunc(d, func() {
		s.logNewStreams.Swap(false)
		logger.Infof("disabled logging of new streams")
	})
}

// partitionWrapper wraps a partition with reference counting for safe concurrent access.
//
// REFERENCE COUNTING LIFECYCLE:
// Partitions are accessed concurrently by ingestion, queries, and background maintenance.
// Reference counting ensures a partition isn't closed while in use:
//
//	incRef() called when:
//	  - Storage.MustAddRows starts using the partition
//	  - Storage.getPartitions returns all partitions for queries
//	  - Hot partition is cached in getPartitionForWriting
//
//	decRef() called when:
//	  - MustAddRows finishes with the partition
//	  - Query finishes (putPartitions)
//	  - Partition is being deleted
//
// When refCount reaches zero:
//   - If mustDrop is set, delete the partition from disk
//   - Close the partition (flush data, release file handles)
//   - Close doneCh to signal any waiters
//
// This pattern allows safe deletion of old partitions while queries may still
// be reading from them - the deletion is deferred until all references are released.
//
// WHY THIS DESIGN?
// The alternative would be to use a global lock during partition operations, but that
// would serialize all queries and ingestion, killing performance. Reference counting
// allows fine-grained concurrency: many readers can access different partitions
// simultaneously, and cleanup happens automatically when safe.
//
// The doneCh channel is crucial for PartitionDetach - it provides a way to block
// until the partition is truly quiesced, which is required for safe backup/restore
// operations where we need to guarantee no concurrent access.
type partitionWrapper struct {
	// refCount is the number of active references to this partition.
	// When it reaches zero, the partition may be closed and optionally deleted.
	refCount atomic.Int32

	// mustDrop indicates the partition should be deleted when refCount reaches zero.
	// Set when partition is removed due to retention or disk space limits.
	mustDrop atomic.Bool

	// day is the partition day in Unix timestamp divided by seconds per day.
	// Used for retention calculations and partition ordering.
	day int64

	// pt is the wrapped partition containing the actual log data.
	pt *partition

	// doneCh is closed when refCount reaches zero, signaling the partition
	// is no longer in use. Used by PartitionDetach to wait for safe removal.
	doneCh chan struct{}
}

func newPartitionWrapper(pt *partition, day int64) *partitionWrapper {
	pw := &partitionWrapper{
		day:    day,
		pt:     pt,
		doneCh: make(chan struct{}),
	}
	pw.incRef()
	return pw
}

func (ptw *partitionWrapper) incRef() {
	ptw.refCount.Add(1)
}

func (ptw *partitionWrapper) decRef() {
	// Atomically decrement and get the new value.
	// If n > 0, other goroutines still hold references - nothing to clean up yet.
	// This check avoids the expensive close/delete operations in the common case
	// where the partition is still being actively used.
	n := ptw.refCount.Add(-1)
	if n > 0 {
		// Other goroutines still hold this partition.
		return
	}

	// At this point, refCount == 0, meaning this is the last reference.
	// We can safely perform cleanup without racing with other goroutines.

	// Check if partition should be deleted from disk (set during retention cleanup or detach)
	deletePath := ""
	if ptw.mustDrop.Load() {
		deletePath = ptw.pt.path
	}

	// Close pw.pt, since nobody refers to it.
	// This flushes any pending data and releases file handles.
	mustClosePartition(ptw.pt)
	ptw.pt = nil

	// Delete partition if needed.
	// Done after close to ensure file handles are released first.
	if deletePath != "" {
		mustDeletePartition(deletePath)
	}

	// signal that the ptw is no longer accessed.
	// This unblocks anyone waiting on <-ptw.doneCh (e.g., PartitionDetach)
	close(ptw.doneCh)
}

// canAddAllRows checks if all rows in lr fit within this partition's day.
// Returns false if any row has a timestamp outside this partition's time range.
// This is used by the fast path in MustAddRows to quickly determine if all rows
// can be added to the hot partition.
func (ptw *partitionWrapper) canAddAllRows(lr *LogRows) bool {
	minTimestamp := ptw.day * nsecsPerDay
	maxTimestamp := minTimestamp + nsecsPerDay - 1
	for _, ts := range lr.timestamps {
		if ts < minTimestamp || ts > maxTimestamp {
			// Any out-of-range row forces slow-path split in Storage.MustAddRows().
			return false
		}
	}
	return true
}

// mustCreateStorage creates Storage at the given path.
func mustCreateStorage(path string) {
	fs.MustMkdirFailIfExist(path)

	partitionsPath := filepath.Join(path, partitionsDirname)
	fs.MustMkdirFailIfExist(partitionsPath)

	fs.MustSyncPathAndParentDir(path)
}

// MustOpenStorage opens Storage at the given path.
//
// MustClose must be called on the returned Storage when it is no longer needed.
func MustOpenStorage(path string, cfg *StorageConfig) *Storage {
	flushInterval := cfg.FlushInterval
	if flushInterval < time.Second {
		// Clamp to avoid too-frequent flush cycles.
		flushInterval = time.Second
	}

	retention := cfg.Retention
	if retention < 24*time.Hour {
		// Retention is partition/day-based, so less than one day is not supported.
		retention = 24 * time.Hour
	}

	futureRetention := cfg.FutureRetention
	if futureRetention < 24*time.Hour {
		futureRetention = 24 * time.Hour
	}

	maxBackfillAge := cfg.MaxBackfillAge
	if maxBackfillAge <= 0 || maxBackfillAge > retention {
		// Keep backfill window bounded and consistent with retention.
		maxBackfillAge = retention
	}

	var minFreeDiskSpaceBytes uint64
	if cfg.MinFreeDiskSpaceBytes >= 0 {
		minFreeDiskSpaceBytes = uint64(cfg.MinFreeDiskSpaceBytes)
	}

	if !fs.IsPathExist(path) {
		mustCreateStorage(path)
	}

	flockF := fs.MustCreateFlockFile(path)

	// Load caches
	streamIDCache := newCache()
	filterStreamCache := newCache()

	// Load delete tasks which may be left since the previous restart
	deleteTasksPath := filepath.Join(path, deleteTasksFilename)
	deleteTasks := mustReadDeleteTasksFromFile(deleteTasksPath)

	s := &Storage{
		path:                   path,
		retention:              retention,
		defaultParallelReaders: cfg.DefaultParallelReaders,
		maxDiskSpaceUsageBytes: cfg.MaxDiskSpaceUsageBytes,
		maxDiskUsagePercent:    cfg.MaxDiskUsagePercent,
		flushInterval:          flushInterval,
		futureRetention:        futureRetention,
		maxBackfillAge:         maxBackfillAge,
		snapshotsMaxAge:        cfg.SnapshotsMaxAge,
		minFreeDiskSpaceBytes:  minFreeDiskSpaceBytes,
		logIngestedRows:        cfg.LogIngestedRows,
		flockF:                 flockF,
		stopCh:                 make(chan struct{}),

		streamIDCache:     streamIDCache,
		filterStreamCache: filterStreamCache,

		deleteTasks: deleteTasks,
	}
	s.logNewStreams.Store(cfg.LogNewStreams)

	partitionsPath := filepath.Join(path, partitionsDirname)
	fs.MustMkdirIfNotExist(partitionsPath)
	fs.MustSyncPath(path)

	des := fs.MustReadDir(partitionsPath)
	ptws := make([]*partitionWrapper, len(des))

	// Open partitions in parallel. This should improve VictoriaLogs initialization duration
	// when it opens many partitions.
	var wg sync.WaitGroup
	concurrencyLimiterCh := make(chan struct{}, cgroup.AvailableCPUs())
	for idx, de := range des {
		fname := de.Name()

		partitionDir := filepath.Join(partitionsPath, fname)
		if fs.IsPartiallyRemovedDir(partitionDir) {
			// Drop partially removed partition directory. This may happen when unclean shutdown happens during partition deletion.
			fs.MustRemoveDir(partitionDir)
			continue
		}

		concurrencyLimiterCh <- struct{}{}
		wg.Go(func() {
			day, err := getPartitionDayFromName(fname)
			if err != nil {
				logger.Panicf("FATAL: cannot parse partition filename %q at %q: %s", fname, partitionsPath, err)
			}

			partitionPath := filepath.Join(partitionsPath, fname)
			pt := mustOpenPartition(s, partitionPath)
			ptws[idx] = newPartitionWrapper(pt, day)

			// Release worker slot after partition is opened.
			<-concurrencyLimiterCh
		})
	}
	wg.Wait()

	sortPartitions(ptws)

	// Delete partitions from the future if needed
	now := time.Now().UnixNano()
	maxAllowedDay := s.getMaxAllowedDay(now)
	j := len(ptws) - 1
	for j >= 0 {
		ptw := ptws[j]
		if ptw.day <= maxAllowedDay {
			break
		}
		logger.Infof("the partition %s is scheduled to be deleted because it is outside the -futureRetention=%dd", ptw.pt.path, durationToDays(s.futureRetention))
		ptw.mustDrop.Store(true)
		ptw.decRef()
		j--
	}
	j++
	for i := j; i < len(ptws); i++ {
		ptws[i] = nil
	}
	ptws = ptws[:j]

	s.partitions = ptws
	// Start background maintenance loops after partition set is finalized.
	s.runRetentionWatcher()
	s.runMaxDiskSpaceUsageWatcher()
	s.runDeleteTasksWatcher()
	s.runSnapshotsMaxAgeWatcher()
	return s
}

func sortPartitions(ptws []*partitionWrapper) {
	sort.Slice(ptws, func(i, j int) bool {
		return ptws[i].day < ptws[j].day
	})
}

func (s *Storage) runRetentionWatcher() {
	s.wg.Go(s.watchRetention)
}

func (s *Storage) runMaxDiskSpaceUsageWatcher() {
	if s.maxDiskSpaceUsageBytes <= 0 && s.maxDiskUsagePercent <= 0 {
		return // nothing to watch
	}
	s.wg.Go(s.watchMaxDiskSpaceUsage)
}

func (s *Storage) runDeleteTasksWatcher() {
	s.wg.Go(s.watchDeleteTasks)
}

func (s *Storage) runSnapshotsMaxAgeWatcher() {
	s.wg.Go(s.watchSnapshotsMaxAge)
}

func (s *Storage) watchRetention() {
	// Use jittered interval to avoid synchronized load spikes across multiple
	// VictoriaLogs instances checking retention at the same time
	d := timeutil.AddJitterToDuration(time.Hour)
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		var ptwsToDelete []*partitionWrapper
		now := time.Now().UnixNano()
		minAllowedDay := s.getMinAllowedDay(now)

		s.partitionsLock.Lock()

		// Delete outdated partitions.
		// s.partitions are sorted by day, so the partitions, which can become outdated, are located at the beginning of the list
		// WHY sorted by day? This allows O(k) deletion of expired partitions where k = number expired.
		// Without sorting, we'd need to scan the entire list each time.
		ptws := s.partitions
		for i, ptw := range ptws {
			if ptw.day < minAllowedDay {
				// This partition is expired - keep scanning for more expired ones
				continue
			}

			// Found the first non-expired partition.
			// All partitions before index i are expired and should be deleted.
			ptwsToDelete = ptws[:i]
			s.partitions = ptws[i:]

			// Track deleted days to prevent re-creation.
			// This guards against late-arriving data re-creating a partition
			// that was already deleted due to retention.
			s.updateDeletedPartitionsLocked(ptwsToDelete)

			// Remove reference to deleted partitions from s.ptwHot
			// If the hot partition was deleted, force re-selection on next write
			if slices.Contains(ptwsToDelete, s.ptwHot) {
				s.ptwHot = nil
			}

			break
		}

		s.partitionsLock.Unlock()

		// Delete partitions outside the lock to avoid blocking other operations.
		// The partitions have already been removed from s.partitions, so new
		// operations won't try to access them.
		for i, ptw := range ptwsToDelete {
			logger.Infof("the partition %s is scheduled to be deleted because it is outside the -retentionPeriod=%dd", ptw.pt.path, durationToDays(s.retention))
			// Mark for deletion - actual deletion happens when refCount reaches 0
			ptw.mustDrop.Store(true)
			// Decrement our reference - this triggers cleanup when safe
			ptw.decRef()
			ptwsToDelete[i] = nil // help GC
		}

		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (s *Storage) watchMaxDiskSpaceUsage() {
	d := timeutil.AddJitterToDuration(10 * time.Second)
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		// Determine dynamic limit in bytes
		var limitBytes uint64
		if s.maxDiskSpaceUsageBytes > 0 {
			limitBytes = uint64(s.maxDiskSpaceUsageBytes)
		} else if s.maxDiskUsagePercent > 0 {
			total := fs.MustGetTotalSpace(s.path)
			if total > 0 {
				limitBytes = (total * uint64(s.maxDiskUsagePercent)) / 100
			}
		}
		if limitBytes == 0 {
			// Nothing to enforce
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				continue
			}
		}

		s.partitionsLock.Lock()
		var n uint64
		ptws := s.partitions
		var ptwsToDelete []*partitionWrapper
		for i := len(ptws) - 1; i >= 0; i-- {
			ptw := ptws[i]
			var ps PartitionStats
			ptw.pt.updateStats(&ps)
			// Accumulate size from newest to oldest and drop oldest overflow.
			n += ps.IndexdbSizeBytes + ps.CompressedSmallPartSize + ps.CompressedBigPartSize
			if n <= limitBytes {
				continue
			}
			if i >= len(ptws)-2 {
				// Keep the last two per-day partitions, so logs could be queried for one day time range.
				continue
			}

			// ptws are sorted by time, so just drop all the partitions until i, including i.
			i++
			ptwsToDelete = ptws[:i]
			s.partitions = ptws[i:]
			s.updateDeletedPartitionsLocked(ptwsToDelete)

			// Remove reference to deleted partitions from s.ptwHot
			if slices.Contains(ptwsToDelete, s.ptwHot) {
				s.ptwHot = nil
			}

			break
		}

		s.partitionsLock.Unlock()

		for i, ptw := range ptwsToDelete {
			var reason string
			if s.maxDiskSpaceUsageBytes > 0 {
				reason = fmt.Sprintf("-retention.maxDiskSpaceUsageBytes=%d", s.maxDiskSpaceUsageBytes)
			} else {
				reason = fmt.Sprintf("-retention.maxDiskUsagePercent=%d%%", s.maxDiskUsagePercent)
			}
			logger.Infof("the partition %s is scheduled to be deleted because the total size of partitions exceeds %s", ptw.pt.path, reason)
			ptw.mustDrop.Store(true)
			ptw.decRef()
			ptwsToDelete[i] = nil
		}

		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
	}
}

func (s *Storage) watchSnapshotsMaxAge() {
	if s.snapshotsMaxAge <= 0 {
		return
	}

	d := timeutil.AddJitterToDuration(time.Minute)
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}

		s.MustDeleteStalePartitionSnapshots(s.snapshotsMaxAge)
	}
}

func (s *Storage) watchDeleteTasks() {
	d := timeutil.AddJitterToDuration(time.Second)
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}

		var dt *DeleteTask

		s.deleteTasksLock.Lock()
		if len(s.deleteTasks) > 0 {
			dt = s.deleteTasks[0]

			// initialize dt.ctx and dt.cancel under the lock in order to avoid races
			// with canceling the task at Storage.DeleteStopTask()
			dt.ctx, dt.cancel = contextutil.NewStopChanContext(s.stopCh)
			dt.doneCh = make(chan struct{})
		}
		s.deleteTasksLock.Unlock()

		if dt == nil {
			// There are no delete tasks.
			continue
		}

		// Process delete tasks sequentially in order to limit resource usage needed for the logs' deletion.

		ok := s.processDeleteTask(dt.ctx, dt)
		close(dt.doneCh)
		dt.cancel()

		s.deleteTasksLock.Lock()

		// Set dt.ctx and dt.cancel to nil under the lock in order to avoid races
		// with canceling the task at Storage.DeleteStopTask().
		dt.ctx = nil
		dt.cancel = nil
		dt.doneCh = nil

		s.deleteTasks = s.deleteTasks[1:]
		if !ok {
			// The delete task coudn't be completed now. Try it later.
			s.deleteTasks = append(s.deleteTasks, dt)
		}
		s.mustSaveDeleteTasksLocked()

		s.deleteTasksLock.Unlock()
	}
}

// processDeleteTask processes dt.
//
// true is returned on successfully processed dt or on explicitly canceled dt.
// false is returned if dt couldn't be processed at the moment, so it must be processed later.
func (s *Storage) processDeleteTask(ctx context.Context, dt *DeleteTask) bool {
	logger.Infof("started processing delete task %s", dt)
	startTime := time.Now()

	f, err := ParseFilter(dt.Filter)
	if err != nil {
		logger.Panicf("BUG: cannot parse filter from delete task: [%s]", dt.Filter)
	}

	q := &Query{
		f:         f.f,
		timestamp: dt.StartTime.UnixNano(),
	}

	// Add time filter ending at the delete task start time.
	// This avoids deleting logs from the future.
	start := int64(math.MinInt64)
	end := dt.StartTime.UnixNano()
	q.AddTimeFilter(start, end)

	var qs QueryStats
	qctx := NewQueryContext(ctx, &qs, dt.TenantIDs, q, false, nil)

	// Initialize subqueries
	qNew, err := initSubqueries(qctx, s.runQuery, true)
	if err != nil {
		logger.Errorf("cannot process delete task with task_id=%q while initializing subqueries: %s; retrying later", dt.TaskID, err)
		return false
	}
	q = qNew

	sso := s.getSearchOptions(dt.TenantIDs, q, qctx.HiddenFieldsFilters)

	// reset fieldsFilter in order to avoid loading all the log fields
	// during search for parts which contain rows to delete, since these fields aren't needed.
	sso.fieldsFilter.Reset()

	// delete rows matching q.f
	stopCh := ctx.Done()
	if !s.deleteRows(sso, stopCh) {
		if needStop(s.stopCh) {
			logger.Infof("the storage is stopped while executing the delete task with task_id=%q; postponing the task for later execution", dt.TaskID)
			return false
		}

		if needStop(stopCh) {
			// The task has been canceled explicitly. Return true, so it isn't re-scheduled for later execution.
			logger.Infof("the delete task with task_id=%q is explicitly canceled after %.3f seconds", dt.TaskID, time.Since(startTime).Seconds())
			return true
		}

		// The task couldn't be processed at the moment
		logger.Warnf("cannot proceeed with the delete task with task_id=%q in %.3f seconds; retrying it later", dt.TaskID, time.Since(startTime).Seconds())
		return false
	}

	logger.Infof("finished processing delete task %s in %.3f seconds", dt, time.Since(startTime).Seconds())
	return true
}

func (s *Storage) deleteRows(sso *storageSearchOptions, stopCh <-chan struct{}) bool {
	ptws, ptwsDecRef := s.getPartitionsForTimeRange(sso.minTimestamp, sso.maxTimestamp)
	defer ptwsDecRef()

	// Delete rows sequentially in every partition in order to limit resource usage needed for the logs' deletion.
	ok := true
	for _, ptw := range ptws {
		if !ptw.pt.deleteRows(sso, stopCh) {
			// Return false if at least a single deletion was unsuccessful.
			// Continue deletion of rows at other partitions, since they may be successful.
			ok = false
		}
	}

	return ok
}

func (s *Storage) updateDeletedPartitionsLocked(ptwsToDelete []*partitionWrapper) {
	for _, ptw := range ptwsToDelete {
		if !slices.Contains(s.deletedPartitions, ptw.day) {
			s.deletedPartitions = append(s.deletedPartitions, ptw.day)
		}
	}
}

func (s *Storage) getMinAllowedDay(now int64) int64 {
	return (now - s.retention.Nanoseconds()) / nsecsPerDay
}

func (s *Storage) getMaxAllowedDay(now int64) int64 {
	return (now + s.futureRetention.Nanoseconds()) / nsecsPerDay
}

// MustClose closes s.
//
// It is expected that nobody uses the storage at the close time.
func (s *Storage) MustClose() {
	// Stop background workers
	close(s.stopCh)
	s.wg.Wait()

	// Close partitions
	for _, pw := range s.partitions {
		pw.decRef()
		if n := pw.refCount.Load(); n != 0 {
			logger.Panicf("BUG: there are %d users of partition", n)
		}
	}
	s.partitions = nil
	s.ptwHot = nil

	// Stop caches

	// Do not persist caches, since they may become out of sync with partitions
	// if partitions are deleted, restored from backups or copied from other sources
	// between VictoriaLogs restarts. This may result in various issues
	// during data ingestion and querying.

	s.streamIDCache.MustStop()
	s.streamIDCache = nil

	s.filterStreamCache.MustStop()
	s.filterStreamCache = nil

	// release lock file
	fs.MustClose(s.flockF)
	s.flockF = nil

	s.path = ""
}

// MustForceMerge force-merges parts in s partitions with names starting from the given partitionPrefix.
//
// Partitions are merged sequentially in order to reduce load on the system.
func (s *Storage) MustForceMerge(partitionPrefix string) {
	ptws := s.getPartitions()
	defer s.putPartitions(ptws)

	s.wg.Add(1)
	defer s.wg.Done()

	for _, ptw := range ptws {
		if !strings.HasPrefix(ptw.pt.name, partitionPrefix) {
			continue
		}

		logger.Infof("started force merge for partition %s", ptw.pt.name)
		startTime := time.Now()
		ptw.pt.mustForceMerge()
		logger.Infof("finished force merge for partition %s in %.3fs", ptw.pt.name, time.Since(startTime).Seconds())
	}
}

// MustAddRows adds lr to s.
//
// This is the primary entry point for data ingestion at the storage level.
// The function routes rows to the appropriate per-day partition(s).
//
// FAST PATH OPTIMIZATION:
// For near-real-time ingestion, most rows have timestamps close to "now" and
// belong to the same day. The fast path exploits this:
//  1. Check if the "hot" partition (ptwHot, where the last row was ingested) exists
//  2. If all rows fit within the hot partition's day, add them directly
//  3. This avoids the overhead of per-row partition lookup and timestamp validation
//
// SLOW PATH (rows spanning multiple days):
// If rows don't all fit in the hot partition (e.g., backfill, delayed logs):
//  1. Group rows by day (ts / nsecsPerDay)
//  2. For each day, get or create the appropriate partition
//  3. Validate timestamps against retention and backfill limits
//  4. Add rows to each partition
//
// TIMESTAMP VALIDATION:
// Rows with timestamps outside allowed ranges are dropped with a warning:
//   - Too old: Before (now - retention) OR before (now - maxBackfillAge)
//   - Too new: After (now + futureRetention)
//
// WHY DROP INSTEAD OF ERROR? The ingestion API is designed for high throughput.
// Returning an error would require the client to handle partial failures and retry,
// which is complex and slow. Dropping with a logged warning is simpler and allows
// the rest of the batch to succeed. Clients that need guaranteed delivery should
// implement their own validation before calling MustAddRows.
//
// It is recommended checking whether the s is in read-only mode by calling IsReadOnly()
// before calling MustAddRows.
//
// The added rows become visible for search after small duration of time.
// Call DebugFlush if the added rows must be queried immediately (for example, in tests).
func (s *Storage) MustAddRows(lr *LogRows) {
	// ==================== FAST PATH ====================
	// Try adding all rows to the hot partition (most common case for real-time ingestion)
	//
	// WHY THIS MATTERS: In a typical real-time logging scenario, 99%+ of logs have
	// timestamps within the last few seconds. The fast path handles this case in O(1)
	// without any timestamp parsing or partition lookup.

	s.partitionsLock.Lock()
	ptwHot := s.ptwHot
	if ptwHot != nil {
		ptwHot.incRef()
	}
	s.partitionsLock.Unlock()

	if ptwHot != nil {
		// Check if all rows fit within the hot partition's day
		if ptwHot.canAddAllRows(lr) {
			// Fast path succeeded - add rows and return
			ptwHot.pt.mustAddRows(lr)
			ptwHot.decRef()
			return
		}
		ptwHot.decRef()
	}

	// ==================== SLOW PATH ====================
	// Rows cannot be added to the hot partition, so split rows among available partitions
	//
	// This happens when:
	// - Batch contains rows from multiple days (e.g., during backfill)
	// - Hot partition doesn't exist yet (first write after startup)
	// - Hot partition was just detached/deleted

	now := time.Now().UnixNano()
	minAllowedDay := s.getMinAllowedDay(now)
	maxAllowedDay := s.getMaxAllowedDay(now)
	minAllowedTimestamp := now - s.maxBackfillAge.Nanoseconds()

	// Group rows by day
	// WHY group by day? Each partition represents one day, so we need to route
	// each row to its correct partition. Grouping minimizes partition lock/unlock cycles.
	m := make(map[int64]*LogRows)
	for i, ts := range lr.timestamps {
		day := ts / nsecsPerDay

		// Validate timestamp against retention (oldest allowed)
		if day < minAllowedDay {
			line := MarshalFieldsToJSON(nil, lr.rows[i])
			tsf := TimeFormatter(ts)
			minAllowedTsf := TimeFormatter(minAllowedDay * nsecsPerDay)
			tooSmallTimestampLogger.Warnf("skipping log entry with too small timestamp=%s; it must be bigger than %s according "+
				"to the configured -retentionPeriod=%dd. See https://docs.victoriametrics.com/victorialogs/#retention ; "+
				"log entry: %s", &tsf, &minAllowedTsf, durationToDays(s.retention), line)
			s.rowsDroppedTooSmallTimestamp.Add(1)
			continue
		}

		// Validate timestamp against future retention (newest allowed)
		if day > maxAllowedDay {
			line := MarshalFieldsToJSON(nil, lr.rows[i])
			tsf := TimeFormatter(ts)
			maxAllowedTsf := TimeFormatter(maxAllowedDay * nsecsPerDay)
			tooBigTimestampLogger.Warnf("skipping log entry with too big timestamp=%s; it must be smaller than %s according "+
				"to the configured -futureRetention=%dd; see https://docs.victoriametrics.com/victorialogs/#retention ; "+
				"log entry: %s", &tsf, &maxAllowedTsf, durationToDays(s.futureRetention), line)
			s.rowsDroppedTooBigTimestamp.Add(1)
			continue
		}

		// Validate timestamp against backfill limit (if configured)
		// WHY separate from retention? -maxBackfillAge is meant to prevent accidental
		// backfill of very old data, while -retentionPeriod is about data lifecycle.
		// A cluster might have 30d retention but want to reject logs older than 7d.
		if ts < minAllowedTimestamp {
			line := MarshalFieldsToJSON(nil, lr.rows[i])
			tsf := TimeFormatter(ts)
			minAllowedTsf := TimeFormatter(minAllowedTimestamp)
			tooSmallTimestampLogger.Warnf("skipping log entry with too small timestamp=%s; it must be bigger than %s according "+
				"to the configured -maxBackfillAge=%s. See https://docs.victoriametrics.com/victorialogs/#backfilling ; "+
				"log entry: %s", &tsf, &minAllowedTsf, s.maxBackfillAge, line)
			s.rowsDroppedTooSmallTimestamp.Add(1)
			continue
		}

		// Get or create the per-day LogRows batch
		lrPart := m[day]
		if lrPart == nil {
			lrPart = GetLogRows(nil, nil, nil, nil, "")
			m[day] = lrPart
		}
		lrPart.mustAddInternal(lr.streamIDs[i], ts, lr.rows[i], lr.streamTagsCanonicals[i])
	}

	// Add each day's rows to the appropriate partition
	for day, lrPart := range m {
		ptw := s.getPartitionForWriting(day)
		if ptw != nil {
			ptw.pt.mustAddRows(lrPart)
			ptw.decRef()
		} else {
			// Partition couldn't be created or is detached - drop the rows
			// WHY nil return? This happens when:
			// - Day is in deletedPartitions (retention already deleted it)
			// - Directory exists but isn't attached (was manually placed or detached)
			// In both cases, we shouldn't silently create data that won't be queryable.
			line := MarshalFieldsToJSON(nil, lrPart.rows[0])
			inactivePartitionLogger.Warnf("skipping log entry because it cannot be saved into inactive per-day partition; "+
				"see https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle; log entry %s", line)
		}
		PutLogRows(lrPart)
	}
}

var tooSmallTimestampLogger = logger.WithThrottler("too_small_timestamp", 5*time.Second)
var tooBigTimestampLogger = logger.WithThrottler("too_big_timestamp", 5*time.Second)
var inactivePartitionLogger = logger.WithThrottler("inactive_partition", 5*time.Second)

// TimeFormatter implements fmt.Stringer for timestamp in nanoseconds
type TimeFormatter int64

// String returns human-readable representation for tf.
func (tf *TimeFormatter) String() string {
	ts := int64(*tf)
	t := time.Unix(0, ts).UTC()
	return t.Format(time.RFC3339Nano)
}

// getPartitionForWriting returns the partition for the given day for writing.
//
// PARTITION LOOKUP:
//  1. Acquire partitionsLock
//  2. Binary search partitions slice (sorted by day) for the target day
//  3. If found, increment ref count and return
//  4. If not found, create a new partition
//
// PARTITION CREATION (on-demand):
// Partitions are created lazily when the first log for that day arrives.
// This avoids creating empty partition directories for days with no data.
//
// NIL RETURN CASES:
// Returns nil if the partition cannot be used for writing:
//   - Day is outside configured retention (already deleted)
//   - Partition directory exists but isn't attached (was detached via API)
//   - Partition directory was manually added but not attached
//
// The caller must log this case and drop pending logs for this partition.
func (s *Storage) getPartitionForWriting(day int64) *partitionWrapper {
	s.partitionsLock.Lock()
	defer s.partitionsLock.Unlock()

	// Binary search for the partition with this day
	ptws := s.partitions
	n := sort.Search(len(ptws), func(i int) bool {
		return ptws[i].day >= day
	})

	var ptw *partitionWrapper
	if n < len(ptws) {
		ptw = ptws[n]
		if ptw.day != day {
			ptw = nil
		}
	}

	if ptw == nil {
		// Partition doesn't exist - check if it was deleted or needs creation
		if slices.Contains(s.deletedPartitions, day) {
			// Partition was already deleted due to retention - don't recreate
			return nil
		}

		fname := getPartitionNameFromDay(day)
		partitionPath := filepath.Join(s.path, partitionsDirname, fname)
		if fs.IsPathExist(partitionPath) {
			// Directory exists but partition isn't attached - can happen if:
			// - Partition was detached via PartitionDetach() API
			// - Directory was manually copied but not attached
			return nil
		}

		// Create new partition on-demand
		mustCreatePartition(partitionPath)
		pt := mustOpenPartition(s, partitionPath)
		ptw = newPartitionWrapper(pt, day)

		// Insert into sorted partitions slice at position n
		if n == len(ptws) {
			ptws = append(ptws, ptw)
		} else {
			ptws = append(ptws[:n+1], ptws[n:]...)
			ptws[n] = ptw
		}
		s.partitions = ptws
	}

	// Cache as hot partition for fast path in MustAddRows
	s.ptwHot = ptw
	ptw.incRef()

	return ptw
}

// UpdateStats updates ss for the given s.
func (s *Storage) UpdateStats(ss *StorageStats) {
	ss.RowsDroppedTooBigTimestamp += s.rowsDroppedTooBigTimestamp.Load()
	ss.RowsDroppedTooSmallTimestamp += s.rowsDroppedTooSmallTimestamp.Load()
	if s.maxDiskSpaceUsageBytes > 0 {
		ss.MaxDiskSpaceUsageBytes = s.maxDiskSpaceUsageBytes
	} else {
		ss.MaxDiskSpaceUsageBytes = int64(fs.MustGetTotalSpace(s.path) * uint64(s.maxDiskUsagePercent) / 100)
	}
	// Use sentinel values to indicate unbounded / no data for consistency
	ss.MinTimestamp, ss.MaxTimestamp = math.MinInt64, math.MaxInt64

	s.partitionsLock.Lock()
	ss.PartitionsCount += uint64(len(s.partitions))
	for _, ptw := range s.partitions {
		ptw.pt.updateStats(&ss.PartitionStats)
	}

	if len(s.partitions) > 0 {
		p0 := s.partitions[0]
		pLast := s.partitions[len(s.partitions)-1]

		ss.MinTimestamp, _ = p0.pt.ddb.getMinMaxTimestamps()
		_, ss.MaxTimestamp = pLast.pt.ddb.getMinMaxTimestamps()
	}
	s.partitionsLock.Unlock()

	ss.IsReadOnly = s.IsReadOnly()
}

// IsReadOnly returns true if s is in read-only mode.
func (s *Storage) IsReadOnly() bool {
	available := fs.MustGetFreeSpace(s.path)
	return available < s.minFreeDiskSpaceBytes
}

// DebugFlush flushes all the buffered rows, so they become visible for search.
//
// This function is for debugging and testing purposes only, since it is slow.
func (s *Storage) DebugFlush() {
	ptws := s.getPartitions()
	defer s.putPartitions(ptws)

	for _, ptw := range ptws {
		ptw.pt.debugFlush()
	}
}

func (s *Storage) getPartitions() []*partitionWrapper {
	s.partitionsLock.Lock()
	ptws := append([]*partitionWrapper{}, s.partitions...)
	for _, ptw := range ptws {
		// Caller receives borrowed partitions and must return them via putPartitions().
		ptw.incRef()
	}
	s.partitionsLock.Unlock()

	return ptws
}

func (s *Storage) putPartitions(ptws []*partitionWrapper) {
	for _, ptw := range ptws {
		// Release refs acquired by getPartitions().
		ptw.decRef()
	}
}

func durationToDays(d time.Duration) int64 {
	return int64(d / (time.Hour * 24))
}
