package kubernetescollector

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// processor processes log lines from a single container log file.
//
// The interface allows accumulating multiple lines within a file before
// committing them to the checkpointsDB. This is essential for handling:
//   - Multi-line log entries (e.g., Java stack traces) that span multiple lines
//   - CRI partial log entries where containerd splits lines at 16 KiB
//   - Batching multiple log lines for efficiency
//
// The checkpoint is only updated when tryAddLine returns true, ensuring we
// don't checkpoint in the middle of a multi-line entry.
type processor interface {
	// tryAddLine processes a log line and returns whether it should be committed.
	//
	// Returns true if the current line should be committed to checkpointsDB.
	// Returns false for partial lines that need more data before committing.
	//
	// This design ensures that if vlagent crashes in the middle of a multi-line
	// log entry, we'll re-read the entire entry on restart rather than having
	// half an entry checkpointed.
	//
	// Note: when a log file is rotated, no checkpoint will be written until
	// tryAddLine returns true, ensuring multi-line entries spanning multiple
	// files are handled correctly.
	tryAddLine(line []byte) bool

	// mustClose releases all resources associated with the processor.
	// It must be called after the target log file is deleted or vlagent is shutting down.
	mustClose()
}

// fileCollector manages log file reading for all containers on a node.
//
// It maintains a map of all log files being tracked and spawns a dedicated
// goroutine for each file. The goroutine runs for the lifetime of the container,
// handling file reading, rotation detection, and checkpoint persistence.
//
// Key responsibilities:
//   - Track which files are being read (deduplication)
//   - Apply exclude filter to skip unwanted containers
//   - Resume from checkpoints on restart
//   - Handle log file rotation (when kubelet rotates logs)
//   - Clean up when containers are deleted
type fileCollector struct {
	// logFiles tracks all log files currently being read.
	// Used for deduplication - we don't want to tail the same file twice.
	logFiles     map[string]struct{}
	logFilesLock sync.Mutex

	// excludeFilter is a LogsQL filter for excluding containers.
	// It matches against common metadata fields (kubernetes.container_name,
	// kubernetes.pod_namespace, etc.) BEFORE reading the log file.
	// This is more efficient than filtering after reading.
	excludeFilter *logstorage.Filter

	// newProcessor is a factory function that creates a processor for each log file.
	// Each processor gets the container's Kubernetes metadata as commonFields.
	newProcessor func(commonFields []logstorage.Field) processor

	// checkpointsDB persists read offsets for crash recovery.
	checkpointsDB *checkpointsDB

	// wg tracks all goroutines spawned by this collector.
	wg sync.WaitGroup

	// stopCh signals all goroutines to stop.
	stopCh chan struct{}
}

// startFileCollector creates and starts a new file collector.
//
// The fileCollector maintains a checkpoint file that serves as persistent state.
// This allows resuming log reading from the exact position where it was interrupted
// when vlagent is restarted, preventing:
//   - Log duplication (re-reading from beginning)
//   - Log loss (missing lines that were written but not yet checkpointed)
//
// The caller must call stop() when the fileCollector is no longer needed.
func startFileCollector(checkpointsPath string, excludeFilter *logstorage.Filter, newProcessor func(commonFields []logstorage.Field) processor) *fileCollector {
	// Load checkpoints from disk.
	// This may fail if the file is corrupted, in which case we panic.
	checkpointsDB, err := startCheckpointsDB(checkpointsPath)
	if err != nil {
		logger.Panicf("FATAL: cannot start checkpoints DB: %s", err)
	}

	return &fileCollector{
		logFiles:      make(map[string]struct{}),
		excludeFilter: excludeFilter,
		newProcessor:  newProcessor,
		checkpointsDB: checkpointsDB,
		stopCh:        make(chan struct{}),
	}
}

// startRead begins tailing a log file if not already doing so.
//
// This function is idempotent - if the file is already being read, it's a no-op.
// This is important because Kubernetes may send multiple ADDED/MODIFIED events
// for the same pod.
//
// Each file gets its own goroutine that runs for the lifetime of the container.
// The goroutine handles:
//   - Reading new lines as they're written
//   - Detecting and handling log rotation
//   - Updating checkpoints
//   - Cleaning up when the file is deleted
func (fc *fileCollector) startRead(filepath string, commonFields []logstorage.Field) {
	fc.logFilesLock.Lock()
	_, ok := fc.logFiles[filepath]
	fc.logFiles[filepath] = struct{}{}
	fc.logFilesLock.Unlock()
	if ok {
		// Already reading from the file - nothing to do.
		// This happens when we get multiple events for the same pod.
		return
	}

	// Spawn a goroutine to handle this file for its entire lifetime.
	fc.wg.Go(func() {
		lf := fc.openLogFile(filepath)
		fc.process(lf, commonFields)
	})
}

// openLogFile opens a log file, optionally resuming from a checkpoint.
//
// If a checkpoint exists for this file, we attempt to resume from the saved
// position. This handles several edge cases:
//   - File was not rotated: Resume from saved offset
//   - File was rotated while vlagent was down: Find the old file by inode
//   - File was deleted: Start fresh from the beginning
func (fc *fileCollector) openLogFile(filepath string) *logFile {
	cp, ok := fc.checkpointsDB.get(filepath)
	if !ok {
		// No checkpoint found - start reading from the beginning of the file.
		return newLogFile(filepath)
	}

	// Attempt to resume from the checkpoint.
	// This handles rotation detection and fingerprint validation.
	lf, ok := tryResumeFromCheckpoint(filepath, cp)
	if !ok {
		// Could not resume (file rotated/deleted) - delete the stale checkpoint
		// and start fresh from the beginning.
		fc.checkpointsDB.delete(filepath)
		return newLogFile(filepath)
	}
	return lf
}

// tryResumeFromCheckpoint attempts to resume reading from a saved checkpoint.
//
// This function handles the complex scenarios that can occur when vlagent
// has been down and the log files may have changed:
//
//  1. File unchanged: The inode matches, we can seek to the saved offset
//
//  2. File rotated: The inode changed, but the old file may still exist
//     with a different name (e.g., with a timestamp suffix). We search
//     the directory for the old inode to finish reading it.
//
//  3. File fingerprint changed: Even if the inode matches, the content may
//     be different (inode reuse). We verify by comparing fingerprints
//     (xxhash of first line).
//
// Returns (logFile, true) if resumption succeeded, (nil, false) otherwise.
func tryResumeFromCheckpoint(filepath string, cp checkpoint) (*logFile, bool) {
	// Try to open the file at the checkpointed path.
	f, inode, ok := openFileWithInode(cp.Path)
	if !ok {
		// The file was deleted just after startRead was called.
		logger.Warnf("log file %q was deleted before being fully read; "+
			"this is expected if the Pod was deleted while vlagent was starting", filepath)
		return nil, false
	}

	if inode != cp.Inode {
		// The inode changed - the file was rotated while vlagent was down.
		_ = f.Close()

		// When kubelet rotates log files, it keeps the previous log file
		// uncompressed in the same directory with a different name.
		// We attempt to find this renamed file to continue reading from our last offset.
		// See: https://github.com/kubernetes/kubernetes/blob/f794aa12d78f5b1f9579ce8a991a116a99a2c43c/pkg/kubelet/logs/container_log_manager.go#L416
		var ok bool
		f, ok = findRenamedFile(cp.Path, cp.Inode)
		if !ok {
			// Could not find the rotated file with matching inode.
			logger.Warnf("skipping log file %q: rotated log file not found (inode=%d); "+
				"some log lines may have been lost; "+
				"this typically happens when Pod logs rotate faster than vlagent can process them during startup or downtime; "+
				"consider increasing kubelet's --container-log-max-size to reduce log rotation frequency",
				filepath, cp.Inode)
			return nil, false
		}
	}

	// Verify the file fingerprint hasn't changed.
	// This catches inode reuse where a new file has the same inode as our old file.
	fp := getFileFingerprint(f)
	if fp == 0 || cp.Fingerprint != 0 && cp.Fingerprint != fp {
		logger.Warnf("skipping log file %q: file content changed unexpectedly (expected fingerprint=%d, got=%d); "+
			"log file was likely rotated and truncated before vlagent could finish reading; "+
			"some log lines may have been lost; "+
			"this typically happens when Pod logs rotate faster than vlagent can process them during startup or downtime; "+
			"consider increasing kubelet's --container-log-max-size to reduce log rotation frequency",
			filepath, cp.Fingerprint, fp)
		return nil, false
	}

	// Create the logFile and seek to the checkpointed offset.
	logfile, err := newLogFileFromFile(f, fp, cp.Path)
	if err != nil {
		logger.Panicf("FATAL: cannot create log file: %s", err)
	}
	logfile.setOffset(cp.Offset)

	return logfile, true
}

// getFileFingerprint returns a fingerprint (hash) of the first line of the file.
//
// This is used to detect if a file's content has changed, which can happen
// when:
//   - The file was truncated and rewritten
//   - An inode was reused for a different file
//
// The fingerprint is a xxhash of the first 64 bytes of the first line.
// 64 bytes is sufficient because Container Runtime log lines start with
// a timestamp with nanosecond precision, making them unique.
//
// Returns 0 if the file doesn't contain any complete lines yet.
func getFileFingerprint(f *os.File) uint64 {
	buf := make([]byte, maxFingerprintDataLen)
	n, err := f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		logger.Panicf("FATAL: cannot read file %q: %s", f.Name(), err)
	}

	// Find the end of the first line.
	nl := bytes.IndexByte(buf[:n], '\n')
	if nl < 0 && n < len(buf) {
		// Line is not yet fully written - cannot calculate fingerprint.
		// This happens when the file is being written to.
		return 0
	}
	if nl >= 0 {
		buf = buf[:nl]
	}

	fp := calcFingerprint(buf)
	return fp
}

// process is the main loop for reading a single log file.
//
// This function runs for the lifetime of the container. It:
//  1. Checks the exclude filter - skip if this container should be excluded
//  2. Reads new lines from the file
//  3. Updates checkpoints after successful reads
//  4. Handles file rotation (when kubelet rotates the log)
//  5. Cleans up when the file is deleted
//
// The loop uses exponential backoff (100ms-10s) when no new lines are available
// to avoid busy-waiting on idle containers.
func (fc *fileCollector) process(lf *logFile, commonFields []logstorage.Field) {
	defer lf.close()

	// Check if this container should be excluded based on metadata.
	// This is checked BEFORE reading to save I/O.
	if fc.excludeFilter != nil && fc.excludeFilter.MatchRow(commonFields) {
		// Filter matches - skip this file entirely.
		fc.forgetFile(lf.path)
		return
	}

	// Backoff timer for idle periods.
	// Starts at 100ms, doubles on each idle check, caps at 10s.
	bt := newBackoffTimer(time.Millisecond*100, time.Second*10)
	defer bt.stop()

	// Create a processor for this file's log lines.
	proc := fc.newProcessor(commonFields)
	defer proc.mustClose()

	for {
		if needStop(fc.stopCh) {
			// vlagent is shutting down - stop reading.
			return
		}

		// Try to read new lines from the file.
		ok := lf.readLines(fc.stopCh, proc)
		if ok {
			// Some lines were read - update checkpoint and wait before checking again.
			fc.checkpointsDB.set(lf.checkpoint())
			bt.reset() // Reset backoff since we made progress.
			bt.wait(fc.stopCh)
			continue
		}

		// No lines read - check the log file status to determine why.
		switch lf.status() {
		case logFileStatusNotRotated:
			// No more lines to read and file hasn't rotated.
			// The container just hasn't written anything new.
			// Wait with increasing backoff before checking again.
			bt.wait(fc.stopCh)
			continue
		case logFileStatusRotated:
			// File was rotated - kubelet created a new log file.
			// We need to drain the remaining lines from the old file
			// before switching to the new one.
			//
			// IMPORTANT: We use nil stopCh here (neverStopCh pattern) to ensure
			// we finish reading the rotated file even during shutdown.
			// This prevents data loss on graceful shutdown.
			var neverStopCh chan struct{}
			bt.reset()
			bt.wait(neverStopCh)

			// Drain remaining lines from the old file.
			if lf.readLines(neverStopCh, proc) {
				// Double-check: if there are still new lines, something is wrong.
				// A rotated file should not be appended to.
				bt.wait(neverStopCh)
				if lf.readLines(neverStopCh, proc) {
					logger.Panicf("BUG: log file %q was appended after rotation", lf.path)
				}
			}

			// Reopen the file to read from the new log file.
			if lf.tryReopen() {
				fc.checkpointsDB.set(lf.checkpoint())
			} else {
				// Cannot reopen the file right now - wait before retrying.
				bt.wait(fc.stopCh)
			}
			continue
		case logFileStatusDeleted:
			// The file was deleted - the container/pod was removed.
			fc.forgetFile(lf.path)

			// Sanity check: tail should be empty when file is deleted.
			if lf.tail != nil {
				logger.Panicf("BUG: tail must be empty when the log file no longer exists; got: %q", lf.tail.B)
			}
			return
		default:
			logger.Panicf("BUG: unexpected log file status")
		}
	}
}

// forgetFile removes a file from tracking and deletes its checkpoint.
//
// This is called when:
//   - The file is deleted (container/pod removed)
//   - The file matches the exclude filter
//
// The checkpoint is deleted because the file is not expected to reappear
// with the same content, so its state no longer needs to be persisted.
func (fc *fileCollector) forgetFile(filePath string) {
	fc.checkpointsDB.delete(filePath)

	fc.logFilesLock.Lock()
	defer fc.logFilesLock.Unlock()
	delete(fc.logFiles, filePath)
}

// findRenamedFile searches for a file with the given inode in the log directory.
//
// When kubelet rotates logs, it renames the old file (e.g., app.log -> app.2024-01-15.log).
// This function searches for the old file by inode so we can finish reading it.
//
// This is important for crash recovery: if vlagent crashes and restarts while
// a log file was being read, the file may have been rotated. We need to find
// and finish reading the old file before switching to the new one.
func findRenamedFile(logPath string, inode uint64) (*os.File, bool) {
	// Resolve the symlink to get the actual directory.
	// /var/log/containers/<name>.log -> /var/log/pods/<namespace>_<pod>_<uid>/<container>/N.log
	actualPath := tryResolveSymlink(logPath)

	dir := path.Dir(actualPath)
	des, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false
		}
		logger.Panicf("FATAL: cannot read dir %q: %s", dir, err)
	}

	// Search all files in the directory for one with the matching inode.
	for _, de := range des {
		if de.IsDir() {
			continue
		}

		fileName := de.Name()
		// Skip compressed files - they're already rotated and compressed.
		if strings.HasSuffix(fileName, ".gz") {
			continue
		}

		filePath := path.Join(dir, fileName)
		file, fileInode, ok := openFileWithInode(filePath)
		if !ok {
			continue
		}

		if fileInode == inode {
			// Found the file with the matching inode.
			return file, true
		}

		_ = file.Close()
	}

	return nil, false
}

// cleanupCheckpoints removes checkpoints for files that are no longer being processed.
//
// This is called during initialization after listing current pods.
// Any checkpoint for a pod that no longer exists is stale and should be removed.
//
// This prevents checkpoint file bloat from old pods that were deleted while
// vlagent was down.
func (fc *fileCollector) cleanupCheckpoints() {
	unusedCheckpoints := fc.getUnusedCheckpoints()
	if len(unusedCheckpoints) == 0 {
		return
	}

	for _, cp := range unusedCheckpoints {
		fc.checkpointsDB.delete(cp.Path)
	}

	logger.Warnf("%d log files were deleted before being fully read; "+
		"this is expected if Pods were deleted while vlagent was restarting; "+
		"an example of such file: %q", len(unusedCheckpoints), unusedCheckpoints[0].Path)
}

// getUnusedCheckpoints returns checkpoints for files not currently being tracked.
func (fc *fileCollector) getUnusedCheckpoints() []checkpoint {
	cps := fc.checkpointsDB.getAll()

	fc.logFilesLock.Lock()
	defer fc.logFilesLock.Unlock()

	var unused []checkpoint
	for _, cp := range cps {
		if _, ok := fc.logFiles[cp.Path]; ok {
			continue
		}
		unused = append(unused, cp)
	}
	return unused
}

// stop gracefully shuts down the file collector.
//
// This:
//  1. Closes stopCh to signal all goroutines to stop
//  2. Waits for all goroutines to finish
//  3. Stops the checkpoint database (flushes to disk)
func (fc *fileCollector) stop() {
	close(fc.stopCh)
	fc.wg.Wait()
	fc.checkpointsDB.stop()
}

// needStop checks if the stop channel has been closed.
// This is a non-blocking check used to avoid unnecessary work during shutdown.
func needStop(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// openFileWithInode opens a file and returns its inode.
//
// The inode is needed for:
//   - Detecting file rotation (inode changes)
//   - Finding renamed files during crash recovery
//
// Returns (nil, 0, false) if the file doesn't exist.
func openFileWithInode(p string) (*os.File, uint64, bool) {
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, false
		}
		logger.Panicf("FATAL: cannot open file %q: %s", p, err)
	}

	fi, err := f.Stat()
	if err != nil {
		logger.Panicf("FATAL: cannot stat file %q: %s", p, err)
	}
	inode := getInode(fi)

	return f, inode, true
}

// tryResolveSymlink resolves a symlink to its target path.
//
// If the symlink cannot be resolved (e.g., it's broken or not a symlink),
// the original path is returned.
//
// This is used to find the actual log file directory when searching for
// rotated files by inode.
func tryResolveSymlink(symlink string) string {
	resolvedPath, err := os.Readlink(symlink)
	if err != nil {
		return symlink
	}
	return resolvedPath
}
