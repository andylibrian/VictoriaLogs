package kubernetescollector

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs/fsutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/cespare/xxhash/v2"
)

// maxLogLineSize is the maximum log line size that VictoriaLogs can accept.
// Lines larger than this are truncated with a warning.
// See: https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have
const maxLogLineSize = 2 * 1024 * 1024

// logFile represents a single container log file being tailed.
//
// It maintains state for:
//   - The file handle and its inode (for rotation detection)
//   - The current read offset
//   - A fingerprint of the first line (for inode reuse detection)
//   - A tail buffer for incomplete lines at read boundaries
//
// The commit* fields track the last position that was successfully committed
// to the checkpoint database, which may be behind the current read position
// when processing multi-line log entries.
type logFile struct {
	// path is the symlink path in /var/log/containers/.
	// This is used for checkpoint persistence and file status checks.
	path string

	// file is the open file handle. May be nil if the file hasn't been opened yet.
	file *os.File

	// inode tracks the inode of the underlying file.
	// It is used to detect file rotations - when kubelet rotates logs,
	// it creates a new file with a different inode.
	//
	// It is unexpected for multiple log files in the same mount point to have
	// the same inode while vlagent is running, because vlagent keeps the current
	// file open until Kubernetes creates a new log file to handle rotation.
	// See fingerprint to distinguish files with the same inode.
	inode uint64

	// fingerprint contains a hash of the first line of the file.
	// It helps distinguish files with the same inode, which can happen if
	// an inode is reused while vlagent is down.
	//
	// This is calculated from the first 64 bytes of the first line using xxhash.
	fingerprint uint64

	// offset tracks the current read position in the file.
	// This is updated as lines are read, even before they're committed.
	offset int64

	// commitInode, commitFingerprint, and commitOffset track the last
	// position that was successfully committed to the checkpoint database.
	//
	// These may differ from inode/fingerprint/offset when:
	// - Processing multi-line log entries (checkpoint only on complete entry)
	// - Processing CRI partial lines (checkpoint only when final part received)
	commitInode       uint64
	commitFingerprint uint64
	commitOffset      int64

	// tail contains the last incomplete line read from the file.
	// This happens when a read ends in the middle of a line.
	// The tail is prepended to the next read's data to form complete lines.
	//
	// Can be truncated if it exceeds maxLogLineSize.
	tail *bytesutil.ByteBuffer

	// tailSize tracks the actual size of the tail buffer.
	// This may differ from len(tail.B) if the line was truncated.
	tailSize int
}

// newLogFile creates a logFile for a file that hasn't been opened yet.
// The file will be opened on the first read attempt.
func newLogFile(symlink string) *logFile {
	return &logFile{
		path: symlink,
	}
}

// newLogFileFromFile creates a logFile from an already-open file.
// This is used when resuming from a checkpoint.
func newLogFileFromFile(f *os.File, fingerprint uint64, symlink string) (*logFile, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cannot get file info of %q: %w", f.Name(), err)
	}
	inode := getInode(fi)

	lf := newLogFile(symlink)
	lf.file = f
	lf.inode = inode
	lf.commitInode = inode
	lf.fingerprint = fingerprint
	lf.commitFingerprint = fingerprint

	return lf, nil
}

// readByteBufferPool provides reusable buffers for file reading.
// Using 256KB buffers balances memory usage with read efficiency.
var readByteBufferPool = sync.Pool{
	New: func() any {
		return &bytesutil.ByteBuffer{B: make([]byte, 256*1024)}
	},
}

// Concurrency limiters for file reading and processing.
// These prevent resource exhaustion when many containers are logging heavily.
var (
	// readConcurrencyCh limits concurrent file read syscalls.
	// This is shared across all log files to prevent too many concurrent disk reads.
	readConcurrencyCh = fsutil.GetConcurrencyCh()

	// processConcurrencyCh limits concurrent line processing.
	// This is CPU-bound, so we limit to the number of available CPUs.
	// Processing includes JSON parsing, CRI parsing, and field extraction.
	processConcurrencyCh = make(chan struct{}, cgroup.AvailableCPUs())
)

// readLines reads new lines from the file and processes them.
//
// This function:
//  1. Opens the file if not already open
//  2. Reads data in 256KB chunks
//  3. Splits data into lines (handling incomplete lines at boundaries)
//  4. Processes each line through the processor
//
// Returns true if any lines were read, false otherwise.
// Returns immediately if stopCh is closed.
//
// Concurrency is limited by readConcurrencyCh (for file I/O) and
// processConcurrencyCh (for CPU-bound processing).
func (lf *logFile) readLines(stopCh <-chan struct{}, proc processor) bool {
	if lf.file == nil {
		// This happens on the first read attempt.
		// File may not exist in the case of races with Container Runtime or OS.
		if !lf.tryReopen() {
			return false
		}
	}

	// Get a buffer from the pool for this read.
	readBuf := readByteBufferPool.Get().(*bytesutil.ByteBuffer)
	defer readByteBufferPool.Put(readBuf)

	anyRead := false

	for {
		if needStop(stopCh) {
			return anyRead
		}

		// Limit concurrent file reads across all log files.
		readConcurrencyCh <- struct{}{}
		n, err := lf.file.Read(readBuf.B)
		<-readConcurrencyCh

		if err != nil {
			if err == io.EOF {
				// No more data available right now.
				// This is normal - the container just hasn't written anything new.
				return anyRead
			}
			logger.Panicf("FATAL: cannot read from file %q: %s", lf.path, err)
		}

		if n > 0 {
			anyRead = true
		}

		// Limit concurrent line processing across all log files.
		// This is CPU-bound work (parsing JSON, extracting fields).
		processConcurrencyCh <- struct{}{}
		lf.processLines(readBuf.B[:n], proc)
		<-processConcurrencyCh

		if n < len(readBuf.B) {
			// Read less than the buffer size - likely at EOF.
			// Stop reading and let the caller decide when to check again.
			return anyRead
		}
	}
}

// processLines splits data into lines and processes each one.
//
// This function handles the complexity of lines that span read boundaries:
//  1. If there's a tail from the previous read, try to complete it
//  2. Process all complete lines
//  3. Save any remaining incomplete data as the new tail
//
// The tail buffer ensures we never process partial lines.
func (lf *logFile) processLines(data []byte, p processor) {
	if len(data) == 0 {
		return
	}

	// Handle incomplete line from the previous read.
	data, tail, ok := lf.tryCompleteTail(data)
	if !ok {
		// Line is not completed yet - need more data.
		return
	}

	if len(tail) > 0 {
		lf.addLine(p, tail)
	}

	// Process complete lines.
	for {
		n := bytes.IndexByte(data, '\n')
		if n < 0 {
			break
		}

		line := data[:n]
		data = data[n+1:]

		lf.addLine(p, line)
	}

	// Save the new incomplete line for the next read.
	lf.setTail(data)
}

// tryCompleteTail attempts to complete a partial line from the previous read.
//
// If there's a tail buffer from a previous incomplete line, this function:
//  1. Looks for a newline in the new data to complete the line
//  2. If found, combines the tail with the new data and returns the complete line
//  3. If not found, appends the new data to the tail and returns false
//
// Lines exceeding maxLogLineSize are truncated with a warning.
// This is unexpected in default Kubernetes installations since containerd
// splits log lines into 16 KiB chunks by default.
//
// Returns (remainingData, completedTail, success).
func (lf *logFile) tryCompleteTail(data []byte) ([]byte, []byte, bool) {
	if lf.tailSize == 0 {
		// Nothing to complete.
		return data, nil, true
	}

	// Look for a newline to complete the tail.
	n := bytes.IndexByte(data, '\n')
	if n < 0 {
		// Tail is not finished yet - append the new data.
		lf.tailSize += len(data)
		if lf.tailSize <= maxLogLineSize {
			lf.tail.B = append(lf.tail.B, data...)
		}
		return nil, nil, false
	}

	// Found the newline - extract the tail end and remaining data.
	tailEnd := data[:n]
	data = data[n+1:]

	lf.tailSize += len(tailEnd)
	if lf.tailSize > maxLogLineSize {
		// Discard the too large log line.
		//
		// This is unexpected in default Kubernetes installations since
		// containerd splits log lines into 16 KiB chunks by default (criLine.partial will be true for such lines).
		// See: https://github.com/containerd/containerd/blob/f37f951f5601b309e3b31fadf66991625370f7ba/docs/cri/config.md?plain=1#L399-L402
		logger.Warnf("log line from file %q with size %d bytes exceeds maximum allowed size of %d MiB",
			lf.path, lf.tailSize, maxLogLineSize/1024/1024)

		// Still need to track the offset even for truncated lines.
		if lf.offset == 0 {
			// This is the first line of the current file.
			lf.fingerprint = calcFingerprint(lf.tail.B)
		}
		lf.offset += int64(lf.tailSize + len("\n"))

		lf.tailSize = 0
		lf.tail.B = lf.tail.B[:0]

		return data, nil, true
	}

	// Complete the tail by appending the final piece.
	lf.tail.B = append(lf.tail.B, tailEnd...)
	tail := lf.tail.B

	// Reset tail state.
	lf.tailSize = 0
	lf.tail.B = lf.tail.B[:0]

	return data, tail, true
}

// setTail stores an incomplete line for the next read.
// This is called when a read ends in the middle of a line.
func (lf *logFile) setTail(tail []byte) {
	if lf.tailSize > 0 {
		logger.Panicf("BUG: cannot set tail when previous tail is not empty")
	}

	if len(tail) == 0 {
		// No tail - release the buffer back to the pool.
		if lf.tail != nil {
			tailByteBufferPool.Put(lf.tail)
			lf.tail = nil
		}
		lf.tailSize = 0
		return
	}

	// Get a buffer from the pool if needed.
	if lf.tail == nil {
		lf.tail = tailByteBufferPool.Get()
	}

	lf.tailSize = len(tail)
	lf.tail.B = append(lf.tail.B[:0], tail...)
}

// tailByteBufferPool provides buffers for incomplete line storage.
var tailByteBufferPool bytesutil.ByteBufferPool

// addLine processes a single complete log line.
//
// This function:
//  1. Updates the fingerprint if this is the first line
//  2. Updates the read offset
//  3. Sends the line to the processor
//  4. Updates the commit position if the processor indicates the line should be committed
//
// The commit position is only updated when tryAddLine returns true, which
// ensures we don't checkpoint in the middle of a multi-line entry.
func (lf *logFile) addLine(p processor, line []byte) {
	// Calculate fingerprint from the first line.
	if lf.offset == 0 {
		// This is the first line of the current file.
		lf.fingerprint = calcFingerprint(line)
	}

	// Update the read offset (including the newline that was already stripped).
	lf.offset += int64(len(line) + len("\n"))

	// Process the line and check if it should be committed.
	if p.tryAddLine(line) {
		// The processor indicates this line should be committed to the checkpoint.
		// Update the commit position to the current read position.
		lf.commitInode = lf.inode
		lf.commitFingerprint = lf.fingerprint
		lf.commitOffset = lf.offset
	}
}

// maxFingerprintDataLen is the maximum length of the first line used for fingerprinting.
// 64 bytes is sufficient because Container Runtime log lines start with a timestamp
// with nanosecond precision, making them unique across files.
const maxFingerprintDataLen = 64

// calcFingerprint calculates a fingerprint (hash) of the given data.
//
// The fingerprint is used to detect if a file's content has changed, which
// can happen when an inode is reused for a different file (common on Linux
// when files are deleted and created rapidly).
//
// Uses xxhash for fast hashing. Returns 1 if the hash would be 0 (reserved value).
func calcFingerprint(data []byte) uint64 {
	if len(data) > maxFingerprintDataLen {
		data = data[:maxFingerprintDataLen]
	}
	h := xxhash.Sum64(data)
	if h == 0 {
		// 0 hash is reserved to indicate no hash calculated.
		h = 1
	}
	return h
}

// logFileStatus represents the current state of a log file.
type logFileStatus byte

const (
	// logFileStatusNotRotated means the file is the same (inode matches)
	// and we're just waiting for more data to be written.
	logFileStatusNotRotated logFileStatus = iota

	// logFileStatusRotated means the file was rotated by kubelet.
	// A new file exists at the same path with a different inode.
	// We need to drain the old file and switch to the new one.
	logFileStatusRotated

	// logFileStatusDeleted means the symlink no longer exists.
	// The container/pod was deleted and we should stop tracking this file.
	logFileStatusDeleted
)

// status reports the current status of the log file.
//
// This is used to determine what action to take when no new lines are available:
//   - NotRotated: Just wait for more data
//   - Rotated: Drain the old file and switch to the new one
//   - Deleted: Stop tracking this file
//
// The status is determined by:
//  1. Checking if the symlink exists (deleted if not)
//  2. Checking if the target file exists (not rotated if symlink exists but target is gone)
//  3. Comparing inodes (rotated if different)
//  4. Checking if the new file has data (may be empty right after rotation)
func (lf *logFile) status() logFileStatus {
	// First check if the symlink itself exists.
	if !symlinkExists(lf.path) {
		// The symlink itself does not exist - pod was deleted.
		return logFileStatusDeleted
	}

	// Check if the target file exists.
	stat, exists := mustStat(lf.path)
	if !exists {
		// The symlink exists, but the target file does not.
		// This can happen during rotation when kubelet hasn't created the new file yet.
		// Treat as not rotated because the file can be created at any moment.
		return logFileStatusNotRotated
	}

	// Compare inodes to detect rotation.
	newInode := getInode(stat)
	if lf.inode == newInode {
		// Same inode - file has not been rotated.
		return logFileStatusNotRotated
	}

	// Different inode - file was rotated.
	// But wait - if the new file is empty, kubelet hasn't switched to it yet.
	if stat.Size() == 0 {
		// The file has been created, but Container Runtime hasn't switched to it yet.
		return logFileStatusNotRotated
	}

	return logFileStatusRotated
}

// setOffset moves the read position to a specific offset.
// This is used when resuming from a checkpoint.
func (lf *logFile) setOffset(offset int64) {
	if lf.fingerprint == 0 {
		logger.Panicf("BUG: cannot set offset when no fingerprint is set")
	}

	lf.offset = offset
	if _, err := lf.file.Seek(offset, io.SeekStart); err != nil {
		logger.Panicf("FATAL: cannot seek to offset %d in file %q: %s", offset, lf.file.Name(), err)
	}

	// Initialize commit position to the resumed offset.
	lf.commitInode = lf.inode
	lf.commitFingerprint = lf.fingerprint
	lf.commitOffset = offset
}

// tryReopen closes the current file and opens a new one at the same path.
// This is used after rotation to switch to the new log file.
//
// Returns true if the file was successfully opened, false if it doesn't exist yet.
func (lf *logFile) tryReopen() bool {
	newFile, newInode, exists := openFileWithInode(lf.path)
	if !exists {
		return false
	}

	lf.close()

	lf.file = newFile
	lf.fingerprint = 0 // Will be calculated from the first line.
	lf.inode = newInode
	lf.offset = 0 // Start from the beginning of the new file.

	return true
}

// close releases the file handle.
func (lf *logFile) close() {
	if lf.file == nil {
		return
	}

	_ = lf.file.Close()
	lf.file = nil
}

// checkpoint returns the current checkpoint state for this file.
// This is called to persist the read position to disk.
func (lf *logFile) checkpoint() checkpoint {
	return checkpoint{
		Path:        lf.path,
		Inode:       lf.commitInode,
		Offset:      lf.commitOffset,
		Fingerprint: lf.commitFingerprint,
	}
}

// mustStat returns file info for the given path.
// Returns (nil, false) if the file doesn't exist.
// Panics on other errors.
func mustStat(path string) (os.FileInfo, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false
		}
		logger.Panicf("FATAL: cannot get file info of %q: %s", path, err)
	}
	return fi, true
}

// symlinkExists checks if a symlink exists at the given path.
// Uses Lstat to check the symlink itself, not its target.
func symlinkExists(path string) bool {
	_, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false
		}
		logger.Panicf("FATAL: cannot get symlink info of %q: %s", path, err)
	}
	return true
}
