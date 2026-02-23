package kubernetescollector

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// checkpointsDB manages persistent log file reading state checkpoints.
//
// The checkpoint system is essential for crash recovery. When vlagent restarts:
//  1. It loads checkpoints from disk
//  2. For each file, it attempts to resume from the saved offset
//  3. If the file was rotated while vlagent was down, it tries to find the old file
//
// This prevents:
//   - Log duplication (re-reading from the beginning)
//   - Log loss (missing lines that were written but not checkpointed)
//
// The checkpoints are persisted to a JSON file for human readability and debugging.
// Atomic writes prevent corruption from partial writes during crashes.
type checkpointsDB struct {
	// checkpointsPath is the path to the JSON file storing checkpoints.
	checkpointsPath string

	// checkpoints maps file paths to their checkpoint data.
	// Protected by checkpointsLock for concurrent access.
	checkpoints     map[string]checkpoint
	checkpointsLock sync.Mutex

	// wg tracks the periodic sync goroutine.
	wg sync.WaitGroup

	// stopCh signals the periodic sync goroutine to stop.
	stopCh chan struct{}
}

// startCheckpointsDB creates and starts a new checkpoints database.
//
// This function:
//  1. Loads existing checkpoints from disk (if any)
//  2. Starts a background goroutine for periodic persistence
//
// The caller must call stop() when the checkpointsDB is no longer needed
// to ensure the final state is persisted.
func startCheckpointsDB(path string) (*checkpointsDB, error) {
	// Load existing checkpoints from disk.
	checkpoints, err := readCheckpoints(path)
	if err != nil {
		return nil, err
	}

	// Convert list to map for efficient lookups.
	checkpointsMap := make(map[string]checkpoint)
	for _, cp := range checkpoints {
		checkpointsMap[cp.Path] = cp
	}

	db := &checkpointsDB{
		checkpointsPath: path,
		checkpoints:     checkpointsMap,
		stopCh:          make(chan struct{}),
	}

	// Start periodic checkpoint persistence.
	db.startPeriodicSyncCheckpoints()

	return db, nil
}

// checkpoint represents a persistent snapshot of a log file reading state.
//
// The checkpoint contains enough information to:
//  1. Find the file (Path)
//  2. Detect if the file was rotated (Inode)
//  3. Verify the file content is the same (Fingerprint)
//  4. Resume reading from the correct position (Offset)
//
// The fingerprint is crucial for handling inode reuse. If vlagent is down
// and a file is deleted and a new file created with the same inode, the
// fingerprint will differ and we'll know not to resume from the old offset.
type checkpoint struct {
	// Path is the symlink path in /var/log/containers/.
	// Example: /var/log/containers/nginx_default_nginx-abc123.log
	Path string `json:"path"`

	// Inode is the file's inode number.
	// Used to detect file rotation (inode changes when kubelet creates a new file).
	Inode uint64 `json:"inode"`

	// Fingerprint is a hash of the first line of the file.
	// Used to detect inode reuse (different content with same inode).
	// Zero means the fingerprint hasn't been calculated yet.
	Fingerprint uint64 `json:"fingerprint"`

	// Offset is the byte offset of the last committed read position.
	// Resume reading from this position on restart.
	Offset int64 `json:"offset"`
}

// set updates or creates a checkpoint for a file.
// This is called after successfully processing log lines.
func (db *checkpointsDB) set(cp checkpoint) {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	db.checkpoints[cp.Path] = cp
}

// get retrieves a checkpoint for a file.
// Returns the checkpoint and true if found, empty checkpoint and false otherwise.
func (db *checkpointsDB) get(path string) (checkpoint, bool) {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	cp, ok := db.checkpoints[path]
	return cp, ok
}

// getAll returns all checkpoints as a slice.
// Used for persistence and cleanup operations.
func (db *checkpointsDB) getAll() []checkpoint {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	cps := make([]checkpoint, 0, len(db.checkpoints))
	for _, cp := range db.checkpoints {
		cps = append(cps, cp)
	}

	return cps
}

// delete removes a checkpoint for a file.
// Called when a file is deleted or no longer needs tracking.
func (db *checkpointsDB) delete(path string) {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	delete(db.checkpoints, path)
}

// mustSync persists all checkpoints to disk.
//
// The checkpoints are written atomically to prevent corruption.
// The file is sorted by path for consistent output and easier debugging.
func (db *checkpointsDB) mustSync() {
	cps := db.getAll()

	// Sort by path for consistent output.
	slices.SortFunc(cps, func(a, b checkpoint) int {
		return strings.Compare(a.Path, b.Path)
	})

	// Marshal to JSON with indentation for readability.
	data, err := json.MarshalIndent(cps, "", "\t")
	if err != nil {
		logger.Panicf("BUG: cannot marshal checkpoints: %s", err)
	}

	// Atomic write prevents corruption from partial writes.
	fs.MustWriteAtomic(db.checkpointsPath, data, true)
}

// readCheckpoints loads checkpoints from disk.
//
// Returns an empty slice if the file doesn't exist (first run).
// Returns an error if the file exists but cannot be read or parsed.
func readCheckpoints(path string) ([]checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// First run - no checkpoints file exists.
			logger.Infof("no checkpoints file found at %q; vlagent will read log files from the beginning", path)
			return nil, nil
		}
		return nil, fmt.Errorf("cannot read file checkpoints: %w", err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	var checkpoints []checkpoint
	if err := json.Unmarshal(data, &checkpoints); err != nil {
		return nil, fmt.Errorf("cannot unmarshal file checkpoints from %q: %w", path, err)
	}

	return checkpoints, nil
}

// startPeriodicSyncCheckpoints starts a background goroutine that persists
// checkpoints to disk every minute.
//
// This complements the explicit sync on graceful stop. If vlagent crashes
// or is killed, the periodic sync ensures we don't lose too much checkpoint
// progress (at most 1 minute of reading).
func (db *checkpointsDB) startPeriodicSyncCheckpoints() {
	db.wg.Go(func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				db.mustSync()
			case <-db.stopCh:
				// Final sync on shutdown.
				db.mustSync()
				return
			}
		}
	})
}

// stop gracefully shuts down the checkpoints database.
// It signals the periodic sync goroutine to stop and waits for it to finish.
// The final sync is performed before returning.
func (db *checkpointsDB) stop() {
	close(db.stopCh)
	db.wg.Wait()
}
