// Package remotewrite implements the remote write subsystem for vlagent.
//
// This package provides a storage backend that forwards log data to one or more
// VictoriaLogs instances over HTTP. It implements the same insertutil.LogRowsStorage
// interface used by vlstorage in the VictoriaLogs server, but instead of writing
// to local disk, it serializes rows and sends them via HTTP POST requests.
//
// # Architecture
//
// The remote write subsystem consists of three layers:
//
//  1. Storage Interface (remotewrite.go) - Implements insertutil.LogRowsStorage,
//     receives log rows from vlinsert and distributes them to per-URL contexts.
//
//  2. Batching Layer (pendinglogrows.go) - Buffers log rows in memory, serializes
//     them to binary format, compresses with zstd, and writes to the persistent queue.
//
//  3. HTTP Client (client.go) - Reads blocks from the persistent queue and sends
//     them to the remote VictoriaLogs with retry logic.
//
// # Data Flow
//
//	log rows → Storage.MustAddRows()
//	    → pushToRemoteStorages() [parallel for multiple URLs]
//	    → pendingLogs.add() [serialize + buffer]
//	    → pendingLogs.mustFlushLocked() [zstd compress]
//	    → persistentqueue.FastQueue [in-memory + disk spill]
//	    → client.runWorker() [read block]
//	    → sendBlockHTTP() [POST with retry]
//
// # Persistent Queue
//
// The persistent queue provides durability across vlagent restarts and network outages:
//   - In-memory blocks for fast access
//   - Disk spill in ~500MB chunks when memory is full
//   - Configurable max disk usage per URL
//   - Graceful degradation (drops oldest data when disk is full)
//
// # Replication
//
// Multiple -remoteWrite.url destinations enable data replication. Each URL gets
// its own independent queue and worker pool. Data is pushed to all URLs in parallel.
package remotewrite

import (
	"flag"
	"fmt"
	"net/url"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/VictoriaMetrics/metrics"
	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Configuration flags for remote write.
var (
	// remoteWriteURLs specifies the VictoriaLogs endpoints to send data to.
	// Multiple URLs enable replication - data is sent to all URLs in parallel.
	// Example: http://victorialogs:9428/insert/native
	remoteWriteURLs = flagutil.NewArrayString("remoteWrite.url", "Remote storage URL to write data to. It must support VictoriaLogs native protocol. "+
		"Example url: http://<victorialogs-host>:9428/insert/native. "+
		"Pass multiple -remoteWrite.url options in order to replicate the collected data to multiple remote storage systems.")

	// maxPendingBytesPerURL limits disk usage per remote URL.
	// When the limit is reached, oldest data is dropped. 0 = unlimited.
	maxPendingBytesPerURL = flagutil.NewArrayBytes("remoteWrite.maxDiskUsagePerURL", 0, "The maximum file-based buffer size in bytes at -remoteWrite.tmpDataPath "+
		"for each -remoteWrite.url. When buffer size reaches the configured maximum, then old data is dropped when adding new data to the buffer. "+
		"Buffered data is stored in ~500MB chunks. It is recommended to set the value for this flag to a multiple of the block size 500MB. "+
		"Disk usage is unlimited if the value is set to 0")

	// remoteWriteTmpDataPath is the base directory for persistent queue data.
	remoteWriteTmpDataPath = flag.String("remoteWrite.tmpDataPath", "", "Path to directory for storing pending data, which isn't sent to the configured -remoteWrite.url . "+
		"if this flag isn't set, then pending data is stored in the vlagent-remotewrite-data subdirectory under the -tmpDataPath directory; "+
		"see also -remoteWrite.maxDiskUsagePerURL")

	// queues specifies the number of concurrent workers per URL.
	// More workers can improve throughput for high-volume data.
	queues = flag.Int("remoteWrite.queues", cgroup.AvailableCPUs()*2, "The number of concurrent queues to each -remoteWrite.url. Set more queues if default number of queues "+
		"isn't enough for sending high volume of collected data to remote storage. "+
		"Default value depends on the number of available CPU cores. It should work fine in most cases since it minimizes resource usage")

	// showRemoteWriteURL controls whether URLs appear in /metrics output.
	// Hidden by default because URLs may contain authentication credentials.
	showRemoteWriteURL = flag.Bool("remoteWrite.showURL", false, "Whether to show -remoteWrite.url in the exported metrics. "+
		"It is hidden by default, since it can contain sensitive info such as auth key")
)

// rwctxsGlobal contains the per-URL contexts, populated during Init().
var rwctxsGlobal []*remoteWriteCtx

// Storage implements insertutil.LogRowsStorage for remote write.
//
// This is the entry point for the remote write subsystem. It receives log rows
// from vlinsert (via insertutil.LogRowsStorage interface) and distributes them
// to all configured remote URLs.
//
// Unlike vlstorage.Storage which writes to local disk, this implementation
// serializes rows to binary format and sends them via HTTP to remote endpoints.
type Storage struct{}

// MustAddRows implements insertutil.LogRowsStorage.
// It distributes the log rows to all configured remote URLs.
func (*Storage) MustAddRows(lr *logstorage.LogRows) {
	pushToRemoteStorages(lr)
}

// CanWriteData implements insertutil.LogRowsStorage.
// Remote write is always ready to accept data (it buffers to disk if needed).
func (*Storage) CanWriteData() error {
	return nil
}

// maxQueues limits the maximum number of concurrent queues per URL.
// Too many queues can cause high memory usage due to per-queue buffers.
var maxQueues = cgroup.AvailableCPUs() * 16

// persistentQueueDirname is the subdirectory name for persistent queue data.
const persistentQueueDirname = "persistent-queue"

// InitSecretFlags hides sensitive URLs from /metrics output.
// Must be called after flag.Parse and before any logging.
func InitSecretFlags() {
	if !*showRemoteWriteURL {
		// remoteWrite.url can contain authentication codes, so hide it at /metrics output.
		flagutil.RegisterSecretFlag("remoteWrite.url")
	}
}

// Init initializes the remote write subsystem.
//
// This function:
//  1. Validates that at least one URL is configured
//  2. Creates per-URL contexts (each with its own queue and workers)
//  3. Cleans up any orphaned queue directories from previous runs
//
// Must be called after flag.Parse(). Stop must be called for graceful shutdown.
func Init(tmpDataPath string) {
	if len(*remoteWriteURLs) == 0 {
		logger.Fatalf("at least one `-remoteWrite.url` command-line flag must be set")
	}
	if *queues > maxQueues {
		*queues = maxQueues
	}
	if *queues <= 0 {
		*queues = 1
	}
	path := *remoteWriteTmpDataPath
	if len(path) == 0 {
		path = filepath.Join(tmpDataPath, "vlagent-remotewrite-data")
	}
	initRemoteWriteCtxs(path, *remoteWriteURLs)
	dropDanglingQueues(path)
}

// Stop gracefully shuts down the remote write subsystem.
//
// This function:
//  1. Stops all pendingLogs instances (flush in-memory data to queue)
//  2. Stops all HTTP clients (finish sending in-flight requests)
//  3. Closes all persistent queues
//
// No calls to TryPush should happen during or after this call.
func Stop() {
	for _, rwctx := range rwctxsGlobal {
		rwctx.mustStop()
	}
	rwctxsGlobal = nil
}

// dropDanglingQueues removes orphaned queue directories.
//
// This is needed when:
//   - The number of queues was changed (-remoteWrite.queues)
//   - URLs were changed or reordered
//
// Orphaned directories are identified by comparing against active queue paths.
// This prevents disk space waste from old unused queues.
func dropDanglingQueues(tmpDataPath string) {
	// Build a set of active queue directory names.
	existingQueues := make(map[string]struct{}, len(rwctxsGlobal))
	for _, rwctx := range rwctxsGlobal {
		existingQueues[rwctx.fq.Dirname()] = struct{}{}
	}

	// Check for directories that don't match any active queue.
	queuesDir := filepath.Join(tmpDataPath, persistentQueueDirname)
	files := fs.MustReadDir(queuesDir)
	removed := 0
	for _, f := range files {
		dirname := f.Name()
		if _, ok := existingQueues[dirname]; !ok {
			logger.Infof("removing dangling queue %q", dirname)
			fullPath := filepath.Join(queuesDir, dirname)
			fs.MustRemoveDir(fullPath)
			removed++
		}
	}
	if removed > 0 {
		logger.Infof("removed %d dangling queues from %q, active queues: %d", removed, tmpDataPath, len(rwctxsGlobal))
	}
}

// initRemoteWriteCtxs creates a remoteWriteCtx for each configured URL.
//
// Each context is independent with its own:
//   - Persistent queue (for durability)
//   - HTTP client with worker pool (for sending)
//   - Batching buffers (for efficiency)
//
// The memory limit for in-memory queue blocks is calculated adaptively based
// on available memory and number of URLs.
func initRemoteWriteCtxs(tmpDataPath string, urls []string) {
	if len(urls) == 0 {
		logger.Panicf("BUG: urls must be non-empty")
	}

	// Calculate in-memory block limit per URL.
	// This is adaptive based on available memory.
	maxInmemoryBlocks := memory.Allowed() / len(urls) / 10000
	if maxInmemoryBlocks / *queues > 100 {
		// There is no much sense in keeping higher number of blocks in memory,
		// since this means that the producer outperforms consumer and the queue
		// will continue growing. It is better storing the queue to file.
		maxInmemoryBlocks = 100 * *queues
	}
	if maxInmemoryBlocks < 2 {
		maxInmemoryBlocks = 2
	}

	rwctxs := make([]*remoteWriteCtx, len(urls))
	rwctxIdx := make([]int, len(urls))
	for i, remoteWriteURLRaw := range urls {
		remoteWriteURL, err := url.Parse(remoteWriteURLRaw)
		if err != nil {
			logger.Fatalf("invalid -remoteWrite.url=%q: %s", remoteWriteURL, err)
		}

		// Create a sanitized URL for metrics/logging.
		// Hide the actual URL unless explicitly requested.
		sanitizedURL := fmt.Sprintf("%d:secret-url", i+1)
		if *showRemoteWriteURL {
			sanitizedURL = fmt.Sprintf("%d:%s", i+1, remoteWriteURL)
		}

		rwctxs[i] = newRemoteWriteCtx(i, remoteWriteURL, maxInmemoryBlocks, sanitizedURL, tmpDataPath)
		rwctxIdx[i] = i
	}

	rwctxsGlobal = rwctxs
}

// pushToRemoteStorages distributes log rows to all configured remote URLs.
//
// For a single URL, this is a fast direct push.
// For multiple URLs, this pushes in parallel to enable replication without
// adding latency (a slow remote doesn't block other remotes).
func pushToRemoteStorages(lr *logstorage.LogRows) {
	rwctxs := rwctxsGlobal
	if len(rwctxs) == 1 {
		// Fast path: single URL - no need for goroutines.
		rwctxs[0].push(lr)
		return
	}

	// Push samples to remote storage systems in parallel.
	// This ensures a slow remote doesn't block other remotes.
	var wg sync.WaitGroup
	for _, rwctx := range rwctxs {
		wg.Go(func() {
			rwctx.push(lr)
		})
	}
	wg.Wait()
}

// remoteWriteCtx manages all state for a single remote URL.
//
// It contains:
//   - fq: Persistent queue for durability
//   - c: HTTP client with worker pool for sending
//   - pls: Sharded batching buffers for parallel log row accumulation
type remoteWriteCtx struct {
	idx int

	// fq is the persistent queue that stores blocks before they're sent.
	// Provides in-memory caching with disk spill when memory is full.
	fq *persistentqueue.FastQueue

	// c is the HTTP client that reads from the queue and sends blocks.
	c *client

	// pls is a slice of batching buffers (pendingLogs).
	// Sharding allows parallel accumulation from multiple producers.
	pls        []*pendingLogs
	pssNextIdx atomic.Uint64
}

// newRemoteWriteCtx creates a new context for a remote URL.
//
// This function:
//  1. Adds the protocol version to the URL query string
//  2. Creates a persistent queue with a hash-based directory name
//  3. Creates an HTTP client with authentication and retry logic
//  4. Creates sharded pendingLogs for batching
func newRemoteWriteCtx(argIdx int, remoteWriteURL *url.URL, maxInmemoryBlocks int, sanitizedURL, tmpDataPath string) *remoteWriteCtx {
	// Add protocol version to the URL. This is required by VictoriaLogs.
	q := remoteWriteURL.Query()
	q.Set("version", netinsert.ProtocolVersion)
	remoteWriteURL.RawQuery = q.Encode()

	// Create a unique queue path based on URL hash.
	// Strip query params to ensure the path is stable across restarts
	// even if non-essential params change.
	pqURL := *remoteWriteURL
	pqURL.RawQuery = ""
	pqURL.Fragment = ""
	h := xxhash.Sum64([]byte(pqURL.String()))
	queuePath := filepath.Join(tmpDataPath, persistentQueueDirname, fmt.Sprintf("%d_%016X", argIdx+1, h))

	// Validate and apply max disk usage limit.
	maxPendingBytes := maxPendingBytesPerURL.GetOptionalArg(argIdx)
	if maxPendingBytes != 0 && maxPendingBytes < persistentqueue.DefaultChunkFileSize {
		// Round up to minimum chunk size.
		// See: https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4195
		logger.Warnf("rounding the -remoteWrite.maxDiskUsagePerURL=%d to the minimum supported value: %d", maxPendingBytes, persistentqueue.DefaultChunkFileSize)
		maxPendingBytes = persistentqueue.DefaultChunkFileSize
	}

	// Open the persistent queue.
	fq := persistentqueue.MustOpenFastQueue(queuePath, sanitizedURL, maxInmemoryBlocks, maxPendingBytes, false)

	// Register metrics for monitoring.
	_ = metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_pending_data_bytes{path=%q, url=%q}`, queuePath, sanitizedURL), func() float64 {
		return float64(fq.GetPendingBytes())
	})
	_ = metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_pending_inmemory_blocks{path=%q, url=%q}`, queuePath, sanitizedURL), func() float64 {
		return float64(fq.GetInmemoryQueueLen())
	})
	_ = metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_queue_blocked{path=%q, url=%q}`, queuePath, sanitizedURL), func() float64 {
		if fq.IsWriteBlocked() {
			return 1
		}
		return 0
	})

	// Create the HTTP client based on URL scheme.
	var c *client
	switch remoteWriteURL.Scheme {
	case "http", "https":
		c = newHTTPClient(argIdx, remoteWriteURL.String(), sanitizedURL, fq, *queues)
	default:
		logger.Fatalf("unsupported scheme: %s for remoteWriteURL: %s, want `http`, `https`", remoteWriteURL.Scheme, sanitizedURL)
	}
	c.init(argIdx, *queues, sanitizedURL)

	// Create sharded pendingLogs for parallel batching.
	// Limit to available CPUs since each pendingLogs can saturate a CPU.
	plsLen := *queues
	if n := cgroup.AvailableCPUs(); plsLen > n {
		plsLen = n
	}
	pls := make([]*pendingLogs, plsLen)
	for i := range pls {
		pls[i] = newPendingLogs(fq)
	}

	rwctx := &remoteWriteCtx{
		idx: argIdx,
		fq:  fq,
		c:   c,
		pls: pls,
	}

	return rwctx
}

// push adds log rows to this context's batching layer.
//
// Uses round-robin sharding across pendingLogs to allow parallel accumulation.
func (rwctx *remoteWriteCtx) push(lr *logstorage.LogRows) {
	pls := rwctx.pls
	// Round-robin to distribute load across shards.
	idx := rwctx.pssNextIdx.Add(1) % uint64(len(pls))
	pls[idx].add(lr)
}

// mustStop gracefully shuts down this context.
//
// The shutdown order is important:
//  1. Stop all pendingLogs (flush in-memory buffers to queue)
//  2. Unblock all queue readers (allow workers to drain)
//  3. Stop the HTTP client (wait for workers to finish)
//  4. Close the persistent queue
func (rwctx *remoteWriteCtx) mustStop() {
	// Stop accepting new data and flush buffers.
	for _, ps := range rwctx.pls {
		ps.mustStop()
	}
	rwctx.idx = 0
	rwctx.pls = nil

	// Unblock workers so they can drain the queue and exit.
	rwctx.fq.UnblockAllReaders()

	// Wait for HTTP workers to finish sending.
	rwctx.c.MustStop()
	rwctx.c = nil

	// Close the queue (may write a final index file).
	rwctx.fq.MustClose()
	rwctx.fq = nil
}
