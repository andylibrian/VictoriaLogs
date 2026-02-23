package remotewrite

import (
	"flag"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Configuration flags for batching behavior.
var (
	// maxUnpackedBlockSize is the maximum size of uncompressed data to accumulate
	// before flushing to the persistent queue. Larger blocks improve compression
	// ratio and network efficiency but use more memory.
	maxUnpackedBlockSize = flagutil.NewBytes("remoteWrite.maxBlockSize", 8*1024*1024, "The maximum block size to send to remote storage. Bigger blocks may improve performance at the cost of the increased memory usage.")

	// flushInterval is the maximum time to wait before flushing a partial block.
	// This ensures low-latency delivery when log volume is low.
	// Only takes effect when less than 2MB/s is being pushed.
	flushInterval = flag.Duration("remoteWrite.flushInterval", time.Second, "Interval for flushing the data to remote storage. "+
		"This option takes effect only when less than 2MB of data per second are pushed to -remoteWrite.url")
)

// pendingLogs buffers and batches log rows before writing to the persistent queue.
//
// Each log row is immediately serialized to its binary InsertRow format and
// accumulated in memory. When a flush trigger fires (size limit or timer),
// the accumulated data is zstd-compressed and written to the persistent queue.
//
// This batching approach:
//   - Reduces the number of writes to the persistent queue
//   - Improves compression ratio (more data = better compression)
//   - Reduces the number of HTTP requests to remote storage
//
// Multiple pendingLogs instances are sharded to allow parallel accumulation
// from multiple producer goroutines.
type pendingLogs struct {
	// lastFlushTime tracks when the last flush occurred.
	// Used by the periodic flusher to avoid unnecessary flushes.
	lastFlushTime atomic.Uint64

	// fq is the destination persistent queue.
	fq *persistentqueue.FastQueue

	// mu protects wr during concurrent access.
	mu sync.Mutex

	// wr accumulates serialized and optionally compressed log rows.
	wr writeRequest

	// stopCh signals the periodic flusher to stop.
	stopCh chan struct{}

	// periodicFlusherWG tracks the background flusher goroutine.
	periodicFlusherWG sync.WaitGroup
}

// newPendingLogs creates a new batching buffer with a background periodic flusher.
func newPendingLogs(fq *persistentqueue.FastQueue) *pendingLogs {
	pl := &pendingLogs{
		fq:     fq,
		stopCh: make(chan struct{}),
	}

	// Start the background periodic flusher.
	// This ensures data is flushed even when log volume is low.
	pl.periodicFlusherWG.Go(pl.periodicFlusher)

	return pl
}

// add serializes log rows and adds them to the pending buffer.
//
// Each row is immediately serialized to its binary InsertRow format via r.Marshal().
// This is done before acquiring the lock to minimize lock contention.
//
// If the buffer exceeds maxUnpackedBlockSize after adding, an immediate flush
// is triggered to prevent unbounded memory growth.
func (pl *pendingLogs) add(lr *logstorage.LogRows) {
	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		pl.addLogRow(r)
	})
}

// addLogRow serializes a single log row and adds it to the buffer.
func (pl *pendingLogs) addLogRow(r *logstorage.InsertRow) {
	// Serialize the row to binary format outside the lock.
	bb := bbPool.Get()
	bb.B = r.Marshal(bb.B)

	pl.mu.Lock()
	// Append the serialized row to the pending buffer.
	_, _ = pl.wr.pendingData.Write(bb.B)
	pl.wr.pendingLogRowsCount++

	// Check if we've reached the size limit and need to flush.
	if len(pl.wr.pendingData.B) > maxUnpackedBlockSize.IntN() {
		pl.mustFlushLocked()
	}
	pl.mu.Unlock()
	bbPool.Put(bb)
}

// mustFlushLocked flushes the pending buffer to the persistent queue.
//
// This function:
//  1. Compresses the accumulated data with zstd (level 1 for speed)
//  2. Writes the compressed block to the persistent queue
//  3. Resets the buffer for reuse
//
// Must be called with mu held.
func (pl *pendingLogs) mustFlushLocked() {
	pl.lastFlushTime.Store(fasttime.UnixTimestamp())
	pl.wr.push(func(b []byte) {
		// Write the compressed block to the persistent queue.
		// TryWriteBlock returns false only if the queue is disabled,
		// which shouldn't happen in vlagent.
		if !pl.fq.TryWriteBlock(b) {
			logger.Fatalf("BUG: TryWriteBlock cannot return false")
		}
	})
	pl.wr.reset()
}

// periodicFlusher runs in a background goroutine to flush data periodically.
//
// This ensures that even with low log volume, data is sent within
// -remoteWrite.flushInterval of being received. This is important for:
//   - Low-latency alerting on recent logs
//   - Preventing data loss on unexpected shutdown
//
// The flusher only triggers if data hasn't been flushed recently,
// avoiding unnecessary work when data is flowing quickly.
func (pl *pendingLogs) periodicFlusher() {
	flushSeconds := int64(flushInterval.Seconds())
	if flushSeconds <= 0 {
		flushSeconds = 1
	}

	// Add jitter to prevent synchronized flushes across multiple vlagent instances.
	d := timeutil.AddJitterToDuration(*flushInterval)
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-pl.stopCh:
			// Shutdown requested - do a final flush.
			pl.mu.Lock()
			pl.mustFlushOnStop()
			pl.mu.Unlock()
			return
		case <-ticker.C:
			// Only flush if we haven't flushed recently.
			// This avoids unnecessary flushes when data is flowing quickly.
			if fasttime.UnixTimestamp()-pl.lastFlushTime.Load() < uint64(flushSeconds) {
				continue
			}
		}

		pl.mu.Lock()
		pl.mustFlushLocked()
		pl.mu.Unlock()
	}
}

// mustFlushOnStop performs a final flush during shutdown.
//
// Unlike mustFlushLocked, this uses MustWriteBlockIgnoreDisabledPQ which
// will always write the data even if the persistent queue is disabled.
// This ensures all in-memory data is saved during graceful shutdown.
func (pl *pendingLogs) mustFlushOnStop() {
	pl.wr.push(pl.fq.MustWriteBlockIgnoreDisabledPQ)
	pl.wr.reset()
}

// mustStop stops the periodic flusher and performs a final flush.
func (pl *pendingLogs) mustStop() {
	close(pl.stopCh)
	pl.periodicFlusherWG.Wait()
}

// writeRequest accumulates serialized log rows before compression and writing.
type writeRequest struct {
	// pendingData contains serialized InsertRow data.
	// Each row is in binary format ready for the native protocol.
	pendingData bytesutil.ByteBuffer

	// pendingLogRowsCount tracks the number of rows for metrics.
	pendingLogRowsCount int64
}

// push compresses the pending data and writes it to the persistent queue.
//
// Compression uses zstd level 1, which provides good compression with
// minimal CPU overhead. The compression ratio typically ranges from 3:1
// to 10:1 for log data.
func (wr *writeRequest) push(pushBlock func([]byte)) {
	if len(wr.pendingData.B) == 0 {
		return
	}
	b := wr.pendingData.B

	// Compress with zstd level 1 (fast compression).
	zb := compressBufPool.Get()
	zb.B = zstd.CompressLevel(zb.B[:0], b, 1)
	zbLen := len(zb.B)
	pushBlock(zb.B)
	compressBufPool.Put(zb)

	// Update metrics.
	blockSizeBytes.Update(float64(zbLen))
	blockSizeLogRows.Update(float64(wr.pendingLogRowsCount))
}

// reset clears the writeRequest for reuse.
func (wr *writeRequest) reset() {
	wr.pendingData.Reset()
	wr.pendingLogRowsCount = 0
}

// Metrics for monitoring block sizes.
var (
	blockSizeBytes   = metrics.NewHistogram(`vlagent_remotewrite_block_size_bytes`)
	blockSizeLogRows = metrics.NewHistogram(`vlagent_remotewrite_block_size_rows`)
)

// Buffer pools for reusable allocations.
var (
	// compressBufPool provides buffers for compressed output.
	compressBufPool bytesutil.ByteBufferPool

	// bbPool provides buffers for row serialization.
	bbPool bytesutil.ByteBufferPool
)
