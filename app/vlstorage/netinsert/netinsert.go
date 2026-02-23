// Package netinsert implements the network insertion layer for cluster mode.
//
// This package is responsible for:
//  1. Buffering log rows from vlinsert into efficient batches
//  2. Sharding rows across vlstorage nodes based on stream hash
//  3. Compressing and sending data via HTTP to /internal/insert endpoints
//  4. Implementing high availability through automatic re-routing on node failure
//
// Data Flow:
//
//	Log row from protocol handler
//	    ↓
//	AddRow(streamHash, row) - route to node based on hash
//	    ↓
//	storageNode.addRow(row) - serialize and buffer
//	    ↓
//	Buffer reaches 2MB OR 1 second timeout
//	    ↓
//	mustSendInsertRequest() - compress with zstd, POST to /internal/insert
//	    ↓
//	On failure: re-route to another available node (HA)
//
// Sharding Strategy (streamRowsTracker):
//
// The sharding uses a two-phase approach to balance locality and parallelism:
//   - First 1000 rows per stream: Deterministic routing (streamHash % nodeCount)
//     Small streams stay on one node for better locality
//   - After 1000 rows: Random distribution across all nodes
//     Large streams spread for parallel query processing
//
// High Availability:
//
// When a vlstorage node becomes unavailable:
//  1. The node is marked disabled for 10 seconds
//  2. Pending data is re-routed to any available node
//  3. If ALL nodes are unavailable, data is buffered and retried every second
//  4. On shutdown, if nodes remain unavailable, buffered data is dropped with a log message
//
// See onboarding/onboarding-cluster.md for cluster architecture details.
package netinsert

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/contextutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastrand"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// maxInsertBlockSize is the maximum size of a single data block sent to a storage node.
// Blocks are flushed when they reach this size OR after 1 second (whichever comes first).
// This balances network efficiency (larger blocks = fewer requests) with latency (smaller = faster visibility).
const maxInsertBlockSize = 2 * 1024 * 1024

// ProtocolVersion is the version of the binary protocol used for /internal/insert.
// It must be incremented every time the data encoding format changes.
// Both sender and receiver verify version compatibility to prevent silent data corruption.
const ProtocolVersion = "v1"

// Storage manages connections to remote vlstorage nodes for data insertion.
// It holds a collection of storageNode instances, each representing one vlstorage node.
type Storage struct {
	// sns is the list of storage nodes to send data to.
	sns []*storageNode

	// disableCompression controls whether data is compressed before sending.
	// Disabling compression reduces CPU usage at the cost of higher bandwidth.
	disableCompression bool

	// srt tracks per-stream row counts for sharding decisions.
	srt *streamRowsTracker

	// pendingDataBuffers is a pool of byte buffers used for serializing data.
	// The channel capacity is concurrency * len(addrs), acting as both a buffer pool
	// and a concurrency limiter for in-flight requests.
	pendingDataBuffers chan *bytesutil.ByteBuffer

	// stopCh signals all goroutines to stop during shutdown.
	stopCh chan struct{}

	// wg tracks background goroutines for graceful shutdown.
	wg sync.WaitGroup
}

// storageNode represents a single vlstorage node in the cluster.
// Each node has its own pending data buffer, HTTP client, and availability state.
type storageNode struct {
	// scheme is "http" or "https" based on -storageNode.tls flag
	scheme string

	// addr is the TCP address (host:port) of the vlstorage node
	addr string

	// s is the parent Storage that owns this node
	s *Storage

	// c is the HTTP client for sending data to this node.
	// It has its own connection pool and timeout settings.
	c *http.Client

	// ac is the authentication config for this node (basic auth, bearer token, TLS)
	ac *promauth.Config

	// pendingDataMu protects pendingData and pendingDataLastFlush
	pendingDataMu sync.Mutex

	// pendingData is the buffer of serialized rows waiting to be sent.
	// Rows are appended here until the buffer reaches maxInsertBlockSize.
	pendingData *bytesutil.ByteBuffer

	// pendingDataLastFlush tracks when data was last sent, for the 1-second timeout
	pendingDataLastFlush time.Time

	// sendErrors counts failed send attempts to this node (for monitoring)
	sendErrors *metrics.Counter

	// disabledUntil is the Unix timestamp until which this node is disabled.
	// Nodes are disabled for 10 seconds after a failed send attempt.
	disabledUntil atomic.Uint64

	// isReachable indicates whether the node is currently accepting data.
	// Exposed as the vl_insert_remote_is_reachable metric.
	isReachable atomic.Bool
}

func newStorageNode(s *Storage, addr string, ac *promauth.Config, isTLS bool) *storageNode {
	tr := httputil.NewTransport(false, "vlinsert_backend")
	tr.TLSHandshakeTimeout = 20 * time.Second
	tr.DisableCompression = true

	scheme := "http"
	if isTLS {
		scheme = "https"
	}

	sn := &storageNode{
		scheme: scheme,
		addr:   addr,
		s:      s,
		c: &http.Client{
			Transport: ac.NewRoundTripper(tr),
		},
		ac: ac,

		sendErrors: metrics.GetOrCreateCounter(fmt.Sprintf(`vl_insert_remote_send_errors_total{addr=%q}`, addr)),

		pendingData: &bytesutil.ByteBuffer{},
	}

	sn.isReachable.Store(true)

	s.wg.Go(sn.backgroundFlusher)

	_ = metrics.GetOrCreateGauge(fmt.Sprintf(`vl_insert_remote_is_reachable{addr=%q}`, addr), func() float64 {
		if sn.isReachable.Load() {
			return 1
		}
		return 0
	})

	return sn
}

// backgroundFlusher runs as a goroutine per storage node.
// It ensures pending data is sent at least once per second, even if the buffer
// hasn't reached maxInsertBlockSize. This bounds the latency for small streams.
func (sn *storageNode) backgroundFlusher() {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-sn.s.stopCh:
			sn.flushPendingData(true)
			return
		case <-t.C:
			sn.flushPendingData(false)
		}
	}
}

// flushPendingData sends buffered data to the storage node if conditions are met.
// If force is true, data is sent regardless of the 1-second minimum interval.
// This is used during shutdown to ensure all pending data is flushed.
func (sn *storageNode) flushPendingData(force bool) {
	sn.pendingDataMu.Lock()
	if !force && time.Since(sn.pendingDataLastFlush) < time.Second {
		// nothing to flush
		sn.pendingDataMu.Unlock()
		return
	}

	pendingData := sn.grabPendingDataForFlushLocked()
	sn.pendingDataMu.Unlock()

	sn.mustSendInsertRequest(pendingData)
}

// debugFlush is used for testing: it flushes pending data and triggers force_flush
// on the remote node to make data immediately visible for queries.
func (sn *storageNode) debugFlush() {
	// Send pending samples to sn.
	sn.flushPendingData(true)

	// Instruct sn to convert the recevied samples into searchable parts.
	if err := sn.doRequest("/internal/force_flush", nil); err != nil {
		logger.Errorf("cannot convert pending samples into searchable parts: %s", err)
	}
}

// addRow serializes a log row and appends it to the pending data buffer.
// If the buffer exceeds maxInsertBlockSize after adding the row, the buffer is
// immediately flushed (swapped for a fresh buffer and sent).
// Rows that individually exceed maxInsertBlockSize are dropped with a warning.
func (sn *storageNode) addRow(r *logstorage.InsertRow) {
	bb := bbPool.Get()
	b := bb.B

	b = r.Marshal(b)

	if len(b) > maxInsertBlockSize {
		logger.Warnf("skipping too long log entry, since its length exceeds %d bytes; the actual log entry length is %d bytes; log entry contents: %s", maxInsertBlockSize, len(b), b)
		bbPool.Put(bb)
		return
	}

	var pendingData *bytesutil.ByteBuffer
	sn.pendingDataMu.Lock()
	if sn.pendingData.Len()+len(b) > maxInsertBlockSize {
		pendingData = sn.grabPendingDataForFlushLocked()
	}
	sn.pendingData.MustWrite(b)
	sn.pendingDataMu.Unlock()

	bb.B = b
	bbPool.Put(bb)

	if pendingData != nil {
		sn.mustSendInsertRequest(pendingData)
	}
}

var bbPool bytesutil.ByteBufferPool

// grabPendingDataForFlushLocked atomically swaps the pending data buffer with a fresh one.
// The old buffer is returned for sending; a new buffer is taken from the pool.
// Must be called with pendingDataMu held.
func (sn *storageNode) grabPendingDataForFlushLocked() *bytesutil.ByteBuffer {
	sn.pendingDataLastFlush = time.Now()
	pendingData := sn.pendingData
	sn.pendingData = <-sn.s.pendingDataBuffers

	return pendingData
}

// mustSendInsertRequest sends pending data to the storage node with HA fallback.
//
// The send attempt follows this sequence:
//  1. Try to send to this node's primary destination
//  2. On failure, attempt to send to ANY available node (HA re-routing)
//  3. If all nodes are unavailable, retry every second until:
//     - A node becomes available and accepts the data, OR
//     - The storage is stopped (stopCh closed), in which case data is dropped
//
// The pendingData buffer is always returned to the pool after this function completes.
func (sn *storageNode) mustSendInsertRequest(pendingData *bytesutil.ByteBuffer) {
	defer func() {
		pendingData.Reset()
		sn.s.pendingDataBuffers <- pendingData
	}()

	err := sn.sendInsertRequest(pendingData)
	if err == nil {
		return
	}

	if !errors.Is(err, errTemporarilyDisabled) {
		logger.Warnf("%s; re-routing the data block to the remaining nodes", err)
	}
	for !sn.s.sendInsertRequestToAnyNode(pendingData) {
		logger.Errorf("cannot send pending data to storage nodes, since all of them are unavailable; re-trying to send the data in a second")

		t := timerpool.Get(time.Second)
		select {
		case <-sn.s.stopCh:
			timerpool.Put(t)
			logger.Errorf("dropping %d bytes of data, since there are no available storage nodes", pendingData.Len())
			return
		case <-t.C:
			timerpool.Put(t)
		}
	}
}

// sendInsertRequest sends pending data to this storage node.
// Returns nil on success, or an error if the node is disabled or unreachable.
// The data is compressed with zstd (level 1) unless compression is disabled.
func (sn *storageNode) sendInsertRequest(pendingData *bytesutil.ByteBuffer) error {
	dataLen := pendingData.Len()
	if dataLen == 0 {
		// Nothing to send.
		return nil
	}

	if sn.disabledUntil.Load() > fasttime.UnixTimestamp() {
		sn.sendErrors.Inc()
		return errTemporarilyDisabled
	}

	var body io.Reader
	if !sn.s.disableCompression {
		bb := zstdBufPool.Get()
		defer zstdBufPool.Put(bb)

		bb.B = zstd.CompressLevel(bb.B[:0], pendingData.B, 1)
		body = bb.NewReader()
	} else {
		body = pendingData.NewReader()
	}

	if err := sn.doRequest("/internal/insert", body); err != nil {
		return fmt.Errorf("cannot send data block with the length %d: %w", pendingData.Len(), err)
	}

	return nil
}

// doRequest sends an HTTP request to this storage node.
// For POST requests (with body), it sets the appropriate content type and encoding headers.
// On connection failure or non-2xx response, the node is marked as temporarily disabled.
func (sn *storageNode) doRequest(path string, body io.Reader) error {
	ctx, cancel := contextutil.NewStopChanContext(sn.s.stopCh)
	defer cancel()

	method := "GET"
	if body != nil {
		method = "POST"
	}

	reqURL := sn.getRequestURL(path)
	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return fmt.Errorf("cannot create http %s request for %s: %w", method, reqURL, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
		if !sn.s.disableCompression {
			req.Header.Set("Content-Encoding", "zstd")
		}
	}
	if err := sn.ac.SetHeaders(req, true); err != nil {
		sn.sendErrors.Inc()
		return fmt.Errorf("cannot set auth headers for %s: %w", reqURL, err)
	}

	resp, err := sn.c.Do(req)
	if err != nil {
		sn.setDisableTemporarily()
		return fmt.Errorf("cannot send http request to %s: %s", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		sn.isReachable.Store(true)
		return nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		respBody = []byte(fmt.Sprintf("%s", err))
	}

	sn.setDisableTemporarily()

	return fmt.Errorf("unexpected response status code for request to %s: %d; want 2xx; response body: %q", reqURL, resp.StatusCode, respBody)
}

// getRequestURL constructs the full URL for a request to this storage node.
// The URL includes the protocol version as a query parameter for compatibility checking.
func (sn *storageNode) getRequestURL(path string) string {
	return fmt.Sprintf("%s://%s%s?version=%s", sn.scheme, sn.addr, path, url.QueryEscape(ProtocolVersion))
}

// setDisableTemporarily marks this node as unavailable for 10 seconds.
// This prevents repeated connection attempts to a failed node, reducing log noise
// and allowing time for the node to recover. The node's isReachable flag is also
// cleared to reflect the unreachable state in metrics.
func (sn *storageNode) setDisableTemporarily() {
	// Disable sending data to this sn for 10 seconds.
	sn.disabledUntil.Store(fasttime.UnixTimestamp() + 10)

	sn.sendErrors.Inc()
	sn.isReachable.Store(false)
}

var zstdBufPool bytesutil.ByteBufferPool

// NewStorage creates a new Storage for distributing log rows across vlstorage nodes.
//
// Parameters:
//   - addrs: List of vlstorage node addresses (host:port format)
//   - authCfgs: Per-node authentication configuration (basic auth, bearer token, TLS)
//   - isTLSs: Per-node TLS enablement (use HTTPS vs HTTP)
//   - concurrency: Average number of concurrent connections per node. The total buffer
//     pool size is concurrency * len(addrs), which limits in-flight data.
//   - disableCompression: If true, send data uncompressed (trades bandwidth for CPU)
//
// The returned Storage starts background flusher goroutines for each node.
// Call MustStop when the Storage is no longer needed to release resources.
func NewStorage(addrs []string, authCfgs []*promauth.Config, isTLSs []bool, concurrency int, disableCompression bool) *Storage {
	pendingDataBuffers := make(chan *bytesutil.ByteBuffer, concurrency*len(addrs))
	for i := 0; i < cap(pendingDataBuffers); i++ {
		pendingDataBuffers <- &bytesutil.ByteBuffer{}
	}

	s := &Storage{
		disableCompression: disableCompression,
		pendingDataBuffers: pendingDataBuffers,
		stopCh:             make(chan struct{}),
	}

	sns := make([]*storageNode, len(addrs))
	for i, addr := range addrs {
		sns[i] = newStorageNode(s, addr, authCfgs[i], isTLSs[i])
	}
	s.sns = sns

	// active streams tracker
	s.srt = newStreamRowsTracker(len(sns))
	_ = metrics.GetOrCreateGauge(`vl_insert_active_streams`, func() float64 {
		return float64(s.getActiveStreams())
	})

	return s
}

// getActiveStreams returns the number of log streams being tracked since the Storage start.
func (s *Storage) getActiveStreams() int {
	s.srt.mu.Lock()
	n := len(s.srt.rowsPerStream)
	s.srt.mu.Unlock()

	return n
}

// MustStop stops the s.
func (s *Storage) MustStop() {
	close(s.stopCh)
	s.wg.Wait()
	s.sns = nil
}

// DebugFlush flushes pending samples to s, so they become visible for querying.
func (s *Storage) DebugFlush() {
	var wg sync.WaitGroup
	for _, sn := range s.sns {
		wg.Go(sn.debugFlush)
	}
	wg.Wait()
}

// AddRow adds a log row to the appropriate storage node based on stream hash.
// The sharding decision is made by the streamRowsTracker, which considers both
// the stream hash and the number of rows already sent for that stream.
func (s *Storage) AddRow(streamHash uint64, r *logstorage.InsertRow) {
	idx := s.srt.getNodeIdx(streamHash)
	sn := s.sns[idx]
	sn.addRow(r)
}

// sendInsertRequestToAnyNode attempts to send pending data to any available storage node.
// It starts from a random node to avoid thundering herd on the first node.
// Returns true if the data was successfully sent to any node, false if all nodes are unavailable.
// This is the HA fallback mechanism used when the primary destination fails.
func (s *Storage) sendInsertRequestToAnyNode(pendingData *bytesutil.ByteBuffer) bool {
	startIdx := int(fastrand.Uint32n(uint32(len(s.sns))))
	for i := range s.sns {
		idx := (startIdx + i) % len(s.sns)
		sn := s.sns[idx]
		err := sn.sendInsertRequest(pendingData)
		if err == nil {
			return true
		}
		if !errors.Is(err, errTemporarilyDisabled) {
			logger.Warnf("cannot send pending data to the storage node %q: %s; trying to send it to another storage node", sn.addr, err)
		}
	}
	return false
}

var errTemporarilyDisabled = fmt.Errorf("writing to the node is temporarily disabled")

// streamRowsTracker implements the two-phase sharding strategy for distributing log rows.
// It tracks the number of rows sent per stream to decide between deterministic and random routing.
type streamRowsTracker struct {
	mu sync.Mutex

	// nodesCount is the number of storage nodes (immutable after creation)
	nodesCount int64

	// rowsPerStream tracks how many rows have been sent for each stream (by hash)
	rowsPerStream map[uint64]uint64
}

func newStreamRowsTracker(nodesCount int) *streamRowsTracker {
	return &streamRowsTracker{
		nodesCount:    int64(nodesCount),
		rowsPerStream: make(map[uint64]uint64),
	}
}

// getNodeIdx determines which storage node should receive a row for the given stream.
//
// Two-phase sharding strategy:
//
//   - Phase 1 (rows 1-1000): Deterministic routing using streamHash % nodesCount.
//     Small streams (most common case) stay on one node for data locality.
//     Different streams have different hashes, so they spread across nodes overall.
//
//   - Phase 2 (rows >1000): Random distribution across all nodes.
//     Large streams are spread for parallel query processing.
//     Random is preferred over round-robin to avoid correlation between
//     ingestion order and node count.
func (srt *streamRowsTracker) getNodeIdx(streamHash uint64) uint64 {
	if srt.nodesCount == 1 {
		// Fast path for a single node.
		return 0
	}

	srt.mu.Lock()
	defer srt.mu.Unlock()

	streamRows := srt.rowsPerStream[streamHash] + 1
	srt.rowsPerStream[streamHash] = streamRows

	if streamRows <= 1000 {
		// Write the initial rows for the stream to a single storage node for better locality.
		// This should work great for log streams containing small number of logs, since will be distributed
		// evenly among available storage nodes because they have different streamHash.
		return streamHash % uint64(srt.nodesCount)
	}

	// The log stream contains more than 1000 rows. Distribute them among storage nodes at random
	// in order to improve query performance over this stream (the data for the log stream
	// can be processed in parallel on all the storage nodes).
	//
	// The random distribution is preferred over round-robin distribution in order to avoid possible
	// dependency between the order of the ingested logs and the number of storage nodes,
	// which may lead to non-uniform distribution of logs among storage nodes.
	return uint64(fastrand.Uint32n(uint32(srt.nodesCount)))
}
