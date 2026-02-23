// Package internalselect implements the /internal/select/* HTTP endpoints for cluster mode.
//
// These endpoints are the receiving side on each vlstorage node. They accept queries
// from vlselect (frontend) nodes, execute them against local storage, and stream
// results back in a binary format.
//
// Unlike public /select/* endpoints:
//   - Request parameters are form-encoded (not URL query strings)
//   - Responses are binary (application/octet-stream), not JSON
//   - Protocol versions are verified for compatibility
//   - Endpoints are concurrency-limited to prevent resource exhaustion
//
// Registered Endpoints:
//
//	/internal/select/query               - Execute LogsQL query, stream DataBlocks
//	/internal/select/field_names         - Get field names seen in query results
//	/internal/select/field_values        - Get unique values for a field
//	/internal/select/stream_field_names  - Get stream field names
//	/internal/select/stream_field_values - Get unique values for a stream field
//	/internal/select/streams             - Get streams seen in results
//	/internal/select/stream_ids          - Get internal stream IDs
//	/internal/select/tenant_ids          - Get tenant IDs (returns JSON)
//
// Delete endpoints (enabled with -internaldelete.enable):
//
//	/internal/delete/run_task     - Start a deletion task
//	/internal/delete/stop_task    - Stop a running deletion task
//	/internal/delete/active_tasks - List active deletion tasks
//
// Response Format for /internal/select/query:
//
// The response is a stream of compressed blocks:
//
//	[8-byte length][zstd-compressed data]
//	[8-byte length][zstd-compressed data]
//	...
//
// Each compressed block contains:
//
//	[0x00 marker][DataBlock binary]   - Regular result block
//	[0x01 marker][QueryStats binary]  - Final block with statistics
//
// Concurrency Control:
//
// Requests are limited by -internalselect.maxConcurrentRequests (default: 100).
// Excess requests are queued until a slot becomes available or the client cancels.
//
// See onboarding/onboarding-cluster.md for cluster architecture details.
package internalselect

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// maxConcurrentRequests limits concurrent requests to /internal/select/* endpoints.
// This prevents a single vlstorage node from being overwhelmed by parallel queries
// from multiple vlselect frontends. Excess requests wait in a queue.
var maxConcurrentRequests = flag.Int("internalselect.maxConcurrentRequests", 100, "The limit on the number of concurrent requests to /internal/select/* endpoints; "+
	"other requests are put into the wait queue; see https://docs.victoriametrics.com/victorialogs/cluster/")

// RequestHandler dispatches requests to /internal/select/* and /internal/delete/* endpoints.
// It implements concurrency limiting via a channel semaphore. Requests that exceed
// the limit wait until a slot becomes available or the client cancels the request.
func RequestHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	select {
	case concurrencyLimitCh <- struct{}{}:
		if d := time.Since(startTime); d > 100*time.Millisecond {
			// Measure the wait duration for requests, which hit the concurrency limit and waited for more than 100 milliseconds to be executed.
			concurrentRequestsWaitDuration.Update(d.Seconds())
		}
		requestHandler(ctx, w, r, startTime)
		<-concurrencyLimitCh
	case <-ctx.Done():
		// Unconditionally measure the wait time until the the request is canceled by the client.
		concurrentRequestsWaitDuration.UpdateDuration(startTime)
	}
}

// Init initializes internalselect package.
func Init() {
	concurrencyLimitCh = make(chan struct{}, *maxConcurrentRequests)
}

// Stop stops vlselect
func Stop() {
	concurrencyLimitCh = nil
}

var concurrencyLimitCh chan struct{}

var concurrentRequestsWaitDuration = metrics.NewSummary(`vl_concurrent_internalselect_requests_wait_duration`)

// requestHandler dispatches the request to the appropriate handler based on URL path.
// It also records metrics for request count, errors, and duration.
func requestHandler(ctx context.Context, w http.ResponseWriter, r *http.Request, startTime time.Time) {
	path := r.URL.Path
	rh := requestHandlers[path]
	if rh == nil {
		httpserver.Errorf(w, r, "unsupported endpoint requested: %s", path)
		return
	}

	metrics.GetOrCreateCounter(fmt.Sprintf(`vl_http_requests_total{path=%q}`, path)).Inc()
	if err := rh(ctx, w, r); err != nil && !netutil.IsTrivialNetworkError(err) {
		metrics.GetOrCreateCounter(fmt.Sprintf(`vl_http_errors_total{path=%q}`, path)).Inc()
		httpserver.Errorf(w, r, "%s", err)
		// The return is skipped intentionally in order to track the duration of failed queries.
	}
	metrics.GetOrCreateSummary(fmt.Sprintf(`vl_http_request_duration_seconds{path=%q}`, path)).UpdateDuration(startTime)
}

// requestHandlers maps URL paths to their handler functions.
// Select endpoints return binary data; delete endpoints return JSON or empty responses.
var requestHandlers = map[string]func(ctx context.Context, w http.ResponseWriter, r *http.Request) error{
	"/internal/select/query":               processQueryRequest,
	"/internal/select/field_names":         processFieldNamesRequest,
	"/internal/select/field_values":        processFieldValuesRequest,
	"/internal/select/stream_field_names":  processStreamFieldNamesRequest,
	"/internal/select/stream_field_values": processStreamFieldValuesRequest,
	"/internal/select/streams":             processStreamsRequest,
	"/internal/select/stream_ids":          processStreamIDsRequest,
	"/internal/select/tenant_ids":          processTenantIDsRequest,

	"/internal/delete/run_task":     processDeleteRunTask,
	"/internal/delete/stop_task":    processDeleteStopTask,
	"/internal/delete/active_tasks": processDeleteActiveTasks,
}

// processQueryRequest executes a LogsQL query and streams binary DataBlocks to the client.
//
// Response streaming strategy:
//  1. Run the query against local storage via vlstorage.RunQuery()
//  2. For each DataBlock produced, marshal it with a 0x00 marker and buffer
//  3. When buffer exceeds 1MB, compress with zstd and send to client
//  4. After all results, send a final block with 0x01 marker containing QueryStats
//
// The streaming approach allows large result sets to be transferred without
// loading everything into memory. Multiple workers can produce blocks in parallel;
// their buffers are merged before sending.
func processQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.QueryProtocolVersion)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	var wLock sync.Mutex
	var dataLenBuf []byte

	sendBuf := func(bb *bytesutil.ByteBuffer) error {
		if len(bb.B) == 0 {
			return nil
		}

		data := bb.B
		if !cp.DisableCompression {
			bufLen := len(bb.B)
			bb.B = zstd.CompressLevel(bb.B, bb.B, 1)
			data = bb.B[bufLen:]
		}

		wLock.Lock()
		dataLenBuf = encoding.MarshalUint64(dataLenBuf[:0], uint64(len(data)))
		_, err := w.Write(dataLenBuf)
		if err == nil {
			_, err = w.Write(data)
		}
		wLock.Unlock()

		// Reset the sent buf
		bb.Reset()

		return err
	}

	var bufs atomicutil.Slice[bytesutil.ByteBuffer]

	var errGlobal atomic.Pointer[error]

	writeBlock := func(workerID uint, db *logstorage.DataBlock) {
		if errGlobal.Load() != nil {
			return
		}

		bb := bufs.Get(workerID)

		// Write the marker of a regular data block.
		bb.B = append(bb.B, 0)

		// Marshal the data block.
		bb.B = db.Marshal(bb.B)

		if len(bb.B) < 1024*1024 {
			// Fast path - the bb is too small to be sent to the client yet.
			return
		}

		// Slow path - the bb must be sent to the client.
		if err := sendBuf(bb); err != nil {
			errGlobal.CompareAndSwap(nil, &err)
		}
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		return err
	}
	if errP := errGlobal.Load(); errP != nil {
		return *errP
	}

	// Send the remaining data
	for _, bb := range bufs.All() {
		if err := sendBuf(bb); err != nil {
			return err
		}
	}

	// Send the query stats block.
	bb := bufs.Get(0)
	// Write the marker of query stats block.
	bb.B = append(bb.B, 1)
	// Marshal the block itself
	bb.B = marshalQueryStatsBlock(bb.B, qctx)
	return sendBuf(bb)
}

func processFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.FieldNamesProtocolVersion)
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldNames, err := vlstorage.GetFieldNames(qctx)
	if err != nil {
		return fmt.Errorf("cannot obtain field names: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldNames, cp.DisableCompression)
}

func processFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.FieldValuesProtocolVersion)
	if err != nil {
		return err
	}

	fieldName := r.FormValue("field")

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldValues, err := vlstorage.GetFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain field values: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldValues, cp.DisableCompression)
}

func processStreamFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamFieldNamesProtocolVersion)
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldNames, err := vlstorage.GetStreamFieldNames(qctx)
	if err != nil {
		return fmt.Errorf("cannot obtain stream field names: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldNames, cp.DisableCompression)
}

func processStreamFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamFieldValuesProtocolVersion)
	if err != nil {
		return err
	}

	fieldName := r.FormValue("field")

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	fieldValues, err := vlstorage.GetStreamFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain stream field values: %w", err)
	}

	return writeValuesWithHits(w, qctx, fieldValues, cp.DisableCompression)
}

func processStreamsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamsProtocolVersion)
	if err != nil {
		return err
	}

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	streams, err := vlstorage.GetStreams(qctx, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain streams: %w", err)
	}

	return writeValuesWithHits(w, qctx, streams, cp.DisableCompression)
}

func processStreamIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cp, err := getCommonParams(r, netselect.StreamIDsProtocolVersion)
	if err != nil {
		return err
	}

	limit, err := getInt64FromRequest(r, "limit")
	if err != nil {
		return err
	}

	qctx := cp.NewQueryContext(ctx)
	defer cp.UpdatePerQueryStatsMetrics()

	streamIDs, err := vlstorage.GetStreamIDs(qctx, uint64(limit))
	if err != nil {
		return fmt.Errorf("cannot obtain streams: %w", err)
	}

	return writeValuesWithHits(w, qctx, streamIDs, cp.DisableCompression)
}

func processDeleteRunTask(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteRunTaskProtocolVersion); err != nil {
		return err
	}

	// Parse query args
	taskID := r.FormValue("task_id")
	if taskID == "" {
		return fmt.Errorf("missing task_id arg")
	}

	timestamp, err := getInt64FromRequest(r, "timestamp")
	if err != nil {
		return err
	}

	tenantIDsStr := r.FormValue("tenant_ids")
	tenantIDs, err := logstorage.UnmarshalTenantIDsFromJSON([]byte(tenantIDsStr))
	if err != nil {
		return fmt.Errorf("cannot unmarshal tenant_ids=%q: %w", tenantIDsStr, err)
	}

	fStr := r.FormValue("filter")
	f, err := logstorage.ParseFilter(fStr)
	if err != nil {
		return fmt.Errorf("cannot unmarshal filter=%q: %w", fStr, err)
	}

	// Execute the delete task
	return vlstorage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f)
}

func processDeleteStopTask(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteStopTaskProtocolVersion); err != nil {
		return err
	}

	taskID := r.FormValue("task_id")
	if taskID == "" {
		return fmt.Errorf("missing task_id arg")
	}

	return vlstorage.DeleteStopTask(ctx, taskID)
}

func processDeleteActiveTasks(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	if err := checkProtocolVersion(r, netselect.DeleteActiveTasksProtocolVersion); err != nil {
		return err
	}

	tasks, err := vlstorage.DeleteActiveTasks(ctx)
	if err != nil {
		return err
	}

	data := logstorage.MarshalDeleteTasksToJSON(tasks)

	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("cannot send response to the client: %w", err)
	}

	return nil
}

func processTenantIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	start, err := getInt64FromRequest(r, "start")
	if err != nil {
		return err
	}
	end, err := getInt64FromRequest(r, "end")
	if err != nil {
		return err
	}

	tenantIDs, err := vlstorage.GetTenantIDs(ctx, start, end)
	if err != nil {
		return fmt.Errorf("cannot obtain tenant IDs: %w", err)
	}

	// Marshal tenantIDs at first
	data, err := json.Marshal(tenantIDs)
	if err != nil {
		return fmt.Errorf("cannot marshal tenantIDs: %w", err)
	}

	// Send the marshaled tenantIDs to the client
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(data); err != nil {
		return fmt.Errorf("cannot send response to the client: %w", err)
	}
	return nil
}

// commonParams holds the parsed parameters from an internal select request.
// These parameters are common across all /internal/select/* endpoints.
type commonParams struct {
	// TenantIDs is the list of tenant IDs to query (multi-tenant isolation)
	TenantIDs []logstorage.TenantID

	// Query is the parsed LogsQL query
	Query *logstorage.Query

	// DisableCompression indicates whether the client expects uncompressed responses
	DisableCompression bool

	// AllowPartialResponse indicates whether partial results are acceptable
	AllowPartialResponse bool

	// HiddenFieldsFilters is a list of field patterns to hide from results
	HiddenFieldsFilters []string

	// qs accumulates query execution statistics
	qs logstorage.QueryStats
}

func (cp *commonParams) NewQueryContext(ctx context.Context) *logstorage.QueryContext {
	return logstorage.NewQueryContext(ctx, &cp.qs, cp.TenantIDs, cp.Query, cp.AllowPartialResponse, cp.HiddenFieldsFilters)
}

func (cp *commonParams) UpdatePerQueryStatsMetrics() {
	vlstorage.UpdatePerQueryStatsMetrics(&cp.qs)
}

// getCommonParams parses the common request parameters from an internal select request.
// It verifies protocol version compatibility and extracts:
//   - tenant_ids: JSON array of TenantID
//   - query: LogsQL query string (parsed at the given timestamp)
//   - timestamp: Reference timestamp for relative time expressions
//   - disable_compression: Whether to skip response compression
//   - allow_partial_response: Whether partial results are acceptable
//   - hidden_fields_filters: JSON array of field patterns to hide
func getCommonParams(r *http.Request, expectedProtocolVersion string) (*commonParams, error) {
	if err := checkProtocolVersion(r, expectedProtocolVersion); err != nil {
		return nil, err
	}

	tenantIDsStr := r.FormValue("tenant_ids")
	tenantIDs, err := logstorage.UnmarshalTenantIDsFromJSON([]byte(tenantIDsStr))
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tenant_ids=%q: %w", tenantIDsStr, err)
	}

	timestamp, err := getInt64FromRequest(r, "timestamp")
	if err != nil {
		return nil, err
	}

	qStr := r.FormValue("query")
	q, err := logstorage.ParseQueryAtTimestamp(qStr, timestamp)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal query=%q: %w", qStr, err)
	}

	disableCompression, err := getBoolFromRequest(r, "disable_compression")
	if err != nil {
		return nil, err
	}

	allowPartialResponse, err := getBoolFromRequest(r, "allow_partial_response")
	if err != nil {
		return nil, err
	}

	hiddenFieldsFilters, err := getStringSliceFromRequest(r, "hidden_fields_filters")
	if err != nil {
		return nil, err
	}

	cp := &commonParams{
		TenantIDs: tenantIDs,
		Query:     q,

		DisableCompression: disableCompression,

		AllowPartialResponse: allowPartialResponse,
		HiddenFieldsFilters:  hiddenFieldsFilters,
	}
	return cp, nil
}

// checkProtocolVersion verifies that the client's protocol version matches the expected version.
// A mismatch typically indicates that vlselect and vlstorage components are at different
// release versions, which could cause protocol incompatibility. The error message guides
// operators to ensure all components are at the same version.
func checkProtocolVersion(r *http.Request, expectedProtocolVersion string) error {
	version := r.FormValue("version")
	if version != expectedProtocolVersion {
		return fmt.Errorf("unexpected protocol version=%q; want %q; the most likely cause of this error is different versions of VictoriaLogs cluster components; "+
			"make sure VictoriaLogs compoments have the same release version", version, expectedProtocolVersion)
	}
	return nil
}

// writeValuesWithHits serializes a slice of ValueWithHits to binary format and writes to w.
// The format is:
//   - [8-byte count of entries]
//   - [ValueWithHits #1]
//   - [ValueWithHits #2]
//   - ...
//   - [QueryStats DataBlock]
//
// The entire payload is optionally compressed with zstd (level 1) before sending.
func writeValuesWithHits(w http.ResponseWriter, qctx *logstorage.QueryContext, vhs []logstorage.ValueWithHits, disableCompression bool) error {
	var b []byte

	// Marshal vhs at first
	b = encoding.MarshalUint64(b, uint64(len(vhs)))
	for i := range vhs {
		b = vhs[i].Marshal(b)
	}

	// Marshal query stats block after that
	b = marshalQueryStatsBlock(b, qctx)

	if !disableCompression {
		b = zstd.CompressLevel(nil, b, 1)
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	if _, err := w.Write(b); err != nil {
		return fmt.Errorf("cannot send response to the client: %w", err)
	}

	return nil
}

// marshalQueryStatsBlock creates a DataBlock containing query execution statistics.
// The block includes metrics like rows scanned, bytes read, and execution time.
// This block is always sent as the final block in a query response (with 0x01 marker).
func marshalQueryStatsBlock(dst []byte, qctx *logstorage.QueryContext) []byte {
	queryDurationNsecs := qctx.QueryDurationNsecs()
	db := qctx.QueryStats.CreateDataBlock(queryDurationNsecs)
	dst = db.Marshal(dst)
	return dst
}

func getInt64FromRequest(r *http.Request, argName string) (int64, error) {
	s := r.FormValue(argName)
	if s == "" {
		return 0, fmt.Errorf("missing the required arg %s", argName)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %s=%q: %w", argName, s, err)
	}
	return n, nil
}

func getBoolFromRequest(r *http.Request, argName string) (bool, error) {
	s := r.FormValue(argName)
	if s == "" {
		return false, fmt.Errorf("missing the required arg %s", argName)
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, fmt.Errorf("cannot parse %s=%q as bool: %w", argName, s, err)
	}

	return b, nil
}

func getStringSliceFromRequest(r *http.Request, argName string) ([]string, error) {
	s := r.FormValue(argName)
	if s == "" {
		return nil, fmt.Errorf("missing the required arg %s", argName)
	}

	var a []string
	if err := json.Unmarshal([]byte(s), &a); err != nil {
		return nil, fmt.Errorf("cannot unmarshal JSON array from %s=%q: %w", argName, s, err)
	}

	return a, nil
}
