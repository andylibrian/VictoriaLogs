// Package netselect implements the network query layer for cluster mode.
//
// This package is responsible for:
//  1. Fanning out queries to all vlstorage nodes in parallel
//  2. Streaming results back to the frontend for merging
//  3. Handling partial responses when some nodes are unavailable
//  4. Managing deletion tasks across the cluster
//
// Query Flow:
//
//	Query from vlselect frontend
//	    ↓
//	RunQuery(qctx, writeBlock)
//	    ↓
//	NewNetQueryRunner - split query into remote and local pipes
//	    ↓
//	runQuery() - fan out to ALL vlstorage nodes in parallel
//	    ↓
//	Each node: POST /internal/select/query with form-encoded params
//	    ↓
//	Stream binary DataBlocks back (with zstd compression)
//	    ↓
//	writeBlock(nodeIdx, db) - merge results from all nodes
//
// Partial Response Support:
//
// When -search.allowPartialResponse is enabled:
//   - Queries succeed as long as at least one vlstorage node responds
//   - Results from unavailable nodes are silently omitted
//   - Configuration errors (non-network issues) are always returned to clients
//
// Binary Protocol:
//
// Responses from /internal/select/query use a streaming binary format:
//
//	[8-byte length][zstd-compressed block]
//	[8-byte length][zstd-compressed block]
//	...
//	[8-byte length][final block with query stats marker 0x01]
//
// See onboarding/onboarding-cluster.md for cluster architecture details.
package netselect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/contextutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Protocol version constants for /internal/select/* endpoints.
// These must be incremented when the request/response format changes.
// Version mismatches between vlselect and vlstorage result in clear error messages.
const (
	// FieldNamesProtocolVersion is for /internal/select/field_names endpoint.
	FieldNamesProtocolVersion = "v4"

	// FieldValuesProtocolVersion is for /internal/select/field_values endpoint.
	FieldValuesProtocolVersion = "v4"

	// StreamFieldNamesProtocolVersion is for /internal/select/stream_field_names endpoint.
	StreamFieldNamesProtocolVersion = "v4"

	// StreamFieldValuesProtocolVersion is for /internal/select/stream_field_values endpoint.
	StreamFieldValuesProtocolVersion = "v4"

	// StreamsProtocolVersion is for /internal/select/streams endpoint.
	StreamsProtocolVersion = "v4"

	// StreamIDsProtocolVersion is for /internal/select/stream_ids endpoint.
	StreamIDsProtocolVersion = "v4"

	// QueryProtocolVersion is for /internal/select/query endpoint.
	// This is the main query endpoint that streams DataBlocks.
	QueryProtocolVersion = "v4"

	// DeleteRunTaskProtocolVersion is for /internal/delete/run_task endpoint.
	DeleteRunTaskProtocolVersion = "v1"

	// DeleteStopTaskProtocolVersion is for /internal/delete/stop_task endpoint.
	DeleteStopTaskProtocolVersion = "v1"

	// DeleteActiveTasksProtocolVersion is for /internal/delete/active_tasks endpoint.
	DeleteActiveTasksProtocolVersion = "v1"
)

// Storage manages connections to remote vlstorage nodes for distributed querying.
// It holds a collection of storageNode instances, each representing one vlstorage node.
type Storage struct {
	// sns is the list of storage nodes to query
	sns []*storageNode

	// disableCompression controls whether responses are expected to be compressed
	disableCompression bool
}

// storageNode represents a single vlstorage node in the cluster for querying.
type storageNode struct {
	// scheme is "http" or "https" based on -storageNode.tls flag
	scheme string

	// addr is the TCP address (host:port) of the vlstorage node
	addr string

	// s is the parent Storage that owns this node
	s *Storage

	// c is the HTTP client for querying this node
	c *http.Client

	// ac is the authentication config for this node (basic auth, bearer token, TLS)
	ac *promauth.Config

	// sendErrors counts failed query attempts to this node (for monitoring)
	sendErrors *metrics.Counter
}

func newStorageNode(s *Storage, addr string, ac *promauth.Config, isTLS bool) *storageNode {
	tr := httputil.NewTransport(false, "vlselect_backend")
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

		sendErrors: metrics.GetOrCreateCounter(fmt.Sprintf(`vl_select_remote_send_errors_total{addr=%q}`, addr)),
	}
	return sn
}

// runQuery executes a query on this storage node and streams results via processBlock.
// The query is sent as a POST to /internal/select/query with form-encoded parameters.
// Results are received as a stream of binary DataBlocks, decompressed, and passed to processBlock.
// Query stats are extracted from the final block and added to qctx.QueryStats.
func (sn *storageNode) runQuery(qctx *logstorage.QueryContext, processBlock func(db *logstorage.DataBlock)) error {
	args := sn.getCommonArgs(QueryProtocolVersion, qctx)

	qsLocal := &logstorage.QueryStats{}
	defer qctx.QueryStats.UpdateAtomic(qsLocal)

	path := "/internal/select/query"
	responseBody, reqURL, err := sn.getResponseBodyForPathAndArgs(qctx.Context, path, args)
	if err != nil {
		return err
	}
	defer responseBody.Close()

	// read the response
	var dataLenBuf [8]byte
	var buf []byte
	var db logstorage.DataBlock
	var valuesBuf []string
	for {
		if _, err := io.ReadFull(responseBody, dataLenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				// The end of response stream
				return nil
			}
			return fmt.Errorf("cannot read block size from %q: %w", reqURL, err)
		}
		blockLen := encoding.UnmarshalUint64(dataLenBuf[:])
		if blockLen > math.MaxInt {
			return fmt.Errorf("too big data block read from %q: %d bytes; mustn't exceed %v bytes", reqURL, blockLen, math.MaxInt)
		}

		buf = slicesutil.SetLength(buf, int(blockLen))
		if _, err := io.ReadFull(responseBody, buf); err != nil {
			return fmt.Errorf("cannot read block with size of %d bytes from %q: %w", blockLen, reqURL, err)
		}

		src := buf
		if !sn.s.disableCompression {
			bufLen := len(buf)
			var err error
			buf, err = encoding.DecompressZSTD(buf, buf)
			if err != nil {
				return fmt.Errorf("cannot decompress data block: %w", err)
			}
			src = buf[bufLen:]
		}

		for len(src) > 0 {
			isQueryStatsBlock := (src[0] == 1)
			src = src[1:]

			if isQueryStatsBlock {
				tail, err := unmarshalQueryStats(qsLocal, src)
				if err != nil {
					return fmt.Errorf("cannot unmarshal query stats received from %q: %w", reqURL, err)
				}
				src = tail
				continue
			}

			tail, vb, err := db.UnmarshalInplace(src, valuesBuf[:0])
			if err != nil {
				return fmt.Errorf("cannot unmarshal data block received from %q: %w", reqURL, err)
			}
			valuesBuf = vb
			src = tail

			processBlock(&db)

			clear(valuesBuf)
		}
	}
}

func (sn *storageNode) getFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	args := sn.getCommonArgs(FieldNamesProtocolVersion, qctx)

	return sn.getValuesWithHits(qctx, "/internal/select/field_names", args)
}

func (sn *storageNode) getFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	args := sn.getCommonArgs(FieldValuesProtocolVersion, qctx)
	args.Set("field", fieldName)
	args.Set("limit", fmt.Sprintf("%d", limit))

	return sn.getValuesWithHits(qctx, "/internal/select/field_values", args)
}

func (sn *storageNode) getStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	args := sn.getCommonArgs(StreamFieldNamesProtocolVersion, qctx)

	return sn.getValuesWithHits(qctx, "/internal/select/stream_field_names", args)
}

func (sn *storageNode) getStreamFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	args := sn.getCommonArgs(StreamFieldValuesProtocolVersion, qctx)
	args.Set("field", fieldName)
	args.Set("limit", fmt.Sprintf("%d", limit))

	return sn.getValuesWithHits(qctx, "/internal/select/stream_field_values", args)
}

func (sn *storageNode) getStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	args := sn.getCommonArgs(StreamsProtocolVersion, qctx)
	args.Set("limit", fmt.Sprintf("%d", limit))

	return sn.getValuesWithHits(qctx, "/internal/select/streams", args)
}

func (sn *storageNode) getStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	args := sn.getCommonArgs(StreamIDsProtocolVersion, qctx)
	args.Set("limit", fmt.Sprintf("%d", limit))

	return sn.getValuesWithHits(qctx, "/internal/select/stream_ids", args)
}

func (sn *storageNode) getTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error) {
	args := url.Values{}
	args.Set("start", fmt.Sprintf("%d", start))
	args.Set("end", fmt.Sprintf("%d", end))

	path := "/internal/select/tenant_ids"
	data, reqURL, err := sn.getPlainResponseBodyForPathAndArgs(ctx, path, args)
	if err != nil {
		return nil, err
	}
	var tenantIDs []logstorage.TenantID
	if err := json.Unmarshal(data, &tenantIDs); err != nil {
		return nil, fmt.Errorf("cannot unmarshal tenantIDs received from %q; data=%q: %w", reqURL, data, err)
	}
	return tenantIDs, nil
}

// getCommonArgs builds the URL form values for a query request to a storage node.
// These arguments are shared across all /internal/select/* endpoints:
//   - version: Protocol version for compatibility checking
//   - tenant_ids: JSON array of tenant IDs to query (multi-tenant isolation)
//   - query: LogsQL query string
//   - timestamp: Reference timestamp for relative time expressions
//   - disable_compression: Whether the sender expects uncompressed responses
//   - allow_partial_response: Whether partial results from unavailable nodes are acceptable
//   - hidden_fields_filters: JSON array of field patterns to hide from results
func (sn *storageNode) getCommonArgs(version string, qctx *logstorage.QueryContext) url.Values {
	// ATTENTION: the *ProtocolVersion consts must be incremented every time the set of common args changes or its format changes.

	args := url.Values{}
	args.Set("version", version)
	args.Set("tenant_ids", string(logstorage.MarshalTenantIDsToJSON(qctx.TenantIDs)))
	args.Set("query", qctx.Query.String())
	args.Set("timestamp", fmt.Sprintf("%d", qctx.Query.GetTimestamp()))
	args.Set("disable_compression", fmt.Sprintf("%v", sn.s.disableCompression))
	args.Set("allow_partial_response", fmt.Sprintf("%v", qctx.AllowPartialResponse))

	hiddenFieldsFilters, err := json.Marshal(qctx.HiddenFieldsFilters)
	if err != nil {
		logger.Panicf("BUG: cannot marshal HiddenFieldsFilters=%#v: %s", qctx.HiddenFieldsFilters, err)
	}
	args.Set("hidden_fields_filters", string(hiddenFieldsFilters))

	return args
}

func (sn *storageNode) getValuesWithHits(qctx *logstorage.QueryContext, path string, args url.Values) ([]logstorage.ValueWithHits, error) {
	data, err := sn.getResponseForPathAndArgs(qctx.Context, path, args)
	if err != nil {
		return nil, err
	}
	return unmarshalValuesWithHits(qctx, data)
}

func (sn *storageNode) getResponseForPathAndArgs(ctx context.Context, path string, args url.Values) ([]byte, error) {
	responseBody, reqURL, err := sn.getResponseBodyForPathAndArgs(ctx, path, args)
	if err != nil {
		return nil, err
	}
	defer responseBody.Close()

	// read the response
	var bb bytesutil.ByteBuffer
	if _, err := bb.ReadFrom(responseBody); err != nil {
		return nil, fmt.Errorf("cannot read response from %q: %w", reqURL, err)
	}

	if sn.s.disableCompression {
		return bb.B, nil
	}

	bbLen := len(bb.B)
	bb.B, err = encoding.DecompressZSTD(bb.B, bb.B)
	if err != nil {
		return nil, err
	}
	return bb.B[bbLen:], nil
}

// getResponseBodyForPathAndArgs sends a POST request to the storage node and returns the response body.
// On connection failure, it returns an httpserver.ErrorWithStatusCode with status 502 (Bad Gateway).
// This error type is used by isUnavailableBackendError() to differentiate network issues from config errors.
func (sn *storageNode) getResponseBodyForPathAndArgs(ctx context.Context, path string, args url.Values) (io.ReadCloser, string, error) {
	reqURL := sn.getRequestURL(path)
	reqBody := strings.NewReader(args.Encode())
	req, err := http.NewRequestWithContext(ctx, "POST", reqURL, reqBody)
	if err != nil {
		return nil, "", fmt.Errorf("cannot create a request for %q: %w", reqURL, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := sn.ac.SetHeaders(req, true); err != nil {
		return nil, "", fmt.Errorf("cannot set auth headers at %q: %w", reqURL, err)
	}

	// send the request to the storage node
	resp, err := sn.c.Do(req)
	if err != nil {
		// Wrap the error into httpserver.ErrorWithStatusCode in order to return the proper status code to the client.
		// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/576
		//
		// This is also used by isUnavailableBackendError() function in order to differentiate unavailable backend errors
		// from improper configuration errors.
		return nil, "", &httpserver.ErrorWithStatusCode{
			Err:        fmt.Errorf("cannot connect to storage node at %q: %w", reqURL, err),
			StatusCode: http.StatusBadGateway,
		}
	}

	if resp.StatusCode != http.StatusOK {
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			responseBody = []byte(err.Error())
		}
		_ = resp.Body.Close()
		return nil, "", fmt.Errorf("unexpected response status code from %q: %d; want %d; response: %q", reqURL, resp.StatusCode, http.StatusOK, responseBody)
	}

	return resp.Body, reqURL, nil
}

func (sn *storageNode) getRequestURL(path string) string {
	return fmt.Sprintf("%s://%s%s", sn.scheme, sn.addr, path)
}

// NewStorage creates a new Storage for querying vlstorage nodes.
//
// Parameters:
//   - addrs: List of vlstorage node addresses (host:port format)
//   - authCfgs: Per-node authentication configuration
//   - isTLSs: Per-node TLS enablement (use HTTPS vs HTTP)
//   - disableCompression: If true, expect uncompressed responses (trades bandwidth for CPU)
//
// Call MustStop when the Storage is no longer needed.
func NewStorage(addrs []string, authCfgs []*promauth.Config, isTLSs []bool, disableCompression bool) *Storage {
	s := &Storage{
		disableCompression: disableCompression,
	}

	sns := make([]*storageNode, len(addrs))
	for i, addr := range addrs {
		sns[i] = newStorageNode(s, addr, authCfgs[i], isTLSs[i])
	}
	s.sns = sns

	return s
}

// MustStop stops the s.
func (s *Storage) MustStop() {
	s.sns = nil
}

// RunQuery executes a distributed query across all vlstorage nodes.
//
// The query execution flow:
//  1. Create a NetQueryRunner that splits the query into remote and local pipes
//  2. Define a search function that fans out to all nodes via runQuery()
//  3. Execute with the specified concurrency, which controls parallel processing
//
// The writeBlock callback receives DataBlocks with a node index prefix for merging.
// Query stats are aggregated from all nodes and updated in qctx.QueryStats.
func (s *Storage) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	nqr, err := logstorage.NewNetQueryRunner(qctx, s.RunQuery, writeBlock)
	if err != nil {
		return err
	}

	search := func(stopCh <-chan struct{}, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
		qctxLocal := qctx.WithQuery(q)
		return s.runQuery(stopCh, qctxLocal, writeBlock)
	}

	concurrency := qctx.Query.GetConcurrency()
	return nqr.Run(qctx.Context, concurrency, search)
}

// runQuery fans out the query to ALL vlstorage nodes in parallel.
// Each node's results are streamed back and passed to writeBlock with the node index.
// Errors are collected from all nodes; the final error depends on allowPartialResponse:
//   - If false: return the first error from any node
//   - If true: return error only if ALL nodes fail or a config error occurs
func (s *Storage) runQuery(stopCh <-chan struct{}, qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	ctxWithCancel, cancel := contextutil.NewStopChanContext(stopCh)
	defer cancel()

	qctxLocal := qctx.WithContext(ctxWithCancel)

	errs := make([]error, len(s.sns))

	var wg sync.WaitGroup
	for nodeIdx := range s.sns {
		wg.Go(func() {
			sn := s.sns[nodeIdx]
			err := sn.runQuery(qctxLocal, func(db *logstorage.DataBlock) {
				writeBlock(uint(nodeIdx), db)
			})
			errs[nodeIdx] = sn.handleError(ctxWithCancel, cancel, err, qctx.AllowPartialResponse)
		})
	}
	wg.Wait()

	return getFirstError(errs, qctx.AllowPartialResponse)
}

// GetFieldNames executes qctx and returns field names seen in results.
func (s *Storage) GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	return s.getValuesWithHits(qctx, 0, false, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
		qctxLocal := qctx.WithContext(ctx)
		return sn.getFieldNames(qctxLocal)
	})
}

// GetFieldValues executes qctx and returns unique values for the fieldName seen in results.
//
// If limit > 0, then up to limit unique values are returned.
func (s *Storage) GetFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return s.getValuesWithHits(qctx, limit, true, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
		qctxLocal := qctx.WithContext(ctx)
		return sn.getFieldValues(qctxLocal, fieldName, limit)
	})
}

// GetStreamFieldNames executes qctx and returns stream field names seen in results.
func (s *Storage) GetStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	return s.getValuesWithHits(qctx, 0, false, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
		qctxLocal := qctx.WithContext(ctx)
		return sn.getStreamFieldNames(qctxLocal)
	})
}

// GetStreamFieldValues executes qctx and returns stream field values for the given fieldName seen in results.
//
// If limit > 0, then up to limit unique stream field values are returned.
func (s *Storage) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return s.getValuesWithHits(qctx, limit, true, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
		qctxLocal := qctx.WithContext(ctx)
		return sn.getStreamFieldValues(qctxLocal, fieldName, limit)
	})
}

// GetStreams executes qctx and returns streams seen in query results.
//
// If limit > 0, then up to limit unique streams are returned.
func (s *Storage) GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return s.getValuesWithHits(qctx, limit, true, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
		qctxLocal := qctx.WithContext(ctx)
		return sn.getStreams(qctxLocal, limit)
	})
}

// GetStreamIDs executes qctx and returns streamIDs seen in query results.
//
// If limit > 0, then up to limit unique streamIDs are returned.
func (s *Storage) GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return s.getValuesWithHits(qctx, limit, true, func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
		qctxLocal := qctx.WithContext(ctx)
		return sn.getStreamIDs(qctxLocal, limit)
	})
}

// DeleteRunTask starts deletion of logs for the given filter f at the given tenantIDs.
func (s *Storage) DeleteRunTask(ctx context.Context, taskID string, timestamp int64, tenantIDs []logstorage.TenantID, f *logstorage.Filter) error {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(s.sns))

	// Return an error to the caller when at least a single storage node is unavailable.
	// This improves awarenes of the caller about unavailable storage nodes.
	// If some storage node is unavailable, then the deletetion task
	// can start on arbitrary number of the remaining available nodes.
	// It is OK to re-run the delete task in this case.
	allowPartialResponse := false

	var wg sync.WaitGroup
	for nodeIdx := range s.sns {
		wg.Go(func() {
			sn := s.sns[nodeIdx]
			err := sn.deleteRunTask(ctxWithCancel, taskID, timestamp, tenantIDs, f)
			errs[nodeIdx] = sn.handleError(ctxWithCancel, cancel, err, allowPartialResponse)
		})
	}
	wg.Wait()

	return getFirstError(errs, allowPartialResponse)
}

// DeleteStopTask stops the delete task with the given taskID.
func (s *Storage) DeleteStopTask(ctx context.Context, taskID string) error {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(s.sns))

	// Return an error to the caller when at least a single storage node is unavailable.
	// This improves awarenes of the caller about unavailable storage nodes.
	// If some storage node is unavailable, then the deletion task can remain uncanceled on such nodes.
	// It is OK to stop the delete task multiple times in this case.
	allowPartialResponse := false

	var wg sync.WaitGroup
	for nodeIdx := range s.sns {
		wg.Go(func() {
			sn := s.sns[nodeIdx]
			err := sn.deleteStopTask(ctxWithCancel, taskID)
			errs[nodeIdx] = sn.handleError(ctxWithCancel, cancel, err, allowPartialResponse)
		})
	}
	wg.Wait()

	return getFirstError(errs, allowPartialResponse)
}

// DeleteActiveTasks returns the list of active delete tasks started via DeleteRunTask
func (s *Storage) DeleteActiveTasks(ctx context.Context) ([]*logstorage.DeleteTask, error) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(s.sns))
	results := make([][]*logstorage.DeleteTask, len(s.sns))

	// Return an error to the caller when at least a single storage node is unavailable,
	// since this prevents from returning the full list of active delete tasks.
	allowPartialResponse := false

	var wg sync.WaitGroup
	for nodeIdx := range s.sns {
		wg.Go(func() {
			sn := s.sns[nodeIdx]
			tasks, err := sn.deleteActiveTasks(ctxWithCancel)
			results[nodeIdx] = tasks
			errs[nodeIdx] = sn.handleError(ctxWithCancel, cancel, err, allowPartialResponse)
		})
	}
	wg.Wait()

	if err := getFirstError(errs, allowPartialResponse); err != nil {
		return nil, err
	}

	// Merge tasks received from storage nodes.
	m := make(map[string]*logstorage.DeleteTask)
	for _, tasks := range results {
		for _, dt := range tasks {
			dst := m[dt.TaskID]
			if dst == nil {
				m[dt.TaskID] = dt
			}
		}
	}

	tasks := make([]*logstorage.DeleteTask, 0, len(m))
	for _, t := range m {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

// GetTenantIDs returns tenantIDs for the given start and end.
func (s *Storage) GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error) {
	return s.getTenantIDs(ctx, start, end)
}

func (s *Storage) getTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error) {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]logstorage.TenantID, len(s.sns))
	errs := make([]error, len(s.sns))

	// Return an error to the caller when at least a single storage node is unavailable,
	// since this may result in incomplete list of the returned tenantIDs, which may mislead the caller.
	allowPartialResponse := false

	var wg sync.WaitGroup
	for nodeIdx := range s.sns {
		wg.Go(func() {
			sn := s.sns[nodeIdx]
			tenantIDs, err := sn.getTenantIDs(ctxWithCancel, start, end)
			results[nodeIdx] = tenantIDs
			errs[nodeIdx] = sn.handleError(ctxWithCancel, cancel, err, allowPartialResponse)

			if err != nil {
				// Cancel the remaining parallel requests
				cancel()
			}
		})
	}
	wg.Wait()

	if err := getFirstError(errs, allowPartialResponse); err != nil {
		return nil, err
	}

	// Deduplicate tenantIDs
	m := make(map[logstorage.TenantID]struct{})
	for _, tenantIDs := range results {
		for _, tenantID := range tenantIDs {
			m[tenantID] = struct{}{}
		}
	}

	tenantIDs := make([]logstorage.TenantID, 0, len(m))
	for tenantID := range m {
		tenantIDs = append(tenantIDs, tenantID)
	}

	return tenantIDs, nil
}

func (s *Storage) getValuesWithHits(qctx *logstorage.QueryContext, limit uint64, resetHitsOnLimitExceeded bool,
	callback func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error)) ([]logstorage.ValueWithHits, error) {

	ctxWithCancel, cancel := context.WithCancel(qctx.Context)
	defer cancel()

	results := make([][]logstorage.ValueWithHits, len(s.sns))
	errs := make([]error, len(s.sns))

	var wg sync.WaitGroup
	for nodeIdx := range s.sns {
		wg.Go(func() {
			sn := s.sns[nodeIdx]
			vhs, err := callback(ctxWithCancel, sn)
			results[nodeIdx] = vhs
			errs[nodeIdx] = sn.handleError(ctxWithCancel, cancel, err, qctx.AllowPartialResponse)
		})
	}
	wg.Wait()

	if err := getFirstError(errs, qctx.AllowPartialResponse); err != nil {
		return nil, err
	}

	vhs := logstorage.MergeValuesWithHits(results, limit, resetHitsOnLimitExceeded)

	return vhs, nil
}

func (sn *storageNode) deleteRunTask(ctx context.Context, taskID string, timestamp int64, tenantIDs []logstorage.TenantID, f *logstorage.Filter) error {
	args := url.Values{}
	args.Set("version", DeleteRunTaskProtocolVersion)
	args.Set("task_id", taskID)
	args.Set("timestamp", fmt.Sprintf("%d", timestamp))
	args.Set("tenant_ids", string(logstorage.MarshalTenantIDsToJSON(tenantIDs)))
	args.Set("filter", f.String())

	path := "/internal/delete/run_task"
	data, reqURL, err := sn.getPlainResponseBodyForPathAndArgs(ctx, path, args)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		return fmt.Errorf("unexpected response body received from %q: %q", reqURL, data)
	}

	return nil
}

func (sn *storageNode) deleteStopTask(ctx context.Context, taskID string) error {
	args := url.Values{}
	args.Set("version", DeleteStopTaskProtocolVersion)
	args.Set("task_id", taskID)

	path := "/internal/delete/stop_task"
	data, reqURL, err := sn.getPlainResponseBodyForPathAndArgs(ctx, path, args)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		return fmt.Errorf("unexpected response body received from %q: %q", reqURL, data)
	}

	return nil
}

func (sn *storageNode) deleteActiveTasks(ctx context.Context) ([]*logstorage.DeleteTask, error) {
	args := url.Values{}
	args.Set("version", DeleteActiveTasksProtocolVersion)

	path := "/internal/delete/active_tasks"
	data, reqURL, err := sn.getPlainResponseBodyForPathAndArgs(ctx, path, args)
	if err != nil {
		return nil, err
	}

	tasks, err := logstorage.UnmarshalDeleteTasksFromJSON(data)
	if err != nil {
		return nil, fmt.Errorf("cannot parse response from %q: %w; response body: %q", reqURL, err, data)
	}

	return tasks, nil
}

func (sn *storageNode) getPlainResponseBodyForPathAndArgs(ctx context.Context, path string, args url.Values) ([]byte, string, error) {
	responseBody, reqURL, err := sn.getResponseBodyForPathAndArgs(ctx, path, args)
	if err != nil {
		return nil, reqURL, err
	}
	defer responseBody.Close()

	data, err := io.ReadAll(responseBody)
	if err != nil {
		return nil, reqURL, fmt.Errorf("cannot read response from %q: %w", reqURL, err)
	}

	return data, reqURL, nil
}

// handleError processes errors from individual storage node queries.
// It increments the error counter and cancels remaining queries if the error should
// be immediately returned to the client (i.e., not a partial response scenario).
func (sn *storageNode) handleError(ctx context.Context, cancel func(), err error, allowPartialResponse bool) error {
	if err == nil {
		// Nothing to handle.
		return nil
	}
	if err := ctx.Err(); err != nil {
		// The context error overrides all the other errors.
		// It must be handled separately by the caller.
		return nil
	}

	sn.sendErrors.Inc()

	if !allowPartialResponse || !isUnavailableBackendError(err) {
		// Cancel the remaining parallel queries, since the error must be returned to the client ASAP
		// without waiting for the remaining parallel queries to other backends.
		cancel()
	}

	return err
}

// getFirstError determines the final error to return from a distributed query.
//
// When allowPartialResponse is false: return the first non-nil error from any node.
// This guarantees query completeness - all data must be available.
//
// When allowPartialResponse is true:
//   - Return nil if at least one node responded successfully
//   - Return the error if it's a configuration error (not network-related)
//   - Return "all nodes unavailable" only if every node failed with network errors
func getFirstError(errs []error, allowPartialResponse bool) error {
	if len(errs) == 0 {
		logger.Panicf("BUG: len(errs) must be bigger than 0")
	}

	if !allowPartialResponse {
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}

	// allowPartialResponse == true. Return the error only if all the backends are unavailable
	// or if some of the backends are improperly configured.
	for _, err := range errs {
		if err == nil {
			// At least a single vlstorage returned full response.
			return nil
		}
		if !isUnavailableBackendError(err) {
			// Return the first error, which isn't related to the backend unavailability, to the client,
			// since this error may point to configuration issues, which must be fixed ASAP.
			// Hiding this error would complicate troubleshooting of improperly configured system.
			return fmt.Errorf("the vlstorage node is available, but it returns an error, which may point to configuration issues: %w", err)
		}
	}

	return fmt.Errorf("all the vlstorage nodes are unavailable for querying; a sample error: %w", errs[0])
}

// isUnavailableBackendError checks if an error is due to backend unavailability.
// Unavailable errors are wrapped in httpserver.ErrorWithStatusCode (typically 502).
// This is used to differentiate "backend down" from "configuration error" for
// partial response handling - we return config errors even with partial responses.
func isUnavailableBackendError(err error) bool {
	// It is expected that unavailable backend errors are wrapped into httpserver.ErrorWithStatusCode.
	var es *httpserver.ErrorWithStatusCode
	return errors.As(err, &es)
}

func unmarshalValuesWithHits(qctx *logstorage.QueryContext, src []byte) ([]logstorage.ValueWithHits, error) {
	// Unmarshal ValuesWithHits at first
	if len(src) < 8 {
		return nil, fmt.Errorf("missing length of ValueWithHits entries")
	}
	vhsLen := encoding.UnmarshalUint64(src[:8])
	src = src[8:]

	vhs := make([]logstorage.ValueWithHits, vhsLen)
	for i := range vhs {
		vh := &vhs[i]

		tail, err := vh.UnmarshalInplace(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal ValueWithHits #%d out of %d: %w", i, len(vhs), err)
		}
		src = tail

		// Clone vh.Value, since it points to src.
		vh.Value = strings.Clone(vh.Value)
	}

	// Unmarshal query stats
	qsLocal := &logstorage.QueryStats{}
	defer qctx.QueryStats.UpdateAtomic(qsLocal)

	tail, err := unmarshalQueryStats(qsLocal, src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal query stats: %w", err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after query stats; len(tail)=%d", len(tail))
	}

	return vhs, nil
}

func unmarshalQueryStats(qs *logstorage.QueryStats, src []byte) ([]byte, error) {
	var db logstorage.DataBlock
	tail, _, err := db.UnmarshalInplace(src, nil)
	if err != nil {
		return tail, fmt.Errorf("cannot unmarshal data block: %w", err)
	}
	if err := qs.UpdateFromDataBlock(&db); err != nil {
		return tail, fmt.Errorf("cannot read query stats: %w", err)
	}
	return tail, nil
}
