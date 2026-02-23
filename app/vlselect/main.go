// Package vlselect implements the HTTP query layer for VictoriaLogs.
//
// This package is the entry point for all log queries from external clients. It handles:
//   - HTTP routing for /select/*, /delete/*, and /internal/select/* endpoints
//   - Concurrency control to prevent resource exhaustion from too many parallel queries
//   - Request timeout management with configurable per-query limits
//   - Live tailing for real-time log streaming
//   - Asynchronous delete task management
//
// Architecture Overview:
//
//	The query flow through this package is:
//
//	HTTP Request → RequestHandler (routing) → selectHandler (concurrency + timeout)
//	    → processSelectRequest (path dispatch) → logsql.ProcessQueryRequest (parsing)
//	    → vlstorage.RunQuery (execution) → streaming JSON response
//
// Concurrency Control:
//
//	Queries are CPU-intensive operations that can saturate all cores. The default
//	concurrency limit is calculated based on CPU count (2× for ≤4 CPUs, capped at 16
//	for larger systems). This prevents CPU thrashing when many queries arrive simultaneously.
//
//	Requests that exceed the limit wait in a queue until a slot opens or the request
//	times out. The wait is bounded by the request's timeout (either the 'timeout' query
//	parameter or -search.maxQueryDuration).
//
// Live Tailing:
//
//	The /select/logsql/tail endpoint is special - it bypasses concurrency limits because
//	it's a long-lived connection that polls for new logs at regular intervals. These
//	connections can run for hours or days, so they're handled separately.
//
// See onboarding/onboarding-select-flow.md for the complete query flow documentation.
package vlselect

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	// Concurrency control flags
	//
	// These flags manage how many queries can run simultaneously. The limits are crucial
	// because log queries are CPU-intensive and can consume significant memory when
	// processing large result sets.

	// maxConcurrentRequests limits parallel query execution. The default is CPU-based:
	// - 2× CPUs when CPUs ≤ 4 (to utilize small systems better)
	// - Capped at 16 for larger systems (a single query can saturate all cores)
	// Going higher causes CPU contention without throughput improvement.
	maxConcurrentRequests = flag.Int("search.maxConcurrentRequests", getDefaultMaxConcurrentRequests(), "The maximum number of concurrent search requests. "+
		"It shouldn't be high, since a single request can saturate all the CPU cores, while many concurrently executed requests may require high amounts of memory. "+
		"See also -search.maxQueueDuration")

	// maxQueueDuration is a hint for queue wait time. The actual wait is bounded by
	// the request's timeout (from 'timeout' param or -search.maxQueryDuration).
	maxQueueDuration = flag.Duration("search.maxQueueDuration", 10*time.Second, "The maximum time the search request waits for execution when -search.maxConcurrentRequests "+
		"limit is reached; see also -search.maxQueryDuration")

	// maxQueryDuration is the default query timeout. Individual queries can request
	// a shorter timeout via the 'timeout' query parameter, but cannot exceed this limit.
	maxQueryDuration = flag.Duration("search.maxQueryDuration", time.Second*30, "The maximum duration for query execution. It can be overridden to a smaller value on a per-query basis via 'timeout' query arg")

	// Endpoint control flags
	// These allow disabling specific endpoint groups for security or operational reasons.

	disableSelect         = flag.Bool("select.disable", false, "Whether to disable /select/* HTTP endpoints")
	disableInternalSelect = flag.Bool("internalselect.disable", false, "Whether to disable /internal/select/* HTTP endpoints")

	// Delete is disabled by default because it's a destructive operation.
	// The internal delete endpoints are used for cluster mode fan-out.
	enableDelete         = flag.Bool("delete.enable", false, "Whether to enable /delete/* HTTP endpoints; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")
	enableInternalDelete = flag.Bool("internaldelete.enable", false, "Whether to enable /internal/delete/* HTTP endpoints, which are used by vlselect for deleting logs "+
		"via delete API at vlstorage nodes; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")

	// Slow query logging helps identify queries that need optimization
	logSlowQueryDuration = flag.Duration("search.logSlowQueryDuration", 5*time.Second,
		"Log queries with execution time exceeding this value. Zero disables slow query logging")
)

// getDefaultMaxConcurrentRequests calculates the default concurrency limit based on CPU count.
//
// The logic balances throughput against CPU contention:
//   - Small systems (≤4 CPUs): Allow 2× CPUs to improve utilization
//   - Large systems: Cap at 16 because a single query can saturate all cores
//
// Why cap at 16? When queries compete for CPU time, they all slow down without
// improving total throughput. It's better to queue excess requests than to thrash.
func getDefaultMaxConcurrentRequests() int {
	n := cgroup.AvailableCPUs()
	if n <= 4 {
		n *= 2
	}
	if n > 16 {
		n = 16
	}
	return n
}

// Init initializes the vlselect package.
//
// This must be called before any requests are handled. It:
//   - Creates the concurrency limit channel (buffered channel used as semaphore)
//   - Initializes the internalselect package for /internal/select/* endpoints
//
// The concurrency limit channel size matches -search.maxConcurrentRequests.
// When full, new requests block until a slot opens (bounded by request timeout).
func Init() {
	concurrencyLimitCh = make(chan struct{}, *maxConcurrentRequests)

	internalselect.Init()
}

// Stop stops vlselect
func Stop() {
	internalselect.Stop()

	concurrencyLimitCh = nil
}

// concurrencyLimitCh is a buffered channel used as a counting semaphore.
//
// How it works:
//   - Capacity = maxConcurrentRequests
//   - Each query sends a struct{}{} to acquire a slot
//   - When done, it receives <-concurrencyLimitCh to release
//   - If full, requests block until a slot opens (bounded by timeout)
//
// This pattern is simpler than mutex-based semaphores and integrates naturally
// with Go's select statement for timeout handling.
var concurrencyLimitCh chan struct{}

var (
	// Metrics for monitoring the concurrency limiter behavior
	concurrencyLimitReached = metrics.NewCounter(`vl_concurrent_select_limit_reached_total`)
	concurrencyLimitTimeout = metrics.NewCounter(`vl_concurrent_select_limit_timeout_total`)

	// Capacity and current usage gauges for dashboards
	_ = metrics.NewGauge(`vl_concurrent_select_capacity`, func() float64 {
		return float64(cap(concurrencyLimitCh))
	})
	_ = metrics.NewGauge(`vl_concurrent_select_current`, func() float64 {
		return float64(len(concurrencyLimitCh))
	})
)

// vmuiFiles embeds the VMUI web interface assets.
// VMUI provides a graphical interface for querying and exploring logs.
//
//go:embed vmui
var vmuiFiles embed.FS

// vmuiFileServer serves the embedded VMUI static files.
var vmuiFileServer = http.FileServer(http.FS(vmuiFiles))

// RequestHandler is the main HTTP router for all /select/*, /delete/*, and /internal/select/* paths.
//
// This function is called by the main HTTP server for every incoming request.
// It returns true if the request was handled (even if an error occurred),
// or false if the path doesn't match any vlselect endpoint.
//
// Routing order matters:
//  1. /delete/* - Check enableDelete flag first
//  2. /select/* - Check disableSelect flag
//  3. /internal/delete/* - Check enableInternalDelete flag
//  4. /internal/select/* - Check both disable flags
//
// The /internal/* endpoints are used for cluster-mode communication between
// vlselect (frontend) and vlstorage nodes. They're typically not exposed to
// external clients.
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
	path := strings.ReplaceAll(r.URL.Path, "//", "/")

	if strings.HasPrefix(path, "/delete/") {
		if !*enableDelete {
			httpserver.Errorf(w, r, "requests to /delete/* are disabled; pass -delete.enable command-line flag for enabling them; "+
				"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")
			return true
		}
		deleteHandler(w, r, path)
		return true
	}

	if strings.HasPrefix(path, "/select/") {
		if *disableSelect {
			httpserver.Errorf(w, r, "requests to /select/* are disabled with -select.disable command-line flag")
			return true
		}

		return selectHandler(w, r, path)
	}

	if strings.HasPrefix(path, "/internal/delete/") {
		if !*enableInternalDelete {
			httpserver.Errorf(w, r, "requests to /internal/delete/*` are disabled; pass -internaldelete.enable command-line flag for enabling them; "+
				"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")
			return true
		}
		internalselect.RequestHandler(r.Context(), w, r)
		return true
	}

	if strings.HasPrefix(path, "/internal/select/") {
		if *disableInternalSelect {
			httpserver.Errorf(w, r, "requests to /internal/select/* are disabled with -internalselect.disable command-line flag")
			return true
		}
		if *disableSelect {
			httpserver.Errorf(w, r, "requests to /internal/select/* are disabled with -select.disable command-line flag")
			return true
		}
		internalselect.RequestHandler(r.Context(), w, r)
		return true
	}

	return false
}

// selectHandler handles all /select/* requests with timeout and concurrency control.
//
// Special cases handled before concurrency control:
//   - /select/buildinfo: Returns version info (no timeout, no concurrency limit)
//   - /select/vmui: Redirects to VMUI (no timeout, no concurrency limit)
//   - /select/logsql/tail: Live tailing (no timeout, no concurrency limit - runs indefinitely)
//
// For all other queries:
//  1. Parse the timeout from request or use -search.maxQueryDuration
//  2. Create a context with the timeout
//  3. Acquire a concurrency slot (blocks if limit reached)
//  4. Execute the query via processSelectRequest
//  5. Release the concurrency slot on completion
//  6. Log slow queries if -search.logSlowQueryDuration is exceeded
//
// The tail endpoint bypasses concurrency limits because it's a long-lived polling
// connection that's mostly idle between poll intervals.
func selectHandler(w http.ResponseWriter, r *http.Request, path string) bool {
	ctx := r.Context()

	// Handle /select/buildinfo - returns VictoriaLogs version info
	if path == "/select/buildinfo" {
		httpserver.EnableCORS(w, r)

		if r.Method != http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			fmt.Fprintf(w, `{"status":"error","msg":"method %q isn't allowed"}`, r.Method)
			return true
		}

		v := buildinfo.ShortVersion()
		if v == "" {
			v = buildinfo.Version
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","data":{"version":%q}}`, v)
		return true
	}

	// Handle /select/vmui - redirect to the VMUI web interface
	if path == "/select/vmui" {
		_ = r.ParseForm()
		newURL := "vmui/?" + r.Form.Encode()
		httpserver.Redirect(w, newURL)
		return true
	}
	if strings.HasPrefix(path, "/select/vmui/") {
		// Static assets get long cache TTL since they're versioned by path
		if strings.HasPrefix(path, "/select/vmui/static/") {
			w.Header().Set("Cache-Control", "max-age=31536000")
		}
		r.URL.Path = strings.TrimPrefix(path, "/select")
		vmuiFileServer.ServeHTTP(w, r)
		return true
	}

	// Live tailing: no timeout, no concurrency limit
	// These are long-lived connections that poll for new logs periodically
	if path == "/select/logsql/tail" {
		logsqlTailRequests.Inc()
		logsql.ProcessLiveTailRequest(ctx, w, r)
		return true
	}

	// All other queries: apply timeout and concurrency limits
	startTime := time.Now()
	d, err := getMaxQueryDuration(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return true
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	// Try to acquire a concurrency slot; returns false if timed out waiting
	if !incRequestConcurrency(ctxWithTimeout, w, r) {
		return true
	}
	defer decRequestConcurrency()

	ok := processSelectRequest(ctxWithTimeout, w, r, path)
	if !ok {
		return false
	}

	// Log slow queries for debugging and optimization
	if *logSlowQueryDuration > 0 {
		d := time.Since(startTime)
		if d >= *logSlowQueryDuration {
			remoteAddr := httpserver.GetQuotedRemoteAddr(r)
			requestURI := httpserver.GetRequestURI(r)
			logger.Warnf("slow query according to -search.logSlowQueryDuration=%s: remoteAddr=%s, duration=%.3f seconds; requestURI: %q",
				*logSlowQueryDuration, remoteAddr, d.Seconds(), requestURI)
			slowQueries.Inc()
		}
	}

	logRequestErrorIfNeeded(ctxWithTimeout, w, r, startTime)
	return true
}

// logRequestErrorIfNeeded logs context errors that occurred during query execution.
//
// This handles three cases:
//   - nil: Query completed successfully, nothing to log
//   - context.Canceled: Client disconnected (expected, don't log as error)
//   - context.DeadlineExceeded: Query timed out - log with helpful suggestions
func logRequestErrorIfNeeded(ctx context.Context, w http.ResponseWriter, r *http.Request, startTime time.Time) {
	err := ctx.Err()
	switch err {
	case nil:
	case context.Canceled:
		// Client canceled - this is expected and legal, don't spam logs
	case context.DeadlineExceeded:
		// Query timed out - provide actionable suggestions
		err = &httpserver.ErrorWithStatusCode{
			Err: fmt.Errorf("the request couldn't be executed in %.3f seconds; possible solutions: "+
				"to increase -search.maxQueryDuration=%s; to pass bigger value to 'timeout' query arg", time.Since(startTime).Seconds(), maxQueryDuration),
			StatusCode: http.StatusServiceUnavailable,
		}
		httpserver.Errorf(w, r, "%s", err)
	default:
		httpserver.Errorf(w, r, "unexpected error: %s", err)
	}
}

// incRequestConcurrency tries to acquire a slot in the concurrency limiter.
//
// Returns true if a slot was acquired, false if the context expired while waiting.
//
// The algorithm uses two-stage blocking:
//  1. Non-blocking try: If channel has space, acquire immediately
//  2. Blocking wait: If full, wait for space or context cancellation
//
// This approach handles short request bursts efficiently (stage 1) while still
// providing queueing for brief overload periods (stage 2).
func incRequestConcurrency(ctx context.Context, w http.ResponseWriter, r *http.Request) bool {
	startTime := time.Now()
	stopCh := ctx.Done()
	select {
	case concurrencyLimitCh <- struct{}{}:
		// Fast path: slot available immediately
		return true
	default:
		// Slow path: need to wait for a slot
		concurrencyLimitReached.Inc()
		select {
		case concurrencyLimitCh <- struct{}{}:
			// Slot became available
			return true
		case <-stopCh:
			// Context expired while waiting
			switch ctx.Err() {
			case context.Canceled:
				// Client disconnected
				remoteAddr := httpserver.GetQuotedRemoteAddr(r)
				requestURI := httpserver.GetRequestURI(r)
				logger.Infof("client has canceled the pending request after %.3f seconds: remoteAddr=%s, requestURI: %q",
					time.Since(startTime).Seconds(), remoteAddr, requestURI)
			case context.DeadlineExceeded:
				// Timeout while waiting - return error with suggestions
				concurrencyLimitTimeout.Inc()
				err := &httpserver.ErrorWithStatusCode{
					Err: fmt.Errorf("couldn't start executing the request in %.3f seconds, since -search.maxConcurrentRequests=%d concurrent requests "+
						"are executed. Possible solutions: to reduce query load; to add more compute resources to the server; "+
						"to increase -search.maxQueueDuration=%s; to increase -search.maxQueryDuration=%s; to increase -search.maxConcurrentRequests; "+
						"to pass bigger value to 'timeout' query arg",
						time.Since(startTime).Seconds(), *maxConcurrentRequests, maxQueueDuration, maxQueryDuration),
					StatusCode: http.StatusServiceUnavailable,
				}
				httpserver.Errorf(w, r, "%s", err)
			}
			return false
		}
	}
}

// decRequestConcurrency releases a concurrency slot.
// This must be called after a successful incRequestConcurrency, typically via defer.
func decRequestConcurrency() {
	<-concurrencyLimitCh
}

// processSelectRequest routes requests to the appropriate handler based on URL path.
//
// Each endpoint has associated metrics (request count and duration) that are
// updated automatically. Duration isn't tracked only for tail and query_time_range.
//
// Returns false if the path doesn't match any known endpoint (for 404 handling).
func processSelectRequest(ctx context.Context, w http.ResponseWriter, r *http.Request, path string) bool {
	httpserver.EnableCORS(w, r)
	startTime := time.Now()
	switch path {
	case "/select/logsql/query_time_range":
		// Returns the effective time range for a query (doesn't execute the query)
		logsqlQueryTimeRangeRequests.Inc()
		logsql.ProcessQueryTimeRangeRequest(ctx, w, r)
		return true
	case "/select/logsql/facets":
		// Returns field value facets with hit counts
		logsqlFacetsRequests.Inc()
		logsql.ProcessFacetsRequest(ctx, w, r)
		logsqlFacetsDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/field_names":
		// Lists field names seen in matching logs
		logsqlFieldNamesRequests.Inc()
		logsql.ProcessFieldNamesRequest(ctx, w, r)
		logsqlFieldNamesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/field_values":
		// Lists unique values for a specific field
		logsqlFieldValuesRequests.Inc()
		logsql.ProcessFieldValuesRequest(ctx, w, r)
		logsqlFieldValuesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/hits":
		// Returns hit counts over time buckets (histogram)
		logsqlHitsRequests.Inc()
		logsql.ProcessHitsRequest(ctx, w, r)
		logsqlHitsDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/query":
		// Main query endpoint - streams matching logs as NDJSON
		logsqlQueryRequests.Inc()
		logsql.ProcessQueryRequest(ctx, w, r)
		logsqlQueryDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stats_query":
		// Prometheus-style instant vector query
		logsqlStatsQueryRequests.Inc()
		logsql.ProcessStatsQueryRequest(ctx, w, r)
		logsqlStatsQueryDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stats_query_range":
		// Prometheus-style range vector query
		logsqlStatsQueryRangeRequests.Inc()
		logsql.ProcessStatsQueryRangeRequest(ctx, w, r)
		logsqlStatsQueryRangeDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stream_field_names":
		// Lists stream field names (fields that define streams)
		logsqlStreamFieldNamesRequests.Inc()
		logsql.ProcessStreamFieldNamesRequest(ctx, w, r)
		logsqlStreamFieldNamesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stream_field_values":
		// Lists values for a specific stream field
		logsqlStreamFieldValuesRequests.Inc()
		logsql.ProcessStreamFieldValuesRequest(ctx, w, r)
		logsqlStreamFieldValuesDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/stream_ids":
		// Lists internal stream IDs
		logsqlStreamIDsRequests.Inc()
		logsql.ProcessStreamIDsRequest(ctx, w, r)
		logsqlStreamIDsDuration.UpdateDuration(startTime)
		return true
	case "/select/logsql/streams":
		// Lists log streams (unique combinations of stream field values)
		logsqlStreamsRequests.Inc()
		logsql.ProcessStreamsRequest(ctx, w, r)
		logsqlStreamsDuration.UpdateDuration(startTime)
		return true
	case "/select/tenant_ids":
		// Lists tenant IDs with data in the specified time range
		tenantIDsRequests.Inc()
		logsql.ProcessTenantIDsRequest(ctx, w, r)
		tenantIDsDuration.UpdateDuration(startTime)
		return true
	default:
		return false
	}
}

// deleteHandler routes delete requests to the appropriate handler.
//
// Delete operations are asynchronous and run as background tasks. This allows
// deletion of large amounts of data without blocking the HTTP request.
func deleteHandler(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	switch path {
	case "/delete/run_task":
		// Start a new delete task
		deleteRunTaskRequests.Inc()
		processDeleteRunTaskRequest(ctx, w, r)
	case "/delete/stop_task":
		// Stop a running delete task
		deleteStopTaskRequests.Inc()
		processDeleteStopTaskRequest(ctx, w, r)
	case "/delete/active_tasks":
		// List all active (running) delete tasks
		deleteActiveTasksRequests.Inc()
		processDeleteActiveTasksRequest(ctx, w, r)
	default:
		httpserver.Errorf(w, r, "unsupported path requested: %q", path)
	}
}

// processDeleteRunTaskRequest starts an asynchronous delete task.
//
// Delete tasks run in the background and can be monitored via /delete/active_tasks
// and stopped via /delete/stop_task. The task ID is generated from the current
// timestamp (nanoseconds) for uniqueness.
//
// In cluster mode, the task is started on ALL vlstorage nodes simultaneously.
// Each node deletes only the logs it stores locally.
func processDeleteRunTaskRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain tenantID: %s", err)
		return
	}

	// Parse the LogsQL filter that determines what to delete
	fStr := r.FormValue("filter")
	f, err := logstorage.ParseFilter(fStr)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse filter [%s]: %s", fStr, err)
		return
	}

	// Generate unique task ID from current timestamp
	timestamp := time.Now().UnixNano()
	taskID := fmt.Sprintf("%d", timestamp)

	tenantIDs := []logstorage.TenantID{tenantID}
	if err := vlstorage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f); err != nil {
		httpserver.Errorf(w, r, "cannot run delete task: %s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"task_id":%q}`, taskID)
}

// processDeleteStopTaskRequest stops a running delete task.
//
// Stopping a task prevents further deletion, but logs that were already deleted
// before the stop request are permanently removed.
func processDeleteStopTaskRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	taskID := r.FormValue("task_id")
	if taskID == "" {
		httpserver.Errorf(w, r, "missing task_id arg")
		return
	}

	if err := vlstorage.DeleteStopTask(ctx, taskID); err != nil {
		httpserver.Errorf(w, r, "cannot stop task with task_id=%q: %s", taskID, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok"}`)
}

// processDeleteActiveTasksRequest returns all currently running delete tasks.
//
// In cluster mode, tasks from all vlstorage nodes are merged and deduplicated.
// The response includes task ID, filter, timestamp, and tenant IDs for each task.
func processDeleteActiveTasksRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	tasks, err := vlstorage.DeleteActiveTasks(ctx)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain active delete tasks: %s", err)
		return
	}

	data := logstorage.MarshalDeleteTasksToJSON(tasks)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "%s", data)
}

// getMaxQueryDuration returns the maximum duration for query from r.
//
// The timeout is determined by:
//  1. If 'timeout' query arg is provided: use that value (capped by -search.maxQueryDuration)
//  2. Otherwise: use -search.maxQueryDuration (default 30s)
//
// This allows clients to request shorter timeouts for interactive queries while
// preventing abuse via excessively long timeouts.
func getMaxQueryDuration(r *http.Request) (time.Duration, error) {
	s := r.FormValue("timeout")
	if s == "" {
		s = "0s"
	}
	nsecs, ok := logstorage.TryParseDuration(s)
	if !ok {
		return 0, fmt.Errorf("cannot parse duration at 'timeout=%s' arg", s)
	}
	d := time.Duration(nsecs)
	// Cap to the maximum allowed duration
	if d <= 0 || d > *maxQueryDuration {
		d = *maxQueryDuration
	}
	return d, nil
}

var (
	// Request counters and duration metrics for each query endpoint
	// These follow the pattern: vl_http_requests_total{path="/select/..."}
	// and vl_http_request_duration_seconds{path="/select/..."}

	logsqlFacetsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/facets"}`)
	logsqlFacetsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/facets"}`)

	logsqlFieldNamesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/field_names"}`)
	logsqlFieldNamesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/field_names"}`)

	logsqlFieldValuesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/field_values"}`)
	logsqlFieldValuesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/field_values"}`)

	logsqlHitsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/hits"}`)
	logsqlHitsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/hits"}`)

	logsqlQueryRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/query"}`)
	logsqlQueryDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/query"}`)

	logsqlStatsQueryRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stats_query"}`)
	logsqlStatsQueryDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stats_query"}`)

	logsqlStatsQueryRangeRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stats_query_range"}`)
	logsqlStatsQueryRangeDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stats_query_range"}`)

	logsqlStreamFieldNamesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stream_field_names"}`)
	logsqlStreamFieldNamesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stream_field_names"}`)

	logsqlStreamFieldValuesRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stream_field_values"}`)
	logsqlStreamFieldValuesDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stream_field_values"}`)

	logsqlStreamIDsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/stream_ids"}`)
	logsqlStreamIDsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/stream_ids"}`)

	logsqlStreamsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/streams"}`)
	logsqlStreamsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/logsql/streams"}`)

	tenantIDsRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/tenant_ids"}`)
	tenantIDsDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/select/tenant_ids"}`)

	// Tail requests don't track duration since they run indefinitely
	logsqlTailRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/tail"}`)

	// query_time_range is instant, no duration tracking needed
	logsqlQueryTimeRangeRequests = metrics.NewCounter(`vl_http_requests_total{path="/select/logsql/query_time_range"}`)

	// Delete endpoints don't track duration since they're asynchronous
	deleteRunTaskRequests     = metrics.NewCounter(`vl_http_requests_total{path="/delete/run_task"}`)
	deleteStopTaskRequests    = metrics.NewCounter(`vl_http_requests_total{path="/delete/stop_task"}`)
	deleteActiveTasksRequests = metrics.NewCounter(`vl_http_requests_total{path="/delete/active_tasks"}`)

	// Slow query counter for monitoring query performance
	slowQueries = metrics.NewCounter(`vl_slow_queries_total`)
)
