// Package logsql implements the HTTP handlers for all /select/logsql/* query endpoints.
//
// This package is responsible for:
//   - Parsing HTTP request parameters into LogsQL queries
//   - Transforming queries for specific endpoint needs (e.g., adding stats pipes for hits)
//   - Executing queries via vlstorage.RunQuery()
//   - Formatting results as JSON responses
//
// Query Endpoint Categories:
//
// 1. Log Query Endpoints:
//   - /select/logsql/query: Stream matching logs as NDJSON
//   - /select/logsql/tail: Live tailing for real-time log streaming
//
// 2. Aggregation Endpoints:
//   - /select/logsql/hits: Hit counts over time buckets
//   - /select/logsql/stats_query: Instant statistics (Prometheus-style)
//   - /select/logsql/stats_query_range: Range statistics (Prometheus-style)
//   - /select/logsql/facets: Field value facets with hit counts
//
// 3. Metadata Endpoints:
//   - /select/logsql/field_names: List field names
//   - /select/logsql/field_values: List unique values for a field
//   - /select/logsql/streams: List log streams
//   - /select/logsql/stream_ids: List internal stream IDs
//   - /select/logsql/stream_field_names: List stream field names
//   - /select/logsql/stream_field_values: List stream field values
//
// Common Request Pattern:
//
// All endpoints (except query_time_range and tenant_ids) follow this pattern:
//  1. Parse common args via parseCommonArgs() (query, tenant, time range, etc.)
//  2. Optionally modify the query (add pipes, drop pipes)
//  3. Create a writeBlock callback to collect results
//  4. Execute via vlstorage.RunQuery()
//  5. Format and write JSON response
//
// See onboarding/onboarding-select-flow.md for the complete query flow documentation.
package logsql

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"
	"github.com/valyala/fastjson"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	// maxQueryTimeRange limits the maximum time range allowed in queries.
	// This prevents resource exhaustion from queries spanning years of data.
	// Set to 0 (default) for unlimited time range.
	maxQueryTimeRange = flagutil.NewExtendedDuration("search.maxQueryTimeRange", "0", "The maximum time range, which can be set in the query sent to querying APIs. "+
		"Queries with bigger time ranges are rejected. See https://docs.victoriametrics.com/victorialogs/querying/#resource-usage-limits")

	// allowPartialResponseFlag controls behavior in cluster mode when some
	// vlstorage nodes are unavailable:
	//   - false (default): Return error if any node is unavailable
	//   - true: Return partial results from available nodes
	//
	// This trades completeness for availability during node outages.
	allowPartialResponseFlag = flag.Bool("search.allowPartialResponse", false, "Whether to allow returning partial responses when some of vlstorage nodes "+
		"from the -storageNode list are unavailable for querying. This flag works only for cluster setup of VictoriaLogs. "+
		"See https://docs.victoriametrics.com/victorialogs/querying/#partial-responses")
)

// ProcessQueryTimeRangeRequest handles /select/logsql/query_time_range request.
//
// This endpoint returns the effective time range that a query would scan,
// WITHOUT actually executing the query. It's useful for UI tools that need
// to know the query scope before running it.
//
// The format of the returned JSON:
//
//	{
//	  "start":"YYYY-MM-DDThh:mm:sss.nnnnnnnnnZ",
//	  "end":"YYYY-MM-DDThh:mm:sss.nnnnnnnnnZ",
//	  "hasTimeFilter":true|false
//	}
//
// hasTimeFilter indicates whether the query itself contains a _time filter.
// If false, start/end come from the request's start/end parameters.
func ProcessQueryTimeRangeRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	minTimestamp, maxTimestamp, hasTimeFilter, err := parseQueryTimeRangeArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	startStr := timestampToRFC3339Nano(minTimestamp)
	endStr := timestampToRFC3339Nano(maxTimestamp)
	fmt.Fprintf(w, `{"start":%q,"end":%q,"hasTimeFilter":%t}`, startStr, endStr, hasTimeFilter)
}

// parseQueryTimeRangeArgs extracts the time range from query and request parameters.
//
// For each boundary:
//  1. Use _time filter boundary from the query if it is present
//  2. Otherwise, fall back to start/end request parameters
//
// Returns hasTimeFilter=true if the query contains its own _time filter.
func parseQueryTimeRangeArgs(r *http.Request) (int64, int64, bool, error) {
	qStr := r.FormValue("query")
	if qStr == "" {
		return 0, 0, false, fmt.Errorf("`query` arg cannot be empty")
	}
	currTimestamp := time.Now().UnixNano()
	q, err := logstorage.ParseQueryAtTimestamp(qStr, currTimestamp)
	if err != nil {
		return 0, 0, false, fmt.Errorf("cannot parse query [%s]: %s", qStr, err)
	}

	minTimestamp, maxTimestamp := q.GetFilterTimeRange()

	// hasTimeFilter is true if the query itself contains a _time filter
	hasTimeFilter := (minTimestamp != math.MinInt64 || maxTimestamp != math.MaxInt64)

	// If query doesn't define start/end boundary, fall back to request parameters
	if minTimestamp == math.MinInt64 {
		start, ok, err := getTimeNsec(r, "start")
		if err != nil {
			return 0, 0, false, err
		}
		if ok {
			minTimestamp = start
		}
	}
	if maxTimestamp == math.MaxInt64 {
		end, ok, err := getTimeNsec(r, "end")
		if err != nil {
			return 0, 0, false, err
		}
		if ok {
			maxTimestamp = end
		}
	}

	return minTimestamp, maxTimestamp, hasTimeFilter, nil
}

func timestampToRFC3339Nano(nsec int64) string {
	return time.Unix(0, nsec).UTC().Format(time.RFC3339Nano)
}

// ProcessFacetsRequest handles /select/logsql/facets request.
//
// Facets provide a way to explore the distribution of field values in log data.
// For each field, it returns the top N values with their hit counts.
//
// Query transformation:
//   - All existing pipes are dropped (facets must come from raw data)
//   - A facets pipe is added to compute the aggregation
//
// Response format:
//
//	{"facets":{"field1":[{"value":"x","hits":100},...],"field2":[...]}}
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-facets
func ProcessFacetsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	maxValuesPerField, err := getPositiveInt(r, "max_values_per_field")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	maxValueLen, err := getPositiveInt(r, "max_value_len")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	keepConstFields := httputil.GetBool(r, "keep_const_fields")

	// Pipes must be dropped, since facets are computed from raw log data,
	// not from transformed results
	ca.q.DropAllPipes()

	ca.q.AddFacetsPipe(limit, maxValuesPerField, maxValueLen, keepConstFields)

	// Collect facets results into a map: fieldName -> []facetEntry
	var mLock sync.Mutex
	m := make(map[string][]facetEntry)
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		if rowsCount == 0 {
			return
		}

		columns := db.Columns
		if len(columns) != 3 {
			logger.Panicf("BUG: expecting 3 columns; got %d columns", len(columns))
		}

		// Fetch columns by name to avoid relying on column ordering
		// (different VictoriaLogs versions may return columns in different order)
		cFieldName := db.GetColumnByName("field_name")
		cFieldValue := db.GetColumnByName("field_value")
		cHits := db.GetColumnByName("hits")
		if cFieldName == nil || cFieldValue == nil || cHits == nil {
			logger.Panicf("BUG: missing expected columns for facets response: field_name=%v, field_value=%v, hits=%v",
				cFieldName != nil, cFieldValue != nil, cHits != nil)
		}

		fieldNames := cFieldName.Values
		fieldValues := cFieldValue.Values
		hits := cHits.Values

		bb := blockResultPool.Get()
		for i := range fieldNames {
			fieldName := strings.Clone(fieldNames[i])
			fieldValue := strings.Clone(fieldValues[i])
			hitsStr := strings.Clone(hits[i])

			mLock.Lock()
			m[fieldName] = append(m[fieldName], facetEntry{
				value: fieldValue,
				hits:  hitsStr,
			})
			mLock.Unlock()
		}
		blockResultPool.Put(bb)
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the query
	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		httpserver.Errorf(w, r, "cannot execute query [%s]: %s", ca.q, err)
		return
	}

	// Write response header
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write response
	WriteFacetsResponse(w, m)
}

// facetEntry represents a single facet value with its hit count
type facetEntry struct {
	value string
	hits  string
}

// ProcessHitsRequest handles /select/logsql/hits request.
//
// Hits returns the count of matching log entries grouped into time buckets,
// optionally further grouped by specified fields. This is useful for building
// time-series visualizations of log volume.
//
// Query transformation:
//   - Adds: | stats by (_time:<step> offset <offset>, <fields...>) count() hits
//   - Adds: | sort by (_time, <fields...>)
//   - Drops unsafe trailing pipes that could alter _time
//
// Response format:
//
//	{"series":[{"fields":"{...}","timestamps":[...],"hits":[...],"hitsTotal":N},...]}
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-hits-stats
func ProcessHitsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Obtain step - the time bucket size
	step, err := parseDuration(r, "step", "")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	if step <= 0 {
		httpserver.Errorf(w, r, "'step' must be bigger than zero")
		return
	}

	// Obtain offset for time alignment (e.g., for timezone adjustment)
	offset, err := parseDuration(r, "offset", "0s")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Obtain field entries for additional grouping beyond time
	fields := r.Form["field"]

	// Obtain limit on the number of top field entries to return
	fieldsLimit, err := getPositiveInt(r, "fields_limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Add the count-by-time pipe to the query
	ca.q.AddCountByTimePipe(step, offset, fields)

	// Collect hits results grouped by field values
	var mLock sync.Mutex
	m := make(map[string]*hitsSeries)
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		if rowsCount == 0 {
			return
		}

		columns := db.Columns
		timestampValues := columns[0].Values         // _time column
		hitsValues := columns[len(columns)-1].Values // hits column (last)
		columns = columns[1 : len(columns)-1]        // grouping fields

		bb := blockResultPool.Get()
		for i := 0; i < rowsCount; i++ {
			timestampNsec, ok := logstorage.TryParseTimestampRFC3339Nano(timestampValues[i])
			if !ok {
				logger.Panicf("BUG: cannot parse timestamp=%q", timestampValues[i])
			}
			hitsStr := strings.Clone(hitsValues[i])
			hits, err := strconv.ParseUint(hitsStr, 10, 64)
			if err != nil {
				logger.Panicf("BUG: cannot parse hitsStr=%q: %s", hitsStr, err)
			}

			// Build a key from the field values for grouping
			bb.Reset()
			WriteFieldsForHits(bb, columns, i)

			mLock.Lock()
			hs, ok := m[string(bb.B)]
			if !ok {
				hs = &hitsSeries{}
				m[string(bb.B)] = hs
			}
			hs.timestamps = append(hs.timestamps, timestampNsec)
			hs.hits = append(hs.hits, hits)
			hs.hitsTotal += hits
			mLock.Unlock()
		}
		blockResultPool.Put(bb)
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the query
	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		httpserver.Errorf(w, r, "cannot execute query [%s]: %s", ca.q, err)
		return
	}

	// Post-process: limit to top series and fill in missing time buckets with zeros
	m = getTopHitsSeries(m, fieldsLimit)
	addMissingZeroHits(m, ca.startAligned, ca.endAligned, step, offset)

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write response
	WriteHitsSeries(w, m)
}

// addMissingZeroHits fills in missing time buckets with zero hit counts.
//
// This ensures that time series charts have continuous data points even when
// some time buckets had no matching logs. The function:
//  1. Determines the actual start/end if not provided (from existing data)
//  2. Aligns the range to the step boundary
//  3. Adds (timestamp, 0) entries for missing buckets
func addMissingZeroHits(m map[string]*hitsSeries, start, end, step, offset int64) {
	// If start/end not provided, derive from actual data
	if start == math.MinInt64 {
		start = math.MaxInt64
		for _, hs := range m {
			start = min(start, slices.Min(hs.timestamps))
		}
	}

	if end == math.MaxInt64 {
		end = math.MinInt64
		for _, hs := range m {
			end = max(end, slices.Max(hs.timestamps))
		}
	}

	start, end = alignStartEndToStep(start, end, step, offset)

	if start > end {
		return
	}

	// For each series, add zero entries for missing timestamps
	for _, hs := range m {
		ts := start
		for ts <= end {
			if !slices.Contains(hs.timestamps, ts) {
				hs.timestamps = append(hs.timestamps, ts)
				hs.hits = append(hs.hits, 0)
			}

			if ts+step < ts {
				// stop on int64 overflow
				break
			}
			ts += step
		}
	}
}

// blockResultPool is a pool for reusing byte buffers during result processing
var blockResultPool bytesutil.ByteBufferPool

// getTopHitsSeries returns the top N series by total hits, merging the rest into "{}"
func getTopHitsSeries(m map[string]*hitsSeries, fieldsLimit int) map[string]*hitsSeries {
	if fieldsLimit <= 0 || fieldsLimit >= len(m) {
		return m
	}

	type fieldsHits struct {
		fieldsStr string
		hs        *hitsSeries
	}
	a := make([]fieldsHits, 0, len(m))
	for fieldsStr, hs := range m {
		a = append(a, fieldsHits{
			fieldsStr: fieldsStr,
			hs:        hs,
		})
	}
	// Sort by total hits descending
	sort.Slice(a, func(i, j int) bool {
		return a[i].hs.hitsTotal > a[j].hs.hitsTotal
	})

	// Merge all remaining series into a single "{}" series
	hitsOther := make(map[int64]uint64)
	for _, x := range a[fieldsLimit:] {
		for i, timestamp := range x.hs.timestamps {
			hitsOther[timestamp] += x.hs.hits[i]
		}
	}
	var hsOther hitsSeries
	for timestamp, hits := range hitsOther {
		hsOther.timestamps = append(hsOther.timestamps, timestamp)
		hsOther.hits = append(hsOther.hits, hits)
		hsOther.hitsTotal += hits
	}

	mNew := make(map[string]*hitsSeries, fieldsLimit+1)
	for _, x := range a[:fieldsLimit] {
		mNew[x.fieldsStr] = x.hs
	}
	mNew["{}"] = &hsOther

	return mNew
}

// hitsSeries represents a single time series of hit counts
type hitsSeries struct {
	hitsTotal  uint64   // Total hits across all timestamps (for sorting)
	timestamps []int64  // Timestamps for each data point
	hits       []uint64 // Hit counts at each timestamp
}

func (hs *hitsSeries) sort() {
	sort.Sort(hs)
}

func (hs *hitsSeries) Len() int {
	return len(hs.timestamps)
}

func (hs *hitsSeries) Swap(i, j int) {
	hs.timestamps[i], hs.timestamps[j] = hs.timestamps[j], hs.timestamps[i]
	hs.hits[i], hs.hits[j] = hs.hits[j], hs.hits[i]
}

func (hs *hitsSeries) Less(i, j int) bool {
	return hs.timestamps[i] < hs.timestamps[j]
}

// ProcessFieldNamesRequest handles /select/logsql/field_names request.
//
// Returns a list of field names that appear in logs matching the query,
// along with the number of hits for each field. Useful for:
//   - Discovering available fields in your logs
//   - Building autocomplete for query editors
//
// Response format: {"values":[{"value":"field_name","hits":N},...]}
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-field-names
func ProcessFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Obtain field names for the given query
	startTime := time.Now()
	fieldNames, err := vlstorage.GetFieldNames(qctx)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain field names: %s", err)
		return
	}

	// Write response headers
	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	// Write results
	WriteValuesWithHitsJSON(w, fieldNames)
}

// ProcessFieldValuesRequest handles /select/logsql/field_values request.
//
// Returns unique values for a specific field from logs matching the query,
// along with hit counts. Useful for:
//   - Building filter dropdowns
//   - Exploring value distributions
//
// Response format: {"values":[{"value":"field_value","hits":N},...]}
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-field-values
func ProcessFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	fieldName := r.FormValue("field")
	if fieldName == "" {
		httpserver.Errorf(w, r, "missing 'field' query arg")
		return
	}

	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	values, err := vlstorage.GetFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain values for field %q: %s", fieldName, err)
		return
	}

	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	WriteValuesWithHitsJSON(w, values)
}

// ProcessStreamFieldNamesRequest processes /select/logsql/stream_field_names request.
//
// Returns the field names that define log streams (e.g., host, app, level).
// These are the fields specified via _stream_fields at ingestion time.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-names
func ProcessStreamFieldNamesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	names, err := vlstorage.GetStreamFieldNames(qctx)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain stream field names: %s", err)
		return
	}

	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	WriteValuesWithHitsJSON(w, names)
}

// ProcessStreamFieldValuesRequest processes /select/logsql/stream_field_values request.
//
// Returns unique values for a specific stream field. Useful for discovering
// all hosts, applications, or other stream dimensions in your logs.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream-field-values
func ProcessStreamFieldValuesRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	fieldName := r.FormValue("field")
	if fieldName == "" {
		httpserver.Errorf(w, r, "missing 'field' query arg")
		return
	}

	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	values, err := vlstorage.GetStreamFieldValues(qctx, fieldName, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain stream field values: %s", err)
		return
	}

	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	WriteValuesWithHitsJSON(w, values)
}

// ProcessStreamIDsRequest processes /select/logsql/stream_ids request.
//
// Returns internal stream IDs (128-bit identifiers) for streams matching
// the query. Stream IDs are useful for low-level debugging.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-stream_ids
func ProcessStreamIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	streamIDs, err := vlstorage.GetStreamIDs(qctx, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain stream_ids: %s", err)
		return
	}

	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	WriteValuesWithHitsJSON(w, streamIDs)
}

// ProcessStreamsRequest processes /select/logsql/streams request.
//
// Returns log streams (unique combinations of stream field values) that
// contain logs matching the query. A stream is like {host="app1",level="error"}.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-streams
func ProcessStreamsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	streams, err := vlstorage.GetStreams(qctx, uint64(limit))
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain streams: %s", err)
		return
	}

	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	WriteValuesWithHitsJSON(w, streams)
}

// ProcessLiveTailRequest processes live tailing request to /select/logsql/tail
//
// Live tailing provides real-time log streaming by periodically polling for new
// log entries and streaming them to the client as NDJSON.
//
// Key design decisions:
//   - No concurrency limit: Tail requests are long-lived and mostly idle
//   - No timeout: Runs until client disconnects
//   - Per-stream deduplication: Tracks last-seen timestamp per stream to avoid duplicates
//   - Overlapping poll windows: Each poll looks back 5 seconds to catch late-arriving logs
//
// Poll algorithm:
//  1. Query logs in [end-startOffset, end-offset] time range
//  2. Deduplicate against last-seen timestamps per stream
//  3. Stream new logs to client
//  4. Wait refresh_interval (default 1s)
//  5. Update time range and repeat
//
// See https://docs.victoriametrics.com/victorialogs/querying/#live-tailing
func ProcessLiveTailRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	liveTailRequests.Inc()
	defer liveTailRequests.Dec()

	// Skip max time range check - tail queries don't have a fixed range
	ca, err := parseCommonArgsWithConfig(r, true)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	if !ca.q.CanLiveTail() {
		httpserver.Errorf(w, r, "the query [%s] cannot be used in live tailing; "+
			"see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing for details", ca.q)
		return
	}

	refreshInterval, err := parseDuration(r, "refresh_interval", "1s")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// How far back to look in the first poll
	startOffset, err := parseDuration(r, "start_offset", "5s")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// How far behind "now" to query (accounts for ingestion delay)
	offset, err := parseDuration(r, "offset", "5s")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	tp := newTailProcessor(cancel)

	ticker := time.NewTicker(time.Duration(refreshInterval))
	defer ticker.Stop()

	// Initial time window
	end := time.Now().UnixNano() - offset
	start := end - startOffset
	doneCh := ctxWithCancel.Done()
	flusher, ok := w.(http.Flusher)
	if !ok {
		logger.Panicf("BUG: it is expected that http.ResponseWriter (%T) supports http.Flusher interface", w)
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	flusher.Flush()

	qctx := ca.newQueryContext(ctxWithCancel)
	defer ca.updatePerQueryStatsMetrics()

	q := ca.q
	qOrig := q
	for {
		// Clone query with updated time filter for this poll window
		q = qOrig.CloneWithTimeFilter(end, start, end)
		qctxLocal := qctx.WithQuery(q)
		if err := vlstorage.RunQuery(qctxLocal, tp.writeBlock); err != nil {
			httpserver.Errorf(w, r, "cannot execute tail query [%s]: %s", q, err)
			return
		}
		// Get deduplicated results
		resultRows, err := tp.getTailRows()
		if err != nil {
			httpserver.Errorf(w, r, "cannot get tail results for query [%q]: %s", q, err)
			return
		}
		if len(resultRows) > 0 {
			WriteJSONRows(w, resultRows)
			flusher.Flush()
		}

		// Wait for next poll interval or client disconnect
		select {
		case <-doneCh:
			return
		case <-ticker.C:
			// Shift time window, with overlap for deduplication
			start = end - tailOffsetNsecs
			end = time.Now().UnixNano() - offset
		}
	}
}

var liveTailRequests = metrics.NewCounter(`vl_live_tailing_requests`)

// tailOffsetNsecs is the overlap window for deduplication (5 seconds)
const tailOffsetNsecs = 5e9

// logRow represents a single log entry with timestamp and fields
type logRow struct {
	timestamp int64
	fields    []logstorage.Field
}

// sortLogRows sorts log rows by timestamp ascending
func sortLogRows(rows []logRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].timestamp < rows[j].timestamp
	})
}

// tailProcessor handles deduplication and buffering for live tailing
type tailProcessor struct {
	cancel func() // Cancel function for the request context

	mu sync.Mutex

	// Per-stream rows collected in current poll window
	perStreamRows map[string][]logRow
	// Last timestamp seen per stream (for deduplication)
	lastTimestamps map[string]int64

	err error // Any error encountered during processing
}

func newTailProcessor(cancel func()) *tailProcessor {
	return &tailProcessor{
		cancel: cancel,

		perStreamRows:  make(map[string][]logRow),
		lastTimestamps: make(map[string]int64),
	}
}

// writeBlock receives DataBlocks from the query and stores rows per-stream
func (tp *tailProcessor) writeBlock(_ uint, db *logstorage.DataBlock) {
	if db.RowsCount() == 0 {
		return
	}

	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.err != nil {
		return
	}

	// Must have _time field for proper tail operation
	timestamps, ok := db.GetTimestamps(nil)
	if !ok {
		tp.err = fmt.Errorf("missing _time field")
		tp.cancel()
		return
	}

	// Copy block rows to tp.perStreamRows, keyed by stream ID
	for i, timestamp := range timestamps {
		streamID := ""
		fields := make([]logstorage.Field, len(db.Columns))
		for j, c := range db.Columns {
			name := strings.Clone(c.Name)
			value := strings.Clone(c.Values[i])

			fields[j] = logstorage.Field{
				Name:  name,
				Value: value,
			}

			if name == "_stream_id" {
				streamID = value
			}
		}

		tp.perStreamRows[streamID] = append(tp.perStreamRows[streamID], logRow{
			timestamp: timestamp,
			fields:    fields,
		})
	}
}

// getTailRows returns deduplicated rows from the current poll window
//
// Deduplication: For each stream, skip rows with timestamps <= the last
// timestamp seen in previous polls. This prevents sending duplicates when
// poll windows overlap.
func (tp *tailProcessor) getTailRows() ([][]logstorage.Field, error) {
	if tp.err != nil {
		return nil, tp.err
	}

	var resultRows []logRow
	for streamID, rows := range tp.perStreamRows {
		sortLogRows(rows)

		lastTimestamp, ok := tp.lastTimestamps[streamID]
		if ok {
			// Skip rows already sent in previous polls
			for len(rows) > 0 && rows[0].timestamp <= lastTimestamp {
				rows = rows[1:]
			}
		}
		if len(rows) > 0 {
			resultRows = append(resultRows, rows...)
			// Update last seen timestamp for this stream
			tp.lastTimestamps[streamID] = rows[len(rows)-1].timestamp
		}
	}
	clear(tp.perStreamRows)

	sortLogRows(resultRows)

	tailRows := make([][]logstorage.Field, len(resultRows))
	for i, row := range resultRows {
		tailRows[i] = row.fields
	}

	return tailRows, nil
}

// ProcessStatsQueryRangeRequest handles /select/logsql/stats_query_range request.
//
// Returns Prometheus-style range vector results, where the query's stats pipe
// grouping is augmented with _time:<step> to produce time series.
//
// Unlike /hits which counts log entries, this endpoint returns the actual
// stats function results (e.g., avg, sum, quantiles) over time.
//
// Query transformation:
//   - Adds _time:<step> offset <offset> to the final stats pipe's grouping
//   - Does NOT add a separate group_by_time pipe
//
// Response format: Prometheus-compatible JSON with "data" containing "result" array
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-range-stats
func ProcessStatsQueryRangeRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	step, err := parseDuration(r, "step", "")
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}
	if step <= 0 {
		err := fmt.Errorf("'step' must be bigger than zero")
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	offset, err := parseDuration(r, "offset", "0s")
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Get the labels that need _time grouping added
	labelFields, err := ca.q.GetStatsLabelsAddGroupingByTime(step, offset)
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Map from series key (name + labels JSON) to stats series
	m := make(map[string]*statsSeries)
	var mLock sync.Mutex

	addPoint := func(name string, labels []logstorage.Field, p statsPoint) {
		dst := append([]byte{}, name...)
		dst = logstorage.MarshalFieldsToJSON(dst, labels)
		key := string(dst)

		mLock.Lock()
		ss := m[key]
		if ss == nil {
			ss = &statsSeries{
				key:    key,
				Name:   name,
				Labels: labels,
			}
			m[key] = ss
		}
		ss.Points = append(ss.Points, p)
		mLock.Unlock()
	}

	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()

		columns := db.Columns
		clonedColumnNames := make([]string, len(columns))
		for i, c := range columns {
			clonedColumnNames[i] = strings.Clone(c.Name)
		}
		for i := 0; i < rowsCount; i++ {
			// Initialize timestamp from query timestamp; may be overridden by _time column
			ts := ca.q.GetTimestamp()
			labels := make([]logstorage.Field, 0, len(labelFields))
			for j, c := range columns {
				if c.Name == "_time" {
					nsec, ok := logstorage.TryParseTimestampRFC3339Nano(c.Values[i])
					if ok {
						ts = nsec
						continue
					}
				}
				if slices.Contains(labelFields, c.Name) {
					labels = append(labels, logstorage.Field{
						Name:  clonedColumnNames[j],
						Value: strings.Clone(c.Values[i]),
					})
				}
			}

			// Process each non-label column as a separate metric
			for j, c := range columns {
				if slices.Contains(labelFields, c.Name) {
					continue
				}

				v := strings.Clone(c.Values[i])
				if v == "[]" || strings.HasPrefix(v, `[{"vmrange":"`) {
					// Special case: histogram() stats function result
					// Expand into individual bucket series
					var buckets []histogramBucket
					if err := json.Unmarshal([]byte(v), &buckets); err == nil {
						name := clonedColumnNames[j] + "_bucket"
						for _, bucket := range buckets {
							bucketLabels := make([]logstorage.Field, 0, len(labels)+1)
							bucketLabels = append(bucketLabels, labels...)
							bucketLabels = append(bucketLabels, logstorage.Field{
								Name:  "vmrange",
								Value: bucket.VMRange,
							})
							p := statsPoint{
								Timestamp: ts,
								Value:     strconv.FormatUint(bucket.Hits, 10),
							}
							addPoint(name, bucketLabels, p)
						}

						continue
					}
				}

				p := statsPoint{
					Timestamp: ts,
					Value:     v,
				}
				addPoint(clonedColumnNames[j], labels, p)
			}
		}
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		err = fmt.Errorf("cannot execute query [%s]: %s", ca.q, err)
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Sort the collected stats by _time
	rows := make([]*statsSeries, 0, len(m))
	for _, ss := range m {
		points := ss.Points
		sort.Slice(points, func(i, j int) bool {
			return points[i].Timestamp < points[j].Timestamp
		})
		rows = append(rows, ss)
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].key < rows[j].key
	})

	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	WriteStatsQueryRangeResponse(w, rows)
}

// statsSeries represents a single time series for stats results
type statsSeries struct {
	key string // Unique key (name + labels JSON)

	Name   string             // Metric name
	Labels []logstorage.Field // Label set
	Points []statsPoint       // Data points over time
}

// statsPoint represents a single data point in a stats series
type statsPoint struct {
	Timestamp int64
	Value     string
}

// ProcessStatsQueryRequest handles /select/logsql/stats_query request.
//
// Returns Prometheus-style instant vector results - stats function values
// evaluated at a single point in time (the 'time' parameter or 'end' or now).
//
// Unlike stats_query_range, this doesn't group by time - it returns a single
// value per metric/label combination.
//
// The query must end with a stats pipe (e.g., | stats count() as total).
//
// Response format: Prometheus-compatible JSON with "data" containing "result" array
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-log-stats
func ProcessStatsQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	// Get the labels that are grouping keys in the stats pipe
	labelFields, err := ca.q.GetStatsLabels()
	if err != nil {
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	var rows []statsRow
	var rowsLock sync.Mutex

	timestamp := ca.q.GetTimestamp()
	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsCount := db.RowsCount()
		columns := db.Columns
		clonedColumnNames := make([]string, len(columns))
		for i, c := range columns {
			clonedColumnNames[i] = strings.Clone(c.Name)
		}
		for i := 0; i < rowsCount; i++ {
			labels := make([]logstorage.Field, 0, len(labelFields))
			for j, c := range columns {
				if slices.Contains(labelFields, c.Name) {
					labels = append(labels, logstorage.Field{
						Name:  clonedColumnNames[j],
						Value: strings.Clone(c.Values[i]),
					})
				}
			}

			// Process each non-label column as a separate metric
			for j, c := range columns {
				if slices.Contains(labelFields, c.Name) {
					continue
				}

				v := strings.Clone(c.Values[i])
				if v == "[]" || strings.HasPrefix(v, `[{"vmrange":"`) {
					// Special case: histogram() stats function result
					// Expand into individual bucket series
					var buckets []histogramBucket
					if err := json.Unmarshal([]byte(v), &buckets); err == nil {
						name := clonedColumnNames[j] + "_bucket"
						bucketRows := make([]statsRow, 0, len(buckets))
						for _, bucket := range buckets {
							bucketLabels := make([]logstorage.Field, 0, len(labels)+1)
							bucketLabels = append(bucketLabels, labels...)
							bucketLabels = append(bucketLabels, logstorage.Field{
								Name:  "vmrange",
								Value: bucket.VMRange,
							})
							bucketRows = append(bucketRows, statsRow{
								Name:      name,
								Labels:    bucketLabels,
								Timestamp: timestamp,
								Value:     strconv.FormatUint(bucket.Hits, 10),
							})
						}
						rowsLock.Lock()
						rows = append(rows, bucketRows...)
						rowsLock.Unlock()

						continue
					}
				}

				r := statsRow{
					Name:      clonedColumnNames[j],
					Labels:    labels,
					Timestamp: timestamp,
					Value:     v,
				}

				rowsLock.Lock()
				rows = append(rows, r)
				rowsLock.Unlock()
			}
		}
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	startTime := time.Now()
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		err = fmt.Errorf("cannot execute query [%s]: %s", ca.q, err)
		httpserver.SendPrometheusError(w, r, err)
		return
	}

	h := w.Header()

	h.Set("Content-Type", "application/json")
	ca.writeResponseHeaders(h, startTime)

	WriteStatsQueryResponse(w, rows)
}

// statsRow represents a single stats result row (metric + labels + value)
type statsRow struct {
	Name      string
	Labels    []logstorage.Field
	Timestamp int64
	Value     string
}

// histogramBucket represents a single bucket in histogram() stats results
type histogramBucket struct {
	VMRange string `json:"vmrange"`
	Hits    uint64 `json:"hits"`
}

// ProcessQueryRequest handles /select/logsql/query request.
//
// This is the main query endpoint for fetching log entries. Results are
// streamed as NDJSON (one JSON object per line) for low latency.
//
// Query transformation (if limit > 0):
//   - Adds: | sort by (_time) desc (if query can return last N results)
//   - Adds: | offset <offset> | limit <limit>
//
// The sort-by-time-desc is automatically optimized to use binary search
// over the time range instead of full scan (see lastnoptimization.go).
//
// Response format: application/stream+json (NDJSON)
// Each line is a JSON object with field name/value pairs.
//
// See https://docs.victoriametrics.com/victorialogs/querying/#querying-logs
func ProcessQueryRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ca, err := parseCommonArgs(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	offset, err := getPositiveInt(r, "offset")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	limit, err := getPositiveInt(r, "limit")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Thread-safe writer for parallel workers
	sw := &syncWriter{
		w: w,
	}

	// Per-worker buffers for batching writes
	var bwShards atomicutil.Slice[bufferedWriter]
	bwShards.Init = func(shard *bufferedWriter) {
		shard.sw = sw
	}
	defer func() {
		// Flush all buffers on exit
		shards := bwShards.All()
		for _, shard := range shards {
			shard.FlushIgnoreErrors()
		}
	}()

	if limit > 0 {
		// Add pagination pipes to the query
		// The sort-by-time-desc is optimized to use binary search
		if ca.q.CanReturnLastNResults() {
			ca.q.AddPipeSortByTimeDesc()
		}
		ca.q.AddPipeOffsetLimit(uint64(offset), uint64(limit))
	}

	startTime := time.Now()
	// Write headers on first result (lazy initialization)
	writeResponseHeadersOnce := sync.OnceFunc(func() {
		h := w.Header()

		h.Set("Content-Type", "application/stream+json")
		ca.writeResponseHeaders(h, startTime)
	})

	writeBlock := func(workerID uint, db *logstorage.DataBlock) {
		writeResponseHeadersOnce()
		rowsCount := db.RowsCount()
		if rowsCount == 0 {
			return
		}
		columns := db.Columns

		// Use per-worker buffer for batching
		bw := bwShards.Get(workerID)
		for i := 0; i < rowsCount; i++ {
			WriteJSONRow(bw, columns, i)
			// Flush when buffer exceeds 16KB
			if len(bw.buf) > 16*1024 {
				bw.FlushIgnoreErrors()
			}
		}
	}

	qctx := ca.newQueryContext(ctx)
	defer ca.updatePerQueryStatsMetrics()

	// Execute the query
	if err := vlstorage.RunQuery(qctx, writeBlock); err != nil {
		httpserver.Errorf(w, r, "cannot execute query [%s]: %s", ca.q, err)
		return
	}

	// Ensure headers are written even for empty results
	writeResponseHeadersOnce()
}

// ProcessTenantIDsRequest processes /select/tenant_ids request.
//
// Returns all tenant IDs that have data within the specified time range.
// This is used for multi-tenant cluster management and administrative queries.
//
// Security note: This endpoint is forbidden when AccountID header is non-empty.
// This allows vmauth to enforce tenant isolation by preventing users from
// discovering other tenants' IDs.
//
// Response format: [{"account_id":N,"project_id":M},...]
func ProcessTenantIDsRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	accountID := r.Header.Get("AccountID")
	if accountID != "" {
		// Security measure - prevent requesting tenant_ids when tenant is already specified
		// This allows vmauth to enforce isolation at the proxy level
		err := &httpserver.ErrorWithStatusCode{
			Err:        fmt.Errorf("the /select/tenant_ids endpoint cannot be requested with non-empty AccountID=%q header", accountID),
			StatusCode: http.StatusForbidden,
		}
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	start, okStart, err := getTimeNsec(r, "start")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	end, okEnd, err := getTimeNsec(r, "end")
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}
	if !okStart {
		start = math.MinInt64
	}
	if !okEnd {
		end = math.MaxInt64
	} else {
		// Treat HTTP 'end' query arg as exclusive: [start, end)
		// Convert to inclusive bound for internal filter by subtracting 1ns.
		if end != math.MinInt64 {
			end--
		}
	}

	if start > end {
		httpserver.Errorf(w, r, "'start=%d' must be smaller than 'end=%d'", start, end)
		return
	}

	tenants, err := vlstorage.GetTenantIDs(ctx, start, end)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain tenantIDs: %s", err)
		return
	}

	data, err := json.Marshal(tenants)
	if err != nil {
		httpserver.Errorf(w, r, "cannot marshal tenantIDs to JSON: %s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if _, err := w.Write(data); err != nil {
		httpserver.Errorf(w, r, "cannot send response to the client: %s", err)
		return
	}
}

// syncWriter is a thread-safe wrapper around io.Writer
// Used for parallel workers writing to the same HTTP response
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (sw *syncWriter) Write(p []byte) (int, error) {
	sw.mu.Lock()
	n, err := sw.w.Write(p)
	sw.mu.Unlock()
	return n, err
}

// bufferedWriter batches writes before flushing to the underlying syncWriter
// This reduces syscalls when writing many small JSON lines
type bufferedWriter struct {
	buf []byte
	sw  *syncWriter
}

func (bw *bufferedWriter) Write(p []byte) (int, error) {
	bw.buf = append(bw.buf, p...)
	// Don't flush here - wait for complete lines
	return len(p), nil
}

func (bw *bufferedWriter) FlushIgnoreErrors() {
	_, _ = bw.sw.Write(bw.buf)
	bw.buf = bw.buf[:0]
}

// commonArgs holds the parsed parameters shared across most /select/logsql/* endpoints.
//
// This struct is created by parseCommonArgs() and contains:
//   - The parsed LogsQL query with time filters and extra filters applied
//   - Tenant IDs extracted from HTTP headers
//   - Options for partial responses and hidden fields
//   - Query execution statistics
//   - Time range aligned to step (for hits/stats_query_range)
type commonArgs struct {
	// The parsed query including optional extra_filters, extra_stream_filters,
	// and (start, end) time range filter.
	q *logstorage.Query

	// tenantIDs is the list of tenant IDs to query.
	// Extracted from AccountID/ProjectID HTTP headers.
	tenantIDs []logstorage.TenantID

	// Whether to allow partial response when some vlstorage nodes
	// are unavailable. Only applies in cluster mode.
	allowPartialResponse bool

	// Optional fields and field prefixes to hide during query execution.
	// Supports both exact field names and prefixes ending with *.
	hiddenFieldsFilters []string

	// qs accumulates query execution statistics (rows scanned, blocks read, etc.)
	qs logstorage.QueryStats

	// startAligned and endAligned are the time range boundaries aligned to step.
	// Used by hits and stats_query_range for consistent time bucketing.
	startAligned int64
	endAligned   int64
}

// newQueryContext creates a QueryContext from commonArgs for query execution.
func (ca *commonArgs) newQueryContext(ctx context.Context) *logstorage.QueryContext {
	return logstorage.NewQueryContext(ctx, &ca.qs, ca.tenantIDs, ca.q, ca.allowPartialResponse, ca.hiddenFieldsFilters)
}

// updatePerQueryStatsMetrics updates global metrics from query statistics
func (ca *commonArgs) updatePerQueryStatsMetrics() {
	vlstorage.UpdatePerQueryStatsMetrics(&ca.qs)
}

// parseCommonArgs parses the shared request parameters for /select/logsql/* endpoints.
//
// This is the main argument parsing function that:
//  1. Extracts tenant ID from AccountID/ProjectID headers
//  2. Parses optional start/end/time parameters for time range
//  3. Parses the LogsQL query string
//  4. Applies time filters and extra filters to the query
//  5. Validates the time range against -search.maxQueryTimeRange
//  6. Parses allow_partial_response and hidden_fields_filters options
func parseCommonArgs(r *http.Request) (*commonArgs, error) {
	return parseCommonArgsWithConfig(r, false)
}

// parseCommonArgsWithConfig is like parseCommonArgs but can skip the max time range check.
// This is used by live tailing which doesn't have a fixed time range.
func parseCommonArgsWithConfig(r *http.Request, skipMaxRangeCheck bool) (*commonArgs, error) {
	// Extract tenantID from HTTP headers
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		return nil, fmt.Errorf("cannot obtain tenantID: %w", err)
	}
	tenantIDs := []logstorage.TenantID{tenantID}

	// Parse optional start and end args for time range filtering
	start, startOK, err := getTimeNsec(r, "start")
	if err != nil {
		return nil, err
	}
	end, endOK, err := getTimeNsec(r, "end")
	if err != nil {
		return nil, err
	}
	if endOK {
		// HTTP 'end' is exclusive [start, end), convert to inclusive for internal filter
		if end != math.MinInt64 {
			end--
		}
	}

	// Parse optional time arg - the evaluation timestamp for relative time expressions
	timestamp, timeOK, err := getTimeNsec(r, "time")
	if err != nil {
		return nil, err
	}
	// Decrease timestamp by 1ns to avoid boundary issues with time-based functions
	// (e.g., "now()/1h" shouldn't spill into next hour)
	timestamp--

	currTimestamp := time.Now().UnixNano()
	if !timeOK {
		// If time not specified, use end timestamp or current time
		if endOK {
			timestamp = end
		} else {
			timestamp = currTimestamp
		}
	}

	// Parse the LogsQL query string
	qStr := r.FormValue("query")
	q, err := logstorage.ParseQueryAtTimestamp(qStr, timestamp)
	if err != nil {
		return nil, fmt.Errorf("cannot parse query [%s]: %s", qStr, err)
	}

	if startOK || endOK {
		// Add _time:[start, end] filter if start or end args were set
		if !startOK {
			start = math.MinInt64
		}
		if !endOK {
			end = math.MaxInt64
		}

		// Align to step if step parameter is provided
		if stepStr := r.FormValue("step"); stepStr != "" {
			if step, ok := logstorage.TryParseDuration(stepStr); ok {
				offset := int64(0)
				if offsetStr := r.FormValue("offset"); offsetStr != "" {
					nsecs, ok := logstorage.TryParseDuration(offsetStr)
					if ok {
						offset = nsecs
					}
				}
				start, end = alignStartEndToStep(start, end, step, offset)
			}
		}

		q.AddTimeFilter(start, end)
	}

	// Initialize aligned time range values
	startAligned := int64(math.MinInt64)
	if startOK {
		startAligned = start
	}
	endAligned := int64(math.MaxInt64)
	if endOK {
		endAligned = end
	}

	// Parse optional extra_filters - additional filters to AND with the query
	for _, extraFiltersStr := range r.Form["extra_filters"] {
		extraFilters, err := parseExtraFilters(extraFiltersStr)
		if err != nil {
			return nil, err
		}
		q.AddExtraFilters(extraFilters)
	}

	// Parse optional extra_stream_filters - additional stream-level filters
	for _, extraStreamFiltersStr := range r.Form["extra_stream_filters"] {
		extraStreamFilters, err := parseExtraStreamFilters(extraStreamFiltersStr)
		if err != nil {
			return nil, err
		}
		q.AddExtraFilters(extraStreamFilters)
	}

	// Validate time range against configured maximum
	if maxRange := maxQueryTimeRange.Duration(); maxRange > 0 && !skipMaxRangeCheck {
		start, end := q.GetFilterTimeRange()
		if end > start {
			queryTimeRange := end - start
			if queryTimeRange < 0 || queryTimeRange > maxRange.Nanoseconds() {
				return nil, fmt.Errorf("too big time range selected: [%s, %s]; it cannot exceed -search.maxQueryTimeRange=%s; "+
					"see https://docs.victoriametrics.com/victorialogs/querying/#resource-usage-limits",
					timestampToString(start), timestampToString(end), maxRange)
			}
		}
	}

	allowPartialResponse := *allowPartialResponseFlag
	if err := getBoolFromRequest(&allowPartialResponse, r, "allow_partial_response"); err != nil {
		return nil, err
	}

	hiddenFieldsFilters, err := getStringSliceFromRequest(r, "hidden_fields_filters")
	if err != nil {
		return nil, err
	}

	ca := &commonArgs{
		q:         q,
		tenantIDs: tenantIDs,

		allowPartialResponse: allowPartialResponse,
		hiddenFieldsFilters:  hiddenFieldsFilters,

		startAligned: startAligned,
		endAligned:   endAligned,
	}
	return ca, nil
}

// alignStartEndToStep aligns the start and end timestamps to step boundaries.
//
// This ensures that time buckets in hits/stats_query_range align properly:
//   - start is rounded down to the nearest step boundary
//   - end is rounded up to the nearest step boundary
//
// The offset parameter shifts the alignment (e.g., for timezone adjustment).
// The final end is decremented by 1ns to make it an inclusive bound.
func alignStartEndToStep(start, end, step, offset int64) (int64, int64) {
	if step <= 0 {
		return start, end
	}

	// Align start: shift by offset, round down to step, shift back
	start = logstorage.SubInt64NoOverflow(start, -offset)
	if start >= 0 {
		start -= start % step
	} else {
		d := step + start%step
		start = logstorage.SubInt64NoOverflow(start, d)
	}
	start = logstorage.SubInt64NoOverflow(start, offset)

	// Align end: shift by offset, round up to step, shift back
	end = logstorage.SubInt64NoOverflow(end, -offset)
	if end <= 0 {
		end -= end % step
	} else {
		d := step - end%step
		end = logstorage.SubInt64NoOverflow(end, -d)
	}
	end = logstorage.SubInt64NoOverflow(end, offset)

	// Convert to inclusive bound
	if end > math.MinInt64 {
		end--
	}

	return start, end
}

func timestampToString(nsecs int64) string {
	t := time.Unix(nsecs/1e9, nsecs%1e9).UTC()
	return t.Format(time.RFC3339Nano)
}

// getTimeNsec parses a time parameter from the request.
// Returns (timestamp, true, nil) if the parameter is present and valid.
// Returns (0, false, nil) if the parameter is absent.
// Returns (0, false, error) if the parameter is invalid.
func getTimeNsec(r *http.Request, argName string) (int64, bool, error) {
	s := r.FormValue(argName)
	if s == "" {
		return 0, false, nil
	}
	currentTimestamp := time.Now().UnixNano()
	nsecs, err := timeutil.ParseTimeAt(s, currentTimestamp)
	if err != nil {
		return 0, false, fmt.Errorf("cannot parse %s=%s: %w", argName, s, err)
	}
	return nsecs, true, nil
}

// parseExtraFilters parses extra_filters parameter which can be:
//   - LogsQL filter syntax: "level:error host:app1"
//   - JSON object: {"level":"error","host":"app1"}
func parseExtraFilters(s string) (*logstorage.Filter, error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, `{"`) {
		return logstorage.ParseFilter(s)
	}

	// JSON format: {"field":"value",...} or {"field":["value1","value2"],...}
	kvs, err := parseExtraFiltersJSON(s)
	if err != nil {
		return nil, err
	}

	filters := make([]string, len(kvs))
	for i, kv := range kvs {
		if len(kv.values) == 1 {
			// Single value: "field":="value"
			filters[i] = fmt.Sprintf("%q:=%q", kv.key, kv.values[0])
		} else {
			// Multiple values: "field":in("value1","value2")
			orValues := make([]string, len(kv.values))
			for j, v := range kv.values {
				orValues[j] = fmt.Sprintf("%q", v)
			}
			filters[i] = fmt.Sprintf("%q:in(%s)", kv.key, strings.Join(orValues, ","))
		}
	}
	s = strings.Join(filters, " ")
	return logstorage.ParseFilter(s)
}

// parseExtraStreamFilters parses extra_stream_filters for stream-level filtering.
// Similar to parseExtraFilters but generates stream filter syntax (= instead of :=)
func parseExtraStreamFilters(s string) (*logstorage.Filter, error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, `{"`) {
		return logstorage.ParseFilter(s)
	}

	kvs, err := parseExtraFiltersJSON(s)
	if err != nil {
		return nil, err
	}

	filters := make([]string, len(kvs))
	for i, kv := range kvs {
		if len(kv.values) == 1 {
			// Single value: "field"="value"
			filters[i] = fmt.Sprintf("%q=%q", kv.key, kv.values[0])
		} else {
			// Multiple values: "field"=~"value1|value2"
			orValues := make([]string, len(kv.values))
			for j, v := range kv.values {
				orValues[j] = regexp.QuoteMeta(v)
			}
			filters[i] = fmt.Sprintf("%q=~%q", kv.key, strings.Join(orValues, "|"))
		}
	}
	s = "{" + strings.Join(filters, ",") + "}"
	return logstorage.ParseFilter(s)
}

// extraFilter represents a parsed key-value(s) pair from JSON extra_filters
type extraFilter struct {
	key    string
	values []string
}

// parseExtraFiltersJSON parses JSON-format extra filters
func parseExtraFiltersJSON(s string) ([]extraFilter, error) {
	v, err := fastjson.Parse(s)
	if err != nil {
		return nil, err
	}
	o := v.GetObject()

	var errOuter error
	var filters []extraFilter
	o.Visit(func(k []byte, v *fastjson.Value) {
		if errOuter != nil {
			return
		}
		switch v.Type() {
		case fastjson.TypeString:
			filters = append(filters, extraFilter{
				key:    string(k),
				values: []string{string(v.GetStringBytes())},
			})
		case fastjson.TypeArray:
			a := v.GetArray()
			if len(a) == 0 {
				return
			}
			orValues := make([]string, len(a))
			for i, av := range a {
				ov, err := av.StringBytes()
				if err != nil {
					errOuter = fmt.Errorf("cannot obtain string item at the array for key %q; item: %s", k, av)
					return
				}
				orValues[i] = string(ov)
			}
			filters = append(filters, extraFilter{
				key:    string(k),
				values: orValues,
			})
		default:
			errOuter = fmt.Errorf("unexpected type of value for key %q: %s; value: %s", k, v.Type(), v)
		}
	})
	if errOuter != nil {
		return nil, errOuter
	}
	return filters, nil
}

func getPositiveInt(r *http.Request, argName string) (int, error) {
	n, err := httputil.GetInt(r, argName)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("%q cannot be smaller than 0; got %d", argName, n)
	}
	return n, nil
}

func getBoolFromRequest(dst *bool, r *http.Request, argName string) error {
	s := r.FormValue(argName)
	if s == "" {
		return nil
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return fmt.Errorf("cannot parse %s=%q as bool: %w", argName, s, err)
	}
	*dst = b
	return nil
}

// getStringSliceFromRequest parses a string slice parameter.
// Accepts either JSON array format or comma-separated values.
func getStringSliceFromRequest(r *http.Request, argName string) ([]string, error) {
	s := r.FormValue(argName)
	if s == "" {
		return nil, nil
	}

	if strings.HasPrefix(s, "[") {
		// Parse as JSON array
		var a []string
		if err := json.Unmarshal([]byte(s), &a); err != nil {
			return nil, fmt.Errorf("cannot unmarshal JSON array from %s=%q: %w", argName, s, err)
		}
		return a, nil
	}

	// Parse as comma-separated list
	return strings.Split(s, ","), nil
}

// writeResponseHeaders writes common response headers for all query endpoints.
//
// Headers written:
//   - VL-Request-Duration-Seconds: Query execution time
//   - AccountID / ProjectID: The tenant used for the query (for client visibility)
//   - Access-Control-Expose-Headers: Makes custom headers visible to CORS clients
func (ca *commonArgs) writeResponseHeaders(h http.Header, startTime time.Time) {
	accessControlExposeHeaders := []string{"VL-Request-Duration-Seconds"}
	h.Set("VL-Request-Duration-Seconds", fmt.Sprintf("%.3f", time.Since(startTime).Seconds()))

	if len(ca.tenantIDs) == 1 {
		// Expose the tenant ID used for the query
		accessControlExposeHeaders = append(accessControlExposeHeaders, "AccountID", "ProjectID")
		tenantID := ca.tenantIDs[0]
		h.Set("AccountID", fmt.Sprintf("%d", tenantID.AccountID))
		h.Set("ProjectID", fmt.Sprintf("%d", tenantID.ProjectID))
	}

	for i, v := range accessControlExposeHeaders {
		accessControlExposeHeaders[i] = http.CanonicalHeaderKey(v)
	}
	h.Set("Access-Control-Expose-Headers", strings.Join(accessControlExposeHeaders, ", "))
}

// parseDuration parses a duration parameter from the request.
// Returns the default value if the parameter is not present.
func parseDuration(r *http.Request, argName, defaultValue string) (int64, error) {
	s := r.FormValue(argName)
	if s == "" {
		s = defaultValue
	}
	nsecs, ok := logstorage.TryParseDuration(s)
	if !ok {
		return 0, fmt.Errorf("cannot parse duration from the arg '%s=%s'", argName, s)
	}
	return nsecs, nil
}
