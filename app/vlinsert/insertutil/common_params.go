// Package insertutil provides shared utilities for log ingestion endpoints.
//
// This package sits between the protocol-specific handlers (jsonline, elasticsearch, loki, etc.)
// and the storage layer (vlstorage). It provides:
//
//   - CommonParams: HTTP parameter parsing shared across all ingestion protocols
//   - LogMessageProcessor: A concurrency-safe buffer that accumulates log entries and flushes to storage
//   - Field validation and size limits enforcement
//
// The typical ingestion flow through this package is:
//
//	HTTP Request
//	    ↓
//	GetCommonParams(r) → Extract tenant, stream fields, ignore fields, etc.
//	    ↓
//	cp.NewLogMessageProcessor(protocol, isStreamMode) → Create a processor for this request
//	    ↓
//	lmp.AddRow(timestamp, fields, streamFieldsLen) → Called for each log entry
//	    ↓
//	lmp.MustClose() → Flush remaining buffer to storage
//	    ↓
//	storage.MustAddRows(lr) → Write to partition (single-node) or shard to nodes (cluster)
//
// Why LogMessageProcessor exists:
// The underlying logstorage.LogRows is a raw data buffer optimized for storage efficiency,
// but it has no thread safety or flushing logic. LogMessageProcessor wraps LogRows and adds:
//   - Mutex protection (LogRows is not thread-safe)
//   - Automatic flushing when buffer reaches capacity
//   - Periodic background flushing for stream connections
//   - Metrics tracking (rows ingested, bytes, flush duration)
//   - Field count limit enforcement
package insertutil

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	defaultMsgValue = flag.String("defaultMsgValue", "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field",
		"Default value for _msg field if the ingested log entry doesn't contain it; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field")
)

// CommonParams contains common HTTP parameters used by log ingestion APIs.
// All fields can be set via query parameters or HTTP headers.
//
// Parameter mapping (query arg → header → field):
//
//	AccountID, ProjectID     → TenantID (multi-tenancy)
//	_time_field              → VL-Time-Field      → TimeFields
//	_msg_field               → VL-Msg-Field       → MsgFields
//	_stream_fields           → VL-Stream-Fields   → StreamFields
//	ignore_fields            → VL-Ignore-Fields   → IgnoreFields
//	decolorize_fields        → VL-Decolorize-Fields → DecolorizeFields
//	preserve_json_keys       → VL-Preserve-JSON-Keys → PreserveJSONKeys
//	extra_fields             → VL-Extra-Fields    → ExtraFields
//	debug                    → VL-Debug           → Debug
//
// See https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-parameters
type CommonParams struct {
	// TenantID identifies the tenant for multi-tenancy.
	// Extracted from AccountID and ProjectID query args or headers.
	TenantID logstorage.TenantID

	// TimeFields specifies which field(s) contain the timestamp.
	// Default: ["_time"]. Multiple fields are tried in order until a valid timestamp is found.
	TimeFields []string

	// MsgFields specifies which field(s) contain the log message.
	// The first non-empty field becomes the _msg field.
	MsgFields []string

	// StreamFields specifies which fields become stream tags.
	// Stream tags are indexed and determine which stream a log entry belongs to.
	// Different stream tag values = different streams = separate indexes.
	StreamFields []string

	// IgnoreFields specifies field name prefixes to drop during ingestion.
	// Useful for filtering noisy or sensitive fields.
	IgnoreFields []string

	// DecolorizeFields specifies field name prefixes to strip ANSI color codes from.
	// Useful for logs that contain terminal color codes which break JSON formatting.
	DecolorizeFields []string

	// PreserveJSONKeys specifies field name prefixes to preserve as-is when parsing JSON.
	// Normally, JSON keys are normalized; this prevents that for specific prefixes.
	PreserveJSONKeys []string

	// ExtraFields are additional key=value pairs to add to every log entry.
	// These override any fields with the same name in the input.
	ExtraFields []logstorage.Field

	// IsTimeFieldSet indicates whether TimeFields was explicitly set by the user.
	// TimeFields defaults to ["_time"] even when not set, so this flag distinguishes
	// "user didn't specify" from "user explicitly chose _time".
	IsTimeFieldSet bool

	// Debug mode: When true, log entries are logged and dropped instead of stored.
	// Useful for verifying what would be ingested without actually storing data.
	Debug           bool
	DebugRequestURI string
	DebugRemoteAddr string
}

// GetCommonParams returns CommonParams from r.
func GetCommonParams(r *http.Request) (*CommonParams, error) {
	// Extract tenantID
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		return nil, err
	}

	var isTimeFieldSet bool
	timeFields := []string{"_time"}
	if tfs := getArray(r, "_time_field", "VL-Time-Field"); len(tfs) > 0 {
		isTimeFieldSet = true
		timeFields = tfs
	}

	msgFields := getArray(r, "_msg_field", "VL-Msg-Field")
	streamFields := getArray(r, "_stream_fields", "VL-Stream-Fields")
	ignoreFields := getArray(r, "ignore_fields", "VL-Ignore-Fields")
	decolorizeFields := getArray(r, "decolorize_fields", "VL-Decolorize-Fields")
	preserveJSONKeys := getArray(r, "preserve_json_keys", "VL-Preserve-JSON-Keys")

	extraFields, err := getExtraFields(r)
	if err != nil {
		return nil, err
	}

	debug := false
	if dv := httputil.GetRequestValue(r, "debug", "VL-Debug"); dv != "" {
		debug, err = strconv.ParseBool(dv)
		if err != nil {
			return nil, fmt.Errorf("cannot parse debug=%q: %w", dv, err)
		}
	}
	debugRequestURI := ""
	debugRemoteAddr := ""
	if debug {
		debugRequestURI = httpserver.GetRequestURI(r)
		debugRemoteAddr = httpserver.GetQuotedRemoteAddr(r)
	}

	cp := &CommonParams{
		TenantID:         tenantID,
		TimeFields:       timeFields,
		MsgFields:        msgFields,
		StreamFields:     streamFields,
		IgnoreFields:     ignoreFields,
		DecolorizeFields: decolorizeFields,
		PreserveJSONKeys: preserveJSONKeys,
		ExtraFields:      extraFields,

		IsTimeFieldSet:  isTimeFieldSet,
		Debug:           debug,
		DebugRequestURI: debugRequestURI,
		DebugRemoteAddr: debugRemoteAddr,
	}

	return cp, nil
}

func getExtraFields(r *http.Request) ([]logstorage.Field, error) {
	efs := getArray(r, "extra_fields", "VL-Extra-Fields")
	if len(efs) == 0 {
		return nil, nil
	}

	extraFields := make([]logstorage.Field, len(efs))
	for i, ef := range efs {
		n := strings.Index(ef, "=")
		if n <= 0 || n == len(ef)-1 {
			return nil, fmt.Errorf(`invalid extra_field format: %q; must be in the form "field=value"`, ef)
		}
		extraFields[i] = logstorage.Field{
			Name:  ef[:n],
			Value: ef[n+1:],
		}
	}
	return extraFields, nil
}

func getArray(r *http.Request, argKey, headerKey string) []string {
	a := httputil.GetArray(r, argKey, headerKey)
	return removeEmptyTokens(a)
}

func removeEmptyTokens(a []string) []string {
	dst := a[:0]
	for _, s := range a {
		s = strings.TrimSpace(s)
		if s != "" {
			dst = append(dst, s)
		}
	}
	return dst
}

// GetCommonParamsForSyslog returns common params needed for parsing syslog messages and storing them to the given tenantID.
func GetCommonParamsForSyslog(tenantID logstorage.TenantID, streamFields, ignoreFields, decolorizeFields []string, extraFields []logstorage.Field) *CommonParams {
	// See https://docs.victoriametrics.com/victorialogs/logsql/#unpack_syslog-pipe
	if streamFields == nil {
		streamFields = []string{
			"hostname",
			"app_name",
			"proc_id",
			"cef.device_vendor",
			"cef.device_product",
			"cef.device_event_class_id",
		}
	}
	cp := &CommonParams{
		TenantID: tenantID,
		TimeFields: []string{
			"timestamp",
		},
		MsgFields: []string{
			"message",
		},
		StreamFields:     streamFields,
		IgnoreFields:     ignoreFields,
		DecolorizeFields: decolorizeFields,
		ExtraFields:      extraFields,
	}

	return cp
}

// LogRowsStorage is the interface for the storage backend that receives ingested logs.
//
// This interface is implemented by vlstorage.Storage (in app/vlstorage/main.go),
// which routes to either local storage (single-node) or network storage (cluster mode).
// The dependency injection pattern via SetLogRowsStorage allows insertutil to remain
// decoupled from the actual storage implementation.
//
// Why an interface? This package (insertutil) is used by protocol handlers, which
// shouldn't need to know whether they're writing to local disk or remote nodes.
type LogRowsStorage interface {
	// MustAddRows adds the buffered log rows to storage.
	// In single-node mode: writes to local partitions.
	// In cluster mode: shards rows to remote vlstorage nodes.
	MustAddRows(lr *logstorage.LogRows)

	// CanWriteData returns non-nil error if storage cannot accept data.
	// Used to provide early feedback to clients (e.g., when disk is full).
	CanWriteData() error
}

// logRowsStorage holds the storage backend set during initialization.
// Set via SetLogRowsStorage() by the main application startup code.
var logRowsStorage LogRowsStorage

// SetLogRowsStorage sets the storage backend for all LogMessageProcessor instances.
// This must be called before any LogMessageProcessor is created.
// Called from app/victoria-logs/main.go after vlstorage.Init().
func SetLogRowsStorage(storage LogRowsStorage) {
	logRowsStorage = storage
}

// CanWriteData checks if the storage backend can accept new data.
// Returns an error if storage is in read-only mode (e.g., disk full).
func CanWriteData() error {
	return logRowsStorage.CanWriteData()
}

// LogMessageProcessor is the public interface for adding log entries during ingestion.
// Protocol handlers receive this interface and call AddRow for each log entry.
//
// The interface hides the buffering and flushing details from callers - they just
// add rows and call MustClose when done. The implementation handles:
//   - Buffering rows until capacity threshold
//   - Flushing to storage when buffer is full
//   - Periodic background flushing (for stream mode)
type LogMessageProcessor interface {
	// AddRow adds a single log entry to the buffer.
	//
	// Parameters:
	//   - timestamp: Unix nanosecond timestamp for the log entry
	//   - fields: Key-value pairs (log fields). Caller can reuse this slice after AddRow returns.
	//   - streamFieldsLen: If >= 0, use the first N fields as stream tags instead of
	//     CommonParams.StreamFields. Use -1 for default behavior.
	AddRow(timestamp int64, fields []logstorage.Field, streamFieldsLen int)

	// MustClose flushes any remaining buffered data to storage and releases resources.
	// Must be called when done adding rows (typically in a defer).
	MustClose()
}

// logMessageProcessor is the concrete implementation of LogMessageProcessor.
//
// ARCHITECTURE OVERVIEW:
// This struct wraps a logstorage.LogRows buffer and provides:
//  1. Thread safety via mutex (LogRows is not thread-safe)
//  2. Automatic flushing when buffer reaches capacity
//  3. Optional periodic flushing for long-lived stream connections
//  4. Metrics tracking (rows, bytes, flush duration)
//  5. Field count validation
//
// LIFECYCLE:
//  1. Created per HTTP request via CommonParams.NewLogMessageProcessor()
//  2. Rows added via AddRow() or AddInsertRow()
//  3. Buffer automatically flushed when full (NeedFlush returns true)
//  4. MustClose() called at end of request to flush remaining data
//
// STREAM MODE (isStreamMode=true):
// For long-lived connections (syslog, journald), a background goroutine flushes
// every ~1 second to bound latency for connections that trickle data slowly.
// This ensures logs don't sit in memory for too long before being queryable.
type logMessageProcessor struct {
	// mu protects all fields below. Must be held when accessing lr or calling flushLocked.
	mu sync.Mutex

	// wg tracks the background flush goroutine (only started in stream mode).
	// MustClose waits for this before returning.
	wg sync.WaitGroup

	// stopCh signals the background flush goroutine to stop.
	// Closed in MustClose.
	stopCh chan struct{}

	// lastFlushTime tracks when the last flush occurred.
	// Used by the periodic flusher to avoid unnecessary flushes.
	lastFlushTime time.Time

	// cp holds the configuration for this processor (tenant, stream fields, etc.)
	cp *CommonParams

	// lr is the underlying LogRows buffer that holds the actual log data.
	// NOT thread-safe - must be accessed under mu.
	lr *logstorage.LogRows

	// Metrics for this protocol type
	rowsIngestedTotal  *metrics.Counter
	bytesIngestedTotal *metrics.Counter
	flushDuration      *metrics.Summary

	// Counters for the current unflushed batch (reset after each flush)
	unflushedRows  int
	unflushedBytes int
}

// initPeriodicFlush starts a background goroutine that flushes the buffer every ~1 second.
// This is only used for stream mode connections (syslog, journald) that may trickle
// data slowly - without periodic flushing, logs could sit in memory indefinitely.
//
// The goroutine checks if it's been >= 1 second since the last flush; if so, it flushes
// even if the buffer isn't full. This bounds the maximum latency for small batches.
func (lmp *logMessageProcessor) initPeriodicFlush() {
	lmp.lastFlushTime = time.Now()

	lmp.wg.Go(func() {
		// Add jitter to prevent all flushers from running at exactly the same time
		d := timeutil.AddJitterToDuration(time.Second)
		ticker := time.NewTicker(d)
		defer ticker.Stop()

		for {
			select {
			case <-lmp.stopCh:
				return
			case <-ticker.C:
				lmp.mu.Lock()
				// Only flush if at least 1 second has passed since the last flush
				// (the jitter may make this interval slightly longer)
				if time.Since(lmp.lastFlushTime) >= d {
					lmp.flushLocked()
				}
				lmp.mu.Unlock()
			}
		}
	})
}

// AddRow adds a single log entry to the buffer.
//
// THREAD SAFETY: This method is safe for concurrent use (acquires mu).
//
// VALIDATION: Rows exceeding -insert.maxFieldsPerLine are dropped with a warning.
//
// DEBUG MODE: If CommonParams.Debug is true, the row is logged and dropped
// instead of being stored. This is useful for verifying ingestion without storing data.
//
// FLUSHING: If the buffer reaches capacity after adding this row (NeedFlush returns true),
// the buffer is immediately flushed to storage.
func (lmp *logMessageProcessor) AddRow(timestamp int64, fields []logstorage.Field, streamFieldsLen int) {
	lmp.mu.Lock()
	defer lmp.mu.Unlock()

	// Track metrics for this row
	lmp.unflushedRows++
	n := logstorage.EstimatedJSONRowLen(fields)
	lmp.unflushedBytes += n

	// Enforce field count limit
	if len(fields) > *MaxFieldsPerLine {
		line := logstorage.MarshalFieldsToJSON(nil, fields)
		logger.Warnf("dropping log line with %d fields; it exceeds -insert.maxFieldsPerLine=%d; %s", len(fields), *MaxFieldsPerLine, line)
		rowsDroppedTotalTooManyFields.Inc()
		return
	}

	// Add the row to the buffer
	lmp.lr.MustAdd(lmp.cp.TenantID, timestamp, fields, streamFieldsLen)

	// Debug mode: log and drop instead of storing
	if lmp.cp.Debug {
		s := lmp.lr.GetRowString(0)
		lmp.lr.ResetKeepSettings()
		logger.Infof("remoteAddr=%s; requestURI=%s; ignoring log entry because of `debug` arg: %s", lmp.cp.DebugRemoteAddr, lmp.cp.DebugRequestURI, s)
		rowsDroppedTotalDebug.Inc()
		return
	}

	// Flush if buffer is approaching capacity
	if lmp.lr.NeedFlush() {
		lmp.flushLocked()
	}
}

// InsertRowProcessor is an alternative interface for adding pre-formatted log entries.
//
// WHY TWO INTERFACES?
//   - LogMessageProcessor.AddRow: Takes raw fields and timestamp, lets LogRows handle stream tag extraction
//   - InsertRowProcessor.AddInsertRow: Takes a pre-formatted InsertRow with stream tags already computed
//
// The InsertRowProcessor interface is used by:
//   - /insert/native endpoint: Native binary protocol
//   - /internal/insert endpoint: Cluster-internal protocol (in internalinsert package)
//
// These protocols receive pre-serialized data from vlagent or other vlinsert nodes,
// where stream tags have already been computed. Using AddInsertRow avoids re-parsing
// and re-computing stream tags.
type InsertRowProcessor interface {
	// AddInsertRow adds a pre-formatted InsertRow to the buffer.
	AddInsertRow(r *logstorage.InsertRow)
}

// AddInsertRow adds a pre-formatted InsertRow to the buffer.
// This is the InsertRowProcessor interface implementation.
//
// The InsertRow already has:
//   - TenantID (from the binary payload or HTTP headers)
//   - StreamTagsCanonical (pre-computed canonical form like {app="nginx",host="server1"})
//   - Timestamp (Unix nanoseconds)
//   - Fields (key-value pairs)
//
// Note: For /insert/native, the TenantID from HTTP headers overrides any tenant in the payload.
func (lmp *logMessageProcessor) AddInsertRow(r *logstorage.InsertRow) {
	lmp.mu.Lock()
	defer lmp.mu.Unlock()

	// Track metrics
	lmp.unflushedRows++
	n := logstorage.EstimatedJSONRowLen(r.Fields)
	lmp.unflushedBytes += n

	// Enforce field count limit
	if len(r.Fields) > *MaxFieldsPerLine {
		line := logstorage.MarshalFieldsToJSON(nil, r.Fields)
		logger.Warnf("dropping log line with %d fields; it exceeds -insert.maxFieldsPerLine=%d; %s", len(r.Fields), *MaxFieldsPerLine, line)
		rowsDroppedTotalTooManyFields.Inc()
		return
	}

	// Add the row to the buffer
	lmp.lr.MustAddInsertRow(r)

	// Debug mode: log and drop instead of storing
	if lmp.cp.Debug {
		s := lmp.lr.GetRowString(0)
		lmp.lr.ResetKeepSettings()
		logger.Infof("remoteAddr=%s; requestURI=%s; ignoring log entry because of `debug` arg: %s", lmp.cp.DebugRemoteAddr, lmp.cp.DebugRequestURI, s)
		rowsDroppedTotalDebug.Inc()
		return
	}

	// Flush if buffer is approaching capacity
	if lmp.lr.NeedFlush() {
		lmp.flushLocked()
	}
}

// flushLocked sends the buffered log rows to storage and resets the buffer.
// Must be called with lmp.mu held.
//
// This is the actual handoff point between the ingestion layer and storage:
//  1. Calls logRowsStorage.MustAddRows(lr) which routes to local or network storage
//  2. Resets LogRows (keeping configuration) for reuse
//  3. Updates metrics (rows ingested, bytes, flush duration)
//  4. Resets unflushed counters
//
// The call to logRowsStorage.MustAddRows is synchronous - it blocks until the
// storage layer has accepted the data. In single-node mode, this means the data
// has been written to in-memory partitions. In cluster mode, it means the data
// has been buffered for sending to remote nodes (actual network send is async).
func (lmp *logMessageProcessor) flushLocked() {
	start := time.Now()
	lmp.lastFlushTime = start

	// Hand off to storage layer
	logRowsStorage.MustAddRows(lmp.lr)

	// Reset buffer for reuse (keeps stream fields, ignore fields, etc.)
	lmp.lr.ResetKeepSettings()

	// Update metrics
	lmp.flushDuration.UpdateDuration(start)
	lmp.rowsIngestedTotal.Add(lmp.unflushedRows)
	lmp.bytesIngestedTotal.Add(lmp.unflushedBytes)

	// Reset counters
	lmp.unflushedRows = 0
	lmp.unflushedBytes = 0
}

// MustClose flushes any remaining buffered data and releases resources.
// This MUST be called when done with the processor (typically via defer).
//
// Shutdown sequence:
//  1. Close stopCh to signal the background flush goroutine to stop
//  2. Wait for the goroutine to finish (wg.Wait)
//  3. Final flush of any remaining data
//  4. Return LogRows to the pool for reuse
//  5. Decrement active processor count
func (lmp *logMessageProcessor) MustClose() {
	// Stop the periodic flush goroutine if running
	close(lmp.stopCh)
	lmp.wg.Wait()

	// Final flush of any remaining data
	lmp.flushLocked()

	// Return LogRows to pool
	logstorage.PutLogRows(lmp.lr)
	lmp.lr = nil

	// Update active processor count
	messageProcessorCount.Add(-1)
}

// NewLogMessageProcessor creates a new processor for ingesting log entries.
//
// Parameters:
//   - protocolName: Used for metrics labels (e.g., "jsonline", "elasticsearch", "syslog")
//   - isStreamMode: If true, enables periodic background flushing every ~1 second.
//     Set to true for stream-like ingestion paths (e.g., syslog, journald, jsonline,
//     elasticsearch_bulk) that may trickle data. Set to false for buffered
//     request/response protocols (e.g., loki, datadog, opentelemetry, nativeinsert).
//
// The returned processor must be closed with MustClose() when done (use defer).
//
// Example usage:
//
//	lmp := cp.NewLogMessageProcessor("jsonline", true)
//	defer lmp.MustClose()
//	for _, entry := range entries {
//	    lmp.AddRow(entry.timestamp, entry.fields, -1)
//	}
func (cp *CommonParams) NewLogMessageProcessor(protocolName string, isStreamMode bool) LogMessageProcessor {
	// Get a configured LogRows from the pool
	lr := logstorage.GetLogRows(cp.StreamFields, cp.IgnoreFields, cp.DecolorizeFields, cp.ExtraFields, *defaultMsgValue)

	// Get or create metrics for this protocol
	rowsIngestedTotal := metrics.GetOrCreateCounter(fmt.Sprintf("vl_rows_ingested_total{type=%q}", protocolName))
	bytesIngestedTotal := metrics.GetOrCreateCounter(fmt.Sprintf("vl_bytes_ingested_total{type=%q}", protocolName))
	flushDuration := metrics.GetOrCreateSummary(fmt.Sprintf("vl_insert_flush_duration_seconds{type=%q}", protocolName))

	lmp := &logMessageProcessor{
		cp: cp,
		lr: lr,

		rowsIngestedTotal:  rowsIngestedTotal,
		bytesIngestedTotal: bytesIngestedTotal,
		flushDuration:      flushDuration,

		stopCh: make(chan struct{}),
	}

	// Start background flusher for stream connections
	if isStreamMode {
		lmp.initPeriodicFlush()
	}

	messageProcessorCount.Add(1)
	return lmp
}

// Metrics for monitoring ingestion behavior
var (
	// Rows dropped because debug mode was enabled (logged but not stored)
	rowsDroppedTotalDebug = metrics.NewCounter(`vl_rows_dropped_total{reason="debug"}`)
	// Rows dropped because they exceeded the field count limit
	rowsDroppedTotalTooManyFields = metrics.NewCounter(`vl_rows_dropped_total{reason="too_many_fields"}`)
	// Active processor count (exposed as gauge)
	_                     = metrics.NewGauge(`vl_insert_processors_count`, func() float64 { return float64(messageProcessorCount.Load()) })
	messageProcessorCount atomic.Int64
)

// IsJSONContentType returns true if ct is JSON content-type.
func IsJSONContentType(ct string) bool {
	return ct == "application/json" || strings.HasPrefix(ct, "application/json;")
}
