package kubernetescollector

import (
	"bytes"
	"cmp"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/valyala/fastjson"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Configuration flags for log processing.
var (
	// tenantID specifies the tenant ID for all logs collected from Kubernetes.
	// Format: <accountID>:<projectID>. Used for multi-tenancy in VictoriaLogs cluster.
	tenantID = flag.String("kubernetesCollector.tenantID", "0:0",
		"Default tenant ID to use for logs collected from Kubernetes pods in format: <accountID>:<projectID>. See https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy")

	// ignoreFields specifies fields to drop during ingestion.
	// Useful for removing noisy or sensitive fields.
	ignoreFields = flagutil.NewArrayString("kubernetesCollector.ignoreFields", "Fields to ignore across logs ingested from Kubernetes")

	// decolorizeFields specifies fields to strip ANSI color codes from.
	// Useful for logs from CLI tools that use color output.
	decolorizeFields = flagutil.NewArrayString("kubernetesCollector.decolorizeFields", "Fields to remove ANSI color codes across logs ingested from Kubernetes")

	// msgField specifies field names that may contain the log message.
	// The first matching field is renamed to _msg.
	msgField = flagutil.NewArrayString("kubernetesCollector.msgField", "Fields that may contain the _msg field. "+
		"Default: message,msg,log. See https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field")

	// timeField specifies field names that may contain the timestamp.
	// The first matching field is used for _time.
	timeField = flagutil.NewArrayString("kubernetesCollector.timeField", "Fields that may contain the _time field. "+
		"Default: time,timestamp,ts. If none of the specified fields is found in the log line, then the write time will be used. "+
		"See https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field")

	// extraFields specifies additional fields to add to every log entry.
	// Useful for adding cluster name, environment, etc.
	extraFields = flag.String("kubernetesCollector.extraFields", "", "Extra fields to add to each log line collected from Kubernetes pods in JSON format. "+
		`For example: -kubernetesCollector.extraFields='{"cluster":"cluster-1","env":"production"}'`)

	// streamFields specifies which fields to use for stream partitioning.
	// Logs with the same stream field values are stored together for efficient querying.
	streamFields = flagutil.NewArrayString("kubernetesCollector.streamFields", "Comma-separated list of fields to use as log stream fields for logs ingested from Kubernetes Pods. "+
		"Default: kubernetes.container_name,kubernetes.pod_name,kubernetes.pod_namespace. "+
		"See: https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields")

	// Label/annotation inclusion flags.
	// Labels and annotations are always available for filtering via -kubernetesCollector.excludeFilter,
	// but these flags control whether they're included as fields in the actual log entries.
	includePodLabels = flag.Bool("kubernetesCollector.includePodLabels", true, "Include Pod labels as additional fields in the log entries. "+
		"Even this setting is disabled, Pod labels are available for filtering via -kubernetes.excludeFilter flag")
	includePodAnnotations = flag.Bool("kubernetesCollector.includePodAnnotations", false, "Include Pod annotations as additional fields in the log entries. "+
		"Even this setting is disabled, Pod annotations are available for filtering via -kubernetes.excludeFilter flag")
	includeNodeLabels = flag.Bool("kubernetesCollector.includeNodeLabels", false, "Include Node labels as additional fields in the log entries. "+
		"Even this setting is disabled, Node labels are available for filtering via -kubernetes.excludeFilter flag")
	includeNodeAnnotations = flag.Bool("kubernetesCollector.includeNodeAnnotations", false, "Include Node annotations as additional fields in the log entries. "+
		"Even this setting is disabled, Node annotations are available for filtering via -kubernetes.excludeFilter flag")
)

// logFileProcessor processes log lines from a single container log file.
//
// It implements the processor interface and handles:
//   - CRI (Container Runtime Interface) log format parsing
//   - Docker json-file format parsing
//   - JSON log content parsing
//   - Kubernetes klog format parsing
//   - Multi-line log entry reassembly (CRI partial lines)
//   - Metadata enrichment (Kubernetes fields + extra fields)
//   - Forwarding to the remote write subsystem
type logFileProcessor struct {
	// storage is the remote write storage that forwards logs to VictoriaLogs.
	// This is the same insertutil.LogRowsStorage interface used by vlinsert,
	// but implemented by remotewrite.Storage instead of vlstorage.Storage.
	storage insertutil.LogRowsStorage

	// lr is a reusable LogRows buffer from a sync.Pool.
	// This is reused across multiple addRow calls for efficiency.
	lr *logstorage.LogRows

	// tenantID is the tenant identifier for all logs from this file.
	tenantID logstorage.TenantID

	// commonFields are the Kubernetes metadata fields for this container.
	// These are prepended to every log entry. Examples:
	// - kubernetes.container_name
	// - kubernetes.pod_name
	// - kubernetes.pod_namespace
	// - kubernetes.pod_labels.*
	commonFields []logstorage.Field

	// fieldsBuf is a scratch buffer for merging commonFields with log fields.
	// Reused across multiple addRow calls to avoid allocations.
	fieldsBuf []logstorage.Field

	// partialCRIContent accumulates content from CRI partial log lines.
	// Containerd splits long log lines (>16KB) into multiple CRI entries
	// with the 'P' (partial) flag. We accumulate these until we get the
	// final entry with the 'F' (final) flag.
	partialCRIContent *bytesutil.ByteBuffer

	// partialCRIContentSize tracks the actual accumulated size.
	// Used to detect and discard oversized log entries.
	partialCRIContentSize int
}

// newLogFileProcessor creates a new processor for a container's log file.
//
// The processor:
//  1. Filters out unwanted labels/annotations based on configuration
//  2. Initializes the LogRows buffer with stream fields and settings
//  3. Stores the Kubernetes metadata for enrichment
//
// commonFields must not be modified after passing to this function,
// as they may be accessed from multiple goroutines.
func newLogFileProcessor(storage insertutil.LogRowsStorage, commonFields []logstorage.Field) *logFileProcessor {
	// Filter out labels or annotations if they should not be included.
	// Even when excluded from log entries, they're still available for
	// the excludeFilter which is applied before reading.
	if !*includePodLabels || !*includePodAnnotations || !*includeNodeLabels || !*includeNodeAnnotations {
		var fields []logstorage.Field
		for _, f := range commonFields {
			excludeField := !*includePodLabels && strings.HasPrefix(f.Name, "kubernetes.pod_labels.") ||
				!*includePodAnnotations && strings.HasPrefix(f.Name, "kubernetes.pod_annotations.") ||
				!*includeNodeLabels && strings.HasPrefix(f.Name, "kubernetes.node_labels.") ||
				!*includeNodeAnnotations && strings.HasPrefix(f.Name, "kubernetes.node_annotations.")

			if !excludeField {
				fields = append(fields, f)
			}
		}
		commonFields = fields
	}

	sfs := getStreamFields()
	efs := getExtraFields()
	const defaultMsgValue = "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field"
	lr := logstorage.GetLogRows(sfs, *ignoreFields, *decolorizeFields, efs, defaultMsgValue)

	return &logFileProcessor{
		storage:      storage,
		lr:           lr,
		tenantID:     getTenantID(),
		commonFields: commonFields,
	}
}

// tryAddLine processes a single log line from the container.
//
// This function:
//  1. Detects the log format (Docker json-file or CRI text)
//  2. Parses the format to extract timestamp and content
//  3. Handles partial CRI lines by accumulating them
//  4. Parses the log content (JSON, klog, or raw text)
//  5. Adds the enriched log entry to storage
//
// Returns true if the line should be committed to the checkpoint.
// Returns false for partial CRI lines that need more data.
//
// This design ensures we only checkpoint after complete multi-line entries,
// preventing half-processed entries on restart.
func (lfp *logFileProcessor) tryAddLine(logLine []byte) bool {
	if len(logLine) == 0 {
		// Empty line - commit immediately.
		return true
	}

	// Detect log format by the first character.
	if logLine[0] == '{' {
		// Most likely, vlagent is running in Docker with 'json-file' logging driver.
		// This format wraps the actual log content in JSON:
		// {"log":"Hello world\n","stream":"stdout","time":"2024-01-15T10:30:00.123456789Z"}
		parser := criJSONParserPool.Get()
		defer criJSONParserPool.Put(parser)

		criLine, err := parseCRILineJSON(parser, logLine)
		if err != nil {
			logger.Panicf("FATAL: cannot parse 'json-file' log content: %s; content: %q", err, logLine)
		}

		lfp.addLineInternal(criLine.timestamp, criLine.content)

		return true
	}

	// Parse as CRI text format.
	// Format: <timestamp> <stream> <partial_flag> <content>
	// Example: 2024-01-15T10:30:00.123456789Z stdout F Hello world
	criLine, err := parseCRILine(logLine)
	if err != nil {
		logger.Panicf("FATAL: cannot parse Container Runtime Interface log line: %s; content: %q", err, logLine)
	}

	// Handle partial CRI lines (split by containerd at 16KB boundaries).
	timestamp, content, ok := lfp.joinPartialLines(criLine)
	if !ok {
		// The log content is not yet complete - need more partial lines.
		return false
	}
	if len(content) == 0 {
		// The log content was truncated (too large) or empty.
		return true
	}

	lfp.addLineInternal(timestamp, content)

	// Release the partial content buffer back to the pool.
	if lfp.partialCRIContent != nil {
		partialCRIContentBufPool.Put(lfp.partialCRIContent)
		lfp.partialCRIContent = nil
	}

	return true
}

// joinPartialLines accumulates CRI partial log lines until the complete entry is received.
//
// Containerd splits log lines longer than 16KB into multiple CRI entries:
//   - First N-1 entries have the 'P' (partial) flag
//   - Final entry has the 'F' (final) flag
//
// This function:
//  1. Accumulates partial ('P') content in a buffer
//  2. Returns (0, nil, false) to signal "not ready yet"
//  3. When final ('F') is received, combines everything and returns
//  4. Discards entries exceeding maxLogLineSize
func (lfp *logFileProcessor) joinPartialLines(criLine criLine) (int64, []byte, bool) {
	if criLine.partial {
		// The log line is split into multiple lines.
		// Accumulate the content until the full line is received.

		if lfp.partialCRIContent == nil {
			lfp.partialCRIContent = partialCRIContentBufPool.Get()
		}

		lfp.partialCRIContentSize += len(criLine.content)
		if lfp.partialCRIContentSize <= maxLogLineSize {
			lfp.partialCRIContent.MustWrite(criLine.content)
		}
		return 0, nil, false // Not ready yet.
	}

	if lfp.partialCRIContent == nil || lfp.partialCRIContent.Len() == 0 {
		// The log line is complete and not split.
		return criLine.timestamp, criLine.content, true
	}

	// The final part of a split log line has been received.
	// Combine with accumulated partial content.

	lfp.partialCRIContentSize += len(criLine.content)
	if lfp.partialCRIContentSize > maxLogLineSize {
		// Discard the too large log line.
		reportLogRowSizeExceeded(lfp.commonFields, lfp.partialCRIContentSize)

		lfp.partialCRIContent.Reset()
		lfp.partialCRIContentSize = 0

		return 0, nil, true // Ready but with empty content (discarded).
	}

	// Append the final part to the accumulated content.
	lfp.partialCRIContent.MustWrite(criLine.content)
	content := lfp.partialCRIContent.B

	// Reset for the next multi-line entry.
	lfp.partialCRIContent.Reset()
	lfp.partialCRIContentSize = 0

	return criLine.timestamp, content, true
}

// addLineInternal parses log content and adds it to storage.
//
// This function:
//  1. Attempts to parse the content as JSON or klog
//  2. Falls back to treating content as raw _msg if parsing fails
//  3. Uses the CRI timestamp if no timestamp was found in the content
//  4. Merges Kubernetes metadata with parsed fields
//  5. Forwards to storage
func (lfp *logFileProcessor) addLineInternal(criTimestamp int64, line []byte) {
	parser := logstorage.GetJSONParser()
	defer logstorage.PutJSONParser(parser)

	// Try to parse the content as JSON or klog.
	timestamp, ok := parseLogRowContent(parser, line)
	if !ok {
		// Content is not JSON or klog - treat as raw message.
		parser.Fields = append(parser.Fields, logstorage.Field{
			Name:  "_msg",
			Value: bytesutil.ToUnsafeString(line),
		})
	}

	// Use CRI timestamp if content didn't have one.
	if timestamp <= 0 {
		// Timestamp from the log line is missing or invalid.
		// Use the timestamp from Container Runtime Interface (when the line was written).
		timestamp = criTimestamp
	}

	// Sanity check: drop entries with too many fields.
	// This prevents memory issues from malformed log entries.
	if len(parser.Fields) > 1000 {
		line := logstorage.MarshalFieldsToJSON(nil, parser.Fields)
		logger.Warnf("dropping log line with %d fields; %s", len(parser.Fields), line)
		return
	}

	lfp.addRow(timestamp, parser.Fields)
}

// addRow merges Kubernetes metadata with log fields and sends to storage.
//
// The field order is: commonFields (Kubernetes metadata) + parsed log fields.
// This order is important because VictoriaLogs uses the first occurrence
// of a field for stream field matching.
func (lfp *logFileProcessor) addRow(timestamp int64, fields []logstorage.Field) {
	// Clear and reuse the fields buffer.
	clear(lfp.fieldsBuf)
	lfp.fieldsBuf = append(lfp.fieldsBuf[:0], lfp.commonFields...)
	lfp.fieldsBuf = append(lfp.fieldsBuf, fields...)

	// Add the row to the LogRows buffer.
	lfp.lr.MustAdd(lfp.tenantID, timestamp, lfp.fieldsBuf, -1)

	// Send to storage immediately.
	// Unlike VictoriaLogs server which batches in logMessageProcessor,
	// vlagent's processor adds each row individually and batching
	// happens downstream in the pendingLogs layer.
	lfp.storage.MustAddRows(lfp.lr)
	lfp.lr.ResetKeepSettings()
}

// parseLogRowContent attempts to parse log content as JSON or klog.
//
// Returns the timestamp (if found in content) and parsed fields.
// Returns (0, false) if the content is not parseable as JSON or klog.
//
// JSON format: Fields are extracted directly. The message field
// (message, msg, or log) is renamed to _msg. The timestamp field
// (time, timestamp, or ts) is used for _time.
//
// klog format: Kubernetes logging format used by kubelet, kube-apiserver, etc.
// Example: I0214 10:30:00.123456 12345 main.go:42] Starting server
// Parsed into: level=INFO, thread_id=12345, source_line=main.go:42, _msg=Starting server
func parseLogRowContent(p *logstorage.JSONParser, data []byte) (int64, bool) {
	if len(data) == 0 {
		return 0, false
	}

	switch data[0] {
	case '{':
		// JSON format - parse as log message.
		err := p.ParseLogMessage(data, nil)
		if err != nil {
			return 0, false
		}

		// Try to parse timestamp from the time fields.
		var timestamp int64
		n := fieldIndex(p.Fields, getTimeFields())
		if n >= 0 {
			f := &p.Fields[n]
			v, ok := logstorage.TryParseTimestampRFC3339Nano(f.Value)
			if ok {
				timestamp = v
				// Set the time field to empty string to ignore it during data ingestion.
				// It was already parsed and will be used as _time.
				f.Value = ""
			}
		}

		// Rename the message field to _msg.
		// This ensures the message is searchable via _msg in LogsQL.
		logstorage.RenameField(p.Fields, getMsgFields(), "_msg")

		return timestamp, true
	case 'I', 'W', 'E', 'F':
		// Possibly klog format (Kubernetes logging format).
		ts := fasttime.UnixTimestamp()
		current := time.Unix(int64(ts), 0).UTC()
		timestamp, fields, ok := tryParseKlog(p.Fields, bytesutil.ToUnsafeString(data), current)
		if !ok {
			return 0, false
		}
		p.Fields = fields
		return timestamp, true
	}

	return 0, false
}

// tryParseKlog parses Kubernetes klog format.
//
// klog format example:
//
//	I0214 10:30:00.123456 12345 main.go:42] Starting server
//
// Where:
//   - I = level (I=INFO, W=WARNING, E=ERROR, F=FATAL)
//   - 0214 = month/day
//   - 10:30:00.123456 = time with microseconds
//   - 12345 = thread ID
//   - main.go:42 = source file and line
//   - Starting server = message
//
// Parsed into fields: level, thread_id, source_line, _msg
//
// See: https://github.com/kubernetes/klog/
func tryParseKlog(dst []logstorage.Field, src string, current time.Time) (int64, []logstorage.Field, bool) {
	// Minimum length check for valid klog format.
	if len(src) < len("I0101 00:00:00.000000 1 p:1] m") {
		return 0, nil, false
	}

	// Parse level (first character).
	level := getKlogLevel(src[0])
	src = src[1:]
	dst = append(dst, logstorage.Field{Name: "level", Value: level})

	// Parse timestamp (month/day time).
	// klog doesn't include year, so we use the current year.
	timestampStr := src[:len("0102 15:04:05.000000")]
	t, err := time.ParseInLocation("0102 15:04:05.000000", timestampStr, time.UTC)
	if err != nil {
		return 0, nil, false
	}
	src = src[len("0102 15:04:05.000000"):]

	// Assume current year.
	t = t.AddDate(current.Year(), 0, 0)

	// Handle year boundary - if the parsed time is more than 24 hours
	// in the future, it's probably from the previous year.
	if t.Add(-time.Hour * 24).After(current) {
		t = t.AddDate(-1, 0, 0)
	}
	timestamp := t.UnixNano()

	// Remove trailing spaces after timestamp.
	if len(src) == 0 || src[0] != ' ' {
		return 0, nil, false
	}
	src = strings.TrimLeft(src, " ")

	// Parse thread ID.
	n := strings.IndexByte(src, ' ')
	if n <= 0 {
		return 0, nil, false
	}
	threadID := src[:n]
	src = src[n+1:]
	dst = append(dst, logstorage.Field{Name: "thread_id", Value: threadID})

	// Parse file:line (ends with ']').
	n = strings.IndexByte(src, ']')
	if n <= 0 {
		return 0, nil, false
	}
	sourceLine := src[:n]
	src = src[n+1:]
	if len(src) == 0 || src[0] != ' ' {
		return 0, nil, false
	}
	src = src[1:]
	dst = append(dst, logstorage.Field{Name: "source_line", Value: sourceLine})

	// Parse log content (may include quoted message and key="value" pairs).
	var ok bool
	dst, ok = tryParseKlogContent(dst, src)
	if !ok {
		return 0, nil, false
	}

	return timestamp, dst, true
}

// tryParseKlogContent parses the message and optional key="value" pairs from klog content.
//
// The message may be:
//   - Unquoted: Everything is the message
//   - Quoted: Message is in quotes, followed by optional key="value" pairs
//
// Example quoted: "Starting server" app="myapp" version="1.0"
func tryParseKlogContent(dst []logstorage.Field, src string) ([]logstorage.Field, bool) {
	if len(src) == 0 {
		return dst, false
	}
	if src[0] != '"' {
		// Fast path: message is not quoted and does not contain additional key="value" fields.
		return append(dst, logstorage.Field{Name: "_msg", Value: src}), true
	}

	// Slow path: message is quoted and may contain additional key="value" fields.
	prefix, err := strconv.QuotedPrefix(src)
	if err != nil {
		return nil, false
	}
	msg, err := strconv.Unquote(prefix)
	if err != nil {
		return nil, false
	}
	src = src[len(prefix):]
	dst = append(dst, logstorage.Field{Name: "_msg", Value: msg})

	// Parse key="value" pairs.
	for len(src) > 0 {
		if src[0] == ' ' {
			src = src[1:]
		}

		n := strings.IndexByte(src, '=')
		if n <= 0 {
			return nil, false
		}
		key := src[:n]
		src = src[n+1:]

		prefix, err := strconv.QuotedPrefix(src)
		if err != nil {
			return nil, false
		}
		value, err := strconv.Unquote(prefix)
		if err != nil {
			return nil, false
		}
		src = src[len(prefix):]

		dst = append(dst, logstorage.Field{Name: key, Value: value})
	}

	return dst, true
}

// getKlogLevel converts klog level character to string.
// See: https://github.com/kubernetes/klog/blob/main/internal/severity/severity.go#L41-L47
func getKlogLevel(l byte) string {
	switch l {
	case 'I':
		return "INFO"
	case 'W':
		return "WARNING"
	case 'E':
		return "ERROR"
	case 'F':
		return "FATAL"
	}
	return "UNKNOWN"
}

// fieldIndex returns the index of the first field matching any of the given names.
// Returns -1 if no match is found.
func fieldIndex(fields []logstorage.Field, names []string) int {
	for _, n := range names {
		for j := range fields {
			f := &fields[j]
			if f.Name == n && f.Value != "" {
				return j
			}
		}
	}
	return -1
}

// mustClose releases the processor's resources.
func (lfp *logFileProcessor) mustClose() {
	logstorage.PutLogRows(lfp.lr)
	lfp.lr = nil
}

// criLine represents a parsed CRI log line.
type criLine struct {
	// timestamp is when the log entry was written by the container runtime.
	timestamp int64

	// partial is true if this is a partial line (containerd splits at 16KB).
	// The content should be accumulated until we receive the final (non-partial) line.
	partial bool

	// content is the actual log message from the container.
	content []byte
}

// parseCRILine parses a CRI (Container Runtime Interface) format log line.
//
// CRI format: <timestamp> <stream> <partial_flag> <content>
// Example: 2024-01-15T10:30:00.123456789Z stdout F Hello world
//
// Where:
//   - timestamp: RFC3339Nano timestamp
//   - stream: "stdout" or "stderr"
//   - partial_flag: "P" for partial (more data coming), "F" for final
//   - content: The actual log message
//
// The stream field is currently ignored - it could be added as a field if needed.
func parseCRILine(b []byte) (criLine, error) {
	// Parse timestamp.
	n := bytes.IndexByte(b, ' ')
	if n < 0 {
		return criLine{}, fmt.Errorf("unexpected end of timestamp")
	}
	v := b[:n]
	b = b[n+1:]
	timestamp, ok := logstorage.TryParseTimestampRFC3339Nano(bytesutil.ToUnsafeString(v))
	if !ok {
		return criLine{}, fmt.Errorf("invalid timestamp %q", v)
	}

	// Skip stream value (stdout/stderr) - we don't currently use it.
	n = bytes.IndexByte(b, ' ')
	if n < 0 {
		return criLine{}, fmt.Errorf("unexpected end of stream")
	}
	b = b[n+1:]

	// Parse partial flag.
	n = bytes.IndexByte(b, ' ')
	if n < 0 {
		return criLine{}, fmt.Errorf("unexpected end of follow flag")
	}
	v = b[:n]
	b = b[n+1:]
	if len(v) != 1 {
		return criLine{}, fmt.Errorf("invalid length of follow flag")
	}
	partial := v[0] == 'P'

	// The rest is the content.
	content := b

	return criLine{
		timestamp: timestamp,
		partial:   partial,
		content:   content,
	}, nil
}

// parseCRILineJSON parses a Docker json-file format log line.
//
// Docker json-file format wraps the actual log in JSON:
//
//	{"log":"Hello world\n","stream":"stdout","time":"2024-01-15T10:30:00.123456789Z"}
//
// Note: Docker json-file doesn't have partial lines - each line is complete.
// The 'log' field includes the trailing newline from the original log.
//
// See: https://docs.docker.com/engine/logging/drivers/json-file/
func parseCRILineJSON(parser *fastjson.Parser, b []byte) (criLine, error) {
	v, err := parser.ParseBytes(b)
	if err != nil {
		return criLine{}, err
	}

	obj, err := v.Object()
	if err != nil {
		return criLine{}, err
	}

	// Extract 'log' field (the actual log content).
	f := obj.Get("log")
	if f == nil {
		return criLine{}, fmt.Errorf("missing 'log' field")
	}

	logContent, err := f.StringBytes()
	if err != nil {
		return criLine{}, err
	}

	// Extract 'time' field.
	f = obj.Get("time")
	if f == nil {
		return criLine{}, fmt.Errorf("missing 'time' field")
	}

	timestampContent, err := f.StringBytes()
	if err != nil {
		return criLine{}, err
	}
	timestampStr := bytesutil.ToUnsafeString(timestampContent)
	timestamp, ok := logstorage.TryParseTimestampRFC3339Nano(timestampStr)
	if !ok {
		return criLine{}, fmt.Errorf("invalid timestamp %q", timestampStr)
	}

	return criLine{
		timestamp: timestamp,
		// Docker json-file always has complete lines (no partial flag).
		partial: false,
		content: logContent,
	}, nil
}

// Tenant ID parsing (cached after first parse).
var tenantIDOnce sync.Once
var parsedTenantID logstorage.TenantID

func getTenantID() logstorage.TenantID {
	tenantIDOnce.Do(initTenantID)
	return parsedTenantID
}

func initTenantID() {
	v, err := logstorage.ParseTenantID(*tenantID)
	if err != nil {
		logger.Fatalf("cannot parse -kubernetesCollector.tenantID=%q: %s", *tenantID, err)
	}
	parsedTenantID = v
}

// Extra fields parsing (cached after first parse).
var extraFieldsOnce sync.Once
var parsedExtraFields []logstorage.Field

func getExtraFields() []logstorage.Field {
	extraFieldsOnce.Do(initExtraFields)
	return parsedExtraFields
}

func initExtraFields() {
	if *extraFields == "" {
		return
	}

	p := logstorage.GetJSONParser()
	if err := p.ParseLogMessage([]byte(*extraFields), nil); err != nil {
		logger.Fatalf("cannot parse -kubernetesCollector.extraFields=%q: %s", *extraFields, err)
	}

	fields := p.Fields
	// Sort for consistent ordering.
	slices.SortFunc(fields, func(a, b logstorage.Field) int {
		return cmp.Compare(a.Name, b.Name)
	})

	parsedExtraFields = fields
}

// Default field name lists.
var defaultMsgFields = []string{"message", "msg", "log"}

func getMsgFields() []string {
	if len(*msgField) == 0 {
		return defaultMsgFields
	}
	return *msgField
}

var defaultTimeFields = []string{"time", "timestamp", "ts"}

func getTimeFields() []string {
	if len(*timeField) == 0 {
		return defaultTimeFields
	}
	return *timeField
}

// defaultStreamFields defines the default stream fields for partitioning.
// Must be synced with getCommonFields in collector.go.
// These fields are used to group related logs together for efficient querying.
var defaultStreamFields = []string{"kubernetes.container_name", "kubernetes.pod_name", "kubernetes.pod_namespace"}

func getStreamFields() []string {
	if len(*streamFields) == 0 {
		return defaultStreamFields
	}
	return *streamFields
}

// Buffer pools for reusable allocations.
var partialCRIContentBufPool bytesutil.ByteBufferPool
var criJSONParserPool fastjson.ParserPool

// reportLogRowSizeExceeded logs a warning about an oversized log entry.
func reportLogRowSizeExceeded(commonFields []logstorage.Field, size int) {
	var pod, namespace string
	for _, f := range commonFields {
		if f.Name == "kubernetes.pod_namespace" {
			namespace = f.Value
		}
		if f.Name == "kubernetes.pod_name" {
			pod = f.Value
		}
		if pod != "" && namespace != "" {
			break
		}
	}
	logger.Warnf("skipping log entry from Pod %q in namespace %q: entry size of %.2f MiB exceeds the maximum allowed size of %d MiB",
		pod, namespace, float64(size)/1024/1024, maxLogLineSize/1024/1024)
}
