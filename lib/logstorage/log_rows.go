// Package logstorage provides the core storage engine for VictoriaLogs.
//
// ============== LogRows Overview ==============
//
// LogRows is the primary data structure for buffering log entries during ingestion.
// It acts as a staging area between receiving log data from clients and writing
// it to persistent storage (partitions).
//
// WHY BUFFER LOGS?
//  1. Batching: Accumulating multiple log entries allows efficient bulk writes
//     to storage, reducing per-entry overhead.
//  2. Arena Allocation: All string data is copied into a single contiguous buffer
//     (arena), improving cache locality and reducing allocation overhead.
//  3. Deduplication: Field names and values can be shared between consecutive
//     log entries, reducing memory usage.
//  4. Validation: Log entries are validated for size limits before being stored.
//
// ============== Two Types of LogRows ==============
//
// There are two related but distinct types:
//
// LogRows (public):
//   - Used by ingestion endpoints (vlinsert) to collect log entries
//   - Contains configuration: stream fields, ignore filters, extra fields
//   - Maintains streamTagsCanonical for each row (the canonical form of stream labels)
//   - Obtained via GetLogRows() with configuration parameters
//
// logRows (private):
//   - Internal representation used by datadb for batch processing
//   - Simpler structure: just streamIDs, timestamps, and field data
//   - No configuration - just raw data storage
//   - Used during the flush process when data is written to parts
//
// ============== Data Flow ==============
//
// Ingestion Path:
//  1. Ingestion endpoint receives log data
//  2. GetLogRows() creates a LogRows with appropriate configuration
//  3. MustAdd() is called for each log entry - fields are validated, stream tags extracted
//  4. LogRows is passed to Storage.MustAddRows()
//  5. partition.mustAddRows() registers new streams in indexdb
//  6. datadb.mustAddRows() converts LogRows to internal logRows
//  7. logRows is flushed to in-memory parts when full
//
// ============== Key Concepts ==============
//
// Stream Tags: Labels that identify a log stream (e.g., {app="nginx", host="server1"}).
// These determine which stream a log entry belongs to and are indexed for fast queries.
//
// Stream ID: A 128-bit hash of the canonical stream tags. Used as the primary key
// for looking up streams in the index.
//
// Canonical Form: Stream tags are serialized in a deterministic (sorted) format
// so that equivalent tag sets always produce the same hash.
//
// _msg Field: The primary log message field. Internally stored with empty name ""
// for efficiency, but displayed as "_msg" in queries.
package logstorage

import (
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// LogRows holds a batch of log entries during the ingestion process.
// It is the public-facing type used by ingestion endpoints.
//
// USAGE PATTERN:
//
//	lr := GetLogRows(streamFields, ignoreFields, decolorizeFields, extraFields, defaultMsgValue)
//	lr.MustAdd(tenantID, timestamp, fields, streamFieldsLen)  // call for each log entry
//	storage.MustAddRows(lr)                                     // flush to storage
//	PutLogRows(lr)                                              // return to pool
//
// THREAD SAFETY: LogRows is NOT thread-safe. Each goroutine should obtain
// its own LogRows from the pool.
//
// MEMORY MANAGEMENT: All string data (field names, values, stream tags) is
// copied into an internal arena buffer. This allows callers to reuse their
// input buffers immediately after MustAdd() returns.
type LogRows struct {
	// a is the arena allocator that holds all string data for this batch.
	// All field names, values, and stream tags are copied here to ensure
	// the data remains valid even after caller's buffers are reused.
	a arena

	// fieldsBuf holds all Field structures for all rows.
	// Each row's fields are a slice into this buffer.
	// This contiguous allocation improves cache efficiency during processing.
	fieldsBuf []Field

	// streamIDs holds the computed stream ID for each row.
	// The stream ID is a 128-bit hash of the canonical stream tags.
	// Index: corresponds to rows array index.
	streamIDs []streamID

	// timestamps holds the Unix nanosecond timestamp for each row.
	// Index: corresponds to rows array index.
	timestamps []int64

	// rows holds the field slices for each log entry.
	// Each element is a slice into fieldsBuf.
	// Index: row index for accessing corresponding streamID and timestamp.
	rows [][]Field

	// streamTagsCanonicals holds the canonical serialized form of stream tags.
	// This is the sorted, standardized representation like: {app="nginx",host="server1"}
	// Used for registering new streams in indexdb and for logging/debugging.
	streamTagsCanonicals []string

	// ==================== Configuration Fields ====================
	// These are set by GetLogRows() and control how log entries are processed.

	// streamFields contains field names that should be treated as stream tags.
	// Stream tags identify the log stream and are indexed for fast filtering.
	// Example: ["app", "host", "environment"]
	streamFields []string

	// ignoreFields is a prefix filter for fields to skip during ingestion.
	// Matching fields are neither stored nor indexed.
	// Useful for filtering out noisy or sensitive fields.
	ignoreFields prefixfilter.Filter

	// decolorizeFields is a prefix filter for fields that should have
	// ANSI color escape sequences stripped. Useful for logs that contain
	// terminal color codes which break JSON formatting.
	decolorizeFields prefixfilter.Filter

	// extraFields are additional fields to add to every log entry.
	// These override any fields with the same name in the input.
	// Useful for adding metadata like hostname, ingestion source, etc.
	extraFields []Field

	// extraStreamFields are extra fields that should also be stream tags.
	// These are added to stream tags AND stored as regular fields.
	extraStreamFields []Field

	// defaultMsgValue is the default value for the _msg field if not provided.
	// If empty, no default _msg is added.
	defaultMsgValue string
}

// logRows is the internal representation used by datadb for batch processing.
// It's simpler than LogRows because it doesn't need the ingestion configuration -
// just the raw data ready to be written to parts.
//
// This type is used during the flush process when in-memory data is converted
// to searchable parts. The separation from LogRows allows the ingestion path
// to continue using LogRows while flush operates on logRows.
type logRows struct {
	// a holds all string data (arena allocator)
	a arena

	// fieldsBuf holds all Field structures
	fieldsBuf []Field

	// streamIDs holds stream IDs for each row
	streamIDs []streamID

	// timestamps holds timestamps for each row
	timestamps []int64

	// rows holds field slices for each log entry
	rows [][]Field

	// sf is a helper for sorting fields within each row.
	// Reused to avoid allocations during sort operations.
	sf sortedFields
}

// ==================== logRows Methods ====================
//
// logRows is the internal type used by datadb during the flush process.

// reset clears all data in logRows, preparing it for reuse.
// This is called when returning logRows to the pool.
//
// The reset is thorough to allow garbage collection of referenced data:
// - Arena buffer is cleared
// - Field values are zeroed (to release string references)
// - StreamIDs are zeroed (to release tenant ID references)
// - Slices are truncated but capacity is preserved for reuse
func (lr *logRows) reset() {
	lr.a.reset()

	fb := lr.fieldsBuf
	for i := range fb {
		fb[i].Reset()
	}
	lr.fieldsBuf = fb[:0]

	sids := lr.streamIDs
	for i := range sids {
		sids[i].reset()
	}
	lr.streamIDs = sids[:0]

	lr.timestamps = lr.timestamps[:0]

	clear(lr.rows)
	lr.rows = lr.rows[:0]

	lr.sf = nil
}

// needFlush returns true if the logRows buffer is approaching capacity.
// This triggers a flush to prevent the buffer from growing too large.
//
// The threshold is 7/8 of the maximum uncompressed block size, providing
// headroom for additional entries before hitting the hard limit.
func (lr *logRows) needFlush() bool {
	return len(lr.a.b) > (maxUncompressedBlockSize/8)*7
}

// mustAddRows copies all rows from src (LogRows) into lr (logRows).
// This is called during the flush process to prepare data for writing to parts.
//
// The function iterates through each row in src and adds it to lr,
// copying field data into lr's arena allocator.
func (lr *logRows) mustAddRows(src *LogRows) {
	streamIDs := src.streamIDs
	timestamps := src.timestamps
	rows := src.rows

	if len(rows) == 0 {
		return
	}

	// a hint for the compiler for preventing from unnecessary bounds checks
	_ = streamIDs[len(rows)-1]
	_ = timestamps[len(rows)-1]

	for i := range rows {
		lr.mustAddRow(streamIDs[i], timestamps[i], rows[i])
	}
}

// mustAddRow adds a single log entry to logRows.
//
// This function:
//  1. Appends the stream ID and timestamp to their respective slices
//  2. Grows fieldsBuf to accommodate the new fields
//  3. Copies each field's name and value into the arena
//  4. Stores a slice reference to the new fields in rows
//
// ARENA ALLOCATION OPTIMIZATION:
// When consecutive rows have identical field names or values, we reuse
// the same string from the arena instead of copying duplicates. This
// significantly reduces memory usage for logs with repetitive field names.
func (lr *logRows) mustAddRow(streamID streamID, timestamp int64, fields []Field) {
	lr.streamIDs = append(lr.streamIDs, streamID)
	lr.timestamps = append(lr.timestamps, timestamp)

	fieldsBuf := lr.fieldsBuf
	fieldsBufLen := len(fieldsBuf)
	fieldsBuf = slicesutil.SetLength(fieldsBuf, fieldsBufLen+len(fields))
	dstFields := fieldsBuf[fieldsBufLen:]
	lr.fieldsBuf = fieldsBuf
	lr.rows = append(lr.rows, dstFields)

	for i := range fields {
		f := &fields[i]

		fieldName := getCanonicalFieldName(f.Name)

		dstField := &dstFields[i]
		if len(fieldsBuf) >= len(fields) {
			fPrev := &fieldsBuf[len(fieldsBuf)-len(fields)]
			if fPrev.Name == fieldName {
				dstField.Name = fPrev.Name
			} else {
				dstField.Name = lr.a.copyString(fieldName)
			}
			if fPrev.Value == f.Value {
				dstField.Value = fPrev.Value
			} else {
				dstField.Value = lr.a.copyString(f.Value)
			}
		} else {
			dstField.Name = lr.a.copyString(fieldName)
			dstField.Value = lr.a.copyString(f.Value)
		}
	}
}

// ==================== Sorting Interface ====================
//
// logRows implements sort.Interface to enable sorting by (streamID, timestamp).
// This ordering is crucial for efficient storage: rows are grouped by stream
// and ordered by time within each stream, improving compression and query speed.

// Len returns the number of log entries.
func (lr *logRows) Len() int {
	return len(lr.streamIDs)
}

// Less returns true if row i should come before row j.
// Sort order: streamID first (group by stream), then timestamp (chronological).
func (lr *logRows) Less(i, j int) bool {
	a := &lr.streamIDs[i]
	b := &lr.streamIDs[j]
	if !a.equal(b) {
		return a.less(b)
	}
	return lr.timestamps[i] < lr.timestamps[j]
}

// Swap exchanges rows i and j.
func (lr *logRows) Swap(i, j int) {
	a := &lr.streamIDs[i]
	b := &lr.streamIDs[j]
	*a, *b = *b, *a

	tsA, tsB := &lr.timestamps[i], &lr.timestamps[j]
	*tsA, *tsB = *tsB, *tsA

	fieldsA, fieldsB := &lr.rows[i], &lr.rows[j]
	*fieldsA, *fieldsB = *fieldsB, *fieldsA
}

// sortFieldsInRows sorts the fields within each row by field name.
// Sorted fields improve compression and enable binary search during queries.
func (lr *logRows) sortFieldsInRows() {
	for _, row := range lr.rows {
		lr.sf = row
		sort.Sort(&lr.sf)
	}
}

// sortedFields implements sort.Interface for sorting fields by name.
type sortedFields []Field

func (sf *sortedFields) Len() int {
	return len(*sf)
}

func (sf *sortedFields) Less(i, j int) bool {
	a := *sf
	return a[i].Name < a[j].Name
}

func (sf *sortedFields) Swap(i, j int) {
	a := *sf
	a[i], a[j] = a[j], a[i]
}

// ==================== Pool Management ====================

func getLogRows() *logRows {
	v := lrPool.Get()
	if v == nil {
		return &logRows{}
	}
	return v.(*logRows)
}

func putLogRows(lr *logRows) {
	lr.reset()
	lrPool.Put(lr)
}

var lrPool sync.Pool

// ==================== LogRows Methods ====================
//
// LogRows is the public type used during ingestion.

// ForEachRow iterates over all rows in LogRows, calling the callback for each.
// This is used by the native ingestion protocol to process rows one at a time.
//
// The callback receives:
//   - streamHash: A 64-bit hash derived from the stream ID (used for sharding)
//   - r: An InsertRow populated with the row's data (borrowed from pool, don't retain)
//
// NOTE: The callback should not retain references to r.Fields after returning,
// as the InsertRow is reused.
func (lr *LogRows) ForEachRow(callback func(streamHash uint64, r *InsertRow)) {
	r := GetInsertRow()
	for i, timestamp := range lr.timestamps {
		sid := &lr.streamIDs[i]

		// Create a 64-bit hash from the 128-bit stream ID for sharding
		streamHash := sid.id.lo ^ sid.id.hi

		r.TenantID = sid.tenantID
		r.StreamTagsCanonical = lr.streamTagsCanonicals[i]
		r.Timestamp = timestamp
		r.Fields = lr.rows[i]

		callback(streamHash, r)
	}
	// remove reference to logRows fields
	// since reset of r can modify actual LogRows
	r.Fields = nil
	PutInsertRow(r)
}

// Reset clears all data AND configuration in LogRows.
// Use ResetKeepSettings() if you want to clear data but keep the configuration.
func (lr *LogRows) Reset() {
	lr.ResetKeepSettings()

	lr.streamFields = lr.streamFields[:0]

	lr.ignoreFields.Reset()
	lr.decolorizeFields.Reset()

	lr.extraFields = nil

	clear(lr.extraStreamFields)
	lr.extraStreamFields = lr.extraStreamFields[:0]

	lr.defaultMsgValue = ""
}

// RowsCount returns the current number of log entries in LogRows.
func (lr *LogRows) RowsCount() int {
	return len(lr.rows)
}

// ResetKeepSettings clears all row data but preserves configuration
// (streamFields, ignoreFields, decolorizeFields, extraFields, defaultMsgValue).
// Use this when reusing LogRows with the same configuration for multiple batches.
func (lr *LogRows) ResetKeepSettings() {
	lr.a.reset()

	fb := lr.fieldsBuf
	for i := range fb {
		fb[i].Reset()
	}
	lr.fieldsBuf = fb[:0]

	sids := lr.streamIDs
	for i := range sids {
		sids[i].reset()
	}
	lr.streamIDs = sids[:0]

	clear(lr.streamTagsCanonicals)
	lr.streamTagsCanonicals = lr.streamTagsCanonicals[:0]

	lr.timestamps = lr.timestamps[:0]

	clear(lr.rows)
	lr.rows = lr.rows[:0]
}

// NeedFlush returns true if LogRows has accumulated enough data to trigger a flush.
// This is based on:
//   - Arena size approaching the maximum block size
//   - Number of rows exceeding a threshold
//
// Proactive flushing prevents memory pressure and ensures data is persisted promptly.
func (lr *LogRows) NeedFlush() bool {
	return len(lr.a.b) > (maxUncompressedBlockSize/8)*7 || len(lr.rows) > maxUncompressedBlockSize/100
}

// MustAddInsertRow adds an InsertRow to LogRows (used by native protocol).
// It validates the streamTagsCanonical and extracts the stream ID before storing.
//
// Invalid rows are skipped with a warning log message (not an error).
// This fail-soft behavior ensures one bad log entry doesn't block ingestion of others.
func (lr *LogRows) MustAddInsertRow(r *InsertRow) {
	// Verify streamTagsCanonical is valid canonical format
	st := GetStreamTags()
	streamTagsCanonical := bytesutil.ToUnsafeBytes(r.StreamTagsCanonical)
	tail, err := st.UnmarshalCanonical(streamTagsCanonical)
	if err != nil {
		line := MarshalFieldsToJSON(nil, r.Fields)
		logger.Warnf("cannot unmarshal streamTagsCanonical: %w; skipping the log entry; log entry: %s", err, line)
		return
	}
	if len(tail) > 0 {
		line := MarshalFieldsToJSON(nil, r.Fields)
		logger.Warnf("unexpected tail left after unmarshaling streamTagsCanonical; len(tail)=%d; streamTags: %s; log entry: %s", len(tail), st, line)
		return
	}

	// TODO: verify that all the stream tags match the corresponding log fields in r.Fields?
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/38

	PutStreamTags(st)

	// Calculate 128-bit stream ID from stream tags
	var sid streamID
	sid.tenantID = r.TenantID
	sid.id = hash128(streamTagsCanonical)

	// Store the row
	lr.mustAddInternal(sid, r.Timestamp, r.Fields, r.StreamTagsCanonical)
}

// mustAdd is a simpler version of MustAdd that uses default stream fields configuration.
func (lr *LogRows) mustAdd(tenantID TenantID, timestamp int64, fields []Field) {
	lr.MustAdd(tenantID, timestamp, fields, -1)
}

// MustAdd adds a log entry to LogRows.
//
// PARAMETERS:
//   - tenantID: The tenant identifier (for multi-tenancy)
//   - timestamp: Unix nanosecond timestamp of the log entry
//   - fields: Key-value pairs representing the log data
//   - streamFieldsLen: If >= 0, use the first N fields as stream tags instead of
//     pre-configured stream fields. Use -1 for default behavior.
//
// VALIDATION:
// The log entry is validated against limits. Invalid entries are logged as warnings
// and skipped (not added). This prevents one bad entry from breaking ingestion.
// Limits include:
//   - Maximum number of fields per entry
//   - Maximum field name length
//   - Maximum total entry size
//
// THREAD SAFETY: MustAdd is NOT thread-safe. Each LogRows should be used by one goroutine.
func (lr *LogRows) MustAdd(tenantID TenantID, timestamp int64, fields []Field, streamFieldsLen int) {
	// ==================== Validation ====================

	// Check field count limit
	if len(fields) > maxColumnsPerBlock {
		line := MarshalFieldsToJSON(nil, fields)
		logger.Warnf("ignoring log entry with too big number of fields %d, since it exceeds the limit %d; "+
			"see https://docs.victoriametrics.com/victorialogs/faq/#how-many-fields-a-single-log-entry-may-contain ; log entry: %s", len(fields), maxColumnsPerBlock, line)
		return
	}

	// Check field name length limits
	for i := range fields {
		fieldName := fields[i].Name
		if len(fieldName) > maxFieldNameSize {
			line := MarshalFieldsToJSON(nil, fields)
			logger.Warnf("ignoring log entry with too long field name %q, since its length (%d) exceeds the limit %d bytes; "+
				"see https://docs.victoriametrics.com/victorialogs/faq/#what-is-the-maximum-supported-field-name-length ; log entry: %s",
				fieldName, len(fieldName), maxFieldNameSize, line)
			return
		}
	}

	// Check total entry size
	rowLen := EstimatedJSONRowLen(fields)
	if rowLen > maxUncompressedBlockSize {
		line := MarshalFieldsToJSON(nil, fields)
		logger.Warnf("ignoring too long log entry with the estimated length of %d bytes, since it exceeds the limit %d bytes; "+
			"see https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have ; log entry: %s", rowLen, maxUncompressedBlockSize, line)
		return
	}

	// ==================== Stream Tags Extraction ====================

	// Build stream tags from the appropriate fields
	st := GetStreamTags()
	if streamFieldsLen >= 0 {
		// Use the first streamFieldsLen fields as stream tags
		for _, f := range fields[:streamFieldsLen] {
			fieldName := getCanonicalFieldName(f.Name)
			if !lr.ignoreFields.MatchString(fieldName) {
				st.Add(fieldName, f.Value)
			}
		}
	} else {
		// Use pre-configured streamFields
		for _, f := range fields {
			fieldName := getCanonicalFieldName(f.Name)
			if slices.Contains(lr.streamFields, fieldName) {
				st.Add(fieldName, f.Value)
			}
		}
		// Add extra stream fields (e.g., global labels from config)
		for _, f := range lr.extraStreamFields {
			fieldName := getCanonicalFieldName(f.Name)
			st.Add(fieldName, f.Value)
		}
	}

	// Serialize stream tags to canonical form
	bb := bbPool.Get()
	bb.B = st.MarshalCanonical(bb.B)
	PutStreamTags(st)

	// Calculate 128-bit stream ID from canonical stream tags
	var sid streamID
	sid.tenantID = tenantID
	sid.id = hash128(bb.B)

	// Store the row
	streamTagsCanonical := bytesutil.ToUnsafeString(bb.B)
	lr.mustAddInternal(sid, timestamp, fields, streamTagsCanonical)
	bbPool.Put(bb)
}

// mustAddInternal is the internal method that actually stores a row in LogRows.
// It handles:
//   - Copying stream tags to arena (with deduplication for consecutive identical tags)
//   - Adding stream ID and timestamp
//   - Processing and copying fields (applying ignore/decolorize filters)
//   - Adding extra fields and default _msg if configured
func (lr *LogRows) mustAddInternal(sid streamID, timestamp int64, fields []Field, streamTagsCanonical string) {
	// Store stream tags with deduplication optimization
	stcs := lr.streamTagsCanonicals
	if len(stcs) > 0 && string(stcs[len(stcs)-1]) == streamTagsCanonical {
		// Reuse previous stream tags string (common for consecutive logs from same stream)
		stcs = append(stcs, stcs[len(stcs)-1])
	} else {
		// Copy new stream tags to arena
		streamTagsCanonicalCopy := lr.a.copyString(streamTagsCanonical)
		stcs = append(stcs, streamTagsCanonicalCopy)
	}
	lr.streamTagsCanonicals = stcs

	lr.streamIDs = append(lr.streamIDs, sid)
	lr.timestamps = append(lr.timestamps, timestamp)

	// Process input fields
	fieldsLen := len(lr.fieldsBuf)
	hasMsgField := lr.addFieldsInternal(fields, &lr.ignoreFields, &lr.decolorizeFields, true)

	// Add extra fields (these override input fields with same name)
	if lr.addFieldsInternal(lr.extraFields, nil, nil, false) {
		hasMsgField = true
	}

	// Add default _msg field if not present and configured
	if !hasMsgField && lr.defaultMsgValue != "" {
		lr.fieldsBuf = append(lr.fieldsBuf, Field{
			Value: lr.defaultMsgValue,
		})
	}

	// Record the slice of fields for this row
	row := lr.fieldsBuf[fieldsLen:]
	lr.rows = append(lr.rows, row)
}

// addFieldsInternal adds fields to fieldsBuf, applying filters and arena allocation.
// Returns true if a _msg field (empty name) was added.
func (lr *LogRows) addFieldsInternal(fields []Field, ignoreFields, decolorizeFields *prefixfilter.Filter, mustCopyFields bool) bool {
	if len(fields) == 0 {
		return false
	}

	// Look at previous row for potential string reuse
	var prevRow []Field
	if len(lr.rows) > 0 {
		prevRow = lr.rows[len(lr.rows)-1]
	}

	fb := lr.fieldsBuf
	hasMsgField := false
	for i := range fields {
		f := &fields[i]

		fieldName := getCanonicalFieldName(f.Name)

		// Skip ignored fields
		if ignoreFields.MatchString(fieldName) {
			continue
		}
		// Skip fields with empty values (VictoriaLogs data model treats empty as non-existent)
		if f.Value == "" {
			continue
		}

		// Look for matching field in previous row for string reuse
		var prevField *Field
		if prevRow != nil && i < len(prevRow) {
			prevField = &prevRow[i]
		}

		fb = append(fb, Field{})
		dstField := &fb[len(fb)-1]

		// Track if this is the _msg field (stored with empty name internally)
		if fieldName == "" {
			hasMsgField = true
		}

		// Copy or reuse field name
		if prevField != nil && prevField.Name == fieldName {
			dstField.Name = prevField.Name
		} else {
			if mustCopyFields {
				dstField.Name = lr.a.copyString(fieldName)
			} else {
				dstField.Name = fieldName
			}
			prevRow = nil // Can't reuse anymore
		}

		// Copy or reuse field value
		if prevField != nil && prevField.Value == f.Value {
			dstField.Value = prevField.Value
		} else {
			if mustCopyFields {
				dstField.Value = lr.a.copyString(f.Value)
			} else {
				dstField.Value = f.Value
			}

			// Strip ANSI color sequences if configured for this field
			if decolorizeFields.MatchString(fieldName) && hasColorSequences(dstField.Value) {
				bLen := len(lr.a.b)
				lr.a.b = dropColorSequences(lr.a.b, dstField.Value)
				dstField.Value = bytesutil.ToUnsafeString(lr.a.b[bLen:])
			}
		}
	}
	lr.fieldsBuf = fb

	return hasMsgField
}

// ==================== Field Name Utilities ====================

// getCanonicalFieldName converts "_msg" to "" for internal storage.
// The _msg field is special - it's stored with an empty name internally
// but displayed as "_msg" in queries and exports.
func getCanonicalFieldName(fieldName string) string {
	if fieldName == "_msg" {
		return ""
	}
	return fieldName
}

// getCanonicalColumnName converts "" back to "_msg" for display.
// This is the inverse of getCanonicalFieldName.
func getCanonicalColumnName(fieldName string) string {
	if fieldName == "" {
		return "_msg"
	}
	return fieldName
}

// GetRowString returns a human-readable JSON representation of a row.
// Used for logging and debugging.
func (lr *LogRows) GetRowString(idx int) string {
	tf := TimeFormatter(lr.timestamps[idx])
	streamTags := getStreamTagsString(lr.streamTagsCanonicals[idx])
	var fields []Field
	fields = append(fields[:0], lr.rows[idx]...)
	fields = append(fields, Field{
		Name:  "_time",
		Value: tf.String(),
	})
	fields = append(fields, Field{
		Name:  "_stream",
		Value: streamTags,
	})
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	line := MarshalFieldsToJSON(nil, fields)
	return string(line)
}

// ==================== LogRows Pool ====================

// GetLogRows returns a LogRows from the pool, configured with the given settings.
//
// PARAMETERS:
//   - streamFields: Field names to use as stream tags (identifies the log stream)
//   - ignoreFields: Field name prefixes to skip during ingestion
//   - decolorizeFields: Field name prefixes to strip ANSI color codes from
//   - extraFields: Additional fields to add to every log entry
//   - defaultMsgValue: Default value for _msg field if not provided
//
// IMPORTANT: Call PutLogRows() when done to return to the pool.
func GetLogRows(streamFields, ignoreFields, decolorizeFields []string, extraFields []Field, defaultMsgValue string) *LogRows {
	v := logRowsPool.Get()
	if v == nil {
		v = &LogRows{}
	}
	lr := v.(*LogRows)

	// Initialize ignoreFields filter
	for _, f := range ignoreFields {
		f = getCanonicalFieldName(f)
		lr.ignoreFields.AddAllowFilter(f)
	}
	for _, f := range extraFields {
		// Extra fields override input fields, so ignore input fields with same names
		fieldName := getCanonicalFieldName(f.Name)
		lr.ignoreFields.AddAllowFilter(fieldName)
	}

	// Initialize decolorizeFields filter
	for _, f := range decolorizeFields {
		f = getCanonicalFieldName(f)
		lr.decolorizeFields.AddAllowFilter(f)
	}

	// Initialize streamFields (excluding ignored ones)
	for _, f := range streamFields {
		f = getCanonicalFieldName(f)
		if !lr.ignoreFields.MatchString(f) {
			lr.streamFields = append(lr.streamFields, f)
		}
	}

	// Initialize extraStreamFields (extra fields that are also stream tags)
	for _, f := range extraFields {
		fieldName := getCanonicalFieldName(f.Name)
		if slices.Contains(streamFields, fieldName) {
			lr.extraStreamFields = append(lr.extraStreamFields, f)
			lr.streamFields = slices.DeleteFunc(lr.streamFields, func(s string) bool { return s == fieldName })
		}
	}

	lr.extraFields = extraFields
	lr.defaultMsgValue = defaultMsgValue

	return lr
}

// PutLogRows returns a LogRows to the pool for reuse.
func PutLogRows(lr *LogRows) {
	lr.Reset()
	logRowsPool.Put(lr)
}

var logRowsPool sync.Pool

// ==================== Utility Functions ====================

// EstimatedJSONRowLen estimates the JSON representation size of a log entry.
// This is used for validation to reject overly large entries.
//
// The calculation must stay in sync with block.uncompressedSizeBytes().
func EstimatedJSONRowLen(fields []Field) int {
	n := len("{}\n")
	n += len(`"_time":""`) + len(time.RFC3339Nano)
	for _, f := range fields {
		// VictoriaLogs data model treats empty values as non-existing values
		if f.Value == "" {
			continue
		}

		name := getCanonicalColumnName(f.Name)
		n += estimatedJSONFieldLen(name, f.Value)
	}
	return n
}

// estimatedJSONFieldLen estimates the JSON size of a single field.
func estimatedJSONFieldLen(name, value string) int {
	return len(`,"":""`) + len(name) + len(value)
}

// ==================== InsertRow ====================
//
// InsertRow is used by the native ingestion protocol.
// It represents a single log entry with pre-computed stream tags.

// GetInsertRow returns an InsertRow from the pool.
func GetInsertRow() *InsertRow {
	v := insertRowsPool.Get()
	if v == nil {
		return &InsertRow{}
	}
	return v.(*InsertRow)
}

// PutInsertRow returns an InsertRow to the pool.
func PutInsertRow(r *InsertRow) {
	r.Reset()
	insertRowsPool.Put(r)
}

var insertRowsPool sync.Pool

// InsertRow represents a log entry for the native ingestion protocol.
// Unlike the regular MustAdd path, this has pre-computed streamTagsCanonical.
type InsertRow struct {
	TenantID            TenantID // Tenant identifier
	StreamTagsCanonical string   // Pre-formatted stream tags (e.g., `{app="nginx",host="server1"}`)
	Timestamp           int64    // Unix nanosecond timestamp
	Fields              []Field  // Log fields (key-value pairs)
}

// Reset clears all fields in InsertRow.
func (r *InsertRow) Reset() {
	r.TenantID.Reset()
	r.StreamTagsCanonical = ""
	r.Timestamp = 0

	clear(r.Fields)
	r.Fields = r.Fields[:0]
}

// Marshal serializes InsertRow to binary format for native protocol.
func (r *InsertRow) Marshal(dst []byte) []byte {
	dst = r.TenantID.marshal(dst)
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(r.StreamTagsCanonical))
	dst = encoding.MarshalUint64(dst, uint64(r.Timestamp))
	dst = encoding.MarshalVarUint64(dst, uint64(len(r.Fields)))
	for _, field := range r.Fields {
		dst = field.marshal(dst, true)
	}
	return dst
}

// UnmarshalInplace deserializes InsertRow from binary format.
// The returned InsertRow references data in src - do not modify src while using r.
func (r *InsertRow) UnmarshalInplace(src []byte) ([]byte, error) {
	srcOrig := src

	tail, err := r.TenantID.unmarshal(src)
	if err != nil {
		return srcOrig, fmt.Errorf("cannot unmarshal tenantID: %w", err)
	}
	src = tail

	streamTagsCanonical, n := encoding.UnmarshalBytes(src)
	if n <= 0 {
		return srcOrig, fmt.Errorf("cannot unmarshal streamTagCanonical")
	}
	r.StreamTagsCanonical = bytesutil.ToUnsafeString(streamTagsCanonical)
	src = src[n:]

	if len(src) < 8 {
		return srcOrig, fmt.Errorf("cannot unmarshal timestamp")
	}
	timestamp := encoding.UnmarshalUint64(src)
	r.Timestamp = int64(timestamp)
	src = src[8:]

	fieldsLen, n := encoding.UnmarshalVarUint64(src)
	if n <= 0 {
		return srcOrig, fmt.Errorf("cannot unmarshal the number of fields")
	}
	if fieldsLen > maxColumnsPerBlock {
		return srcOrig, fmt.Errorf("too many fields in the log entry: %d; mustn't exceed %d", fieldsLen, maxColumnsPerBlock)
	}
	src = src[n:]

	r.Fields = slicesutil.SetLength(r.Fields, int(fieldsLen))
	for i := range r.Fields {
		tail, err = r.Fields[i].unmarshalInplace(src, true)
		if err != nil {
			return srcOrig, fmt.Errorf("cannot unmarshal field #%d: %w", i, err)
		}
		src = tail
	}

	return src, nil
}
