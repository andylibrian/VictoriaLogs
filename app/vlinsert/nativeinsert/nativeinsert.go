// Package nativeinsert implements the /insert/native HTTP endpoint.
//
// This endpoint accepts log data in VictoriaLogs' native binary format - the same format
// used internally for cluster communication (/internal/insert). It's the most efficient
// way to ingest data when you control both the sender and receiver.
//
// NATIVE vs /internal/insert:
// Both endpoints use the same binary format, but they differ in how they handle metadata:
//
//	/insert/native:
//	  - Public endpoint for external clients (vlagent, custom integrations)
//	  - Tenant ID: From AccountID/ProjectID HTTP headers (NOT from payload)
//	  - Supports limited CommonParams options (tenant override, ignore_fields, extra_fields, debug)
//	  - Ignores field-mapping options (_time_field, _msg_field, _stream_fields, decolorize_fields)
//	  - Use case: vlagent sending logs to a single VictoriaLogs instance
//
//	/internal/insert:
//	  - Internal endpoint for cluster communication
//	  - Tenant ID: From payload (each row carries its own tenant)
//	  - No field mapping options (data is pre-formatted by vlinsert)
//	  - Use case: vlinsert frontend → vlstorage node communication
//
// PROTOCOL:
//   - Method: POST
//   - URL: /insert/native?version=v1
//   - Content-Type: application/octet-stream
//   - Content-Encoding: zstd (optional, decompressed automatically)
//   - Body: Concatenated binary InsertRow records
//
// Use cases for /insert/native:
//   - vlagent forwarding logs to VictoriaLogs
//   - Custom integrations that want maximum efficiency
//   - Bulk data migration between VictoriaLogs instances
package nativeinsert

import (
	"fmt"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	maxRequestSize = flagutil.NewBytes("nativeinsert.maxRequestSize", 64*1024*1024, "The maximum size in bytes of a single request, which can be accepted at /insert/native HTTP endpoint")
)

// RequestHandler processes /insert/native HTTP requests.
//
// VALIDATION SEQUENCE:
//  1. Verify POST method
//  2. Verify protocol version matches netinsert.ProtocolVersion ("v1")
//  3. Extract CommonParams (tenant, stream fields, ignore fields, etc.)
//  4. Check if storage can accept writes
//
// FIELD MAPPING OPTIONS:
// Several CommonParams options are NOT supported for native insert because the
// binary payload already contains formatted data:
//   - _time_field: Timestamps are in the payload
//   - _msg_field: Message fields are in the payload
//   - _stream_fields: Stream fields are pre-computed in the payload
//   - decolorize_fields: Color codes should be stripped before sending
//
// These options are logged as warnings and ignored.
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Protocol version check - prevents silent data corruption from format mismatches
	version := r.FormValue("version")
	if version != netinsert.ProtocolVersion {
		httpserver.Errorf(w, r, "unsupported protocol version=%q; want %q", version, netinsert.ProtocolVersion)
		return
	}

	requestsTotal.Inc()

	// Extract HTTP parameters (tenant, stream fields, ignore fields, etc.)
	cp, err := insertutil.GetCommonParams(r)
	if err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Check if storage can accept writes (e.g., not in read-only mode due to disk full)
	if err := insertutil.CanWriteData(); err != nil {
		httpserver.Errorf(w, r, "%s", err)
		return
	}

	// Native protocol doesn't support these field mapping options - warn and ignore them.
	// These are logged with throttling to avoid log spam if a client repeatedly sends them.
	if cp.IsTimeFieldSet {
		unsupportedOptionsLogger.Warnf("/insert/native endpoint doesn't support setting time fields via _time_field query arg and via VL-Time-Field request header; "+
			"ignoring them; timeFields=%q", cp.TimeFields)
	}
	// Unconditionally reset cp.TimeFields, since the code below shouldn't depend on this field.
	cp.TimeFields = nil

	if len(cp.MsgFields) > 0 {
		unsupportedOptionsLogger.Warnf("/insert/native endpoint doesn't support setting msg fields via _msg_field query arg and via VL-Msg-Field request header; "+
			"ignoring them; msgFields=%q", cp.MsgFields)
		cp.MsgFields = nil
	}
	if len(cp.StreamFields) > 0 {
		unsupportedOptionsLogger.Warnf("/insert/native endpoint doesn't support setting stream fields via _stream_fields query arg and via VL-Stream-Fields request header; "+
			"ignoring them; streamFields=%q", cp.StreamFields)
		cp.StreamFields = nil
	}
	if len(cp.DecolorizeFields) > 0 {
		unsupportedOptionsLogger.Warnf("/insert/native endpoint doesn't support setting decolorize_fields query arg and VL-Decolorize-Fields request header; "+
			"ignoring them; decolorizeFields=%q", cp.DecolorizeFields)
		cp.DecolorizeFields = nil
	}

	// Read and decompress the request body, then parse the binary rows
	encoding := r.Header.Get("Content-Encoding")
	err = protoparserutil.ReadUncompressedData(r.Body, encoding, maxRequestSize, func(data []byte) error {
		// Create a processor for this request
		lmp := cp.NewLogMessageProcessor("nativeinsert", false)

		// Cast to InsertRowProcessor - the native protocol uses pre-formatted InsertRows
		irp := lmp.(insertutil.InsertRowProcessor)

		err := parseData(irp, data, cp.TenantID)
		lmp.MustClose()
		return err
	})
	if err != nil {
		errorsTotal.Inc()
		httpserver.Errorf(w, r, "cannot parse native insert request: %s", err)
		return
	}

	requestDuration.UpdateDuration(startTime)
}

// unsupportedOptionsLogger uses a throttled logger to avoid spamming logs when
// a client repeatedly sends unsupported options. Messages are suppressed if
// the same message was logged within the last 5 seconds.
var unsupportedOptionsLogger = logger.WithThrottler("unsuppoted_options", 5*time.Second)

// parseData deserializes binary InsertRow records from data and adds them to irp.
//
// TENANT HANDLING:
// The /insert/native endpoint enforces tenant isolation at the HTTP layer:
//   - Tenant is extracted from AccountID/ProjectID HTTP headers (in RequestHandler)
//   - Any tenant in the binary payload is OVERWRITTEN with the HTTP header tenant
//   - This prevents a client from writing to another tenant's data
//
// For /internal/insert (cluster-internal), the tenant comes from the payload because
// the vlinsert frontend has already validated and determined the correct tenant.
func parseData(irp insertutil.InsertRowProcessor, data []byte, tenantID logstorage.TenantID) error {
	var zeroTenantID logstorage.TenantID

	// Get an InsertRow from the pool for reuse
	r := logstorage.GetInsertRow()
	defer logstorage.PutInsertRow(r)

	src := data
	i := 0
	for len(src) > 0 {
		// Parse the next InsertRow from the binary data
		tail, err := r.UnmarshalInplace(src)
		if err != nil {
			return fmt.Errorf("cannot parse row #%d: %s", i, err)
		}
		src = tail
		i++

		// Security check: warn if payload contains a different tenant than headers.
		// This could indicate misconfiguration or an attempt to write to another tenant.
		if !r.TenantID.Equal(&zeroTenantID) && !r.TenantID.Equal(&tenantID) {
			invalidTenantIDLogger.Warnf("use %q from AccountID and ProjectID request headers as tenantID for the log entry instead of %q; "+
				"see https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy ; "+
				"log entry: %s", tenantID, r.TenantID, logstorage.MarshalFieldsToJSON(nil, r.Fields))
		}

		// Override tenant with the one from HTTP headers (security isolation)
		r.TenantID = tenantID

		// Add the row to the processor
		irp.AddInsertRow(r)
	}

	return nil
}

// invalidTenantIDLogger warns when payload tenant differs from header tenant.
// Throttled to avoid log spam from misconfigured clients.
var invalidTenantIDLogger = logger.WithThrottler("invalid_tenant_id", 5*time.Second)

// Metrics for monitoring the /insert/native endpoint
var (
	requestsTotal = metrics.NewCounter(`vl_http_requests_total{path="/insert/native"}`)
	errorsTotal   = metrics.NewCounter(`vl_http_errors_total{path="/insert/native"}`)

	requestDuration = metrics.NewSummary(`vl_http_request_duration_seconds{path="/insert/native"}`)
)
