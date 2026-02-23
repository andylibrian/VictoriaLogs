// Package vlinsert implements the data ingestion layer for VictoriaLogs.
//
// This package is the HTTP frontend for all log ingestion. It routes incoming requests
// to protocol-specific handlers based on the URL path.
//
// ARCHITECTURE:
//
//	HTTP Request → vlinsert.RequestHandler()
//	                   ↓
//	          URL path routing
//	                   ↓
//	   ┌───────────────┼───────────────┐
//	   ↓               ↓               ↓
//	/insert/jsonline  /insert/elasticsearch/*  /insert/loki/*
//	/insert/native    /insert/opentelemetry/*  /insert/datadog/*
//	/internal/insert  /insert/journald/*
//	   ↓               ↓               ↓
//	   └───────────────┼───────────────┘
//	                   ↓
//	          insertutil.LogMessageProcessor
//	                   ↓
//	          vlstorage.MustAddRows()
//
// Syslog ingestion doesn't go through HTTP routing above. It is handled by
// dedicated listeners initialized via syslog.MustInit().
//
// PROTOCOL HANDLERS:
// Each protocol handler (jsonline, elasticsearch, loki, etc.) is in its own package
// under app/vlinsert/. They all follow the same pattern:
//  1. Parse the incoming request format
//  2. Extract CommonParams (tenant, stream fields, etc.)
//  3. Create a LogMessageProcessor
//  4. Add rows to the processor
//  5. Close the processor to flush data
//
// SECURITY:
//   - -insert.disable: Disables all /insert/* endpoints
//   - -internalinsert.disable: Disables /internal/insert (cluster-internal endpoint)
//
// See onboarding/onboarding-insert-flow.md for detailed ingestion flow documentation.
package vlinsert

import (
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/datadog"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/elasticsearch"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/internalinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/journald"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/jsonline"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/loki"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/nativeinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/opentelemetry"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/syslog"
)

var (
	// disableInsert controls whether all /insert/* endpoints are disabled.
	// When true, clients receive an error when trying to ingest data.
	// Useful for maintenance or read-only deployments.
	disableInsert = flag.Bool("insert.disable", false, "Whether to disable /insert/* HTTP endpoints")

	// disableInternalInsert controls whether the cluster-internal endpoint is disabled.
	// In cluster mode, this should typically remain enabled for vlinsert→vlstorage communication.
	// Can be disabled for security in certain deployments.
	disableInternalInsert = flag.Bool("internalinsert.disable", false, "Whether to disable /internal/insert HTTP endpoint. See https://docs.victoriametrics.com/victorialogs/cluster/#security")
)

// Init initializes the vlinsert package.
// Currently only initializes the syslog listener if configured.
func Init() {
	syslog.MustInit()
}

// Stop shuts down the vlinsert package.
// Stops any background listeners (e.g., syslog UDP/TCP servers).
func Stop() {
	syslog.MustStop()
}

// RequestHandler is the entry point for all insert-related HTTP requests.
// It routes requests to the appropriate protocol handler based on URL path.
//
// Returns true if the request was handled (even if an error occurred),
// false if the path doesn't match any insert endpoint.
func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
	path := strings.ReplaceAll(r.URL.Path, "//", "/")

	// Route /insert/* requests
	if strings.HasPrefix(path, "/insert/") {
		if *disableInsert {
			httpserver.Errorf(w, r, "requests to /insert/* are disabled with -insert.disable command-line flag")
			return true
		}

		return insertHandler(w, r, path)
	}

	// Route /internal/insert (cluster-internal binary protocol)
	if path == "/internal/insert" {
		if *disableInternalInsert || *disableInsert {
			httpserver.Errorf(w, r, "requests to /internal/insert are disabled with -internalinsert.disable or -insert.disable command-line flag")
			return true
		}
		internalinsert.RequestHandler(w, r)
		return true
	}

	return false
}

// insertHandler routes /insert/* requests to the appropriate protocol handler.
// The routing is based on the URL path prefix.
func insertHandler(w http.ResponseWriter, r *http.Request, path string) bool {
	// Exact path matches
	switch path {
	case "/insert/jsonline":
		// JSON Lines format - most common for custom integrations
		jsonline.RequestHandler(w, r)
		return true
	case "/insert/native":
		// Native binary format - most efficient for vlagent
		nativeinsert.RequestHandler(w, r)
		return true
	case "/insert/ready":
		// Health check endpoint
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		fmt.Fprintf(w, `{"status":"ok"}`)
		return true
	}

	// Prefix matches for protocols with sub-paths
	switch {
	// Elasticsearch bulk API - handles /insert/elasticsearch and /insert/elasticsearch/*
	// Some clients omit the trailing slash, so we handle both cases.
	case strings.HasPrefix(path, "/insert/elasticsearch"):
		return elasticsearch.RequestHandler(path, w, r)

	// Grafana Loki push API
	case strings.HasPrefix(path, "/insert/loki/"):
		return loki.RequestHandler(path, w, r)

	// OpenTelemetry log export
	case strings.HasPrefix(path, "/insert/opentelemetry/"):
		return opentelemetry.RequestHandler(path, w, r)

	// systemd-journal format
	case strings.HasPrefix(path, "/insert/journald/"):
		return journald.RequestHandler(path, w, r)

	// Datadog log intake API
	case strings.HasPrefix(path, "/insert/datadog/"):
		return datadog.RequestHandler(path, w, r)
	}

	return false
}
