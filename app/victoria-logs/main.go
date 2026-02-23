// Package main is the entry point for the VictoriaLogs server.
//
// VictoriaLogs Architecture Overview:
//
// The server chains three subsystems that handle all HTTP traffic:
//
//   - vlinsert: Handles data ingestion at /insert/* endpoints. Supports multiple protocols
//     (JSON Lines, Elasticsearch bulk, Loki, OpenTelemetry, Datadog, etc.). In cluster mode,
//     vlinsert shards incoming log rows across remote vlstorage nodes.
//
//   - vlselect: Handles query execution at /select/* endpoints. Parses LogsQL queries and
//     executes them against the storage layer. In cluster mode, vlselect fans out queries
//     to all vlstorage nodes in parallel and merges results.
//
//   - vlstorage: The storage backend that persists logs to disk (single-node mode) or routes
//     to remote storage nodes (cluster mode). Also handles internal maintenance operations
//     like retention, merges, and snapshots.
//
// Mode Selection:
//
// The server operates in one of two modes based on the -storageNode flag:
//
//   - Single-node (no -storageNode): All data is stored locally at -storageDataPath.
//     vlinsert writes directly to local storage, vlselect queries local storage.
//
//   - Cluster mode (with -storageNode): The process becomes a vlinsert+vlselect frontend.
//     Data is sharded across the configured vlstorage nodes, queries are fanned out.
//     The actual storage happens on separate vlstorage processes.
//
// Request Routing Flow:
//
//	HTTP Request → requestHandler()
//	    ├─> vlinsert.RequestHandler()  for /insert/* paths
//	    ├─> vlselect.RequestHandler()  for /select/* paths
//	    └─> vlstorage.RequestHandler() for /internal/* maintenance paths
//
// See onboarding/onboarding-cluster.md for detailed cluster architecture documentation.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/pushmetrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
)

var (
	httpListenAddrs  = flagutil.NewArrayString("httpListenAddr", "TCP address to listen for incoming http requests. See also -httpListenAddr.useProxyProtocol")
	useProxyProtocol = flagutil.NewArrayBool("httpListenAddr.useProxyProtocol", "Whether to use proxy protocol for connections accepted at the given -httpListenAddr . "+
		"See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt . "+
		"With enabled proxy protocol http server cannot serve regular /metrics endpoint. Use -pushmetrics.url for metrics pushing")
)

func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = usage
	envflag.Parse()
	buildinfo.Init()
	logger.Init()

	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{":9428"}
	}
	logger.Infof("starting VictoriaLogs at %q...", listenAddrs)
	startTime := time.Now()

	// Initialize subsystems in dependency order:
	// 1. vlstorage: Must be first since it's used by both vlinsert and vlselect.
	//    Determines local vs cluster mode based on -storageNode flag.
	vlstorage.Init()
	// 2. vlselect: Initializes internal select endpoint handlers.
	vlselect.Init()

	// Wire up the storage layer to the insert pipeline.
	// This allows protocol handlers (jsonline, elasticsearch, etc.) to write rows
	// through the insertutil.LogMessageProcessor interface.
	insertutil.SetLogRowsStorage(&vlstorage.Storage{})
	// 3. vlinsert: Initializes protocol handlers and syslog listeners.
	vlinsert.Init()

	// Start the HTTP server after all subsystems are initialized.
	// The server accepts connections but requests are queued until ready.
	go httpserver.Serve(listenAddrs, requestHandler, httpserver.ServeOptions{
		UseProxyProtocol: useProxyProtocol,
	})
	logger.Infof("started VictoriaLogs in %.3f seconds; see https://docs.victoriametrics.com/victorialogs/", time.Since(startTime).Seconds())

	pushmetrics.Init()
	// Wait for shutdown signal (SIGTERM, SIGINT).
	sig := procutil.WaitForSigterm()
	logger.Infof("received signal %s", sig)
	pushmetrics.Stop()

	// Graceful shutdown sequence - reverse order of initialization.
	// Stop accepting new HTTP connections first, then drain subsystems.
	logger.Infof("gracefully shutting down webservice at %q", listenAddrs)
	startTime = time.Now()
	if err := httpserver.Stop(listenAddrs); err != nil {
		logger.Fatalf("cannot stop the webservice: %s", err)
	}
	logger.Infof("successfully shut down the webservice in %.3f seconds", time.Since(startTime).Seconds())

	// Stop subsystems in reverse initialization order.
	// This ensures in-flight operations complete before resources are released.
	vlinsert.Stop()
	vlselect.Stop()
	vlstorage.Stop()

	logger.Infof("the VictoriaLogs has been stopped in %.3f seconds", time.Since(startTime).Seconds())
}

// requestHandler is the central HTTP request dispatcher for VictoriaLogs.
//
// It routes incoming requests to the appropriate subsystem based on URL path:
//   - "/"           → Root page with basic info and useful endpoint links
//   - "/insert/*"   → vlinsert for data ingestion (JSON Lines, Elasticsearch, Loki, etc.)
//   - "/select/*"   → vlselect for LogsQL queries and log exploration
//   - "/internal/*" → vlstorage for maintenance operations (force merge, snapshots, etc.)
//
// The function returns true if the request was handled, false otherwise.
// Unhandled requests fall through to the default httpserver handler (metrics, flags, etc.).
//
// This chaining pattern allows each subsystem to own its URL namespace while keeping
// the main routing logic simple and explicit.
func requestHandler(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/" {
		if r.Method != http.MethodGet {
			return false
		}
		w.Header().Add("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<h2>Single-node VictoriaLogs</h2></br>")
		fmt.Fprintf(w, "Version %s<br>", buildinfo.Version)
		fmt.Fprintf(w, "See docs at <a href='https://docs.victoriametrics.com/victorialogs/'>https://docs.victoriametrics.com/victorialogs/</a></br>")
		fmt.Fprintf(w, "Useful endpoints:</br>")
		httpserver.WriteAPIHelp(w, [][2]string{
			{"select/vmui", "Web UI for VictoriaLogs"},
			{"metrics", "available service metrics"},
			{"flags", "command-line flags"},
		})
		return true
	}
	if vlinsert.RequestHandler(w, r) {
		return true
	}
	if vlselect.RequestHandler(w, r) {
		return true
	}
	if vlstorage.RequestHandler(w, r) {
		return true
	}
	return false
}

func usage() {
	const s = `
victoria-logs is a log management and analytics service.

See the docs at https://docs.victoriametrics.com/victorialogs/
`
	flagutil.Usage(s)
}
