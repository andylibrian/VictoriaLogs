// Package main provides the entry point for vlagent - VictoriaLogs' log collection agent.
//
// vlagent is a lightweight agent that runs as a DaemonSet on each Kubernetes node.
// It discovers pods via the Kubernetes API, tails their container log files,
// enriches each log line with Kubernetes metadata, and ships the results to
// one or more VictoriaLogs instances over the native binary protocol (/insert/native).
//
// # Architecture Overview
//
// vlagent reuses the same ingestion library (vlinsert) as the VictoriaLogs server.
// The key difference is the storage backend: instead of writing to local disk via vlstorage,
// vlagent injects a remotewrite.Storage{} that serializes rows and sends them over HTTP.
// This means external clients can also push logs directly to vlagent via any supported
// protocol (Elasticsearch, Loki, OTLP, etc.) on port 9429, and vlagent will forward
// them to the configured -remoteWrite.url destinations.
//
// # Initialization Order
//
// The initialization order is critical:
//  1. remotewrite.Storage is injected as the storage backend
//  2. remotewrite.Init() opens persistent queues and starts workers
//  3. kubernetescollector.Init() starts pod watching + file tailing
//  4. vlinsert.Init() registers HTTP handlers for external ingestion
//  5. HTTP server starts on port 9429
//
// Shutdown reverses this order to ensure graceful data flushing.
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

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/kubernetescollector"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/remotewrite"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
)

var (
	// httpListenAddrs specifies the TCP address(es) to listen for incoming HTTP requests.
	// Default is ":9429". Set to empty to disable HTTP listening, which is useful when
	// running multiple vlagent instances on the same server.
	httpListenAddrs = flagutil.NewArrayString("httpListenAddr", "TCP address to listen for incoming http requests. "+
		"Set this flag to empty value in order to disable listening on any port. This mode may be useful for running multiple vlagent instances on the same server. "+
		"Note that /targets and /metrics pages aren't available if -httpListenAddr=''. See also -tls and -httpListenAddr.useProxyProtocol")

	// useProxyProtocol enables the HAProxy proxy protocol for connections.
	// This is useful when vlagent is behind a load balancer that uses proxy protocol.
	useProxyProtocol = flagutil.NewArrayBool("httpListenAddr.useProxyProtocol", "Whether to use proxy protocol for connections accepted at the corresponding -httpListenAddr . "+
		"See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt . "+
		"With enabled proxy protocol http server cannot serve regular /metrics endpoint. Use -pushmetrics.url for metrics pushing")

	// tmpDataPath is the base directory for storing vlagent data including:
	// - Persistent queue data for remote write (see -remoteWrite.tmpDataPath)
	// - Checkpoint files for Kubernetes log collection (see -kubernetesCollector.checkpointsPath)
	tmpDataPath = flag.String("tmpDataPath", "", "Default path for storing vlagent data; see also -remoteWrite.tmpDataPath and -kubernetesCollector.checkpointsPath")
)

// main is the entry point for vlagent.
//
// The initialization follows a specific order to ensure components are ready
// before data starts flowing:
//
//  1. remotewrite.Storage is set as the global storage implementation.
//     This intercepts all log row insertions and routes them to remote URLs
//     instead of local disk storage.
//
//  2. remotewrite.Init() initializes the persistent queue and HTTP clients.
//     This must happen before any data is pushed, otherwise data would be lost.
//
//  3. kubernetescollector.Init() starts watching pods and tailing log files.
//     This begins producing data that flows through remotewrite.
//
//  4. vlinsert.Init() registers HTTP handlers for external ingestion protocols.
//     External clients can then push logs via /insert/* endpoints.
//
//  5. HTTP server starts to serve requests and metrics.
//
// Shutdown reverses this order to ensure all in-flight data is flushed:
// vlinsert stops accepting new data, then kubernetescollector stops reading,
// then remotewrite flushes its queues before closing.
func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = usage
	envflag.Parse()
	buildinfo.Init()

	// Hide sensitive URLs from /metrics output unless explicitly requested.
	// remoteWrite.url can contain authentication tokens/passwords.
	remotewrite.InitSecretFlags()
	logger.Init()

	// Default to port 9429 if no listen address specified.
	// This port was chosen to be distinct from VictoriaMetrics (8428) and
	// VictoriaLogs server (9428).
	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{":9429"}
	}
	logger.Infof("starting vlagent at %q...", listenAddrs)
	startTime := time.Now()

	// CRITICAL: Set remotewrite.Storage as the global storage BEFORE initializing
	// any component that might produce log data. This ensures all data flows
	// through the remote write pipeline.
	//
	// The insertutil.LogRowsStorage interface is the same one used by VictoriaLogs
	// server, but there it's implemented by vlstorage.Storage (local disk).
	// Here, remotewrite.Storage marshals rows and sends them via HTTP.
	insertutil.SetLogRowsStorage(&remotewrite.Storage{})

	// Initialize remote write FIRST - opens persistent queues and starts workers.
	// If this fails, vlagent cannot function as it has nowhere to send data.
	remotewrite.Init(*tmpDataPath)

	// Initialize Kubernetes collector AFTER remote write is ready.
	// This starts the data production pipeline (pod watching + file tailing).
	kubernetescollector.Init(*tmpDataPath)

	// Initialize vlinsert AFTER collector is running.
	// This registers HTTP handlers so external clients can also push logs
	// through vlagent (which will forward to remote URLs).
	vlinsert.Init()

	// Start HTTP server last - all components must be ready before accepting requests.
	go httpserver.Serve(listenAddrs, requestHandler, httpserver.ServeOptions{
		UseProxyProtocol: useProxyProtocol,
	})
	logger.Infof("started vlagent in %.3f seconds", time.Since(startTime).Seconds())

	// Optional: push metrics to external Prometheus-compatible endpoints.
	pushmetrics.Init()

	// Wait for termination signal (SIGTERM, SIGINT).
	sig := procutil.WaitForSigterm()
	logger.Infof("received signal %s", sig)
	pushmetrics.Stop()

	// GRACEFUL SHUTDOWN: Reverse order of initialization.
	// Each component must stop accepting new work before the next one stops,
	// to ensure all in-flight data is properly flushed.
	startTime = time.Now()
	logger.Infof("gracefully shutting down webservice at %q", listenAddrs)
	if err := httpserver.Stop(listenAddrs); err != nil {
		logger.Fatalf("cannot stop the webservice: %s", err)
	}

	// Stop accepting new data from external clients.
	vlinsert.Stop()

	// Stop reading from Kubernetes log files.
	kubernetescollector.Stop()

	// Flush remaining data in persistent queues and close connections.
	// This may take time if remote storage is slow or unreachable.
	remotewrite.Stop()
	logger.Infof("successfully shut down the webservice in %.3f seconds", time.Since(startTime).Seconds())
	logger.Infof("successfully stopped vlagent in %.3f seconds", time.Since(startTime).Seconds())
}

// requestHandler routes incoming HTTP requests to appropriate handlers.
//
// It handles two cases:
//   - "/" root path: serves a simple HTML page with links to docs and useful endpoints
//   - All other paths: delegates to vlinsert.RequestHandler which handles all
//     /insert/* endpoints (jsonline, elasticsearch, loki, otlp, native, etc.)
//
// This design allows vlagent to accept logs via the same protocols as VictoriaLogs
// server, acting as a log collector/forwarder.
func requestHandler(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/" {
		if r.Method != http.MethodGet {
			return false
		}
		w.Header().Add("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<h2>vlagent</h2>")
		fmt.Fprintf(w, "See docs at <a href='https://docs.victoriametrics.com/victorialogs/vlagent/'>https://docs.victoriametrics.com/victorialogs/vlagent/</a></br>")
		fmt.Fprintf(w, "Useful endpoints:</br>")
		httpserver.WriteAPIHelp(w, [][2]string{
			{"metrics", "available service metrics"},
			{"flags", "command-line flags"},
		})
		return true
	}
	// Delegate to vlinsert for all /insert/* and other ingestion endpoints.
	// vlinsert handles jsonline, elasticsearch, loki, otlp, native protocols, etc.
	return vlinsert.RequestHandler(w, r)
}

// usage prints the command-line help message.
func usage() {
	const s = `
vlagent collects logs via popular data ingestion protocols and routes it to VictoriaLogs.

See the docs at https://docs.victoriametrics.com/victorialogs/vlagent/ .
`
	flagutil.Usage(s)
}
