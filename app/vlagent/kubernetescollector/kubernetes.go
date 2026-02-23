// Package kubernetescollector implements Kubernetes-aware log collection for vlagent.
//
// This package discovers pods running on the current node via the Kubernetes API,
// tails their container log files from /var/log/containers, enriches each log line
// with Kubernetes metadata (pod name, namespace, labels, etc.), and forwards the
// enriched logs to the remote write subsystem.
//
// # Key Components
//
//   - kubernetes.go: Entry point and initialization
//   - collector.go: Pod discovery and watching via Kubernetes API
//   - file_collector.go: File monitoring and rotation handling
//   - logFile.go: Low-level log file reading with tail tracking
//   - processor.go: Log line parsing and metadata enrichment
//   - checkpoints_db.go: Persistent state for crash recovery
//   - client_config.go: Kubernetes API client configuration
//
// # Node-Scoped Design
//
// Each vlagent instance only collects logs from pods on its own node. This is
// achieved by filtering the Kubernetes API watch query with spec.nodeName=<current_node>.
// This design:
//   - Reduces API server load (each node only watches its own pods)
//   - Provides natural load distribution across the cluster
//   - Simplifies deployment (one DaemonSet pod per node)
//
// # Checkpoint System
//
// The collector persists read offsets to disk, enabling crash recovery with minimal
// data loss or duplication. See checkpoints_db.go for details.
package kubernetescollector

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	// enabled controls whether Kubernetes log collection is active.
	// When disabled (-kubernetesCollector=false), vlagent still accepts logs
	// via HTTP endpoints but won't tail Kubernetes container logs.
	enabled = flag.Bool("kubernetesCollector", false, "Whether to enable collecting logs from Kubernetes")

	// checkpointsPath specifies where to persist log file read offsets.
	// Checkpoints enable resuming from the last read position after restarts,
	// preventing log duplication or loss.
	// Default: vlagent-kubernetes-checkpoints.json under -tmpDataPath.
	checkpointsPath = flag.String("kubernetesCollector.checkpointsPath", "",
		"Path to file with checkpoints for Kubernetes logs. "+
			"Checkpoints are used to persist the read offsets for Kubernetes container logs. "+
			"When vlagent is restarted, it resumes reading logs from the stored offsets to avoid log duplication; "+
			"if this flag isn't set, then checkpoints are saved into vlagent-kubernetes-checkpoints.json under -tmpDataPath directory")

	// logsPath points to the directory containing symlinks to container log files.
	// Standard Kubernetes path is /var/log/containers, where each symlink follows
	// the pattern: <pod_name>_<namespace>_<container_name>-<container_id>.log
	// The symlinks point to actual files under /var/log/pods/.
	logsPath = flag.String("kubernetesCollector.logsPath", "/var/log/containers",
		"Path to the directory with Kubernetes container logs (usually /var/log/containers). "+
			"This should point to the kubelet-managed directory containing symlinks to pod logs. "+
			"vlagent must have read access to this directory and to the target log files, typically located under /var/log/pods and /var/lib on the host")

	// excludeFilter is a LogsQL filter for excluding containers from log collection.
	// This is applied BEFORE reading log files, making it efficient for skipping
	// noisy system pods or containers that don't need log collection.
	// Example: kubernetes.pod_namespace="kube-system"
	excludeFilter = flag.String("kubernetesCollector.excludeFilter", "", "Optional LogsQL filter for excluding container logs. "+
		"The filter is applied to container metadata fields (e.g., kubernetes.namespace_name, kubernetes.container_name) before reading the log files. "+
		"This significantly reduces CPU and I/O usage by skipping logs from unwanted containers. "+
		"See https://docs.victoriametrics.com/victorialogs/vlagent/#filtering-kubernetes-logs")
)

// collector is the global Kubernetes collector instance.
// It's initialized by Init() and stopped by Stop().
var collector *kubernetesCollector

// Init initializes the Kubernetes log collector.
//
// This function:
//  1. Loads Kubernetes API configuration (in-cluster or local kubeconfig)
//  2. Determines which node this vlagent instance is running on
//  3. Lists existing pods on this node and starts tailing their logs
//  4. Begins watching for pod changes (additions, modifications)
//
// The function will panic if:
//   - Kubernetes API configuration cannot be loaded
//   - The Kubernetes API client cannot be created
//   - The current node name cannot be determined
//   - The excludeFilter is invalid LogsQL syntax
//
// This function is idempotent when -kubernetesCollector=false.
func Init(tmpDataPath string) {
	if !*enabled {
		// Kubernetes collection is disabled - vlagent still accepts logs via HTTP.
		return
	}

	// Load kubeconfig: auto-detects in-cluster vs local development config.
	// In-cluster: uses service account token at /var/run/secrets/kubernetes.io/serviceaccount/
	// Local: reads from KUBECONFIG env or ~/.kube/config
	cfg, isLocal, err := loadKubeAPIConfig()
	if err != nil {
		logger.Fatalf("cannot load Kubernetes config: %s", err)
	}

	// Create the Kubernetes API client with the loaded configuration.
	c, err := newKubeAPIClient(cfg)
	if err != nil {
		logger.Fatalf("cannot create Kubernetes client: %s", err)
	}

	// Determine which node this vlagent is running on.
	// In-cluster: looks up own pod's spec.nodeName via the API.
	// Local: uses the first available node (for development/testing).
	currentNodeName, err := getCurrentNodeName(c, isLocal)
	if err != nil {
		logger.Fatalf("cannot get current node name: %s", err)
	}

	// Determine checkpoint file path.
	// Checkpoints persist read offsets for crash recovery.
	path := *checkpointsPath
	if len(path) == 0 {
		path = filepath.Join(tmpDataPath, "vlagent-kubernetes-checkpoints.json")
	}

	// Parse the exclude filter if provided.
	// This is a LogsQL filter applied to metadata BEFORE reading log files,
	// making it efficient for skipping unwanted containers.
	var excludeF *logstorage.Filter
	if *excludeFilter != "" {
		excludeF, err = logstorage.ParseFilter(*excludeFilter)
		if err != nil {
			logger.Fatalf("cannot parse LogsQL -kubernetesContainer.excludeFilter=%q: %s", *excludeFilter, err)
		}
	}

	// Start the collector:
	// - Loads checkpoints from disk
	// - Lists current pods on this node
	// - Begins tailing their log files
	// - Starts watching for pod changes
	kc, err := startKubernetesCollector(c, currentNodeName, *logsPath, path, excludeF)
	if err != nil {
		logger.Fatalf("cannot start kubernetes collector: %s", err)
	}
	collector = kc

	logger.Infof("started Kubernetes log collector for node %q", currentNodeName)
}

// Stop gracefully shuts down the Kubernetes log collector.
//
// This function:
//  1. Signals all goroutines to stop
//  2. Waits for in-flight operations to complete
//  3. Flushes checkpoints to disk
//
// This is called during vlagent shutdown, AFTER vlinsert.Stop() to ensure
// no new data is being produced while we wait for collectors to finish.
func Stop() {
	if collector != nil {
		collector.stop()
	}
}

// getCurrentNodeName determines which Kubernetes node this vlagent is running on.
//
// The approach differs based on execution context:
//   - In-cluster (production): Queries the Kubernetes API for this pod's spec.nodeName
//   - Local (development): Returns the first node in the cluster
//
// This node name is used to filter the pod watch query, ensuring each vlagent
// instance only collects logs from pods on its own node.
func getCurrentNodeName(client *kubeAPIClient, isLocal bool) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if isLocal {
		return getCurrentNodeNameLocal(ctx, client)
	}
	return getCurrentNodeNameInCluster(ctx, client)
}

// getCurrentNodeNameLocal returns the first available node name.
// This is used when running vlagent locally (outside a Kubernetes cluster)
// for development or testing purposes.
func getCurrentNodeNameLocal(ctx context.Context, client *kubeAPIClient) (string, error) {
	nodes, err := client.getNodes(ctx)
	if err != nil {
		return "", fmt.Errorf("cannot get nodes from the cluster: %w", err)
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("cannot find any nodes in the cluster")
	}
	// Use the first node - acceptable for development/testing.
	firstNode := nodes[0]
	return firstNode, nil
}

// getCurrentNodeNameInCluster determines the node name by looking up
// the current pod's spec.nodeName field via the Kubernetes API.
//
// This is the production code path when vlagent runs as a DaemonSet.
// Kubernetes automatically sets the hostname to the pod name, and we
// read the namespace from the service account mount.
func getCurrentNodeNameInCluster(ctx context.Context, client *kubeAPIClient) (string, error) {
	// Read the namespace from the service account mount.
	// Kubernetes mounts this file automatically in every pod.
	ns, err := getCurrentNamespace()
	if err != nil {
		return "", fmt.Errorf("cannot get current namespace: %w", err)
	}

	// The hostname is set to the pod name by Kubernetes.
	podName, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("cannot get hostname: %w", err)
	}

	// Query the Kubernetes API for this pod's details.
	currentPod, err := client.getPod(ctx, ns, podName)
	if err != nil {
		return "", fmt.Errorf("cannot get pod %q at namespace %q: %w", podName, ns, err)
	}

	// spec.nodeName indicates which node this pod is scheduled on.
	return currentPod.Spec.NodeName, nil
}

// getCurrentNamespace reads the current namespace from the service account mount.
//
// Kubernetes automatically mounts this file at /var/run/secrets/kubernetes.io/serviceaccount/namespace
// in every pod. This is the standard way for pods to discover their own namespace.
func getCurrentNamespace() (string, error) {
	ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", fmt.Errorf("cannot read current namespace: %w", err)
	}
	// Remove any trailing whitespace/newlines.
	ns = bytes.TrimSpace(ns)
	return string(ns), nil
}
