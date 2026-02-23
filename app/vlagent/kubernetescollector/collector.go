package kubernetescollector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/remotewrite"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// kubernetesCollector orchestrates Kubernetes log collection for a single node.
//
// It maintains a connection to the Kubernetes API to watch for pod changes on
// the current node, and delegates actual log file reading to fileCollector.
// Each container gets its own goroutine that tails its log file for the
// lifetime of the container.
//
// The collector is node-scoped: it only watches pods scheduled on the node
// where this vlagent instance is running. This is achieved by filtering the
// Kubernetes API watch query with spec.nodeName=<current_node>.
type kubernetesCollector struct {
	// client is the Kubernetes API client used to watch for pod changes.
	client *kubeAPIClient

	// currentNode contains metadata about the node this vlagent is running on.
	// Used for enriching log entries with node labels/annotations.
	currentNode node

	// ctx and cancel control the lifecycle of all collector goroutines.
	// When stop() is called, cancel() is invoked to signal all goroutines to stop.
	ctx    context.Context
	cancel context.CancelFunc

	// wg tracks all goroutines spawned by this collector.
	// stop() waits on this to ensure clean shutdown.
	wg sync.WaitGroup

	// logsPath is the path to the directory containing Kubernetes container log symlinks.
	// This is typically /var/log/containers in standard Kubernetes deployments.
	// The directory contains symlinks with names like:
	// <pod_name>_<namespace>_<container_name>-<container_id>.log
	// These symlinks point to actual log files under /var/log/pods/.
	logsPath string

	// fileCollector manages the actual log file reading and checkpoint persistence.
	fileCollector *fileCollector
}

// startKubernetesCollector creates and starts a new Kubernetes log collector.
//
// The initialization process:
//  1. Verify the logs directory exists and is accessible
//  2. Fetch node metadata from the Kubernetes API (for labels/annotations enrichment)
//  3. Start the file collector subsystem (loads checkpoints, prepares for tailing)
//  4. List all current pods on this node and start tailing their logs
//  5. Clean up checkpoints for pods that no longer exist
//  6. Start the pod watch loop in a background goroutine
//
// The caller must call stop() when the collector is no longer needed to ensure
// checkpoints are flushed and all goroutines are stopped.
func startKubernetesCollector(client *kubeAPIClient, currentNodeName, logsPath, checkpointsPath string, excludeFilter *logstorage.Filter) (*kubernetesCollector, error) {
	// Verify the logs directory is accessible.
	// This is typically /var/log/containers which should be mounted from the host.
	_, err := os.Stat(logsPath)
	if err != nil {
		return nil, fmt.Errorf("cannot access logs dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Fetch full node metadata from the Kubernetes API.
	// We need this for node labels and annotations which are added to log entries.
	currentNode, err := client.getNodeByName(ctx, currentNodeName)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot get information about current node %q: %w", currentNodeName, err)
	}

	kc := &kubernetesCollector{
		client:      client,
		currentNode: currentNode,
		ctx:         ctx,
		cancel:      cancel,
		logsPath:    logsPath,
	}

	// Create the processor factory that will be used for each log file.
	// Each file gets its own processor instance with the container's metadata.
	storage := &remotewrite.Storage{}
	newProcessor := func(commonFields []logstorage.Field) processor {
		return newLogFileProcessor(storage, commonFields)
	}

	// Start the file collector subsystem.
	// This loads checkpoints and prepares for file tailing.
	fc := startFileCollector(checkpointsPath, excludeFilter, newProcessor)
	kc.fileCollector = fc

	// List existing pods on this node and start reading their logs immediately.
	// This is done before starting the watch to ensure we don't miss any pods.
	pl, err := client.getNodePods(ctx, currentNodeName)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot get Pods on node %q: %w", currentNodeName, err)
	}

	// Start reading existing Pod logs.
	// Each container gets its own goroutine that tails its log file.
	for _, pod := range pl.Items {
		kc.startReadPodLogs(pod)
	}

	// Cleanup checkpoints for deleted Pods.
	// This prevents checkpoint file bloat from old pods that no longer exist.
	fc.cleanupCheckpoints()

	// Begin watching for new Pods and start reading their logs.
	// This runs in a background goroutine for the lifetime of the collector.
	kc.wg.Go(func() {
		kc.watchForPodsUpdates(ctx, pl.Metadata.ResourceVersion)
	})

	return kc, nil
}

// watchForPodsUpdates maintains a long-lived watch on pods scheduled on the current node.
//
// The Kubernetes watch API provides a stream of events (ADDED, MODIFIED, DELETED)
// for pods matching the node filter. This allows vlagent to:
//   - Start tailing logs for newly created pods
//   - Handle pod updates (though we mainly care about new containers)
//   - Ignore deleted pods (file collector will detect file deletion)
//
// The watch uses exponential backoff (200ms-30s) for reconnection to avoid
// hammering the API server during outages. Special handling is included for:
//   - 410 Gone responses: The resourceVersion is stale, restart from beginning
//   - EOF errors: Kubernetes API server closed connection, reconnect
//   - Network errors: Retry with backoff
func (kc *kubernetesCollector) watchForPodsUpdates(ctx context.Context, resourceVersion string) {
	currentNodeName := kc.currentNode.Metadata.Name

	// Backoff timer for reconnection attempts.
	// Starts at 200ms, doubles on each failure, caps at 30s.
	// Reset on successful event processing.
	bt := newBackoffTimer(time.Millisecond*200, time.Second*30)
	defer bt.stop()

	// Track if we're in an error state for logging purposes.
	// This allows us to log "reconnected" messages only after failures.
	errorFired := false

	// handleEvent processes a single watch event.
	handleEvent := func(event watchEvent) error {
		switch event.Type {
		case "ADDED", "MODIFIED":
			bt.reset() // Reset backoff on successful event.

			if errorFired {
				logger.Infof("successfully re-established watching Pods on Node %q", currentNodeName)
			}
			errorFired = false

			// Parse the pod object from the event.
			var pod pod
			if err := json.Unmarshal(event.Object, &pod); err != nil {
				logger.Panicf("FATAL: cannot parse Kubernetes event object %q: %s", event.Object, err)
			}

			// Start reading this pod's logs (if not already doing so).
			kc.startReadPodLogs(pod)

			// Update resourceVersion for next watch request.
			// This allows resuming from where we left off on reconnection.
			resourceVersion = pod.Metadata.ResourceVersion
			return nil
		case "DELETED":
			// Ignore deleted pods - the file collector will detect file deletion
			// and clean up. We don't need to do anything here.
			return nil
		case "ERROR":
			// Parse the error to check if it's a "410 Gone" response.
			errorMessage := struct {
				Code int `json:"code"`
			}{}
			if err := json.Unmarshal(event.Object, &errorMessage); err != nil {
				logger.Panicf("FATAL: cannot parse Kubernetes error message %q: %s", event.Object, err)
			}

			// HTTP 410 Gone means the resourceVersion is too old and has been
			// garbage collected by the API server. We need to restart from scratch.
			// See: https://kubernetes.io/docs/reference/using-api/api-concepts/#410-gone-responses
			if errorMessage.Code == http.StatusGone && resourceVersion != "" {
				resourceVersion = ""
				return nil
			}

			return fmt.Errorf("unexpected error message: %q", event.Object)
		default:
			return fmt.Errorf("unexpected event type %q: %q", event.Type, event.Object)
		}
	}

	stopCh := ctx.Done()

	// Track last EOF time to avoid spamming logs.
	// Kubernetes API server closes connections periodically, which is expected.
	lastEOF := time.Time{}

	for {
		// Start a new watch request.
		// This is a long-lived HTTP request that streams events.
		r, err := kc.client.watchNodePods(ctx, currentNodeName, resourceVersion)
		if err != nil {
			if ctx.Err() != nil {
				// Context was cancelled - we're shutting down.
				return
			}

			errorFired = true

			logger.Errorf("failed to start watching Pods on node %q: %s; will retry in %s", currentNodeName, err, bt.currentDelay())
			bt.wait(stopCh)
			continue
		}

		// Read events from the watch stream.
		err = r.readEvents(handleEvent)
		_ = r.close()
		if err != nil {
			if ctx.Err() != nil {
				// Context was cancelled - we're shutting down.
				return
			}

			// EOF errors are expected when the API server closes the connection.
			// This happens periodically for various reasons (timeouts, load balancing, etc.)
			isEOF := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
			if isEOF && time.Since(lastEOF) > time.Minute {
				// Only log EOF once per minute to avoid spam.
				// Kubernetes API server closed the connection - this is expected.
				lastEOF = time.Now()
				continue
			}

			errorFired = true

			logger.Errorf("failed to read Pod events from the Kubernetes API: %s; will retry in %s", err, bt.currentDelay())
			bt.wait(stopCh)
			continue
		}
	}
}

// startReadPodLogs initiates log collection for all containers in a pod.
//
// For each running container in the pod (including init containers), it:
//  1. Builds the log file path from pod metadata
//  2. Extracts Kubernetes metadata (pod name, namespace, labels, etc.)
//  3. Delegates to fileCollector.startRead() to begin tailing
//
// This function is idempotent - if we're already reading a container's logs,
// the duplicate request is ignored.
func (kc *kubernetesCollector) startReadPodLogs(pod pod) {
	// startRead is a closure that handles a single container.
	startRead := func(pc podContainer, cs containerStatus) {
		// Build Kubernetes metadata fields for this container.
		// These fields are added to every log line from this container.
		commonFields := getCommonFields(kc.currentNode, pod, cs)

		// Determine the log file path.
		// Path format: /var/log/containers/<pod>_<namespace>_<container>-<containerID>.log
		filePath := kc.getLogFilePath(pod, pc, cs)

		// Start tailing this log file.
		// This is idempotent - if already reading, this is a no-op.
		kc.fileCollector.startRead(filePath, commonFields)
	}

	// Process regular containers.
	for _, pc := range pod.Spec.Containers {
		cs, ok := pod.Status.findContainerStatus(pc.Name)
		if !ok || cs.ContainerID == "" {
			// Container in the pod is not running yet or has no ID.
			// Skip it - we'll get another event when it starts.
			continue
		}
		startRead(pc, cs)
	}

	// Process init containers.
	// Init containers may still be relevant for logs, especially for debugging.
	for _, pc := range pod.Spec.InitContainers {
		cs, ok := pod.Status.findInitContainerStatus(pc.Name)
		if !ok || cs.ContainerID == "" {
			// Init container is not running or has no ID.
			continue
		}
		startRead(pc, cs)
	}
}

// getCommonFields builds the Kubernetes metadata fields for a container's logs.
//
// These fields are added to every log line from this container, enabling
// powerful queries in VictoriaLogs like:
//   - kubernetes.pod_namespace="production"
//   - kubernetes.pod_labels.app="nginx"
//
// The field names match Vector.dev's kubernetes_source schema for easy migration.
// See: https://vector.dev/docs/reference/configuration/sources/kubernetes_logs/
func getCommonFields(n node, p pod, cs containerStatus) []logstorage.Field {
	var fs logstorage.Fields

	// Core identifying fields - always included.
	// These match Vector.dev kubernetes_source schema.
	fs.Add("kubernetes.container_name", cs.Name)
	fs.Add("kubernetes.pod_name", p.Metadata.Name)
	fs.Add("kubernetes.pod_namespace", p.Metadata.Namespace)
	fs.Add("kubernetes.container_id", cs.ContainerID)
	fs.Add("kubernetes.pod_ip", p.Status.PodIP)
	fs.Add("kubernetes.pod_node_name", p.Spec.NodeName)

	// Pod labels - useful for filtering by application, environment, etc.
	// Controlled by -kubernetesCollector.includePodLabels (default: true).
	for k, v := range p.Metadata.Labels {
		fieldName := "kubernetes.pod_labels." + k
		fs.Add(fieldName, v)
	}

	// Pod annotations - may contain useful metadata.
	// Controlled by -kubernetesCollector.includePodAnnotations (default: false).
	for k, v := range p.Metadata.Annotations {
		fieldName := "kubernetes.pod_annotations." + k
		fs.Add(fieldName, v)
	}

	// Node labels - useful for filtering by node role, zone, etc.
	// Controlled by -kubernetesCollector.includeNodeLabels (default: false).
	for k, v := range n.Metadata.Labels {
		fieldName := "kubernetes.node_labels." + k
		fs.Add(fieldName, v)
	}

	// Node annotations - may contain useful node metadata.
	// Controlled by -kubernetesCollector.includeNodeAnnotations (default: false).
	for k, v := range n.Metadata.Annotations {
		fieldName := "kubernetes.node_annotations." + k
		fs.Add(fieldName, v)
	}

	return fs.Fields
}

// getLogFilePath constructs the log file path for a container.
//
// Kubernetes container log files follow a specific naming convention:
// /var/log/containers/<pod_name>_<namespace>_<container_name>-<container_id>.log
//
// The container ID may have a runtime prefix like "docker://" or "containerd://"
// which needs to be stripped. The symlink points to the actual log file under
// /var/log/pods/<namespace>_<pod_name>_<pod_uid>/<container>/NN.log
func (kc *kubernetesCollector) getLogFilePath(p pod, pc podContainer, cs containerStatus) string {
	cid := cs.ContainerID

	// Trim the container runtime prefix from the container ID.
	// Container ID formats:
	//   - Docker: docker://<container_id>
	//   - containerd: containerd://<container_id>
	//   - CRI-O: cri-o://<container_id>
	if n := strings.Index(cs.ContainerID, "://"); n >= 0 {
		cid = cs.ContainerID[n+len("://"):]
	}

	// Validate that we have all required information.
	if p.Metadata.Name == "" || p.Metadata.Namespace == "" || pc.Name == "" || cid == "" {
		logger.Panicf("FATAL: got invalid container info from Kubernetes API: pod name %q, namespace %q, container name %q, container ID %q",
			p.Metadata.Name, p.Metadata.Namespace, pc.Name, cid)
	}

	// Construct the log file path.
	// Format: <pod_name>_<namespace>_<container_name>-<container_id>.log
	filename := p.Metadata.Name + "_" + p.Metadata.Namespace + "_" + pc.Name + "-" + cid + ".log"
	logfilePath := path.Join(kc.logsPath, filename)
	return logfilePath
}

// stop gracefully shuts down the Kubernetes collector.
//
// The shutdown process:
//  1. Cancel the context to signal all goroutines to stop
//  2. Wait for all goroutines to finish
//  3. Stop the file collector (flushes checkpoints)
//
// This ensures all in-flight log processing completes and checkpoints are
// persisted before returning.
func (kc *kubernetesCollector) stop() {
	kc.cancel()
	kc.wg.Wait()
	kc.fileCollector.stop()
}
