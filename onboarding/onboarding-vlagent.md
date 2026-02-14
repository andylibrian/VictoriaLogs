# VictoriaLogs vlagent Internals - Developer Onboarding Guide

This document provides a comprehensive overview of vlagent, VictoriaLogs' log collection agent. It covers the Kubernetes collector, the native insert client (remote write), configuration, retry/backpressure behavior, and the checkpoint system.

## Table of Contents

- [Overview](#overview)
- [Complete Data Flow](#complete-data-flow)
- [Architecture Layers](#architecture-layers)
  - [1. Entry Point & Initialization](#1-entry-point--initialization)
  - [2. Kubernetes Collector — Pod Discovery](#2-kubernetes-collector--pod-discovery)
  - [3. Kubernetes Collector — File Collection](#3-kubernetes-collector--file-collection)
  - [4. Log Processing & Metadata Enrichment](#4-log-processing--metadata-enrichment)
  - [5. Checkpoint System](#5-checkpoint-system)
  - [6. Remote Write — Batching & Queuing](#6-remote-write--batching--queuing)
  - [7. Remote Write — HTTP Client & Retry](#7-remote-write--http-client--retry)
- [Key Design Patterns](#key-design-patterns)
- [Configuration Flags](#configuration-flags)
- [Document Maintenance Guidelines](#document-maintenance-guidelines)

---

## Overview

**vlagent** is VictoriaLogs' own log collection agent. It runs as a DaemonSet on each Kubernetes node, discovers pods via the Kubernetes API, tails their container log files, enriches each line with Kubernetes metadata, and ships the results to one or more VictoriaLogs instances over the native binary protocol (`/insert/native`).

### Key characteristics

- **Node-scoped**: Each vlagent instance collects logs only from pods on its own node.
- **Checkpoint-based**: Persists file read offsets to resume after restarts and reduce duplication/loss risk.
- **Persistent queue**: Buffers data on disk when the remote VictoriaLogs is unreachable.
- **Replication**: Supports multiple `-remoteWrite.url` destinations for data replication.
- **Default port**: 9429 (configurable via `-httpListenAddr`).

### How vlagent relates to the VictoriaLogs server

vlagent uses the **same ingestion library** (`app/vlinsert/`) as the VictoriaLogs server. The difference is the storage backend: instead of writing to local disk via `vlstorage`, vlagent injects a `remotewrite.Storage{}` that serializes rows and sends them over HTTP. This is the same `insertutil.LogRowsStorage` interface described in the [Insert Flow Guide](./onboarding-insert-flow.md#5-storage-interface).

```
VictoriaLogs Server                       vlagent
─────────────────                         ───────
insertutil.SetLogRowsStorage(             insertutil.SetLogRowsStorage(
  &vlstorage.Storage{}                      &remotewrite.Storage{}
)                                         )
  ↓ MustAddRows(lr)                         ↓ MustAddRows(lr)
localStorage.MustAddRows(lr)              pushToRemoteStorages(lr)
  → local disk                              → HTTP POST /insert/native
```

---

## Complete Data Flow

**Key Files**:
- [`app/vlagent/main.go`](../app/vlagent/main.go#L34) - Entry point
- [`app/vlagent/kubernetescollector/kubernetes.go`](../app/vlagent/kubernetescollector/kubernetes.go#L36) - Kubernetes collector init
- [`app/vlagent/kubernetescollector/collector.go`](../app/vlagent/kubernetescollector/collector.go#L43) - Pod discovery and watching
- [`app/vlagent/kubernetescollector/file_collector.go`](../app/vlagent/kubernetescollector/file_collector.go#L63) - File monitoring
- [`app/vlagent/kubernetescollector/logfile.go`](../app/vlagent/kubernetescollector/logfile.go#L22) - Log file reading
- [`app/vlagent/kubernetescollector/processor.go`](../app/vlagent/kubernetescollector/processor.go#L50) - Log processing
- [`app/vlagent/remotewrite/remotewrite.go`](../app/vlagent/remotewrite/remotewrite.go#L48) - Remote write orchestration
- [`app/vlagent/remotewrite/pendinglogrows.go`](../app/vlagent/remotewrite/pendinglogrows.go#L27) - Batching and compression
- [`app/vlagent/remotewrite/client.go`](../app/vlagent/remotewrite/client.go#L66) - HTTP client with retry

```
Kubernetes API (watch /api/v1/pods)
    ↓
kubernetesCollector.watchForPodsUpdates  [collector.go:95](../app/vlagent/kubernetescollector/collector.go#L95)
    ↓
startReadPodLogs(pod)                    [collector.go:189](../app/vlagent/kubernetescollector/collector.go#L189)
    ↓
getCommonFields(node, pod, cs)           [collector.go:217](../app/vlagent/kubernetescollector/collector.go#L217)
    ↓
fileCollector.startRead(filePath, fields) [file_collector.go:78](../app/vlagent/kubernetescollector/file_collector.go#L78)
    ↓
logFile.readLines(stopCh, proc)          [logfile.go:90](../app/vlagent/kubernetescollector/logfile.go#L90)
    ↓
logFileProcessor.tryAddLine(line)        [processor.go:100](../app/vlagent/kubernetescollector/processor.go#L100)
    ↓
parseCRILine(line) / parseCRILineJSON    [processor.go:426](../app/vlagent/kubernetescollector/processor.go#L426)
    ↓
parseLogRowContent (JSON / klog)         [processor.go:226](../app/vlagent/kubernetescollector/processor.go#L226)
    ↓
addRow(timestamp, fields)                [processor.go:216](../app/vlagent/kubernetescollector/processor.go#L216)
    ↓
remotewrite.Storage.MustAddRows(lr)      [remotewrite.go:51](../app/vlagent/remotewrite/remotewrite.go#L51)
    ↓
pushToRemoteStorages(lr)                 [remotewrite.go:170](../app/vlagent/remotewrite/remotewrite.go#L170)
    ↓
pendingLogs.add(lr) → marshal + zstd    [pendinglogrows.go:52](../app/vlagent/remotewrite/pendinglogrows.go#L52)
    ↓
persistentqueue.FastQueue.TryWriteBlock  [pendinglogrows.go:75](../app/vlagent/remotewrite/pendinglogrows.go#L75)
    ↓
client.runWorker → fq.MustReadBlock      [client.go:232](../app/vlagent/remotewrite/client.go#L232)
    ↓
sendBlockHTTP(block)                     [client.go:326](../app/vlagent/remotewrite/client.go#L326)
    ↓
POST http://<victorialogs>:9428/insert/native?version=v1
    Content-Encoding: zstd
    Content-Type: application/octet-stream
```

---

## Architecture Layers

### 1. Entry Point & Initialization

**File**: [`app/vlagent/main.go`](../app/vlagent/main.go#L34)

**Key Functions**:
- [`main()`](../app/vlagent/main.go#L34) - Entry point
- [`requestHandler(w, r)`](../app/vlagent/main.go#L79) - HTTP request handler

The initialization order is important — remote write must be ready before any log collection begins:

```go
func main() {
    // 1. Inject remotewrite as the storage backend
    insertutil.SetLogRowsStorage(&remotewrite.Storage{})

    // 2. Initialize remote write (opens persistent queues, starts workers)
    remotewrite.Init(*tmpDataPath)

    // 3. Initialize Kubernetes collector (starts pod watching + file tailing)
    kubernetescollector.Init(*tmpDataPath)

    // 4. Initialize vlinsert (registers HTTP handlers for external ingestion)
    vlinsert.Init()

    // 5. Start HTTP server on port 9429
    go httpserver.Serve(listenAddrs, requestHandler, ...)
}
```

Shutdown is the reverse order — ingestion stops first, then the collector, then remote write flushes its queues:

```go
vlinsert.Stop()
kubernetescollector.Stop()
remotewrite.Stop()
```

**Key design choice**: vlagent reuses the full `vlinsert` package. This means external clients can also push logs directly to vlagent via any supported protocol (Elasticsearch, Loki, OTLP, etc.) on port 9429, and vlagent will forward them to the configured `-remoteWrite.url` destinations. The Kubernetes collector is just one data source among potentially many.

**Location**: [`app/vlagent/main.go:34-76`](../app/vlagent/main.go#L34)

---

### 2. Kubernetes Collector — Pod Discovery

**File**: [`app/vlagent/kubernetescollector/collector.go`](../app/vlagent/kubernetescollector/collector.go#L22)

**Key Functions**:
- [`startKubernetesCollector(client, nodeName, logsPath, checkpointsPath, excludeFilter)`](../app/vlagent/kubernetescollector/collector.go#L43) - Start the collector
- [`watchForPodsUpdates(ctx, resourceVersion)`](../app/vlagent/kubernetescollector/collector.go#L95) - Continuous pod watch loop
- [`startReadPodLogs(pod)`](../app/vlagent/kubernetescollector/collector.go#L189) - Start log collection for a pod
- [`getCommonFields(node, pod, cs)`](../app/vlagent/kubernetescollector/collector.go#L217) - Build Kubernetes metadata fields

```go
type kubernetesCollector struct {
    client      *kubeAPIClient     // Kubernetes API client
    currentNode node               // Node this vlagent is running on
    ctx         context.Context    // Cancellation context
    cancel      context.CancelFunc
    wg          sync.WaitGroup
    logsPath    string             // /var/log/containers
    fileCollector *fileCollector   // File monitoring subsystem
}
```

#### Initialization flow

1. **Load kubeconfig** — auto-detects in-cluster vs local config ([`client_config.go`](../app/vlagent/kubernetescollector/client_config.go))
2. **Determine current node** — in-cluster: looks up own pod to get `spec.nodeName`; local: uses the first node
3. **List existing pods** — `GET /api/v1/pods?fieldSelector=spec.nodeName=<name>` — starts tailing their log files
4. **Watch for changes** — long-lived `GET /api/v1/pods?watch=true&fieldSelector=spec.nodeName=<name>` — handles ADDED/MODIFIED events

#### Watch reconnection with backoff

The watch loop uses a [`backoffTimer`](../app/vlagent/kubernetescollector/backoff_timer.go#L11) (200ms–30s exponential with jitter) to handle API server disconnections:

```go
func (kc *kubernetesCollector) watchForPodsUpdates(ctx context.Context, resourceVersion string) {
    bt := newBackoffTimer(time.Millisecond*200, time.Second*30)
    for {
        r, err := kc.client.watchNodePods(ctx, currentNodeName, resourceVersion)
        if err != nil {
            logger.Errorf("failed to start watching; will retry in %s", bt.currentDelay())
            bt.wait(stopCh)
            continue
        }
        err = r.readEvents(handleEvent)
        // ...
    }
}
```

The watch also handles `410 Gone` responses (stale `resourceVersion`) by resetting to watch from the beginning.

#### Metadata enrichment

[`getCommonFields()`](../app/vlagent/kubernetescollector/collector.go#L217) builds the following fields for each container, matching the Vector.dev `kubernetes_source` schema:

| Field | Source |
|-------|--------|
| `kubernetes.container_name` | `containerStatus.Name` |
| `kubernetes.pod_name` | `pod.Metadata.Name` |
| `kubernetes.pod_namespace` | `pod.Metadata.Namespace` |
| `kubernetes.container_id` | `containerStatus.ContainerID` |
| `kubernetes.pod_ip` | `pod.Status.PodIP` |
| `kubernetes.pod_node_name` | `pod.Spec.NodeName` |
| `kubernetes.pod_labels.*` | `pod.Metadata.Labels` (configurable) |
| `kubernetes.pod_annotations.*` | `pod.Metadata.Annotations` (configurable) |
| `kubernetes.node_labels.*` | `node.Metadata.Labels` (configurable) |
| `kubernetes.node_annotations.*` | `node.Metadata.Annotations` (configurable) |

Labels/annotations inclusion is controlled by `-kubernetesCollector.includePodLabels` (default: true), `-kubernetesCollector.includePodAnnotations` (default: false), etc.

**Location**: [`app/vlagent/kubernetescollector/collector.go:22-272`](../app/vlagent/kubernetescollector/collector.go#L22)

---

### 3. Kubernetes Collector — File Collection

**File**: [`app/vlagent/kubernetescollector/file_collector.go`](../app/vlagent/kubernetescollector/file_collector.go#L38)

**Key Functions**:
- [`startFileCollector(checkpointsPath, excludeFilter, newProcessor)`](../app/vlagent/kubernetescollector/file_collector.go#L63) - Start file monitoring
- [`startRead(filepath, commonFields)`](../app/vlagent/kubernetescollector/file_collector.go#L78) - Begin tailing a log file
- [`process(lf, commonFields)`](../app/vlagent/kubernetescollector/file_collector.go#L181) - Main file processing loop
- [`tryResumeFromCheckpoint(filepath, cp)`](../app/vlagent/kubernetescollector/file_collector.go#L109) - Resume from saved offset

```go
type fileCollector struct {
    logFiles      map[string]struct{}    // Currently tracked files (deduplication)
    logFilesLock  sync.Mutex
    excludeFilter *logstorage.Filter     // LogsQL filter for excluding containers
    newProcessor  func([]logstorage.Field) processor  // Factory for log processors
    checkpointsDB *checkpointsDB         // Persistent read offset storage
    wg            sync.WaitGroup
    stopCh        chan struct{}
}
```

#### One goroutine per container

When `startRead()` is called for a new file, a dedicated goroutine is spawned to run the [`process()`](../app/vlagent/kubernetescollector/file_collector.go#L181) loop. This goroutine runs for the lifetime of the container.

#### The process() loop

The core loop polls the log file with exponential backoff (100ms–10s):

```go
func (fc *fileCollector) process(lf *logFile, commonFields []logstorage.Field) {
    // Skip files matching the exclude filter.
    if fc.excludeFilter != nil && fc.excludeFilter.MatchRow(commonFields) {
        fc.forgetFile(lf.path)
        return
    }

    bt := newBackoffTimer(time.Millisecond*100, time.Second*10)
    proc := fc.newProcessor(commonFields)

    for {
        ok := lf.readLines(fc.stopCh, proc)
        if ok {
            fc.checkpointsDB.set(lf.checkpoint())  // Save progress
            bt.reset()                               // Reset backoff on successful read
            bt.wait(fc.stopCh)
            continue
        }

        switch lf.status() {
        case logFileStatusNotRotated:
            bt.wait(fc.stopCh)  // Wait and retry with increasing delay
        case logFileStatusRotated:
            // Drain remaining lines, then reopen the new file
            lf.readLines(neverStopCh, proc)
            lf.tryReopen()
        case logFileStatusDeleted:
            fc.forgetFile(lf.path)
            return
        }
    }
}
```

#### File rotation detection

**File**: [`app/vlagent/kubernetescollector/logfile.go`](../app/vlagent/kubernetescollector/logfile.go#L282)

Kubernetes container log files are symlinks in `/var/log/containers/` that point to the actual log files under `/var/log/pods/`. When kubelet rotates logs, it creates a new file with a new inode. vlagent detects rotation by comparing inodes:

```go
func (lf *logFile) status() logFileStatus {
    if !symlinkExists(lf.path) {
        return logFileStatusDeleted        // Pod was deleted
    }
    newInode := getInode(stat)
    if lf.inode == newInode {
        return logFileStatusNotRotated      // Same file, keep reading
    }
    return logFileStatusRotated             // New inode → kubelet rotated the file
}
```

On rotation, vlagent drains remaining lines from the old file (even during shutdown), then reopens the symlink to read the new file from offset 0.

#### Concurrency limits

Reading and processing are guarded by separate concurrency channels to prevent resource exhaustion:

```go
var (
    readConcurrencyCh    = fsutil.GetConcurrencyCh()                     // Limits concurrent file reads
    processConcurrencyCh = make(chan struct{}, cgroup.AvailableCPUs())    // Limits concurrent line processing
)
```

**Location**: [`app/vlagent/kubernetescollector/file_collector.go:38-345`](../app/vlagent/kubernetescollector/file_collector.go#L38)

---

### 4. Log Processing & Metadata Enrichment

**File**: [`app/vlagent/kubernetescollector/processor.go`](../app/vlagent/kubernetescollector/processor.go#L20)

**Key Functions**:
- [`tryAddLine(logLine)`](../app/vlagent/kubernetescollector/processor.go#L100) - Parse and process a log line
- [`addLineInternal(criTimestamp, line)`](../app/vlagent/kubernetescollector/processor.go#L190) - Parse content and route to storage
- [`addRow(timestamp, fields)`](../app/vlagent/kubernetescollector/processor.go#L216) - Merge metadata fields and send to storage
- [`parseCRILine(b)`](../app/vlagent/kubernetescollector/processor.go#L426) - Parse CRI format log lines
- [`parseCRILineJSON(parser, b)`](../app/vlagent/kubernetescollector/processor.go#L467) - Parse Docker json-file format

#### The `processor` interface

```go
type processor interface {
    // tryAddLine returns true if the line should be committed to checkpointsDB.
    // Returns false for partial CRI lines that need more data.
    tryAddLine(line []byte) bool

    mustClose()
}
```

The [`logFileProcessor`](../app/vlagent/kubernetescollector/processor.go#L50) is the concrete implementation:

```go
type logFileProcessor struct {
    storage      insertutil.LogRowsStorage  // remotewrite.Storage
    lr           *logstorage.LogRows        // Reusable row buffer from sync.Pool
    tenantID     logstorage.TenantID        // From -kubernetesCollector.tenantID
    commonFields []logstorage.Field         // Kubernetes metadata (pod name, namespace, etc.)
    fieldsBuf    []logstorage.Field         // Scratch buffer for merging fields
    partialCRIContent *bytesutil.ByteBuffer // Accumulator for multi-line CRI entries
}
```

#### Processing pipeline

Each log line goes through these stages:

1. **CRI format detection**: Lines starting with `{` are treated as Docker json-file format; all others as CRI text format.

2. **CRI parsing** (`<timestamp> <stream> <partial_flag> <content>`):
   ```
   2026-02-14T10:30:00.123456789Z stdout F Hello world
   ─────────────────────────────── ────── ─ ───────────
         timestamp                stream P/F  content
   ```
   The `P` flag indicates a partial line (container runtime splits lines at 16 KiB by default). vlagent accumulates partial lines up to 2 MB.

3. **Content parsing**: Attempts to parse the content as JSON or klog format. If neither matches, the raw content becomes `_msg`.

4. **Field merging**: Kubernetes metadata (`commonFields`) is prepended to the parsed fields:
   ```go
   func (lfp *logFileProcessor) addRow(timestamp int64, fields []logstorage.Field) {
       lfp.fieldsBuf = append(lfp.fieldsBuf[:0], lfp.commonFields...)
       lfp.fieldsBuf = append(lfp.fieldsBuf, fields...)
       lfp.lr.MustAdd(lfp.tenantID, timestamp, lfp.fieldsBuf, -1)
       lfp.storage.MustAddRows(lfp.lr)
       lfp.lr.ResetKeepSettings()
   }
   ```

5. **Immediate flush**: Unlike the VictoriaLogs server's batching in `logMessageProcessor`, vlagent's processor adds each row individually and calls `MustAddRows` immediately. Batching happens downstream in the `pendingLogs` layer.

#### klog parsing

vlagent detects and parses Kubernetes klog format (used by kubelet, kube-apiserver, etc.):

```
I0214 10:30:00.123456 12345 main.go:42] Starting server
```

Parsed into fields: `level=INFO`, `thread_id=12345`, `source_line=main.go:42`, `_msg=Starting server`.

**Location**: [`app/vlagent/kubernetescollector/processor.go:50-602`](../app/vlagent/kubernetescollector/processor.go#L50)

---

### 5. Checkpoint System

**File**: [`app/vlagent/kubernetescollector/checkpoints_db.go`](../app/vlagent/kubernetescollector/checkpoints_db.go#L23)

**Key Functions**:
- [`startCheckpointsDB(path)`](../app/vlagent/kubernetescollector/checkpoints_db.go#L35) - Start checkpoint persistence
- [`set(cp)`](../app/vlagent/kubernetescollector/checkpoints_db.go#L71) - Update checkpoint for a file
- [`get(path)`](../app/vlagent/kubernetescollector/checkpoints_db.go#L78) - Retrieve checkpoint for a file
- [`mustSync()`](../app/vlagent/kubernetescollector/checkpoints_db.go#L105) - Persist all checkpoints to disk
- [`readCheckpoints(path)`](../app/vlagent/kubernetescollector/checkpoints_db.go#L120) - Load checkpoints from disk

The checkpoint system persists the exact read position for each log file, enabling best-effort crash recovery with minimal duplication.

```go
type checkpoint struct {
    Path        string `json:"path"`        // Symlink path in /var/log/containers/
    Inode       uint64 `json:"inode"`       // File inode (for rotation detection)
    Fingerprint uint64 `json:"fingerprint"` // xxhash of first line (for inode reuse detection)
    Offset      int64  `json:"offset"`      // Byte offset in the file
}
```

#### Persistence strategy

- **Periodic sync**: Every 1 minute via a background goroutine
- **Graceful shutdown**: Final sync on `stop()`
- **Storage format**: JSON file at `-kubernetesCollector.checkpointsPath` (default: `vlagent-kubernetes-checkpoints.json` under `-tmpDataPath`)
- **Atomic writes**: Uses `fs.MustWriteAtomic` to prevent corruption

#### Resume logic on restart

When vlagent restarts and finds a checkpoint for a file:

1. Open the file at the checkpointed path
2. Compare the current inode with the checkpointed inode
3. If inodes differ (file was rotated while vlagent was down), search the directory for a file with the old inode
4. Verify the file fingerprint (xxhash of first line) to detect inode reuse
5. If everything matches, seek to the checkpointed offset and continue reading

This is handled in [`tryResumeFromCheckpoint()`](../app/vlagent/kubernetescollector/file_collector.go#L109).

If the rotated file cannot be found (or fingerprint validation fails), vlagent logs a warning and resumes from the current file, so some historical lines may be lost.

**Location**: [`app/vlagent/kubernetescollector/checkpoints_db.go:23-167`](../app/vlagent/kubernetescollector/checkpoints_db.go#L23)

---

### 6. Remote Write — Batching & Queuing

**File**: [`app/vlagent/remotewrite/remotewrite.go`](../app/vlagent/remotewrite/remotewrite.go#L48)

**Key Functions**:
- [`Init(tmpDataPath)`](../app/vlagent/remotewrite/remotewrite.go#L79) - Initialize remote write contexts
- [`Stop()`](../app/vlagent/remotewrite/remotewrite.go#L100) - Flush and stop
- [`pushToRemoteStorages(lr)`](../app/vlagent/remotewrite/remotewrite.go#L170) - Distribute to all URLs
- [`newRemoteWriteCtx(argIdx, url, maxInmemoryBlocks, sanitizedURL, tmpDataPath)`](../app/vlagent/remotewrite/remotewrite.go#L198) - Create a per-URL context

#### Storage interface

```go
// Storage implements insertutil.LogRowsStorage — the same interface used by
// vlstorage in the VictoriaLogs server, but routes data to remote HTTP endpoints
// instead of local disk.
type Storage struct{}

func (*Storage) MustAddRows(lr *logstorage.LogRows) {
    pushToRemoteStorages(lr)
}
```

#### Per-URL architecture

Each `-remoteWrite.url` gets its own `remoteWriteCtx`:

```
remoteWriteCtx
├── fq  *persistentqueue.FastQueue   ← File-based persistent queue
├── c   *client                       ← HTTP client with worker pool
└── pls []*pendingLogs                ← Sharded batching buffers (up to AvailableCPUs)
```

When multiple URLs are configured, `pushToRemoteStorages()` pushes to all of them **in parallel** for replication:

```go
func pushToRemoteStorages(lr *logstorage.LogRows) {
    if len(rwctxs) == 1 {
        rwctxs[0].push(lr)  // Fast path: single URL
        return
    }
    var wg sync.WaitGroup
    for _, rwctx := range rwctxs {
        wg.Go(func() { rwctx.push(lr) })
    }
    wg.Wait()
}
```

#### Batching layer (pendingLogs)

**File**: [`app/vlagent/remotewrite/pendinglogrows.go`](../app/vlagent/remotewrite/pendinglogrows.go#L27)

**Key Functions**:
- [`add(lr)`](../app/vlagent/remotewrite/pendinglogrows.go#L52) - Add log rows to the pending buffer
- [`mustFlushLocked()`](../app/vlagent/remotewrite/pendinglogrows.go#L72) - Compress and write to persistent queue
- [`periodicFlusher()`](../app/vlagent/remotewrite/pendinglogrows.go#L82) - Background flush timer

```go
type pendingLogs struct {
    lastFlushTime atomic.Uint64
    fq            *persistentqueue.FastQueue  // Destination queue
    mu            sync.Mutex
    wr            writeRequest                // Accumulates marshaled rows
    stopCh        chan struct{}
    periodicFlusherWG sync.WaitGroup
}
```

Each row is serialized to its binary `InsertRow` format immediately via `r.Marshal()`. The serialized bytes accumulate in `wr.pendingData` until a flush trigger fires:

| Trigger | Threshold |
|---------|-----------|
| Buffer size | `> -remoteWrite.maxBlockSize` (default: 8 MB) |
| Periodic timer | Every `-remoteWrite.flushInterval` (default: 1s) |
| Shutdown | `mustFlushOnStop()` |

On flush, the accumulated data is **zstd-compressed** (level 1) and written to the persistent queue:

```go
func (wr *writeRequest) push(pushBlock func([]byte)) {
    b := wr.pendingData.B
    zb := compressBufPool.Get()
    zb.B = zstd.CompressLevel(zb.B[:0], b, 1)
    pushBlock(zb.B)
    compressBufPool.Put(zb)
}
```

#### Persistent queue

The `persistentqueue.FastQueue` (from VictoriaMetrics shared lib) provides a two-tier buffer:

1. **In-memory blocks** for fast access (sized adaptively: `memory.Allowed() / numURLs / 10000`, capped at `100 * queues`)
2. **Disk spill** in ~500 MB chunk files when memory tier is full
3. **Configurable max disk usage** via `-remoteWrite.maxDiskUsagePerURL` (default: unlimited)

When disk is full and writes are blocked, the queue drops the oldest data.

**Location**: [`app/vlagent/remotewrite/remotewrite.go:48-281`](../app/vlagent/remotewrite/remotewrite.go#L48)

---

### 7. Remote Write — HTTP Client & Retry

**File**: [`app/vlagent/remotewrite/client.go`](../app/vlagent/remotewrite/client.go#L66)

**Key Functions**:
- [`newHTTPClient(argIdx, url, sanitizedURL, fq, concurrency)`](../app/vlagent/remotewrite/client.go#L95) - Create HTTP client
- [`init(argIdx, concurrency, sanitizedURL)`](../app/vlagent/remotewrite/client.go#L137) - Start worker goroutines
- [`runWorker()`](../app/vlagent/remotewrite/client.go#L232) - Worker loop: read from queue, send blocks
- [`sendBlockHTTP(block)`](../app/vlagent/remotewrite/client.go#L326) - Send a single block with retry
- [`getRetryDuration(retryAfter, retryDuration, maxRetryDuration)`](../app/vlagent/remotewrite/client.go#L407) - Exponential backoff calculation

```go
type client struct {
    sanitizedURL     string                         // For metrics (may hide credentials)
    remoteWriteURL   string                         // Actual URL with ?version=v1
    fq               *persistentqueue.FastQueue     // Source queue
    hc               *http.Client                   // HTTP client with auth transport
    retryMinInterval time.Duration                  // Default: 1s
    retryMaxTime     time.Duration                  // Default: 1m (max backoff cap)
    sendBlock        func(block []byte) bool        // Pluggable send function
    authCfg          *promauth.Config               // Auth configuration
    rl               *ratelimiter.RateLimiter        // Optional rate limit
    // ... metrics counters ...
    wg               sync.WaitGroup
    stopCh           chan struct{}
}
```

#### Worker pool

Each URL gets `-remoteWrite.queues` (default: 2 x CPUs) concurrent worker goroutines. Each worker reads blocks from the persistent queue and sends them:

```go
func (c *client) runWorker() {
    var block []byte
    for {
        block, ok = c.fq.MustReadBlock(block[:0])  // Blocking read from queue
        if !ok {
            return  // Queue closed
        }
        go func() {
            ch <- c.sendBlock(block)  // Send in background
        }()
        select {
        case ok := <-ch:
            if !ok {
                c.fq.MustWriteBlockIgnoreDisabledPQ(block)  // Return unsent block
                return
            }
        case <-c.stopCh:
            // Graceful shutdown: wait up to 5 seconds for in-flight send
        }
    }
}
```

#### Retry behavior

`sendBlockHTTP()` implements retry with exponential backoff:

```
HTTP error or non-2XX (except 400/404)
    ↓
retryDuration starts from jitter(retryMinInterval)
    ↓  retry
retryDuration *= 2 (first wait is ~2x retryMinInterval)
    ↓  retry
retryDuration *= 2 (capped at retryMaxTime, default: 1m)
    ↓  ...
Retries indefinitely until success or stopCh closes
```

| Status Code | Behavior |
|-------------|----------|
| 2XX | Success — block acknowledged |
| 400, 404 | Client error — block **dropped** (logged, not retried) |
| Other (5XX, etc.) | Retry with exponential backoff |
| Connection error | Retry; EOF on first attempt triggers automatic re-send |

Special handling:
- **`Retry-After` header**: If the server sends this header (RFC 7231), it takes priority over the exponential backoff
- **Jitter**: Delays use `timeutil.AddJitterToDuration` (adds up to +10%, capped at +10s)
- **Stale connections**: EOF/unexpected-EOF on the first attempt triggers an immediate retry via `doRequest()`, which helps with connection reuse issues

#### Rate limiting

Optional per-URL rate limiting via `-remoteWrite.rateLimit` (bytes/second). When enabled, `c.rl.Register(len(block))` blocks until the rate budget is available before sending.

#### Authentication

Supports basic auth, bearer token, OAuth2, custom headers, and TLS client certificates. All configured via `-remoteWrite.*` flags and managed through VictoriaMetrics' `promauth` library.

**Location**: [`app/vlagent/remotewrite/client.go:66-458`](../app/vlagent/remotewrite/client.go#L66)

---

## Key Design Patterns

### 1. Pipeline Architecture

The data flow forms a clear pipeline with distinct stages:

```
Discovery → File I/O → Parsing → Enrichment → Batching → Queue → HTTP Send
```

Each stage can be tuned independently (concurrency limits, backoff timers, buffer sizes).

### 2. Exponential Backoff with Jitter

Used in three places:
- **Kubernetes watch reconnection**: 200ms–30s ([`backoff_timer.go`](../app/vlagent/kubernetescollector/backoff_timer.go#L11))
- **File polling**: 100ms–10s ([`file_collector.go:190`](../app/vlagent/kubernetescollector/file_collector.go#L190))
- **HTTP retry**: configurable min/max ([`client.go:326`](../app/vlagent/remotewrite/client.go#L326))

All use `timeutil.AddJitterToDuration()` (up to +10% positive jitter, max +10s) to prevent synchronized retry storms across agents.

### 3. Persistent Queue with Disk Spill

The `persistentqueue.FastQueue` provides durability across vlagent restarts and network outages. Data flows: memory buffer → disk chunks → HTTP workers. This ensures:
- No data loss during brief network interruptions
- Bounded memory usage (excess spills to disk)
- Graceful degradation when disk fills (oldest data dropped)

### 4. Checkpoint-Based Recovery

The checkpoint system (inode + fingerprint + offset) handles three difficult scenarios:
- **Normal restart**: Resume from exact offset in same file
- **Rotation during downtime**: Best-effort find the rotated file by inode, finish reading it, then switch to the new file
- **Inode reuse**: Detect via fingerprint (xxhash of first line) that a file is different despite having the same inode

### 5. Interface-Based Storage Injection

The `insertutil.LogRowsStorage` interface ([`common_params.go:174`](../app/vlinsert/insertutil/common_params.go#L174)) decouples ingestion from storage. VictoriaLogs server injects `vlstorage.Storage` (local disk), while vlagent injects `remotewrite.Storage` (HTTP send). The ingestion code (`vlinsert`) is identical in both binaries.

---

## Configuration Flags

### Server

```bash
-httpListenAddr array
    TCP listen address (default: ":9429"). Set to empty to disable HTTP.

-tmpDataPath string
    Default path for storing vlagent data (queues, checkpoints)
```

### Remote Write

```bash
-remoteWrite.url array
    Remote VictoriaLogs URL(s). Example: http://victorialogs:9428/insert/native
    Multiple URLs enable replication.

-remoteWrite.tmpDataPath string
    Path for persistent queue data (default: vlagent-remotewrite-data under -tmpDataPath)

-remoteWrite.queues int
    Concurrent workers per URL (default: 2 × CPUs)

-remoteWrite.maxBlockSize bytes
    Max batch size before flush (default: 8MB)

-remoteWrite.flushInterval duration
    Periodic flush interval (default: 1s)

-remoteWrite.maxDiskUsagePerURL bytes
    Max disk buffer per URL (default: 0 = unlimited)

-remoteWrite.sendTimeout duration
    HTTP send timeout (default: 1m)

-remoteWrite.retryMinInterval duration
    Initial retry delay (default: 1s)

-remoteWrite.retryMaxTime duration
    Maximum retry delay cap (default: 1m)

-remoteWrite.rateLimit int
    Optional bytes/second rate limit per URL (default: 0 = disabled)

-remoteWrite.showURL bool
    Show URL in metrics (default: false, hidden for security)
```

### Remote Write Authentication

```bash
-remoteWrite.basicAuth.username array
-remoteWrite.basicAuth.password array
-remoteWrite.basicAuth.passwordFile array

-remoteWrite.bearerToken array
-remoteWrite.bearerTokenFile array

-remoteWrite.oauth2.clientID array
-remoteWrite.oauth2.clientSecret array
-remoteWrite.oauth2.clientSecretFile array
-remoteWrite.oauth2.tokenUrl array
-remoteWrite.oauth2.scopes array
-remoteWrite.oauth2.endpointParams array

-remoteWrite.tlsCertFile array
-remoteWrite.tlsKeyFile array
-remoteWrite.tlsCAFile array
-remoteWrite.tlsServerName array
-remoteWrite.tlsInsecureSkipVerify array
-remoteWrite.tlsHandshakeTimeout duration (default: 20s)

-remoteWrite.headers array
    Custom HTTP headers (multiple headers delimited by ^^)

-remoteWrite.proxyURL array
    HTTP/HTTPS/SOCKS5 proxy
```

### Kubernetes Collector

```bash
-kubernetesCollector bool
    Enable Kubernetes log collection (default: false)

-kubernetesCollector.logsPath string
    Path to container logs directory (default: /var/log/containers)

-kubernetesCollector.checkpointsPath string
    Path to checkpoint file (default: vlagent-kubernetes-checkpoints.json under -tmpDataPath)

-kubernetesCollector.excludeFilter string
    LogsQL filter for excluding containers by metadata fields

-kubernetesCollector.tenantID string
    Tenant ID for collected logs (default: "0:0")

-kubernetesCollector.streamFields array
    Stream fields (default: kubernetes.container_name, kubernetes.pod_name, kubernetes.pod_namespace)

-kubernetesCollector.msgField array
    Message field candidates (default: message, msg, log)

-kubernetesCollector.timeField array
    Time field candidates (default: time, timestamp, ts)

-kubernetesCollector.ignoreFields array
    Fields to ignore during ingestion

-kubernetesCollector.decolorizeFields array
    Fields to strip ANSI color codes from

-kubernetesCollector.extraFields string
    Extra fields as JSON. Example: '{"cluster":"prod","env":"us-east"}'

-kubernetesCollector.includePodLabels bool (default: true)
-kubernetesCollector.includePodAnnotations bool (default: false)
-kubernetesCollector.includeNodeLabels bool (default: false)
-kubernetesCollector.includeNodeAnnotations bool (default: false)
```

---

## Document Maintenance Guidelines

### File Linking Syntax and Policy

This document uses VS Code-compatible markdown links to enable quick navigation from documentation to source code. All file references should follow these guidelines to maintain clickability.

#### Required Format

All file and line number references must use the following format:

```markdown
[DisplayText](path/to/file.go#LLineNumber)
```

**Key requirements**:
- Use capital `L` prefix before the line number (e.g., `#L28`, not `#28`)
- Use consistent relative paths from this document location (this file currently uses `../` to reach repo root)
- Include line numbers wherever possible

#### Standard Patterns

**1. Section Headers - File References**

```markdown
**File**: [`app/vlagent/main.go`](../app/vlagent/main.go#L34)
```

**2. Function References in Key Functions Lists**

```markdown
**Key Functions**:
- [`Init(tmpDataPath)`](../app/vlagent/remotewrite/remotewrite.go#L79) - Initialize remote write
```

**3. Flow Diagram References**

```markdown
logFile.readLines(stopCh, proc)          [logfile.go:90](../app/vlagent/kubernetescollector/logfile.go#L90)
```

**4. Location References**

```markdown
**Location**: [`app/vlagent/remotewrite/client.go:66-458`](../app/vlagent/remotewrite/client.go#L66)
```

#### Line Number Selection

When linking to code:

- **Single function**: Link to the function definition line
- **Struct definition**: Link to the struct type declaration
- **Interface**: Link to the interface declaration
- **Code section**: Link to the first line of the section
- **Range (lines X-Y)**: Link to the start line (X)

#### Examples

Correct:
```markdown
- [`Init(tmpDataPath)`](../app/vlagent/remotewrite/remotewrite.go#L79) - Initialize remote write
- **File**: [`client.go`](../app/vlagent/remotewrite/client.go#L66)
- [pendinglogrows.go:72](../app/vlagent/remotewrite/pendinglogrows.go#L72)
```

Incorrect:
```markdown
- `Init(tmpDataPath)` - Initialize remote write (line 79)  # Not clickable
- **File**: `client.go` (line 66)  # Not clickable
- [pendinglogrows.go:72](../app/vlagent/remotewrite/pendinglogrows.go#72)  # Missing L prefix
```

#### Updating File References

When adding new file references to this document:

1. **Always include line numbers** - Even if referencing a general concept, link to the most relevant line
2. **Use the #L prefix** - Required for VS Code compatibility
3. **Test the links** - Cmd/Ctrl + Click in VS Code to verify they navigate correctly
4. **Keep display text concise** - Balance readability with information
5. **Update stale references** - If code moves, update line numbers accordingly

#### Verification Checklist

Before committing changes to this document:

- [ ] All file paths use a consistent relative base (as used throughout this file)
- [ ] All line number anchors use `#L` prefix (capital L)
- [ ] All links tested and navigate correctly in VS Code
- [ ] No plain text file references (should be clickable markdown links)
- [ ] Display text is appropriate for context (full path, filename, or function)

#### Tools for Validation

You can verify all links have the correct format using:

```bash
# Check for links missing the #L prefix
grep -n '\](.*\.go#[0-9]' onboarding/onboarding-vlagent.md

# Count total clickable line references
grep -o "#L[0-9]\+" onboarding/onboarding-vlagent.md | wc -l

# Find non-clickable file references
grep -n '`app/.*\.go`' onboarding/onboarding-vlagent.md | grep -v '\]('
```

#### Why This Matters

Clickable file references significantly improve the onboarding experience by:
- Reducing friction when exploring the codebase
- Enabling instant navigation from concept to implementation
- Making the documentation a living, interactive guide
- Helping developers quickly verify documented behavior against actual code

**Remember**: Every file reference is an opportunity to help a developer learn faster. Make them all clickable!
