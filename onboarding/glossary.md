# Glossary

This glossary defines terms used across the onboarding docs that may not be familiar to all readers. Each entry is kept brief; follow the links for full context.

---

**Arena allocation** — A memory management pattern where many small allocations share a single contiguous byte slice (the "arena"). Instead of allocating each string or field independently (which creates GC pressure), values are appended to one growing `[]byte` and referenced by offset. VictoriaLogs uses this in [`LogRows`](./onboarding-insert-flow.md#4-in-memory-batching) and block encoding to minimize Go garbage collection overhead during high-throughput ingestion.

**Bloom filter** — A space-efficient probabilistic data structure that answers "is this element in the set?" with either "definitely no" or "maybe yes." VictoriaLogs stores a bloom filter per column per block. At query time, search tokens are checked against the bloom filter first — if it says "no," the entire block is skipped without reading its values. False positives are possible but false negatives are not. See [Storage Engine: Bloom Filters](./onboarding-storage-engine.md#9-bloom-filters).

**Block** — The fundamental unit of storage in VictoriaLogs. A block contains log entries for a single stream within a single part, stored in columnar format. Each block has a maximum uncompressed size of 2 MB. Blocks are the granularity at which bloom filters are checked and data is read from disk during queries. See [Storage Engine: Block Encoding](./onboarding-storage-engine.md#7-block-encoding).

**Columnar storage** — A data layout where each field (column) is stored separately rather than keeping all fields of a row together. This enables type-specific compression (e.g., integers as fixed-width values, repeated values as dictionary bytes) and allows queries to read only the columns they need. Contrast with row-oriented storage where reading one field requires reading the entire row. See [Storage Engine: Column Value Encoding](./onboarding-storage-engine.md#8-column-value-encoding).

**Const column** — An optimization for block encoding. When all values in a column within a block are identical (e.g., `hostname`, `log_level`), the value is stored once in the block header instead of once per row. This saves space and speeds up queries because the value is available without reading the values file. See [Storage Engine: Const Column Optimization](./onboarding-storage-engine.md#const-column-optimization).

**DataBlock** — The in-memory columnar representation of query results, used to stream data between components. Contains a list of `BlockColumn` entries (each with a name and string values). Used both for local pipe processing and for binary serialization in cluster inter-node communication. See [Select Flow: Result Delivery](./onboarding-select-flow.md#7-result-delivery).

**datadb** — The subdirectory within each partition that stores actual log data. Internally manages an LSM-tree of parts (in-memory, small file, big file) with background merge workers. See [Storage Engine: DataDB Layer](./onboarding-storage-engine.md#3-datadb-layer).

**Dictionary encoding** — A compression technique where a column with few unique values (up to 8, totaling up to 256 bytes) is encoded by storing the unique values once as a dictionary and representing each row's value as a single-byte index into that dictionary. This is the fastest encoding for queries because filtering can compare against dictionary entries directly without reading the values file. See [Storage Engine: Column Value Encoding](./onboarding-storage-engine.md#8-column-value-encoding).

**indexdb** — The subdirectory within each partition that stores stream metadata as an inverted index. Maps stream IDs to stream tags and provides reverse lookups (tag value to stream IDs) for `_stream:{...}` filters. Backed by VictoriaMetrics' mergeset library. See [Storage Engine: IndexDB](./onboarding-storage-engine.md#10-indexdb-stream-metadata).

**LSM-tree** (Log-Structured Merge-tree) — A data structure optimized for write-heavy workloads. New data is written sequentially to in-memory buffers, which are periodically flushed to immutable sorted files on disk. Background merge workers continuously compact smaller files into larger ones to reduce read amplification. VictoriaLogs uses a three-tier LSM design: in-memory parts, small file parts, and big file parts. See [Storage Engine: Merge & Compaction](./onboarding-storage-engine.md#merge--compaction).

**mergeset** — A VictoriaMetrics-internal library (`lib/mergeset`) that implements a sorted key-value store with background merging and compaction. Used by indexdb to store stream metadata. Not a public/external library — it is vendored within the VictoriaMetrics codebase. See [Storage Engine: IndexDB](./onboarding-storage-engine.md#10-indexdb-stream-metadata).

**NDJSON** (Newline-Delimited JSON) — A streaming JSON format where each line is a complete, independent JSON object. VictoriaLogs uses NDJSON (content type `application/stream+json` or `application/x-ndjson`) for streaming query results from `/select/logsql/query` and `/select/logsql/tail`. This allows clients to start processing results before the query completes, unlike regular JSON which requires the entire response to be valid.

**Part** — A self-contained unit of stored data within a datadb. Each part contains sorted blocks for one or more streams, along with its own index, bloom filters, and metadata. Parts are immutable once written — new data creates new parts, and background merging combines small parts into larger ones. VictoriaLogs has three tiers: in-memory parts, small file parts (cached in OS page cache), and big file parts. See [Storage Engine: File-Backed Parts](./onboarding-storage-engine.md#6-file-backed-parts).

**Partition** — A self-contained directory holding one calendar day of log data, named in YYYYMMDD format (e.g., `20260212`). Each partition contains an indexdb and a datadb. Day-based partitioning enables cheap retention (delete the directory) and efficient time-range queries (skip partitions outside the range). See [Storage Engine: Partition Layer](./onboarding-storage-engine.md#2-partition-layer) and [Partition Lifecycle](./onboarding-partition-lifecycle.md).

**Pipe** — A processing stage in a LogsQL query, chained with `|`. Examples: `| filter level:error`, `| stats count() as total`, `| sort by (_time)`, `| limit 100`. Pipes transform, filter, aggregate, or reorder the rows produced by the root filter. Each pipe type implements a `pipeProcessor` interface for runtime execution. See [LogsQL Parser & Pipes](./onboarding-logsql-parser-pipes.md#4-pipe-parsing).

**Reference counting** — A concurrency-safety pattern used for partitions and parts. Each access increments a counter (`incRef`); when done, the counter is decremented (`decRef`). The resource is only closed or deleted when the counter reaches zero, ensuring no concurrent reader or writer is interrupted. See [Partition Lifecycle: partitionWrapper](./onboarding-partition-lifecycle.md#partitionwrapper-and-reference-counting).

**Stream / StreamID** — The primary grouping key for log data in VictoriaLogs. A stream is defined by a set of label key-value pairs (configured via `_stream_fields` at ingestion). The stream ID is a 128-bit hash of canonically sorted stream labels, combined with the tenant ID. All blocks within a part are sorted by stream ID, so blocks for the same stream are physically adjacent. See [Storage Engine: Stream Identification](./onboarding-storage-engine.md#stream-identification).

**Tenant / TenantID** — VictoriaLogs' multi-tenancy identifier, consisting of an `AccountID` (uint32) and `ProjectID` (uint32). Extracted from HTTP headers during ingestion and query. Tenants are isolated — queries only see data for the requested tenant. The zero tenant (`0:0`) is the default when no headers are provided. See [System Overview: Tenant Propagation](./onboarding-system-overview.md#tenant-propagation).

**zstd** (Zstandard) — A fast compression algorithm developed by Facebook/Meta, used by VictoriaLogs for inter-node communication (cluster insert and select protocols) and for compressing index blocks on disk. Offers better compression ratios than gzip at similar or faster speeds. VictoriaLogs uses compression level 1 (fastest) for network transfers. Can be disabled per-direction via `-insert.disableCompression` and `-select.disableCompression`.
