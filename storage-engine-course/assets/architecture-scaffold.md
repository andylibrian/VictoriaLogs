# VictoriaLogs Architecture Scaffold

## High-Level Architecture

```mermaid
graph TB
    subgraph "Ingestion Layer"
        INSERT[HTTP Endpoints<br/>/insert/*]
        VLINSERT[vlinsert<br/>Protocol Handlers]
    end

    subgraph "Storage Layer"
        STORAGE[storage.go]
        PARTITION[Partition<br/>Per-Tenant, Per-Day]
        INDEXDB[indexdb<br/>Stream Metadata]
        DATADB[datadb<br/>Log Row Data]
    end

    subgraph "Query Layer"
        SELECT[HTTP Endpoints<br/>/select/*]
        VLSELECT[vlselect<br/>Query Engine]
        LOGSQL[LogsQL Parser]
    end

    INSERT --> VLINSERT
    VLINSERT --> STORAGE
    STORAGE --> PARTITION
    PARTITION --> INDEXDB
    PARTITION --> DATADB

    SELECT --> VLSELECT
    VLSELECT --> LOGSQL
    LOGSQL --> STORAGE
    STORAGE --> DATADB
    STORAGE --> INDEXDB
```

## Data Flow: Write Path

```mermaid
graph LR
    subgraph "Write Path"
        ROW[Log Row] --> BUFFER[rowsBuffer<br/>Sharded Buffer]
        BUFFER --> SORT[Sorted LogRows]
        SORT --> BLOCK[Block<br/>Columnar Encoding]
        BLOCK --> INMEM[partInmemory]
        INMEM --> |Flush| SMALL[partSmall<br/>On-Disk]
        SMALL --> |Merge| BIG[partBig<br/>Merged]
    end
```

## Data Flow: Query Path

```mermaid
graph LR
    subgraph "Query Path"
        QUERY[LogsQL Query] --> PARSE[Parse & Plan]
        PARSE --> PRUNE[Metadata Pruning]
        PRUNE --> BLOOM[Bloom Filter Check]
        BLOOM --> SCAN[Block Scan]
        SCAN --> FILTER[Row Filtering]
        FILTER --> AGG[Aggregation<br/>if any]
        AGG --> RESULT[Results]
    end
```

## Key Components Reference

| Component | Source File | Responsibility |
|-----------|-------------|----------------|
| storage | `lib/logstorage/storage.go` | Top-level coordinator, partition management |
| partition | `lib/logstorage/partition.go` | Per-tenant, per-day data container |
| indexdb | `lib/logstorage/indexdb.go` | Stream ID and tag indexing |
| datadb | `lib/logstorage/datadb.go` | Log row storage, parts, merges |
| block | `lib/logstorage/block.go` | Columnar block encoding |
| bloomfilter | `lib/logstorage/bloomfilter.go` | Probabilistic skip indexing |

## Your Task

As you progress through levels, annotate this diagram with:
1. Specific function names at each transition
2. Key invariants at each boundary
3. Performance bottlenecks you discover
