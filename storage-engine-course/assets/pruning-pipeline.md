# Query Pruning Pipeline

## Pipeline Overview

```mermaid
flowchart TD
    Q[Query Received] --> P1[Partition Time Pruning]
    P1 --> |skip| P2[Part Time Pruning]
    P2 --> |skip| P3[Tenant/Stream ID Pruning]
    P3 --> |skip| P4[Block Header Pruning]
    P4 --> |skip| P5[Bloom Filter Check]
    P5 --> |maybe| P6[Value Decode + Row Filter]
    P6 --> P7[Result Assembly]

    P1 -.-> |eliminate| E1[Old/Empty Partitions]
    P2 -.-> |eliminate| E2[Parts outside time range]
    P3 -.-> |eliminate| E3[Blocks with wrong stream]
    P4 -.-> |eliminate| E4[Blocks with no matching columns]
    P5 -.-> |eliminate| E5[Blocks with Bloom miss]
```

## Pruning Cost vs Benefit

```mermaid
graph LR
    subgraph "Cheap Pruning (Metadata Only)"
        A1[Partition time check]
        A2[Part time check]
        A3[Stream ID binary search]
    end

    subgraph "Medium Cost (Block Header)"
        B1[Column existence]
        B2[Min/max value bounds]
    end

    subgraph "Higher Cost (I/O Required)"
        C1[Bloom filter read]
        C2[Value block decode]
    end

    subgraph "Full Cost"
        D1[Row-by-row evaluation]
    end

    A1 --> A2 --> A3 --> B1 --> B2 --> C1 --> C2 --> D1
```

## Bloom Filter Decision Tree

```mermaid
flowchart TD
    START[Need to filter column?] --> CONST{Const column?}
    CONST --> |Yes| FAST[Use const value<br/>O(1) check]
    CONST --> |No| DICT{Dict encoded?}
    DICT --> |Yes| LOOKUP[Dict lookup<br/>O(1) check]
    DICT --> |No| BLOOM{Bloom available?}
    BLOOM --> |No| SCAN[Full value scan]
    BLOOM --> |Yes| CHECK{Bloom check}
    CHECK --> |false| SKIP[Skip block<br/>Guaranteed no match]
    CHECK --> |true| VERIFY[Decode & verify<br/>May have false positive]
    VERIFY --> MATCH{Match found?}
    MATCH --> |Yes| INCLUDE[Include in results]
    MATCH --> |No| EXCLUDE[Exclude row]
```

## Pruning Statistics to Track

| Stage | Metric | Formula |
|-------|--------|---------|
| Partition | `partitions_scanned / partitions_total` | Lower = better |
| Part | `parts_scanned / parts_total` | Lower = better |
| Block Header | `blocks_after_header / blocks_before_header` | Reduction ratio |
| Bloom | `blocks_after_bloom / blocks_before_bloom` | Reduction ratio |
| Bloom FP | `false_positives / bloom_matches` | Should be < 10% |
| Final | `rows_returned / rows_scanned` | Selectivity |

## Example: Query `_stream:*error*` AND `message:*timeout*`

```mermaid
flowchart LR
    subgraph "Stage 1: Partition"
        P1[100 partitions] --> P2[20 partitions<br/>time range filter]
    end

    subgraph "Stage 2: Part"
        P2 --> PT1[200 parts] --> PT2[40 parts<br/>time range]
    end

    subgraph "Stage 3: Stream"
        PT2 --> S1[500 blocks] --> S2[50 blocks<br/>stream filter]
    end

    subgraph "Stage 4: Bloom"
        S2 --> B1[50 blocks] --> B2[5 blocks<br/>Bloom on 'timeout']
    end

    subgraph "Stage 5: Scan"
        B2 --> V1[1000 rows scanned] --> V2[23 rows match]
    end
```

## Your Task

As you trace queries in Level 8:
1. Fill in actual numbers for a real query
2. Identify which stage provides most reduction
3. Propose an optimization if a stage is ineffective
