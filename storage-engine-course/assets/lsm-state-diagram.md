# LSM State Diagram

## Part Lifecycle (VictoriaLogs Style)

```mermaid
stateDiagram-v2
    [*] --> InMemory: Write ingested
    InMemory --> Small: Flush (size/time trigger)
    Small --> Big: Merge (heuristic selected)
    Big --> Big: Merge (consolidation)
    Small --> Deleted: Retention/Delete
    Big --> Deleted: Retention/Delete
    InMemory --> Deleted: Partition drop
    Deleted --> [*]: Cleanup
```

## Merge State Machine

```mermaid
stateDiagram-v2
    [*] --> Idle: Parts exist
    Idle --> Selecting: Merge worker wakes
    Selecting --> Merging: Candidates found
    Selecting --> Idle: No candidates (skip)
    Merging --> Installing: Merge complete
    Installing --> Cleanup: Swap successful
    Cleanup --> Idle: Old parts removed
    
    Merging --> Failed: Error
    Failed --> Idle: Retry later
```

## Write Amplification Visualization

```mermaid
graph LR
    subgraph "Level 0 - In Memory"
        M1[Memtable 1]
        M2[Memtable 2]
    end

    subgraph "Level 1 - Small Parts"
        S1[Small 1]
        S2[Small 2]
        S3[Small 3]
        S4[Small 4]
    end

    subgraph "Level 2 - Big Parts"
        B1[Big 1]
        B2[Big 2]
    end

    M1 --> |Flush| S1
    M2 --> |Flush| S2
    S1 --> |Merge| B1
    S2 --> |Merge| B1
    S3 --> |Merge| B2
    S4 --> |Merge| B2
```

## Key State Invariants

| State | Invariant |
|-------|-----------|
| InMemory | Mutable until frozen for flush |
| Small | Immutable, may overlap with other Small |
| Big | Immutable, non-overlapping time ranges (after merge) |
| Merging | Source parts remain readable until swap |
| Installing | Atomic swap: readers see old OR new, never mixed |

## Merge Candidate Selection Logic

```
for each part p in sorted-by-size:
    find overlapping parts with p
    if total_size >= minMergeMultiplier * p.size:
        add to candidates
    if enough candidates:
        return candidates
return none (skip merge)
```

## Your Task

As you implement Level 5 phases:
1. Add state transition triggers (what causes each arrow?)
2. Document failure modes at each state
3. Identify where backpressure applies
