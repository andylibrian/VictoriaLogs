# Level 2 Concurrency Analysis

## Context
- Level: 2 - Ingestion Skeleton: Rows, Streams, Buffers
- Date: 2026-03-02

## Problem: Lock Contention in High-Throughput Ingestion

### The Naive Approach: Single Global Lock

```
┌─────────────────────────────────────────────────────────────┐
│                    rowsBuffer (Global Lock)                  │
│                                                              │
│  ┌─────────┐                                                │
│  │   mu    │ ◄─── ALL goroutines compete for this lock      │
│  └─────────┘                                                │
│      │                                                       │
│      ▼                                                       │
│  ┌─────────────────────────────────────┐                    │
│  │           logRows buffer            │                    │
│  └─────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────┘

Goroutines:  G1 ──┐
             G2 ──┼──► ALL wait for same lock
             G3 ──┤
             G4 ──┘
```

**Problem:** With N concurrent ingestion goroutines, only 1 can hold the lock at a time. The other N-1 are blocked, waiting.

**Cost:**
- Context switches when lock is contended
- CPU cache thrashing as lock bounces between cores
- Serialized execution defeats parallelism

### The Solution: Sharded Buffer

```
┌──────────────────────────────────────────────────────────────────────┐
│                       rowsBuffer (Sharded)                            │
│                                                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │   Shard 0   │  │   Shard 1   │  │   Shard 2   │  │   Shard 3   │ │
│  │             │  │             │  │             │  │             │ │
│  │  mu0 (lock) │  │  mu1 (lock) │  │  mu2 (lock) │  │  mu3 (lock) │ │
│  │  logRows    │  │  logRows    │  │  logRows    │  │  logRows    │ │
│  │  [64 bytes] │  │  [64 bytes] │  │  [64 bytes] │  │  [64 bytes] │ │
│  │   padding   │  │   padding   │  │   padding   │  │   padding   │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │
│        ▲                ▲                ▲                ▲          │
└────────┼────────────────┼────────────────┼────────────────┼──────────┘
         │                │                │                │
    G1 ──┘           G2 ──┘           G3 ──┘           G4 ──┘
    
    Round-robin distribution: nextIdx.Add(1) % numShards
```

**Benefit:** With N shards, only ~1/Nth of goroutines compete for each lock.

## Sharding Strategy: Round-Robin

```go
// datadb.go:938-940
idx := rb.nextIdx.Add(1) % uint64(len(shards))
shard := &rb.shards[idx]
```

**Why round-robin?**
1. **Simple and fast**: Single atomic increment
2. **Even distribution**: Statistically spreads load across shards
3. **No routing logic needed**: Don't need to hash by streamID or tenant

**Alternative considered: Hash-based routing**
- Route to shard based on `hash(streamID) % numShards`
- Would keep same-stream rows together
- BUT: Creates "hot shards" for popular streams
- Round-robin is more balanced

## Contention Reduction Math

**Single lock scenario:**
- N goroutines, 1 lock
- Average waiters: N-1
- Lock utilization: ~1/N (only 1/N of time doing useful work)

**Sharded scenario (S shards):**
- N goroutines, S locks
- Average goroutines per shard: N/S
- Average waiters per lock: (N/S) - 1
- Lock utilization: ~S/N (S times better)

**Example:** 100 goroutines, 8 CPU cores

| Approach | Shards | Avg Waiters | Speedup |
|----------|--------|-------------|---------|
| Global lock | 1 | 99 | 1x |
| Sharded | 4 | 24 | ~4x |
| Sharded | 8 | 11.5 | ~8x |
| Sharded | 16 | 5.25 | ~12x* |

*Diminishing returns beyond CPU count due to OS scheduling

## Cache Line Padding: Preventing False Sharing

```go
// datadb.go:918
type rowsBufferShard struct {
    // ... fields ...
    
    // padding for preventing false sharing between shards
    _ [atomicutil.CacheLineSize]byte
}
```

**The Problem: False Sharing**

Without padding, adjacent shards may share a CPU cache line (typically 64 bytes):

```
Memory layout without padding:
┌──────────────────────────────────────────────────────────┐
│ Cache Line (64 bytes)                                    │
│                                                          │
│ [shard0.mu (8B)] [shard0.lr (8B)] [shard1.mu (8B)] ...   │
│                                                          │
└──────────────────────────────────────────────────────────┘
        CPU 0 modifies           CPU 1 must reload
        shard0.mu                entire cache line!
```

When CPU 0 modifies `shard0.mu`, the entire cache line is invalidated on CPU 1, even though `shard1.mu` wasn't touched. This causes unnecessary cache misses.

**The Solution: Padding**

```
Memory layout with padding:
┌──────────────────────────────────────────────────────────┐
│ Cache Line 0 (64 bytes)                                  │
│ [shard0.mu] [shard0.lr] [padding.....................]   │
└──────────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────────┐
│ Cache Line 1 (64 bytes)                                  │
│ [shard1.mu] [shard1.lr] [padding.....................]   │
└──────────────────────────────────────────────────────────┘

Each shard on its own cache line → No false sharing
```

## Edge Cases Where Contention Still Exists

### Case 1: Hot Stream

If one stream receives disproportionate traffic:

```
Stream {app="nginx"} → 90% of all logs

All these logs go to various shards (round-robin)
BUT: During flush, they're all sorted together
→ Single block writer for that stream
```

**Mitigation:** Flush is fast (in-memory). Disk writes are asynchronous.

### Case 2: Shard Lock Held Long

If a single `MustAddRows` call has a very large batch:

```go
// Large batch holds lock longer
shard.mu.Lock()
for i := range largeBatch {  // 10,000 rows
    shard.lr.mustAddRow(...)
    // Lock held the entire time
}
shard.mu.Unlock()
```

**Mitigation:** Batches are typically small. Size limit enforced upstream.

### Case 3: Flush Under Load

When flush triggers, the shard lock is held during sort and part creation:

```go
shard.mu.Lock()
sort.Sort(shard.lr)        // O(n log n)
mp.mustInitFromRows(lr)    // Block creation
shard.mu.Unlock()
```

**Mitigation:** Flush threshold (87.5%) leaves room for incoming rows. Flush is relatively fast for typical buffer sizes.

## Why CPU Count for Shard Count?

```go
// datadb.go:899
shards := make([]rowsBufferShard, cgroup.AvailableCPUs())
```

**Reasoning:**
1. More shards than CPUs → unnecessary memory overhead
2. Fewer shards than CPUs → some CPUs can't be fully utilized
3. Shards = CPUs → optimal parallelism

**Example:** On an 8-core machine:
- 8 shards = 8 independent lock domains
- Up to 8 goroutines can write simultaneously without contention
- Beyond 8: some contention is inevitable (OS scheduling)

## Evidence

Lab program `concurrency_demo.go` demonstrates:
- Global lock vs sharded buffer performance
- Contention measurement
- Speedup calculation

## Conclusions

1. **Sharding converts O(N) contention to O(N/S)**: S shards reduce lock competition by factor of S
2. **Round-robin is sufficient**: No need for complex routing logic
3. **Cache line padding matters**: Prevents invisible performance degradation
4. **Shard count = CPU count**: Optimal balance of parallelism and overhead

## Open Questions

- How does contention change with SSD vs HDD?
- Should shard count be configurable for NUMA systems?
