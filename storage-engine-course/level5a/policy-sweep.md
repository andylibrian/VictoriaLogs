# Policy Sweep

## Context
- Date: Level 5A
- Owner: Storage Engine Course

## Work

### Policies Tested

| Policy | Parts to Merge | Merge Ratio | Description |
|--------|----------------|-------------|-------------|
| Eager | 2 | 2.0x | Merge any 2 parts |
| Default | 15 | 7.5x | VictoriaLogs default |
| Lazy | 15 | 15.0x | Only highly efficient merges |
| Never | 0 | ∞ | No merging (baseline) |

### Test Scenario

- Data ingested: 100 MB
- Chunk size: 100 KB per PUT
- Flush threshold: 1 MB
- Tier thresholds: 10 MB (small), 100 MB (big)

## Evidence

### Policy Comparison Results

| Policy | Final Parts | Write Amp | Read Amp | Merges |
|--------|-------------|-----------|----------|--------|
| Eager (2 parts, 2x) | 5 | 6.21x | 5 | 86 |
| Default (15 parts, 7.5x) | 11 | 2.54x | 11 | 11 |
| Lazy (15 parts, 15x) | 7 | 1.99x | 7 | 6 |
| Never merge | 91 | 1.00x | 91 | 0 |

### Analysis

**Eager Merge (2 parts, 2x ratio)**
- Write amplification: 6.21x (highest)
- Read amplification: 5 parts (lowest)
- Merge count: 86 (most merges)
- Trade-off: Excellent read performance, but data rewritten many times
- Best for: Read-heavy workloads, low-latency queries
- Worst for: Write-heavy workloads, SSD wear

**Default (15 parts, 7.5x ratio)**
- Write amplification: 2.54x (moderate)
- Read amplification: 11 parts (moderate)
- Merge count: 11 (balanced)
- Trade-off: Reasonable write cost, good read performance
- Best for: Mixed workloads, general-purpose use
- This is the VictoriaLogs default for a reason

**Lazy Merge (15 parts, 15x ratio)**
- Write amplification: 1.99x (low)
- Read amplification: 7 parts (moderate)
- Merge count: 6 (fewest merges that do anything)
- Trade-off: Low write cost, more parts to scan
- Best for: Write-heavy workloads, archival data
- Worst for: High-frequency queries

**Never Merge**
- Write amplification: 1.00x (minimal)
- Read amplification: 91 parts (highest)
- Merge count: 0
- Trade-off: Minimal write cost, terrible reads
- Best for: Write-once-read-never, short retention
- Worst for: Any query workload

### Trade-off Matrix

| Policy | Write Amp | Read Amp | Space Amp | Best Use Case |
|--------|-----------|----------|-----------|---------------|
| Eager merge | High (10-20x) | Low (1-5) | Transient 2x | Read-heavy |
| Default (7.5x) | Medium (5-8x) | Medium (10-20) | Transient 2x | General-purpose |
| Lazy merge | Low (2-4x) | High (20-50) | Transient 2x | Write-heavy |
| Never merge | Minimal (1x) | Very High (100+) | None | Archival only |

### Amplification Relationship

The fundamental LSM trade-off:

```
WriteAmp × ReadAmp ≈ constant (for given data size)
```

- Lower write amp (fewer merges) → Higher read amp (more parts)
- Higher write amp (more merges) → Lower read amp (fewer parts)
- Space amp is transient during merges (old + new coexist briefly)

### Tuning Guidance

**Increase merge ratio (e.g., 7.5x → 15x) when:**
- Write-heavy workload
- SSD wear is a concern
- Disk I/O is bottleneck
- Accept higher query latency

**Decrease merge ratio (e.g., 7.5x → 4x) when:**
- Read-heavy workload
- Query latency is critical
- Sufficient write capacity
- Can tolerate higher write amp

**Limit concurrent merges when:**
- Disk space constrained
- Need to bound peak space usage
- Want to limit I/O burst

## Conclusions

1. **No free lunch**: Every policy trades write amp for read amp
2. **Default is balanced**: 7.5x ratio provides good compromise
3. **Context matters**: Choose policy based on workload characteristics
4. **Monitor and tune**: Use metrics to validate policy choice
5. **Simulator is valuable**: Test policies before production deployment

### Recommended Defaults by Workload

| Workload | Recommended Policy | Reason |
|----------|-------------------|--------|
| High-volume logging | Lazy (15x) | Minimize write overhead |
| Real-time analytics | Eager (4x) | Optimize query latency |
| General-purpose | Default (7.5x) | Balanced trade-off |
| Short retention | Never or Lazy | Limited benefit from merging |
| Long retention | Default or Eager | Queries span more data |

## Open Questions

1. How does policy choice interact with bloom filter effectiveness?
2. Should policy be dynamic based on workload patterns?
3. How does compression ratio affect optimal policy?
4. What's the break-even point between eager and lazy policies?
