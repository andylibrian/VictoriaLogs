# Level 10 Retention Incident Drill

## Context
- Level: 10 - Lifecycle Ops, Retention/Delete Semantics, and Capstone
- Date: 2026-03-02

## Incident Scenario

**Simulated conditions:**
- Storage path: `/data/vlogs` (500 GB volume)
- Current usage: 450 GB (90% full)
- Configured retention: 30 days
- Ingestion rate: 15 GB/day
- Partitions: 30 (one per day)
- `maxDiskSpaceUsageBytes`: 480 GB (96% of volume)
- `minFreeDiskSpaceBytes`: 10 GB

**Incident trigger:**
Ingestion spike at 08:00 increases rate to 25 GB/day for 3 days, consuming disk space faster than retention can reclaim.

## Timeline Simulation

### Day 1: Normal Operation (08:00)

```
Status:
  Disk usage: 450 GB / 500 GB (90%)
  Free space: 50 GB
  Oldest partition: 2026-01-01
  Newest partition: 2026-01-30

Retention watcher (runs hourly):
  No action - all partitions within 30-day window
  Next run: 09:00

Disk pressure watcher (runs every 10s):
  totalSize: 450 GB
  maxDiskSpaceUsageBytes: 480 GB
  450 < 480 → No action needed
```

### Day 2: Spike Begins (08:00)

```
Status:
  Disk usage: 465 GB / 500 GB (93%)
  Free space: 35 GB
  Ingestion rate: 25 GB/day (spike started)

Retention watcher (09:00):
  Partition 2026-01-01 age: 31 days
  2026-01-01 + 30 days < 2026-02-01 → EXPIRED
  
  Actions:
    1. Remove 2026-01-01 from active list
    2. Add to deletedPartitions set
    3. ptw.mustDrop = true
    4. ptw.decRef() → refCount: 1 → 0
    
  Result: Partition directory deleted
  Disk usage after: 450 GB (15 GB reclaimed)
  
Disk pressure watcher:
  450 < 480 → No action needed
```

### Day 3: Spike Continues (08:00)

```
Status:
  Disk usage: 475 GB / 500 GB (95%)
  Free space: 25 GB
  Ingestion rate: 25 GB/day

Retention watcher (09:00):
  Partition 2026-01-02 age: 31 days
  2026-01-02 expired → deleted
  Disk usage after: 460 GB

Disk pressure watcher (08:00:10):
  totalSize: 475 GB
  475 < 480 → No action
```

### Day 4: Disk Pressure Triggered (08:00)

```
Status:
  Disk usage: 485 GB / 500 GB (97%)
  Free space: 15 GB
  Ingestion rate: 25 GB/day (spike continues)

Retention watcher (09:00):
  Partition 2026-01-03 not yet 31 days old
  No action

Disk pressure watcher (08:00:10):
  totalSize: 485 GB
  485 > 480 → EMERGENCY TRIGGERED
  
  Actions:
    1. Find oldest partition: 2026-01-03
    2. Keep newest 2 partitions (2026-01-30, 2026-01-31)
    3. 2026-01-03 is not newest → can drop
    4. Remove 2026-01-03
    5. totalSize: 485 - 15 = 470 GB
    6. 470 < 480 → Stop
  
  Disk usage after: 470 GB
```

### Day 5: Read-Only Mode (08:00)

```
Status:
  Disk usage: 490 GB / 500 GB (98%)
  Free space: 10 GB
  Spike ended, but accumulation continues

Disk pressure watcher (08:00:10):
  totalSize: 490 GB
  490 > 480 → EMERGENCY TRIGGERED
  
  Actions:
    1. Find oldest partition: 2026-01-04
    2. Can drop → Remove 2026-01-04
    3. totalSize: 490 - 15 = 475 GB
    4. 475 < 480 → Stop
  
  Disk usage after: 475 GB

  Check free disk space:
    Free: 25 GB
    minFreeDiskSpaceBytes: 10 GB
    25 > 10 → OK, continue normal operation
```

### Alternative: Read-Only Mode Triggered

```
If free space had dropped below 10 GB:

Disk pressure watcher:
  Free: 8 GB
  minFreeDiskSpaceBytes: 10 GB
  8 < 10 → READ-ONLY MODE ACTIVATED
  
  Actions:
    1. Set IsReadOnly = true
    2. All write operations return HTTP 429 (Too Many Requests)
    3. Read operations continue normally
    4. Alert sent to operators

Recovery:
  - Add disk capacity
  - Increase maxDiskSpaceUsageBytes
  - Reduce retention period
  - Force delete old partitions manually
```

## Expected Behavior Analysis

### Safeguards

| Safeguard | Mechanism | Effect |
|-----------|-----------|--------|
| Retention | Hourly watcher | Removes partitions older than retention period |
| Disk pressure | 10s watcher | Removes oldest partitions when over limit |
| Newest protection | Keep 2 newest | Always preserves recent data |
| Read-only mode | Min free space | Prevents disk exhaustion |
| deletedPartitions guard | Set of day values | Prevents partition resurrection |

### deletedPartitions Guard

```
Why it's needed:

Scenario without guard:
  T0: Partition 2026-01-01 deleted by retention
  T1: Late-arriving logs with timestamp 2026-01-01 arrive
  T2: Partition 2026-01-01 recreated
  T3: Retention violated - old data reappeared!

With deletedPartitions guard:
  T0: Partition 2026-01-01 deleted
      deletedPartitions.add(2026-01-01)
  T1: Late-arriving logs with timestamp 2026-01-01 arrive
  T2: Check: 2026-01-01 in deletedPartitions? YES
  T3: REJECT logs or send to dead letter queue
  T4: Retention preserved
```

### Order of Operations

```
Priority for space reclamation:

1. Retention expiry (planned)
   - Removes partitions older than retention period
   - Runs hourly
   - Predictable, controlled

2. Disk pressure (emergency)
   - Removes oldest partitions to stay under limit
   - Runs every 10s
   - Unplanned, reactive

3. Read-only mode (last resort)
   - Stops all writes
   - Preserves existing data
   - Gives operators time to react
```

## Monitoring During Incident

### Key Metrics to Watch

```bash
# Disk usage
victoria_logs_disk_usage_bytes

# Free disk space
node_filesystem_avail_bytes{mountpoint="/data"}

# Partition count
victoria_logs_partitions_total

# Read-only status
victoria_logs_read_only

# Deleted partitions
victoria_logs_deleted_partitions_total

# Retention deletions
victoria_logs_retention_deletions_total

# Disk pressure deletions
victoria_logs_disk_pressure_deletions_total
```

### Alert Rules

```yaml
# Alert: Disk usage high
- alert: VictoriaLogsDiskUsageHigh
  expr: victoria_logs_disk_usage_bytes / node_filesystem_size_bytes > 0.9
  for: 5m
  annotations:
    summary: "VictoriaLogs disk usage > 90%"
    
# Alert: Disk pressure active
- alert: VictoriaLogsDiskPressure
  expr: victoria_logs_disk_pressure_deletions_total > 0
  for: 1m
  annotations:
    summary: "VictoriaLogs disk pressure triggered - old partitions being deleted"
    
# Alert: Read-only mode
- alert: VictoriaLogsReadOnly
  expr: victoria_logs_read_only == 1
  for: 1m
  annotations:
    summary: "VictoriaLogs in read-only mode - disk space critical"
```

## Operator Response Playbook

### Level 1: Disk Usage 85-95%

```
Actions:
  1. Monitor ingestion rate
  2. Check retention settings
  3. Review upcoming retention expiries
  4. Consider increasing retention if data is valuable
  5. Plan capacity expansion
```

### Level 2: Disk Pressure Triggered

```
Actions:
  1. Verify which partitions are being deleted
  2. Check if deletions match expectations
  3. Reduce ingestion rate if possible
  4. Emergency capacity addition
  5. Review data importance of deleted partitions
```

### Level 3: Read-Only Mode

```
Actions:
  1. STOP - Do not restart service (will remain read-only)
  2. Add disk capacity immediately
  3. OR reduce retention period
  4. OR manually delete old partitions via API
  5. Monitor for exit from read-only mode
  6. Communicate to users about ingestion pause
```

## Manual Intervention Options

### Force Delete Old Partitions

```bash
# List partitions
curl http://localhost:9428/api/v1/partitions

# Force delete specific partition (emergency)
curl -X DELETE http://localhost:9428/api/v1/partitions/2026-01-01

# WARNING: This bypasses retention, use with caution
```

### Adjust Limits Dynamically

```bash
# Increase max disk usage (if capacity available)
curl -X POST http://localhost:9428/config \
  -d 'maxDiskSpaceUsageBytes=550000000000'

# Reduce retention period
curl -X POST http://localhost:9428/config \
  -d 'retention=15d'
```

## Post-Incident Review

### Questions to Answer

1. **Root cause:** Why did disk fill faster than expected?
2. **Detection:** How quickly was the issue detected?
3. **Response:** Did safeguards work as expected?
4. **Impact:** What data was lost (if any)?
5. **Prevention:** What can prevent recurrence?

### Lessons Learned

1. **Monitor ingestion rate changes** - Spikes can surprise
2. **Set alerts early** - 80% usage, not 90%
3. **Test disk pressure behavior** - Know what to expect
4. **Document data value** - Know what can be sacrificed
5. **Plan capacity buffer** - 20% headroom minimum

## Conclusions

1. **Multi-layer protection** - Retention, disk pressure, read-only mode
2. **Automated response** - System self-heals within limits
3. **Operator visibility** - Clear metrics and alerts
4. **Graceful degradation** - Reads continue even when writes stop
5. **Recovery paths** - Multiple options for operators

## Open Questions

- Should disk pressure be more aggressive (drop more partitions)?
- How to handle multi-tenant scenarios where one tenant spikes?
- Should read-only mode trigger automatic capacity alerts?
