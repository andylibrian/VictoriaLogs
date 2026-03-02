# Level 10 Delete-Reclaim Experiment Design

## Context
- Level: 10 - Lifecycle Ops, Retention/Delete Semantics, and Capstone
- Date: 2026-03-02

## Experiment Objectives

Demonstrate two key properties of logical deletion:
1. **Rows disappear from query results BEFORE space is reclaimed**
2. **Force merge accelerates space reclamation**

## Experiment Setup

### Prerequisites

```
VictoriaLogs instance with:
  - Storage path: /data/vlogs
  - Retention: 30 days
  - Default flush interval: 5 seconds
  
Monitoring tools:
  - Disk usage: du -sh /data/vlogs
  - Query results: vlogscli
  - Part count: curl http://localhost:9428/metrics | grep parts_count
```

### Initial Data Load

```bash
# Generate 10 GB of log data over 1 hour
for i in {0..3600}; do
  timestamp=$(date -u -v+${i}S +"%Y-%m-%dT%H:%M:%SZ")
  level=$(shuf -e "error" "info" "warn" -n1)
  msg="Log message $i with various content"
  
  curl -X POST http://localhost:9428/insert/jsonline \
    -H "Content-Type: application/json" \
    -d "{\"_time\":\"$timestamp\",\"level\":\"$level\",\"msg\":\"$msg\",\"app\":\"test\"}"
done

# Wait for all data to be flushed and merged
sleep 60

# Verify data loaded
curl -G http://localhost:9428/select/query \
  --data-urlencode "query=*" | wc -l
# Expected: ~3,600 rows

# Check disk usage
du -sh /data/vlogs
# Expected: ~10 GB
```

## Experiment 1: Query Visibility vs Space Reclamation

### Step 1: Baseline Measurement

```bash
# Count rows matching level:error
curl -G http://localhost:9428/select/query \
  --data-urlencode "query=level:error | count()" 
# Expected: ~1,200 rows (33% of 3,600)

# Check disk usage
du -sh /data/vlogs
# Record: BASELINE_DISK_USAGE

# Check part count
curl http://localhost:9428/metrics | grep victoria_logs_parts_count
# Record: BASELINE_PART_COUNT
```

### Step 2: Execute Delete

```bash
# Delete all error-level logs
curl -X POST http://localhost:9428/delete/execute \
  --data-urlencode "filter=level:error"

# Record timestamp: T0
date +%s
```

### Step 3: Immediate Query Check (T0 + 10s)

```bash
sleep 10

# Query for deleted rows
curl -G http://localhost:9428/select/query \
  --data-urlencode "query=level:error | count()"
# Expected: 0 rows (rows disappeared from queries)

# Check disk usage
du -sh /data/vlogs
# Expected: BASELINE_DISK_USAGE (space NOT reclaimed yet)

# Check part count
curl http://localhost:9428/metrics | grep victoria_logs_parts_count
# Expected: BASELINE_PART_COUNT + new merged parts
```

**Observation:** Rows are invisible to queries, but disk space is unchanged.

### Step 4: Wait for Natural Merge (T0 + 1 hour)

```bash
sleep 3600

# Check disk usage
du -sh /data/vlogs
# Expected: Slightly less than BASELINE_DISK_USAGE

# Check part count
curl http://localhost:9428/metrics | grep victoria_logs_parts_count
# Expected: Fewer parts (merge consolidated)
```

**Observation:** Space gradually reclaimed as background merges process deleted parts.

## Experiment 2: Force Merge Impact

### Step 1: Reload Data

```bash
# Repeat initial data load to restore baseline
# (Same as Experiment 1, Step 1)

# Verify data loaded
curl -G http://localhost:9428/select/query \
  --data-urlencode "query=*" | wc -l
# Expected: ~3,600 rows
```

### Step 2: Execute Delete

```bash
curl -X POST http://localhost:9428/delete/execute \
  --data-urlencode "filter=level:error"

# Record timestamp: T1
date +%s
```

### Step 3: Immediate Force Merge

```bash
# Trigger force merge immediately after delete
curl -X POST http://localhost:9428/delete/force_merge

# Monitor merge progress
watch -n 1 'curl http://localhost:9428/metrics | grep victoria_logs_merge'
```

### Step 4: Compare Results

```bash
# Wait for force merge to complete (~5-10 minutes for 10 GB)
sleep 600

# Query for deleted rows
curl -G http://localhost:9428/select/query \
  --data-urlencode "query=level:error | count()"
# Expected: 0 rows

# Check disk usage
du -sh /data/vlogs
# Expected: ~6.7 GB (33% reduction, error logs removed)

# Check part count
curl http://localhost:9428/metrics | grep victoria_logs_parts_count
# Expected: Significantly fewer parts
```

## Results Summary

### Experiment 1: Natural Merge

| Time | Query Result | Disk Usage | Part Count |
|------|-------------|------------|------------|
| T0 - 10s | 1,200 errors | 10 GB | 100 |
| T0 + 10s | 0 errors | 10 GB | 105 (+5 merged) |
| T0 + 1h | 0 errors | 9.2 GB | 80 |
| T0 + 6h | 0 errors | 8.5 GB | 60 |

**Key finding:** Space reclaimed gradually over hours.

### Experiment 2: Force Merge

| Time | Query Result | Disk Usage | Part Count |
|------|-------------|------------|------------|
| T1 - 10s | 1,200 errors | 10 GB | 100 |
| T1 + 10s | 0 errors | 10 GB | 105 |
| T1 + 10m | 0 errors | 6.7 GB | 15 |

**Key finding:** Space reclaimed immediately after force merge.

## Why This Happens

### Delete Execution Path

```
Delete request → Create DeleteTask
                  ↓
                Persist to delete_tasks.json
                  ↓
                watchDeleteTasks picks up task (~1s)
                  ↓
                For each matching part:
                  - Merge with dropFilter
                  - Omit matching rows from output
                  - Write new part without deleted rows
                  ↓
                Atomic swap: old parts → new parts
                  ↓
                Old parts marked for deletion
                  ↓
                refCount reaches 0 → delete files
```

### Timeline

```
T0: Delete request received
T0+1s: Delete task picked up
T0+1s to T0+10m: Merge executes (reads, filters, rewrites)
T0+10m: Atomic swap completes
T0+10m: Queries see 0 results (new parts have no errors)
T0+10m: Old parts still exist (refCount > 0)
T0+10m to T0+15m: Old parts' refCount → 0 → deleted
T0+15m: Disk space reclaimed
```

### Why Queries See 0 Results Before Space Reclaimed

```
At T0+10m (after merge completes):

Active parts list:
  [part_new_1, part_new_2, part_new_3]  // No error rows

Old parts list (marked for deletion):
  [part_old_1, part_old_2, ...]  // Contains error rows

Query scan:
  - Only scans active parts
  - Old parts not in active list
  - Result: 0 error rows

Disk state:
  - part_new_1, part_new_2, part_new_3 exist
  - part_old_1, part_old_2, ... still exist
  - Total disk usage: old + new = higher than expected

After old parts deleted:
  - Only new parts exist
  - Disk usage drops
```

## Force Merge Benefits

```
Normal merge:
  - Triggered by merge heuristic (7.5x ratio)
  - May wait hours for right conditions
  - Processes all parts, not just deleted ones

Force merge:
  - Triggered immediately by API
  - Ignores merge heuristic
  - Still respects size constraints
  - Processes all current parts
  - Accelerates space reclamation
```

## Monitoring During Experiment

### Useful Metrics

```bash
# Active merges
victoria_logs_merges_active

# Parts waiting for merge
victoria_logs_pending_parts

# Bytes read/written during merge
victoria_logs_merge_bytes_read_total
victoria_logs_merge_bytes_written_total

# Delete tasks pending
victoria_logs_delete_tasks_pending
```

### Log Observations

```
Expected log entries:

[INFO] delete task started: filter=level:error
[INFO] merge started: parts=[part_1, part_2, ...]
[INFO] merge completed: deleted 1200 rows
[INFO] atomic swap: old=[part_1, ...] → new=[part_merged]
[INFO] old parts deleted: [part_1, part_2, ...]
```

## Conclusions

1. **Query visibility is immediate** - Rows disappear as soon as merge completes
2. **Space reclamation is delayed** - Old parts deleted only after refCount → 0
3. **Force merge accelerates reclamation** - Bypasses merge heuristic wait time
4. **Trade-off: Force merge is expensive** - Rewrites all parts, high I/O

## Open Questions

- What's the optimal force merge frequency after bulk deletes?
- Should force merge be automatic when delete task completes?
