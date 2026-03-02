package main

import (
	"fmt"
	"strings"
	"time"
)

// ==================== Delete Reclaim Types ====================

type ExperimentState struct {
	Time           time.Duration
	QueryResult    int
	DiskUsage      float64 // GB
	PartCount      int
	DeletedRows    int
	SpaceReclaimed float64 // GB
}

// ==================== Delete Reclaim Demo ====================

func DeleteReclaimDemo() {
	fmt.Println("=== Level 10 Lab 2: Delete-Reclaim Experiment ===")
	fmt.Println()
	fmt.Println("This program demonstrates two key properties:")
	fmt.Println("  1. Rows disappear from query results BEFORE space is reclaimed")
	fmt.Println("  2. Force merge accelerates space reclamation")
	fmt.Println()

	ShowExperiment1NaturalMerge()
	ShowExperiment2ForceMerge()
	ShowWhyThisHappens()
	ShowTimeline()
}

func ShowExperiment1NaturalMerge() {
	fmt.Println("=== Experiment 1: Natural Merge ===")
	fmt.Println()

	fmt.Println("Setup:")
	fmt.Println("  - Initial data: 10 GB, 3,600 rows")
	fmt.Println("  - Error rows: 1,200 (33%)")
	fmt.Println()

	// Experiment timeline
	states := []ExperimentState{
		{Time: -10 * time.Second, QueryResult: 1200, DiskUsage: 10.0, PartCount: 100},
		{Time: 10 * time.Second, QueryResult: 0, DiskUsage: 10.0, PartCount: 105},
		{Time: 1 * time.Hour, QueryResult: 0, DiskUsage: 9.2, PartCount: 80},
		{Time: 6 * time.Hour, QueryResult: 0, DiskUsage: 8.5, PartCount: 60},
		{Time: 24 * time.Hour, QueryResult: 0, DiskUsage: 6.8, PartCount: 40},
	}

	fmt.Println("Timeline:")
	fmt.Println()
	PrintExperimentTable(states)

	fmt.Println()
	fmt.Println("Key observation:")
	fmt.Println("  - Rows disappear from queries at T+10s")
	fmt.Println("  - But disk space remains at 10 GB")
	fmt.Println("  - Space gradually reclaimed over 24 hours")
	fmt.Println()
}

func ShowExperiment2ForceMerge() {
	fmt.Println("=== Experiment 2: Force Merge ===")
	fmt.Println()

	fmt.Println("Setup:")
	fmt.Println("  - Same as Experiment 1")
	fmt.Println("  - Force merge triggered immediately after delete")
	fmt.Println()

	states := []ExperimentState{
		{Time: -10 * time.Second, QueryResult: 1200, DiskUsage: 10.0, PartCount: 100},
		{Time: 10 * time.Second, QueryResult: 0, DiskUsage: 10.0, PartCount: 105},
		{Time: 5 * time.Minute, QueryResult: 0, DiskUsage: 6.7, PartCount: 15},
		{Time: 10 * time.Minute, QueryResult: 0, DiskUsage: 6.7, PartCount: 15},
	}

	fmt.Println("Timeline:")
	fmt.Println()
	PrintExperimentTable(states)

	fmt.Println()
	fmt.Println("Key observation:")
	fmt.Println("  - Rows disappear from queries at T+10s")
	fmt.Println("  - Space reclaimed at T+5m (after force merge)")
	fmt.Println("  - 33% reduction in disk usage")
	fmt.Println()
}

func PrintExperimentTable(states []ExperimentState) {
	fmt.Printf("%-15s %12s %12s %12s\n", "Time", "Query Result", "Disk (GB)", "Parts")
	fmt.Println(strings.Repeat("-", 55))

	for _, s := range states {
		timeStr := formatTime(s.Time)
		fmt.Printf("%-15s %12d %12.1f %12d\n", timeStr, s.QueryResult, s.DiskUsage, s.PartCount)
	}
}

func formatTime(d time.Duration) string {
	if d < 0 {
		return fmt.Sprintf("T%d s", d/time.Second)
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("T+%d s", d/time.Second)
	case d < time.Hour:
		return fmt.Sprintf("T+%d m", d/time.Minute)
	default:
		return fmt.Sprintf("T+%d h", d/time.Hour)
	}
}

func ShowWhyThisHappens() {
	fmt.Println("=== Why This Happens ===")
	fmt.Println()

	fmt.Println("Delete execution path:")
	fmt.Println()
	fmt.Println("  T0: Delete request received")
	fmt.Println("        ↓")
	fmt.Println("  T0+1s: Delete task picked up by watcher")
	fmt.Println("        ↓")
	fmt.Println("  T0+1s to T0+10m: Merge executes")
	fmt.Println("        - Read matching parts")
	fmt.Println("        - Decompress blocks")
	fmt.Println("        - Filter out matching rows")
	fmt.Println("        - Write new parts")
	fmt.Println("        ↓")
	fmt.Println("  T0+10m: Atomic swap completes")
	fmt.Println("        - Old parts removed from active list")
	fmt.Println("        - New parts (without deleted rows) added")
	fmt.Println("        ↓")
	fmt.Println("  T0+10m: Queries see 0 results")
	fmt.Println("        - Only scan active parts")
	fmt.Println("        - Old parts not in active list")
	fmt.Println("        ↓")
	fmt.Println("  T0+10m to T0+15m: Old parts deleted")
	fmt.Println("        - Old parts still on disk")
	fmt.Println("        - refCount > 0 (queries may still hold refs)")
	fmt.Println("        - When refCount → 0: files deleted")
	fmt.Println("        ↓")
	fmt.Println("  T0+15m: Disk space reclaimed")
	fmt.Println()

	fmt.Println("Why queries see 0 results before space reclaimed:")
	fmt.Println()
	fmt.Println("  Active parts:     [part_new_1, part_new_2, part_new_3]")
	fmt.Println("  Old parts:        [part_old_1, part_old_2, ...] (not in active list)")
	fmt.Println()
	fmt.Println("  Query scan:")
	fmt.Println("    - Only scans active parts")
	fmt.Println("    - Old parts not visible to queries")
	fmt.Println("    - Result: 0 deleted rows")
	fmt.Println()
	fmt.Println("  Disk state:")
	fmt.Println("    - part_new_* exist (active)")
	fmt.Println("    - part_old_* exist (inactive, waiting for refCount → 0)")
	fmt.Println("    - Total: old + new = higher than expected")
	fmt.Println()
}

func ShowTimeline() {
	fmt.Println("=== Detailed Timeline ===")
	fmt.Println()

	events := []struct {
		time  string
		event string
	}{
		{"T0", "Delete request: DELETE WHERE level='error'"},
		{"T0+1s", "Delete task created and persisted"},
		{"T0+2s", "watchDeleteTasks picks up task"},
		{"T0+2s", "Identify parts with error rows (bloom + block search)"},
		{"T0+3s", "Merge starts: read part_1, part_2, part_3"},
		{"T0+30s", "Merge: decompress blocks from part_1"},
		{"T0+60s", "Merge: filter rows, write new_block_1"},
		{"T0+3m", "Merge: process part_2"},
		{"T0+6m", "Merge: process part_3"},
		{"T0+8m", "Merge: write part_merged"},
		{"T0+8m", "Atomic swap: old=[part_1,2,3] → new=[part_merged]"},
		{"T0+8m", "Queries now see 0 error rows"},
		{"T0+8m", "Old parts: refCount=2 (merge worker + query)"},
		{"T0+9m", "Merge worker: decRef() → refCount=1"},
		{"T0+10m", "Query finishes: decRef() → refCount=0"},
		{"T0+10m", "Old parts deleted, space reclaimed"},
	}

	fmt.Printf("%-10s %s\n", "Time", "Event")
	fmt.Println(strings.Repeat("-", 80))
	for _, e := range events {
		fmt.Printf("%-10s %s\n", e.time, e.event)
	}
	fmt.Println()
}

// ==================== Force Merge Comparison ====================

func ShowForceMergeComparison() {
	fmt.Println("=== Natural Merge vs Force Merge ===")
	fmt.Println()

	fmt.Printf("%-20s %-20s %-20s\n", "Metric", "Natural Merge", "Force Merge")
	fmt.Println(strings.Repeat("-", 60))

	comparisons := []struct {
		metric  string
		natural string
		force   string
	}{
		{"Trigger", "7.5× merge ratio", "API call"},
		{"Latency", "Minutes to hours", "Minutes"},
		{"I/O cost", "Lower (selective)", "Higher (all parts)"},
		{"Space reclaimed", "Gradual", "Immediate"},
		{"CPU usage", "Spread over time", "Burst"},
	}

	for _, c := range comparisons {
		fmt.Printf("%-20s %-20s %-20s\n", c.metric, c.natural, c.force)
	}
	fmt.Println()

	fmt.Println("When to use force merge:")
	fmt.Println("  - After bulk delete (need space immediately)")
	fmt.Println("  - Before capacity planning (accurate sizing)")
	fmt.Println("  - Performance testing (consistent state)")
	fmt.Println()

	fmt.Println("When to avoid force merge:")
	fmt.Println("  - Production load (high I/O impact)")
	fmt.Println("  - Frequent deletes (inefficient)")
	fmt.Println("  - Low disk pressure (not needed)")
	fmt.Println()
}

// ==================== Monitoring ====================

func ShowMonitoringQueries() {
	fmt.Println("=== Monitoring During Delete ===")
	fmt.Println()

	fmt.Println("Key metrics to watch:")
	fmt.Println()

	metrics := []struct {
		name        string
		description string
	}{
		{"victoria_logs_merges_active", "Active merge count"},
		{"victoria_logs_pending_parts", "Parts waiting for merge"},
		{"victoria_logs_merge_bytes_read_total", "Bytes read during merge"},
		{"victoria_logs_merge_bytes_written_total", "Bytes written during merge"},
		{"victoria_logs_delete_tasks_pending", "Delete tasks in queue"},
		{"victoria_logs_disk_usage_bytes", "Total disk usage"},
	}

	fmt.Printf("%-45s %s\n", "Metric", "Description")
	fmt.Println(strings.Repeat("-", 80))
	for _, m := range metrics {
		fmt.Printf("%-45s %s\n", m.name, m.description)
	}
	fmt.Println()

	fmt.Println("Example queries:")
	fmt.Println()
	fmt.Println("  # Disk usage over time")
	fmt.Println("  rate(victoria_logs_disk_usage_bytes[5m])")
	fmt.Println()
	fmt.Println("  # Merge progress")
	fmt.Println("  victoria_logs_merge_bytes_written_total / victoria_logs_merge_bytes_read_total")
	fmt.Println()
}
