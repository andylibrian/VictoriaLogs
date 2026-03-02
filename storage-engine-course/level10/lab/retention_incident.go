package main

import (
	"fmt"
	"strings"
)

// ==================== Retention Incident Types ====================

type SystemState struct {
	Day            int
	DiskUsage      float64 // GB
	FreeSpace      float64 // GB
	PartitionCount int
	IngestionRate  float64 // GB/day
	Status         string
}

type Safeguard struct {
	Name       string
	Trigger    string
	Action     string
	Reversible bool
}

// ==================== Retention Incident Demo ====================

func RetentionIncidentDrill() {
	fmt.Println("=== Level 10 Lab 3: Retention Incident Drill ===")
	fmt.Println()
	fmt.Println("Scenario: Ingestion spike causes disk pressure")
	fmt.Println()

	ShowIncidentTimeline()
	ShowSafeguards()
	ShowOperatorPlaybook()
	ShowMonitoringStrategy()
}

func ShowIncidentTimeline() {
	fmt.Println("=== Incident Timeline ===")
	fmt.Println()

	states := []SystemState{
		{Day: 1, DiskUsage: 450, FreeSpace: 50, PartitionCount: 30, IngestionRate: 15, Status: "Normal"},
		{Day: 2, DiskUsage: 465, FreeSpace: 35, PartitionCount: 30, IngestionRate: 25, Status: "Spike begins"},
		{Day: 3, DiskUsage: 475, FreeSpace: 25, PartitionCount: 30, IngestionRate: 25, Status: "Retention active"},
		{Day: 4, DiskUsage: 485, FreeSpace: 15, PartitionCount: 29, IngestionRate: 25, Status: "Disk pressure"},
		{Day: 5, DiskUsage: 490, FreeSpace: 10, PartitionCount: 28, IngestionRate: 15, Status: "Read-only risk"},
		{Day: 6, DiskUsage: 475, FreeSpace: 25, PartitionCount: 27, IngestionRate: 15, Status: "Recovery"},
	}

	fmt.Printf("%-5s %12s %12s %12s %12s %15s\n",
		"Day", "Disk (GB)", "Free (GB)", "Partitions", "Ingest (GB/d)", "Status")
	fmt.Println(strings.Repeat("-", 85))

	for _, s := range states {
		fmt.Printf("%-5d %12.0f %12.0f %12d %12.0f %15s\n",
			s.Day, s.DiskUsage, s.FreeSpace, s.PartitionCount, s.IngestionRate, s.Status)
	}
	fmt.Println()

	ShowDayByDayDetails()
}

func ShowDayByDayDetails() {
	fmt.Println("=== Day-by-Day Details ===")
	fmt.Println()

	details := []struct {
		day    string
		events []string
	}{
		{
			day: "Day 1 (Normal)",
			events: []string{
				"Disk usage: 450 GB / 500 GB (90%)",
				"Retention watcher: No action (all partitions < 30 days)",
				"Disk pressure watcher: 450 < 480 GB → No action",
			},
		},
		{
			day: "Day 2 (Spike Begins)",
			events: []string{
				"Disk usage: 465 GB / 500 GB (93%)",
				"Ingestion rate increases: 15 → 25 GB/day",
				"Retention watcher: Partition 2026-01-01 expired (31 days)",
				"  → Partition deleted, 15 GB reclaimed",
				"Disk usage after retention: 450 GB",
			},
		},
		{
			day: "Day 3 (Spike Continues)",
			events: []string{
				"Disk usage: 475 GB / 500 GB (95%)",
				"Retention watcher: Partition 2026-01-02 expired",
				"  → Partition deleted, 15 GB reclaimed",
				"Disk usage after retention: 460 GB",
			},
		},
		{
			day: "Day 4 (Disk Pressure)",
			events: []string{
				"Disk usage: 485 GB / 500 GB (97%)",
				"Retention watcher: Partition 2026-01-03 not yet 31 days",
				"Disk pressure watcher: 485 > 480 GB → EMERGENCY",
				"  → Oldest partition (2026-01-03) deleted",
				"  → totalSize: 485 - 15 = 470 GB",
				"  → 470 < 480 → Stop",
				"Disk usage after: 470 GB",
			},
		},
		{
			day: "Day 5 (Read-Only Risk)",
			events: []string{
				"Disk usage: 490 GB / 500 GB (98%)",
				"Spike ended, but accumulation continues",
				"Disk pressure watcher: 490 > 480 GB → EMERGENCY",
				"  → Oldest partition deleted",
				"Free space check: 25 GB > 10 GB → OK",
				"If free space < 10 GB: READ-ONLY MODE",
			},
		},
		{
			day: "Day 6 (Recovery)",
			events: []string{
				"Disk usage: 475 GB / 500 GB (95%)",
				"Retention continues normal operation",
				"System stabilized",
			},
		},
	}

	for _, d := range details {
		fmt.Printf("%s:\n", d.day)
		for _, e := range d.events {
			fmt.Printf("  %s\n", e)
		}
		fmt.Println()
	}
}

func ShowSafeguards() {
	fmt.Println("=== Safeguards ===")
	fmt.Println()

	safeguards := []Safeguard{
		{
			Name:       "Retention watcher",
			Trigger:    "Partition age > retention period",
			Action:     "Delete entire partition directory",
			Reversible: false,
		},
		{
			Name:       "Disk pressure watcher",
			Trigger:    "totalSize > maxDiskSpaceUsageBytes",
			Action:     "Delete oldest partitions",
			Reversible: false,
		},
		{
			Name:       "Newest protection",
			Trigger:    "Disk pressure active",
			Action:     "Keep 2 newest partitions",
			Reversible: false,
		},
		{
			Name:       "Read-only mode",
			Trigger:    "Free space < minFreeDiskSpaceBytes",
			Action:     "Stop all writes (HTTP 429)",
			Reversible: true,
		},
		{
			Name:       "deletedPartitions guard",
			Trigger:    "Late-arriving logs for deleted partition",
			Action:     "Reject logs or send to dead letter",
			Reversible: false,
		},
	}

	fmt.Printf("%-25s %-35s %-25s %s\n",
		"Safeguard", "Trigger", "Action", "Reversible")
	fmt.Println(strings.Repeat("-", 100))

	for _, s := range safeguards {
		revStr := "No"
		if s.Reversible {
			revStr = "Yes"
		}
		fmt.Printf("%-25s %-35s %-25s %s\n",
			s.Name, s.Trigger, s.Action, revStr)
	}
	fmt.Println()

	ShowdeletedPartitionsGuard()
}

func ShowdeletedPartitionsGuard() {
	fmt.Println("=== deletedPartitions Guard ===")
	fmt.Println()

	fmt.Println("Why it's needed:")
	fmt.Println()

	fmt.Println("Scenario WITHOUT guard:")
	fmt.Println("  T0: Partition 2026-01-01 deleted by retention")
	fmt.Println("  T1: Late-arriving logs with timestamp 2026-01-01 arrive")
	fmt.Println("  T2: Partition 2026-01-01 recreated")
	fmt.Println("  T3: RETENTION VIOLATED - old data reappeared!")
	fmt.Println()

	fmt.Println("Scenario WITH guard:")
	fmt.Println("  T0: Partition 2026-01-01 deleted")
	fmt.Println("      deletedPartitions.add(2026-01-01)")
	fmt.Println("  T1: Late-arriving logs with timestamp 2026-01-01 arrive")
	fmt.Println("  T2: Check: 2026-01-01 in deletedPartitions? YES")
	fmt.Println("  T3: REJECT logs or send to dead letter queue")
	fmt.Println("  T4: Retention preserved")
	fmt.Println()
}

func ShowOperatorPlaybook() {
	fmt.Println("=== Operator Response Playbook ===")
	fmt.Println()

	levels := []struct {
		level   string
		actions []string
	}{
		{
			level: "Level 1: Disk Usage 85-95%",
			actions: []string{
				"1. Monitor ingestion rate",
				"2. Check retention settings",
				"3. Review upcoming retention expiries",
				"4. Consider increasing retention if data valuable",
				"5. Plan capacity expansion",
			},
		},
		{
			level: "Level 2: Disk Pressure Triggered",
			actions: []string{
				"1. Verify which partitions are being deleted",
				"2. Check if deletions match expectations",
				"3. Reduce ingestion rate if possible",
				"4. Emergency capacity addition",
				"5. Review data importance of deleted partitions",
			},
		},
		{
			level: "Level 3: Read-Only Mode",
			actions: []string{
				"1. STOP - Do not restart service",
				"2. Add disk capacity immediately",
				"3. OR reduce retention period",
				"4. OR manually delete old partitions via API",
				"5. Monitor for exit from read-only mode",
				"6. Communicate to users about ingestion pause",
			},
		},
	}

	for _, l := range levels {
		fmt.Printf("%s:\n", l.level)
		for _, a := range l.actions {
			fmt.Printf("  %s\n", a)
		}
		fmt.Println()
	}
}

func ShowMonitoringStrategy() {
	fmt.Println("=== Monitoring Strategy ===")
	fmt.Println()

	fmt.Println("Key metrics to watch:")
	fmt.Println()

	metrics := []struct {
		name      string
		threshold string
		action    string
	}{
		{"Disk usage", "> 85%", "Review capacity"},
		{"Disk usage", "> 90%", "Alert, plan expansion"},
		{"Disk usage", "> 95%", "Critical alert"},
		{"Free disk space", "< 20 GB", "Warning"},
		{"Free disk space", "< 10 GB", "Critical"},
		{"Partition count", "Growing", "Check retention"},
		{"Read-only status", "= 1", "Emergency"},
	}

	fmt.Printf("%-25s %-15s %s\n", "Metric", "Threshold", "Action")
	fmt.Println(strings.Repeat("-", 70))

	for _, m := range metrics {
		fmt.Printf("%-25s %-15s %s\n", m.name, m.threshold, m.action)
	}
	fmt.Println()

	ShowAlertRules()
}

func ShowAlertRules() {
	fmt.Println("Recommended alert rules:")
	fmt.Println()

	fmt.Println("# Alert: Disk usage high")
	fmt.Println("- alert: VictoriaLogsDiskUsageHigh")
	fmt.Println("  expr: victoria_logs_disk_usage_bytes / node_filesystem_size_bytes > 0.9")
	fmt.Println("  for: 5m")
	fmt.Println("  annotations:")
	fmt.Println("    summary: \"VictoriaLogs disk usage > 90%\"")
	fmt.Println()

	fmt.Println("# Alert: Disk pressure active")
	fmt.Println("- alert: VictoriaLogsDiskPressure")
	fmt.Println("  expr: victoria_logs_disk_pressure_deletions_total > 0")
	fmt.Println("  for: 1m")
	fmt.Println("  annotations:")
	fmt.Println("    summary: \"Disk pressure triggered\"")
	fmt.Println()

	fmt.Println("# Alert: Read-only mode")
	fmt.Println("- alert: VictoriaLogsReadOnly")
	fmt.Println("  expr: victoria_logs_read_only == 1")
	fmt.Println("  for: 1m")
	fmt.Println("  annotations:")
	fmt.Println("    summary: \"Read-only mode - disk critical\"")
	fmt.Println()
}

// ==================== Manual Intervention ====================

func ShowManualIntervention() {
	fmt.Println("=== Manual Intervention Options ===")
	fmt.Println()

	fmt.Println("Force delete old partitions (emergency):")
	fmt.Println("  # List partitions")
	fmt.Println("  curl http://localhost:9428/api/v1/partitions")
	fmt.Println()
	fmt.Println("  # Force delete specific partition")
	fmt.Println("  curl -X DELETE http://localhost:9428/api/v1/partitions/2026-01-01")
	fmt.Println()
	fmt.Println("  WARNING: Bypasses retention, use with caution")
	fmt.Println()

	fmt.Println("Adjust limits dynamically:")
	fmt.Println("  # Increase max disk usage")
	fmt.Println("  curl -X POST http://localhost:9428/config \\")
	fmt.Println("    -d 'maxDiskSpaceUsageBytes=550000000000'")
	fmt.Println()
	fmt.Println("  # Reduce retention period")
	fmt.Println("  curl -X POST http://localhost:9428/config \\")
	fmt.Println("    -d 'retention=15d'")
	fmt.Println()
}

// ==================== Post-Incident Review ====================

func ShowPostIncidentReview() {
	fmt.Println("=== Post-Incident Review Questions ===")
	fmt.Println()

	questions := []string{
		"1. Root cause: Why did disk fill faster than expected?",
		"2. Detection: How quickly was the issue detected?",
		"3. Response: Did safeguards work as expected?",
		"4. Impact: What data was lost (if any)?",
		"5. Prevention: What can prevent recurrence?",
	}

	for _, q := range questions {
		fmt.Printf("  %s\n", q)
	}
	fmt.Println()

	fmt.Println("Lessons learned:")
	fmt.Println("  1. Monitor ingestion rate changes - Spikes can surprise")
	fmt.Println("  2. Set alerts early - 80% usage, not 90%")
	fmt.Println("  3. Test disk pressure behavior - Know what to expect")
	fmt.Println("  4. Document data value - Know what can be sacrificed")
	fmt.Println("  5. Plan capacity buffer - 20% headroom minimum")
	fmt.Println()
}
