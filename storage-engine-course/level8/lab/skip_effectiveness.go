package main

import (
	"fmt"
	"strings"
)

// ==================== Skip Effectiveness Types ====================

type QueryClass struct {
	Name        string
	Description string
	Query       string
	Selectivity string
}

type SkipStats struct {
	Stage          string
	Input          int64
	Output         int64
	Skipped        int64
	StageReduction float64
	Cumulative     float64
}

// ==================== Skip Effectiveness Demo ====================

func SkipEffectivenessDemo() {
	fmt.Println("=== Level 8 Lab 2: Skip Effectiveness ===")
	fmt.Println()
	fmt.Println("This program analyzes pruning effectiveness at each stage")
	fmt.Println("for different query classes.")
	fmt.Println()

	// Show query classes
	ShowQueryClasses()

	// Analyze high-selectivity query
	fmt.Println("========================================")
	fmt.Println()
	AnalyzeHighSelectivityQuery()

	// Analyze low-selectivity query
	fmt.Println("========================================")
	fmt.Println()
	AnalyzeLowSelectivityQuery()

	// Show comparison
	fmt.Println("========================================")
	fmt.Println()
	ShowSkipComparison()
}

func ShowQueryClasses() {
	fmt.Println("=== Query Classes ===")
	fmt.Println()

	classes := []QueryClass{
		{
			Name:        "High-Selectivity",
			Description: "Specific stream, narrow time, row filter",
			Query:       "_stream:{app=\"api\"} AND _time:[00:00, 01:00] AND level:error",
			Selectivity: "~0.001% of data",
		},
		{
			Name:        "Medium-Selectivity",
			Description: "Specific stream, broad time, no row filter",
			Query:       "_stream:{app=\"api\"} AND _time:[00:00, 23:59]",
			Selectivity: "~0.1% of data",
		},
		{
			Name:        "Low-Selectivity",
			Description: "All streams, broad time, no filters",
			Query:       "_time:[00:00, 23:59]",
			Selectivity: "100% of data (full scan)",
		},
	}

	for _, c := range classes {
		fmt.Printf("--- %s ---\n", c.Name)
		fmt.Printf("Description: %s\n", c.Description)
		fmt.Printf("Query:       %s\n", c.Query)
		fmt.Printf("Selectivity: %s\n", c.Selectivity)
		fmt.Println()
	}
}

func AnalyzeHighSelectivityQuery() {
	fmt.Println("=== High-Selectivity Query Analysis ===")
	fmt.Println()
	fmt.Println("Query: _stream:{app=\"api\"} AND _time:[00:00, 01:00] AND level:error")
	fmt.Println()

	// Baseline
	fmt.Println("Baseline (without pruning):")
	fmt.Println("  Total data:   1 TB")
	fmt.Println("  Total rows:   10 billion")
	fmt.Println()

	// Stage-by-stage analysis
	stats := []SkipStats{
		{"Partition", 30, 1, 29, 96.7, 96.7},
		{"Part", 163, 9, 154, 94.5, 99.8},
		{"Metaindex", 420, 12, 408, 97.1, 99.995},
		{"Block Header", 280, 18, 262, 93.6, 99.9997},
		{"Bloom", 18, 15, 3, 16.7, 99.9998},
		{"Row Eval", 150000, 150, 149850, 99.9, 99.9999985},
	}

	PrintSkipStats(stats)
	fmt.Println()

	// I/O analysis
	fmt.Println("I/O Analysis:")
	fmt.Println()
	fmt.Printf("  Original I/O:  1 TB\n")
	fmt.Printf("  Actual I/O:    ~%.2f MB\n", 0.000015*1024*1024)
	fmt.Printf("  I/O saved:     99.9985%%\n")
	fmt.Println()

	// Key insights
	fmt.Println("Key Insights:")
	fmt.Println("  1. Partition pruning eliminates 96.7% (time range)")
	fmt.Println("  2. Part pruning eliminates another 94.5% (time range)")
	fmt.Println("  3. Metaindex eliminates 97.1% (stream targeting)")
	fmt.Println("  4. Row evaluation is most expensive but runs on tiny dataset")
	fmt.Println()
}

func AnalyzeLowSelectivityQuery() {
	fmt.Println("=== Low-Selectivity Query Analysis ===")
	fmt.Println()
	fmt.Println("Query: _time:[00:00, 23:59] (full day scan, no filters)")
	fmt.Println()

	// Baseline
	fmt.Println("Baseline (without pruning):")
	fmt.Println("  Total data:   33 GB (1 partition)")
	fmt.Println("  Total rows:   333 million")
	fmt.Println()

	// Stage-by-stage analysis
	stats := []SkipStats{
		{"Partition", 30, 1, 29, 96.7, 96.7},
		{"Part", 163, 163, 0, 0, 96.7},
		{"Metaindex", 420, 420, 0, 0, 96.7},
		{"Block Header", 2800, 2800, 0, 0, 96.7},
		{"Bloom", 2800, 2800, 0, 0, 96.7},
		{"Row Eval", 28000000, 28000000, 0, 0, 96.7},
	}

	PrintSkipStats(stats)
	fmt.Println()

	// I/O analysis
	fmt.Println("I/O Analysis:")
	fmt.Println()
	fmt.Printf("  Original I/O:  33 GB\n")
	fmt.Printf("  Actual I/O:    33 GB (no pruning possible)\n")
	fmt.Printf("  I/O saved:     0%% (but partition pruning still helps)\n")
	fmt.Println()

	// Why it's still efficient
	fmt.Println("Why full scan is still efficient:")
	fmt.Println("  1. Sequential I/O (not random access)")
	fmt.Println("  2. OS page cache for hot data")
	fmt.Println("  3. Parallel workers (8× throughput)")
	fmt.Println("  4. Column pruning (only read needed columns)")
	fmt.Println()
}

func PrintSkipStats(stats []SkipStats) {
	fmt.Println("Stage-by-Stage Reduction:")
	fmt.Println()
	fmt.Printf("%-15s %12s %12s %12s %10s %12s\n",
		"Stage", "Input", "Output", "Skipped", "Stage %", "Cumulative %")
	fmt.Println(strings.Repeat("-", 80))

	for _, s := range stats {
		fmt.Printf("%-15s %12d %12d %12d %9.1f%% %11.4f%%\n",
			s.Stage, s.Input, s.Output, s.Skipped, s.StageReduction, s.Cumulative)
	}
}

func ShowSkipComparison() {
	fmt.Println("=== Skip Effectiveness Comparison ===")
	fmt.Println()

	fmt.Printf("%-20s %15s %15s %15s\n", "Stage", "High-Select", "Medium-Select", "Low-Select")
	fmt.Println(strings.Repeat("-", 70))

	comparisons := []struct {
		stage      string
		highSkip   float64
		mediumSkip float64
		lowSkip    float64
	}{
		{"Partition", 96.7, 96.7, 96.7},
		{"Part", 94.5, 0, 0},
		{"Metaindex", 97.1, 0, 0},
		{"Block Header", 93.6, 0, 0},
		{"Bloom", 16.7, 0, 0},
		{"Row Eval", 99.9, 0, 0},
	}

	for _, c := range comparisons {
		fmt.Printf("%-20s %14.1f%% %14.1f%% %14.1f%%\n",
			c.stage, c.highSkip, c.mediumSkip, c.lowSkip)
	}

	fmt.Println()
	fmt.Println("Conclusions:")
	fmt.Println("  1. Time range + stream filter = massive pruning (99.99%+)")
	fmt.Println("  2. Broad queries = limited pruning (rely on parallelism)")
	fmt.Println("  3. Even full scans are efficient due to sequential I/O")
	fmt.Println()

	// Cost analysis
	ShowCostAnalysis()
}

func ShowCostAnalysis() {
	fmt.Println("=== Cost per Stage ===")
	fmt.Println()

	fmt.Printf("%-15s %-25s %-25s\n", "Stage", "Cost per Check", "Typical Elimination")
	fmt.Println(strings.Repeat("-", 70))

	costs := []struct {
		stage       string
		cost        string
		elimination string
	}{
		{"Partition", "~50 ns (binary search)", "Billions of rows"},
		{"Part", "~100 ns (comparison)", "Millions of rows"},
		{"Metaindex", "~1 µs (RAM scan)", "~2.5M rows per skip"},
		{"Block Header", "~10 µs (decompress)", "~10K rows per skip"},
		{"Bloom", "~50 ns (hash probes)", "~10K rows per skip"},
		{"Row Eval", "~1 µs per row", "Individual rows"},
	}

	for _, c := range costs {
		fmt.Printf("%-15s %-25s %-25s\n", c.stage, c.cost, c.elimination)
	}

	fmt.Println()
	fmt.Println("Key insight: Early stages are O(1) or O(log N), eliminate O(billions)")
	fmt.Println("             Late stages are O(rows), but only run on survivors")
}
