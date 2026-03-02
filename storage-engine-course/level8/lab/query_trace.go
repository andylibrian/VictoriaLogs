package main

import (
	"fmt"
	"strings"
)

// ==================== Query Trace Types ====================

type PruningStage int

const (
	StagePartition PruningStage = iota
	StagePart
	StageMetaindex
	StageBlockHeader
	StageBloom
	StageRowEval
)

func (s PruningStage) String() string {
	switch s {
	case StagePartition:
		return "Partition"
	case StagePart:
		return "Part"
	case StageMetaindex:
		return "Metaindex"
	case StageBlockHeader:
		return "Block Header"
	case StageBloom:
		return "Bloom"
	case StageRowEval:
		return "Row Eval"
	default:
		return "Unknown"
	}
}

type PruningStats struct {
	Stage    PruningStage
	Total    int
	Selected int
	Skipped  int
}

func (ps *PruningStats) Reduction() float64 {
	if ps.Total == 0 {
		return 0
	}
	return float64(ps.Skipped) / float64(ps.Total) * 100
}

func (ps *PruningStats) CumulativeInput(previousTotal int) float64 {
	if previousTotal == 0 {
		return 0
	}
	return float64(previousTotal-ps.Skipped) / float64(previousTotal) * 100
}

// ==================== Query Trace Demo ====================

func QueryTraceDemo() {
	fmt.Println("=== Level 8 Lab 1: Query Trace ===")
	fmt.Println()
	fmt.Println("Query: _stream:{host=\"web-01\"} AND level:error AND _time:[10:00, 10:05]")
	fmt.Println()

	// Show the complete query execution trace
	ShowQueryTrace()
}

func ShowQueryTrace() {
	fmt.Println("=== Complete Query Execution Trace ===")
	fmt.Println()

	// Phase 1: Setup
	fmt.Println("--- Phase 1: Setup ---")
	fmt.Println()
	fmt.Println("Storage.RunQuery(qctx, writeBlock)")
	fmt.Println("  ├── initSubqueries() - no subqueries for this query")
	fmt.Println("  ├── getSearchOptions()")
	fmt.Println("  │     minTimestamp = 10:00 UTC")
	fmt.Println("  │     maxTimestamp = 10:05 UTC")
	fmt.Println("  │     streamFilter = {host=\"web-01\"}")
	fmt.Println("  │     filter = level:error")
	fmt.Println("  └── runPipes() - build processor chain")
	fmt.Println()

	// Phase 2: Scheduling
	fmt.Println("--- Phase 2: Scheduling ---")
	fmt.Println()

	stats := []PruningStats{
		{StagePartition, 30, 1, 29},
		{StagePart, 140, 13, 127},
		{StageMetaindex, 85, 3, 82},
		{StageBlockHeader, 67, 6, 61},
		{StageBloom, 6, 6, 0},
		{StageRowEval, 60000, 19200, 40800},
	}

	PrintPruningTable(stats)
	fmt.Println()

	// Show detailed trace
	ShowDetailedTrace(stats)
}

func PrintPruningTable(stats []PruningStats) {
	fmt.Println("Pruning Stage Summary:")
	fmt.Println()
	fmt.Printf("%-15s %10s %10s %10s %10s\n", "Stage", "Total", "Selected", "Skipped", "Reduction")
	fmt.Println(strings.Repeat("-", 60))

	cumulativeReduction := 1.0
	for _, ps := range stats {
		reduction := ps.Reduction()
		cumulativeReduction *= (1 - reduction/100)

		fmt.Printf("%-15s %10d %10d %10d %9.1f%%\n",
			ps.Stage,
			ps.Total,
			ps.Selected,
			ps.Skipped,
			reduction)
	}

	fmt.Println()
	fmt.Printf("Cumulative reduction: %.4f%% of data eliminated\n", (1-cumulativeReduction)*100)
	fmt.Printf("Data read: %.4f%% of total\n", cumulativeReduction*100)
}

func ShowDetailedTrace(stats []PruningStats) {
	fmt.Println("=== Detailed Stage Trace ===")
	fmt.Println()

	// Stage 1: Partition pruning
	fmt.Println("--- Stage 1: Partition Pruning ---")
	fmt.Println()
	fmt.Println("Binary search on sorted partition list:")
	fmt.Println("  Partitions sorted by day")
	fmt.Println("  Query time range: 10:00-10:05 on March 2")
	fmt.Println("  minDay = March 2 → partition[28]")
	fmt.Println("  maxDay = March 2 → partition[28]")
	fmt.Println()
	fmt.Printf("Result: %d partition selected, %d skipped (%.1f%% reduction)\n",
		stats[0].Selected, stats[0].Skipped, stats[0].Reduction())
	fmt.Println()

	// Stage 2: Part pruning
	fmt.Println("--- Stage 2: Part Pruning ---")
	fmt.Println()
	fmt.Println("Scan all three tiers:")
	fmt.Println("  inmemoryParts: 5 total, 3 match time range")
	fmt.Println("  smallParts: 120 total, 8 match time range")
	fmt.Println("  bigParts: 15 total, 2 match time range")
	fmt.Println()
	fmt.Printf("Result: %d parts selected, %d skipped (%.1f%% reduction)\n",
		stats[1].Selected, stats[1].Skipped, stats[1].Reduction())
	fmt.Println()

	// Stage 3: Metaindex pruning
	fmt.Println("--- Stage 3: Metaindex Pruning ---")
	fmt.Println()
	fmt.Println("Binary search on sorted indexBlockHeaders:")
	fmt.Println("  Metaindex sorted by streamID")
	fmt.Println("  Query targets streamIDs [S42, S43, S44]")
	fmt.Println("  Binary search for S42 → jump to ih[12]")
	fmt.Println("  Load 3 matching index blocks")
	fmt.Println()
	fmt.Printf("Result: %d index blocks selected, %d skipped (%.1f%% reduction)\n",
		stats[2].Selected, stats[2].Skipped, stats[2].Reduction())
	fmt.Println()

	// Stage 4: Block header pruning
	fmt.Println("--- Stage 4: Block Header Pruning ---")
	fmt.Println()
	fmt.Println("For each index block:")
	fmt.Println("  ZSTD-decompress index block")
	fmt.Println("  Check each blockHeader:")
	fmt.Println("    - streamID match")
	fmt.Println("    - time range overlap")
	fmt.Println()
	fmt.Printf("Result: %d blocks selected, %d skipped (%.1f%% reduction)\n",
		stats[3].Selected, stats[3].Skipped, stats[3].Reduction())
	fmt.Println()

	// Stage 5: Bloom filter
	fmt.Println("--- Stage 5: Bloom Filter Check ---")
	fmt.Println()
	fmt.Println("Query filter: level:error")
	fmt.Println()
	fmt.Println("For each block:")
	fmt.Println("  1. Check const column (level is not const)")
	fmt.Println("  2. Check dict column (level has dict)")
	fmt.Println("     dict = [\"debug\", \"info\", \"warn\", \"error\"]")
	fmt.Println("     \"error\" in dict → pass")
	fmt.Println()
	fmt.Printf("Result: All %d blocks pass (filter has high selectivity)\n", stats[4].Selected)
	fmt.Println()

	// Stage 6: Row evaluation
	fmt.Println("--- Stage 6: Row-Level Evaluation ---")
	fmt.Println()
	fmt.Println("Bitmap lifecycle:")
	fmt.Println()
	fmt.Println("  bm.init(10000)  // 10K rows per block")
	fmt.Println("  bm.setBits()    // [1111111111...]")
	fmt.Println()
	fmt.Println("  filterPhrase(\"level\", \"error\").applyToBlockSearch(bs, bm)")
	fmt.Println("    Load level column values")
	fmt.Println("    Scan rows, clear non-matching bits")
	fmt.Println("    bm = [0100110001...]  // 3200 bits remaining")
	fmt.Println()
	fmt.Printf("Result: %d rows found, %d skipped (%.1f%% reduction)\n",
		stats[5].Selected, stats[5].Skipped, stats[5].Reduction())
	fmt.Println()

	// Final summary
	ShowFinalSummary(stats)
}

func ShowFinalSummary(stats []PruningStats) {
	fmt.Println("=== Final Summary ===")
	fmt.Println()

	fmt.Println("Data reduction cascade:")
	fmt.Println()

	totalReduction := 1.0
	input := 1.0

	for i, ps := range stats {
		reduction := ps.Reduction() / 100
		input = input * (1 - reduction)
		totalReduction = 1 - input

		fmt.Printf("  After %s: %.4f%% of original data remaining\n",
			ps.Stage, input*100)

		if i == len(stats)-1 {
			fmt.Println()
			fmt.Printf("Final rows: %d (from ~10 billion)\n", ps.Selected)
			fmt.Printf("Total reduction: %.6f%%\n", totalReduction*100)
		}
	}

	fmt.Println()
	fmt.Println("I/O saved:")
	fmt.Println("  Original: 1 TB")
	fmt.Printf("  Actual:   ~%.2f MB\n", input*1000000/1024)
	fmt.Printf("  Saved:    %.2f%%\n", (1-input)*100)
}

// ==================== Code Reference ====================

func ShowCodeReferences() {
	fmt.Println("=== Code References ===")
	fmt.Println()

	refs := []struct {
		file  string
		lines string
		desc  string
	}{
		{"storage_search.go", "274-312", "RunQuery - entry point"},
		{"storage_search.go", "1412-1493", "searchParallel - worker pool"},
		{"storage_search.go", "1495-1530", "getPartitionsForTimeRange"},
		{"storage_search.go", "1650-1671", "getPartsForTimeRange"},
		{"storage_search.go", "1860-1961", "searchByStreamIDs"},
		{"block_search.go", "217-238", "blockSearch.search"},
		{"filter_and.go", "85-120", "matchBloomFilters"},
	}

	fmt.Printf("%-25s %-15s %s\n", "File", "Lines", "Description")
	fmt.Println(strings.Repeat("-", 70))
	for _, ref := range refs {
		fmt.Printf("%-25s %-15s %s\n", ref.file, ref.lines, ref.desc)
	}
}
