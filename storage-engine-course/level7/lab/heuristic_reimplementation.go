package main

import (
	"fmt"
	"sort"
)

// ==================== Constants ====================

const (
	minMergeMultiplier  = 1.7
	defaultPartsToMerge = 15
	maxOutBytes         = 500 * 1024 * 1024 // 500 MB
)

// ==================== Part Simulation ====================

type Part struct {
	ID                    int
	CompressedSizeBytes   uint64
	UncompressedSizeBytes uint64
	MinTimestamp          int64
	MaxTimestamp          int64
	RowsCount             uint64
	BlocksCount           uint64
}

// PartWrapper wraps a part for merge selection
type PartWrapper struct {
	Part      *Part
	IsInMerge bool
}

// ==================== Merge Selection Heuristic ====================

// AppendPartsToMerge implements the merge candidate selection algorithm
// This mirrors appendPartsToMerge in datadb.go:1571-1635
func AppendPartsToMerge(src []*PartWrapper, maxOutBytes uint64) []*PartWrapper {
	if len(src) < 2 {
		return nil
	}

	// Step 1: Filter out too big parts
	maxInPartBytes := uint64(float64(maxOutBytes) / minMergeMultiplier)
	filtered := make([]*PartWrapper, 0, len(src))
	for _, pw := range src {
		if pw.Part.CompressedSizeBytes > maxInPartBytes {
			continue
		}
		filtered = append(filtered, pw)
	}
	src = filtered

	if len(src) < 2 {
		return nil
	}

	// Step 2: Sort parts for optimal merge
	sortPartsForOptimalMerge(src)

	// Step 3: Calculate window bounds
	maxSrcParts := defaultPartsToMerge
	if maxSrcParts > len(src) {
		maxSrcParts = len(src)
	}
	minSrcParts := (maxSrcParts + 1) / 2
	if minSrcParts < 2 {
		minSrcParts = 2
	}

	// Step 4: Exhaustive search for best merge candidate
	var bestWindow []*PartWrapper
	maxMergeRatio := float64(0)

	for windowSize := minSrcParts; windowSize <= maxSrcParts; windowSize++ {
		for startPos := 0; startPos <= len(src)-windowSize; startPos++ {
			window := src[startPos : startPos+windowSize]

			// Balance check: smallest * count >= largest
			smallest := window[0].Part.CompressedSizeBytes
			largest := window[len(window)-1].Part.CompressedSizeBytes
			if smallest*uint64(len(window)) < largest {
				// Too lopsided - skip
				continue
			}

			// Size check: output must fit
			outSize := getCompressedSize(window)
			if outSize > maxOutBytes {
				// Further windows only get bigger - break
				break
			}

			// Calculate merge ratio
			mergeRatio := float64(outSize) / float64(largest)
			if mergeRatio > maxMergeRatio {
				maxMergeRatio = mergeRatio
				bestWindow = make([]*PartWrapper, len(window))
				copy(bestWindow, window)
			}
		}
	}

	// Step 5: Threshold gate
	minRatio := float64(defaultPartsToMerge) / 2
	if minRatio < minMergeMultiplier {
		minRatio = minMergeMultiplier
	}

	if maxMergeRatio < minRatio {
		return nil
	}

	return bestWindow
}

func sortPartsForOptimalMerge(pws []*PartWrapper) {
	sort.Slice(pws, func(i, j int) bool {
		a := pws[i].Part
		b := pws[j].Part
		if a.CompressedSizeBytes == b.CompressedSizeBytes {
			// Tie-breaker: newer first
			return a.MinTimestamp > b.MinTimestamp
		}
		return a.CompressedSizeBytes < b.CompressedSizeBytes
	})
}

func getCompressedSize(pws []*PartWrapper) uint64 {
	total := uint64(0)
	for _, pw := range pws {
		total += pw.Part.CompressedSizeBytes
	}
	return total
}

// ==================== Heuristic Analysis ====================

func AnalyzeMergeSelection(parts []*Part, maxOutBytes uint64) {
	fmt.Println("=== Merge Selection Analysis ===")
	fmt.Println()

	// Create wrappers
	src := make([]*PartWrapper, len(parts))
	for i, p := range parts {
		src[i] = &PartWrapper{Part: p}
	}

	// Show input parts
	fmt.Printf("Input parts: %d\n", len(parts))
	fmt.Println()
	PrintPartsTable(parts)
	fmt.Println()

	// Step 1: Size filter
	maxInPartBytes := uint64(float64(maxOutBytes) / minMergeMultiplier)
	fmt.Printf("Size filter: maxInPartBytes = %d bytes (%.1f MB)\n",
		maxInPartBytes, float64(maxInPartBytes)/(1024*1024))

	filtered := 0
	for _, p := range parts {
		if p.CompressedSizeBytes > maxInPartBytes {
			fmt.Printf("  EXCLUDED: Part %d (%.1f MB) > %.1f MB\n",
				p.ID, float64(p.CompressedSizeBytes)/(1024*1024),
				float64(maxInPartBytes)/(1024*1024))
			filtered++
		}
	}
	fmt.Printf("Filtered out: %d parts\n", filtered)
	fmt.Println()

	// Run selection
	selected := AppendPartsToMerge(src, maxOutBytes)

	if selected == nil {
		fmt.Println("Result: NO MERGE SELECTED")
		fmt.Println()
		ExplainWhyNoMerge(src, maxOutBytes)
	} else {
		fmt.Printf("Result: %d parts selected for merge\n", len(selected))
		fmt.Println()
		PrintSelectedParts(selected)
	}
}

func PrintPartsTable(parts []*Part) {
	fmt.Printf("%-6s %12s %12s %15s\n", "Part", "Size (MB)", "Rows", "Timestamp")
	fmt.Println("---------------------------------------------------")
	for _, p := range parts {
		fmt.Printf("%-6d %12.2f %12d %15d\n",
			p.ID,
			float64(p.CompressedSizeBytes)/(1024*1024),
			p.RowsCount,
			p.MinTimestamp)
	}
}

func PrintSelectedParts(pws []*PartWrapper) {
	fmt.Println("Selected parts:")
	totalSize := uint64(0)
	for _, pw := range pws {
		fmt.Printf("  Part %d: %.2f MB, %d rows\n",
			pw.Part.ID,
			float64(pw.Part.CompressedSizeBytes)/(1024*1024),
			pw.Part.RowsCount)
		totalSize += pw.Part.CompressedSizeBytes
	}

	largest := pws[len(pws)-1].Part.CompressedSizeBytes
	mergeRatio := float64(totalSize) / float64(largest)

	fmt.Println()
	fmt.Printf("Merge statistics:\n")
	fmt.Printf("  Total output size: %.2f MB\n", float64(totalSize)/(1024*1024))
	fmt.Printf("  Largest input: %.2f MB\n", float64(largest)/(1024*1024))
	fmt.Printf("  Merge ratio: %.2fx\n", mergeRatio)
	fmt.Printf("  Threshold: %.2fx\n", float64(defaultPartsToMerge)/2)
}

func ExplainWhyNoMerge(src []*PartWrapper, maxOutBytes uint64) {
	if len(src) < 2 {
		fmt.Println("Reason: Less than 2 parts available")
		return
	}

	// Check if all filtered
	maxInPartBytes := uint64(float64(maxOutBytes) / minMergeMultiplier)
	filtered := make([]*PartWrapper, 0)
	for _, pw := range src {
		if pw.Part.CompressedSizeBytes <= maxInPartBytes {
			filtered = append(filtered, pw)
		}
	}

	if len(filtered) < 2 {
		fmt.Println("Reason: All parts exceed size filter")
		return
	}

	// Sort and check windows
	sortPartsForOptimalMerge(filtered)

	maxSrcParts := defaultPartsToMerge
	if maxSrcParts > len(filtered) {
		maxSrcParts = len(filtered)
	}
	minSrcParts := (maxSrcParts + 1) / 2
	if minSrcParts < 2 {
		minSrcParts = 2
	}

	bestRatio := float64(0)

	for windowSize := minSrcParts; windowSize <= maxSrcParts; windowSize++ {
		for startPos := 0; startPos <= len(filtered)-windowSize; startPos++ {
			window := filtered[startPos : startPos+windowSize]

			smallest := window[0].Part.CompressedSizeBytes
			largest := window[len(window)-1].Part.CompressedSizeBytes

			if smallest*uint64(len(window)) < largest {
				continue // Balance check failed
			}

			outSize := getCompressedSize(window)
			if outSize > maxOutBytes {
				continue
			}

			mergeRatio := float64(outSize) / float64(largest)
			if mergeRatio > bestRatio {
				bestRatio = mergeRatio
			}
		}
	}

	threshold := float64(defaultPartsToMerge) / 2
	if threshold < minMergeMultiplier {
		threshold = minMergeMultiplier
	}

	fmt.Printf("Best merge ratio found: %.2fx\n", bestRatio)
	fmt.Printf("Required threshold: %.2fx\n", threshold)

	if bestRatio < threshold {
		fmt.Println("Reason: Merge ratio below threshold (not enough benefit)")
	}
}

// ==================== No-Merge Examples ====================

func NoMergeExamples() {
	fmt.Println("=== Examples Where No Merge Is Selected ===")
	fmt.Println()

	// Example 1: All parts large and similar
	fmt.Println("--- Example 1: Large Similar Parts ---")
	fmt.Println()

	parts1 := []*Part{
		{ID: 1, CompressedSizeBytes: 100 * 1024 * 1024, RowsCount: 1000000, MinTimestamp: 1000},
		{ID: 2, CompressedSizeBytes: 105 * 1024 * 1024, RowsCount: 1050000, MinTimestamp: 2000},
		{ID: 3, CompressedSizeBytes: 110 * 1024 * 1024, RowsCount: 1100000, MinTimestamp: 3000},
	}

	AnalyzeMergeSelection(parts1, maxOutBytes)
	fmt.Println()

	// Example 2: One huge part dominates
	fmt.Println("--- Example 2: One Huge Part Dominates ---")
	fmt.Println()

	parts2 := []*Part{
		{ID: 1, CompressedSizeBytes: 1 * 1024 * 1024, RowsCount: 10000, MinTimestamp: 1000},
		{ID: 2, CompressedSizeBytes: 2 * 1024 * 1024, RowsCount: 20000, MinTimestamp: 2000},
		{ID: 3, CompressedSizeBytes: 500 * 1024 * 1024, RowsCount: 5000000, MinTimestamp: 3000},
	}

	AnalyzeMergeSelection(parts2, 1024*1024*1024) // 1 GB max
	fmt.Println()

	// Example 3: Only one part
	fmt.Println("--- Example 3: Only One Part ---")
	fmt.Println()

	parts3 := []*Part{
		{ID: 1, CompressedSizeBytes: 50 * 1024 * 1024, RowsCount: 500000, MinTimestamp: 1000},
	}

	AnalyzeMergeSelection(parts3, maxOutBytes)
	fmt.Println()
}

// ==================== Successful Merge Examples ====================

func SuccessfulMergeExamples() {
	fmt.Println("=== Examples Where Merge IS Selected ===")
	fmt.Println()

	// Example 1: Many small similar parts
	fmt.Println("--- Example 1: Many Small Similar Parts ---")
	fmt.Println()

	parts1 := make([]*Part, 15)
	for i := 0; i < 15; i++ {
		parts1[i] = &Part{
			ID:                  i + 1,
			CompressedSizeBytes: 2 * 1024 * 1024, // 2 MB each
			RowsCount:           20000,
			MinTimestamp:        int64(i * 1000),
		}
	}

	AnalyzeMergeSelection(parts1, maxOutBytes)
	fmt.Println()

	// Example 2: Graduated sizes
	fmt.Println("--- Example 2: Graduated Sizes ---")
	fmt.Println()

	parts2 := []*Part{
		{ID: 1, CompressedSizeBytes: 1 * 1024 * 1024, RowsCount: 10000, MinTimestamp: 1000},
		{ID: 2, CompressedSizeBytes: 2 * 1024 * 1024, RowsCount: 20000, MinTimestamp: 2000},
		{ID: 3, CompressedSizeBytes: 3 * 1024 * 1024, RowsCount: 30000, MinTimestamp: 3000},
		{ID: 4, CompressedSizeBytes: 4 * 1024 * 1024, RowsCount: 40000, MinTimestamp: 4000},
		{ID: 5, CompressedSizeBytes: 5 * 1024 * 1024, RowsCount: 50000, MinTimestamp: 5000},
		{ID: 6, CompressedSizeBytes: 6 * 1024 * 1024, RowsCount: 60000, MinTimestamp: 6000},
		{ID: 7, CompressedSizeBytes: 7 * 1024 * 1024, RowsCount: 70000, MinTimestamp: 7000},
		{ID: 8, CompressedSizeBytes: 8 * 1024 * 1024, RowsCount: 80000, MinTimestamp: 8000},
	}

	AnalyzeMergeSelection(parts2, maxOutBytes)
	fmt.Println()
}

// ==================== Main Demo ====================

func HeuristicReimplementationDemo() {
	fmt.Println("=== Level 7 Lab 1: Merge Selection Heuristic ===")
	fmt.Println()
	fmt.Println("This program re-implements the merge candidate selection algorithm")
	fmt.Printf("Constants: minMergeMultiplier=%.1f, defaultPartsToMerge=%d, maxOutBytes=%d MB\n",
		minMergeMultiplier, defaultPartsToMerge, maxOutBytes/(1024*1024))
	fmt.Println()

	// Show no-merge examples
	NoMergeExamples()

	fmt.Println("========================================")
	fmt.Println()

	// Show successful merge examples
	SuccessfulMergeExamples()

	fmt.Println("========================================")
	fmt.Println()

	// Algorithm summary
	PrintAlgorithmSummary()
}

func PrintAlgorithmSummary() {
	fmt.Println("=== Algorithm Summary ===")
	fmt.Println()
	fmt.Println("Step 1: Size Filter")
	fmt.Printf("  - Exclude parts > maxOutBytes / %.1f = %.1f MB\n",
		minMergeMultiplier, float64(maxOutBytes/minMergeMultiplier)/(1024*1024))
	fmt.Println()

	fmt.Println("Step 2: Sort")
	fmt.Println("  - Sort by size ascending")
	fmt.Println("  - Tie-breaker: newer timestamp first")
	fmt.Println()

	fmt.Println("Step 3: Window Bounds")
	fmt.Printf("  - minSrcParts = (maxSrcParts + 1) / 2 (min 2)\n")
	fmt.Printf("  - maxSrcParts = min(%d, available parts)\n", defaultPartsToMerge)
	fmt.Println()

	fmt.Println("Step 4: Exhaustive Search")
	fmt.Println("  - Try all consecutive windows in bounds")
	fmt.Println("  - Balance check: smallest * count >= largest")
	fmt.Println("  - Track best merge ratio")
	fmt.Println()

	fmt.Println("Step 5: Threshold Gate")
	fmt.Printf("  - Required ratio: max(%.1f, %d/2) = %.1fx\n",
		minMergeMultiplier, defaultPartsToMerge, float64(defaultPartsToMerge)/2)
	fmt.Println("  - If best ratio < threshold: no merge")
}
