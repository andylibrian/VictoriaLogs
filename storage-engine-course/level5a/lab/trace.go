package main

import (
	"fmt"
	"sort"
	"strings"
)

func RunMergeTraceDemo() {
	fmt.Println("=== Merge Selection Algorithm Trace ===")
	fmt.Println()
	fmt.Println("This traces the appendPartsToMerge algorithm from datadb.go")
	fmt.Println()

	ShowAlgorithmSteps()
	ShowMergeTraceExample()
	ShowEdgeCases()
}

func ShowAlgorithmSteps() {
	fmt.Println("=== Algorithm Steps ===")
	fmt.Println()

	steps := []struct {
		step        string
		description string
		code        string
	}{
		{
			step:        "1. Filter large parts",
			description: "Remove parts larger than maxOutBytes / minMergeMultiplier",
			code:        "maxInPartBytes := uint64(float64(maxOutBytes) / minMergeMultiplier)",
		},
		{
			step:        "2. Sort by size",
			description: "Sort remaining parts by size (ascending), then by timestamp (descending)",
			code:        "sortPartsForOptimalMerge(src)",
		},
		{
			step:        "3. Determine window sizes",
			description: "Try windows from ceil(maxSrcParts/2) to maxSrcParts",
			code:        "minSrcParts := (maxSrcParts + 1) / 2",
		},
		{
			step:        "4. Exhaustive search",
			description: "Check all consecutive windows of each size",
			code:        "for i := minSrcParts; i <= maxSrcParts; i++",
		},
		{
			step:        "5. Score windows",
			description: "Calculate merge ratio: outputSize / largestInputSize",
			code:        "m := float64(outSize) / float64(largestInputSize)",
		},
		{
			step:        "6. Apply threshold",
			description: "Only accept if ratio >= max(defaultPartsToMerge/2, minMergeMultiplier)",
			code:        "if maxM < minM { return dst }",
		},
	}

	for _, s := range steps {
		fmt.Printf("%s\n", s.step)
		fmt.Printf("  Description: %s\n", s.description)
		fmt.Printf("  Code: %s\n", s.code)
		fmt.Println()
	}
}

type TracePart struct {
	ID   int
	Size uint64
}

func ShowMergeTraceExample() {
	fmt.Println("=== Example: Merge Selection Trace ===")
	fmt.Println()

	parts := []TracePart{
		{ID: 1, Size: 1 * 1024 * 1024},
		{ID: 2, Size: 1 * 1024 * 1024},
		{ID: 3, Size: 1 * 1024 * 1024},
		{ID: 4, Size: 1 * 1024 * 1024},
		{ID: 5, Size: 1 * 1024 * 1024},
		{ID: 6, Size: 2 * 1024 * 1024},
		{ID: 7, Size: 2 * 1024 * 1024},
		{ID: 8, Size: 5 * 1024 * 1024},
		{ID: 9, Size: 10 * 1024 * 1024},
		{ID: 10, Size: 20 * 1024 * 1024},
	}

	fmt.Println("Input parts (before sorting):")
	for _, p := range parts {
		fmt.Printf("  Part %d: %d MB\n", p.ID, p.Size/(1024*1024))
	}
	fmt.Println()

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].Size < parts[j].Size
	})

	fmt.Println("After sorting by size:")
	for _, p := range parts {
		fmt.Printf("  Part %d: %d MB\n", p.ID, p.Size/(1024*1024))
	}
	fmt.Println()

	fmt.Println("Searching for best merge window:")
	fmt.Println()

	minSrcParts := 8
	maxSrcParts := 15
	if maxSrcParts > len(parts) {
		maxSrcParts = len(parts)
	}

	type candidate struct {
		parts   []TracePart
		ratio   float64
		outSize uint64
	}
	var best candidate

	for count := minSrcParts; count <= maxSrcParts; count++ {
		for start := 0; start <= len(parts)-count; start++ {
			window := parts[start : start+count]
			var outSize uint64
			for _, p := range window {
				outSize += p.Size
			}
			largestSize := window[len(window)-1].Size
			ratio := float64(outSize) / float64(largestSize)

			partIDs := make([]int, len(window))
			for i, p := range window {
				partIDs[i] = p.ID
			}

			threshold := 7.5
			status := "✗ rejected"
			if ratio >= threshold && ratio > best.ratio {
				status = "✓ accepted"
				best = candidate{parts: window, ratio: ratio, outSize: outSize}
			}

			fmt.Printf("  Window [parts %v]: %d MB / %d MB = %.2fx %s\n",
				partIDs, outSize/(1024*1024), largestSize/(1024*1024), ratio, status)
		}
	}
	fmt.Println()

	if best.parts != nil {
		fmt.Println("Best merge candidate:")
		fmt.Printf("  Parts: %v\n", getPartIDs(best.parts))
		fmt.Printf("  Output size: %d MB\n", best.outSize/(1024*1024))
		fmt.Printf("  Merge ratio: %.2fx\n", best.ratio)
		fmt.Println("  Result: MERGE WILL PROCEED")
	} else {
		fmt.Println("No merge candidate meets threshold (7.5x)")
		fmt.Println("  Result: NO MERGE")
	}
	fmt.Println()
}

func getPartIDs(parts []TracePart) []int {
	ids := make([]int, len(parts))
	for i, p := range parts {
		ids[i] = p.ID
	}
	return ids
}

func ShowEdgeCases() {
	fmt.Println("=== Edge Cases ===")
	fmt.Println()

	cases := []struct {
		name        string
		description string
		parts       []TracePart
		result      string
	}{
		{
			name:        "Single part",
			description: "Cannot merge a single part",
			parts:       []TracePart{{ID: 1, Size: 10 * 1024 * 1024}},
			result:      "NO MERGE (need at least 2 parts)",
		},
		{
			name:        "All same size",
			description: "Equal-size parts give maximum ratio",
			parts: []TracePart{
				{ID: 1, Size: 10 * 1024 * 1024},
				{ID: 2, Size: 10 * 1024 * 1024},
				{ID: 3, Size: 10 * 1024 * 1024},
				{ID: 4, Size: 10 * 1024 * 1024},
				{ID: 5, Size: 10 * 1024 * 1024},
				{ID: 6, Size: 10 * 1024 * 1024},
				{ID: 7, Size: 10 * 1024 * 1024},
				{ID: 8, Size: 10 * 1024 * 1024},
			},
			result: "MERGE: 8 parts → 1 part (ratio = 8.0x)",
		},
		{
			name:        "One huge, many tiny",
			description: "Ratio check prevents wasteful merge",
			parts: []TracePart{
				{ID: 1, Size: 1 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024},
				{ID: 3, Size: 1 * 1024 * 1024},
				{ID: 4, Size: 1 * 1024 * 1024},
				{ID: 5, Size: 1 * 1024 * 1024},
				{ID: 6, Size: 1 * 1024 * 1024},
				{ID: 7, Size: 1 * 1024 * 1024},
				{ID: 8, Size: 100 * 1024 * 1024},
			},
			result: "NO MERGE (would include 100 MB part, ratio too low)",
		},
		{
			name:        "Two parts only",
			description: "Minimum window size is 2",
			parts: []TracePart{
				{ID: 1, Size: 10 * 1024 * 1024},
				{ID: 2, Size: 10 * 1024 * 1024},
			},
			result: "NO MERGE (ratio = 2.0x < 7.5x threshold)",
		},
		{
			name:        "Just above threshold",
			description: "8 equal parts gives 8.0x ratio",
			parts: []TracePart{
				{ID: 1, Size: 1 * 1024 * 1024},
				{ID: 2, Size: 1 * 1024 * 1024},
				{ID: 3, Size: 1 * 1024 * 1024},
				{ID: 4, Size: 1 * 1024 * 1024},
				{ID: 5, Size: 1 * 1024 * 1024},
				{ID: 6, Size: 1 * 1024 * 1024},
				{ID: 7, Size: 1 * 1024 * 1024},
				{ID: 8, Size: 1 * 1024 * 1024},
			},
			result: "MERGE: 8 parts → 1 part (ratio = 8.0x > 7.5x)",
		},
	}

	for _, c := range cases {
		fmt.Printf("%s:\n", c.name)
		fmt.Printf("  Description: %s\n", c.description)
		fmt.Printf("  Parts: %v\n", formatParts(c.parts))
		fmt.Printf("  Result: %s\n", c.result)
		fmt.Println()
	}

	fmt.Println("Key insight: The algorithm protects against unbalanced merges.")
	fmt.Println("  - Merging a 1 MB part with a 100 MB part wastes 99 MB of I/O")
	fmt.Println("  - The 7.5x threshold ensures each merge is worth the cost")
	fmt.Println()
}

func formatParts(parts []TracePart) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, p := range parts {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%dMB", p.Size/(1024*1024)))
	}
	sb.WriteString("]")
	return sb.String()
}
