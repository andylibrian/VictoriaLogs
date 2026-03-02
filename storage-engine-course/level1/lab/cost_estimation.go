package main

import (
	"fmt"
	"math"
)

// CostEstimation compares linear scan vs binary search complexity
func CostEstimation() {
	fmt.Println("=== Lab 2: Cost Estimation Exercise ===")
	fmt.Println()
	fmt.Println("This program compares the operation count of linear scan vs binary search")
	fmt.Println("for finding partitions in a time range query.")
	fmt.Println()

	// Table header
	fmt.Println("| Partitions | Linear Scan | Binary Search | Speedup  |")
	fmt.Println("|------------|-------------|---------------|----------|")

	partitionCounts := []int{10, 100, 365, 1000, 10000, 100000, 1000000, 10000000}

	for _, n := range partitionCounts {
		linearOps := n
		binaryOps := 2 * int(math.Ceil(math.Log2(float64(n))))

		speedup := float64(linearOps) / float64(binaryOps)

		fmt.Printf("| %10d | %11d | %13d | %8.0fx |\n", n, linearOps, binaryOps, speedup)
	}

	fmt.Println()
	fmt.Println("=== Analysis ===")
	fmt.Println()
	fmt.Println("LINEAR SCAN (O(n)):")
	fmt.Println("  - Must examine EVERY partition to check if it overlaps the query range")
	fmt.Println("  - Operations = number of partitions")
	fmt.Println("  - Example: 1,000,000 partitions = 1,000,000 comparisons")
	fmt.Println()
	fmt.Println("BINARY SEARCH (O(log n)):")
	fmt.Println("  - Two binary searches: one for left boundary, one for right boundary")
	fmt.Println("  - Each search: ~log₂(n) comparisons")
	fmt.Println("  - Total: ~2 × log₂(n) comparisons")
	fmt.Println("  - Example: 1,000,000 partitions = 2 × 20 = 40 comparisons")
	fmt.Println()
	fmt.Println("=== Why This Matters for VictoriaLogs ===")
	fmt.Println()
	fmt.Println("The storage hierarchy has MULTIPLE levels, each requiring lookups:")
	fmt.Println()
	fmt.Println("  Level 1: Partitions (sorted by day)")
	fmt.Println("    → Binary search finds relevant day partitions")
	fmt.Println()
	fmt.Println("  Level 2: Index Block Headers (sorted by streamID)")
	fmt.Println("    → Binary search finds relevant stream blocks")
	fmt.Println()
	fmt.Println("  Level 3: Block Headers (sorted by streamID, then time)")
	fmt.Println("    → Binary search finds relevant time ranges")
	fmt.Println()
	fmt.Println("If each level used linear scan:")
	fmt.Println("  Cost = O(n1) × O(n2) × O(n3)")
	fmt.Println()
	fmt.Println("With binary search at each level:")
	fmt.Println("  Cost = O(log n1) + O(log n2) + O(log n3)")
	fmt.Println()
	fmt.Println("=== Multi-Level Cost Example ===")
	fmt.Println()

	partitions := 365
	partsPerPartition := 50
	blocksPerPart := 1000

	fmt.Printf("Storage: %d partitions × %d parts × %d blocks = %d total blocks\n",
		partitions, partsPerPartition, blocksPerPart,
		partitions*partsPerPartition*blocksPerPart)
	fmt.Println()

	linearTotal := partitions * partsPerPartition * blocksPerPart
	fmt.Printf("Linear scan at every level: %d × %d × %d = %d block checks\n",
		partitions, partsPerPartition, blocksPerPart, linearTotal)

	partitionOps := 2 * int(math.Ceil(math.Log2(float64(partitions))))
	partsOps := 2 * int(math.Ceil(math.Log2(float64(partsPerPartition))))
	blocksOps := 2 * int(math.Ceil(math.Log2(float64(blocksPerPart))))
	selectedParts := 5
	selectedBlocks := 50
	binaryTotal := partitionOps + partsOps*selectedParts + blocksOps*selectedBlocks

	fmt.Printf("Binary search: %d (partition) + %d×%d (parts) + %d×%d (blocks) = %d operations\n",
		partitionOps, partsOps, selectedParts, blocksOps, selectedBlocks, binaryTotal)

	speedup := float64(linearTotal) / float64(binaryTotal)
	fmt.Printf("\nSpeedup: %.0fx\n", speedup)

	fmt.Println()
	fmt.Println("=== Real-World Impact ===")
	fmt.Println()
	fmt.Println("Each 'operation' at the block level involves:")
	fmt.Println("  1. Reading metadata from memory or disk")
	fmt.Println("  2. Comparing timestamps")
	fmt.Println("  3. Potentially checking bloom filters")
	fmt.Println()
	fmt.Println("Skipping 99.99% of blocks means:")
	fmt.Println("  - Less disk I/O")
	fmt.Println("  - Less CPU for decompression")
	fmt.Println("  - Lower memory usage")
	fmt.Println("  - Faster query response")
	fmt.Println()
	fmt.Println("This is why VictoriaLogs can query billions of logs in milliseconds.")
}
