package main

import "fmt"

func main() {
	// Run Lab 1: Heuristic Reimplementation
	HeuristicReimplementationDemo()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Run Lab 2: Heap Merge
	HeapMergeDemo()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Run Lab 3: Delete-Aware Merge
	DeleteAwareMergeDemo()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Bonus: Complexity Analysis
	ComplexityAnalysis()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Bonus: Space Reclamation
	SpaceReclamationAnalysis()
}
