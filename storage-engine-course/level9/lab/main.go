package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("=== Level 9: IndexDB and Mergeset Labs ===")
	fmt.Println()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "reconstruction":
			IndexItemReconstructionDemo()
		case "merge":
			MergeConsolidationDemo()
		case "cache":
			CacheCorrectnessDemo()
		default:
			printUsage()
		}
		return
	}

	// Run all labs
	IndexItemReconstructionDemo()
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	MergeConsolidationDemo()
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	CacheCorrectnessDemo()
}

func printUsage() {
	fmt.Println("Usage: lab [reconstruction|merge|cache]")
	fmt.Println()
	fmt.Println("  reconstruction - Index item reconstruction")
	fmt.Println("  merge          - Merge consolidation examples")
	fmt.Println("  cache          - Cache correctness analysis")
}
