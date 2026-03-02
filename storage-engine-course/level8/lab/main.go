package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("=== Level 8: Query Path Labs ===")
	fmt.Println()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "trace":
			QueryTraceDemo()
		case "skip":
			SkipEffectivenessDemo()
		case "parallel":
			ParallelismDemo()
		default:
			printUsage()
		}
		return
	}

	// Run all labs
	QueryTraceDemo()
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	SkipEffectivenessDemo()
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	ParallelismDemo()
}

func printUsage() {
	fmt.Println("Usage: lab [trace|skip|parallel]")
	fmt.Println()
	fmt.Println("  trace    - Query execution trace")
	fmt.Println("  skip     - Skip effectiveness analysis")
	fmt.Println("  parallel - Parallelism analysis")
}
