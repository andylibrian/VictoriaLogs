package main

import "fmt"

func main() {
	// Run Lab 1: Bloom Implementation
	BloomImplementationDemo()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Run Lab 2: Parameter Sweep
	ParameterSweep(1000, 0.01)
	DetailedParameterAnalysis()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Run Lab 3: VictoriaLogs Alignment
	VictoriaLogsAnalysis()
	TokenizationAnalysis()
	BloomBestPractices()
}
