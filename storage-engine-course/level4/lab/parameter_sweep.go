package main

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// ==================== Parameter Sweep Lab ====================

// ParameterSet represents a bloom filter configuration
type ParameterSet struct {
	BitsPerItem int
	NumHashes   int
}

// SweepResult holds results from testing a parameter set
type SweepResult struct {
	Params       ParameterSet
	MemoryBytes  int
	FPRate       float64
	CPUTime      time.Duration
	Operations   int
	OpsPerSecond float64
}

// ParameterSweep tests multiple bloom filter configurations
func ParameterSweep(n int, fpTarget float64) {
	fmt.Println("=== Level 4 Lab 2: Parameter Sweep ===")
	fmt.Println()
	fmt.Printf("Testing bloom filter configurations for %d items\n", n)
	fmt.Println()

	// Define parameter sets to test
	paramSets := []ParameterSet{
		{BitsPerItem: 8, NumHashes: 3},
		{BitsPerItem: 8, NumHashes: 4},
		{BitsPerItem: 10, NumHashes: 4},
		{BitsPerItem: 10, NumHashes: 5},
		{BitsPerItem: 12, NumHashes: 5},
		{BitsPerItem: 12, NumHashes: 6},
		{BitsPerItem: 14, NumHashes: 5},
		{BitsPerItem: 14, NumHashes: 6},
		{BitsPerItem: 16, NumHashes: 5}, // Near VictoriaLogs default
		{BitsPerItem: 16, NumHashes: 6}, // VictoriaLogs default
		{BitsPerItem: 16, NumHashes: 7},
		{BitsPerItem: 20, NumHashes: 6},
		{BitsPerItem: 20, NumHashes: 7},
		{BitsPerItem: 24, NumHashes: 7},
		{BitsPerItem: 24, NumHashes: 8},
	}

	results := make([]SweepResult, len(paramSets))

	// Generate test data
	testSet := generateWords(n, "test")
	querySet := generateWords(n*5, "query") // Larger query set for better stats

	// Test each parameter set
	for i, params := range paramSets {
		results[i] = testParameterSet(params, n, testSet, querySet)
	}

	// Print results table
	printResultsTable(results)

	// Analysis
	fmt.Println()
	fmt.Println("=== Analysis ===")
	fmt.Println()

	// Find best configurations
	bestFP := findBestByFP(results)
	bestCPU := findBestByCPU(results)
	bestBalance := findBestBalance(results)

	fmt.Printf("Best FP rate: bits/item=%d, hashes=%d (FP: %.2f%%)\n",
		bestFP.Params.BitsPerItem, bestFP.Params.NumHashes, bestFP.FPRate*100)
	fmt.Printf("Best CPU: bits/item=%d, hashes=%d (ops/sec: %.0f)\n",
		bestCPU.Params.BitsPerItem, bestCPU.Params.NumHashes, bestCPU.OpsPerSecond)
	fmt.Printf("Best balance: bits/item=%d, hashes=%d (score: %.2f)\n",
		bestBalance.Params.BitsPerItem, bestBalance.Params.NumHashes,
		calculateBalanceScore(bestBalance))
	fmt.Println()

	// Theoretical comparison
	fmt.Println("=== Theoretical vs Actual ===")
	fmt.Println()
	fmt.Printf("%-10s %-10s %-12s %-12s %-10s\n",
		"Bits/Item", "Hashes", "Theoretical", "Actual", "Diff")
	fmt.Println(strings.Repeat("-", 60))

	for _, r := range results {
		theoretical := theoreticalFPRate(r.Params.BitsPerItem, r.Params.NumHashes)
		diff := r.FPRate - theoretical
		fmt.Printf("%-10d %-10d %-12.2f%% %-12.2f%% %-10.2f%%\n",
			r.Params.BitsPerItem, r.Params.NumHashes,
			theoretical*100, r.FPRate*100, diff*100)
	}
}

func testParameterSet(params ParameterSet, n int, testSet, querySet []string) SweepResult {
	bf := NewBloomFilterWithParams(n, params.BitsPerItem, params.NumHashes)

	// Measure add time
	start := time.Now()
	for _, item := range testSet {
		bf.Add(item)
	}
	addDuration := time.Since(start)

	// Measure contains time (multiple iterations for accuracy)
	iterations := 100
	start = time.Now()
	for i := 0; i < iterations; i++ {
		for _, item := range querySet {
			bf.Contains(item)
		}
	}
	queryDuration := time.Since(start)

	// Test FP rate
	result := TestBloomFilter(bf, testSet, querySet)

	// Calculate operations per second
	totalOps := len(testSet) + len(querySet)*iterations
	totalTime := addDuration + queryDuration

	return SweepResult{
		Params:       params,
		MemoryBytes:  result.MemoryBytes,
		FPRate:       result.FPRate,
		CPUTime:      totalTime,
		Operations:   totalOps,
		OpsPerSecond: float64(totalOps) / totalTime.Seconds(),
	}
}

func printResultsTable(results []SweepResult) {
	fmt.Println()
	fmt.Printf("%-10s %-10s %12s %12s %15s\n",
		"Bits/Item", "Hashes", "Memory", "FP Rate", "Ops/Second")
	fmt.Println(strings.Repeat("-", 65))

	for _, r := range results {
		fmt.Printf("%-10d %-10d %12s %11.2f%% %15.0f\n",
			r.Params.BitsPerItem,
			r.Params.NumHashes,
			formatBytes(r.MemoryBytes),
			r.FPRate*100,
			r.OpsPerSecond)
	}
}

func formatBytes(n int) string {
	if n < 1024 {
		return fmt.Sprintf("%dB", n)
	} else if n < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(n)/1024)
	}
	return fmt.Sprintf("%.1fMB", float64(n)/(1024*1024))
}

// theoreticalFPRate calculates the theoretical false positive rate
// Formula: (1 - e^(-kn/m))^k
// where k = numHashes, n = numItems, m = bitsPerItem * n
func theoreticalFPRate(bitsPerItem, numHashes int) float64 {
	k := float64(numHashes)
	m := float64(bitsPerItem)

	// (1 - e^(-k/m))^k
	return math.Pow(1-math.Exp(-k/m), k)
}

func findBestByFP(results []SweepResult) SweepResult {
	best := results[0]
	for _, r := range results[1:] {
		if r.FPRate < best.FPRate {
			best = r
		}
	}
	return best
}

func findBestByCPU(results []SweepResult) SweepResult {
	best := results[0]
	for _, r := range results[1:] {
		if r.OpsPerSecond > best.OpsPerSecond {
			best = r
		}
	}
	return best
}

func findBestBalance(results []SweepResult) SweepResult {
	best := results[0]
	bestScore := calculateBalanceScore(best)

	for _, r := range results[1:] {
		score := calculateBalanceScore(r)
		if score > bestScore {
			best = r
			bestScore = score
		}
	}
	return best
}

// calculateBalanceScore scores a result based on FP rate, memory, and CPU
// Higher is better
func calculateBalanceScore(r SweepResult) float64 {
	// Normalize each metric (0-1 scale)
	// Lower FP is better, lower memory is better, higher ops/sec is better

	// These weights favor low FP rate (most important for correctness)
	// while still considering performance
	fpWeight := 0.5
	memWeight := 0.2
	cpuWeight := 0.3

	// FP score: inverse of FP rate (capped to avoid division by zero)
	fpScore := 1.0 / (r.FPRate + 0.001)

	// Memory score: inverse of memory (normalized to KB)
	memScore := 1.0 / (float64(r.MemoryBytes)/1024.0 + 0.1)

	// CPU score: ops per second (normalized)
	cpuScore := r.OpsPerSecond / 1e6 // Normalize to millions of ops

	return fpWeight*fpScore + memWeight*memScore + cpuWeight*cpuScore
}

// ==================== Detailed Analysis ====================

func DetailedParameterAnalysis() {
	fmt.Println("=== Detailed Parameter Analysis ===")
	fmt.Println()

	// Test VictoriaLogs defaults specifically
	n := 1000
	testSet := generateWords(n, "test")
	querySet := generateWords(n*10, "query") // 10x query set

	fmt.Printf("Test configuration:\n")
	fmt.Printf("  Items to insert: %d\n", n)
	fmt.Printf("  Query set size: %d\n", len(querySet))
	fmt.Println()

	// Test different bits per item with k=6
	fmt.Println("--- Effect of Bits Per Item (k=6 hashes) ---")
	fmt.Println()

	bitsPerItemTests := []int{8, 10, 12, 14, 16, 18, 20, 24, 32}
	fmt.Printf("%-12s %-12s %-12s\n", "Bits/Item", "FP Rate", "Memory")
	fmt.Println(strings.Repeat("-", 40))

	for _, bpi := range bitsPerItemTests {
		bf := NewBloomFilterWithParams(n, bpi, 6)
		result := TestBloomFilter(bf, testSet, querySet)
		fmt.Printf("%-12d %-11.2f%% %s\n",
			bpi, result.FPRate*100, formatBytes(result.MemoryBytes))
	}
	fmt.Println()

	// Test different hash counts with m=16
	fmt.Println("--- Effect of Hash Count (16 bits/item) ---")
	fmt.Println()

	hashCountTests := []int{3, 4, 5, 6, 7, 8, 10, 12}
	fmt.Printf("%-12s %-12s %-15s\n", "Hashes", "FP Rate", "Ops/Second")
	fmt.Println(strings.Repeat("-", 45))

	for _, k := range hashCountTests {
		params := ParameterSet{BitsPerItem: 16, NumHashes: k}
		result := testParameterSet(params, n, testSet, querySet)
		fmt.Printf("%-12d %-11.2f%% %-15.0f\n",
			k, result.FPRate*100, result.OpsPerSecond)
	}
	fmt.Println()

	// Diminishing returns analysis
	fmt.Println("--- Diminishing Returns Analysis ---")
	fmt.Println()

	// Going from 16->20 bits/item
	bf16 := NewBloomFilterWithParams(n, 16, 6)
	result16 := TestBloomFilter(bf16, testSet, querySet)

	bf20 := NewBloomFilterWithParams(n, 20, 6)
	result20 := TestBloomFilter(bf20, testSet, querySet)

	fmt.Printf("16 bits/item: FP=%.2f%%, Memory=%s\n",
		result16.FPRate*100, formatBytes(result16.MemoryBytes))
	fmt.Printf("20 bits/item: FP=%.2f%%, Memory=%s\n",
		result20.FPRate*100, formatBytes(result20.MemoryBytes))
	fmt.Printf("Improvement: %.1fx better FP for %.1fx more memory\n",
		result16.FPRate/result20.FPRate,
		float64(result20.MemoryBytes)/float64(result16.MemoryBytes))
	fmt.Println()

	// Going from 6->8 hashes
	bf6 := NewBloomFilterWithParams(n, 16, 6)
	result6 := TestBloomFilter(bf6, testSet, querySet)

	bf8 := NewBloomFilterWithParams(n, 16, 8)
	result8 := TestBloomFilter(bf8, testSet, querySet)

	fmt.Printf("6 hashes: FP=%.2f%%\n", result6.FPRate*100)
	fmt.Printf("8 hashes: FP=%.2f%%\n", result8.FPRate*100)
	fmt.Printf("Improvement: %.1fx better FP for %.1fx more CPU\n",
		result6.FPRate/result8.FPRate,
		8.0/6.0)
}
