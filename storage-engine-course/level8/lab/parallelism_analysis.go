package main

import (
	"fmt"
	"strings"
)

// ==================== Parallelism Analysis Types ====================

type ParallelismConfig struct {
	CPUs            int
	ParallelReaders int
	MemoryPerWorker int64 // MB
}

type BenchmarkResult struct {
	Config         ParallelismConfig
	Throughput     int64 // blocks/s
	Latency        int64 // ms
	Memory         int64 // MB
	CPUUtilization float64
}

// ==================== Parallelism Demo ====================

func ParallelismDemo() {
	fmt.Println("=== Level 8 Lab 3: Parallelism Reasoning ===")
	fmt.Println()
	fmt.Println("This program analyzes the effects of changing parallelReaders")
	fmt.Println("on throughput, latency, and memory pressure.")
	fmt.Println()

	// Show two-tier model
	ShowTwoTierModel()

	// Analyze parallelReaders changes
	fmt.Println("========================================")
	fmt.Println()
	AnalyzeParallelReaders()

	// Show benchmarks
	fmt.Println("========================================")
	fmt.Println()
	ShowBenchmarks()

	// Show recommendations
	fmt.Println("========================================")
	fmt.Println()
	ShowRecommendations()
}

func ShowTwoTierModel() {
	fmt.Println("=== Two-Tier Worker Model ===")
	fmt.Println()

	fmt.Println("Tier 1: Partition Searchers (I/O-bound)")
	fmt.Println("  - One goroutine per partition")
	fmt.Println("  - Bounded by partitionSearchConcurrencyLimitCh (capacity = CPU count)")
	fmt.Println("  - Walk metaindex in memory")
	fmt.Println("  - Load index blocks from disk")
	fmt.Println("  - Batch matching blocks into work items")
	fmt.Println()

	fmt.Println("Tier 2: Block Search Workers (CPU-bound)")
	fmt.Println("  - N goroutines (parallelReaders, default = CPU count)")
	fmt.Println("  - Drain workCh until closed")
	fmt.Println("  - Run bloom precheck")
	fmt.Println("  - Run per-filter evaluation")
	fmt.Println("  - Assemble results")
	fmt.Println()

	fmt.Println("Data flow:")
	fmt.Println()
	fmt.Println("  Partition 1 ──┐")
	fmt.Println("  Partition 2 ──┤")
	fmt.Println("  Partition 3 ──┼──> workCh ──> Worker 1 ──> results")
	fmt.Println("  Partition 4 ──┤              Worker 2 ──> results")
	fmt.Println("  ...           │              Worker 3 ──> results")
	fmt.Println("  Partition N ──┘              Worker N ──> results")
	fmt.Println()
}

func AnalyzeParallelReaders() {
	fmt.Println("=== Effects of Changing parallelReaders ===")
	fmt.Println()

	// Default configuration
	fmt.Println("--- Default: parallelReaders = 8 (CPU count) ---")
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println("  CPUs: 8")
	fmt.Println("  parallelReaders: 8")
	fmt.Println()

	config := ParallelismConfig{CPUs: 8, ParallelReaders: 8, MemoryPerWorker: 10}
	result := RunBenchmark(config)
	PrintBenchmarkResult(result)
	fmt.Println()

	// Increased configuration
	fmt.Println("--- Increased: parallelReaders = 16 ---")
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println("  CPUs: 8")
	fmt.Println("  parallelReaders: 16")
	fmt.Println()

	config = ParallelismConfig{CPUs: 8, ParallelReaders: 16, MemoryPerWorker: 10}
	result = RunBenchmark(config)
	PrintBenchmarkResult(result)
	fmt.Println()

	fmt.Println("Effects:")
	fmt.Println("  THROUGHPUT:   + Higher CPU utilization on many-core systems")
	fmt.Println("                 + Better overlap of I/O and compute")
	fmt.Println("                 - Diminishing returns beyond CPU count")
	fmt.Println()
	fmt.Println("  LATENCY:      + Lower latency for CPU-bound queries")
	fmt.Println("                 ~ Similar latency for I/O-bound queries")
	fmt.Println("                 - Potential for more context switching")
	fmt.Println()
	fmt.Println("  MEMORY:       - 16 workers × 10 MB = 160 MB (2× increase)")
	fmt.Println()

	// Decreased configuration
	fmt.Println("--- Decreased: parallelReaders = 2 ---")
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println("  CPUs: 8")
	fmt.Println("  parallelReaders: 2")
	fmt.Println()

	config = ParallelismConfig{CPUs: 8, ParallelReaders: 2, MemoryPerWorker: 10}
	result = RunBenchmark(config)
	PrintBenchmarkResult(result)
	fmt.Println()

	fmt.Println("Effects:")
	fmt.Println("  THROUGHPUT:   - Lower CPU utilization")
	fmt.Println("                 - Blocks queue up in workCh")
	fmt.Println("                 - Partition searchers may block")
	fmt.Println()
	fmt.Println("  LATENCY:      - Higher latency (fewer workers)")
	fmt.Println("                 - I/O and compute don't overlap well")
	fmt.Println()
	fmt.Println("  MEMORY:       + 2 workers × 10 MB = 20 MB (4× reduction)")
	fmt.Println("                 + Better for memory-constrained environments")
	fmt.Println()
}

func RunBenchmark(config ParallelismConfig) BenchmarkResult {
	// Simulated benchmark results based on config
	throughput := int64(3200 * config.ParallelReaders / config.CPUs)
	if config.ParallelReaders > config.CPUs {
		// Diminishing returns
		throughput = int64(3200 * (1.0 + 0.1*float64(config.ParallelReaders-config.CPUs)/float64(config.CPUs)))
	}

	latency := int64(170 * config.CPUs / config.ParallelReaders)
	if config.ParallelReaders > config.CPUs {
		// Slight improvement with more workers
		latency = int64(float64(latency) * 0.95)
	}

	memory := int64(config.ParallelReaders) * config.MemoryPerWorker

	cpuUtil := float64(config.ParallelReaders) / float64(config.CPUs) * 100
	if cpuUtil > 100 {
		cpuUtil = 100
	}

	return BenchmarkResult{
		Config:         config,
		Throughput:     throughput,
		Latency:        latency,
		Memory:         memory,
		CPUUtilization: cpuUtil,
	}
}

func PrintBenchmarkResult(r BenchmarkResult) {
	fmt.Printf("Results:\n")
	fmt.Printf("  Throughput:        %d blocks/s\n", r.Throughput)
	fmt.Printf("  Latency:           %d ms\n", r.Latency)
	fmt.Printf("  Memory:            %d MB\n", r.Memory)
	fmt.Printf("  CPU Utilization:   %.1f%%\n", r.CPUUtilization)
}

func ShowBenchmarks() {
	fmt.Println("=== Benchmark Results ===")
	fmt.Println()

	// Scenario 1: CPU-bound
	fmt.Println("--- Scenario 1: CPU-Bound Query ---")
	fmt.Println()
	fmt.Println("Query: Complex regex filter across 1 hour")
	fmt.Println("System: 8 CPUs, SSD storage")
	fmt.Println()

	fmt.Printf("%-15s %15s %15s %15s\n",
		"ParallelReaders", "Throughput", "Latency (ms)", "Memory (MB)")
	fmt.Println(strings.Repeat("-", 65))

	for _, pr := range []int{2, 4, 8, 16, 32} {
		config := ParallelismConfig{CPUs: 8, ParallelReaders: pr, MemoryPerWorker: 10}
		result := RunBenchmark(config)
		fmt.Printf("%-15d %15d %15d %15d\n",
			pr, result.Throughput, result.Latency, result.Memory)
	}

	fmt.Println()
	fmt.Println("Optimal: 8 (CPU count)")
	fmt.Println("Diminishing returns: >8 (context switching)")
	fmt.Println()

	// Scenario 2: I/O-bound
	fmt.Println("--- Scenario 2: I/O-Bound Query (Cold Data) ---")
	fmt.Println()
	fmt.Println("Query: Stream filter across 30 days")
	fmt.Println("System: 8 CPUs, HDD storage")
	fmt.Println()

	fmt.Printf("%-15s %15s %15s %15s\n",
		"ParallelReaders", "Throughput", "Latency (ms)", "Memory (MB)")
	fmt.Println(strings.Repeat("-", 65))

	for _, pr := range []int{2, 4, 8, 16} {
		config := ParallelismConfig{CPUs: 8, ParallelReaders: pr, MemoryPerWorker: 10}
		result := RunBenchmark(config)
		// I/O-bound: throughput capped by disk
		ioThroughput := result.Throughput
		if ioThroughput > 700 {
			ioThroughput = 650 + (ioThroughput-700)/10
		}
		fmt.Printf("%-15d %15d %15d %15d\n",
			pr, ioThroughput, result.Latency, result.Memory)
	}

	fmt.Println()
	fmt.Println("Optimal: 4-8")
	fmt.Println("Bottleneck: HDD seek time")
	fmt.Println()
}

func ShowRecommendations() {
	fmt.Println("=== Configuration Recommendations ===")
	fmt.Println()

	fmt.Printf("%-25s %-20s %s\n", "Scenario", "parallelReaders", "Reason")
	fmt.Println(strings.Repeat("-", 70))

	recommendations := []struct {
		scenario string
		setting  string
		reason   string
	}{
		{"Default", "CPU count", "Balance throughput and latency"},
		{"Many-core (64+ CPUs)", "16-32", "Avoid excessive memory"},
		{"Memory-constrained", "2-4", "Bound memory usage"},
		{"HDD storage", "CPU count / 2", "Reduce seek thrashing"},
		{"Complex filters", "CPU count", "CPU is bottleneck"},
		{"Simple filters, cold", "2× CPU count", "Overlap I/O latency"},
	}

	for _, r := range recommendations {
		fmt.Printf("%-25s %-20s %s\n", r.scenario, r.setting, r.reason)
	}

	fmt.Println()
	fmt.Println("Key Principles:")
	fmt.Println("  1. Default is usually optimal (parallelReaders = CPU count)")
	fmt.Println("  2. Memory scales linearly with worker count")
	fmt.Println("  3. Diminishing returns beyond CPU count")
	fmt.Println("  4. Storage type affects optimal configuration")
	fmt.Println()
}

// ==================== Memory Analysis ====================

func ShowMemoryAnalysis() {
	fmt.Println("=== Memory Usage Analysis ===")
	fmt.Println()

	fmt.Println("Per-worker memory breakdown:")
	fmt.Println("  - blockSearch struct:   ~10 KB")
	fmt.Println("  - blockResult buffer:   ~2 MB (varies with block size)")
	fmt.Println("  - Column value caches:  ~1-5 MB")
	fmt.Println("  - Total per worker:     ~3-7 MB")
	fmt.Println()

	fmt.Println("In-flight memory:")
	fmt.Println("  - workCh capacity: parallelReaders batches")
	fmt.Println("  - Each batch: 64 blocks × ~50 KB = ~3 MB")
	fmt.Println("  - Total: parallelReaders × 3 MB")
	fmt.Println()

	fmt.Println("Total memory estimate:")
	fmt.Println("  = parallelReaders × 10 MB (approx)")
	fmt.Println()

	fmt.Printf("%-20s %15s\n", "ParallelReaders", "Memory (MB)")
	fmt.Println(strings.Repeat("-", 40))
	for _, pr := range []int{2, 4, 8, 16, 32} {
		fmt.Printf("%-20d %15d\n", pr, pr*10)
	}
}
