package main

import (
	"fmt"
	"strings"
)

func RunPolicySweepDemo() {
	fmt.Println("=== Lab 2: Policy Sweep ===")
	fmt.Println()
	fmt.Println("Comparing different merge policies:")
	fmt.Println("  - Eager: Merge any 2 parts (ratio >= 2x)")
	fmt.Println("  - Balanced: Default VictoriaLogs (ratio >= 7.5x)")
	fmt.Println("  - Lazy: Only merge when ratio >= 15x")
	fmt.Println("  - Never: No merging (baseline)")
	fmt.Println()

	ShowPolicyComparison()
	ShowTradeOffMatrix()
}

type PolicyResult struct {
	Name         string
	PartsToMerge int
	MergeRatio   float64
	FinalParts   int
	WriteAmp     float64
	ReadAmp      int
	TotalMerges  int
	TotalFlushes int
	BytesWritten uint64
}

func ShowPolicyComparison() {
	fmt.Println("=== Policy Comparison: Ingesting 100 MB ===")
	fmt.Println()

	policies := []struct {
		name         string
		partsToMerge int
		mergeRatio   float64
	}{
		{"Eager (2 parts, 2x)", 2, 2.0},
		{"Default (15 parts, 7.5x)", 15, 7.5},
		{"Lazy (15 parts, 15x)", 15, 15.0},
		{"Never merge", 0, 999999},
	}

	results := make([]PolicyResult, len(policies))

	for i, p := range policies {
		sim := NewLSMSimulator(
			1*1024*1024,
			10*1024*1024,
			100*1024*1024,
			p.partsToMerge,
			p.mergeRatio,
		)

		chunkSize := uint64(100 * 1024)
		for j := 0; j < 1000; j++ {
			sim.Put(chunkSize)
			if sim.State.MemtableSize >= sim.FlushThreshold {
				sim.Flush()
			}
			for sim.Compact() {
			}
		}

		if sim.State.MemtableSize > 0 {
			sim.Flush()
		}
		for sim.Compact() {
		}

		results[i] = PolicyResult{
			Name:         p.name,
			PartsToMerge: p.partsToMerge,
			MergeRatio:   p.mergeRatio,
			FinalParts:   sim.GetPartCount(),
			WriteAmp:     sim.GetWriteAmp(),
			ReadAmp:      sim.GetReadAmp(),
			TotalMerges:  sim.State.TotalMerges,
			TotalFlushes: sim.State.TotalFlushes,
			BytesWritten: sim.State.BytesWritten,
		}
	}

	fmt.Printf("%-25s %8s %10s %10s %10s\n",
		"Policy", "Parts", "Write Amp", "Read Amp", "Merges")
	fmt.Println(strings.Repeat("-", 70))
	for _, r := range results {
		fmt.Printf("%-25s %8d %10.2fx %10d %10d\n",
			r.Name, r.FinalParts, r.WriteAmp, r.ReadAmp, r.TotalMerges)
	}
	fmt.Println()

	ShowPolicyAnalysis(results)
}

func ShowPolicyAnalysis(results []PolicyResult) {
	fmt.Println("=== Policy Analysis ===")
	fmt.Println()

	for _, r := range results {
		fmt.Printf("%s:\n", r.Name)
		fmt.Printf("  Write amplification: %.2fx\n", r.WriteAmp)
		fmt.Printf("  Read amplification:  %d parts to check\n", r.ReadAmp)
		fmt.Printf("  Merge count:         %d\n", r.TotalMerges)

		if r.Name == "Eager (2 parts, 2x)" {
			fmt.Println("  Analysis: Excellent read performance, but high write cost.")
			fmt.Println("            Data rewritten many times through small merges.")
		} else if r.Name == "Default (15 parts, 7.5x)" {
			fmt.Println("  Analysis: Balanced trade-off.")
			fmt.Println("            Reasonable write amp, good read performance.")
		} else if r.Name == "Lazy (15 parts, 15x)" {
			fmt.Println("  Analysis: Low write cost, but more parts to scan.")
			fmt.Println("            Better for write-heavy workloads.")
		} else if r.Name == "Never merge" {
			fmt.Println("  Analysis: Minimal write cost (1x), terrible reads.")
			fmt.Println("            Only suitable for write-once-read-never.")
		}
		fmt.Println()
	}
}

func ShowTradeOffMatrix() {
	fmt.Println("=== Trade-off Matrix ===")
	fmt.Println()

	fmt.Printf("%-15s %-15s %-15s %-15s\n",
		"Policy", "Write Amp", "Read Amp", "Space Amp")
	fmt.Println(strings.Repeat("-", 60))

	policies := []struct {
		name     string
		writeAmp string
		readAmp  string
		spaceAmp string
	}{
		{"Eager merge", "High (10-20x)", "Low (1-5)", "Transient 2x"},
		{"Default (7.5x)", "Medium (5-8x)", "Medium (10-20)", "Transient 2x"},
		{"Lazy merge", "Low (2-4x)", "High (20-50)", "Transient 2x"},
		{"Never merge", "Minimal (1x)", "Very High (100+)", "None"},
	}

	for _, p := range policies {
		fmt.Printf("%-15s %-15s %-15s %-15s\n",
			p.name, p.writeAmp, p.readAmp, p.spaceAmp)
	}
	fmt.Println()

	fmt.Println("Key insight: There is no free lunch.")
	fmt.Println("  - Reducing write amp increases read amp")
	fmt.Println("  - Reducing read amp increases write amp")
	fmt.Println("  - Space amp is transient during merges (old + new)")
	fmt.Println("  - VictoriaLogs default (7.5x) is a balanced choice")
	fmt.Println()
}
