package main

import (
	"fmt"
	"math"
	"strings"
)

func RunAmplificationDemo() {
	fmt.Println("=== Amplification Analysis ===")
	fmt.Println()
	fmt.Println("LSM trees trade between three types of amplification:")
	fmt.Println()

	ShowWriteAmplification()
	ShowReadAmplification()
	ShowSpaceAmplification()
	ShowAmplificationFormula()
}

func ShowWriteAmplification() {
	fmt.Println("=== Write Amplification ===")
	fmt.Println()

	fmt.Println("Definition: Total bytes written / Logical data size")
	fmt.Println()

	fmt.Println("Example: Ingesting 100 MB with binary merge (2 parts at a time)")
	fmt.Println()

	sizes := []uint64{1, 2, 4, 8, 16, 32, 64, 100}
	var totalWritten uint64

	fmt.Printf("%-10s %-15s %-15s\n", "Step", "Part Size (MB)", "Cumulative Writes (MB)")
	fmt.Println(strings.Repeat("-", 45))

	for i, size := range sizes {
		totalWritten += size
		fmt.Printf("%-10d %-15d %-15d\n", i+1, size, totalWritten)
	}
	fmt.Println()

	writeAmp := float64(totalWritten) / 100.0
	fmt.Printf("Write amplification: %.2fx\n", writeAmp)
	fmt.Println()

	fmt.Println("Why binary merge is expensive:")
	fmt.Println("  1 MB → 2 MB → 4 MB → 8 MB → 16 MB → 32 MB → 64 MB → 100 MB")
	fmt.Println("  Original 1 MB was rewritten 7 times!")
	fmt.Println()

	fmt.Println("VictoriaLogs approach (7.5x ratio, up to 15 parts):")
	fmt.Println("  With 7.5x minimum ratio, each merge creates much larger output")
	fmt.Println("  Fewer merge steps → less rewriting → lower write amp")
	fmt.Println()

	fmt.Println("Formula for binary merge:")
	fmt.Println("  WriteAmp = log₂(N) where N = final_size / initial_size")
	fmt.Println("  For 100 MB from 1 MB: log₂(100) ≈ 6.6x")
	fmt.Println()

	fmt.Println("Formula for k-way merge:")
	fmt.Println("  WriteAmp = log_k(N) where k = merge fan-in")
	fmt.Println("  For 15-way merge: log₁₅(100) ≈ 1.6x (theoretical minimum)")
	fmt.Println()

	ShowWriteAmpComparison()
}

func ShowWriteAmpComparison() {
	fmt.Println("Write amplification comparison (100 MB from 1 MB parts):")
	fmt.Println()

	strategies := []struct {
		name     string
		fanIn    int
		ratio    float64
		writeAmp float64
	}{
		{"Binary (2-way, 2x ratio)", 2, 2.0, 6.6},
		{"4-way merge", 4, 4.0, 3.3},
		{"8-way merge", 8, 8.0, 2.2},
		{"15-way (VictoriaLogs default)", 15, 7.5, 1.6},
		{"32-way merge", 32, 32.0, 1.3},
	}

	fmt.Printf("%-35s %10s %10s %10s\n", "Strategy", "Fan-in", "Ratio", "Write Amp")
	fmt.Println(strings.Repeat("-", 70))
	for _, s := range strategies {
		fmt.Printf("%-35s %10d %10.1fx %10.1fx\n", s.name, s.fanIn, s.ratio, s.writeAmp)
	}
	fmt.Println()

	fmt.Println("Trade-off: Higher fan-in reduces write amp but:")
	fmt.Println("  - Uses more memory during merge")
	fmt.Println("  - Increases merge latency")
	fmt.Println("  - Higher temporary space usage")
	fmt.Println()
}

func ShowReadAmplification() {
	fmt.Println("=== Read Amplification ===")
	fmt.Println()

	fmt.Println("Definition: Number of parts a point query must check")
	fmt.Println()

	fmt.Println("Scenario: Query for a specific log entry")
	fmt.Println()

	fmt.Println("Without bloom filters:")
	fmt.Println("  Must check every part (linear scan)")
	fmt.Println("  ReadAmp = number of parts")
	fmt.Println()

	fmt.Println("With bloom filters (VictoriaLogs approach):")
	fmt.Println("  Check bloom filter first (in-memory)")
	fmt.Println("  Only read parts where bloom filter matches")
	fmt.Println("  ReadAmp ≈ matching parts / total parts (for selective queries)")
	fmt.Println()

	fmt.Println("Example: 1000 parts, query matches 1 part")
	fmt.Println("  Without bloom: Read 1000 part indexes")
	fmt.Println("  With bloom:    Read 1 part (bloom filters eliminate 999)")
	fmt.Println()

	ShowReadAmpVsPartCount()
}

func ShowReadAmpVsPartCount() {
	fmt.Println("Read amplification vs part count:")
	fmt.Println()

	partCounts := []int{10, 50, 100, 500, 1000}
	selectivity := 0.01

	fmt.Printf("%-15s %-15s %-15s %-15s\n",
		"Part Count", "No Bloom", "With Bloom (1%)", "Improvement")
	fmt.Println(strings.Repeat("-", 60))

	for _, n := range partCounts {
		noBloom := n
		withBloom := int(math.Ceil(float64(n) * selectivity))
		improvement := float64(noBloom) / float64(max(withBloom, 1))
		fmt.Printf("%-15d %-15d %-15d %-15.0fx\n",
			n, noBloom, withBloom, improvement)
	}
	fmt.Println()

	fmt.Println("This is why merging reduces read cost:")
	fmt.Println("  Fewer parts → fewer bloom checks → fewer seeks")
	fmt.Println()
}

func ShowSpaceAmplification() {
	fmt.Println("=== Space Amplification ===")
	fmt.Println()

	fmt.Println("Definition: (Disk used) / (Logical data size)")
	fmt.Println()

	fmt.Println("Sources of space amplification:")
	fmt.Println()

	fmt.Println("1. During merge (transient):")
	fmt.Println("   - Old parts + new part exist simultaneously")
	fmt.Println("   - Peak: 2x for that merge set")
	fmt.Println("   - Example: Merging 5 x 100 MB parts")
	fmt.Println("     - Old parts: 500 MB")
	fmt.Println("     - New part: 500 MB (being written)")
	fmt.Println("     - Peak usage: 1000 MB")
	fmt.Println()

	fmt.Println("2. Due to merge policy (steady-state):")
	fmt.Println("   - Multiple small parts not yet merged")
	fmt.Println("   - Typically < 1.5x with good policy")
	fmt.Println()

	fmt.Println("3. Due to compression differences:")
	fmt.Println("   - Smaller parts compress less efficiently")
	fmt.Println("   - Merged parts have better compression")
	fmt.Println("   - Space amp can be < 1.0 after merge!")
	fmt.Println()

	fmt.Println("Space usage timeline during merge:")
	fmt.Println()

	fmt.Println("  Time:  T0 -------- T1 -------- T2 -------- T3")
	fmt.Println("  Old:   500 MB      500 MB      250 MB        0 MB")
	fmt.Println("  New:     0 MB      250 MB      500 MB      500 MB")
	fmt.Println("  Total: 500 MB      750 MB      750 MB      500 MB")
	fmt.Println()

	fmt.Println("Peak space during merge: 750 MB (1.5x)")
	fmt.Println()

	fmt.Println("VictoriaLogs mitigation:")
	fmt.Println("  - Merge one tier at a time")
	fmt.Println("  - Bound concurrent merges per tier")
	fmt.Println("  - Disk pressure monitoring")
	fmt.Println()
}

func ShowAmplificationFormula() {
	fmt.Println("=== Amplification Trade-off Formula ===")
	fmt.Println()

	fmt.Println("The fundamental LSM trade-off:")
	fmt.Println()

	fmt.Println("  WriteAmp × ReadAmp ≈ constant (for given data size)")
	fmt.Println()

	fmt.Println("Lower write amp (fewer merges) → Higher read amp (more parts)")
	fmt.Println("Higher write amp (more merges) → Lower read amp (fewer parts)")
	fmt.Println()

	fmt.Println("VictoriaLogs policy choice:")
	fmt.Println("  - 7.5x merge ratio")
	fmt.Println("  - Up to 15 parts per merge")
	fmt.Println("  - Expected write amp: 5-8x")
	fmt.Println("  - Expected read amp: 10-20 parts")
	fmt.Println()

	fmt.Println("When to tune:")
	fmt.Println()

	fmt.Println("  High write, low read workload:")
	fmt.Println("    → Increase merge ratio (e.g., 15x)")
	fmt.Println("    → Lower write amp, accept higher read amp")
	fmt.Println()

	fmt.Println("  High read, moderate write workload:")
	fmt.Println("    → Decrease merge ratio (e.g., 4x)")
	fmt.Println("    → Higher write amp, lower read amp")
	fmt.Println()

	fmt.Println("  Disk space constrained:")
	fmt.Println("    → Limit concurrent merges")
	fmt.Println("    → Accept higher read amp")
	fmt.Println()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
