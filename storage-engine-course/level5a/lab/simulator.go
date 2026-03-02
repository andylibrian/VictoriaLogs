package main

import (
	"fmt"
	"strings"
)

func RunSimulatorDemo() {
	fmt.Println("=== Lab 1: Event-Driven LSM Simulator ===")
	fmt.Println()
	fmt.Println("This simulator models the VictoriaLogs LSM behavior:")
	fmt.Println("  - PUT: Add data to memtable")
	fmt.Println("  - FLUSH: Convert memtable to in-memory part")
	fmt.Println("  - COMPACT: Merge parts according to policy")
	fmt.Println()

	ShowSimulatorTrace()
	ShowSimulatorMetrics()
}

func ShowSimulatorTrace() {
	fmt.Println("=== Simulator Trace: Ingesting 100 MB ===")
	fmt.Println()

	sim := NewLSMSimulator(
		1*1024*1024,
		10*1024*1024,
		100*1024*1024,
		15,
		7.5,
	)

	fmt.Println("Policy settings:")
	fmt.Println("  Flush threshold: 1 MB")
	fmt.Println("  Small threshold: 10 MB")
	fmt.Println("  Big threshold:   100 MB")
	fmt.Println("  Parts to merge:  15")
	fmt.Println("  Merge ratio:     7.5x")
	fmt.Println()

	events := []string{}
	chunkSize := uint64(100 * 1024)

	for i := 0; i < 1000; i++ {
		sim.Put(chunkSize)

		if sim.State.MemtableSize >= sim.FlushThreshold {
			sim.Flush()
			events = append(events, fmt.Sprintf("T+%d: FLUSH (total parts: %d)", i+1, sim.GetPartCount()))
		}

		for sim.Compact() {
			events = append(events, fmt.Sprintf("T+%d: COMPACT (parts: %d, merges: %d)",
				i+1, sim.GetPartCount(), sim.State.TotalMerges))
		}
	}

	if sim.State.MemtableSize > 0 {
		sim.Flush()
	}

	for sim.Compact() {
	}

	fmt.Println("Event trace (showing every 50th event):")
	fmt.Printf("%-40s %s\n", "Event", "State After")
	fmt.Println(strings.Repeat("-", 70))

	for i, e := range events {
		if i%50 == 0 || i == len(events)-1 {
			fmt.Printf("%-40s parts=%d, writes=%d MB\n",
				e, sim.GetPartCount(), sim.State.BytesWritten/(1024*1024))
		}
	}
	fmt.Println()

	fmt.Println("Final state:")
	fmt.Printf("  Total data size:   %d MB\n", sim.GetTotalDataSize()/(1024*1024))
	fmt.Printf("  Total bytes written: %d MB\n", sim.State.BytesWritten/(1024*1024))
	fmt.Printf("  Final part count:  %d\n", sim.GetPartCount())
	fmt.Printf("  Write amplification: %.2fx\n", sim.GetWriteAmp())
	fmt.Printf("  Read amplification:  %d parts to scan\n", sim.GetReadAmp())
	fmt.Println()
}

func ShowSimulatorMetrics() {
	fmt.Println("=== Metrics Over Time ===")
	fmt.Println()

	sim := NewLSMSimulator(
		1*1024*1024,
		10*1024*1024,
		100*1024*1024,
		15,
		7.5,
	)

	fmt.Printf("%-10s %12s %12s %12s %12s\n",
		"Ops", "Data (MB)", "Parts", "Write Amp", "Read Amp")
	fmt.Println(strings.Repeat("-", 60))

	chunkSize := uint64(100 * 1024)
	checkpoints := []int{100, 250, 500, 750, 1000}

	for i := 0; i <= 1000; i++ {
		sim.Put(chunkSize)

		if sim.State.MemtableSize >= sim.FlushThreshold {
			sim.Flush()
		}

		for sim.Compact() {
		}

		for _, cp := range checkpoints {
			if i == cp {
				fmt.Printf("%-10d %12d %12d %12.2fx %12d\n",
					i,
					sim.GetTotalDataSize()/(1024*1024),
					sim.GetPartCount(),
					sim.GetWriteAmp(),
					sim.GetReadAmp())
			}
		}
	}
	fmt.Println()
}
