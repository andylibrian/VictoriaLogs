package main

import (
	"fmt"
	"os"
	"sort"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║    Level 5A: LSM Mental Model, Invariants, and Simulator        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "simulator":
			RunSimulatorDemo()
		case "policy":
			RunPolicySweepDemo()
		case "invariants":
			RunInvariantsDemo()
		case "amplification":
			RunAmplificationDemo()
		case "trace":
			RunMergeTraceDemo()
		default:
			fmt.Printf("Unknown command: %s\n", os.Args[1])
			printUsage()
			os.Exit(1)
		}
		return
	}

	runAllDemos()
}

func printUsage() {
	fmt.Println("Usage: lab [command]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  simulator     - Run LSM event-driven simulator")
	fmt.Println("  policy        - Run policy sweep comparison")
	fmt.Println("  invariants    - Demonstrate LSM invariants")
	fmt.Println("  amplification - Show write/read/space amplification")
	fmt.Println("  trace         - Trace merge selection algorithm")
	fmt.Println()
	fmt.Println("Run without arguments to execute all demos.")
}

func runAllDemos() {
	fmt.Println("Running all Level 5A labs...")
	fmt.Println()

	RunSimulatorDemo()
	fmt.Println()
	fmt.Println("Press Enter to continue to next lab...")
	fmt.Scanln()

	RunAmplificationDemo()
	fmt.Println()
	fmt.Println("Press Enter to continue to next lab...")
	fmt.Scanln()

	RunMergeTraceDemo()
	fmt.Println()
	fmt.Println("Press Enter to continue to next lab...")
	fmt.Scanln()

	RunPolicySweepDemo()
	fmt.Println()
	fmt.Println("Press Enter to continue to next lab...")
	fmt.Scanln()

	RunInvariantsDemo()

	fmt.Println()
	fmt.Println("══════════════════════════════════════════════════════════════════")
	fmt.Println("              Level 5A Labs Complete!")
	fmt.Println("══════════════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("Key takeaways:")
	fmt.Println("  1. LSM trades write cost for read efficiency via background merges")
	fmt.Println("  2. Immutable parts enable safe concurrent reads during writes")
	fmt.Println("  3. Merge ratio threshold (7.5x) prevents wasteful rewrites")
	fmt.Println("  4. Three-tier architecture isolates fast and slow merges")
	fmt.Println("  5. Policy knobs control write/read/space amplification trade-offs")
	fmt.Println()
	fmt.Println("Run 'lab <command>' for specific demos.")
}

type Part struct {
	ID         int
	Size       uint64
	IsInMemory bool
}

type LSMState struct {
	MemtableSize  uint64
	InMemoryParts []Part
	SmallParts    []Part
	BigParts      []Part
	BytesWritten  uint64
	TotalFlushes  int
	TotalMerges   int
}

type LSMSimulator struct {
	State           LSMState
	FlushThreshold  uint64
	SmallThreshold  uint64
	BigThreshold    uint64
	PartsToMerge    int
	MergeMultiplier float64
	nextPartID      int
}

func NewLSMSimulator(flushThreshold, smallThreshold, bigThreshold uint64, partsToMerge int, mergeMultiplier float64) *LSMSimulator {
	return &LSMSimulator{
		FlushThreshold:  flushThreshold,
		SmallThreshold:  smallThreshold,
		BigThreshold:    bigThreshold,
		PartsToMerge:    partsToMerge,
		MergeMultiplier: mergeMultiplier,
		nextPartID:      1,
	}
}

func (s *LSMSimulator) Put(size uint64) {
	s.State.MemtableSize += size
}

func (s *LSMSimulator) Flush() {
	if s.State.MemtableSize == 0 {
		return
	}
	part := Part{
		ID:         s.nextPartID,
		Size:       s.State.MemtableSize,
		IsInMemory: true,
	}
	s.nextPartID++
	s.State.InMemoryParts = append(s.State.InMemoryParts, part)
	s.State.BytesWritten += s.State.MemtableSize
	s.State.TotalFlushes++
	s.State.MemtableSize = 0
}

func (s *LSMSimulator) Compact() bool {
	merged := s.tryMergeTier(&s.State.InMemoryParts, true)
	if merged {
		return true
	}
	merged = s.tryMergeTier(&s.State.SmallParts, false)
	return merged
}

func (s *LSMSimulator) tryMergeTier(parts *[]Part, isInMemory bool) bool {
	if len(*parts) < 2 {
		return false
	}

	sort.Slice(*parts, func(i, j int) bool {
		return (*parts)[i].Size < (*parts)[j].Size
	})

	minSrcParts := (s.PartsToMerge + 1) / 2
	if minSrcParts < 2 {
		minSrcParts = 2
	}

	maxSrcParts := s.PartsToMerge
	if maxSrcParts > len(*parts) {
		maxSrcParts = len(*parts)
	}

	var bestStart, bestCount int
	bestRatio := float64(0)

	for count := minSrcParts; count <= maxSrcParts; count++ {
		for start := 0; start <= len(*parts)-count; start++ {
			window := (*parts)[start : start+count]
			totalSize := uint64(0)
			for _, p := range window {
				totalSize += p.Size
			}
			largestSize := window[len(window)-1].Size
			ratio := float64(totalSize) / float64(largestSize)

			if ratio >= s.MergeMultiplier && ratio > bestRatio {
				bestRatio = ratio
				bestStart = start
				bestCount = count
			}
		}
	}

	if bestCount == 0 {
		return false
	}

	toMerge := (*parts)[bestStart : bestStart+bestCount]
	var totalSize uint64
	for _, p := range toMerge {
		totalSize += p.Size
	}

	newPart := Part{
		ID:         s.nextPartID,
		Size:       totalSize,
		IsInMemory: false,
	}
	s.nextPartID++

	s.State.BytesWritten += totalSize
	s.State.TotalMerges++

	var newParts []Part
	for i, p := range *parts {
		if i < bestStart || i >= bestStart+bestCount {
			newParts = append(newParts, p)
		}
	}

	if isInMemory {
		if totalSize <= s.SmallThreshold {
			newPart.IsInMemory = true
			newParts = append(newParts, newPart)
			*parts = newParts
		} else {
			newParts = append(newParts, newPart)
			*parts = newParts
			s.moveToSmallOrBig(newPart.ID)
		}
	} else {
		if totalSize <= s.BigThreshold {
			newParts = append(newParts, newPart)
			*parts = newParts
		} else {
			s.State.BigParts = append(s.State.BigParts, newPart)
			*parts = newParts
		}
	}

	return true
}

func (s *LSMSimulator) moveToSmallOrBig(partID int) {
	for i, p := range s.State.InMemoryParts {
		if p.ID == partID {
			if p.Size <= s.BigThreshold {
				s.State.SmallParts = append(s.State.SmallParts, p)
			} else {
				s.State.BigParts = append(s.State.BigParts, p)
			}
			s.State.InMemoryParts = append(s.State.InMemoryParts[:i], s.State.InMemoryParts[i+1:]...)
			return
		}
	}
}

func (s *LSMSimulator) GetTotalDataSize() uint64 {
	var total uint64
	for _, p := range s.State.InMemoryParts {
		total += p.Size
	}
	for _, p := range s.State.SmallParts {
		total += p.Size
	}
	for _, p := range s.State.BigParts {
		total += p.Size
	}
	total += s.State.MemtableSize
	return total
}

func (s *LSMSimulator) GetPartCount() int {
	return len(s.State.InMemoryParts) + len(s.State.SmallParts) + len(s.State.BigParts)
}

func (s *LSMSimulator) GetWriteAmp() float64 {
	totalData := s.GetTotalDataSize()
	if totalData == 0 {
		return 1.0
	}
	return float64(s.State.BytesWritten) / float64(totalData)
}

func (s *LSMSimulator) GetReadAmp() int {
	return s.GetPartCount()
}
