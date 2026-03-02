package main

import (
	"container/heap"
	"fmt"
	"strings"
)

// ==================== Block Stream Types ====================

type StreamID struct {
	TenantID uint64
	ID       uint64
}

func (s *StreamID) Less(other *StreamID) bool {
	if s.TenantID != other.TenantID {
		return s.TenantID < other.TenantID
	}
	return s.ID < other.ID
}

func (s *StreamID) Equal(other *StreamID) bool {
	return s.TenantID == other.TenantID && s.ID == other.ID
}

func (s *StreamID) String() string {
	return fmt.Sprintf("S%d", s.ID)
}

type Block struct {
	StreamID       StreamID
	MinTimestamp   int64
	MaxTimestamp   int64
	RowsCount      int
	CompressedSize int
	SourcePart     string
}

func (b *Block) String() string {
	return fmt.Sprintf("%s:t=%d-%d", b.StreamID.String(), b.MinTimestamp, b.MaxTimestamp)
}

type BlockStreamReader struct {
	PartName string
	Blocks   []*Block
	Current  int
}

func (r *BlockStreamReader) CurrentBlock() *Block {
	if r.Current >= len(r.Blocks) {
		return nil
	}
	return r.Blocks[r.Current]
}

func (r *BlockStreamReader) Advance() bool {
	r.Current++
	return r.Current < len(r.Blocks)
}

// ==================== Heap Implementation ====================

type BlockReaderHeap []*BlockStreamReader

func (h BlockReaderHeap) Len() int { return len(h) }

func (h BlockReaderHeap) Less(i, j int) bool {
	a := h[i].CurrentBlock()
	b := h[j].CurrentBlock()

	if a == nil {
		return false
	}
	if b == nil {
		return true
	}

	// Primary: streamID
	if !a.StreamID.Equal(&b.StreamID) {
		return a.StreamID.Less(&b.StreamID)
	}

	// Secondary: minTimestamp
	return a.MinTimestamp < b.MinTimestamp
}

func (h BlockReaderHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *BlockReaderHeap) Push(x interface{}) {
	*h = append(*h, x.(*BlockStreamReader))
}

func (h *BlockReaderHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// ==================== Heap Merge Trace ====================

type MergeEvent struct {
	Iteration int
	Action    string
	HeapState []string
	Output    string
}

func HeapMergeTrace(readers []*BlockStreamReader) []MergeEvent {
	events := []MergeEvent{}

	// Initialize heap
	h := make(BlockReaderHeap, 0, len(readers))
	for _, r := range readers {
		if r.CurrentBlock() != nil {
			h = append(h, r)
		}
	}
	heap.Init(&h)

	iteration := 0

	for len(h) > 0 {
		iteration++

		// Capture heap state
		heapState := make([]string, len(h))
		for i, r := range h {
			b := r.CurrentBlock()
			if b != nil {
				heapState[i] = fmt.Sprintf("%s(%s)", r.PartName, b.String())
			}
		}

		// Pop minimum
		minReader := heap.Pop(&h).(*BlockStreamReader)
		minBlock := minReader.CurrentBlock()

		// Record output
		output := fmt.Sprintf("%s → write %s", minReader.PartName, minBlock.String())

		events = append(events, MergeEvent{
			Iteration: iteration,
			Action:    fmt.Sprintf("Pop %s, write block", minReader.PartName),
			HeapState: heapState,
			Output:    output,
		})

		// Advance reader
		if minReader.Advance() {
			heap.Push(&h, minReader)
			events = append(events, MergeEvent{
				Iteration: iteration,
				Action:    fmt.Sprintf("Advance %s to next block", minReader.PartName),
				HeapState: getHeapState(&h),
				Output:    "",
			})
		}
	}

	return events
}

func getHeapState(h *BlockReaderHeap) []string {
	state := make([]string, len(*h))
	for i, r := range *h {
		b := r.CurrentBlock()
		if b != nil {
			state[i] = fmt.Sprintf("%s(%s)", r.PartName, b.String())
		}
	}
	return state
}

// ==================== Demo ====================

func HeapMergeDemo() {
	fmt.Println("=== Level 7 Lab 2: Heap Merge Walkthrough ===")
	fmt.Println()
	fmt.Println("This program traces heap state transitions during k-way merge.")
	fmt.Println()

	// Create three source parts with sorted blocks
	partA := &BlockStreamReader{
		PartName: "A",
		Blocks: []*Block{
			{StreamID: StreamID{ID: 1}, MinTimestamp: 1, MaxTimestamp: 5, RowsCount: 100},
			{StreamID: StreamID{ID: 1}, MinTimestamp: 5, MaxTimestamp: 10, RowsCount: 100},
			{StreamID: StreamID{ID: 2}, MinTimestamp: 3, MaxTimestamp: 8, RowsCount: 50},
		},
	}

	partB := &BlockStreamReader{
		PartName: "B",
		Blocks: []*Block{
			{StreamID: StreamID{ID: 1}, MinTimestamp: 2, MaxTimestamp: 4, RowsCount: 80},
			{StreamID: StreamID{ID: 1}, MinTimestamp: 4, MaxTimestamp: 7, RowsCount: 90},
			{StreamID: StreamID{ID: 3}, MinTimestamp: 1, MaxTimestamp: 3, RowsCount: 60},
		},
	}

	partC := &BlockStreamReader{
		PartName: "C",
		Blocks: []*Block{
			{StreamID: StreamID{ID: 1}, MinTimestamp: 3, MaxTimestamp: 6, RowsCount: 70},
			{StreamID: StreamID{ID: 2}, MinTimestamp: 1, MaxTimestamp: 5, RowsCount: 40},
		},
	}

	// Show input parts
	fmt.Println("=== Input Parts ===")
	fmt.Println()
	PrintPartBlocks(partA)
	PrintPartBlocks(partB)
	PrintPartBlocks(partC)
	fmt.Println()

	// Run merge trace
	fmt.Println("=== Heap Merge Trace ===")
	fmt.Println()

	readers := []*BlockStreamReader{partA, partB, partC}
	events := HeapMergeTrace(readers)

	PrintMergeEvents(events)

	fmt.Println()

	// Show final output order
	fmt.Println("=== Output Block Order ===")
	fmt.Println()
	PrintOutputOrder(events)

	fmt.Println()

	// Explain heap comparator
	ExplainHeapComparator()
}

func PrintPartBlocks(r *BlockStreamReader) {
	fmt.Printf("Part %s blocks (sorted by streamID, timestamp):\n", r.PartName)
	for i, b := range r.Blocks {
		fmt.Printf("  [%d] %s, rows=%d\n", i, b.String(), b.RowsCount)
	}
	fmt.Println()
}

func PrintMergeEvents(events []MergeEvent) {
	fmt.Printf("%-10s %-30s %s\n", "Iteration", "Action", "Heap State")
	fmt.Println(strings.Repeat("-", 80))

	for _, e := range events {
		heapStr := strings.Join(e.HeapState, ", ")
		if len(heapStr) == 0 {
			heapStr = "(empty)"
		}

		fmt.Printf("%-10d %-30s [%s]\n", e.Iteration, e.Action, heapStr)

		if e.Output != "" {
			fmt.Printf("            → %s\n", e.Output)
		}
	}
}

func PrintOutputOrder(events []MergeEvent) {
	fmt.Println("Blocks written to output (in order):")

	order := 1
	for _, e := range events {
		if e.Output != "" {
			fmt.Printf("  %d. %s\n", order, e.Output)
			order++
		}
	}
}

func ExplainHeapComparator() {
	fmt.Println("=== Heap Comparator Explanation ===")
	fmt.Println()
	fmt.Println("The heap comparator sorts by (streamID, minTimestamp):")
	fmt.Println()
	fmt.Println("1. PRIMARY: streamID")
	fmt.Println("   - All blocks for the same stream are emitted contiguously")
	fmt.Println("   - This maintains the (streamID, timestamp) sort order invariant")
	fmt.Println()
	fmt.Println("2. SECONDARY: minTimestamp")
	fmt.Println("   - Within the same stream, blocks are emitted in time order")
	fmt.Println("   - Ensures chronological ordering within stream")
	fmt.Println()
	fmt.Println("Why NOT timestamp-first?")
	fmt.Println()
	fmt.Println("  If comparator were timestamp-first:")
	fmt.Println("    - S1:t=1, S2:t=2, S1:t=3, S2:t=4...")
	fmt.Println("    - Blocks from different streams would INTERLEAVE")
	fmt.Println("    - Output would NOT be sorted by streamID")
	fmt.Println("    - Index structure would be CORRUPTED")
	fmt.Println()
	fmt.Println("  With streamID-first:")
	fmt.Println("    - S1:t=1, S1:t=3, S1:t=5, S2:t=2, S2:t=4...")
	fmt.Println("    - All S1 blocks contiguous")
	fmt.Println("    - All S2 blocks contiguous")
	fmt.Println("    - Correct (streamID, timestamp) ordering maintained")
}

// ==================== Complexity Analysis ====================

func ComplexityAnalysis() {
	fmt.Println("=== Heap Merge Complexity Analysis ===")
	fmt.Println()

	fmt.Println("K-way heap merge vs pairwise repeated merge:")
	fmt.Println()

	// Example: 15 parts of 10 MB each
	k := 15
	partSize := 10 * 1024 * 1024
	totalSize := k * partSize

	fmt.Printf("Example: %d parts × %.1f MB = %.1f MB total\n", k,
		float64(partSize)/(1024*1024), float64(totalSize)/(1024*1024))
	fmt.Println()

	// Pairwise merge
	rounds := 0
	tmp := k
	for tmp > 1 {
		tmp = (tmp + 1) / 2
		rounds++
	}
	pairwiseBytes := totalSize * rounds

	fmt.Println("Pairwise repeated merge:")
	fmt.Printf("  Rounds needed: %d (⌈log₂(%d)⌉)\n", rounds, k)
	fmt.Printf("  Bytes written: %d rounds × %.1f MB = %.1f MB\n",
		rounds, float64(totalSize)/(1024*1024), float64(pairwiseBytes)/(1024*1024))
	fmt.Printf("  Write amplification: %.1fx\n", float64(pairwiseBytes)/float64(totalSize))
	fmt.Println()

	// K-way merge
	// Assume ~1000 blocks per part
	blocksPerPart := 1000
	totalBlocks := k * blocksPerPart
	heapOpsPerBlock := 2 // One pop, potentially one push
	_ = heapOpsPerBlock  // Used for documentation

	fmt.Println("K-way heap merge:")
	fmt.Printf("  Rounds: 1 (single pass)\n")
	fmt.Printf("  Bytes written: %.1f MB\n", float64(totalSize)/(1024*1024))
	fmt.Printf("  Heap operations: %d blocks × %d ops = %d\n",
		totalBlocks, heapOpsPerBlock, totalBlocks*heapOpsPerBlock)
	fmt.Printf("  Write amplification: 1.0x\n")
	fmt.Println()

	improvement := float64(pairwiseBytes) / float64(totalSize)
	fmt.Printf("Improvement: %.1fx less I/O with k-way merge\n", improvement)
	fmt.Println()

	// Memory comparison
	fmt.Println("Memory comparison:")
	fmt.Printf("  Pairwise: 2 readers × ~128 KB = ~256 KB\n")
	fmt.Printf("  K-way: %d readers × ~128 KB = ~%d KB\n", k, k*128)
	fmt.Println("  K-way uses more memory, but still trivial")
}
