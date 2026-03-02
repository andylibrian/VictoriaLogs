package main

import (
	"fmt"
	"sort"
)

// DayPartition represents a single day partition in the storage.
// In VictoriaLogs, partitions are organized by day to enable efficient
// time-based pruning before reading any actual log data.
type DayPartition struct {
	Day   int64  // Day number (timestamp / nanosecondsPerDay)
	Label string // Human-readable label for demonstration
}

// PartitionIndex maintains a sorted collection of day partitions.
// CRITICAL INVARIANT: partitions MUST remain sorted by Day for binary search to work.
// This is the same invariant VictoriaLogs maintains in Storage.partitions.
type PartitionIndex struct {
	partitions []DayPartition
}

// NewPartitionIndex creates an empty index.
func NewPartitionIndex() *PartitionIndex {
	return &PartitionIndex{
		partitions: make([]DayPartition, 0),
	}
}

// AddPartition inserts a new partition while maintaining sorted order.
// This mirrors getPartitionForWriting's insertion logic in storage.go:1448-1455
func (idx *PartitionIndex) AddPartition(day int64, label string) {
	// Find the insertion point using binary search
	n := sort.Search(len(idx.partitions), func(i int) bool {
		return idx.partitions[i].Day >= day
	})

	// Check if partition already exists
	if n < len(idx.partitions) && idx.partitions[n].Day == day {
		return // Already exists
	}

	// Insert at position n to maintain sorted order
	newPart := DayPartition{Day: day, Label: label}
	if n == len(idx.partitions) {
		idx.partitions = append(idx.partitions, newPart)
	} else {
		idx.partitions = append(idx.partitions[:n+1], idx.partitions[n:]...)
		idx.partitions[n] = newPart
	}
}

// FindOverlappingPartitions returns all partitions that overlap with [minDay, maxDay].
// This is the EXACT algorithm used in getPartitionsForTimeRange (storage_search.go:1498-1530).
//
// WHY TWO BINARY SEARCHES?
// - First search finds the LEFT boundary: first partition where day >= minDay
// - Second search finds the RIGHT boundary: first partition where day > maxDay
// - The slice [left:right] contains exactly the overlapping partitions
//
// COMPLEXITY: O(log n) for each search = O(log n) total
// Compare to linear scan: O(n)
func (idx *PartitionIndex) FindOverlappingPartitions(minDay, maxDay int64) []DayPartition {
	if len(idx.partitions) == 0 {
		return nil
	}

	ptws := idx.partitions

	// STEP 1: Find left boundary (first partition with day >= minDay)
	// This eliminates all partitions that end BEFORE our query range starts
	leftIdx := sort.Search(len(ptws), func(i int) bool {
		return ptws[i].Day >= minDay
	})
	ptws = ptws[leftIdx:] // Slice off everything before left boundary

	// STEP 2: Find right boundary (first partition with day > maxDay)
	// This eliminates all partitions that start AFTER our query range ends
	rightIdx := sort.Search(len(ptws), func(i int) bool {
		return ptws[i].Day > maxDay
	})
	ptws = ptws[:rightIdx] // Keep only partitions up to (but not including) right boundary

	// Return a copy to avoid mutation issues
	result := make([]DayPartition, len(ptws))
	copy(result, ptws)
	return result
}

// FindPartitionForWriting finds the partition for a given day, creating it if needed.
// This mirrors getPartitionForWriting in storage.go:1409-1463
func (idx *PartitionIndex) FindPartitionForWriting(day int64) *DayPartition {
	// Binary search for the partition
	n := sort.Search(len(idx.partitions), func(i int) bool {
		return idx.partitions[i].Day >= day
	})

	// Check if we found an exact match
	if n < len(idx.partitions) && idx.partitions[n].Day == day {
		return &idx.partitions[n]
	}

	// Partition doesn't exist - in real VictoriaLogs, this would create a new one
	// For this lab, we return nil to indicate not found
	return nil
}

// Count returns the number of partitions
func (idx *PartitionIndex) Count() int {
	return len(idx.partitions)
}

// RangeLookupDemo demonstrates binary search for partition lookup
func RangeLookupDemo() {
	fmt.Println("=== Lab 1: Range Lookup with Binary Search ===")
	fmt.Println()
	fmt.Println("This program demonstrates how VictoriaLogs uses binary search")
	fmt.Println("to efficiently find partitions overlapping a time range.")
	fmt.Println()

	idx := NewPartitionIndex()

	// Simulate a year of daily partitions
	fmt.Println("Creating 365 daily partitions (Jan 1 - Dec 31)...")
	for day := int64(0); day < 365; day++ {
		month := day / 30
		dayOfMonth := day % 30
		label := fmt.Sprintf("Month%d_Day%d", month+1, dayOfMonth+1)
		idx.AddPartition(day, label)
	}
	fmt.Printf("Total partitions: %d\n\n", idx.Count())

	// Test Case 1: Full overlap (query covers all data)
	fmt.Println("--- Test Case 1: Full Overlap ---")
	fmt.Println("Query range: [0, 364] (entire year)")
	results := idx.FindOverlappingPartitions(0, 364)
	fmt.Printf("Found %d partitions (expected 365)\n\n", len(results))

	// Test Case 2: No overlap (query outside data range)
	fmt.Println("--- Test Case 2: No Overlap ---")
	fmt.Println("Query range: [400, 500] (future dates, no data)")
	results = idx.FindOverlappingPartitions(400, 500)
	fmt.Printf("Found %d partitions (expected 0)\n\n", len(results))

	// Test Case 3: Exact boundary match
	fmt.Println("--- Test Case 3: Exact Boundary Match ---")
	fmt.Println("Query range: [60, 90] (exact day boundaries)")
	results = idx.FindOverlappingPartitions(60, 90)
	fmt.Printf("Found %d partitions (expected 31)\n", len(results))
	if len(results) > 0 {
		fmt.Printf("First: Day %d (%s)\n", results[0].Day, results[0].Label)
		fmt.Printf("Last:  Day %d (%s)\n", results[len(results)-1].Day, results[len(results)-1].Label)
	}
	fmt.Println()

	// Test Case 4: Partial overlap at boundaries
	fmt.Println("--- Test Case 4: Partial Overlap (Query Extends Beyond Data) ---")
	fmt.Println("Query range: [350, 400] (extends beyond available data)")
	results = idx.FindOverlappingPartitions(350, 400)
	fmt.Printf("Found %d partitions (expected 15)\n", len(results))
	if len(results) > 0 {
		fmt.Printf("First: Day %d (%s)\n", results[0].Day, results[0].Label)
		fmt.Printf("Last:  Day %d (%s)\n", results[len(results)-1].Day, results[len(results)-1].Label)
	}
	fmt.Println()

	// Test Case 5: Single day query
	fmt.Println("--- Test Case 5: Single Day Query ---")
	fmt.Println("Query range: [100, 100] (single day)")
	results = idx.FindOverlappingPartitions(100, 100)
	fmt.Printf("Found %d partitions (expected 1)\n", len(results))
	if len(results) > 0 {
		fmt.Printf("Partition: Day %d (%s)\n", results[0].Day, results[0].Label)
	}
	fmt.Println()

	// Demonstrate write path
	fmt.Println("--- Write Path Demo ---")
	fmt.Println("Finding partition for day 42 for writing...")
	pt := idx.FindPartitionForWriting(42)
	if pt != nil {
		fmt.Printf("Found: Day %d (%s)\n", pt.Day, pt.Label)
	}
	fmt.Println("Finding partition for day 500 (doesn't exist)...")
	pt = idx.FindPartitionForWriting(500)
	if pt == nil {
		fmt.Println("Not found (would create new partition in real system)")
	}
	fmt.Println()

	// Complexity demonstration
	fmt.Println("=== Complexity Analysis ===")
	fmt.Println("For 365 partitions:")
	fmt.Println("  Linear scan:  365 comparisons (worst case)")
	fmt.Println("  Binary search: ~9 comparisons per boundary × 2 = ~18 total")
	fmt.Println("  Speedup: 20x")
	fmt.Println()
	fmt.Println("For 1,000,000 partitions:")
	fmt.Println("  Linear scan:  1,000,000 comparisons")
	fmt.Println("  Binary search: ~20 comparisons per boundary × 2 = ~40 total")
	fmt.Println("  Speedup: 25,000x")
}
