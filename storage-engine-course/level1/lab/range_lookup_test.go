package main

import (
	"fmt"
	"testing"
)

func TestPartitionIndex_AddPartition_MaintainsSortedOrder(t *testing.T) {
	idx := NewPartitionIndex()

	// Add partitions in random order
	idx.AddPartition(5, "day5")
	idx.AddPartition(1, "day1")
	idx.AddPartition(3, "day3")
	idx.AddPartition(2, "day2")
	idx.AddPartition(4, "day4")

	// Verify sorted order
	for i := 1; i < idx.Count(); i++ {
		if idx.partitions[i].Day <= idx.partitions[i-1].Day {
			t.Errorf("Partitions not sorted at index %d: day[%d]=%d, day[%d]=%d",
				i, i-1, idx.partitions[i-1].Day, i, idx.partitions[i].Day)
		}
	}

	// Verify exact order
	expectedDays := []int64{1, 2, 3, 4, 5}
	for i, expected := range expectedDays {
		if idx.partitions[i].Day != expected {
			t.Errorf("Expected day %d at index %d, got %d", expected, i, idx.partitions[i].Day)
		}
	}
}

func TestPartitionIndex_AddPartition_Deduplication(t *testing.T) {
	idx := NewPartitionIndex()

	idx.AddPartition(1, "first")
	idx.AddPartition(1, "duplicate") // Should be ignored

	if idx.Count() != 1 {
		t.Errorf("Expected 1 partition after duplicate add, got %d", idx.Count())
	}

	if idx.partitions[0].Label != "first" {
		t.Errorf("Expected original label preserved, got %s", idx.partitions[0].Label)
	}
}

func TestFindOverlappingPartitions_FullOverlap(t *testing.T) {
	idx := NewPartitionIndex()
	for day := int64(0); day < 10; day++ {
		idx.AddPartition(day, "")
	}

	// Query covers entire range
	results := idx.FindOverlappingPartitions(0, 9)

	if len(results) != 10 {
		t.Errorf("Full overlap: expected 10 partitions, got %d", len(results))
	}
}

func TestFindOverlappingPartitions_NoOverlap(t *testing.T) {
	idx := NewPartitionIndex()
	for day := int64(0); day < 10; day++ {
		idx.AddPartition(day, "")
	}

	// Query entirely before data
	results := idx.FindOverlappingPartitions(-10, -1)
	if len(results) != 0 {
		t.Errorf("No overlap (before): expected 0 partitions, got %d", len(results))
	}

	// Query entirely after data
	results = idx.FindOverlappingPartitions(20, 30)
	if len(results) != 0 {
		t.Errorf("No overlap (after): expected 0 partitions, got %d", len(results))
	}
}

func TestFindOverlappingPartitions_ExactBoundaryMatch(t *testing.T) {
	idx := NewPartitionIndex()
	for day := int64(0); day < 100; day++ {
		idx.AddPartition(day, "")
	}

	// Query with exact boundaries
	results := idx.FindOverlappingPartitions(10, 20)

	if len(results) != 11 {
		t.Errorf("Exact boundary: expected 11 partitions [10-20], got %d", len(results))
	}

	// Verify first and last
	if results[0].Day != 10 {
		t.Errorf("First partition should be day 10, got %d", results[0].Day)
	}
	if results[len(results)-1].Day != 20 {
		t.Errorf("Last partition should be day 20, got %d", results[len(results)-1].Day)
	}
}

func TestFindOverlappingPartitions_SingleDay(t *testing.T) {
	idx := NewPartitionIndex()
	for day := int64(0); day < 10; day++ {
		idx.AddPartition(day, "")
	}

	results := idx.FindOverlappingPartitions(5, 5)

	if len(results) != 1 {
		t.Errorf("Single day: expected 1 partition, got %d", len(results))
	}

	if results[0].Day != 5 {
		t.Errorf("Expected day 5, got %d", results[0].Day)
	}
}

func TestFindOverlappingPartitions_PartialOverlapStart(t *testing.T) {
	idx := NewPartitionIndex()
	for day := int64(10); day < 20; day++ {
		idx.AddPartition(day, "")
	}

	// Query starts before data
	results := idx.FindOverlappingPartitions(5, 15)

	if len(results) != 6 {
		t.Errorf("Partial overlap (start): expected 6 partitions [10-15], got %d", len(results))
	}
}

func TestFindOverlappingPartitions_PartialOverlapEnd(t *testing.T) {
	idx := NewPartitionIndex()
	for day := int64(10); day < 20; day++ {
		idx.AddPartition(day, "")
	}

	// Query ends after data
	results := idx.FindOverlappingPartitions(15, 25)

	if len(results) != 5 {
		t.Errorf("Partial overlap (end): expected 5 partitions [15-19], got %d", len(results))
	}
}

func TestFindOverlappingPartitions_EmptyIndex(t *testing.T) {
	idx := NewPartitionIndex()

	results := idx.FindOverlappingPartitions(0, 10)

	if results != nil {
		t.Errorf("Empty index should return nil, got %d partitions", len(results))
	}
}

func TestFindPartitionForWriting_Existing(t *testing.T) {
	idx := NewPartitionIndex()
	idx.AddPartition(5, "day5")
	idx.AddPartition(10, "day10")

	pt := idx.FindPartitionForWriting(5)
	if pt == nil {
		t.Error("Expected to find existing partition")
		return
	}
	if pt.Day != 5 {
		t.Errorf("Expected day 5, got %d", pt.Day)
	}
}

func TestFindPartitionForWriting_NotExisting(t *testing.T) {
	idx := NewPartitionIndex()
	idx.AddPartition(5, "day5")
	idx.AddPartition(10, "day10")

	pt := idx.FindPartitionForWriting(7)
	if pt != nil {
		t.Errorf("Expected nil for non-existing partition, got day %d", pt.Day)
	}
}

func TestBinarySearchVsLinearScan(t *testing.T) {
	// This test demonstrates the algorithmic difference
	// It doesn't actually measure performance, just verifies correctness

	idx := NewPartitionIndex()
	for day := int64(0); day < 1000; day++ {
		idx.AddPartition(day, "")
	}

	// Our method uses binary search internally
	results := idx.FindOverlappingPartitions(500, 600)

	// Verify we got correct results
	if len(results) != 101 {
		t.Errorf("Expected 101 partitions, got %d", len(results))
	}

	// Verify boundaries
	if results[0].Day != 500 {
		t.Errorf("First should be 500, got %d", results[0].Day)
	}
	if results[len(results)-1].Day != 600 {
		t.Errorf("Last should be 600, got %d", results[len(results)-1].Day)
	}
}

// Benchmark to demonstrate O(log n) vs O(n)
func BenchmarkFindOverlappingPartitions(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			idx := NewPartitionIndex()
			for day := int64(0); day < int64(size); day++ {
				idx.AddPartition(day, "")
			}

			// Query middle of range
			minDay := int64(size / 2)
			maxDay := minDay + 10

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx.FindOverlappingPartitions(minDay, maxDay)
			}
		})
	}
}
