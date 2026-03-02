package main

import (
	"fmt"
	"sort"
	"strings"
)

// ==================== Merge Consolidation Types ====================

type StreamIDSimple string

type IndexItem struct {
	TenantID  string
	TagName   string
	TagValue  string
	StreamIDs []StreamIDSimple
}

func (item *IndexItem) Key() string {
	return fmt.Sprintf("[%s][%s][%s]", item.TenantID, item.TagName, item.TagValue)
}

func (item *IndexItem) String() string {
	ids := make([]string, len(item.StreamIDs))
	for i, id := range item.StreamIDs {
		ids[i] = string(id)
	}
	return fmt.Sprintf("%s → [%s]", item.Key(), strings.Join(ids, ", "))
}

// ==================== Merge Consolidation Demo ====================

func MergeConsolidationDemo() {
	fmt.Println("=== Level 9 Lab 2: Merge Consolidation ===")
	fmt.Println()
	fmt.Println("This program demonstrates how the merge callback consolidates")
	fmt.Println("tag-to-streamID entries during compaction.")
	fmt.Println()

	// Simple example
	fmt.Println("========================================")
	fmt.Println()
	SimpleConsolidationExample()

	// Example with duplicates
	fmt.Println("========================================")
	fmt.Println()
	DuplicateConsolidationExample()

	// Unsorted fallback
	fmt.Println("========================================")
	fmt.Println()
	UnsortedFallbackExample()

	// maxStreamIDsPerRow
	fmt.Println("========================================")
	fmt.Println()
	MaxStreamIDsPerRowExample()
}

func SimpleConsolidationExample() {
	fmt.Println("=== Example 1: Simple Consolidation ===")
	fmt.Println()
	fmt.Println("Tag: host=\"web-01\"")
	fmt.Println("TenantID: 12345")
	fmt.Println()

	// Initial state
	fmt.Println("Initial state (4 separate items):")
	items := []IndexItem{
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S1"}},
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S5"}},
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S12"}},
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S99"}},
	}

	for i, item := range items {
		fmt.Printf("  Item %d: %s\n", i+1, item.String())
	}
	fmt.Println()

	fmt.Printf("Storage: 4 items × ~35 bytes = 140 bytes\n")
	fmt.Printf("Query cost: 4 separate reads\n")
	fmt.Println()

	// Merge process
	fmt.Println("Merge process:")
	fmt.Println("  1. Identify items with same prefix: [12345][host][web-01]")
	fmt.Println("  2. Collect all streamIDs: [S1, S5, S12, S99]")
	fmt.Println("  3. Sort streamIDs: [S1, S5, S12, S99]")
	fmt.Println("  4. Deduplicate (no duplicates in this case)")
	fmt.Println("  5. Emit consolidated item")
	fmt.Println()

	// Result
	fmt.Println("After merge (1 consolidated item):")
	consolidated := IndexItem{
		TenantID:  "12345",
		TagName:   "host",
		TagValue:  "web-01",
		StreamIDs: []StreamIDSimple{"S1", "S5", "S12", "S99"},
	}
	fmt.Printf("  %s\n", consolidated.String())
	fmt.Println()
	fmt.Printf("Storage: 1 item × ~83 bytes = 83 bytes\n")
	fmt.Printf("Query cost: 1 read\n")
	fmt.Println()
	fmt.Printf("Savings: 41%% storage reduction, 75%% I/O reduction\n")
	fmt.Println()
}

func DuplicateConsolidationExample() {
	fmt.Println("=== Example 2: Consolidation with Duplicates ===")
	fmt.Println()
	fmt.Println("Tag: host=\"web-01\"")
	fmt.Println("TenantID: 12345")
	fmt.Println()

	// Initial state with duplicates
	fmt.Println("Initial state (5 items with duplicate streamIDs):")
	items := []IndexItem{
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S1", "S5"}},
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S1", "S12"}},
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S5", "S99"}},
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S12", "S42"}},
		{TenantID: "12345", TagName: "host", TagValue: "web-01", StreamIDs: []StreamIDSimple{"S1", "S99"}},
	}

	for i, item := range items {
		fmt.Printf("  Item %d: %s\n", i+1, item.String())
	}
	fmt.Println()

	// Collect all streamIDs
	allIDs := make([]StreamIDSimple, 0)
	for _, item := range items {
		allIDs = append(allIDs, item.StreamIDs...)
	}

	fmt.Printf("Total streamIDs collected: %d\n", len(allIDs))
	fmt.Printf("  [S1, S5, S1, S12, S5, S99, S12, S42, S1, S99]\n")
	fmt.Println()

	// Sort
	sort.Slice(allIDs, func(i, j int) bool {
		return allIDs[i] < allIDs[j]
	})
	fmt.Printf("After sort:\n")
	fmt.Printf("  %v\n", allIDs)
	fmt.Println()

	// Deduplicate
	unique := deduplicate(allIDs)
	fmt.Printf("After deduplication:\n")
	fmt.Printf("  %v\n", unique)
	fmt.Printf("Unique streamIDs: %d\n", len(unique))
	fmt.Println()

	// Result
	fmt.Println("After merge (1 consolidated item):")
	consolidated := IndexItem{
		TenantID:  "12345",
		TagName:   "host",
		TagValue:  "web-01",
		StreamIDs: unique,
	}
	fmt.Printf("  %s\n", consolidated.String())
	fmt.Println()
}

func deduplicate(sorted []StreamIDSimple) []StreamIDSimple {
	if len(sorted) < 2 {
		return sorted
	}

	result := []StreamIDSimple{sorted[0]}
	for i := 1; i < len(sorted); i++ {
		if sorted[i] != sorted[i-1] {
			result = append(result, sorted[i])
		}
	}
	return result
}

func UnsortedFallbackExample() {
	fmt.Println("=== Example 3: Unsorted Fallback ===")
	fmt.Println()
	fmt.Println("Problem case: Duplicate streamIDs across items can cause")
	fmt.Println("consolidated output to become unsorted.")
	fmt.Println()

	fmt.Println("Initial items:")
	fmt.Println("  Item 1: [prefix][S1, S1, S5]    // Duplicate S1")
	fmt.Println("  Item 2: [prefix][S1, S4]")
	fmt.Println()

	fmt.Println("After consolidation:")
	fmt.Println("  Item 1: [prefix][S1, S5]        // Deduplicated")
	fmt.Println("  Item 2: [prefix][S1, S4]")
	fmt.Println()

	fmt.Println("Sort check:")
	fmt.Println("  [prefix][S1, S5] > [prefix][S1, S4] ?")
	fmt.Println("  S5 > S4 → YES")
	fmt.Println()
	fmt.Println("  VIOLATION: Items became unsorted!")
	fmt.Println()

	fmt.Println("Fallback handling:")
	fmt.Println("  1. Detect unsorted output")
	fmt.Println("  2. Return original unmodified items")
	fmt.Println("  3. Retry will happen in next merge")
	fmt.Println()

	fmt.Println("Why fallback is safe:")
	fmt.Println("  - Original items are sorted and valid")
	fmt.Println("  - Next merge will have different arrangement")
	fmt.Println("  - This case is rare (concurrent duplicates)")
	fmt.Println()
}

func MaxStreamIDsPerRowExample() {
	fmt.Println("=== Example 4: maxStreamIDsPerRow Limit ===")
	fmt.Println()
	fmt.Println("const maxStreamIDsPerRow = 32")
	fmt.Println()

	fmt.Println("Scenario: 80 streamIDs for host=\"web-01\"")
	fmt.Println()

	fmt.Println("Cannot fit all 80 streamIDs in one item.")
	fmt.Println("Solution: Emit multiple items with same prefix")
	fmt.Println()

	fmt.Println("After consolidation:")
	fmt.Println("  Item 1: [prefix][S1...S32]     // 32 IDs")
	fmt.Println("  Item 2: [prefix][S33...S64]    // 32 IDs")
	fmt.Println("  Item 3: [prefix][S65...S80]    // 16 IDs")
	fmt.Println()

	fmt.Println("Benefits:")
	fmt.Println("  - Prevents individual items from becoming too large")
	fmt.Println("  - Maintains reasonable index block sizes")
	fmt.Println("  - Query still reads fewer items than without consolidation")
	fmt.Println()

	fmt.Println("Query behavior:")
	fmt.Println("  Seek: [prefix]")
	fmt.Println("  Scan: All 3 items match prefix")
	fmt.Println("  Collect: All 80 streamIDs")
	fmt.Println()
}

// ==================== Merge Callback Summary ====================

func ShowMergeCallbackSummary() {
	fmt.Println("=== Merge Callback Summary ===")
	fmt.Println()

	fmt.Println("Key operations:")
	fmt.Println("  1. Identify consecutive items with same (tenant, tag, value)")
	fmt.Println("  2. Collect streamIDs from all matching items")
	fmt.Println("  3. Sort streamIDs")
	fmt.Println("  4. Deduplicate streamIDs")
	fmt.Println("  5. Cap at maxStreamIDsPerRow (32)")
	fmt.Println("  6. Emit consolidated items")
	fmt.Println()

	fmt.Println("Constraints:")
	fmt.Println("  - First and last items pass through unchanged (boundary anchors)")
	fmt.Println("  - Output must remain sorted (fallback if not)")
	fmt.Println()

	fmt.Println("Benefits:")
	fmt.Println("  - Storage reduction: ~97% for typical workloads")
	fmt.Println("  - Query speedup: ~31× fewer reads")
	fmt.Println("  - Automatic deduplication")
	fmt.Println()
}
