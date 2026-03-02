package main

import (
	"fmt"
	"strings"
)

func RunInvariantsDemo() {
	fmt.Println("=== Lab 3: LSM Invariants ===")
	fmt.Println()
	fmt.Println("Invariants are properties that must always hold true.")
	fmt.Println("Violations indicate bugs or corruption.")
	fmt.Println()

	ShowCoreInvariants()
	ShowInvariantChecking()
	ShowViolationExamples()
}

func ShowCoreInvariants() {
	fmt.Println("=== Core LSM Invariants ===")
	fmt.Println()

	invariants := []struct {
		id          string
		invariant   string
		description string
		check       string
	}{
		{
			id:          "I1",
			invariant:   "Immutability",
			description: "Once a part is flushed to disk, its content never changes",
			check:       "Part hash before merge == Part hash after reading",
		},
		{
			id:          "I2",
			invariant:   "No Data Loss",
			description: "All rows written must be readable until explicitly deleted",
			check:       "sum(rows_in_parts) + rows_in_memtable == total_rows_ingested - rows_deleted",
		},
		{
			id:          "I3",
			invariant:   "Single Writer",
			description: "Each part is owned by exactly one tier at any time",
			check:       "part in at most one of: inmemoryParts, smallParts, bigParts",
		},
		{
			id:          "I4",
			invariant:   "Reference Safety",
			description: "A part with refCount > 0 is never deleted",
			check:       "if refCount > 0 then mustDrop == false or deletion blocked",
		},
		{
			id:          "I5",
			invariant:   "Merge Atomicity",
			description: "Source parts remain visible until merge completes",
			check:       "Atomic swap: remove sources AND add result under single lock",
		},
		{
			id:          "I6",
			invariant:   "Size Monotonicity",
			description: "Merged part size >= sum of source part sizes (no data loss)",
			check:       "output.Size >= sum(input[i].Size for all i)",
		},
		{
			id:          "I7",
			invariant:   "Partition Ordering",
			description: "Parts within a partition are sorted by timestamp",
			check:       "part[i].maxTime <= part[i+1].minTime for sorted parts",
		},
		{
			id:          "I8",
			invariant:   "Tier Boundaries",
			description: "Parts respect tier size limits",
			check:       "inmemory < smallThreshold, small < bigThreshold",
		},
	}

	fmt.Printf("%-4s %-20s %s\n", "ID", "Invariant", "Description")
	fmt.Println(strings.Repeat("-", 80))
	for _, inv := range invariants {
		fmt.Printf("%-4s %-20s %s\n", inv.id, inv.invariant, inv.description)
	}
	fmt.Println()

	fmt.Println("Check predicates:")
	fmt.Println()
	for _, inv := range invariants {
		fmt.Printf("%s: %s\n", inv.id, inv.check)
	}
	fmt.Println()
}

func ShowInvariantChecking() {
	fmt.Println("=== Invariant Checking in Code ===")
	fmt.Println()

	fmt.Println("VictoriaLogs enforces invariants at these points:")
	fmt.Println()

	checks := []struct {
		location       string
		invariants     []string
		implementation string
	}{
		{
			location:       "Part creation (flush)",
			invariants:     []string{"I1", "I2", "I6"},
			implementation: "fsync before adding to active list",
		},
		{
			location:       "Merge completion",
			invariants:     []string{"I3", "I5", "I6"},
			implementation: "partsLock held for atomic swap",
		},
		{
			location:       "Reference counting",
			invariants:     []string{"I4"},
			implementation: "incRef before use, decRef after use",
		},
		{
			location:       "Part deletion",
			invariants:     []string{"I4"},
			implementation: "delete only when refCount == 0 AND mustDrop == true",
		},
		{
			location:       "Tier transitions",
			invariants:     []string{"I8"},
			implementation: "check size before adding to tier",
		},
	}

	for _, c := range checks {
		fmt.Printf("%s:\n", c.location)
		fmt.Printf("  Invariants: %s\n", strings.Join(c.invariants, ", "))
		fmt.Printf("  How: %s\n", c.implementation)
		fmt.Println()
	}
}

func ShowViolationExamples() {
	fmt.Println("=== Invariant Violation Scenarios ===")
	fmt.Println()

	violations := []struct {
		invariant string
		violation string
		symptom   string
		rootCause string
	}{
		{
			invariant: "I1 (Immutability)",
			violation: "Part content changes after flush",
			symptom:   "Query returns different results for same query",
			rootCause: "Memory corruption, disk corruption, or concurrent write bug",
		},
		{
			invariant: "I2 (No Data Loss)",
			violation: "Rows missing after merge",
			symptom:   "Count queries return fewer results over time",
			rootCause: "Merge bug dropping rows, or premature part deletion",
		},
		{
			invariant: "I3 (Single Writer)",
			violation: "Part appears in multiple tiers",
			symptom:   "Double counting in metrics, duplicate query results",
			rootCause: "Missing lock or race condition in tier assignment",
		},
		{
			invariant: "I4 (Reference Safety)",
			violation: "Part deleted while query in progress",
			symptom:   "Panic, segfault, or garbage data in results",
			rootCause: "decRef without checking refCount, or missing incRef",
		},
		{
			invariant: "I5 (Merge Atomicity)",
			violation: "Sources deleted before merge completes",
			symptom:   "Data disappears during merge, partial results",
			rootCause: "Lock not held during part list modification",
		},
		{
			invariant: "I6 (Size Monotonicity)",
			violation: "Merged part smaller than inputs",
			symptom:   "Unexpected data loss, compression issues",
			rootCause: "Bug in merge logic or compression",
		},
	}

	fmt.Printf("%-20s %s\n", "Invariant", "Violation → Symptom → Root Cause")
	fmt.Println(strings.Repeat("-", 90))
	for _, v := range violations {
		fmt.Printf("%-20s %s\n", v.invariant, v.violation)
		fmt.Printf("%-20s → Symptom: %s\n", "", v.symptom)
		fmt.Printf("%-20s → Cause: %s\n", "", v.rootCause)
		fmt.Println()
	}

	fmt.Println("Testing invariants:")
	fmt.Println("  1. Unit tests with assertions")
	fmt.Println("  2. Fuzz testing with invariant checks")
	fmt.Println("  3. Integration tests with before/after verification")
	fmt.Println("  4. Runtime assertions in debug builds")
	fmt.Println("  5. Metrics and alerts for production monitoring")
	fmt.Println()
}
