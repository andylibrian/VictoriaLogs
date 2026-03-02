package main

import (
	"fmt"
	"strings"
)

// ==================== Merge Trace Demo ====================

func MergeTraceDemo() {
	fmt.Println("=== Level 6 Lab 1: Merge Trace ===")
	fmt.Println()
	fmt.Println("This program traces a complete merge cycle with state snapshots.")
	fmt.Println()

	ddb := NewDataDB()

	// Scenario 1: In-memory merge (stays in-memory)
	fmt.Println("--- Scenario 1: In-memory Merge ---")
	fmt.Println()

	// Add some in-memory parts
	ddb.AddInmemoryPart(1000, 200*1024) // 200 KB
	ddb.AddInmemoryPart(1000, 200*1024) // 200 KB
	ddb.AddInmemoryPart(1000, 200*1024) // 200 KB

	state := ddb.GetTierState()
	fmt.Printf("Initial state: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	// Select and merge in-memory parts
	inmemoryParts := ddb.SelectPartsForMerge(partInmemory)
	trace1 := ddb.MustMergePartsInternal(inmemoryParts, false)
	PrintMergeTrace(trace1)

	fmt.Println()
	state = ddb.GetTierState()
	fmt.Printf("After merge: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	// Scenario 2: Final flush to disk
	fmt.Println("--- Scenario 2: Final Flush to Disk ---")
	fmt.Println()

	ddb.AddInmemoryPart(5000, 500*1024) // 500 KB
	ddb.AddInmemoryPart(5000, 500*1024) // 500 KB

	state = ddb.GetTierState()
	fmt.Printf("Initial state: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	inmemoryParts = ddb.SelectPartsForMerge(partInmemory)
	trace2 := ddb.MustMergePartsInternal(inmemoryParts, true) // isFinal=true
	PrintMergeTrace(trace2)

	fmt.Println()
	state = ddb.GetTierState()
	fmt.Printf("After merge: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	// Scenario 3: Small to big promotion
	fmt.Println("--- Scenario 3: Small to Big Promotion ---")
	fmt.Println()

	// Add large small parts that will exceed maxSmallPartSize when merged
	ddb.AddSmallPart(50000, 40*1024*1024) // 40 MB
	ddb.AddSmallPart(50000, 40*1024*1024) // 40 MB
	ddb.AddSmallPart(50000, 40*1024*1024) // 40 MB

	state = ddb.GetTierState()
	fmt.Printf("Initial state: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	smallParts := ddb.SelectPartsForMerge(partSmall)
	trace3 := ddb.MustMergePartsInternal(smallParts, false)
	PrintMergeTrace(trace3)

	fmt.Println()
	state = ddb.GetTierState()
	fmt.Printf("After merge: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	// Scenario 4: Mixed merge (durability preservation)
	fmt.Println("--- Scenario 4: Durability Preservation ---")
	fmt.Println()

	// This demonstrates the "never regress durability" rule
	// Even if output is small, file-based inputs must produce file-based output
	ddb.AddSmallPart(1000, 100*1024) // 100 KB (on disk)
	ddb.AddSmallPart(1000, 100*1024) // 100 KB (on disk)

	state = ddb.GetTierState()
	fmt.Printf("Initial state: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	fmt.Println("Key point: Even though total size is 200KB (< 1MB inmemory limit),")
	fmt.Println("the output will be 'small' (file-based) because inputs are file-based.")
	fmt.Println("This prevents regressing from durable to volatile storage.")
	fmt.Println()

	smallParts = ddb.SelectPartsForMerge(partSmall)
	trace4 := ddb.MustMergePartsInternal(smallParts, false)
	PrintMergeTrace(trace4)

	fmt.Println()
	state = ddb.GetTierState()
	fmt.Printf("After merge: inmemory=%d, small=%d, big=%d\n",
		state.InmemoryCount, state.SmallCount, state.BigCount)
	fmt.Println()

	// Full trace summary
	fmt.Println("=== Complete Trace Summary ===")
	fmt.Println()
	PrintTraceSummary(trace1, trace2, trace3, trace4)
}

func PrintMergeTrace(trace *MergeTrace) {
	if trace == nil {
		fmt.Println("No trace available")
		return
	}

	fmt.Println("Merge Trace:")
	fmt.Printf("  Duration: %v\n", trace.EndTime.Sub(trace.StartTime))
	fmt.Printf("  Source parts: %d\n", len(trace.SourceParts))
	fmt.Printf("  Destination type: %s\n", trace.DstPartType)
	if trace.DstPart != nil {
		fmt.Printf("  Destination part: %d (%d rows)\n",
			trace.DstPart.id, trace.DstPart.rowsCount)
	}
	fmt.Println()

	fmt.Println("  Event timeline:")
	for _, event := range trace.Events {
		fmt.Printf("    [%s] %s\n", event.Action, event.Details)
	}
}

func PrintTraceSummary(traces ...*MergeTrace) {
	fmt.Printf("%-15s %-10s %-15s %-15s %-15s\n",
		"Scenario", "Sources", "Src Tiers", "Dst Type", "Dst Rows")
	fmt.Println(strings.Repeat("-", 75))

	for i, trace := range traces {
		if trace == nil {
			continue
		}

		srcTiers := make(map[partType]bool)
		for _, pw := range trace.SourceParts {
			srcTiers[pw.partType] = true
		}

		tierStr := ""
		for pt := range srcTiers {
			if tierStr != "" {
				tierStr += "+"
			}
			tierStr += pt.String()
		}

		dstRows := uint64(0)
		if trace.DstPart != nil {
			dstRows = trace.DstPart.rowsCount
		}

		fmt.Printf("%-15d %-10d %-15s %-15s %-15d\n",
			i+1, len(trace.SourceParts), tierStr,
			trace.DstPartType, dstRows)
	}
}

// ==================== Failure Scenarios Demo ====================

func FailureScenariosDemo() {
	fmt.Println("=== Level 6 Lab 2: Failure Scenario Reasoning ===")
	fmt.Println()

	// Scenario 1: Crash during merge (before swap)
	fmt.Println("--- Scenario 1: Crash During Merge (Before Swap) ---")
	fmt.Println()

	fmt.Println("Timeline:")
	fmt.Println("  1. Merge starts reading parts [P1, P2, P3]")
	fmt.Println("  2. Creating output part P4...")
	fmt.Println("  3. CRASH!")
	fmt.Println()

	fmt.Println("On recovery:")
	fmt.Println("  - parts.json still lists: [P1, P2, P3]")
	fmt.Println("  - On disk: [P1, P2, P3, partial-P4]")
	fmt.Println("  - P4 is not in parts.json → orphan → deleted")
	fmt.Println("  - Result: [P1, P2, P3] - no data loss")
	fmt.Println()

	fmt.Println("Why safe:")
	fmt.Println("  - parts.json is source of truth")
	fmt.Println("  - Partial P4 is ignored (not in manifest)")
	fmt.Println("  - Original data intact")
	fmt.Println()

	// Scenario 2: Crash after metadata write, before parts.json
	fmt.Println("--- Scenario 2: Crash After Metadata, Before parts.json ---")
	fmt.Println()

	fmt.Println("Timeline:")
	fmt.Println("  1. Merge completes, P4 fully written")
	fmt.Println("  2. P4 metadata fsynced")
	fmt.Println("  3. About to update parts.json...")
	fmt.Println("  4. CRASH!")
	fmt.Println()

	fmt.Println("On recovery:")
	fmt.Println("  - parts.json still lists: [P1, P2, P3]")
	fmt.Println("  - On disk: [P1, P2, P3, P4]")
	fmt.Println("  - P4 is not in parts.json → orphan → deleted")
	fmt.Println("  - Result: [P1, P2, P3] - no data loss")
	fmt.Println()

	fmt.Println("Why safe:")
	fmt.Println("  - Same as scenario 1")
	fmt.Println("  - Complete but unreferenced P4 is treated as orphan")
	fmt.Println()

	// Scenario 3: Crash after parts.json update
	fmt.Println("--- Scenario 3: Crash After parts.json Update ---")
	fmt.Println()

	fmt.Println("Timeline:")
	fmt.Println("  1. Merge completes, P4 fully written")
	fmt.Println("  2. P4 metadata fsynced")
	fmt.Println("  3. parts.json updated to: [P1, P4] (P2, P3 removed)")
	fmt.Println("  4. About to delete P2, P3...")
	fmt.Println("  5. CRASH!")
	fmt.Println()

	fmt.Println("On recovery:")
	fmt.Println("  - parts.json lists: [P1, P4]")
	fmt.Println("  - On disk: [P1, P2, P3, P4]")
	fmt.Println("  - P2, P3 not in parts.json → orphans → deleted")
	fmt.Println("  - Result: [P1, P4] - clean state")
	fmt.Println()

	fmt.Println("Why safe:")
	fmt.Println("  - parts.json updated atomically (temp + fsync + rename)")
	fmt.Println("  - Orphan cleanup removes old parts")
	fmt.Println("  - All referenced parts exist")
	fmt.Println()

	// Scenario 4: Crash during parts.json write
	fmt.Println("--- Scenario 4: Crash During parts.json Write ---")
	fmt.Println()

	fmt.Println("Timeline:")
	fmt.Println("  1. Writing parts.json.temp...")
	fmt.Println("  2. CRASH!")
	fmt.Println()

	fmt.Println("On recovery:")
	fmt.Println("  - parts.json.temp exists (incomplete)")
	fmt.Println("  - parts.json still intact (not renamed yet)")
	fmt.Println("  - parts.json.temp deleted on startup")
	fmt.Println("  - Result: [P1, P2, P3] - no data loss")
	fmt.Println()

	fmt.Println("Why safe:")
	fmt.Println("  - Atomic rename: parts.json.temp → parts.json")
	fmt.Println("  - If rename didn't complete, original parts.json intact")
	fmt.Println("  - Startup cleans up temp files")
	fmt.Println()

	// Summary table
	fmt.Println("=== Failure Scenario Summary ===")
	fmt.Println()

	fmt.Printf("%-40s %-20s %-15s\n",
		"Crash Point", "parts.json Shows", "Recovery Action")
	fmt.Println(strings.Repeat("-", 80))

	scenarios := []struct {
		point    string
		manifest string
		action   string
	}{
		{"During merge (step 7)", "[P1, P2, P3]", "Delete orphan P4"},
		{"After metadata write (step 8)", "[P1, P2, P3]", "Delete orphan P4"},
		{"After parts.json (step 9)", "[P1, P4]", "Delete orphans P2, P3"},
		{"During parts.json write", "[P1, P2, P3]", "Delete temp file"},
		{"After cleanup complete", "[P1, P4]", "Clean state"},
	}

	for _, s := range scenarios {
		fmt.Printf("%-40s %-20s %-15s\n", s.point, s.manifest, s.action)
	}

	fmt.Println()
	fmt.Println("Key insight: parts.json is always the source of truth.")
	fmt.Println("Any directory not in parts.json is an orphan and safe to delete.")
}

// ==================== Concurrent Query Demo ====================

func ConcurrentQueryDemo() {
	fmt.Println("=== Bonus: Concurrent Query + Merge Interaction ===")
	fmt.Println()

	ddb := NewDataDB()

	// Add parts
	p1 := ddb.AddInmemoryPart(1000, 200*1024)
	p2 := ddb.AddInmemoryPart(1000, 200*1024)
	p3 := ddb.AddInmemoryPart(1000, 200*1024)
	p4 := ddb.AddInmemoryPart(1000, 200*1024)
	p5 := ddb.AddInmemoryPart(1000, 200*1024)

	fmt.Println("Initial parts: P1, P2, P3, P4, P5")
	fmt.Println()

	// Timeline simulation
	fmt.Println("Time 0: Parts list = [P1, P2, P3, P4, P5]")
	fmt.Println()

	fmt.Println("Time 1: Query starts")
	fmt.Println("  partsLock.Lock()")
	fmt.Println("  Select P2, P3 (time range match)")
	fmt.Println("  P2.incRef() → refCount=2")
	fmt.Println("  P3.incRef() → refCount=2")
	fmt.Println("  partsLock.Unlock()")
	fmt.Println("  → Query begins scanning P2 and P3")
	fmt.Println()

	fmt.Println("Time 2: Merge worker starts")
	fmt.Println("  partsLock.Lock()")
	fmt.Println("  Select P2, P3, P4 (best merge ratio)")
	fmt.Println("  P2.isInMerge = true")
	fmt.Println("  P3.isInMerge = true")
	fmt.Println("  P4.isInMerge = true")
	fmt.Println("  partsLock.Unlock()")
	fmt.Println("  → Merge begins reading P2, P3, P4 and writing P6")
	fmt.Println()

	fmt.Println("Time 3: Merge completes")
	fmt.Println("  partsLock.Lock()")
	fmt.Println("  Remove P2, P3, P4 from list")
	fmt.Println("  Add P6 to list")
	fmt.Println("  Write parts.json = [P1, P5, P6]")
	fmt.Println("  partsLock.Unlock()")
	fmt.Println("  P2.mustDrop = true; P2.decRef() → refCount=1 (query still holds)")
	fmt.Println("  P3.mustDrop = true; P3.decRef() → refCount=1 (query still holds)")
	fmt.Println("  P4.mustDrop = true; P4.decRef() → refCount=0 → delete immediately")
	fmt.Println()

	fmt.Println("Time 4: Query finishes")
	fmt.Println("  P2.decRef() → refCount=0, mustDrop=true → delete")
	fmt.Println("  P3.decRef() → refCount=0, mustDrop=true → delete")
	fmt.Println()

	fmt.Println("Time 5: Parts list = [P1, P5, P6], all old parts deleted")
	fmt.Println()

	// Demonstrate reference counting
	fmt.Println("=== Reference Counting Demo ===")
	fmt.Println()

	fmt.Printf("P1 refCount: %d\n", p1.refCount.Load())
	fmt.Printf("P2 refCount: %d (would be 0 after query)\n", p2.refCount.Load())
	fmt.Printf("P3 refCount: %d (would be 0 after query)\n", p3.refCount.Load())
	fmt.Printf("P4 refCount: %d (deleted immediately)\n", p4.refCount.Load())
	fmt.Printf("P5 refCount: %d\n", p5.refCount.Load())

	fmt.Println()
	fmt.Println("Key insight: Reference counting ensures safe concurrent access.")
	fmt.Println("Parts are only deleted when no queries reference them.")
}

func main() {
	// Run Lab 1: Merge Trace
	MergeTraceDemo()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Run Lab 2: Failure Scenarios
	FailureScenariosDemo()

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println()

	// Bonus: Concurrent Query Demo
	ConcurrentQueryDemo()
}
