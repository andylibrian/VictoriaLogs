package main

import (
	"fmt"
	"strings"
)

// ==================== Snapshot Types ====================

type Inode struct {
	ID        int
	LinkCount int
	RefCount  int
	Blocks    []string
	IsDeleted bool
}

type FileRef struct {
	Path       string
	Inode      *Inode
	IsHardLink bool
}

type Part struct {
	Name     string
	RefCount int
	MustDrop bool
	Inode    *Inode
}

// ==================== Snapshot Demo ====================

func SnapshotConsistencyDemo() {
	fmt.Println("=== Level 10 Lab 1: Snapshot Consistency ===")
	fmt.Println()
	fmt.Println("This program demonstrates why snapshot + hard-links")
	fmt.Println("remain consistent while background merges continue.")
	fmt.Println()

	ShowHardLinkSemantics()
	ShowSnapshotScenario()
	ShowMergeDuringSnapshot()
	ShowConsistencyGuarantees()
}

func ShowHardLinkSemantics() {
	fmt.Println("=== Unix Hard Link Semantics ===")
	fmt.Println()

	fmt.Println("A hard link creates an additional directory entry")
	fmt.Println("pointing to the same physical disk blocks (inode).")
	fmt.Println()

	fmt.Println("Before hard link:")
	fmt.Println("  Live: part_ABC/timestamps.bin → inode #42")
	fmt.Println("  Inode #42: link_count=1, blocks=[B1, B2, B3]")
	fmt.Println()

	fmt.Println("After hard link:")
	fmt.Println("  Live:     part_ABC/timestamps.bin → inode #42")
	fmt.Println("  Snapshot: part_ABC/timestamps.bin → inode #42")
	fmt.Println("  Inode #42: link_count=2, blocks=[B1, B2, B3]")
	fmt.Println()

	fmt.Println("After merge deletes live part:")
	fmt.Println("  Live:     part_ABC/ → DELETED")
	fmt.Println("  Snapshot: part_ABC/timestamps.bin → inode #42")
	fmt.Println("  Inode #42: link_count=1, blocks=[B1, B2, B3] (still alive)")
	fmt.Println()

	fmt.Println("Unix guarantee: Blocks freed only when:")
	fmt.Println("  1. link_count = 0 (no directory entries)")
	fmt.Println("  2. ref_count = 0 (no open file handles)")
	fmt.Println()
}

func ShowSnapshotScenario() {
	fmt.Println("=== Snapshot Creation Timeline ===")
	fmt.Println()

	fmt.Println("T0: Snapshot starts")
	fmt.Println("    - Acquire snapshotLock")
	fmt.Println("    - Live parts: [part_ABC, part_DEF, part_GHI]")
	fmt.Println()

	fmt.Println("T1: Force-flush in-memory parts")
	fmt.Println("    - All data now file-backed")
	fmt.Println("    - Live parts: [part_ABC, part_DEF, part_GHI, part_JKL]")
	fmt.Println()

	fmt.Println("T2: Collect parts under partsLock")
	fmt.Println("    - partsLock acquired")
	fmt.Println("    - part_ABC.incRef() → refCount: 0 → 1")
	fmt.Println("    - part_DEF.incRef() → refCount: 0 → 1")
	fmt.Println("    - part_GHI.incRef() → refCount: 0 → 1")
	fmt.Println("    - partsLock released")
	fmt.Println()

	fmt.Println("T3: Hard-link parts to snapshot")
	fmt.Println("    - fs.MustHardLinkFiles(live/part_ABC, snapshot/part_ABC)")
	fmt.Println("    - fs.MustHardLinkFiles(live/part_DEF, snapshot/part_DEF)")
	fmt.Println("    - fs.MustHardLinkFiles(live/part_GHI, snapshot/part_GHI)")
	fmt.Println()

	fmt.Println("T4: Release references")
	fmt.Println("    - part_ABC.decRef() → refCount: 1 → 0 (or stays > 0)")
	fmt.Println("    - part_DEF.decRef() → refCount: 1 → 0")
	fmt.Println("    - part_GHI.decRef() → refCount: 1 → 0")
	fmt.Println()

	fmt.Println("T5: Snapshot complete")
	fmt.Println("    - Release snapshotLock")
	fmt.Println("    - Snapshot directory contains hard links")
	fmt.Println()
}

func ShowMergeDuringSnapshot() {
	fmt.Println("=== Scenario: Merge During Snapshot ===")
	fmt.Println()

	fmt.Println("Initial state:")
	fmt.Println("  Live:     part_ABC → inode #42, refCount: 0")
	fmt.Println("  Snapshot: (not yet created)")
	fmt.Println()

	fmt.Println("T1: Snapshot collects parts")
	fmt.Println("    partsLock acquired")
	fmt.Println("    part_ABC.incRef() → refCount: 1")
	fmt.Println("    partsLock released")
	fmt.Println()

	fmt.Println("T2: Merge completes (concurrently)")
	fmt.Println("    Merge creates part_MERGED")
	fmt.Println("    Under partsLock:")
	fmt.Println("      - Remove part_ABC from active list")
	fmt.Println("      - part_ABC.mustDrop = true")
	fmt.Println("      - part_ABC.decRef() → refCount: 1 → 0? NO!")
	fmt.Println()
	fmt.Println("    part_ABC.refCount still = 1 (snapshot holds reference)")
	fmt.Println("    part_ABC is NOT deleted yet!")
	fmt.Println()

	fmt.Println("T3: Snapshot hard-links part_ABC")
	fmt.Println("    fs.MustHardLinkFiles(part_ABC, snapshot/part_ABC)")
	fmt.Println()
	fmt.Println("    Live:     part_ABC → inode #42, refCount: 1")
	fmt.Println("    Snapshot: part_ABC → inode #42, link_count: 2")
	fmt.Println()

	fmt.Println("T4: Snapshot releases reference")
	fmt.Println("    part_ABC.decRef() → refCount: 1 → 0")
	fmt.Println()
	fmt.Println("    But live already removed part_ABC from active list")
	fmt.Println("    Live holds no reference to part_ABC")
	fmt.Println("    Snapshot's hard link keeps inode #42 alive")
	fmt.Println()

	fmt.Println("Result: Snapshot is consistent!")
	fmt.Println("  - Snapshot has hard link to inode #42")
	fmt.Println("  - Even though live deleted part_ABC")
	fmt.Println("  - Data blocks remain until snapshot deleted")
	fmt.Println()
}

func ShowConsistencyGuarantees() {
	fmt.Println("=== Consistency Guarantees ===")
	fmt.Println()

	guarantees := []struct {
		property string
		achieved string
	}{
		{"Point-in-time", "Flush all in-memory parts before snapshot"},
		{"Atomic", "Hard links are atomic at filesystem level"},
		{"Concurrent-safe", "incRef prevents deletion during snapshot"},
		{"Space-efficient", "Hard links share blocks, zero copy"},
		{"Crash-safe", "fsync ensures directory entries persisted"},
	}

	fmt.Printf("%-20s %s\n", "Property", "How Achieved")
	fmt.Println(strings.Repeat("-", 70))
	for _, g := range guarantees {
		fmt.Printf("%-20s %s\n", g.property, g.achieved)
	}
	fmt.Println()

	fmt.Println("What's NOT in the snapshot:")
	fmt.Println("  1. Rows in shard buffers (not yet in parts)")
	fmt.Println("  2. Rows arriving during snapshot execution")
	fmt.Println("  3. In-memory parts created after flush")
	fmt.Println()

	fmt.Println("Trade-off:")
	fmt.Println("  - Snapshot may miss ~1 second of data")
	fmt.Println("  - But ingestion throughput is unaffected")
	fmt.Println("  - No global pause needed")
	fmt.Println()
}

// ==================== Reference Counting Demo ====================

func ShowReferenceCounting() {
	fmt.Println("=== Reference Counting Mechanism ===")
	fmt.Println()

	fmt.Println("Every lifecycle operation uses reference counting:")
	fmt.Println()

	operations := []struct {
		operation string
		uses      string
		reason    string
	}{
		{"Query scan", "partWrapper.incRef/decRef", "Prevent part deletion mid-scan"},
		{"Merge execution", "partWrapper.incRef/decRef", "Prevent source part deletion"},
		{"Snapshot creation", "partWrapper.incRef/decRef", "Prevent part deletion during hard-linking"},
		{"Retention removal", "partitionWrapper.decRef", "Wait for queries to finish"},
		{"Disk pressure", "partitionWrapper.decRef", "Same as retention"},
		{"Partition detach", "partitionWrapper.decRef", "Block until all activity ceases"},
	}

	fmt.Printf("%-20s %-30s %s\n", "Operation", "Uses", "Reason")
	fmt.Println(strings.Repeat("-", 80))
	for _, op := range operations {
		fmt.Printf("%-20s %-30s %s\n", op.operation, op.uses, op.reason)
	}
	fmt.Println()

	fmt.Println("Without reference counting:")
	fmt.Println("  - Every operation would need its own coordination")
	fmt.Println("  - Read-write locks, condition variables, quiesce protocols")
	fmt.Println()

	fmt.Println("With reference counting:")
	fmt.Println("  - Single primitive: incRef before use, decRef after use")
	fmt.Println("  - Delete when refCount reaches 0")
	fmt.Println("  - Replaces all complex coordination")
	fmt.Println()
}

// ==================== Simulation ====================

func SimulateSnapshotMerge() {
	fmt.Println("=== Simulation: Snapshot with Concurrent Merge ===")
	fmt.Println()

	// Initial state
	inode := &Inode{ID: 42, LinkCount: 1, RefCount: 0, Blocks: []string{"B1", "B2", "B3"}}
	partABC := &Part{Name: "part_ABC", RefCount: 0, MustDrop: false, Inode: inode}

	fmt.Println("Initial state:")
	fmt.Printf("  part_ABC: refCount=%d, mustDrop=%v\n", partABC.RefCount, partABC.MustDrop)
	fmt.Printf("  inode #42: linkCount=%d, refCount=%d\n", inode.LinkCount, inode.RefCount)
	fmt.Println()

	// Snapshot starts
	fmt.Println("Step 1: Snapshot acquires reference")
	partABC.RefCount++
	fmt.Printf("  part_ABC: refCount=%d\n", partABC.RefCount)
	fmt.Println()

	// Merge completes
	fmt.Println("Step 2: Merge completes, marks part for deletion")
	partABC.MustDrop = true
	fmt.Printf("  part_ABC: mustDrop=%v\n", partABC.MustDrop)
	fmt.Println("  (But refCount > 0, so not deleted yet)")
	fmt.Println()

	// Hard link created
	fmt.Println("Step 3: Snapshot creates hard link")
	inode.LinkCount++
	fmt.Printf("  inode #42: linkCount=%d\n", inode.LinkCount)
	fmt.Println()

	// Snapshot releases
	fmt.Println("Step 4: Snapshot releases reference")
	partABC.RefCount--
	fmt.Printf("  part_ABC: refCount=%d\n", partABC.RefCount)
	fmt.Println("  Live has removed part from active list")
	fmt.Println("  Live holds no reference")
	fmt.Println()

	// Live deleted
	fmt.Println("Step 5: Live part directory deleted")
	inode.LinkCount--
	fmt.Printf("  inode #42: linkCount=%d\n", inode.LinkCount)
	fmt.Println("  Snapshot's hard link keeps inode alive")
	fmt.Println()

	// Eventually
	fmt.Println("Step 6: Snapshot deleted (hours/days later)")
	inode.LinkCount--
	fmt.Printf("  inode #42: linkCount=%d\n", inode.LinkCount)
	fmt.Println("  linkCount = 0 → blocks freed")
	fmt.Println()

	fmt.Println("Result: Snapshot remained consistent throughout!")
}
