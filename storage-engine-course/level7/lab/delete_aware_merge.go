package main

import (
	"fmt"
	"strings"
)

// ==================== Delete-Aware Merge Types ====================

type DeleteFilter struct {
	StreamID  *StreamID
	StartTime int64
}

type BlockDecision int

const (
	DecisionWrite BlockDecision = iota
	DecisionDrop
	DecisionFilter
)

func (d BlockDecision) String() string {
	switch d {
	case DecisionWrite:
		return "WRITE"
	case DecisionDrop:
		return "DROP"
	case DecisionFilter:
		return "FILTER"
	default:
		return "UNKNOWN"
	}
}

// ==================== Delete-Aware Merge Analysis ====================

func DeleteAwareMergeDemo() {
	fmt.Println("=== Level 7 Lab 3: Delete-Aware Merge ===")
	fmt.Println()
	fmt.Println("This program explains how deleted rows are physically removed")
	fmt.Println("through merge operations.")
	fmt.Println()

	// Explain the concept
	ExplainDeleteConcept()

	// Show decision tree
	ShowBlockDecisionTree()

	// Examples
	DeleteExamples()

	// Timeline
	DeleteTimeline()
}

func ExplainDeleteConcept() {
	fmt.Println("=== How Deletion Works in VictoriaLogs ===")
	fmt.Println()

	fmt.Println("KEY INSIGHT: No tombstones!")
	fmt.Println()

	fmt.Println("Traditional LSM deletion:")
	fmt.Println("  1. Write tombstone record")
	fmt.Println("  2. Queries check for tombstones")
	fmt.Println("  3. Compaction removes tombstones + deleted data")
	fmt.Println()

	fmt.Println("VictoriaLogs deletion:")
	fmt.Println("  1. Delete request triggers targeted merge")
	fmt.Println("  2. Merge uses dropFilter to skip matching rows")
	fmt.Println("  3. New part created WITHOUT deleted rows")
	fmt.Println("  4. Old parts replaced atomically")
	fmt.Println("  5. Deleted rows are PHYSICALLY GONE")
	fmt.Println()

	fmt.Println("Advantages:")
	fmt.Println("  - No tombstone tracking overhead")
	fmt.Println("  - No garbage collection pass needed")
	fmt.Println("  - Immediate space reclamation")
	fmt.Println()
}

func ShowBlockDecisionTree() {
	fmt.Println("=== Block Decision Tree During Merge ===")
	fmt.Println()

	fmt.Println("mustWriteBlock(block):")
	fmt.Println()

	fmt.Println("  Step 1: Check stream change")
	fmt.Println("    IF block.streamID != currentStream:")
	fmt.Println("      → Flush accumulated rows")
	fmt.Println("      → Start new stream context")
	fmt.Println()

	fmt.Println("  Step 2: Check dropFilter (if present)")
	fmt.Println("    IF dropFilter matches block.streamID:")
	fmt.Println()

	fmt.Println("      Sub-check: Is filter stream-only (filterNoop)?")
	fmt.Println("        YES → DROP entire block (no decompression)")
	fmt.Println("              → All rows belong to deleted stream")
	fmt.Println("        NO  → FILTER: decompress, apply per-row")
	fmt.Println("              → Keep non-matching rows")
	fmt.Println()

	fmt.Println("  Step 3: Check fast path eligibility")
	fmt.Println("    IF accumulator empty AND block ≥ 2 MB:")
	fmt.Println("      AND no dropFilter match:")
	fmt.Println("        → FAST PATH: copy raw bytes")
	fmt.Println("        → No decompression needed")
	fmt.Println()

	fmt.Println("  Step 4: Merge path")
	fmt.Println("    OTHERWISE:")
	fmt.Println("      → Decompress block")
	fmt.Println("      → Merge with accumulated rows")
	fmt.Println("      → Flush when ≥ 2 MB accumulated")
	fmt.Println()
}

func DeleteExamples() {
	fmt.Println("=== Delete Examples ===")
	fmt.Println()

	// Example 1: Stream-level delete
	fmt.Println("--- Example 1: Delete Entire Stream ---")
	fmt.Println()

	fmt.Println("Delete request: DELETE FROM logs WHERE _stream='{app=\"old-app\"}'")
	fmt.Println()

	blocks := []struct {
		streamID string
		rows     int
		decision BlockDecision
		reason   string
	}{
		{"app=old-app", 1000, DecisionDrop, "Stream matches, filterNoop"},
		{"app=old-app", 500, DecisionDrop, "Stream matches, filterNoop"},
		{"app=new-app", 800, DecisionWrite, "Stream doesn't match"},
		{"app=old-app", 200, DecisionDrop, "Stream matches, filterNoop"},
		{"app=other", 300, DecisionWrite, "Stream doesn't match"},
	}

	PrintBlockDecisions(blocks)
	fmt.Println()

	fmt.Println("Result:")
	fmt.Println("  - 3 blocks from 'app=old-app' dropped (1700 rows)")
	fmt.Println("  - 2 blocks from other streams written (1100 rows)")
	fmt.Println("  - No decompression needed (stream-only filter)")
	fmt.Println()

	// Example 2: Row-level delete
	fmt.Println("--- Example 2: Delete with Row Filter ---")
	fmt.Println()

	fmt.Println("Delete request: DELETE FROM logs WHERE level='error' AND _msg:contains('timeout')")
	fmt.Println()

	blocks2 := []struct {
		streamID string
		level    string
		msg      string
		rows     int
		decision BlockDecision
		reason   string
	}{
		{"app=api", "mixed", "mixed", 1000, DecisionFilter, "Must check per-row"},
		{"app=api", "mixed", "mixed", 800, DecisionFilter, "Must check per-row"},
		{"app=web", "mixed", "mixed", 600, DecisionFilter, "Must check per-row"},
	}

	fmt.Println("Block decisions:")
	for _, b := range blocks2 {
		fmt.Printf("  Stream %s: %s (decompress, filter per-row)\n",
			b.streamID, b.decision)
	}
	fmt.Println()

	fmt.Println("Result:")
	fmt.Println("  - All blocks decompressed")
	fmt.Println("  - Per-row filter applied")
	fmt.Println("  - Only rows matching (level=error AND msg:contains('timeout')) dropped")
	fmt.Println("  - Survivors re-blocked and written")
	fmt.Println()
}

func PrintBlockDecisions(blocks []struct {
	streamID string
	rows     int
	decision BlockDecision
	reason   string
}) {
	fmt.Printf("%-20s %-10s %-10s %s\n", "Stream", "Rows", "Decision", "Reason")
	fmt.Println(strings.Repeat("-", 70))
	for _, b := range blocks {
		fmt.Printf("%-20s %-10d %-10s %s\n",
			b.streamID, b.rows, b.decision, b.reason)
	}
}

func DeleteTimeline() {
	fmt.Println("=== Delete Timeline ===")
	fmt.Println()

	fmt.Println("T0: Data ingestion")
	fmt.Println("  Parts: [P1, P2, P3]")
	fmt.Println("  P1 contains rows for stream S1")
	fmt.Println()

	fmt.Println("T1: Delete request received")
	fmt.Println("  DELETE FROM logs WHERE _stream_id=S1")
	fmt.Println("  DeleteTask.StartTime = T1")
	fmt.Println()

	fmt.Println("T2: New rows arrive for S1")
	fmt.Println("  Parts: [P1, P2, P3, P4]")
	fmt.Println("  P4 contains rows for stream S1 (ingested AFTER T1)")
	fmt.Println("  → These rows are PROTECTED (StartTime bound)")
	fmt.Println()

	fmt.Println("T3: Delete-aware merge triggered")
	fmt.Println("  Merge P1, P2, P3 with dropFilter for S1")
	fmt.Println("  dropFilter only matches rows ingested BEFORE T1")
	fmt.Println()

	fmt.Println("T4: Merge completes")
	fmt.Println("  Old parts: [P1, P2, P3]")
	fmt.Println("  New parts: [P5, P4]")
	fmt.Println("  P5 = merged data WITHOUT deleted S1 rows from P1")
	fmt.Println("  P4 = UNTOUCHED (rows ingested after StartTime)")
	fmt.Println()

	fmt.Println("T5: Query sees consistent state")
	fmt.Println("  - P5 contains S1 rows from P4 (after T1)")
	fmt.Println("  - P1, P2, P3 S1 rows are GONE")
	fmt.Println()

	fmt.Println("=== Key Timing Constraint ===")
	fmt.Println()
	fmt.Println("DeleteTask.StartTime bounds the delete:")
	fmt.Println("  - Rows ingested BEFORE StartTime: eligible for deletion")
	fmt.Println("  - Rows ingested AFTER StartTime: PROTECTED")
	fmt.Println()
	fmt.Println("Why?")
	fmt.Println("  - Without this, newly ingested rows could be deleted")
	fmt.Println("  - Race between ingestion and delete")
	fmt.Println("  - StartTime ensures clean cut-off")
}

// ==================== Space Reclamation ====================

func SpaceReclamationAnalysis() {
	fmt.Println("=== Space Reclamation Analysis ===")
	fmt.Println()

	fmt.Println("Checkpoint Question: Why can delete tasks require post-delete force merge?")
	fmt.Println()

	fmt.Println("Scenario:")
	fmt.Println("  - Delete request issued at T0")
	fmt.Println("  - Merge scheduled, but not immediately executed")
	fmt.Println("  - Space NOT reclaimed until merge completes")
	fmt.Println()

	fmt.Println("Why merge is required for space reclamation:")
	fmt.Println()

	fmt.Println("1. No in-place deletion")
	fmt.Println("   - Parts are immutable")
	fmt.Println("   - Cannot delete rows from existing files")
	fmt.Println("   - Must create NEW part without deleted rows")
	fmt.Println()

	fmt.Println("2. Merge is the only rewrite mechanism")
	fmt.Println("   - Only merge creates new parts")
	fmt.Println("   - Only merge can exclude rows")
	fmt.Println("   - Old parts replaced atomically")
	fmt.Println()

	fmt.Println("3. Force merge option")
	fmt.Println("   - API: /delete/force_merge")
	fmt.Println("   - Triggers immediate merge of all parts")
	fmt.Println("   - Guarantees space reclamation")
	fmt.Println("   - But: expensive operation")
	fmt.Println()

	fmt.Println("Timeline without force merge:")
	fmt.Println("  T0: Delete request")
	fmt.Println("  T1-T10: Normal merge cycles")
	fmt.Println("  T11: Parts with deleted data finally merged")
	fmt.Println("  → Space reclaimed at T11, not T0")
	fmt.Println()

	fmt.Println("Timeline with force merge:")
	fmt.Println("  T0: Delete request")
	fmt.Println("  T1: Force merge triggered")
	fmt.Println("  T2: All parts merged, deleted rows removed")
	fmt.Println("  → Space reclaimed at T2")
	fmt.Println()
}
