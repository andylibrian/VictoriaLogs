package main

import (
	"fmt"
	"strings"
)

// ==================== Cache Correctness Demo ====================

func CacheCorrectnessDemo() {
	fmt.Println("=== Level 9 Lab 3: Cache Correctness ===")
	fmt.Println()
	fmt.Println("This program explains why the generation counter is embedded")
	fmt.Println("in the cache key to prevent stale results.")
	fmt.Println()

	// Problem scenario
	fmt.Println("========================================")
	fmt.Println()
	ShowProblemScenario()

	// Solution
	fmt.Println("========================================")
	fmt.Println()
	ShowSolution()

	// Implementation details
	fmt.Println("========================================")
	fmt.Println()
	ShowImplementationDetails()

	// Lazy invalidation
	fmt.Println("========================================")
	fmt.Println()
	ShowLazyInvalidation()
}

func ShowProblemScenario() {
	fmt.Println("=== Problem: Stale Cache Results ===")
	fmt.Println()

	fmt.Println("Timeline WITHOUT generation counter:")
	fmt.Println()

	fmt.Println("T0: Query Q1 arrives")
	fmt.Println("    Filter: {app=\"nginx\"}")
	fmt.Println()
	fmt.Println("    Cache lookup: [partition][tenantIDs][filter]")
	fmt.Println("    Cache miss → search index → find [S1, S5, S12]")
	fmt.Println("    Cache store: [partition][tenantIDs][filter] → [S1, S5, S12]")
	fmt.Println()

	fmt.Println("T1: New stream registered")
	fmt.Println("    Stream: {app=\"nginx\", host=\"web-02\"}")
	fmt.Println("    StreamID: S42")
	fmt.Println()
	fmt.Println("    Index updated with new stream")
	fmt.Println()

	fmt.Println("T2: Query Q2 arrives (same filter)")
	fmt.Println("    Filter: {app=\"nginx\"}")
	fmt.Println()
	fmt.Println("    Cache lookup: [partition][tenantIDs][filter]")
	fmt.Println("    Cache hit → return [S1, S5, S12]")
	fmt.Println()
	fmt.Println("    BUG: S42 is MISSING from result!")
	fmt.Println("    New stream not included in cached result.")
	fmt.Println()

	fmt.Println("Result: User doesn't see newly registered stream")
	fmt.Println("        Violates expectation of immediate visibility")
	fmt.Println()
}

func ShowSolution() {
	fmt.Println("=== Solution: Generation Counter in Cache Key ===")
	fmt.Println()

	fmt.Println("Timeline WITH generation counter:")
	fmt.Println()

	fmt.Println("T0: Generation = 42")
	fmt.Println("    Query Q1: {app=\"nginx\"}")
	fmt.Println()
	fmt.Println("    Cache key: [gen=42][partition][tenantIDs][filter]")
	fmt.Println("    Cache miss → search index → find [S1, S5, S12]")
	fmt.Println("    Cache store: [gen=42][...] → [S1, S5, S12]")
	fmt.Println()

	fmt.Println("T1: New stream registered")
	fmt.Println("    Stream: {app=\"nginx\", host=\"web-02\"}")
	fmt.Println("    StreamID: S42")
	fmt.Println()
	fmt.Println("    Generation incremented: 42 → 43")
	fmt.Println("    (via invalidateStreamFilterCache callback)")
	fmt.Println()

	fmt.Println("T2: Generation = 43")
	fmt.Println("    Query Q2: {app=\"nginx\"}")
	fmt.Println()
	fmt.Println("    Cache key: [gen=43][partition][tenantIDs][filter]")
	fmt.Println("    Cache miss (no entry at gen=43)")
	fmt.Println("    Search index → find [S1, S5, S12, S42]")
	fmt.Println("    Cache store: [gen=43][...] → [S1, S5, S12, S42]")
	fmt.Println()

	fmt.Println("Result: S42 correctly included!")
	fmt.Println("        Cache key changed, forcing fresh lookup")
	fmt.Println()
}

func ShowImplementationDetails() {
	fmt.Println("=== Implementation Details ===")
	fmt.Println()

	fmt.Println("--- Generation Increment ---")
	fmt.Println()
	fmt.Println("func (idb *indexdb) invalidateStreamFilterCache() {")
	fmt.Println("    // Called when new index data is flushed")
	fmt.Println("    // This is coalesced and asynchronous")
	fmt.Println("    idb.filterStreamCacheGeneration.Add(1)")
	fmt.Println("}")
	fmt.Println()

	fmt.Println("--- Cache Key Construction ---")
	fmt.Println()
	fmt.Println("func marshalStreamFilterCacheKey(dst []byte, tenantIDs []TenantID, sf *StreamFilter) []byte {")
	fmt.Println("    dst = MarshalUint32(dst, generation)")
	fmt.Println("    dst = MarshalBytes(dst, partitionName)")
	fmt.Println("    dst = MarshalVarUint64(dst, len(tenantIDs))")
	fmt.Println("    for _, tenantID := range tenantIDs {")
	fmt.Println("        dst = tenantID.marshal(dst)")
	fmt.Println("    }")
	fmt.Println("    dst = sf.marshalForCacheKey(dst)")
	fmt.Println("    return dst")
	fmt.Println("}")
	fmt.Println()

	fmt.Println("--- Cache Key Format ---")
	fmt.Println()
	fmt.Println("[generation 4B][partitionLen 4B][partition][tenantCount][tenantIDs...][filterBytes]")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  Generation:   43 (0x0000002B)")
	fmt.Println("  Partition:    \"20260302\"")
	fmt.Println("  TenantIDs:    [{AccountID: 12345, ProjectID: 0}]")
	fmt.Println("  Filter:       {app=\"nginx\"}")
	fmt.Println()
	fmt.Println("Full key (hex):")
	fmt.Println("  00 00 00 2B                      // generation = 43")
	fmt.Println("  00 00 00 08 32 30 32 36 30 33 30 32  // partition = \"20260302\"")
	fmt.Println("  01                               // tenantCount = 1")
	fmt.Println("  00 00 00 00 00 00 30 39 00 00 00 00  // tenantID = 12345:0")
	fmt.Println("  [filter bytes]                   // {app=\"nginx\"}")
	fmt.Println()
}

func ShowLazyInvalidation() {
	fmt.Println("=== Lazy Invalidation Strategy ===")
	fmt.Println()

	fmt.Println("--- Why Lazy? ---")
	fmt.Println()
	fmt.Println("Eager invalidation (expensive):")
	fmt.Println("  - Track which cache entries are affected by new stream")
	fmt.Println("  - Find all matching filters")
	fmt.Println("  - Explicitly delete those entries")
	fmt.Println()
	fmt.Println("  Problems:")
	fmt.Println("    - Complex: need reverse index from stream → cached filters")
	fmt.Println("    - Expensive: O(cache size) scan")
	fmt.Println("    - Race conditions: new stream might match during scan")
	fmt.Println()

	fmt.Println("Lazy invalidation (simple):")
	fmt.Println("  - Increment generation counter")
	fmt.Println("  - Old entries become unreachable")
	fmt.Println("  - Natural cache eviction reclaims memory")
	fmt.Println()
	fmt.Println("  Benefits:")
	fmt.Println("    - O(1) invalidation")
	fmt.Println("    - No tracking needed")
	fmt.Println("    - No race conditions")
	fmt.Println()

	fmt.Println("--- Trade-offs ---")
	fmt.Println()
	fmt.Println("Lazy invalidation characteristics:")
	fmt.Println()
	fmt.Println("1. Coarse granularity")
	fmt.Println("   - All cached filters invalidated at once")
	fmt.Println("   - Even filters unrelated to new stream")
	fmt.Println()

	fmt.Println("2. Asynchronous")
	fmt.Println("   - Delay between stream registration and generation bump")
	fmt.Println("   - ~10 second cadence (coalesced callback)")
	fmt.Println()

	fmt.Println("3. Memory overhead")
	fmt.Println("   - Old cache entries linger until natural eviction")
	fmt.Println("   - Multiple generations may coexist briefly")
	fmt.Println()

	fmt.Println("4. Simplicity")
	fmt.Println("   - No complex invalidation logic")
	fmt.Println("   - Easy to reason about correctness")
	fmt.Println()

	fmt.Println("--- Generation Bump Timing ---")
	fmt.Println()
	fmt.Println("Stream registration flow:")
	fmt.Println()
	fmt.Println("1. mustRegisterStream()")
	fmt.Println("   → items added to mergeset")
	fmt.Println()
	fmt.Println("2. Data sits in rawItems buffer")
	fmt.Println("   (not yet visible to search)")
	fmt.Println()
	fmt.Println("3. Buffer flush (256 blocks or 1 second timer)")
	fmt.Println("   → items moved to inmemoryParts")
	fmt.Println("   (now visible to search)")
	fmt.Println()
	fmt.Println("4. needFlushCallbackCall set")
	fmt.Println("   → signals callback worker")
	fmt.Println()
	fmt.Println("5. Callback worker runs (coalesced, ~10s cadence)")
	fmt.Println("   → filterStreamCacheGeneration.Add(1)")
	fmt.Println()
	fmt.Println("Visibility timeline:")
	fmt.Println("  T0: Stream registered")
	fmt.Println("  T0+1s: Stream visible to search")
	fmt.Println("  T0+11s: Cache generation bumped")
	fmt.Println()
}

// ==================== Per-Partition Isolation ====================

func ShowPartitionIsolation() {
	fmt.Println("=== Per-Partition Isolation ===")
	fmt.Println()

	fmt.Println("Each partition has its own indexdb with its own generation:")
	fmt.Println()
	fmt.Println("Partition 20260301:")
	fmt.Println("  indexdb.filterStreamCacheGeneration = 100")
	fmt.Println()
	fmt.Println("Partition 20260302:")
	fmt.Println("  indexdb.filterStreamCacheGeneration = 43")
	fmt.Println()

	fmt.Println("Cache keys include partition name:")
	fmt.Println("  [gen=100][20260301][...]")
	fmt.Println("  [gen=43][20260302][...]")
	fmt.Println()

	fmt.Println("Result: No cross-partition cache collisions")
	fmt.Println("        Independent invalidation per partition")
	fmt.Println()
}

// ==================== Summary Table ====================

func ShowCacheSummary() {
	fmt.Println("=== Cache Correctness Summary ===")
	fmt.Println()

	fmt.Printf("%-25s %s\n", "Aspect", "Behavior")
	fmt.Println(strings.Repeat("-", 70))

	summary := []struct {
		aspect   string
		behavior string
	}{
		{"Generation location", "In cache key (not just metadata)"},
		{"Increment trigger", "New stream registration (flushed)"},
		{"Increment timing", "Asynchronous, coalesced (~10s)"},
		{"Invalidation scope", "All filters for partition"},
		{"Old entries", "Become unreachable, eventually evicted"},
		{"Per-partition", "Independent generation counters"},
	}

	for _, s := range summary {
		fmt.Printf("%-25s %s\n", s.aspect, s.behavior)
	}
}
