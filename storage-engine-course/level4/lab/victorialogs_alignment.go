package main

import (
	"fmt"
	"math"
	"strings"
)

// ==================== VictoriaLogs Alignment Lab ====================

// VictoriaLogs uses these specific constants:
const (
	victorialogsBitsPerItem = 16 // bloomFilterBitsPerItem
	victorialogsNumHashes   = 6  // bloomFilterHashesCount
)

// VictoriaLogsAnalysis compares our implementation with VictoriaLogs defaults
func VictoriaLogsAnalysis() {
	fmt.Println("=== Level 4 Lab 3: VictoriaLogs Alignment ===")
	fmt.Println()
	fmt.Println("VictoriaLogs uses specific bloom filter parameters:")
	fmt.Printf("  - Bits per item: %d\n", victorialogsBitsPerItem)
	fmt.Printf("  - Number of hashes: %d\n", victorialogsNumHashes)
	fmt.Println()

	// Analyze why these parameters were chosen
	fmt.Println("=== Why These Parameters? ===")
	fmt.Println()

	// 1. False positive rate analysis
	theoreticalFP := theoreticalFPRate(victorialogsBitsPerItem, victorialogsNumHashes)
	fmt.Printf("1. False Positive Rate\n")
	fmt.Printf("   Theoretical: %.2f%%\n", theoreticalFP*100)
	fmt.Printf("   Impact: ~1 in %.0f queries may needlessly scan a block\n", 1/theoreticalFP)
	fmt.Println()

	// 2. Memory overhead analysis
	fmt.Printf("2. Memory Overhead\n")
	fmt.Printf("   Per token: %d bits = %d bytes\n", victorialogsBitsPerItem, victorialogsBitsPerItem/8)
	fmt.Printf("   For 1000 unique tokens: %d bytes = %.1f KB\n",
		1000*victorialogsBitsPerItem/8,
		float64(1000*victorialogsBitsPerItem)/8/1024)
	fmt.Println()

	// 3. CPU cost analysis
	fmt.Printf("3. CPU Cost\n")
	fmt.Printf("   Hash computations per token: %d\n", victorialogsNumHashes)
	fmt.Printf("   Bit operations per check: %d reads\n", victorialogsNumHashes)
	fmt.Println()

	// Compare with alternatives
	fmt.Println("=== Comparison with Alternatives ===")
	fmt.Println()

	comparisons := []struct {
		name        string
		bitsPerItem int
		numHashes   int
		reason      string
	}{
		{"Minimal", 8, 4, "Low memory, high FP rate"},
		{"Conservative", 24, 8, "Low FP rate, high memory/CPU"},
		{"VictoriaLogs", 16, 6, "Balanced choice"},
		{"High-precision", 32, 10, "Very low FP, high overhead"},
	}

	fmt.Printf("%-15s %-10s %-10s %-12s %-12s %-25s\n",
		"Config", "Bits", "Hashes", "FP Rate", "Memory/1K", "Notes")
	fmt.Println(strings.Repeat("-", 95))

	n := 1000
	testSet := generateWords(n, "test")
	querySet := generateWords(n*10, "query")

	for _, c := range comparisons {
		bf := NewBloomFilterWithParams(n, c.bitsPerItem, c.numHashes)
		result := TestBloomFilter(bf, testSet, querySet)
		theoretical := theoreticalFPRate(c.bitsPerItem, c.numHashes)

		fmt.Printf("%-15s %-10d %-10d %-11.2f%% %-12s %-25s\n",
			c.name,
			c.bitsPerItem,
			c.numHashes,
			theoretical*100,
			formatBytes(result.MemoryBytes),
			c.reason)
	}
	fmt.Println()

	// Production considerations
	fmt.Println("=== Production Considerations ===")
	fmt.Println()
	fmt.Println("Why 16 bits/item and 6 hashes are production-friendly:")
	fmt.Println()

	fmt.Println("1. MEMORY: 2 bytes per token")
	fmt.Println("   - 1000 unique tokens = 2KB bloom filter")
	fmt.Println("   - Small enough to cache per block")
	fmt.Println("   - Much smaller than actual column data (typically 100KB+)")
	fmt.Println()

	fmt.Println("2. CPU: 6 hash computations")
	fmt.Println("   - Fast enough for high-throughput ingestion")
	fmt.Println("   - xxhash is extremely fast (~10 GB/s)")
	fmt.Println("   - 6 hashes = 6 memory reads per query check")
	fmt.Println()

	fmt.Println("3. FALSE POSITIVE RATE: ~1.5%")
	fmt.Println("   - 98.5% of irrelevant blocks are correctly skipped")
	fmt.Println("   - 1.5% false positives is acceptable (values still verified)")
	fmt.Println("   - Better to occasionally scan extra blocks than miss data")
	fmt.Println()

	fmt.Println("4. SCALABILITY:")
	fmt.Println("   - Works well from 100 to 100,000+ tokens per block")
	fmt.Println("   - Consistent performance across block sizes")
	fmt.Println("   - No tuning needed per deployment")
	fmt.Println()

	// Real-world scenario
	RealWorldScenario()
}

func RealWorldScenario() {
	fmt.Println("=== Real-World Scenario ===")
	fmt.Println()
	fmt.Println("Query: _msg:contains(\"error\")")
	fmt.Println("Data: 1000 blocks, each with 10,000 log entries")
	fmt.Println()

	// Simulate token distribution
	// In reality, "error" appears in only ~5% of blocks
	blocksWithError := 50     // 5%
	blocksWithoutError := 950 // 95%

	// With bloom filter
	bloomChecks := 1000
	bloomHits := blocksWithError + int(float64(blocksWithoutError)*0.015) // + 1.5% FP
	valueReads := bloomHits
	totalWorkWithBloom := bloomChecks + valueReads

	// Without bloom filter
	totalWorkWithoutBloom := 1000 // Must read all blocks

	fmt.Printf("Without bloom filter:\n")
	fmt.Printf("  Blocks to read: %d (100%% of blocks)\n", totalWorkWithoutBloom)
	fmt.Println()

	fmt.Printf("With bloom filter:\n")
	fmt.Printf("  Bloom checks: %d\n", bloomChecks)
	fmt.Printf("  Bloom hits: %d (50 true + ~14 false positives)\n", bloomHits)
	fmt.Printf("  Value reads: %d\n", valueReads)
	fmt.Printf("  Total work: %d\n", totalWorkWithBloom)
	fmt.Printf("  Speedup: %.1fx\n", float64(totalWorkWithoutBloom)/float64(totalWorkWithBloom))
	fmt.Println()

	// Memory cost
	tokensPerBlock := 1000
	bloomSizePerBlock := tokensPerBlock * victorialogsBitsPerItem / 8
	totalBloomMemory := bloomSizePerBlock * 1000

	fmt.Printf("Memory cost:\n")
	fmt.Printf("  Bloom filter per block: %s\n", formatBytes(bloomSizePerBlock))
	fmt.Printf("  Total bloom memory: %s\n", formatBytes(totalBloomMemory))
	fmt.Printf("  Compared to actual data (~100KB/block): %.1f%% overhead\n",
		float64(bloomSizePerBlock)/100000*100)
	fmt.Println()

	// When bloom becomes counter-productive
	WhenBloomIsCounterProductive()
}

func WhenBloomIsCounterProductive() {
	fmt.Println("=== When Bloom Checks Are Counter-Productive ===")
	fmt.Println()

	fmt.Println("VictoriaLogs skips bloom checks in these cases:")
	fmt.Println()

	fmt.Println("1. DICTIONARY-ENCODED COLUMNS")
	fmt.Println("   - Dict already stores all unique values in header")
	fmt.Println("   - Direct lookup is O(1), no false positives")
	fmt.Println("   - Bloom filter would add overhead with no benefit")
	fmt.Println()

	fmt.Println("2. LARGE IN(...) VALUE LISTS")
	fmt.Println("   - Query: status:in(\"200\", \"201\", ..., \"504\")")
	fmt.Println("   - 100+ values = 100+ bloom probes per block")
	fmt.Println("   - Cost of probing > cost of reading values")
	fmt.Println("   - VictoriaLogs skips bloom when token sets > 1000")
	fmt.Println()

	fmt.Println("3. EMPTY TOKEN SETS")
	fmt.Println("   - Query for empty string or non-tokenizable pattern")
	fmt.Println("   - No tokens = no bits to check")
	fmt.Println("   - Bloom returns 'maybe present' unconditionally")
	fmt.Println()

	// Demonstrate large IN list scenario
	fmt.Println("Example: Large IN list comparison")
	fmt.Println()

	tokenSets := []int{10, 50, 100, 500, 1000, 2000, 5000}
	fmt.Printf("%-15s %-20s %-20s %-15s\n",
		"Token Sets", "Bloom Cost (probes)", "Scan Cost (rows)", "Recommendation")
	fmt.Println(strings.Repeat("-", 75))

	rowsPerBlock := 10000
	for _, numSets := range tokenSets {
		bloomCost := numSets * 6 // 6 hashes per check
		scanCost := rowsPerBlock

		var recommendation string
		if bloomCost < scanCost/10 {
			recommendation = "Use bloom"
		} else if bloomCost < scanCost {
			recommendation = "Marginal benefit"
		} else {
			recommendation = "Skip bloom, scan"
		}

		fmt.Printf("%-15d %-20d %-20d %-15s\n",
			numSets, bloomCost, scanCost, recommendation)
	}
	fmt.Println()
}

// ==================== Tokenization Analysis ====================

func TokenizationAnalysis() {
	fmt.Println("=== Tokenization in VictoriaLogs ===")
	fmt.Println()

	examples := []struct {
		message string
		query   string
	}{
		{"connection timeout to 10.0.1.5", "timeout"},
		{"User john@example.com logged in", "john"},
		{"Error: file not found at /var/log/app.log", "Error"},
		{"Request took 1500ms from 192.168.1.1", "192"},
	}

	fmt.Println("Why tokenize before bloom insertion:")
	fmt.Println()
	fmt.Println("1. MATCHING FLEXIBILITY")
	fmt.Println("   - Full string: only matches exact string")
	fmt.Println("   - Tokenized: matches any word in the string")
	fmt.Println()

	fmt.Println("2. STORAGE EFFICIENCY")
	fmt.Println("   - Each unique token hashed once")
	fmt.Println("   - Repeated words don't waste bloom capacity")
	fmt.Println()

	fmt.Println("3. QUERY PATTERNS")
	fmt.Println("   - Most queries search for keywords, not full messages")
	fmt.Println("   - '_msg:contains(\"error\")' needs 'error' token, not full message")
	fmt.Println()

	fmt.Println("Examples:")
	fmt.Println()

	for _, ex := range examples {
		tokens := Tokenize(ex.message)
		bf := NewBloomFilterWithParams(len(tokens), 16, 6)
		for _, t := range tokens {
			bf.Add(t)
		}

		fmt.Printf("Message: %q\n", ex.message)
		fmt.Printf("Tokens: %v\n", tokens)
		fmt.Printf("Query '%s': %v\n", ex.query, bf.Contains(ex.query))
		fmt.Println()
	}

	// Deduplication benefit
	fmt.Println("Token Deduplication Benefit:")
	fmt.Println()

	message := "error error error timeout timeout error"
	tokens := Tokenize(message)
	uniqueTokens := make(map[string]bool)
	for _, t := range tokens {
		uniqueTokens[t] = true
	}

	fmt.Printf("Message: %q\n", message)
	fmt.Printf("Total tokens: %d\n", len(tokens))
	fmt.Printf("Unique tokens: %d\n", len(uniqueTokens))
	fmt.Printf("Bloom capacity needed: %d (not %d)\n",
		len(uniqueTokens), len(tokens))
	fmt.Println()
}

// ==================== Best Practices ====================

func BloomBestPractices() {
	fmt.Println("=== Bloom Filter Best Practices ===")
	fmt.Println()

	fmt.Println("1. PARAMETER SELECTION")
	fmt.Println("   - Use VictoriaLogs defaults (16 bits/item, 6 hashes)")
	fmt.Println("   - Only tune if you have specific requirements")
	fmt.Println("   - Profile before optimizing")
	fmt.Println()

	fmt.Println("2. UNDERSTAND THE TRADEOFFS")
	fmt.Println("   - More bits = less FP, more memory")
	fmt.Println("   - More hashes = less FP, more CPU")
	fmt.Println("   - Diminishing returns after optimal point")
	fmt.Println()

	fmt.Println("3. WHEN TO SKIP BLOOM")
	fmt.Println("   - Dictionary-encoded columns")
	fmt.Println("   - Very large IN(...) lists")
	fmt.Println("   - Columns with high selectivity anyway")
	fmt.Println()

	fmt.Println("4. TESTING")
	fmt.Println("   - Verify zero false negatives (critical!)")
	fmt.Println("   - Measure actual FP rate in production")
	fmt.Println("   - Compare query performance with/without bloom")
	fmt.Println()

	// Optimal parameters calculation
	fmt.Println("=== Calculating Optimal Parameters ===")
	fmt.Println()

	fpTargets := []float64{0.001, 0.005, 0.01, 0.02, 0.05}
	n := 1000

	fmt.Printf("For %d items:\n", n)
	fmt.Println()
	fmt.Printf("%-15s %-15s %-15s %-15s\n",
		"Target FP", "Optimal m/n", "Optimal k", "Memory")
	fmt.Println(strings.Repeat("-", 65))

	for _, fp := range fpTargets {
		// Optimal m/n = -n*ln(p) / (ln(2)^2)
		mn := -math.Log(fp) / (math.Ln2 * math.Ln2)
		// Optimal k = (m/n)*ln(2)
		k := mn * math.Ln2

		memory := int(mn) * n / 8

		fmt.Printf("%-14.1f%% %-15.1f %-15.1f %-15s\n",
			fp*100, mn, k, formatBytes(memory))
	}
}
