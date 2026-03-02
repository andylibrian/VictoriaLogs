package main

import (
	"fmt"
	"math"
)

// ==================== Bloom Filter Implementation ====================

// BloomFilter is a probabilistic data structure for membership testing.
// It can tell if an item is DEFINITELY NOT in the set, or MIGHT be in the set.
type BloomFilter struct {
	bits    []uint64 // Bit array stored as 64-bit words
	numBits int      // Total number of bits
	k       int      // Number of hash functions
}

// NewBloomFilter creates a new bloom filter with the given parameters.
// n: expected number of items
// fpRate: desired false positive rate (e.g., 0.01 for 1%)
func NewBloomFilter(n int, fpRate float64) *BloomFilter {
	// Calculate optimal number of bits
	// m = -n * ln(p) / (ln(2)^2)
	m := int(float64(n) * -math.Log(fpRate) / (math.Ln2 * math.Ln2))

	// Calculate optimal number of hash functions
	// k = (m/n) * ln(2)
	k := int(float64(m) / float64(n) * math.Ln2)
	if k < 1 {
		k = 1
	}

	// Round up to 64-bit word boundary
	numWords := (m + 63) / 64

	return &BloomFilter{
		bits:    make([]uint64, numWords),
		numBits: numWords * 64,
		k:       k,
	}
}

// NewBloomFilterWithParams creates a bloom filter with explicit parameters.
// This matches VictoriaLogs' approach of using fixed parameters.
func NewBloomFilterWithParams(n int, bitsPerItem int, numHashes int) *BloomFilter {
	m := n * bitsPerItem
	numWords := (m + 63) / 64

	return &BloomFilter{
		bits:    make([]uint64, numWords),
		numBits: numWords * 64,
		k:       numHashes,
	}
}

// Add inserts an item into the bloom filter.
func (bf *BloomFilter) Add(item string) {
	hashes := bf.getHashes(item)
	for _, h := range hashes {
		bf.setBit(h)
	}
}

// Contains checks if an item might be in the set.
// Returns true if the item MIGHT be present (may be false positive).
// Returns false if the item is DEFINITELY NOT present (no false negatives).
func (bf *BloomFilter) Contains(item string) bool {
	hashes := bf.getHashes(item)
	for _, h := range hashes {
		if !bf.getBit(h) {
			return false // Definitely not present
		}
	}
	return true // Might be present (could be false positive)
}

// getHashes generates k hash values for an item.
// This mimics VictoriaLogs' approach using xxhash with incrementing seed.
func (bf *BloomFilter) getHashes(item string) []uint64 {
	hashes := make([]uint64, bf.k)

	// Start with base hash
	h := hashString(item)

	for i := 0; i < bf.k; i++ {
		// Use hash of (base_hash + i) to generate k different hashes
		// This is similar to VictoriaLogs' approach
		hashes[i] = hashUint64(h + uint64(i))
	}

	return hashes
}

// setBit sets a bit at the given position.
func (bf *BloomFilter) setBit(pos uint64) {
	idx := pos % uint64(bf.numBits)
	wordIdx := idx / 64
	bitIdx := idx % 64
	bf.bits[wordIdx] |= (1 << bitIdx)
}

// getBit checks if a bit is set at the given position.
func (bf *BloomFilter) getBit(pos uint64) bool {
	idx := pos % uint64(bf.numBits)
	wordIdx := idx / 64
	bitIdx := idx % 64
	return (bf.bits[wordIdx] & (1 << bitIdx)) != 0
}

// Size returns the size of the bloom filter in bytes.
func (bf *BloomFilter) Size() int {
	return len(bf.bits) * 8
}

// ==================== Hash Functions ====================

// hashString hashes a string to a uint64 using FNV-1a.
func hashString(s string) uint64 {
	h := uint64(14695981039346656037) // FNV offset basis
	for _, c := range s {
		h ^= uint64(c)
		h *= 1099511628211 // FNV prime
	}
	return h
}

// hashUint64 hashes a uint64 to another uint64.
func hashUint64(h uint64) uint64 {
	// Simple mixing function
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// ==================== Tokenizer ====================

// Tokenize splits a string into tokens at non-alphanumeric boundaries.
// This mimics VictoriaLogs' tokenization for bloom filter insertion.
func Tokenize(s string) []string {
	tokens := []string{}
	start := -1

	for i := 0; i < len(s); i++ {
		c := s[i]
		isTokenChar := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')

		if isTokenChar {
			if start == -1 {
				start = i
			}
		} else {
			if start != -1 {
				tokens = append(tokens, s[start:i])
				start = -1
			}
		}
	}

	if start != -1 {
		tokens = append(tokens, s[start:])
	}

	return tokens
}

// ==================== Testing Utilities ====================

// TestResult holds the results of a bloom filter test.
type TestResult struct {
	TruePositives  int
	FalsePositives int
	TrueNegatives  int
	FalseNegatives int // Should always be 0!
	MemoryBytes    int
	FPRate         float64
}

// TestBloomFilter tests a bloom filter with given test and query sets.
func TestBloomFilter(bf *BloomFilter, testSet, querySet []string) TestResult {
	result := TestResult{
		MemoryBytes: bf.Size(),
	}

	testSetMap := make(map[string]bool)
	for _, item := range testSet {
		testSetMap[item] = true
		bf.Add(item)
	}

	for _, item := range querySet {
		contains := bf.Contains(item)
		inTestSet := testSetMap[item]

		if contains && inTestSet {
			result.TruePositives++
		} else if contains && !inTestSet {
			result.FalsePositives++
		} else if !contains && !inTestSet {
			result.TrueNegatives++
		} else {
			// !contains && inTestSet - THIS SHOULD NEVER HAPPEN
			result.FalseNegatives++
		}
	}

	totalNegatives := result.FalsePositives + result.TrueNegatives
	if totalNegatives > 0 {
		result.FPRate = float64(result.FalsePositives) / float64(totalNegatives)
	}

	return result
}

// ==================== Demo ====================

func BloomImplementationDemo() {
	fmt.Println("=== Level 4 Lab 1: Minimal Bloom Implementation ===")
	fmt.Println()
	fmt.Println("This program demonstrates bloom filter behavior:")
	fmt.Println("- No false negatives (guaranteed)")
	fmt.Println("- Measurable false positives (probabilistic)")
	fmt.Println()

	// Test 1: Basic functionality
	fmt.Println("--- Test 1: Basic Functionality ---")
	fmt.Println()

	bf1 := NewBloomFilter(100, 0.01) // 100 items, 1% FP rate
	fmt.Printf("Created bloom filter for 100 items with 1%% target FP rate\n")
	fmt.Printf("Memory usage: %d bytes\n", bf1.Size())
	fmt.Printf("Number of hash functions: %d\n", bf1.k)
	fmt.Println()

	// Add some items
	words := []string{"error", "warning", "info", "debug", "trace"}
	for _, word := range words {
		bf1.Add(word)
	}

	// Test membership
	fmt.Println("Testing membership:")
	for _, word := range words {
		if bf1.Contains(word) {
			fmt.Printf("  %q: PRESENT (expected)\n", word)
		} else {
			fmt.Printf("  %q: NOT PRESENT (BUG - false negative!)\n", word)
		}
	}

	// Test non-members
	nonWords := []string{"fatal", "panic", "critical"}
	fmt.Println()
	fmt.Println("Testing non-members (may have false positives):")
	for _, word := range nonWords {
		if bf1.Contains(word) {
			fmt.Printf("  %q: PRESENT (false positive)\n", word)
		} else {
			fmt.Printf("  %q: NOT PRESENT (correct)\n", word)
		}
	}
	fmt.Println()

	// Test 2: False positive measurement
	fmt.Println("--- Test 2: False Positive Measurement ---")
	fmt.Println()

	n := 1000
	testSet := generateWords(n, "word")
	querySet := generateWords(n*3, "query") // More diverse query set

	bf2 := NewBloomFilter(n, 0.01)
	result := TestBloomFilter(bf2, testSet, querySet)

	fmt.Printf("Test set size: %d items\n", n)
	fmt.Printf("Query set size: %d items\n", len(querySet))
	fmt.Printf("Memory usage: %d bytes\n", result.MemoryBytes)
	fmt.Println()
	fmt.Println("Results:")
	fmt.Printf("  True Positives:  %d\n", result.TruePositives)
	fmt.Printf("  False Positives: %d\n", result.FalsePositives)
	fmt.Printf("  True Negatives:  %d\n", result.TrueNegatives)
	fmt.Printf("  False Negatives: %d (MUST be 0!)\n", result.FalseNegatives)
	fmt.Printf("  Actual FP Rate:  %.2f%% (target: 1%%)\n", result.FPRate*100)
	fmt.Println()

	if result.FalseNegatives > 0 {
		fmt.Println("ERROR: Bloom filter has false negatives - this is a bug!")
	} else {
		fmt.Println("SUCCESS: No false negatives (as expected)")
	}
	fmt.Println()

	// Test 3: Tokenization
	fmt.Println("--- Test 3: Tokenization (VictoriaLogs-style) ---")
	fmt.Println()

	message := "connection timeout to 10.0.1.5 from host web-01"
	tokens := Tokenize(message)

	fmt.Printf("Message: %q\n", message)
	fmt.Printf("Tokens: %v\n", tokens)
	fmt.Println()

	bf3 := NewBloomFilterWithParams(len(tokens), 16, 6) // VictoriaLogs defaults
	for _, token := range tokens {
		bf3.Add(token)
	}

	// Query for tokens
	queries := []string{"timeout", "connection", "web", "10", "error"}
	fmt.Println("Query results:")
	for _, q := range queries {
		if bf3.Contains(q) {
			fmt.Printf("  %q: might be present\n", q)
		} else {
			fmt.Printf("  %q: definitely not present\n", q)
		}
	}
	fmt.Println()

	// Test 4: No false negatives verification
	fmt.Println("--- Test 4: Verifying No False Negatives ---")
	fmt.Println()

	// Create many bloom filters and test extensively
	totalTests := 0
	falseNegatives := 0

	for i := 0; i < 100; i++ {
		bf := NewBloomFilter(100, 0.01)
		testItems := generateWords(100, fmt.Sprintf("test%d", i))

		for _, item := range testItems {
			bf.Add(item)
		}

		// Every item we added MUST be found
		for _, item := range testItems {
			totalTests++
			if !bf.Contains(item) {
				falseNegatives++
			}
		}
	}

	fmt.Printf("Total tests: %d\n", totalTests)
	fmt.Printf("False negatives: %d\n", falseNegatives)
	if falseNegatives == 0 {
		fmt.Println("VERIFIED: Zero false negatives across all tests")
	} else {
		fmt.Printf("BUG: Found %d false negatives!\n", falseNegatives)
	}
}

func generateWords(n int, prefix string) []string {
	words := make([]string, n)
	for i := 0; i < n; i++ {
		words[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return words
}
