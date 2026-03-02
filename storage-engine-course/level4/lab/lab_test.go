package main

import (
	"testing"
)

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	// This is the most critical test - bloom filters must NEVER have false negatives
	n := 1000
	bf := NewBloomFilter(n, 0.01)

	items := generateWords(n, "test")

	// Add all items
	for _, item := range items {
		bf.Add(item)
	}

	// Every item MUST be found
	for _, item := range items {
		if !bf.Contains(item) {
			t.Errorf("FALSE NEGATIVE: item %q was added but not found", item)
		}
	}
}

func TestBloomFilterFalsePositivesExist(t *testing.T) {
	// Bloom filters should have some false positives (that's expected)
	n := 100
	bf := NewBloomFilter(n, 0.1) // High FP rate for testing

	testSet := generateWords(n, "test")
	querySet := generateWords(n*10, "query")

	result := TestBloomFilter(bf, testSet, querySet)

	// Should have some false positives
	if result.FalsePositives == 0 {
		t.Log("Warning: no false positives detected (unusual for bloom filter)")
	}

	// Must have zero false negatives
	if result.FalseNegatives != 0 {
		t.Errorf("FALSE NEGATIVES detected: %d (must be 0)", result.FalseNegatives)
	}
}

func TestBloomFilterSize(t *testing.T) {
	tests := []struct {
		n           int
		fpRate      float64
		expectedMin int // Minimum expected size
		expectedMax int // Maximum expected size
	}{
		{100, 0.01, 100, 500},
		{1000, 0.01, 1000, 5000},
		{10000, 0.01, 10000, 50000},
	}

	for _, tt := range tests {
		bf := NewBloomFilter(tt.n, tt.fpRate)
		size := bf.Size()

		if size < tt.expectedMin || size > tt.expectedMax {
			t.Errorf("BloomFilter(%d, %.2f) size = %d, expected [%d, %d]",
				tt.n, tt.fpRate, size, tt.expectedMin, tt.expectedMax)
		}
	}
}

func TestBloomFilterWithParams(t *testing.T) {
	n := 100
	bitsPerItem := 16
	numHashes := 6

	bf := NewBloomFilterWithParams(n, bitsPerItem, numHashes)

	// Check parameters
	if bf.k != numHashes {
		t.Errorf("Expected %d hashes, got %d", numHashes, bf.k)
	}

	// Expected size: n * bitsPerItem / 8 bytes, rounded up to 64-bit words
	expectedBits := n * bitsPerItem
	expectedWords := (expectedBits + 63) / 64
	expectedSize := expectedWords * 8

	if bf.Size() != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, bf.Size())
	}
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"hello world", []string{"hello", "world"}},
		{"connection timeout to 10.0.1.5", []string{"connection", "timeout", "to", "10", "0", "1", "5"}},
		{"error!", []string{"error"}},
		{"  spaces  ", []string{"spaces"}},
		{"test123", []string{"test123"}},
		{"", []string{}},
		{"   ", []string{}},
	}

	for _, tt := range tests {
		result := Tokenize(tt.input)

		if len(result) != len(tt.expected) {
			t.Errorf("Tokenize(%q) = %v, expected %v", tt.input, result, tt.expected)
			continue
		}

		for i, token := range result {
			if token != tt.expected[i] {
				t.Errorf("Tokenize(%q)[%d] = %q, expected %q", tt.input, i, token, tt.expected[i])
			}
		}
	}
}

func TestBloomFilterWithTokens(t *testing.T) {
	message := "connection timeout to 10.0.1.5"
	tokens := Tokenize(message)

	bf := NewBloomFilterWithParams(len(tokens), 16, 6)
	for _, token := range tokens {
		bf.Add(token)
	}

	// All tokens should be found
	for _, token := range tokens {
		if !bf.Contains(token) {
			t.Errorf("Token %q not found in bloom filter", token)
		}
	}

	// Non-token should not be found (may have false positives, but unlikely for random strings)
	if bf.Contains("xyzzy123nonexistent") {
		t.Log("False positive for 'xyzzy123nonexistent' (acceptable)")
	}
}

func TestTheoreticalFPRate(t *testing.T) {
	// Test that theoretical FP rate calculation is reasonable
	tests := []struct {
		bitsPerItem int
		numHashes   int
		maxFP       float64 // Maximum expected FP rate
	}{
		{8, 4, 0.10},   // Low bits, few hashes -> high FP
		{16, 6, 0.02},  // VictoriaLogs default -> ~1.5% FP
		{24, 8, 0.005}, // High bits, many hashes -> low FP
	}

	for _, tt := range tests {
		fp := theoreticalFPRate(tt.bitsPerItem, tt.numHashes)
		if fp > tt.maxFP {
			t.Errorf("theoreticalFPRate(%d, %d) = %.4f, expected <= %.4f",
				tt.bitsPerItem, tt.numHashes, fp, tt.maxFP)
		}
	}
}

func TestParameterSweepResult(t *testing.T) {
	params := ParameterSet{BitsPerItem: 16, NumHashes: 6}
	n := 100

	testSet := generateWords(n, "test")
	querySet := generateWords(n*5, "query")

	result := testParameterSet(params, n, testSet, querySet)

	// Check that results are reasonable
	if result.MemoryBytes <= 0 {
		t.Error("Memory bytes should be positive")
	}

	if result.FPRate < 0 || result.FPRate > 1 {
		t.Errorf("FP rate should be between 0 and 1, got %f", result.FPRate)
	}

	if result.OpsPerSecond <= 0 {
		t.Error("Ops per second should be positive")
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(10000, 0.01)
	items := generateWords(10000, "bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(items[i%len(items)])
	}
}

func BenchmarkBloomFilterContains(b *testing.B) {
	bf := NewBloomFilter(10000, 0.01)
	items := generateWords(10000, "bench")

	for _, item := range items {
		bf.Add(item)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Contains(items[i%len(items)])
	}
}

func BenchmarkBloomFilterContainsMiss(b *testing.B) {
	bf := NewBloomFilter(10000, 0.01)
	items := generateWords(10000, "bench")
	missItems := generateWords(10000, "miss")

	for _, item := range items {
		bf.Add(item)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Contains(missItems[i%len(missItems)])
	}
}
