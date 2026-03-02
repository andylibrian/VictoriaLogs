package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
)

// ==================== Encoding Impact Analysis ====================

// EncodingStats represents the storage and query characteristics of an encoding
type EncodingStats struct {
	ValueType        valueType
	StorageBytes     int
	BloomFilterBytes int
	CanRangeFilter   bool
	CanDictLookup    bool
	CompressionRatio float64
}

// ColumnSimulator simulates different column types and their encoding
type ColumnSimulator struct {
	Name   string
	Values []string
}

// AnalyzeEncoding returns statistics for how a column would be encoded
func (cs *ColumnSimulator) AnalyzeEncoding() EncodingStats {
	stats := EncodingStats{}
	vt := detectValueType(cs.Values)
	stats.ValueType = vt

	// Calculate storage size
	switch vt {
	case valueTypeDict:
		stats.StorageBytes = cs.estimateDictSize()
		stats.BloomFilterBytes = 0 // Dict columns skip bloom filter
		stats.CanDictLookup = true
		stats.CompressionRatio = cs.dictCompressionRatio()

	case valueTypeUint8:
		stats.StorageBytes = len(cs.Values) * 1
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	case valueTypeUint16:
		stats.StorageBytes = len(cs.Values) * 2
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	case valueTypeUint32:
		stats.StorageBytes = len(cs.Values) * 4
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	case valueTypeUint64:
		stats.StorageBytes = len(cs.Values) * 8
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	case valueTypeInt64:
		stats.StorageBytes = len(cs.Values) * 8
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	case valueTypeFloat64:
		stats.StorageBytes = len(cs.Values) * 8
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	case valueTypeIPv4:
		stats.StorageBytes = len(cs.Values) * 4
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	case valueTypeTimestampISO8601:
		stats.StorageBytes = len(cs.Values) * 8
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CanRangeFilter = true

	default: // valueTypeString
		stats.StorageBytes = cs.estimateStringSize()
		stats.BloomFilterBytes = cs.estimateBloomSize()
		stats.CompressionRatio = 1.0 // No compression
	}

	return stats
}

func (cs *ColumnSimulator) estimateDictSize() int {
	uniqueValues := make(map[string]bool)
	for _, v := range cs.Values {
		uniqueValues[v] = true
	}

	// Dict encoding: 1 byte per value for index + unique values stored once
	dictSize := 0
	for v := range uniqueValues {
		dictSize += len(v) + 2 // value + length prefix
	}
	return len(cs.Values) + dictSize // 1 byte per row + dictionary
}

func (cs *ColumnSimulator) estimateStringSize() int {
	total := 0
	for _, v := range cs.Values {
		total += len(v) + 2 // value + length prefix
	}
	return total
}

func (cs *ColumnSimulator) estimateBloomSize() int {
	// Bloom filter size is roughly proportional to unique values
	// VictoriaLogs uses ~10 bits per item for ~1% false positive rate
	uniqueValues := make(map[string]bool)
	for _, v := range cs.Values {
		uniqueValues[v] = true
	}
	bits := len(uniqueValues) * 10
	return (bits + 7) / 8 // Convert to bytes
}

func (cs *ColumnSimulator) dictCompressionRatio() float64 {
	stringSize := cs.estimateStringSize()
	dictSize := cs.estimateDictSize()
	if stringSize == 0 {
		return 1.0
	}
	return float64(dictSize) / float64(stringSize)
}

// ==================== Scenario Generators ====================

func generateLowCardinalityColumn(numRows int, cardinality int) []string {
	values := make([]string, numRows)
	options := []string{"error", "warn", "info", "debug", "trace"}
	for i := 0; i < numRows; i++ {
		values[i] = options[i%cardinality]
	}
	return values
}

func generateHighCardinalityColumn(numRows int) []string {
	values := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		// Generate unique-ish messages
		values[i] = fmt.Sprintf("User %d performed action %s at %d",
			rand.Intn(10000),
			[]string{"login", "logout", "purchase", "view", "click"}[rand.Intn(5)],
			rand.Intn(1000000))
	}
	return values
}

func generateNumericColumn(numRows int, isFloat bool) []string {
	values := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		if isFloat {
			values[i] = fmt.Sprintf("%.6f", rand.Float64()*1000)
		} else {
			values[i] = fmt.Sprintf("%d", rand.Intn(100000))
		}
	}
	return values
}

func generateIPColumn(numRows int) []string {
	values := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		values[i] = fmt.Sprintf("%d.%d.%d.%d",
			rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
	}
	return values
}

func generateTimestampColumn(numRows int) []string {
	values := make([]string, numRows)
	baseTime := int64(1709337600) // 2024-03-02
	for i := 0; i < numRows; i++ {
		t := baseTime + int64(i)
		values[i] = fmt.Sprintf("2024-03-02T%02d:%02d:%02dZ",
			(t/3600)%24, (t/60)%60, t%60)
	}
	return values
}

// ==================== Demo ====================

func EncodingAnalysisDemo() {
	fmt.Println("=== Level 3 Lab 2: Encoding Impact Analysis ===")
	fmt.Println()
	fmt.Println("This program compares storage and query characteristics")
	fmt.Println("for different column types and encoding strategies.")
	fmt.Println()

	numRows := 10000
	fmt.Printf("Dataset size: %d rows\n\n", numRows)

	// Scenario 1: Low cardinality (dict encoding)
	fmt.Println("=== Scenario 1: Low Cardinality Column ===")
	fmt.Println()
	fmt.Println("Column: 'level' - log levels with only a few distinct values")
	fmt.Println()

	lowCard := &ColumnSimulator{
		Name:   "level",
		Values: generateLowCardinalityColumn(numRows, 4),
	}
	stats1 := lowCard.AnalyzeEncoding()

	printColumnStats(lowCard, stats1)

	fmt.Println("Query Performance Implications:")
	fmt.Println("  ✓ Dict lookup: O(1) to check if value exists")
	fmt.Println("  ✓ No bloom filter needed: dictionary provides exact membership")
	fmt.Println("  ✓ Filter: level='error' → direct dict ID comparison")
	fmt.Println("  ✗ No range filtering: cannot do level > 'debug'")
	fmt.Println()

	// Scenario 2: High cardinality free text (string encoding)
	fmt.Println("=== Scenario 2: High Cardinality Free-Text Column ===")
	fmt.Println()
	fmt.Println("Column: 'message' - unique log messages")
	fmt.Println()

	highCard := &ColumnSimulator{
		Name:   "message",
		Values: generateHighCardinalityColumn(numRows),
	}
	stats2 := highCard.AnalyzeEncoding()

	printColumnStats(highCard, stats2)

	fmt.Println("Query Performance Implications:")
	fmt.Println("  ✗ No dict lookup: every value is unique")
	fmt.Println("  ✓ Bloom filter useful: can quickly reject non-matching blocks")
	fmt.Println("  ✗ String comparison: must scan and compare each value")
	fmt.Println("  ✗ No range filtering: string ranges are rarely useful")
	fmt.Println()

	// Scenario 3: Numeric column (int encoding)
	fmt.Println("=== Scenario 3: Numeric Column (Integer) ===")
	fmt.Println()
	fmt.Println("Column: 'status_code' - HTTP status codes")
	fmt.Println()

	numericInt := &ColumnSimulator{
		Name:   "status_code",
		Values: generateNumericColumn(numRows, false),
	}
	stats3 := numericInt.AnalyzeEncoding()

	printColumnStats(numericInt, stats3)

	fmt.Println("Query Performance Implications:")
	fmt.Println("  ✓ Range filtering: status >= 400 AND status < 500")
	fmt.Println("  ✓ Bloom filter: can skip blocks without target value")
	fmt.Println("  ✓ Compact storage: 8 bytes per value vs variable-length strings")
	fmt.Println()

	// Scenario 4: Float column
	fmt.Println("=== Scenario 4: Numeric Column (Float) ===")
	fmt.Println()
	fmt.Println("Column: 'latency_seconds' - request latencies")
	fmt.Println()

	numericFloat := &ColumnSimulator{
		Name:   "latency_seconds",
		Values: generateNumericColumn(numRows, true),
	}
	stats4 := numericFloat.AnalyzeEncoding()

	printColumnStats(numericFloat, stats4)

	fmt.Println("Query Performance Implications:")
	fmt.Println("  ✓ Range filtering: latency > 0.5")
	fmt.Println("  ✓ Bloom filter available")
	fmt.Println("  ✓ Compact storage: 8 bytes per value")
	fmt.Println()

	// Scenario 5: IPv4 column
	fmt.Println("=== Scenario 5: IPv4 Column ===")
	fmt.Println()
	fmt.Println("Column: 'client_ip' - IP addresses")
	fmt.Println()

	ipCol := &ColumnSimulator{
		Name:   "client_ip",
		Values: generateIPColumn(numRows),
	}
	stats5 := ipCol.AnalyzeEncoding()

	printColumnStats(ipCol, stats5)

	fmt.Println("Query Performance Implications:")
	fmt.Println("  ✓ Range filtering: can filter by IP ranges")
	fmt.Println("  ✓ Compact storage: 4 bytes per IPv4")
	fmt.Println("  ✓ Bloom filter available")
	fmt.Println()

	// Comparison summary
	fmt.Println("=== Encoding Comparison Summary ===")
	fmt.Println()
	fmt.Printf("%-20s %-10s %12s %12s %10s\n",
		"Column", "Type", "Storage", "Bloom", "Ratio")
	fmt.Println(strings.Repeat("-", 70))

	cols := []struct {
		name  string
		stats EncodingStats
		raw   int
	}{
		{"level (low card)", stats1, estimateRawSize(lowCard.Values)},
		{"message (high card)", stats2, estimateRawSize(highCard.Values)},
		{"status_code (int)", stats3, estimateRawSize(numericInt.Values)},
		{"latency (float)", stats4, estimateRawSize(numericFloat.Values)},
		{"client_ip (ipv4)", stats5, estimateRawSize(ipCol.Values)},
	}

	for _, c := range cols {
		ratio := float64(c.stats.StorageBytes+c.stats.BloomFilterBytes) / float64(c.raw)
		fmt.Printf("%-20s %-10s %12d %12d %10.2f%%\n",
			c.name,
			c.stats.ValueType,
			c.stats.StorageBytes,
			c.stats.BloomFilterBytes,
			ratio*100)
	}

	fmt.Println()
	fmt.Println("Key Observations:")
	fmt.Println()
	fmt.Println("1. DICT encoding is most efficient for low-cardinality columns")
	fmt.Println("   - Small storage footprint")
	fmt.Println("   - No bloom filter needed (dict provides exact membership)")
	fmt.Println("   - Fastest query path for equality filters")
	fmt.Println()
	fmt.Println("2. STRING encoding is least efficient but most flexible")
	fmt.Println("   - Large storage footprint")
	fmt.Println("   - Bloom filter helps skip irrelevant blocks")
	fmt.Println("   - Must scan values for filtering")
	fmt.Println()
	fmt.Println("3. Numeric/IPv4 encodings offer good balance")
	fmt.Println("   - Compact fixed-size storage")
	fmt.Println("   - Range filtering support")
	fmt.Println("   - Bloom filter for exact matches")
	fmt.Println()
}

func printColumnStats(col *ColumnSimulator, stats EncodingStats) {
	uniqueCount := len(makeUniqueSet(col.Values))
	rawSize := estimateRawSize(col.Values)

	fmt.Printf("Value type:        %s\n", stats.ValueType)
	fmt.Printf("Unique values:     %d / %d (%.1f%%)\n",
		uniqueCount, len(col.Values),
		float64(uniqueCount)/float64(len(col.Values))*100)
	fmt.Printf("Raw size:          %d bytes\n", rawSize)
	fmt.Printf("Encoded size:      %d bytes\n", stats.StorageBytes)
	fmt.Printf("Bloom filter:      %d bytes\n", stats.BloomFilterBytes)
	fmt.Printf("Total size:        %d bytes\n", stats.StorageBytes+stats.BloomFilterBytes)
	fmt.Printf("Compression ratio: %.2f%%\n",
		float64(stats.StorageBytes+stats.BloomFilterBytes)/float64(rawSize)*100)
	fmt.Println()
}

func makeUniqueSet(values []string) map[string]bool {
	set := make(map[string]bool)
	for _, v := range values {
		set[v] = true
	}
	return set
}

func estimateRawSize(values []string) int {
	total := 0
	for _, v := range values {
		total += len(v)
	}
	return total
}

// ==================== Bloom Filter Simulation ====================

type BloomFilter struct {
	bits    []uint64
	numBits int
}

func NewBloomFilter(numItems int, falsePositiveRate float64) *BloomFilter {
	// Calculate optimal number of bits
	// m = -n * ln(p) / (ln(2)^2)
	numBits := int(float64(numItems) * -math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	numWords := (numBits + 63) / 64
	return &BloomFilter{
		bits:    make([]uint64, numWords),
		numBits: numBits,
	}
}

func (bf *BloomFilter) Add(item string) {
	h := hash(item)
	for i := 0; i < 3; i++ { // 3 hash functions
		idx := (h + uint64(i)*h) % uint64(bf.numBits)
		wordIdx := idx / 64
		bitIdx := idx % 64
		bf.bits[wordIdx] |= 1 << bitIdx
	}
}

func (bf *BloomFilter) MightContain(item string) bool {
	h := hash(item)
	for i := 0; i < 3; i++ {
		idx := (h + uint64(i)*h) % uint64(bf.numBits)
		wordIdx := idx / 64
		bitIdx := idx % 64
		if bf.bits[wordIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

func hash(s string) uint64 {
	h := uint64(0)
	for _, c := range s {
		h = h*31 + uint64(c)
	}
	return h
}
