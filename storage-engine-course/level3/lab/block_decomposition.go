package main

import (
	"fmt"
	"sort"
	"strings"
)

// ==================== Field and Row Types ====================

// Field represents a single key-value pair in a log entry.
type Field struct {
	Name  string
	Value string
}

// ==================== Value Types ====================

type valueType int

const (
	valueTypeUnknown valueType = iota
	valueTypeString
	valueTypeDict
	valueTypeUint8
	valueTypeUint16
	valueTypeUint32
	valueTypeUint64
	valueTypeInt64
	valueTypeFloat64
	valueTypeIPv4
	valueTypeTimestampISO8601
)

func (vt valueType) String() string {
	switch vt {
	case valueTypeUnknown:
		return "unknown"
	case valueTypeString:
		return "string"
	case valueTypeDict:
		return "dict"
	case valueTypeUint8:
		return "uint8"
	case valueTypeUint16:
		return "uint16"
	case valueTypeUint32:
		return "uint32"
	case valueTypeUint64:
		return "uint64"
	case valueTypeInt64:
		return "int64"
	case valueTypeFloat64:
		return "float64"
	case valueTypeIPv4:
		return "ipv4"
	case valueTypeTimestampISO8601:
		return "iso8601"
	default:
		return "unknown"
	}
}

// ==================== Block Structure ====================

const maxConstColumnValueSize = 256

// Column represents a non-const column in a block
type Column struct {
	Name   string
	Values []string
}

// ConstColumn represents a column where all values are the same
type ConstColumn struct {
	Name  string
	Value string
}

// Block represents a columnar block of log entries
type Block struct {
	Timestamps   []int64
	Columns      []Column
	ConstColumns []ConstColumn
}

// ==================== Const Column Detection ====================

// canStoreInConstColumn checks if all values in a column are identical
// and small enough to store in the header.
// Mirrors canStoreInConstColumn in block.go:355-370
func canStoreInConstColumn(values []string) bool {
	if len(values) == 0 {
		return true
	}
	firstValue := values[0]
	if len(firstValue) > maxConstColumnValueSize {
		return false
	}
	for i := 1; i < len(values); i++ {
		if values[i] != firstValue {
			return false
		}
	}
	return true
}

// ==================== Value Type Detection ====================

// detectValueType attempts to determine the optimal encoding type
// Mirrors valuesEncoder.encode in values_encoder.go:138-183
func detectValueType(values []string) valueType {
	if len(values) == 0 {
		return valueTypeString
	}

	// Try dict encoding first (best for low cardinality)
	if vt := tryDictEncoding(values); vt != valueTypeUnknown {
		return vt
	}

	// Try numeric types
	if vt := tryUintEncoding(values); vt != valueTypeUnknown {
		return vt
	}

	if vt := tryIntEncoding(values); vt != valueTypeUnknown {
		return vt
	}

	if vt := tryFloatEncoding(values); vt != valueTypeUnknown {
		return vt
	}

	// Try IPv4
	if vt := tryIPv4Encoding(values); vt != valueTypeUnknown {
		return vt
	}

	// Try timestamp
	if vt := tryTimestampEncoding(values); vt != valueTypeUnknown {
		return vt
	}

	// Fall back to string
	return valueTypeString
}

func tryDictEncoding(values []string) valueType {
	// Dict encoding is used when cardinality is low relative to row count
	uniqueValues := make(map[string]int)
	for _, v := range values {
		uniqueValues[v]++
	}

	cardinality := len(uniqueValues)

	// In real VictoriaLogs, dict encoding is tried first and used if:
	// - Cardinality is low (unique values <= some threshold)
	// - The compression benefit is worth the overhead
	//
	// For this demo, we use a simplified heuristic:
	// Use dict if cardinality <= 256 AND cardinality is much smaller than row count
	// OR if cardinality is very small (<= 8) which is common for log levels

	if cardinality > 256 {
		return valueTypeUnknown
	}

	// Small cardinality always gets dict encoding
	if cardinality <= 8 {
		return valueTypeDict
	}

	// Medium cardinality: only use dict if it's much smaller than row count
	if cardinality < len(values)/3 {
		return valueTypeDict
	}

	return valueTypeUnknown
}

func tryUintEncoding(values []string) valueType {
	hasUint := false
	for _, v := range values {
		if isUint(v) {
			hasUint = true
		} else {
			return valueTypeUnknown
		}
	}
	if hasUint {
		return valueTypeUint64
	}
	return valueTypeUnknown
}

func tryIntEncoding(values []string) valueType {
	hasInt := false
	for _, v := range values {
		if isInt(v) {
			hasInt = true
		} else {
			return valueTypeUnknown
		}
	}
	if hasInt {
		return valueTypeInt64
	}
	return valueTypeUnknown
}

func tryFloatEncoding(values []string) valueType {
	hasFloat := false
	for _, v := range values {
		if isFloat(v) {
			hasFloat = true
		} else {
			return valueTypeUnknown
		}
	}
	if hasFloat {
		return valueTypeFloat64
	}
	return valueTypeUnknown
}

func tryIPv4Encoding(values []string) valueType {
	for _, v := range values {
		if !isIPv4(v) {
			return valueTypeUnknown
		}
	}
	return valueTypeIPv4
}

func tryTimestampEncoding(values []string) valueType {
	for _, v := range values {
		if !isISO8601(v) {
			return valueTypeUnknown
		}
	}
	return valueTypeTimestampISO8601
}

// Helper functions for type detection
func isUint(s string) bool {
	if len(s) == 0 {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func isInt(s string) bool {
	if len(s) == 0 {
		return false
	}
	if s[0] == '-' {
		s = s[1:]
	}
	return isUint(s)
}

func isFloat(s string) bool {
	if len(s) == 0 {
		return false
	}
	hasDot := false
	start := 0
	if s[0] == '-' {
		start = 1
	}
	for i := start; i < len(s); i++ {
		if s[i] == '.' {
			if hasDot {
				return false
			}
			hasDot = true
		} else if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return hasDot
}

func isIPv4(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if !isUint(p) {
			return false
		}
	}
	return true
}

func isISO8601(s string) bool {
	// Simplified check: contains 'T' and has date-like format
	return strings.Contains(s, "T") && len(s) >= 10
}

// ==================== Block Construction ====================

// MustInitFromRows transforms rows into columnar block format
// Mirrors block.MustInitFromRows in block.go:224-230
func (b *Block) MustInitFromRows(timestamps []int64, rows [][]Field) {
	b.Timestamps = append(b.Timestamps, timestamps...)

	// Collect all field names
	fieldNames := make(map[string]int)
	for _, row := range rows {
		for _, f := range row {
			if _, exists := fieldNames[f.Name]; !exists {
				fieldNames[f.Name] = len(fieldNames)
			}
		}
	}

	// Build columns
	columnsData := make(map[string][]string)
	for name := range fieldNames {
		columnsData[name] = make([]string, len(rows))
	}

	// Fill column data
	for i, row := range rows {
		rowFields := make(map[string]string)
		for _, f := range row {
			rowFields[f.Name] = f.Value
		}
		for name := range fieldNames {
			if val, exists := rowFields[name]; exists {
				columnsData[name][i] = val
			}
			// Missing fields remain empty string
		}
	}

	// Separate const and non-const columns
	b.Columns = nil
	b.ConstColumns = nil

	for name, values := range columnsData {
		if canStoreInConstColumn(values) {
			b.ConstColumns = append(b.ConstColumns, ConstColumn{
				Name:  name,
				Value: values[0],
			})
		} else {
			b.Columns = append(b.Columns, Column{
				Name:   name,
				Values: values,
			})
		}
	}

	// Sort columns by name for deterministic layout
	sort.Slice(b.Columns, func(i, j int) bool {
		return b.Columns[i].Name < b.Columns[j].Name
	})
	sort.Slice(b.ConstColumns, func(i, j int) bool {
		return b.ConstColumns[i].Name < b.ConstColumns[j].Name
	})
}

// ==================== Analysis Functions ====================

func (b *Block) PrintAnalysis() {
	fmt.Println("=== Block Analysis ===")
	fmt.Println()
	fmt.Printf("Total rows: %d\n", len(b.Timestamps))
	fmt.Println()

	fmt.Println("--- Const Columns (stored once in header) ---")
	if len(b.ConstColumns) == 0 {
		fmt.Println("  (none)")
	} else {
		for _, cc := range b.ConstColumns {
			fmt.Printf("  %-15s = %q\n", cc.Name, cc.Value)
		}
	}
	fmt.Println()

	fmt.Println("--- Non-Const Columns ---")
	for _, col := range b.Columns {
		vt := detectValueType(col.Values)
		uniqueCount := countUnique(col.Values)

		fmt.Printf("  %-15s: type=%-8s unique=%d/%d\n",
			col.Name, vt, uniqueCount, len(col.Values))

		// Show sample values
		fmt.Printf("    Sample values: ")
		max := 5
		if len(col.Values) < max {
			max = len(col.Values)
		}
		for i := 0; i < max; i++ {
			if i > 0 {
				fmt.Printf(", ")
			}
			val := col.Values[i]
			if len(val) > 20 {
				val = val[:20] + "..."
			}
			fmt.Printf("%q", val)
		}
		if len(col.Values) > 5 {
			fmt.Printf(" ... (%d more)", len(col.Values)-5)
		}
		fmt.Println()
	}
}

func countUnique(values []string) int {
	seen := make(map[string]bool)
	for _, v := range values {
		seen[v] = true
	}
	return len(seen)
}

// ==================== Demo ====================

func BlockDecompositionDemo() {
	fmt.Println("=== Level 3 Lab 1: Block Decomposition ===")
	fmt.Println()
	fmt.Println("This program demonstrates how log rows are transformed")
	fmt.Println("into columnar blocks with const column optimization.")
	fmt.Println()

	// Dataset 1: Typical web server logs
	fmt.Println("=== Dataset 1: Web Server Logs ===")
	fmt.Println()

	webRows := [][]Field{
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "error"}, {Name: "msg", Value: "Connection timeout"}},
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "info"}, {Name: "msg", Value: "Request processed"}},
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "error"}, {Name: "msg", Value: "Connection timeout"}},
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "warn"}, {Name: "msg", Value: "High latency"}},
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "error"}, {Name: "msg", Value: "Connection timeout"}},
	}
	webTimestamps := []int64{1000, 2000, 3000, 4000, 5000}

	block1 := &Block{}
	block1.MustInitFromRows(webTimestamps, webRows)
	block1.PrintAnalysis()

	fmt.Println()
	fmt.Println("Expected query fast paths:")
	fmt.Println("  - Filter by host='web-01': O(1) - matches const column")
	fmt.Println("  - Filter by level='error': dict lookup (3 unique values)")
	fmt.Println("  - Filter by msg contains 'timeout': bloom filter check")
	fmt.Println()

	// Dataset 2: Mixed types
	fmt.Println("=== Dataset 2: Metrics with Mixed Types ===")
	fmt.Println()

	metricRows := [][]Field{
		{{Name: "app", Value: "api"}, {Name: "status", Value: "200"}, {Name: "latency", Value: "0.045"}, {Name: "ip", Value: "192.168.1.1"}},
		{{Name: "app", Value: "api"}, {Name: "status", Value: "404"}, {Name: "latency", Value: "0.012"}, {Name: "ip", Value: "192.168.1.2"}},
		{{Name: "app", Value: "api"}, {Name: "status", Value: "500"}, {Name: "latency", Value: "1.234"}, {Name: "ip", Value: "10.0.0.1"}},
		{{Name: "app", Value: "api"}, {Name: "status", Value: "200"}, {Name: "latency", Value: "0.089"}, {Name: "ip", Value: "192.168.1.1"}},
		{{Name: "app", Value: "api"}, {Name: "status", Value: "200"}, {Name: "latency", Value: "0.056"}, {Name: "ip", Value: "172.16.0.1"}},
	}
	metricTimestamps := []int64{1000, 2000, 3000, 4000, 5000}

	block2 := &Block{}
	block2.MustInitFromRows(metricTimestamps, metricRows)
	block2.PrintAnalysis()

	fmt.Println()
	fmt.Println("Expected encoding benefits:")
	fmt.Println("  - app: const column (1 value stored instead of 5)")
	fmt.Println("  - status: uint encoding (compact binary)")
	fmt.Println("  - latency: float64 encoding (8 bytes per value)")
	fmt.Println("  - ip: IPv4 encoding (4 bytes per value)")
	fmt.Println()

	// Dataset 3: High cardinality text
	fmt.Println("=== Dataset 3: High Cardinality Text ===")
	fmt.Println()

	textRows := [][]Field{
		{{Name: "level", Value: "info"}, {Name: "msg", Value: "User john logged in from 192.168.1.1"}},
		{{Name: "level", Value: "info"}, {Name: "msg", Value: "User jane logged in from 10.0.0.1"}},
		{{Name: "level", Value: "error"}, {Name: "msg", Value: "User bob failed login from 172.16.0.1"}},
		{{Name: "level", Value: "info"}, {Name: "msg", Value: "User alice logged in from 192.168.1.2"}},
		{{Name: "level", Value: "warn"}, {Name: "msg", Value: "User charlie rate limited from 10.0.0.2"}},
	}
	textTimestamps := []int64{1000, 2000, 3000, 4000, 5000}

	block3 := &Block{}
	block3.MustInitFromRows(textTimestamps, textRows)
	block3.PrintAnalysis()

	fmt.Println()
	fmt.Println("Expected storage implications:")
	fmt.Println("  - level: dict encoding (4 unique values)")
	fmt.Println("  - msg: string encoding (all unique, bloom filter useful)")
	fmt.Println()

	// Const column size limit demonstration
	fmt.Println("=== Dataset 4: Const Column Size Limit ===")
	fmt.Println()

	longValue := strings.Repeat("x", 300) // Exceeds maxConstColumnValueSize (256)
	limitRows := [][]Field{
		{{Name: "long_const", Value: longValue}, {Name: "level", Value: "info"}},
		{{Name: "long_const", Value: longValue}, {Name: "level", Value: "info"}},
		{{Name: "long_const", Value: longValue}, {Name: "level", Value: "info"}},
	}
	limitTimestamps := []int64{1000, 2000, 3000}

	block4 := &Block{}
	block4.MustInitFromRows(limitTimestamps, limitRows)

	fmt.Printf("Value length: %d bytes (limit: %d)\n", len(longValue), maxConstColumnValueSize)
	fmt.Println()
	block4.PrintAnalysis()

	fmt.Println()
	fmt.Println("Note: 'long_const' is NOT stored as const column because")
	fmt.Printf("      its value exceeds maxConstColumnValueSize (%d bytes)\n", maxConstColumnValueSize)
	fmt.Println("      This prevents large values from bloating the header.")
}
