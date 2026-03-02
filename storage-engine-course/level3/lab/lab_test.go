package main

import (
	"testing"
)

func TestCanStoreInConstColumn(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected bool
	}{
		{
			name:     "all same values",
			values:   []string{"web-01", "web-01", "web-01"},
			expected: true,
		},
		{
			name:     "different values",
			values:   []string{"web-01", "web-02", "web-01"},
			expected: false,
		},
		{
			name:     "empty slice",
			values:   []string{},
			expected: true,
		},
		{
			name:     "single value",
			values:   []string{"value"},
			expected: true,
		},
		{
			name:     "value too large",
			values:   []string{makeLongString(300), makeLongString(300)},
			expected: false,
		},
		{
			name:     "value at limit",
			values:   []string{makeLongString(256), makeLongString(256)},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := canStoreInConstColumn(tt.values)
			if result != tt.expected {
				t.Errorf("canStoreInConstColumn() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func makeLongString(length int) string {
	result := make([]byte, length)
	for i := range result {
		result[i] = 'x'
	}
	return string(result)
}

func TestDetectValueType(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected valueType
	}{
		{
			name:     "dict encoding - low cardinality",
			values:   []string{"error", "info", "warn", "error", "info", "warn", "error", "info", "warn", "error"},
			expected: valueTypeDict,
		},
		{
			name:     "uint encoding - high cardinality",
			values:   []string{"200", "404", "500", "200", "301", "302", "403", "503", "200", "201", "202", "203", "204", "205", "206"},
			expected: valueTypeUint64,
		},
		{
			name:     "int encoding - high cardinality",
			values:   []string{"-100", "200", "-50", "0", "150", "-200", "300", "-75", "25", "-25"},
			expected: valueTypeInt64,
		},
		{
			name:     "float encoding - high cardinality",
			values:   []string{"1.5", "2.7", "0.1", "3.14", "2.71", "0.001", "99.9", "1.0", "0.5", "2.0"},
			expected: valueTypeFloat64,
		},
		{
			name:     "ipv4 encoding - high cardinality",
			values:   []string{"192.168.1.1", "10.0.0.1", "172.16.0.1", "8.8.8.8", "1.1.1.1", "192.168.0.1", "10.1.1.1", "172.17.0.1", "8.8.4.4", "1.0.0.1"},
			expected: valueTypeIPv4,
		},
		{
			name:     "string fallback - unique values",
			values:   []string{"hello world", "foo bar", "random text", "another one", "more text", "even more", "unique msg", "log entry", "debug info", "trace data"},
			expected: valueTypeString,
		},
		{
			name:     "mixed types - string fallback",
			values:   []string{"200", "error", "1.5", "404", "warn", "2.7", "info", "500", "0.1", "debug"},
			expected: valueTypeString,
		},
		{
			name:     "empty slice",
			values:   []string{},
			expected: valueTypeString,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectValueType(tt.values)
			if result != tt.expected {
				t.Errorf("detectValueType() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBlockMustInitFromRows(t *testing.T) {
	timestamps := []int64{1000, 2000, 3000}
	rows := [][]Field{
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "error"}},
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "info"}},
		{{Name: "host", Value: "web-01"}, {Name: "level", Value: "warn"}},
	}

	block := &Block{}
	block.MustInitFromRows(timestamps, rows)

	// Check timestamps
	if len(block.Timestamps) != 3 {
		t.Errorf("Expected 3 timestamps, got %d", len(block.Timestamps))
	}

	// Check const columns (host should be const)
	foundHost := false
	for _, cc := range block.ConstColumns {
		if cc.Name == "host" {
			foundHost = true
			if cc.Value != "web-01" {
				t.Errorf("Expected host value 'web-01', got %s", cc.Value)
			}
		}
	}
	if !foundHost {
		t.Error("Expected 'host' to be a const column")
	}

	// Check non-const columns (level should not be const)
	foundLevel := false
	for _, col := range block.Columns {
		if col.Name == "level" {
			foundLevel = true
			if len(col.Values) != 3 {
				t.Errorf("Expected 3 level values, got %d", len(col.Values))
			}
		}
	}
	if !foundLevel {
		t.Error("Expected 'level' to be a non-const column")
	}
}

func TestBlockWithMissingFields(t *testing.T) {
	timestamps := []int64{1000, 2000, 3000}
	rows := [][]Field{
		{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
		{{Name: "a", Value: "3"}}, // missing b
		{{Name: "b", Value: "6"}}, // missing a
	}

	block := &Block{}
	block.MustInitFromRows(timestamps, rows)

	// Both a and b should be non-const columns (not all values same)
	if len(block.Columns) < 2 {
		t.Errorf("Expected at least 2 non-const columns, got %d", len(block.Columns))
	}

	// Find column 'a' and check it has empty string for missing value
	for _, col := range block.Columns {
		if col.Name == "a" {
			if col.Values[2] != "" {
				t.Errorf("Expected empty string for missing field 'a' in row 3, got %s", col.Values[2])
			}
		}
		if col.Name == "b" {
			if col.Values[1] != "" {
				t.Errorf("Expected empty string for missing field 'b' in row 2, got %s", col.Values[1])
			}
		}
	}
}

func TestColumnSorting(t *testing.T) {
	timestamps := []int64{1000}
	rows := [][]Field{
		{{Name: "zebra", Value: "z"}, {Name: "apple", Value: "a"}, {Name: "mango", Value: "m"}},
	}

	block := &Block{}
	block.MustInitFromRows(timestamps, rows)

	// Check columns are sorted alphabetically
	if len(block.ConstColumns) >= 2 {
		for i := 1; i < len(block.ConstColumns); i++ {
			if block.ConstColumns[i-1].Name > block.ConstColumns[i].Name {
				t.Errorf("Const columns not sorted: %s > %s",
					block.ConstColumns[i-1].Name, block.ConstColumns[i].Name)
			}
		}
	}
}

func TestEncodingStats(t *testing.T) {
	// Test dict encoding stats - need many repeated values for dict
	dictCol := &ColumnSimulator{
		Name:   "level",
		Values: []string{"error", "info", "warn", "error", "info", "warn", "error", "info", "warn", "error", "info", "warn"},
	}
	stats := dictCol.AnalyzeEncoding()
	if stats.ValueType != valueTypeDict {
		t.Errorf("Expected dict encoding, got %v", stats.ValueType)
	}
	if stats.CanDictLookup != true {
		t.Error("Dict encoding should support dict lookup")
	}
	if stats.BloomFilterBytes != 0 {
		t.Error("Dict encoding should not have bloom filter")
	}

	// Test string encoding stats - need many unique values
	stringCol := &ColumnSimulator{
		Name:   "message",
		Values: []string{"msg1", "msg2", "msg3", "msg4", "msg5", "msg6", "msg7", "msg8", "msg9", "msg10"},
	}
	stats = stringCol.AnalyzeEncoding()
	if stats.ValueType != valueTypeString {
		t.Errorf("Expected string encoding, got %v", stats.ValueType)
	}
	if stats.BloomFilterBytes == 0 {
		t.Error("String encoding should have bloom filter")
	}
}

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Add some items
	items := []string{"error", "warn", "info"}
	for _, item := range items {
		bf.Add(item)
	}

	// Check items that were added
	for _, item := range items {
		if !bf.MightContain(item) {
			t.Errorf("Bloom filter should contain %s", item)
		}
	}

	// Check item that wasn't added (might have false positive)
	// We just check it doesn't panic
	_ = bf.MightContain("debug")
}
