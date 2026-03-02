# Level 3 Block Decomposition

## Context
- Level: 3 - Blocks, Columns, and Type-Aware Encoding
- Date: 2026-03-02

## Row-to-Column Transformation

### Input: Log Rows

```
Row 1: { time: 1, host: "web-01", level: "error", msg: "timeout" }
Row 2: { time: 2, host: "web-01", level: "info",  msg: "started" }
Row 3: { time: 3, host: "web-01", level: "error", msg: "timeout" }
Row 4: { time: 4, host: "web-01", level: "info",  msg: "ok"      }
Row 5: { time: 5, host: "web-01", level: "error", msg: "timeout" }
```

### Step 1: Column Extraction

Extract all values for each field:

```
time:  [1, 2, 3, 4, 5]
host:  ["web-01", "web-01", "web-01", "web-01", "web-01"]
level: ["error", "info", "error", "info", "error"]
msg:   ["timeout", "started", "timeout", "ok", "timeout"]
```

### Step 2: Const Column Detection

Check each column to see if all values are identical AND small enough:

```go
// block.go:355-370
func canStoreInConstColumn(rows [][]Field, colIdx int) bool {
    value := rows[0][colIdx].Value
    if len(value) > maxConstColumnValueSize {  // 256 bytes
        return false
    }
    for i := range rows[1:] {
        if value != rows[i][colIdx].Value {
            return false
        }
    }
    return true
}
```

Result:
- `time`: NOT const (different values)
- `host`: CONST (all "web-01", size < 256 bytes)
- `level`: NOT const (different values)
- `msg`: NOT const (different values)

### Step 3: Block Structure

```
Block {
    Timestamps: [1, 2, 3, 4, 5]
    
    ConstColumns: [
        { Name: "host", Value: "web-01" }
    ]
    
    Columns: [
        { Name: "level", Values: ["error", "info", "error", "info", "error"] },
        { Name: "msg", Values: ["timeout", "started", "timeout", "ok", "timeout"] }
    ]
}
```

### Step 4: Value Type Detection

For each non-const column, determine optimal encoding:

```go
// values_encoder.go:138-183
func (ve *valuesEncoder) encode(values []string, dict *valuesDict) valueType {
    // Try in order:
    // 1. Dict encoding (low cardinality)
    // 2. Uint encoding
    // 3. Int encoding
    // 4. Float64 encoding
    // 5. IPv4 encoding
    // 6. Timestamp encoding
    // 7. String fallback
}
```

| Column | Unique | Type | Reason |
|--------|--------|------|--------|
| level | 2/5 (40%) | dict | Low cardinality (error, info) |
| msg | 3/5 (60%) | dict | Low cardinality (timeout, started, ok) |

### Step 5: Column Sorting

Columns are sorted alphabetically for deterministic layout:

```go
// block.go:229
func (b *block) sortColumnsByName()
```

## Fast Path vs Slow Path

### Fast Path (Same Fields in All Rows)

When all rows have identical field structure:

```go
// block.go:253-272
if areSameFieldsInRows(rows) {
    // Fast path - all the log entries have the same fields
    timestamps = append(timestamps, input...)
    for each field index {
        if canStoreInConstColumn(rows, i) {
            // Add to constColumns
        } else {
            // Add to columns with all values
        }
    }
}
```

Benefits:
- O(n) field iteration
- No map lookups needed
- Direct array access

### Slow Path (Different Fields)

When rows have varying field sets:

```go
// block.go:275-348
// 1. Build columnIdxs map (field name → column index)
// 2. Initialize columns with empty values
// 3. Fill values from each row
// 4. Detect const columns after all values populated
// 5. Swap const columns to end, truncate
```

Costs:
- Map lookups for field name resolution
- Memory for empty string placeholders
- Post-processing for const detection

## Storage Impact

### Before Const Column Optimization

```
host column: 5 × "web-01" = 35 bytes (7 bytes × 5)
```

### After Const Column Optimization

```
constColumn: { name: "host", value: "web-01" } = ~12 bytes
```

Savings: 35 - 12 = **23 bytes (65% reduction)**

For a block with 10,000 rows from the same host:
- Without optimization: 70,000 bytes
- With optimization: ~12 bytes
- Savings: **99.98% reduction**

## Query Fast Paths

### Const Column Match

Query: `host = "web-01"`

```
1. Check blockHeader.constColumns
2. Found "host" = "web-01"
3. Match! No need to read column data
4. Cost: O(1) memory lookup
```

### Const Column Mismatch

Query: `host = "web-02"`

```
1. Check blockHeader.constColumns
2. Found "host" = "web-01" ≠ "web-02"
3. No match! Skip entire block
4. Cost: O(1) memory lookup, 0 bytes read from disk
```

### Non-Const Column with Dict

Query: `level = "error"`

```
1. Check columnHeader.valueType = dict
2. Lookup "error" in valuesDict
3. Found at index 0
4. Scan column values as single-byte indices
5. Rows with index 0 match
6. Cost: O(1) dict lookup + O(n) single-byte comparison
```

## Evidence

Lab program `block_decomposition.go` demonstrates:
- Const column detection
- Value type inference
- Storage size estimation
- Query fast path identification

## Conclusions

1. **Const columns save massive space** for repeated values (host, app, env)
2. **Dict encoding optimizes** low-cardinality columns (level, status)
3. **Column sorting** ensures deterministic, cacheable layouts
4. **Fast path** for uniform field sets avoids map overhead

## Open Questions

- How does merge affect const column detection?
- What happens when a const column becomes non-const during merge?
