# Level 3 Encoding Impact Analysis

## Context
- Level: 3 - Blocks, Columns, and Type-Aware Encoding
- Date: 2026-03-02

## Scenario 1: Low-Cardinality Column (Dict Encoding)

### Example: Log Levels

```
Column: level
Values: ["error", "info", "warn", "error", "info", "warn", ...]
Unique: 4 values out of 10,000 rows
```

### Encoding Process

1. **Dictionary Construction**
   ```
   Dict: {
     0: "error",
     1: "info", 
     2: "warn",
     3: "debug"
   }
   ```

2. **Value Encoding**
   ```
   Original: ["error", "info", "warn", "error", ...]
   Encoded:  [0, 1, 2, 0, ...]  // 1 byte per value
   ```

3. **Storage Layout**
   ```
   | Dict size | Dict entries | Values (1 byte each) |
   | 4 bytes   | ~20 bytes    | 10,000 bytes         |
   ```

### Storage Impact

| Metric | String Encoding | Dict Encoding |
|--------|-----------------|---------------|
| Raw size | ~50,000 bytes | ~50,000 bytes |
| Encoded size | ~50,000 bytes | ~10,020 bytes |
| Bloom filter | ~6,000 bytes | 0 bytes |
| **Total** | **56,000 bytes** | **10,020 bytes** |
| **Ratio** | 100% | **18%** |

### Query Performance

| Operation | String Encoding | Dict Encoding |
|-----------|-----------------|---------------|
| Exact match | O(n) string compare | O(1) dict lookup + O(n) byte compare |
| Range filter | Not supported | Not supported |
| Bloom check | Required | Not needed |

**Why no bloom filter for dict?**
```go
// block.go:180-188
if ch.valueType != valueTypeDict {
    // create bloom filter
} else {
    // there is no need in encoding bloom filter for dictionary type,
    // since it isn't used during querying - all the dictionary values 
    // are available in ch.valuesDict
}
```

The dictionary itself provides exact membership testing - no false positives.

### Filtering Speed

Query: `level = "error"`

**String encoding:**
1. Tokenize "error" → bloom filter check
2. If bloom passes, read all 10,000 values
3. Compare each string (5 bytes average)
4. Total: ~50,000 bytes read + 10,000 comparisons

**Dict encoding:**
1. Lookup "error" in dictionary → index 0
2. Read 10,000 single-byte indices
3. Compare each byte to 0
4. Total: ~10 bytes (dict) + 10,000 bytes (values) + 10,000 comparisons

**Speedup: ~5x**

---

## Scenario 2: High-Cardinality Free-Text Column (String Encoding)

### Example: Log Messages

```
Column: message
Values: ["User 1234 logged in", "Request failed with timeout", ...]
Unique: 9,500 values out of 10,000 rows
```

### Encoding Process

1. **Dict Attempt (Fails)**
   - Too many unique values (9,500 > threshold)
   - Would use more space than raw strings

2. **Numeric Attempts (Fail)**
   - Not valid integers
   - Not valid floats
   - Not valid IPs
   - Not valid timestamps

3. **String Fallback**
   ```
   Store each value as: [length prefix][bytes]
   ```

3. **Bloom Filter Construction**
   ```
   For each value:
     - Tokenize into words
     - Hash each token
     - Add to bloom filter
   ```

### Storage Impact

| Metric | Without Bloom | With Bloom |
|--------|---------------|------------|
| Values | ~450,000 bytes | ~450,000 bytes |
| Bloom filter | 0 bytes | ~12,500 bytes |
| **Total** | **450,000 bytes** | **462,500 bytes** |

### Query Performance

| Operation | Without Bloom | With Bloom |
|-----------|---------------|------------|
| Exact match | Always scan | Bloom check first |
| Contains "timeout" | Always scan | Bloom check first |
| Bloom false positive | N/A | ~1% |

### Bloom Filter Usefulness

Query: `message contains "timeout"`

**Without bloom filter:**
1. Read all 450,000 bytes of message values
2. Search each message for "timeout"
3. Even if only 10 messages contain "timeout"

**With bloom filter:**
1. Hash "timeout" → check bloom filter
2. If bloom says "no": skip block (0 bytes of values read)
3. If bloom says "maybe": read values and search
4. False positive rate: ~1%

**Typical case (few matches):**
- 99% of blocks don't contain "timeout"
- Bloom filter eliminates 99% of value reads
- **Speedup: ~100x**

---

## Comparison Summary

| Column | Type | Storage | Bloom | Dict Lookup | Range Filter |
|--------|------|---------|-------|-------------|--------------|
| level (low card) | dict | 10KB | 0 | ✓ | ✗ |
| message (high card) | string | 463KB | 12KB | ✗ | ✗ |
| status_code (int) | uint64 | 80KB | 12KB | ✗ | ✓ |
| latency (float) | float64 | 93KB | 12KB | ✗ | ✓ |
| client_ip (ipv4) | ipv4 | 53KB | 12KB | ✗ | ✓ |

## Encoding Decision Tree

```
Column values
    │
    ├─► Cardinality ≤ 256 AND < 30% unique?
    │       │
    │       └─► YES → valueTypeDict
    │
    ├─► All values are valid uints?
    │       │
    │       └─► YES → valueTypeUint*
    │
    ├─► All values are valid ints?
    │       │
    │       └─► YES → valueTypeInt64
    │
    ├─► All values are valid floats?
    │       │
    │       └─► YES → valueTypeFloat64
    │
    ├─► All values are valid IPv4?
    │       │
    │       └─► YES → valueTypeIPv4
    │
    ├─► All values are valid ISO8601 timestamps?
    │       │
    │       └─► YES → valueTypeTimestampISO8601
    │
    └─► None of the above
            │
            └─► valueTypeString
```

## Evidence

Lab program `encoding_analysis.go` demonstrates:
- Storage size comparison for 10,000 rows
- Bloom filter impact analysis
- Compression ratio calculations
- Query performance implications

## Conclusions

1. **Dict encoding is optimal for low-cardinality** - saves 80%+ storage, no bloom needed
2. **String encoding is flexible but expensive** - bloom filter critical for query performance
3. **Numeric types enable range filtering** - worth using when data fits
4. **Bloom filter is a space/time tradeoff** - 10% space overhead for 100x query speedup

## Open Questions

- How does dict encoding performance change with 200+ unique values?
- Should bloom filter size be configurable per column?
