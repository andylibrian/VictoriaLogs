# Level 4 Checkpoint Answers

## Context
- Level: 4 - Bloom Filters: Theory To VictoriaLogs Practice
- Date: 2026-03-02

## Checkpoint Questions

### 1. Why is Bloom ideal for skip indexing, but not final matching?

**Answer:**

Bloom filters have asymmetric guarantees that make them perfect for skipping but insufficient for final matching:

**Skip Indexing (Ideal):**

```
Bloom says "NOT PRESENT" → DEFINITELY not in block → Skip safely
```

This is the key property: **no false negatives**. If the bloom filter says the item is not there, you can skip the entire block without reading any data.

**Final Matching (Insufficient):**

```
Bloom says "MIGHT BE PRESENT" → Could be false positive → Must verify
```

Bloom filters can have **false positives** - the filter may say "might be present" even when the item isn't actually there. This means:

1. You cannot trust a "might be present" result
2. You must still read and verify actual values
3. The bloom filter only helps you skip, not confirm

**Example:**

```
Query: _msg:contains("error")

Without bloom:
  - Read all 1000 blocks
  - Scan all values in each block
  - Cost: 1000 block reads

With bloom:
  - Check bloom filter for all 1000 blocks
  - Bloom says "no" for 985 blocks → skip
  - Bloom says "maybe" for 15 blocks → read and verify
  - Cost: 15 block reads + 1000 bloom checks

But you still MUST verify those 15 blocks:
  - Some may actually contain "error" (true positive)
  - Some may NOT contain "error" (false positive)
  - Only scanning values tells you which is which
```

**Source reference:** `bloomfilter.go:246-267` - `containsAll` returns true for "might be present"

### 2. Why does VictoriaLogs tokenize then hash values before Bloom insertion?

**Answer:**

Tokenization before bloom insertion serves three critical purposes:

**1. Matching Flexibility**

```
Without tokenization:
  Message: "connection timeout to 10.0.1.5"
  Bloom contains: hash("connection timeout to 10.0.1.5")
  Query: _msg:contains("timeout")
  Result: NOT FOUND (hash doesn't match full string)

With tokenization:
  Message: "connection timeout to 10.0.1.5"
  Bloom contains: hash("connection"), hash("timeout"), hash("to"), ...
  Query: _msg:contains("timeout")
  Result: FOUND (hash("timeout") matches)
```

Users search for keywords, not exact full messages.

**2. Storage Efficiency**

```go
// hash_tokenizer.go:77-100
func (t *hashTokenizer) tokenizeString(dst []uint64, s string) []uint64 {
    // Extract tokens at non-alphanumeric boundaries
    // Deduplicate within the string
}
```

```
Message: "error error error timeout timeout error"
Without dedup: 6 bloom entries
With dedup: 2 bloom entries (error, timeout)
```

Deduplication prevents repeated words from wasting bloom capacity.

**3. Query Pattern Alignment**

Most log queries are word-based:
- `_msg:contains("error")` - search for word "error"
- `_msg:contains("timeout")` - search for word "timeout"
- `_msg:contains("192.168")` - search for partial IP (tokenized as "192", "168")

Tokenization aligns the bloom filter with actual query patterns.

**Source reference:** `hash_tokenizer.go:15-28` - `tokenizeHashes` extracts and deduplicates tokens

### 3. When can Bloom checks become net-negative and be bypassed?

**Answer:**

Bloom checks become counter-productive when the cost of checking exceeds the cost of scanning:

**1. Dictionary-Encoded Columns**

```go
// block.go:180-188
if ch.valueType != valueTypeDict {
    // Create bloom filter
} else {
    // No bloom filter for dict type
}
```

Dictionary columns store all unique values in the header:
- Bloom check: hash + bit probe → "maybe" → still need to verify
- Dict check: direct lookup → exact answer → no verification needed

Dict is strictly better: O(1) exact match vs O(1) probabilistic match.

**2. Large IN(...) Lists**

```go
// filter_in.go - checks maxTokenSetsToInit
// If token sets > 1000 OR > 10x row count, skip bloom
```

```
Query: status:in("200", "201", "202", ..., "504")  // 100 values

Per block:
  - Bloom probes: 100 values × 6 hashes = 600 operations
  - Direct scan: 10,000 row comparisons

At 100 values: bloom is faster (600 < 10,000)
At 2000 values: scan is faster (12,000 > 10,000)
```

The crossover point depends on row count and token set size.

**3. Empty Token Sets**

```
Query: _msg:contains("")  // Empty string

Tokenization: []  // No tokens
Bloom check: always returns "maybe" (nothing to check)
Result: bloom provides no value, just overhead
```

**4. Very High Selectivity Columns**

If a value appears in >50% of blocks anyway:
- Bloom will say "maybe" for most blocks
- Most blocks will be scanned regardless
- Bloom check adds overhead without benefit

**Cost Comparison:**

| Scenario | Bloom Cost | Scan Cost | Decision |
|----------|------------|-----------|----------|
| 1 token, 5% selectivity | 6 ops | 10,000 ops | Use bloom |
| 100 tokens, 5% selectivity | 600 ops | 10,000 ops | Use bloom |
| 1000 tokens, 50% selectivity | 6,000 ops | 10,000 ops | Marginal |
| 2000 tokens, any selectivity | 12,000 ops | 10,000 ops | Skip bloom |
| Dict column | N/A | 1 lookup | Skip bloom (use dict) |

**Source reference:** 
- `block.go:180-188` - Dict columns skip bloom
- `filter_in.go` - Large IN lists skip bloom
