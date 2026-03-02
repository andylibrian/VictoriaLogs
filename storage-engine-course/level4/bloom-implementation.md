# Level 4 Bloom Filter Implementation

## Context
- Level: 4 - Bloom Filters: Theory To VictoriaLogs Practice
- Date: 2026-03-02

## Implementation Overview

### Core Data Structure

```go
type BloomFilter struct {
    bits    []uint64 // Bit array stored as 64-bit words
    numBits int      // Total number of bits
    k       int      // Number of hash functions
}
```

The bloom filter uses a bit array stored as `uint64` words for efficient bit operations:
- Each word holds 64 bits
- Bit N is at: word index = N / 64, bit position = N % 64

### Hash Generation

```go
func (bf *BloomFilter) getHashes(item string) []uint64 {
    hashes := make([]uint64, bf.k)
    h := hashString(item)
    for i := 0; i < bf.k; i++ {
        hashes[i] = hashUint64(h + uint64(i))
    }
    return hashes
}
```

This mirrors VictoriaLogs' approach using xxhash with incrementing seed values.

### Add Operation

```go
func (bf *BloomFilter) Add(item string) {
    hashes := bf.getHashes(item)
    for _, h := range hashes {
        bf.setBit(h)
    }
}

func (bf *BloomFilter) setBit(pos uint64) {
    idx := pos % uint64(bf.numBits)
    wordIdx := idx / 64
    bitIdx := idx % 64
    bf.bits[wordIdx] |= (1 << bitIdx)
}
```

### Contains Operation

```go
func (bf *BloomFilter) Contains(item string) bool {
    hashes := bf.getHashes(item)
    for _, h := range hashes {
        if !bf.getBit(h) {
            return false // Definitely not present
        }
    }
    return true // Might be present (could be false positive)
}
```

## Test Results

### Test 1: No False Negatives (Critical!)

```
Configuration: 100 items, 1% target FP rate
Total tests: 10,000
False negatives: 0

VERIFIED: Zero false negatives across all tests
```

**This is the most important property:** If the bloom filter says "not present", the item is definitely not there.

### Test 2: False Positive Measurement

```
Test set size: 1,000 items
Query set size: 3,000 items
Memory usage: 1,200 bytes

Results:
  True Positives:  0
  False Positives: 18
  True Negatives:  2,982
  False Negatives: 0
  Actual FP Rate:  0.60% (target: 1%)
```

False positives are expected and acceptable. They cause extra work (scanning values) but never wrong results.

### Test 3: Tokenization

```
Message: "connection timeout to 10.0.1.5 from host web-01"
Tokens: [connection timeout to 10 0 1 5 from host web 01]

Query results:
  "timeout": might be present ✓
  "connection": might be present ✓
  "web": might be present ✓
  "10": might be present ✓
  "error": definitely not present ✓
```

Tokenization enables substring matching within log messages.

## Tokenization Implementation

```go
func Tokenize(s string) []string {
    tokens := []string{}
    start := -1
    
    for i := 0; i < len(s); i++ {
        c := s[i]
        isTokenChar := (c >= 'a' && c <= 'z') || 
                       (c >= 'A' && c <= 'Z') || 
                       (c >= '0' && c <= '9')
        
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
```

Splits strings at non-alphanumeric boundaries, enabling word-level matching.

## Evidence

Lab program `bloom_implementation.go` demonstrates:
- Zero false negatives (verified across 10,000 tests)
- Measurable false positives (~0.6% with 16 bits/item, 6 hashes)
- Tokenization for log message matching
- Memory efficiency (2 bytes per token)

## Conclusions

1. **False negatives are impossible** - guaranteed by the algorithm
2. **False positives are acceptable** - values are verified anyway
3. **Memory is efficient** - 2 bytes per token regardless of token length
4. **Tokenization enables flexible matching** - query any word in a message

## Open Questions

- How does bloom filter performance scale with millions of tokens?
- What is the optimal bloom filter size for different block sizes?
