# Level 7 Heuristic Reimplementation

## Context
- Level: 7 - Merge Selection Heuristics and K-Way Merge
- Date: 2026-03-02

## Algorithm Implementation

### Constants

```go
const (
    minMergeMultiplier    = 1.7
    defaultPartsToMerge   = 15
    maxOutBytes           = 500 * 1024 * 1024  // 500 MB
)
```

### Step-by-Step Implementation

#### Step 1: Size Filter

```go
maxInPartBytes := uint64(float64(maxOutBytes) / minMergeMultiplier)
// maxInPartBytes = 500 MB / 1.7 ≈ 294 MB

filtered := make([]*PartWrapper, 0, len(src))
for _, pw := range src {
    if pw.Part.CompressedSizeBytes > maxInPartBytes {
        continue  // Exclude too large parts
    }
    filtered = append(filtered, pw)
}
```

**Purpose:** Remove parts that are too large to benefit from merging. If a part is >294 MB, merging it would produce output >500 MB (exceeding limit).

#### Step 2: Sort for Optimal Merge

```go
sort.Slice(pws, func(i, j int) bool {
    a := pws[i].Part
    b := pws[j].Part
    if a.CompressedSizeBytes == b.CompressedSizeBytes {
        return a.MinTimestamp > b.MinTimestamp  // Newer first
    }
    return a.CompressedSizeBytes < b.CompressedSizeBytes
})
```

**Purpose:** Place similarly-sized parts adjacent. Tie-breaker (newer first) improves temporal locality.

#### Step 3: Calculate Window Bounds

```go
maxSrcParts := defaultPartsToMerge  // 15
if maxSrcParts > len(src) {
    maxSrcParts = len(src)
}

minSrcParts := (maxSrcParts + 1) / 2  // 8 if 15+ parts
if minSrcParts < 2 {
    minSrcParts = 2
}
```

**Window sizes:**
- 15+ parts: try windows of 8-15
- 6 parts: try windows of 4-6
- 3 parts: try windows of 2-3

#### Step 4: Exhaustive Search

```go
for windowSize := minSrcParts; windowSize <= maxSrcParts; windowSize++ {
    for startPos := 0; startPos <= len(src)-windowSize; startPos++ {
        window := src[startPos : startPos+windowSize]
        
        // Balance check
        smallest := window[0].Part.CompressedSizeBytes
        largest := window[len(window)-1].Part.CompressedSizeBytes
        if smallest * uint64(len(window)) < largest {
            continue  // Too lopsided
        }
        
        // Size check
        outSize := getCompressedSize(window)
        if outSize > maxOutBytes {
            break  // Further windows only bigger
        }
        
        // Track best ratio
        mergeRatio := float64(outSize) / float64(largest)
        if mergeRatio > maxMergeRatio {
            maxMergeRatio = mergeRatio
            bestWindow = window
        }
    }
}
```

**Why consecutive windows?** After sorting by size, consecutive parts are most similar in size. Similar-sized parts produce best merge ratio.

**Balance check:** Prevents merging tiny parts with huge part (wasteful I/O).

#### Step 5: Threshold Gate

```go
minRatio := float64(defaultPartsToMerge) / 2  // 7.5
if minRatio < minMergeMultiplier {
    minRatio = minMergeMultiplier  // 1.7
}

if maxMergeRatio < minRatio {
    return nil  // No merge - not enough benefit
}
```

**Effective threshold:** 7.5x (since 7.5 > 1.7)

## Three No-Merge Examples

### Example 1: Large Similar Parts

```
Parts: [100 MB] [105 MB] [110 MB]
maxOutBytes: 500 MB
maxInPartBytes: 294 MB

All pass size filter.

Best window: [100 MB, 105 MB, 110 MB]
  Output: 315 MB
  Largest: 110 MB
  Merge ratio: 315/110 = 2.86x
  
Threshold: 7.5x
2.86 < 7.5 → NO MERGE

Reason: Rewriting 315 MB to combine 3 parts provides only 2.86x improvement.
Not worth the I/O cost.
```

### Example 2: One Huge Part Dominates

```
Parts: [1 MB] [2 MB] [500 MB]
maxOutBytes: 1 GB
maxInPartBytes: 588 MB

All pass size filter.

Window [1 MB, 2 MB]:
  Balance check: 1 * 2 = 2 < 500 → skip (lopsided)

Window [1 MB, 2 MB, 500 MB]:
  Balance check: 1 * 3 = 3 < 500 → skip (lopsided)

Window [2 MB, 500 MB]:
  Balance check: 2 * 2 = 4 < 500 → skip (lopsided)

NO MERGE

Reason: The 500 MB part is too large relative to others.
Merging would rewrite 500 MB to absorb 3 MB - 99% wasted I/O.
```

### Example 3: Only One Part

```
Parts: [50 MB]

len(src) < 2 → return nil

NO MERGE

Reason: Merge requires at least 2 input parts.
```

## Evidence

Lab program `heuristic_reimplementation.go` demonstrates:
- Complete algorithm implementation
- All three no-merge scenarios
- Successful merge scenarios
- Algorithm summary with complexity

## Conclusions

1. **Conservative merging** - High threshold (7.5x) prevents wasteful merges
2. **Size-aware selection** - Large parts excluded to prevent exceeding output limit
3. **Balance enforcement** - Prevents merging tiny parts with huge parts
4. **Consecutive windows** - Similar-sized parts produce best ratios

## Open Questions

- How does the algorithm perform with highly skewed part size distributions?
- Should the threshold be configurable per deployment?
