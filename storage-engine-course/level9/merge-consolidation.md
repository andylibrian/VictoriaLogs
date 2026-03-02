# Level 9 Merge Consolidation

## Context
- Level: 9 - IndexDB and `mergeset` Deep Dive
- Date: 2026-03-02

## Toy Example: Duplicate StreamIDs

### Initial State (Before Merge)

```
Tag: host="web-01"
TenantID: 12345

4 separate items (one streamID each):
  Item 1: [0x02][12345][host][sep][web-01][S1]
  Item 2: [0x02][12345][host][sep][web-01][S5]
  Item 3: [0x02][12345][host][sep][web-01][S12]
  Item 4: [0x02][12345][host][sep][web-01][S99]

Storage overhead: 4 items × ~35 bytes = 140 bytes
Query cost: 4 separate reads
```

### After Merge (Consolidated)

```
1 consolidated item (4 streamIDs):
  Item 1: [0x02][12345][host][sep][web-01][S1][S5][S12][S99]

Storage overhead: 1 item × ~83 bytes = 83 bytes
Query cost: 1 read

Savings: 41% storage reduction, 75% I/O reduction
```

## Merge Callback Process

### Step 1: Identify Consecutive Items with Same Prefix

```go
func mergeTagToStreamIDsRows(data []byte, items []mergeset.Item) {
    for i, it := range items {
        item := it.Bytes(data)
        
        // Skip non-nsPrefixTagToStreamIDs items
        if item[0] != nsPrefixTagToStreamIDs {
            // Pass through unchanged
            continue
        }
        
        // Skip first and last items (boundary anchors)
        if i == 0 || i == len(items)-1 {
            // Pass through unchanged
            continue
        }
        
        // Parse the item
        sp.Init(item)
        
        // Check if prefix matches previous
        if sp.EqualPrefix(spPrev) {
            // Same prefix - collect streamIDs
            sp.ParseStreamIDs()
            pendingStreamIDs = append(pendingStreamIDs, sp.StreamIDs...)
        } else {
            // Different prefix - flush pending
            flushPendingStreamIDs()
        }
    }
}
```

### Step 2: Collect and Deduplicate StreamIDs

```
Pending streamIDs: [S1, S5, S12, S99]

After sort:
  [S1, S5, S12, S99]

After deduplication (no duplicates in this case):
  [S1, S5, S12, S99]
```

### Step 3: Emit Consolidated Item

```
Marshal prefix: [0x02][12345][host][sep][web-01]
Marshal streamIDs: [S1][S5][S12][S99]

Result: [0x02][12345][host][sep][web-01][S1][S5][S12][S99]
```

## Example with Duplicates

### Initial State

```
5 items with duplicate streamIDs:
  Item 1: [0x02][12345][host][sep][web-01][S1][S5]
  Item 2: [0x02][12345][host][sep][web-01][S1][S12]
  Item 3: [0x02][12345][host][sep][web-01][S5][S99]
  Item 4: [0x02][12345][host][sep][web-01][S12][S42]
  Item 5: [0x02][12345][host][sep][web-01][S1][S99]

Total streamIDs: 10 (with duplicates)
Unique streamIDs: 6 (S1, S5, S12, S42, S99)
```

### Merge Process

```
1. Collect all streamIDs:
   pendingStreamIDs = [S1, S5, S1, S12, S5, S99, S12, S42, S1, S99]

2. Sort:
   pendingStreamIDs = [S1, S1, S1, S5, S5, S12, S12, S42, S99, S99]

3. Deduplicate:
   pendingStreamIDs = [S1, S5, S12, S42, S99]

4. Cap at maxStreamIDsPerRow (32):
   (Not needed - only 5 streamIDs)

5. Emit consolidated item:
   [0x02][12345][host][sep][web-01][S1][S5][S12][S42][S99]
```

### Result

```
1 consolidated item with 5 unique streamIDs
Storage: 5 items × ~50 bytes → 1 item × ~95 bytes = 51% reduction
```

## maxStreamIDsPerRow Limit

```
const maxStreamIDsPerRow = 32

If > 32 streamIDs collected:
  Emit multiple items with same prefix
  
Example: 80 streamIDs for host="web-01"
  Item 1: [prefix][S1...S32]
  Item 2: [prefix][S33...S64]
  Item 3: [prefix][S65...S80]
```

## Unsorted Fallback

### Problem Case

```
Initial items:
  Item 1: [prefix][S1, S1, S5]    // Duplicate S1
  Item 2: [prefix][S1, S4]

After merge:
  Item 1: [prefix][S1, S5]        // Deduplicated
  Item 2: [prefix][S1, S4]

Sort check: Item 1 > Item 2?
  [prefix][S1, S5] > [prefix][S1, S4]
  Because S5 > S4

VIOLATION: Items became unsorted!
```

### Fallback Handling

```go
if !checkItemsSorted(dstData, dstItems) {
    // Items became unsorted after consolidation
    // This is rare - happens when duplicates span items
    
    // Fallback: return original unmodified items
    dstData = append(dstData[:0], tsm.dataCopy...)
    dstItems = append(dstItems[:0], tsm.itemsCopy...)
    
    // Retry will happen in next merge
}
```

### Why Fallback is Safe

1. **Correctness:** Original items are sorted and valid
2. **Eventual consistency:** Next merge will have different item arrangement
3. **Rare case:** Only happens with concurrent duplicate insertions

## Boundary Constraint

```
Items passed to merge callback:
  [Item A] [Item B] [Item C] [Item D] [Item E]
   ^first                           ^last

Rule: First and last items MUST pass through unchanged

Why? They serve as sort-order anchors for adjacent blocks

Interior items (B, C, D): Can be consolidated
Boundary items (A, E): Must remain unchanged
```

## Code References

- `lib/logstorage/indexdb.go:867-954` - `mergeTagToStreamIDsRows`
- `lib/logstorage/indexdb.go:956` - `maxStreamIDsPerRow = 32`
- `lib/logstorage/indexdb.go:991-1013` - `flushPendingStreamIDs`
- `lib/logstorage/indexdb.go:1016-1038` - `removeDuplicateStreamIDs`
- `lib/logstorage/indexdb.go:1196-1210` - `checkItemsSorted`

## Conclusions

1. **Semantic compaction** - Merge understands data meaning
2. **Deduplication** - Removes duplicate streamIDs during merge
3. **Capping** - maxStreamIDsPerRow limits item size
4. **Fallback safety** - Unsorted result triggers retry
5. **Boundary anchors** - First/last items preserve sort order

## Open Questions

- How often does the unsorted fallback occur in practice?
- Should maxStreamIDsPerRow be configurable?
