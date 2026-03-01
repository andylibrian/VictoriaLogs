# Level 4 - Bloom Filters: Theory To VictoriaLogs Practice

## Objective
Learn Bloom filters deeply, then map directly to VictoriaLogs block-level filtering.

## Outcomes
By the end of this level, you can:
- explain false-positive behavior precisely
- explain VictoriaLogs Bloom parameter choices
- trace Bloom generation and use during query filtering

## Source Anchors
- `lib/logstorage/bloomfilter.go`
  - `bloomFilterHashesCount`
  - `bloomFilterBitsPerItem`
  - `containsAll`
- `lib/logstorage/hash_tokenizer.go`
  - `tokenizeHashes`
- `lib/logstorage/block.go`
  - `bloomFilterMarshalHashes`
- `lib/logstorage/block_search.go`
  - `getBloomFilterForColumn`
- `lib/logstorage/filter_phrase.go`
  - `matchBloomFilterAllTokens`
- `lib/logstorage/filter_in.go`
  - `matchBloomFilterAnyTokenSet`

## Core Concepts
1. Bloom filter is a probabilistic "may contain" test.
2. No false negatives (if implemented correctly), but possible false positives.
3. Query pipeline implication:
   - Bloom false => skip block safely
   - Bloom true => still verify values
4. Parameter tradeoffs:
   - more bits/item lowers false positives but increases storage
   - more hash rounds increase CPU

## What Problem Do Bloom Filters Solve? (Conceptual Background)

### The block-scanning bottleneck

Level 3 showed how VictoriaLogs groups rows into blocks and stores each column separately. A query like `_msg:error` only needs to read the `_msg` column — but it still must read *every* block's `_msg` column to check whether the word "error" appears. With millions of blocks, that is a lot of I/O for a term that may appear in only a few of them.

What we need is a cheap per-block index that can answer: "Could this block contain the word 'error'?" If the answer is definitely no, the entire block can be skipped without reading its values at all. This is called **skip indexing**, and Bloom filters are the data structure VictoriaLogs uses for it.

### How a Bloom filter works

A Bloom filter is a fixed-size bit array. To add an item, you hash it with multiple hash functions and set the corresponding bits. To check membership, you hash the query item the same way and verify that all bits are set.

Consider a tiny example with an 16-bit array and 3 hash functions. Inserting the token `"error"`:

```
hash1("error") % 16 = 2    → set bit 2
hash2("error") % 16 = 7    → set bit 7
hash3("error") % 16 = 11   → set bit 11

Bit array: 0 0 1 0 0 0 0 1 0 0 0 1 0 0 0 0
           ^       ^             ^
```

Now insert `"timeout"`:

```
hash1("timeout") % 16 = 1  → set bit 1
hash2("timeout") % 16 = 7  → already set
hash3("timeout") % 16 = 14 → set bit 14

Bit array: 0 1 1 0 0 0 0 1 0 0 0 1 0 0 1 0
```

Querying for `"error"`: bits 2, 7, 11 are all set → **might be present** (correct).
Querying for `"warning"`: suppose hash results are bits 2, 5, 11 — bit 5 is 0 → **definitely not present** (correct).
Querying for `"crash"`: suppose hash results are bits 1, 7, 11 — all set → **might be present** (false positive — `"crash"` was never inserted, but its hash positions overlap with other tokens).

This gives two guarantees:
- **No false negatives**: if the Bloom filter says "not present", the token truly was not inserted. Blocks can be safely skipped.
- **Possible false positives**: if the Bloom filter says "present", the token might not actually be there. The query must still verify against actual values.

### How VictoriaLogs uses Bloom filters

Each non-dict column in each block has its own Bloom filter. During ingestion, VictoriaLogs tokenizes each value into words, hashes each token with xxhash, then generates 6 probe hashes per token to set bits in the filter (`bloomFilterHashesCount = 6`). The filter is sized at 16 bits per unique token (`bloomFilterBitsPerItem = 16`), giving roughly 1.5% false positive rate.

During a query like `_msg:error`, the pipeline works as follows:

```
Query: _msg:error

For each block:
  1. Tokenize "error" → ["error"]
  2. Compute 6 bloom probe hashes for "error"
  3. Read the block's bloom filter for the _msg column
  4. Check: are all 6 bits set?
     ├─ NO  → skip this block entirely (zero I/O for values)
     └─ YES → read _msg values and verify row by row
```

This is the code path you can trace through `filterPhrase.applyToBlockSearch` → `matchStringByPhrase` → `matchBloomFilterAllTokens` → `bloomFilter.containsAll`.

### Why tokenize before hashing?

Log messages are not single words — they are sentences like `"connection timeout to 10.0.1.5"`. If VictoriaLogs inserted the entire string as one Bloom entry, you could only match the exact full string. Instead, it tokenizes the string into words at non-alphanumeric boundaries:

```
"connection timeout to 10.0.1.5"
  → tokens: ["connection", "timeout", "to", "10", "0", "1", "5"]
```

Each token is individually inserted into the Bloom filter. Now a query for `_msg:timeout` hashes the single token `"timeout"` and checks the Bloom filter — which works because that exact token was inserted during ingestion. The tokenizer (`hash_tokenizer.go`) also deduplicates tokens within a block, so repeated words do not waste Bloom capacity.

### When Bloom checks become counter-productive

Bloom filters are most valuable for selective queries — terms that appear in a small fraction of blocks. But there are cases where checking the Bloom filter is slower than just scanning values:

1. **Dictionary-encoded columns**: If a column uses dict encoding (Level 3), all unique values are already stored in the `columnHeader`. VictoriaLogs matches against the dictionary directly — no Bloom filter needed, and none is stored. See `matchValuesDictByPhrase` in `filter_phrase.go`.

2. **Large `in(...)` value lists**: A filter like `status:in("200", "201", "301", "302", ..., "504")` produces one token set per value. If there are more than 1000 token sets (`maxTokenSetsToInit`), or the number of sets exceeds 10× the block's row count, VictoriaLogs skips the Bloom check and scans values directly. The cost of probing the Bloom filter thousands of times exceeds the cost of just reading the column. See `matchBloomFilterAnyTokenSet` in `filter_in.go`.

3. **Empty token sets**: An empty phrase produces zero tokens, so no Bloom bits can be checked. The filter returns "might be present" unconditionally.

### The cost of a Bloom filter

For a block with `N` unique tokens in one column, the Bloom filter occupies `N × 16 bits = N × 2 bytes`. A block with 1000 unique message tokens has a 2KB Bloom filter. Compared to potentially hundreds of KB of compressed message values, this is a small price to pay for the ability to skip irrelevant blocks without decompressing them.

| Block column tokens | Bloom filter size | Effect |
|---------------------|-------------------|--------|
| 100 | 200 bytes | Tiny overhead, high skip rate for selective queries |
| 1,000 | 2 KB | Typical case, good tradeoff |
| 10,000 | 20 KB | Larger filter, but still far smaller than raw values |

The 6-hash, 16-bit configuration sits at a practical sweet spot: enough hash rounds to keep the false positive rate near 1.5%, but not so many that CPU cost dominates. More hashes would lower false positives but add diminishing returns — going from 6 to 8 hashes with 16 bits/item barely changes the rate while increasing per-lookup cost by 33%.

## Guided Reading Tasks
1. In `bloomfilter.go`, explain how each hash maps to a bit position.
2. Explain why `appendTokensHashes` and `appendHashesHashes` both exist.
3. In `block.go`, identify when Bloom is intentionally omitted.
4. In `block_search.go`, trace where Bloom bytes are read and cached.
5. In `filter_in.go`, explain why large token-set counts bypass Bloom checks.

## Hands-On Lab
### Lab 1: Minimal Bloom Implementation
Implement your own small Bloom filter in Go and verify:
- no false negatives in test set
- measurable false positives outside test set

### Lab 2: Parameter Sweep
Test combinations for bits/item and hash count; report:
- memory usage
- false-positive rate
- rough CPU cost trend

### Lab 3: VictoriaLogs Alignment
Compare your best setting with VictoriaLogs defaults (`16` bits/item, `6` hashes).
State likely reasons these defaults are production-friendly.

## Checkpoint Questions
1. Why is Bloom ideal for skip indexing, but not final matching?
2. Why does VictoriaLogs tokenize then hash values before Bloom insertion?
3. When can Bloom checks become net-negative and be bypassed?

## Deliverables
- `level4/bloom-implementation.md`
- `level4/parameter-sweep.md`
- `level4/victorialogs-bloom-analysis.md`
- `level4/checkpoint.md`

## Pass Criteria
- You can defend Bloom tradeoffs with data, not intuition.
- You can trace exactly how Bloom participates in query execution.
