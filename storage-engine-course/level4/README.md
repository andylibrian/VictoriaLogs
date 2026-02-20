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
