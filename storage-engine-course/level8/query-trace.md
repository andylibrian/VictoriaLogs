# Level 8 Query Trace

## Context
- Level: 8 - Query Path: Pruning, Scheduling, and Block Evaluation
- Date: 2026-03-02

## Example Query

```logsql
_stream:{host="web-01"} AND level:error AND _time:[2026-03-02T10:00:00Z, 2026-03-02T10:05:00Z]
```

## Function-by-Function Trace

### Phase 1: Setup

```
Storage.RunQuery(qctx, writeBlock)
│
├── initSubqueries(qctx, runQuery, keepInSubquery)
│     Materialize in(subquery), join maps, union hooks
│     For this query: no subqueries → returns q unchanged
│
├── getSearchOptions(tenantIDs, q, hiddenFieldsFilters)
│     Extract from parsed query:
│       - minTimestamp = 1740913200000000000 (10:00 UTC)
│       - maxTimestamp = 1740913500000000000 (10:05 UTC)
│       - streamFilter = {host="web-01"}
│       - filter = level:error
│       - fieldsFilter = all fields (no projection)
│
└── runPipes(qctx, pipes, search, writeBlock, concurrency)
      Build processor chain (reverse order), execute search
```

### Phase 2: Scheduling

```
Storage.searchParallel(workersCount=8, sso, qs, stopCh, writeBlock)
│
├── [START WORKERS] Spin up 8 goroutines
│     workCh := make(chan *blockSearchWorkBatch, 8)
│     for workerID := 0..7:
│       wg.Go(func() {
│         qsLocal := &QueryStats{}
│         for bswb := range workCh:
│           for each bsw in bswb:
│             bs.search(qsLocal, bsw, bm)
│             if bs.br.rowsLen > 0:
│               writeBlock(workerID, &bs.br)
│         qs.UpdateAtomic(qsLocal)  // Merge stats at exit
│       })
│
├── [STAGE 1] getPartitionsForTimeRange(minTs, maxTs)
│     Binary search on sorted partition list
│     Storage has 30 partitions (30 days of data)
│     Query spans 5 minutes on March 2
│     
│     minDay = 1740913200 / 86400 = 20149 (March 2)
│     Binary search finds partition at index 28
│     maxDay = 1740913500 / 86400 = 20149 (same day)
│     
│     Result: 1 partition selected (29 skipped)
│     Pruning counters:
│       - Partitions total: 30
│       - Partitions selected: 1
│       - Partitions skipped: 29 (97% reduction)
│
├── [STAGE 2] partition.search(sso, qs, workCh, stopCh)
│     │
│     ├── getSearchOptions(sso)
│     │     streamIDs = idb.searchStreamIDs(tenantIDs, streamFilter)
│     │     Resolves {host="web-01"} → [S42, S43, S44]
│     │     (cached in indexdb with generation counter)
│     │
│     └── datadb.search(pso, qs, workCh, stopCh)
│           │
│           ├── [STAGE 3] getPartsForTimeRange(minTs, maxTs)
│           │     Scan all three tiers (inmemory, small, big)
│           │     
│           │     Partition has:
│           │       - inmemoryParts: 5 parts
│           │       - smallParts: 120 parts
│           │       - bigParts: 15 parts
│           │     
│           │     Time range check per part:
│           │       - inmemoryParts: 3 match (2 skipped)
│           │       - smallParts: 8 match (112 skipped)
│           │       - bigParts: 2 match (13 skipped)
│           │     
│           │     Result: 13 parts selected (132 skipped)
│           │     Pruning counters:
│           │       - Parts total: 140
│           │       - Parts selected: 13
│           │       - Parts skipped: 127 (91% reduction)
│           │
│           └── For each selected part:
│                 part.search(pso, qs, workCh, stopCh)
```

### Phase 3: Part-Level Search (per part)

```
part.search(pso, qs, workCh, stopCh)
│
├── [STAGE 4] Metaindex pruning (in memory)
│     Scan indexBlockHeaders[] (~400 entries for 1B rows)
│     
│     Part has 85 index block headers
│     Query targets streamIDs [S42, S43, S44]
│     Metaindex is sorted by streamID
│     
│     Binary search for S42 → jump to ih[12]
│     Check: S42 in [ih[12].minStreamID, ih[12].maxStreamID]? Yes
│     Check time range: ih[12] overlaps [10:00, 10:05]? Yes
│     Load this index block
│     
│     Next: ih[13] covers S43 → load
│     Next: ih[14] covers S44 → load
│     Next: ih[15] covers S45 → stop (S45 > S44)
│     
│     Result: 3 index blocks selected (82 skipped)
│     Pruning counters:
│       - Index blocks total: 85
│       - Index blocks selected: 3
│       - Index blocks skipped: 82 (96% reduction)
│
└── [STAGE 5] Block header pruning (one disk read per index block)
      For each selected index block:
        mustReadBlockHeaders(indexBlock)
        ZSTD-decompress index block
        For each blockHeader:
          Check streamID match
          Check time range overlap
        
        Index block 1 (covers S42):
          - 25 block headers
          - 3 match streamID and time range (22 skipped)
        
        Index block 2 (covers S43):
          - 20 block headers
          - 2 match streamID and time range (18 skipped)
        
        Index block 3 (covers S44):
          - 22 block headers
          - 1 match streamID and time range (21 skipped)
        
        Result: 6 blocks selected (61 skipped)
        
        [STAGE 6] Batch into work items
          bswb := getBlockSearchWorkBatch()
          for each matching block:
            bswb.appendBlockSearchWork(p, pso, bh)
            if bswb full (64 blocks):
              workCh <- bswb
              bswb = getBlockSearchWorkBatch()
          
          Send remaining blocks:
            workCh <- bswb  // 6 blocks in this batch
```

### Phase 4: Block Evaluation (per worker)

```
Worker receives bswb with 6 blocks
│
├── [STAGE 7] Bitmap initialization
│     bs.search(qsLocal, bsw, bm)
│     bm.init(rowsCount=10000)  // 10K rows in this block
│     bm.setBits()              // All bits set: [1111111111...]
│
├── [STAGE 8] Bloom precheck
│     filterAnd.matchBloomFilters(bs)
│     
│     Query filter: level:error
│     
│     1. Check const column (level is not const) → skip
│     2. Check dict column (level has dict encoding)
│        ch.valuesDict.values = ["debug", "info", "warn", "error"]
│        Does dict contain "error"? Yes → continue
│     
│     Bloom passes for this block
│     
│     (If bloom failed: bm.resetBits(), return immediately)
│
├── [STAGE 9] Per-filter row evaluation
│     filterPhrase("level", "error").applyToBlockSearch(bs, bm)
│     
│     Load column values (lazy, cached):
│       getValuesForColumn("level")
│       Read from values.bin
│       Decompress ZSTD
│       Cache for this block
│     
│     Scan rows where bm bit is set (10000 rows):
│       row 0: level="info"  → clear bit 0
│       row 1: level="error" → keep bit 1
│       row 2: level="error" → keep bit 2
│       row 3: level="warn"  → clear bit 3
│       ...
│     
│     bm = [0100110001...]  // 3200 bits remaining
│     
│     if bm.isZero() → short-circuit (not triggered here)
│
├── [STAGE 10] Result assembly
│     bm is not zero (3200 bits set)
│     
│     br.mustInit(bs, bm)
│       Fetch timestamps for matching rows
│     
│     br.initColumns(fieldsFilter)
│       Load only needed columns:
│         - _time (timestamps already loaded)
│         - _stream (from indexdb lookup)
│         - level (already in cache)
│         - _msg (load now)
│     
│     writeBlock(workerID, &br)
│       Push to pipe chain
│
└── Update local stats:
      qsLocal.BlocksProcessed++
      qsLocal.RowsProcessed += 10000
      qsLocal.RowsFound += 3200
```

### Phase 5: Finalization

```
All partitions searched
│
├── close(workCh)
│     Signal workers to stop
│
├── wg.Wait()
│     Wait for all workers to finish
│     Each worker merges qsLocal into global qs
│
└── psf()  // Partition search finalizers
      Decrement part references
```

## Pruning Counters Summary

| Stage | Total | Selected | Skipped | Reduction |
|-------|-------|----------|---------|-----------|
| Partitions | 30 | 1 | 29 | 97% |
| Parts | 140 | 13 | 127 | 91% |
| Index blocks | 85 | 3 | 82 | 96% |
| Blocks | 67 | 6 | 61 | 91% |
| Rows (per block) | 10000 | 3200 | 6800 | 68% |

**Total data reduction:** From 30 partitions × billions of rows → 6 blocks × 3200 rows

## Code References

- `lib/logstorage/storage_search.go:274-312` - `RunQuery`
- `lib/logstorage/storage_search.go:1412-1493` - `searchParallel`
- `lib/logstorage/storage_search.go:1495-1530` - `getPartitionsForTimeRange`
- `lib/logstorage/storage_search.go:1650-1671` - `getPartsForTimeRange`
- `lib/logstorage/storage_search.go:1860-1961` - `searchByStreamIDs`
- `lib/logstorage/block_search.go:217-238` - `blockSearch.search`
- `lib/logstorage/filter_and.go:85-120` - `matchBloomFilters`

## Conclusions

1. **Cascading elimination** - Each stage reduces data before expensive stages run
2. **Binary search optimization** - Sorted metadata enables O(log N) lookups
3. **Lazy loading** - Column data loaded only when needed
4. **Batched work items** - 64 blocks per batch amortizes channel overhead

## Open Questions

- How does pruning effectiveness vary with query selectivity?
- What's the optimal batch size for different hardware configurations?
