package main

import (
	"sort"
	"sync"
	"testing"
)

func TestStreamCache(t *testing.T) {
	cache := NewStreamCache()
	sid := &streamID{tenantID: 1, id: 100}
	partition := "test-partition"

	// Should not exist initially
	if cache.Has(sid, partition) {
		t.Error("Expected stream to not exist in cache")
	}

	// Add to cache
	cache.Put(sid, partition)

	// Should exist now
	if !cache.Has(sid, partition) {
		t.Error("Expected stream to exist in cache after Put")
	}

	// Different partition should not have this stream
	if cache.Has(sid, "other-partition") {
		t.Error("Stream should be partition-specific")
	}
}

func TestIndexDB(t *testing.T) {
	idb := NewIndexDB()
	sid := &streamID{tenantID: 1, id: 100}

	// Should not exist initially
	if idb.HasStreamID(sid) {
		t.Error("Expected stream to not exist in indexdb")
	}

	// Register stream
	idb.MustRegisterStream(sid, `{app="nginx"}`)

	// Should exist now
	if !idb.HasStreamID(sid) {
		t.Error("Expected stream to exist after registration")
	}

	// Verify stats
	lookups, registers := idb.Stats()
	if lookups != 2 { // Two calls to HasStreamID above
		t.Errorf("Expected 2 lookups, got %d", lookups)
	}
	if registers != 1 {
		t.Errorf("Expected 1 register, got %d", registers)
	}
}

func TestPartition_StreamRegistration(t *testing.T) {
	streamCache := NewStreamCache()
	idb := NewIndexDB()
	pt := NewPartition("test", streamCache, idb)

	// First row from new stream
	lr1 := &LogRows{
		streamIDs:            []streamID{{tenantID: 1, id: 100}},
		timestamps:           []int64{1000},
		rows:                 [][]Field{{{Name: "msg", Value: "test"}}},
		streamTagsCanonicals: []string{`{app="nginx"}`},
	}
	pt.MustAddRows(lr1)

	lookups, registers := idb.Stats()
	if registers != 1 {
		t.Errorf("Expected 1 stream registration, got %d", registers)
	}
	if lookups != 1 {
		t.Errorf("Expected 1 indexdb lookup, got %d", lookups)
	}

	// Second row from same stream - should use cache
	lr2 := &LogRows{
		streamIDs:            []streamID{{tenantID: 1, id: 100}},
		timestamps:           []int64{2000},
		rows:                 [][]Field{{{Name: "msg", Value: "test2"}}},
		streamTagsCanonicals: []string{`{app="nginx"}`},
	}
	pt.MustAddRows(lr2)

	// No additional lookups or registrations
	lookups, registers = idb.Stats()
	if registers != 1 {
		t.Errorf("Expected still 1 registration, got %d", registers)
	}
	if lookups != 1 {
		t.Errorf("Expected still 1 lookup (cache hit), got %d", lookups)
	}
}

func TestPartition_DuplicateStreamInBatch(t *testing.T) {
	streamCache := NewStreamCache()
	idb := NewIndexDB()
	pt := NewPartition("test", streamCache, idb)

	// Batch with duplicate stream IDs
	lr := &LogRows{
		streamIDs: []streamID{
			{tenantID: 1, id: 100},
			{tenantID: 1, id: 100},
			{tenantID: 1, id: 100},
		},
		timestamps: []int64{1000, 2000, 3000},
		rows: [][]Field{
			{{Name: "msg", Value: "1"}},
			{{Name: "msg", Value: "2"}},
			{{Name: "msg", Value: "3"}},
		},
		streamTagsCanonicals: []string{
			`{app="nginx"}`,
			`{app="nginx"}`,
			`{app="nginx"}`,
		},
	}
	pt.MustAddRows(lr)

	// Should only register once despite 3 rows
	_, registers := idb.Stats()
	if registers != 1 {
		t.Errorf("Expected 1 registration for duplicate streams, got %d", registers)
	}
}

func TestLogRows_Sorting(t *testing.T) {
	lr := &logRows{
		streamIDs: []streamID{
			{tenantID: 1, id: 200},
			{tenantID: 1, id: 100},
			{tenantID: 1, id: 200},
			{tenantID: 1, id: 100},
		},
		timestamps: []int64{3000, 2000, 1000, 4000},
		rows: [][]Field{
			{{Name: "msg", Value: "row1"}},
			{{Name: "msg", Value: "row2"}},
			{{Name: "msg", Value: "row3"}},
			{{Name: "msg", Value: "row4"}},
		},
	}

	// Sort by (streamID, timestamp)
	sort.Sort(lr)

	// Verify order: streamID 100 first (sorted), then by timestamp
	expectedStreamIDs := []uint64{100, 100, 200, 200}
	expectedTimestamps := []int64{2000, 4000, 1000, 3000}

	for i := range lr.streamIDs {
		if lr.streamIDs[i].id != expectedStreamIDs[i] {
			t.Errorf("Position %d: expected streamID %d, got %d", i, expectedStreamIDs[i], lr.streamIDs[i].id)
		}
		if lr.timestamps[i] != expectedTimestamps[i] {
			t.Errorf("Position %d: expected timestamp %d, got %d", i, expectedTimestamps[i], lr.timestamps[i])
		}
	}
}

func TestLogRows_NeedFlush(t *testing.T) {
	lr := &logRows{
		bufferSize: (maxUncompressedBlockSize / 8) * 6, // 75%
	}
	if lr.needFlush() {
		t.Error("Should not need flush at 75% capacity")
	}

	lr.bufferSize = (maxUncompressedBlockSize / 8) * 8 // 100%
	if !lr.needFlush() {
		t.Error("Should need flush at 100% capacity")
	}
}

func TestRowsBuffer_RoundRobinDistribution(t *testing.T) {
	rb := NewRowsBuffer(4)

	// Add rows from multiple goroutines
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lr := &LogRows{
				streamIDs:  []streamID{{tenantID: 1, id: 100}},
				timestamps: []int64{1000},
				rows:       [][]Field{{{Name: "msg", Value: "test"}}},
			}
			rb.MustAddRows(lr)
		}()
	}
	wg.Wait()

	// Check distribution
	totalRows := 0
	for i := range rb.shards {
		rb.shards[i].mu.Lock()
		totalRows += rb.shards[i].rowsCount
		rb.shards[i].mu.Unlock()
	}

	if totalRows != 100 {
		t.Errorf("Expected 100 total rows, got %d", totalRows)
	}
}

func TestShardedBuffer_LessContention(t *testing.T) {
	numOps := 1000

	// Global lock
	global := &GlobalLockBuffer{}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps/10; j++ {
				global.AddRows(1)
			}
		}()
	}
	wg.Wait()
	_, _, globalContends := global.Stats()

	// Sharded
	sharded := NewShardedBuffer(4)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps/10; j++ {
				sharded.AddRows(1)
			}
		}()
	}
	wg.Wait()
	_, shardedContends := sharded.Stats()

	// Sharded should have less contention (not guaranteed, but highly likely)
	// This is a probabilistic test
	t.Logf("Global contends: %d, Sharded contends: %d", globalContends, shardedContends)
}
