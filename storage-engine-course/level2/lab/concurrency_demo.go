package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== Lab 2: Concurrency Analysis ====================

// GlobalLockBuffer demonstrates the contention problem with a single lock
type GlobalLockBuffer struct {
	mu       sync.Mutex
	rows     int
	flushes  int
	contends atomic.Int64 // Number of times lock was contended
}

func (b *GlobalLockBuffer) AddRows(count int) {
	// Try to acquire lock - if we can't get it immediately, it's contention
	start := time.Now()
	b.mu.Lock()
	waitTime := time.Since(start)
	if waitTime > time.Microsecond {
		b.contends.Add(1)
	}
	b.rows += count
	// Simulate some work while holding lock
	time.Sleep(10 * time.Microsecond)
	b.mu.Unlock()
}

func (b *GlobalLockBuffer) Stats() (int, int, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rows, b.flushes, b.contends.Load()
}

// ShardedBuffer demonstrates the sharding solution
type ShardedBufferShard struct {
	mu       sync.Mutex
	rows     int
	contends atomic.Int64
	_        [64]byte // Cache line padding to prevent false sharing
}

type ShardedBuffer struct {
	shards  []ShardedBufferShard
	nextIdx atomic.Uint64
}

func NewShardedBuffer(numShards int) *ShardedBuffer {
	return &ShardedBuffer{
		shards: make([]ShardedBufferShard, numShards),
	}
}

func (b *ShardedBuffer) AddRows(count int) {
	idx := b.nextIdx.Add(1) % uint64(len(b.shards))
	shard := &b.shards[idx]

	start := time.Now()
	shard.mu.Lock()
	waitTime := time.Since(start)
	if waitTime > time.Microsecond {
		shard.contends.Add(1)
	}
	shard.rows += count
	time.Sleep(10 * time.Microsecond)
	shard.mu.Unlock()
}

func (b *ShardedBuffer) Stats() (int, int64) {
	totalRows := 0
	totalContends := int64(0)
	for i := range b.shards {
		shard := &b.shards[i]
		shard.mu.Lock()
		totalRows += shard.rows
		shard.mu.Unlock()
		totalContends += shard.contends.Load()
	}
	return totalRows, totalContends
}

// ConcurrencyDemo compares global lock vs sharded buffer performance
func ConcurrencyDemo() {
	fmt.Println("=== Level 2 Lab 2: Concurrency Analysis ===")
	fmt.Println()
	fmt.Println("This program demonstrates how sharding reduces lock contention")
	fmt.Println("compared to a single global lock.")
	fmt.Println()

	numGoroutines := 100
	opsPerGoroutine := 1000
	totalOps := numGoroutines * opsPerGoroutine

	fmt.Printf("Configuration:\n")
	fmt.Printf("  Goroutines:      %d\n", numGoroutines)
	fmt.Printf("  Ops per routine: %d\n", opsPerGoroutine)
	fmt.Printf("  Total operations: %d\n", totalOps)
	fmt.Printf("  Available CPUs:  %d\n", runtime.NumCPU())
	fmt.Println()

	// Test 1: Global Lock
	fmt.Println("--- Test 1: Global Lock (Single Buffer) ---")
	fmt.Println()
	globalBuffer := &GlobalLockBuffer{}

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				globalBuffer.AddRows(1)
			}
		}()
	}
	wg.Wait()
	globalDuration := time.Since(start)

	rows, _, contends := globalBuffer.Stats()
	fmt.Printf("Duration:         %v\n", globalDuration)
	fmt.Printf("Rows added:       %d\n", rows)
	fmt.Printf("Contentions:      %d (%.1f%% of operations)\n", contends, float64(contends)/float64(totalOps)*100)
	fmt.Printf("Throughput:       %.0f ops/sec\n", float64(totalOps)/globalDuration.Seconds())
	fmt.Println()

	// Test 2: Sharded Buffer
	fmt.Println("--- Test 2: Sharded Buffer (4 Shards) ---")
	fmt.Println()
	sharded4 := NewShardedBuffer(4)

	start = time.Now()
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				sharded4.AddRows(1)
			}
		}()
	}
	wg.Wait()
	sharded4Duration := time.Since(start)

	rows, contends = sharded4.Stats()
	fmt.Printf("Duration:         %v\n", sharded4Duration)
	fmt.Printf("Rows added:       %d\n", rows)
	fmt.Printf("Contentions:      %d (%.1f%% of operations)\n", contends, float64(contends)/float64(totalOps)*100)
	fmt.Printf("Throughput:       %.0f ops/sec\n", float64(totalOps)/sharded4Duration.Seconds())
	fmt.Printf("Speedup vs global: %.1fx\n", float64(globalDuration)/float64(sharded4Duration))
	fmt.Println()

	// Test 3: Sharded Buffer with CPU count shards
	numShards := runtime.NumCPU()
	fmt.Printf("--- Test 3: Sharded Buffer (%d Shards = CPU count) ---\n", numShards)
	fmt.Println()
	shardedCPU := NewShardedBuffer(numShards)

	start = time.Now()
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				shardedCPU.AddRows(1)
			}
		}()
	}
	wg.Wait()
	shardedCPUDuration := time.Since(start)

	rows, contends = shardedCPU.Stats()
	fmt.Printf("Duration:         %v\n", shardedCPUDuration)
	fmt.Printf("Rows added:       %d\n", rows)
	fmt.Printf("Contentions:      %d (%.1f%% of operations)\n", contends, float64(contends)/float64(totalOps)*100)
	fmt.Printf("Throughput:       %.0f ops/sec\n", float64(totalOps)/shardedCPUDuration.Seconds())
	fmt.Printf("Speedup vs global: %.1fx\n", float64(globalDuration)/float64(shardedCPUDuration))
	fmt.Println()

	// Explanation
	fmt.Println("=== Why Sharding Reduces Contention ===")
	fmt.Println()
	fmt.Println("With a GLOBAL LOCK:")
	fmt.Println("  - All goroutines compete for ONE lock")
	fmt.Println("  - Only ONE goroutine can hold the lock at a time")
	fmt.Println("  - Others must wait (contention)")
	fmt.Println("  - Contention increases with number of goroutines")
	fmt.Println()
	fmt.Printf("  Example: %d goroutines, 1 lock\n", numGoroutines)
	fmt.Println("    Each goroutine waits for 99 others on average")
	fmt.Println()

	fmt.Println("With SHARDED LOCKS:")
	fmt.Printf("  - Goroutines are distributed across %d locks (round-robin)\n", numShards)
	fmt.Printf("  - On average, only %d goroutines compete per lock\n", numGoroutines/numShards)
	fmt.Println("  - Much less waiting, higher parallelism")
	fmt.Println()

	fmt.Println("=== Edge Case: When Contention Still Exists ===")
	fmt.Println()
	fmt.Println("Even with sharding, contention can occur when:")
	fmt.Println()
	fmt.Println("1. HOT SHARD: If one stream receives disproportionate traffic")
	fmt.Println("   - All writes to that stream go to the same shard")
	fmt.Println("   - Example: {app=\"nginx\"} receives 90% of logs")
	fmt.Println()
	fmt.Println("2. TINY SHARDS: If shard count < CPU count")
	fmt.Println("   - Multiple CPUs compete for same shard lock")
	fmt.Println()
	fmt.Println("3. BATCH WRITES: If a single batch is very large")
	fmt.Println("   - Lock held longer while processing batch")
	fmt.Println()
	fmt.Println("VictoriaLogs mitigates these by:")
	fmt.Println("  - Using CPU count for shard count (max parallelism)")
	fmt.Println("  - Round-robin distribution spreads load")
	fmt.Println("  - Cache line padding prevents false sharing")
	fmt.Println()

	// False sharing explanation
	fmt.Println("=== False Sharing Prevention ===")
	fmt.Println()
	fmt.Println("Without padding, adjacent shards might share a CPU cache line:")
	fmt.Println()
	fmt.Println("  Cache Line (64 bytes):")
	fmt.Println("  [shard0.mu (8 bytes)][shard1.mu (8 bytes)]...")
	fmt.Println()
	fmt.Println("  When CPU0 updates shard0.mu:")
	fmt.Println("    → Invalidates cache line on CPU1")
	fmt.Println("    → CPU1 must re-fetch even though shard1.mu unchanged")
	fmt.Println()
	fmt.Println("With padding:")
	fmt.Println("  [shard0 data][padding (64 bytes)][shard1 data]")
	fmt.Println("  Each shard on its own cache line")
	fmt.Println("  No false sharing between CPUs")
}
