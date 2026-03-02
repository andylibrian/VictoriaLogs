package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== Part Types ====================

type partType int

const (
	partInmemory partType = iota
	partSmall
	partBig
)

func (pt partType) String() string {
	switch pt {
	case partInmemory:
		return "inmemory"
	case partSmall:
		return "small"
	case partBig:
		return "big"
	default:
		return "unknown"
	}
}

// ==================== Part Wrapper Simulation ====================

// PartWrapper simulates the lifecycle of a part in VictoriaLogs
type PartWrapper struct {
	// Identity
	id       int64
	partType partType
	path     string

	// Lifecycle state
	refCount  atomic.Int32
	mustDrop  atomic.Bool
	isInMerge bool

	// In-memory specific
	inmemoryData *InmemoryPart

	// Metadata
	rowsCount      uint64
	compressedSize uint64
	flushDeadline  time.Time

	// Simulation helpers
	createdAt time.Time
}

// InmemoryPart simulates in-memory part data
type InmemoryPart struct {
	blocks [][]byte
}

// IncRef increments the reference count
func (pw *PartWrapper) IncRef() {
	pw.refCount.Add(1)
}

// DecRef decrements the reference count and potentially deletes the part
func (pw *PartWrapper) DecRef() {
	n := pw.refCount.Add(-1)
	if n > 0 {
		return
	}

	// Reference count hit zero - clean up if needed
	if pw.mustDrop.Load() {
		if pw.partType != partInmemory {
			fmt.Printf("  [DELETE] Part %d (%s) deleted from disk: %s\n",
				pw.id, pw.partType, pw.path)
		} else {
			fmt.Printf("  [DELETE] Part %d (inmemory) returned to pool\n", pw.id)
		}
	}
}

// ==================== DataDB Simulation ====================

// DataDB simulates the three-tier part storage
type DataDB struct {
	mu sync.RWMutex

	// Three tiers of parts
	inmemoryParts []*PartWrapper
	smallParts    []*PartWrapper
	bigParts      []*PartWrapper

	// Configuration
	maxInmemoryPartSize uint64
	maxSmallPartSize    uint64
	flushInterval       time.Duration

	// Metrics
	mergeCount      atomic.Int64
	mergeInProgress atomic.Bool

	// Simulation state
	nextPartID atomic.Int64
}

func NewDataDB() *DataDB {
	return &DataDB{
		maxInmemoryPartSize: 1 * 1024 * 1024,   // 1 MB
		maxSmallPartSize:    100 * 1024 * 1024, // 100 MB
		flushInterval:       time.Second,
	}
}

// ==================== Merge Trace ====================

// MergeTrace captures a complete merge operation trace
type MergeTrace struct {
	StartTime   time.Time
	EndTime     time.Time
	SourceParts []*PartWrapper
	DstPartType partType
	DstPart     *PartWrapper

	// State snapshots
	PreMergeState  TierState
	PostMergeState TierState

	// Events
	Events []MergeEvent
}

type TierState struct {
	InmemoryCount int
	SmallCount    int
	BigCount      int
}

type MergeEvent struct {
	Time    time.Time
	Action  string
	Details string
}

// ==================== Merge Operations ====================

// GetDstPartType determines the destination part type for a merge
// This mirrors getDstPartType in datadb.go:814-828
func (ddb *DataDB) GetDstPartType(pws []*PartWrapper, isFinal bool) partType {
	dstPartSize := uint64(0)
	for _, pw := range pws {
		dstPartSize += pw.compressedSize
	}

	// Branch 1: Output too large for small tier
	if dstPartSize > ddb.maxSmallPartSize {
		return partBig
	}

	// Branch 2: Final flush or output too large for memory
	if isFinal || dstPartSize > ddb.maxInmemoryPartSize {
		return partSmall
	}

	// Branch 3: Never regress durability
	for _, pw := range pws {
		if pw.partType != partInmemory {
			return partSmall
		}
	}

	// Branch 4: All in-memory, stay in-memory
	return partInmemory
}

// SelectPartsForMerge simulates part selection for merging
func (ddb *DataDB) SelectPartsForMerge(tier partType) []*PartWrapper {
	ddb.mu.Lock()
	defer ddb.mu.Unlock()

	var parts []*PartWrapper
	switch tier {
	case partInmemory:
		parts = ddb.inmemoryParts
	case partSmall:
		parts = ddb.smallParts
	case partBig:
		parts = ddb.bigParts
	}

	// Select parts not already in merge
	var selected []*PartWrapper
	for _, pw := range parts {
		if !pw.isInMerge {
			pw.isInMerge = true
			pw.IncRef()
			selected = append(selected, pw)
		}
	}

	// For demo, select at most 3 parts
	if len(selected) > 3 {
		selected = selected[:3]
	}

	return selected
}

// MustMergePartsInternal simulates the merge operation
// This mirrors mustMergePartsInternal in datadb.go:641-780
func (ddb *DataDB) MustMergePartsInternal(pws []*PartWrapper, isFinal bool) *MergeTrace {
	if len(pws) == 0 {
		return nil
	}

	trace := &MergeTrace{
		StartTime:   time.Now(),
		SourceParts: pws,
	}

	trace.AddEvent("START", fmt.Sprintf("Merging %d parts", len(pws)))

	// Step 1: Verify all parts are marked for merge
	for _, pw := range pws {
		if !pw.isInMerge {
			trace.AddEvent("ERROR", fmt.Sprintf("Part %d not marked for merge", pw.id))
			return trace
		}
	}
	trace.AddEvent("VERIFY", "All parts verified as isInMerge=true")

	// Capture pre-merge state
	trace.PreMergeState = ddb.GetTierState()
	trace.AddEvent("SNAPSHOT", fmt.Sprintf("Pre-merge: inmemory=%d, small=%d, big=%d",
		trace.PreMergeState.InmemoryCount, trace.PreMergeState.SmallCount, trace.PreMergeState.BigCount))

	// Step 2: Determine destination type
	dstPartType := ddb.GetDstPartType(pws, isFinal)
	trace.DstPartType = dstPartType
	trace.AddEvent("DST_TYPE", fmt.Sprintf("Destination type: %s", dstPartType))

	// Step 3: Reserve disk space (simulated)
	if dstPartType != partInmemory {
		trace.AddEvent("DISK_RESERVE", "Disk space reserved for merge output")
	}

	// Step 4: Update merge counters
	ddb.mergeCount.Add(1)
	trace.AddEvent("METRICS", "Merge counters updated")

	// Step 5: Generate destination path
	dstPath := fmt.Sprintf("/data/part_%016X", ddb.nextPartID.Add(1))
	trace.AddEvent("DST_PATH", fmt.Sprintf("Destination path: %s", dstPath))

	// Step 6: Fast path for single in-memory part
	if isFinal && len(pws) == 1 && pws[0].inmemoryData != nil {
		trace.AddEvent("FAST_PATH", "Single in-memory part flush")
	}

	// Step 7: Normal merge path (simulated)
	trace.AddEvent("MERGE_START", "Opening block stream readers for source parts")
	trace.AddEvent("MERGE_START", "Opening block stream writer for destination")
	trace.AddEvent("MERGE_EXEC", "Executing k-way merge of block streams")
	trace.AddEvent("MERGE_DONE", "Merge complete, finalizing writer")

	// Step 8: Create destination part
	dstPart := &PartWrapper{
		id:             ddb.nextPartID.Load(),
		partType:       dstPartType,
		path:           dstPath,
		rowsCount:      ddb.sumRowsCount(pws),
		compressedSize: ddb.sumCompressedSize(pws),
		createdAt:      time.Now(),
	}
	trace.DstPart = dstPart
	trace.AddEvent("DST_CREATE", fmt.Sprintf("Created part %d with %d rows", dstPart.id, dstPart.rowsCount))

	// Step 9: Atomic swap
	ddb.SwapSrcWithDstParts(pws, dstPart, dstPartType, trace)

	// Capture post-merge state
	trace.PostMergeState = ddb.GetTierState()
	trace.AddEvent("SNAPSHOT", fmt.Sprintf("Post-merge: inmemory=%d, small=%d, big=%d",
		trace.PostMergeState.InmemoryCount, trace.PostMergeState.SmallCount, trace.PostMergeState.BigCount))

	trace.EndTime = time.Now()
	trace.AddEvent("END", fmt.Sprintf("Merge completed in %v", trace.EndTime.Sub(trace.StartTime)))

	// Step 10: Release parts to merge (clears isInMerge)
	defer ddb.releasePartsToMerge(pws)

	return trace
}

// SwapSrcWithDstParts atomically replaces source parts with destination
// This mirrors swapSrcWithDstParts in datadb.go:1192-1246
func (ddb *DataDB) SwapSrcWithDstParts(pws []*PartWrapper, pwNew *PartWrapper, dstPartType partType, trace *MergeTrace) {
	trace.AddEvent("SWAP_START", "Acquiring partsLock")

	ddb.mu.Lock()
	defer ddb.mu.Unlock()

	trace.AddEvent("SWAP_LOCK", "partsLock acquired")

	// Remove source parts from all tiers
	removedInmemory := 0
	removedSmall := 0
	removedBig := 0

	for _, pw := range pws {
		switch pw.partType {
		case partInmemory:
			ddb.removePartFromSlice(&ddb.inmemoryParts, pw)
			removedInmemory++
		case partSmall:
			ddb.removePartFromSlice(&ddb.smallParts, pw)
			removedSmall++
		case partBig:
			ddb.removePartFromSlice(&ddb.bigParts, pw)
			removedBig++
		}
	}
	trace.AddEvent("SWAP_REMOVE", fmt.Sprintf("Removed: inmemory=%d, small=%d, big=%d",
		removedInmemory, removedSmall, removedBig))

	// Add new part to correct tier
	if pwNew != nil {
		switch dstPartType {
		case partInmemory:
			ddb.inmemoryParts = append(ddb.inmemoryParts, pwNew)
		case partSmall:
			ddb.smallParts = append(ddb.smallParts, pwNew)
		case partBig:
			ddb.bigParts = append(ddb.bigParts, pwNew)
		}
		trace.AddEvent("SWAP_ADD", fmt.Sprintf("Added part %d to %s tier", pwNew.id, dstPartType))
	}

	// Write parts.json (simulated)
	if removedSmall > 0 || removedBig > 0 || (pwNew != nil && dstPartType != partInmemory) {
		trace.AddEvent("PARTS_JSON", "Writing parts.json atomically")
	}

	trace.AddEvent("SWAP_UNLOCK", "Releasing partsLock")

	// Mark old parts for deletion and decrement reference
	for _, pw := range pws {
		pw.mustDrop.Store(true)
		pw.DecRef()
	}
	trace.AddEvent("SWAP_CLEANUP", "Marked old parts for deletion, decremented refs")
}

func (ddb *DataDB) removePartFromSlice(parts *[]*PartWrapper, pw *PartWrapper) {
	for i, p := range *parts {
		if p == pw {
			*parts = append((*parts)[:i], (*parts)[i+1:]...)
			return
		}
	}
}

func (ddb *DataDB) releasePartsToMerge(pws []*PartWrapper) {
	for _, pw := range pws {
		pw.isInMerge = false
	}
}

func (ddb *DataDB) sumRowsCount(pws []*PartWrapper) uint64 {
	total := uint64(0)
	for _, pw := range pws {
		total += pw.rowsCount
	}
	return total
}

func (ddb *DataDB) sumCompressedSize(pws []*PartWrapper) uint64 {
	total := uint64(0)
	for _, pw := range pws {
		total += pw.compressedSize
	}
	return total
}

func (ddb *DataDB) GetTierState() TierState {
	ddb.mu.RLock()
	defer ddb.mu.RUnlock()

	return TierState{
		InmemoryCount: len(ddb.inmemoryParts),
		SmallCount:    len(ddb.smallParts),
		BigCount:      len(ddb.bigParts),
	}
}

func (mt *MergeTrace) AddEvent(action, details string) {
	mt.Events = append(mt.Events, MergeEvent{
		Time:    time.Now(),
		Action:  action,
		Details: details,
	})
}

// ==================== Simulation Helpers ====================

func (ddb *DataDB) AddInmemoryPart(rowsCount, compressedSize uint64) *PartWrapper {
	ddb.mu.Lock()
	defer ddb.mu.Unlock()

	pw := &PartWrapper{
		id:             ddb.nextPartID.Add(1),
		partType:       partInmemory,
		rowsCount:      rowsCount,
		compressedSize: compressedSize,
		inmemoryData:   &InmemoryPart{},
		flushDeadline:  time.Now().Add(ddb.flushInterval),
		createdAt:      time.Now(),
	}

	ddb.inmemoryParts = append(ddb.inmemoryParts, pw)
	return pw
}

func (ddb *DataDB) AddSmallPart(rowsCount, compressedSize uint64) *PartWrapper {
	ddb.mu.Lock()
	defer ddb.mu.Unlock()

	pw := &PartWrapper{
		id:             ddb.nextPartID.Add(1),
		partType:       partSmall,
		path:           fmt.Sprintf("/data/small_%016X", ddb.nextPartID.Load()),
		rowsCount:      rowsCount,
		compressedSize: compressedSize,
		createdAt:      time.Now(),
	}

	ddb.smallParts = append(ddb.smallParts, pw)
	return pw
}

func (ddb *DataDB) AddBigPart(rowsCount, compressedSize uint64) *PartWrapper {
	ddb.mu.Lock()
	defer ddb.mu.Unlock()

	pw := &PartWrapper{
		id:             ddb.nextPartID.Add(1),
		partType:       partBig,
		path:           fmt.Sprintf("/data/big_%016X", ddb.nextPartID.Load()),
		rowsCount:      rowsCount,
		compressedSize: compressedSize,
		createdAt:      time.Now(),
	}

	ddb.bigParts = append(ddb.bigParts, pw)
	return pw
}
