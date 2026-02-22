// Package logstorage provides the core storage engine for VictoriaLogs.
//
// ============== Indexdb Overview ==============
//
// The indexdb is the metadata index for VictoriaLogs, storing information about
// log streams and their associated tags. It enables efficient stream discovery
// based on label filters without scanning the actual log data.
//
// WHY INDEX STREAMS?
// When a query filters logs by labels (e.g., app="nginx" AND host="server1"),
// we need to quickly find which streams match those criteria. Without an index,
// we'd have to scan all log data to find matching entries - extremely slow.
//
// With the index, we can:
//  1. Narrow candidate streams via prefix lookups/scans in mergeset
//  2. Then search only those streams' log data
//  3. Skip entire streams that don't match the filter
//
// ============== Storage Architecture ==============
//
// indexdb uses the `mergeset` storage engine (from VictoriaMetrics) which provides:
//   - LSM-tree style storage with automatic background merges
//   - Efficient prefix-based searches
//   - Compression and block-based storage
//   - Point-in-time snapshots
//
// ============== Index Entry Types ==============
//
// Three types of entries are stored, distinguished by namespace prefix:
//
//  1. nsPrefixStreamID (0): Stream existence marker
//     Key: tenantID + streamID
//     Value: (none - just the key presence indicates the stream exists)
//     Purpose: Quickly check if a stream is already registered
//
//  2. nsPrefixStreamIDToStreamTags (1): Stream ID to tags mapping
//     Key: tenantID + streamID
//     Value: streamTagsCanonical (e.g., {app="nginx",host="server1"})
//     Purpose: Look up stream labels from stream ID (for display, logging)
//
//  3. nsPrefixTagToStreamIDs (2): Tag to streams reverse index
//     Key: tenantID + tagName + tagValue
//     Value: list of streamIDs that have this tag=value
//     Purpose: Find all streams matching a label filter
//
// ============== Query Flow ==============
//
// When querying with a stream filter like "app=~"nginx.*" AND host!="localhost":
//  1. Parse the filter into individual conditions
//  2. For each condition, look up matching stream IDs using nsPrefixTagToStreamIDs
//  3. Intersect (AND) or subtract (!=) the stream ID sets
//  4. Return the final set of stream IDs to search
//
// ============== Caching ==============
//
// Stream ID lookups are cached in two levels:
//  1. streamIDCache: Per-stream existence cache (shared across partitions)
//  2. filterStreamCache: Stream filter result cache (shared across partitions)
//
// filterStreamCache is generation-keyed and effectively invalidated when new streams
// are registered. streamIDCache is updated incrementally during ingestion.
package logstorage

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/regexutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"
)

// ==================== Namespace Prefixes ====================
//
// These prefixes distinguish different types of entries in the mergeset table.
// The first byte of each key indicates the entry type.

const (
	// nsPrefixStreamID marks stream existence entries.
	// Entry format: [0][tenantID][streamID] -> (empty value)
	// Used for fast existence checks: "is this stream already registered?"
	nsPrefixStreamID = 0

	// nsPrefixStreamIDToStreamTags marks stream ID to tags mapping entries.
	// Entry format: [1][tenantID][streamID] -> [streamTagsCanonical]
	// Used for reverse lookup: "what are the labels for stream ID X?"
	nsPrefixStreamIDToStreamTags = 1

	// nsPrefixTagToStreamIDs marks the reverse index from tags to streams.
	// Entry format: [2][tenantID][tagName][tagValue] -> [streamID1][streamID2]...
	// Used for forward lookup: "which streams have tag=value?"
	nsPrefixTagToStreamIDs = 2
)

// ==================== Indexdb Statistics ====================

// IndexdbStats contains metrics about the index database.
// These are exposed via the /metrics endpoint for monitoring.
type IndexdbStats struct {
	// StreamsCreatedTotal is the cumulative count of streams created since startup.
	// This helps track stream cardinality growth.
	StreamsCreatedTotal uint64

	// IndexdbSizeBytes is the total size of index data (in-memory + on-disk).
	IndexdbSizeBytes uint64

	// IndexdbItemsCount is the number of index entries.
	// Roughly: streams * (2 + number_of_tags_per_stream), plus merge/layout overhead.
	IndexdbItemsCount uint64

	// IndexdbBlocksCount is the number of storage blocks.
	// More blocks = more files to read during queries.
	IndexdbBlocksCount uint64

	// IndexdbPartsCount is the number of parts in the LSM tree.
	// Higher count may indicate compaction lag.
	IndexdbPartsCount uint64

	// IndexdbPendingItems is the count of items waiting to be merged.
	IndexdbPendingItems uint64

	// IndexdbActiveFileMerges is the current number of active on-disk merges.
	IndexdbActiveFileMerges uint64

	// IndexdbActiveInmemoryMerges is the current number of active in-memory merges.
	IndexdbActiveInmemoryMerges uint64

	// IndexdbFileMergesCount is the total number of on-disk merges completed.
	IndexdbFileMergesCount uint64

	// IndexdbInmemoryMergesCount is the total number of in-memory merges completed.
	IndexdbInmemoryMergesCount uint64

	// IndexdbFileItemsMerged is the total count of items merged to disk.
	IndexdbFileItemsMerged uint64

	// IndexdbInmemoryItemsMerged is the total count of items merged in memory.
	IndexdbInmemoryItemsMerged uint64
}

// ==================== Indexdb Structure ====================

// indexdb manages the stream metadata index for a partition.
// It is backed by a mergeset.Table which provides the actual storage.
//
// Each partition has its own indexdb, which stores:
//   - Stream existence markers (for deduplication)
//   - Stream ID to labels mappings (for reverse lookup)
//   - Label to stream ID reverse index (for query filtering)
type indexdb struct {
	// streamsCreatedTotal counts streams created since indexdb initialization.
	streamsCreatedTotal atomic.Uint64

	// filterStreamCacheGeneration is incremented each time a new stream is registered.
	// This invalidates the filterStreamCache because new streams may match existing queries.
	filterStreamCacheGeneration atomic.Uint32

	// path is the filesystem path to the indexdb directory.
	path string

	// partitionName is the name of the parent partition (e.g., "20240115").
	// Used in cache keys to distinguish streams across partitions.
	partitionName string

	// tb is the underlying mergeset table that stores index entries.
	// All index data is stored here using the namespace prefixes defined above.
	tb *mergeset.Table

	// indexSearchPool is a pool of indexSearch structs for query reuse.
	// Avoids allocations during frequent stream lookups.
	indexSearchPool sync.Pool

	// s is the parent Storage that owns this indexdb's partition.
	// Provides access to shared caches and configuration.
	s *Storage
}

// ==================== Indexdb Lifecycle ====================

// mustCreateIndexdb creates the indexdb directory structure on disk.
// This is called when creating a new partition.
func mustCreateIndexdb(path string) {
	fs.MustMkdirFailIfExist(path)
	fs.MustSyncPathAndParentDir(path)
}

// mustOpenIndexdb opens an existing indexdb for use.
//
// The mergeset.Table is initialized with:
//   - A callback to invalidate the stream filter cache when new data is added
//   - A merge callback to combine tag-to-streamID entries
//
// The isReadOnly flag is shared with the caller but not set here; it's managed
// by the storage layer for snapshot operations.
func mustOpenIndexdb(path, partitionName string, s *Storage) *indexdb {
	idb := &indexdb{
		path:          path,
		partitionName: partitionName,
		s:             s,
	}
	var isReadOnly atomic.Bool
	idb.tb = mergeset.MustOpenTable(path, s.flushInterval, idb.invalidateStreamFilterCache, mergeTagToStreamIDsRows, &isReadOnly)
	return idb
}

// mustCloseIndexdb closes the indexdb and releases resources.
// Must be called before deleting the partition.
func mustCloseIndexdb(idb *indexdb) {
	idb.tb.MustClose()
	idb.tb = nil
	idb.s = nil
	idb.partitionName = ""
	idb.path = ""
}

// debugFlush forces pending index data to be persisted and searchable.
// Used primarily for testing to ensure query visibility.
func (idb *indexdb) debugFlush() {
	idb.tb.DebugFlush()
}

// mustCreateSnapshotAt creates a point-in-time snapshot of the indexdb.
func (idb *indexdb) mustCreateSnapshotAt(dstDir string) {
	idb.tb.MustCreateSnapshotAt(dstDir)
}

// updateStats populates the provided stats structure with current metrics.
func (idb *indexdb) updateStats(d *IndexdbStats) {
	d.StreamsCreatedTotal += idb.streamsCreatedTotal.Load()

	var tm mergeset.TableMetrics
	idb.tb.UpdateMetrics(&tm)

	d.IndexdbSizeBytes += tm.InmemorySizeBytes + tm.FileSizeBytes
	d.IndexdbItemsCount += tm.InmemoryItemsCount + tm.FileItemsCount
	d.IndexdbPendingItems += tm.PendingItems
	d.IndexdbPartsCount += tm.InmemoryPartsCount + tm.FilePartsCount
	d.IndexdbBlocksCount += tm.InmemoryBlocksCount + tm.FileBlocksCount
	d.IndexdbActiveFileMerges = tm.ActiveFileMerges
	d.IndexdbActiveInmemoryMerges = tm.ActiveInmemoryMerges
	d.IndexdbFileMergesCount += tm.FileMergesCount
	d.IndexdbInmemoryMergesCount += tm.InmemoryMergesCount
	d.IndexdbFileItemsMerged += tm.FileItemsMerged
	d.IndexdbInmemoryItemsMerged += tm.InmemoryItemsMerged
}

// ==================== Stream Tag Lookup ====================

// appendStreamString returns the human-readable stream tags string for a stream ID.
// Used for logging and displaying stream information.
func (idb *indexdb) appendStreamString(dst []byte, sid *streamID) []byte {
	dstLen := len(dst)
	dst = idb.appendStreamTagsByStreamID(dst, sid)
	if len(dst) == dstLen {
		// Couldn't find stream tags by sid. This may be the case when the corresponding log stream
		// was recently registered and its tags aren't visible to search yet.
		// The stream tags must become visible in a few seconds.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/6042
		return dst
	}

	st := GetStreamTags()
	streamTagsCanonical := bytesutil.ToUnsafeString(dst[dstLen:])
	mustUnmarshalStreamTags(st, streamTagsCanonical)
	dst = st.marshalString(dst[:dstLen])
	PutStreamTags(st)

	return dst
}

// appendStreamTagsByStreamID looks up the canonical stream tags for a stream ID.
// Returns the tags as a canonical string (e.g., {app="nginx",host="server1"}).
func (idb *indexdb) appendStreamTagsByStreamID(dst []byte, sid *streamID) []byte {
	is := idb.getIndexSearch()
	defer idb.putIndexSearch(is)

	ts := &is.ts
	kb := &is.kb

	// Build key: [nsPrefixStreamIDToStreamTags][tenantID][streamID]
	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixStreamIDToStreamTags, sid.tenantID)
	kb.B = sid.id.marshal(kb.B)

	// Look up the entry
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return dst
		}
		logger.Panicf("FATAL: unexpected error when searching for StreamTags by streamID=%s in indexdb: %s", sid, err)
	}
	data := ts.Item[len(kb.B):]
	dst = append(dst, data...)
	return dst
}

// hasStreamID checks if a stream with the given ID exists in the index.
// This is used during ingestion to avoid re-registering known streams.
func (idb *indexdb) hasStreamID(sid *streamID) bool {
	is := idb.getIndexSearch()
	defer idb.putIndexSearch(is)

	ts := &is.ts
	kb := &is.kb

	// Build key: [nsPrefixStreamID][tenantID][streamID]
	kb.B = marshalCommonPrefix(kb.B, nsPrefixStreamID, sid.tenantID)
	kb.B = sid.id.marshal(kb.B)

	// Check for exact match
	if err := ts.FirstItemWithPrefix(kb.B); err != nil {
		if err == io.EOF {
			return false
		}
		logger.Panicf("FATAL: unexpected error when searching for streamID=%s in indexdb: %s", sid, err)
	}
	return len(kb.B) == len(ts.Item)
}

// ==================== Index Search Pool ====================

// indexSearch is a reusable structure for searching the index.
// Pooling reduces allocations during high-throughput queries.
type indexSearch struct {
	idb *indexdb
	ts  mergeset.TableSearch
	kb  bytesutil.ByteBuffer
}

// getIndexSearch retrieves an indexSearch from the pool.
func (idb *indexdb) getIndexSearch() *indexSearch {
	v := idb.indexSearchPool.Get()
	if v == nil {
		v = &indexSearch{
			idb: idb,
		}
	}
	is := v.(*indexSearch)
	is.ts.Init(idb.tb, false)
	return is
}

// putIndexSearch returns an indexSearch to the pool.
func (idb *indexdb) putIndexSearch(is *indexSearch) {
	is.idb = nil
	is.ts.MustClose()
	is.kb.Reset()

	idb.indexSearchPool.Put(is)
}

// ==================== Stream ID Search ====================

// searchStreamIDs finds all stream IDs matching the given filter for the specified tenants.
//
// This is the core function for query optimization - it returns the set of streams
// that match the stream label filter, allowing the query to skip non-matching streams.
//
// The function uses a two-tier caching strategy:
//  1. Check filterStreamCache for cached results (fast path)
//  2. If miss, search indexdb and cache the results (slow path)
func (idb *indexdb) searchStreamIDs(tenantIDs []TenantID, sf *StreamFilter) []streamID {
	// Try obtaining streamIDs from cache
	streamIDs, ok := idb.loadStreamIDsFromCache(tenantIDs, sf)
	if ok {
		// Fast path - streamIDs found in the cache.
		return streamIDs
	}

	// Slow path - collect streamIDs from indexdb.

	// Collect streamIDs for all the specified tenantIDs.
	is := idb.getIndexSearch()
	m := make(map[streamID]struct{})
	for _, tenantID := range tenantIDs {
		for _, asf := range sf.orFilters {
			is.updateStreamIDs(m, tenantID, asf)
		}
	}
	idb.putIndexSearch(is)

	// Convert the collected streamIDs from m to sorted slice.
	streamIDs = make([]streamID, 0, len(m))
	for streamID := range m {
		streamIDs = append(streamIDs, streamID)
	}
	sortStreamIDs(streamIDs)

	// Store the collected streamIDs to cache.
	idb.storeStreamIDsToCache(tenantIDs, sf, streamIDs)

	return streamIDs
}

// sortStreamIDs sorts stream IDs by (tenantID, streamID) for consistent ordering.
func sortStreamIDs(streamIDs []streamID) {
	sort.Slice(streamIDs, func(i, j int) bool {
		return streamIDs[i].less(&streamIDs[j])
	})
}

// updateStreamIDs adds stream IDs matching the AND filter to the destination map.
// For an AND filter, the result is the intersection of all tag filter results.
func (is *indexSearch) updateStreamIDs(dst map[streamID]struct{}, tenantID TenantID, asf *andStreamFilter) {
	var m map[u128]struct{}
	for _, tf := range asf.tagFilters {
		ids := is.getStreamIDsForTagFilter(tenantID, tf)
		if len(ids) == 0 {
			// There is no need in checking the remaining filters,
			// since the result will be empty in any case.
			return
		}
		if m == nil {
			m = ids
		} else {
			// Intersection: remove IDs not in the new set
			for id := range m {
				if _, ok := ids[id]; !ok {
					delete(m, id)
				}
			}
		}
	}

	// Add matched stream IDs to destination with tenant ID
	var sid streamID
	for id := range m {
		sid.tenantID = tenantID
		sid.id = id
		dst[sid] = struct{}{}
	}
}

// getStreamIDsForTagFilter returns stream IDs matching a single tag filter.
// Handles all filter operators: =, !=, =~, !~
func (is *indexSearch) getStreamIDsForTagFilter(tenantID TenantID, tf *streamTagFilter) map[u128]struct{} {
	switch tf.op {
	case "=":
		if tf.value == "" {
			// (field="") - find streams WITHOUT this tag
			return is.getStreamIDsForEmptyTagValue(tenantID, tf.tagName)
		}
		// (field="value") - exact match
		return is.getStreamIDsForNonEmptyTagValue(tenantID, tf.tagName, tf.value)
	case "!=":
		if tf.value == "" {
			// (field!="") - find streams WITH this tag (any value)
			return is.getStreamIDsForTagName(tenantID, tf.tagName)
		}
		// (field!="value") => (all streams) minus (streams with value)
		ids := is.getStreamIDsForTenant(tenantID)
		idsForTag := is.getStreamIDsForNonEmptyTagValue(tenantID, tf.tagName, tf.value)
		for id := range idsForTag {
			delete(ids, id)
		}
		return ids
	case "=~":
		re := tf.regexp
		if re.MatchString("") {
			// (field=~"|re") => (field="" or field=~"re")
			// Regex matches empty string, so include streams without the tag
			ids := is.getStreamIDsForEmptyTagValue(tenantID, tf.tagName)
			idsForRe := is.getStreamIDsForTagRegexp(tenantID, tf.tagName, re)
			for id := range idsForRe {
				ids[id] = struct{}{}
			}
			return ids
		}
		// Standard regex match
		return is.getStreamIDsForTagRegexp(tenantID, tf.tagName, re)
	case "!~":
		re := tf.regexp
		if re.MatchString("") {
			// (field!~"|re") => (field!="" and not field=~"re")
			// Regex matches empty string, exclude streams without the tag
			ids := is.getStreamIDsForTagName(tenantID, tf.tagName)
			if len(ids) == 0 {
				return ids
			}
			idsForRe := is.getStreamIDsForTagRegexp(tenantID, tf.tagName, re)
			for id := range idsForRe {
				delete(ids, id)
			}
			return ids
		}
		// (field!~"re") => (all streams) minus (streams matching regex)
		ids := is.getStreamIDsForTenant(tenantID)
		idsForRe := is.getStreamIDsForTagRegexp(tenantID, tf.tagName, re)
		for id := range idsForRe {
			delete(ids, id)
		}
		return ids
	default:
		logger.Panicf("BUG: unexpected operation in stream tag filter: %q", tf.op)
		return nil
	}
}

// getStreamIDsForNonEmptyTagValue finds streams with tagName=tagValue.
func (is *indexSearch) getStreamIDsForNonEmptyTagValue(tenantID TenantID, tagName, tagValue string) map[u128]struct{} {
	ids := make(map[u128]struct{})
	var sp tagToStreamIDsRowParser

	ts := &is.ts
	kb := &is.kb
	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixTagToStreamIDs, tenantID)
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagName))
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagValue))
	prefix := kb.B
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		tail := item[len(prefix):]
		sp.UpdateStreamIDs(ids, tail)
	}
	if err := ts.Error(); err != nil {
		logger.Panicf("FATAL: unexpected error: %s", err)
	}

	return ids
}

// getStreamIDsForEmptyTagValue finds streams that do NOT have tagName.
// This is computed as: (all streams) minus (streams with tagName).
func (is *indexSearch) getStreamIDsForEmptyTagValue(tenantID TenantID, tagName string) map[u128]struct{} {
	ids := is.getStreamIDsForTenant(tenantID)
	idsForTag := is.getStreamIDsForTagName(tenantID, tagName)
	for id := range idsForTag {
		delete(ids, id)
	}
	return ids
}

// getStreamIDsForTenant returns all stream IDs for a tenant.
func (is *indexSearch) getStreamIDsForTenant(tenantID TenantID) map[u128]struct{} {
	ids := make(map[u128]struct{})
	ts := &is.ts
	kb := &is.kb
	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixStreamID, tenantID)
	prefix := kb.B
	ts.Seek(prefix)
	var id u128
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		tail, err := id.unmarshal(item[len(prefix):])
		if err != nil {
			logger.Panicf("FATAL: cannot unmarshal streamID from (tenantID:streamID) entry: %s", err)
		}
		if len(tail) > 0 {
			logger.Panicf("FATAL: unexpected non-empty tail left after unmarshaling streamID from (tenantID:streamID); tail len=%d", len(tail))
		}
		ids[id] = struct{}{}
	}
	if err := ts.Error(); err != nil {
		logger.Panicf("FATAL: unexpected error: %s", err)
	}

	return ids
}

// getStreamIDsForTagName finds streams that have tagName with ANY value.
func (is *indexSearch) getStreamIDsForTagName(tenantID TenantID, tagName string) map[u128]struct{} {
	ids := make(map[u128]struct{})
	var sp tagToStreamIDsRowParser

	ts := &is.ts
	kb := &is.kb
	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixTagToStreamIDs, tenantID)
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagName))
	prefix := kb.B
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		tail := item[len(prefix):]
		n := bytes.IndexByte(tail, tagSeparatorChar)
		if n < 0 {
			logger.Panicf("FATAL: cannot find the end of tag value")
		}
		tail = tail[n+1:]
		sp.UpdateStreamIDs(ids, tail)
	}
	if err := ts.Error(); err != nil {
		logger.Panicf("FATAL: unexpected error: %s", err)
	}

	return ids
}

// getStreamIDsForTagRegexp finds streams where tagName matches the regex.
func (is *indexSearch) getStreamIDsForTagRegexp(tenantID TenantID, tagName string, re *regexutil.PromRegex) map[u128]struct{} {
	ids := make(map[u128]struct{})
	var sp tagToStreamIDsRowParser
	var tagValue, prevMatchingTagValue []byte
	var err error

	ts := &is.ts
	kb := &is.kb
	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixTagToStreamIDs, tenantID)
	kb.B = marshalTagValue(kb.B, bytesutil.ToUnsafeBytes(tagName))
	prefix := kb.B
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		tail := item[len(prefix):]
		tail, tagValue, err = unmarshalTagValue(tagValue[:0], tail)
		if err != nil {
			logger.Panicf("FATAL: cannot unmarshal tag value: %s", err)
		}
		// Optimization: skip regex check if same value as previous match
		if !bytes.Equal(tagValue, prevMatchingTagValue) {
			if !re.MatchString(bytesutil.ToUnsafeString(tagValue)) {
				continue
			}
			prevMatchingTagValue = append(prevMatchingTagValue[:0], tagValue...)
		}
		sp.UpdateStreamIDs(ids, tail)
	}
	if err := ts.Error(); err != nil {
		logger.Panicf("FATAL: unexpected error: %s", err)
	}

	return ids
}

// getTenantIDs returns all tenant IDs that have streams in this partition.
func (is *indexSearch) getTenantIDs() []TenantID {
	var tenantIDs []TenantID // return as result
	var tenantID TenantID    // variable for unmarshal

	ts := &is.ts
	kb := &is.kb

	kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixStreamID, tenantID)
	ts.Seek(kb.B)

	for ts.NextItem() {
		_, prefix, err := unmarshalCommonPrefix(&tenantID, ts.Item)
		if err != nil {
			logger.Panicf("FATAL: cannot unmarshal tenantID: %s", err)
		}
		if prefix != nsPrefixStreamID {
			// Reached the end of entries with the needed prefix.
			break
		}
		tenantIDs = append(tenantIDs, tenantID)
		// Seek for the next (accountID, projectID)
		tenantID.ProjectID++
		if tenantID.ProjectID == 0 {
			tenantID.AccountID++
			if tenantID.AccountID == 0 {
				// Reached the end (accountID, projectID) space
				break
			}
		}

		kb.B = marshalCommonPrefix(kb.B[:0], nsPrefixStreamID, tenantID)
		ts.Seek(kb.B)
	}

	if err := ts.Error(); err != nil {
		logger.Panicf("FATAL: error when searching for tenant ids: %s", err)
	}

	return tenantIDs
}

// ==================== Stream Registration ====================

// mustRegisterStream adds a new stream to the index.
// This creates three types of entries:
//  1. Stream existence marker (nsPrefixStreamID)
//  2. Stream ID to tags mapping (nsPrefixStreamIDToStreamTags)
//  3. Tag to stream ID reverse entries (nsPrefixTagToStreamIDs) for each tag
func (idb *indexdb) mustRegisterStream(streamID *streamID, streamTagsCanonical string) {
	st := GetStreamTags()
	mustUnmarshalStreamTags(st, streamTagsCanonical)
	tenantID := streamID.tenantID

	bi := getBatchItems()
	buf := bi.buf[:0]
	items := bi.items[:0]

	// Register tenantID:streamID entry (existence marker).
	bufLen := len(buf)
	buf = marshalCommonPrefix(buf, nsPrefixStreamID, tenantID)
	buf = streamID.id.marshal(buf)
	items = append(items, buf[bufLen:])

	// Register tenantID:streamID -> streamTagsCanonical entry.
	bufLen = len(buf)
	buf = marshalCommonPrefix(buf, nsPrefixStreamIDToStreamTags, tenantID)
	buf = streamID.id.marshal(buf)
	buf = append(buf, streamTagsCanonical...)
	items = append(items, buf[bufLen:])

	// Register tenantID:name:value -> streamIDs entries for each tag.
	tags := st.tags
	for i := range tags {
		bufLen = len(buf)
		buf = marshalCommonPrefix(buf, nsPrefixTagToStreamIDs, tenantID)
		buf = tags[i].indexdbMarshal(buf)
		buf = streamID.id.marshal(buf)
		items = append(items, buf[bufLen:])
	}
	PutStreamTags(st)

	// Add items to the storage
	idb.tb.AddItems(items)

	bi.buf = buf
	bi.items = items
	putBatchItems(bi)

	idb.streamsCreatedTotal.Add(1)
}

// ==================== Cache Management ====================

// invalidateStreamFilterCache is called when new data is added to the index.
// It increments the generation counter, invalidating all cached filter results.
func (idb *indexdb) invalidateStreamFilterCache() {
	// This function must be fast, since it is called each
	// time new indexdb entry is added.
	idb.filterStreamCacheGeneration.Add(1)
}

// marshalStreamFilterCacheKey creates a unique cache key for a stream filter query.
// The key includes the cache generation, partition name, tenant IDs, and filter.
func (idb *indexdb) marshalStreamFilterCacheKey(dst []byte, tenantIDs []TenantID, sf *StreamFilter) []byte {
	dst = encoding.MarshalUint32(dst, idb.filterStreamCacheGeneration.Load())
	dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(idb.partitionName))
	dst = encoding.MarshalVarUint64(dst, uint64(len(tenantIDs)))
	for i := range tenantIDs {
		dst = tenantIDs[i].marshal(dst)
	}
	dst = sf.marshalForCacheKey(dst)
	return dst
}

// loadStreamIDsFromCache attempts to load cached stream IDs for a filter query.
// Returns (nil, false) on cache miss.
func (idb *indexdb) loadStreamIDsFromCache(tenantIDs []TenantID, sf *StreamFilter) ([]streamID, bool) {
	bb := bbPool.Get()
	bb.B = idb.marshalStreamFilterCacheKey(bb.B[:0], tenantIDs, sf)
	v, ok := idb.s.filterStreamCache.Get(bb.B)
	bbPool.Put(bb)
	if !ok {
		// Cache miss
		return nil, false
	}
	// Cache hit - unpack streamIDs from data.
	data := *(v.(*[]byte))
	n, nSize := encoding.UnmarshalVarUint64(data)
	if nSize <= 0 {
		logger.Panicf("BUG: unexpected error when unmarshaling the number of streamIDs from cache")
	}
	src := data[nSize:]
	streamIDs := make([]streamID, n)
	for i := uint64(0); i < n; i++ {
		tail, err := streamIDs[i].unmarshal(src)
		if err != nil {
			logger.Panicf("BUG: unexpected error when unmarshaling streamID #%d: %s", i, err)
		}
		src = tail
	}
	if len(src) > 0 {
		logger.Panicf("BUG: unexpected non-empty tail left with len=%d", len(src))
	}
	return streamIDs, true
}

// storeStreamIDsToCache caches the result of a stream filter query.
func (idb *indexdb) storeStreamIDsToCache(tenantIDs []TenantID, sf *StreamFilter, streamIDs []streamID) {
	// marshal streamIDs
	var b []byte
	b = encoding.MarshalVarUint64(b, uint64(len(streamIDs)))
	for i := 0; i < len(streamIDs); i++ {
		b = streamIDs[i].marshal(b)
	}

	// Store marshaled streamIDs to cache.
	bb := bbPool.Get()
	bb.B = idb.marshalStreamFilterCacheKey(bb.B[:0], tenantIDs, sf)
	idb.s.filterStreamCache.Set(bb.B, &b)
	bbPool.Put(bb)
}

// searchTenants returns all tenant IDs with streams in this partition.
func (idb *indexdb) searchTenants() []TenantID {
	is := idb.getIndexSearch()
	defer idb.putIndexSearch(is)

	return is.getTenantIDs()
}

// ==================== Helper Structures ====================

// batchItems is a reusable buffer for batch index insertions.
// Pooling reduces allocations during stream registration.
type batchItems struct {
	buf []byte

	items [][]byte
}

func (bi *batchItems) reset() {
	bi.buf = bi.buf[:0]

	items := bi.items
	for i := range items {
		items[i] = nil
	}
	bi.items = items[:0]
}

func getBatchItems() *batchItems {
	v := batchItemsPool.Get()
	if v == nil {
		return &batchItems{}
	}
	return v.(*batchItems)
}

func putBatchItems(bi *batchItems) {
	bi.reset()
	batchItemsPool.Put(bi)
}

var batchItemsPool sync.Pool

// ==================== Tag-to-StreamIDs Merge Logic ====================
//
// When index entries are merged (during compaction), we optimize by combining
// multiple tag-to-streamID entries that share the same (tenantID, tagName, tagValue).
// This reduces storage overhead and improves query performance.

// mergeTagToStreamIDsRows is a mergeset callback that combines tag-to-streamID entries.
// During compaction, multiple entries like:
//
//	[tenantID][tag][value] -> [streamID1]
//	[tenantID][tag][value] -> [streamID2]
//
// Are merged into:
//
//	[tenantID][tag][value] -> [streamID1][streamID2]
//
// This reduces the number of index entries while maintaining the same information.
func mergeTagToStreamIDsRows(data []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
	// Perform quick checks whether items contain rows starting from nsPrefixTagToStreamIDs
	// based on the fact that items are sorted.
	if len(items) <= 2 {
		// The first and the last row must remain unchanged.
		return data, items
	}
	firstItem := items[0].Bytes(data)
	if len(firstItem) > 0 && firstItem[0] > nsPrefixTagToStreamIDs {
		return data, items
	}
	lastItem := items[len(items)-1].Bytes(data)
	if len(lastItem) > 0 && lastItem[0] < nsPrefixTagToStreamIDs {
		return data, items
	}

	// items contain at least one row starting from nsPrefixTagToStreamIDs. Merge rows with common tag.
	tsm := getTagToStreamIDsRowsMerger()
	tsm.dataCopy = append(tsm.dataCopy[:0], data...)
	tsm.itemsCopy = append(tsm.itemsCopy[:0], items...)
	sp := &tsm.sp
	spPrev := &tsm.spPrev
	dstData := data[:0]
	dstItems := items[:0]
	for i, it := range items {
		item := it.Bytes(data)
		if len(item) == 0 || item[0] != nsPrefixTagToStreamIDs || i == 0 || i == len(items)-1 {
			// Write rows not starting with nsPrefixTagToStreamIDs as-is.
			// Additionally write the first and the last row as-is in order to preserve
			// sort order for adjacent blocks.
			dstData, dstItems = tsm.flushPendingStreamIDs(dstData, dstItems, spPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, mergeset.Item{
				Start: uint32(len(dstData) - len(item)),
				End:   uint32(len(dstData)),
			})
			continue
		}
		if err := sp.Init(item); err != nil {
			logger.Panicf("FATAL: cannot parse row during merge: %s", err)
		}
		if sp.StreamIDsLen() >= maxStreamIDsPerRow {
			dstData, dstItems = tsm.flushPendingStreamIDs(dstData, dstItems, spPrev)
			dstData = append(dstData, item...)
			dstItems = append(dstItems, mergeset.Item{
				Start: uint32(len(dstData) - len(item)),
				End:   uint32(len(dstData)),
			})
			continue
		}
		if !sp.EqualPrefix(spPrev) {
			dstData, dstItems = tsm.flushPendingStreamIDs(dstData, dstItems, spPrev)
		}
		sp.ParseStreamIDs()
		tsm.pendingStreamIDs = append(tsm.pendingStreamIDs, sp.StreamIDs...)
		spPrev, sp = sp, spPrev
		if len(tsm.pendingStreamIDs) >= maxStreamIDsPerRow {
			dstData, dstItems = tsm.flushPendingStreamIDs(dstData, dstItems, spPrev)
		}
	}
	if len(tsm.pendingStreamIDs) > 0 {
		logger.Panicf("BUG: tsm.pendingStreamIDs must be empty at this point; got %d items", len(tsm.pendingStreamIDs))
	}
	if !checkItemsSorted(dstData, dstItems) {
		// Items could become unsorted if initial items contain duplicate streamIDs:
		//
		//   item1: 1, 1, 5
		//   item2: 1, 4
		//
		// Items could become the following after the merge:
		//
		//   item1: 1, 5
		//   item2: 1, 4
		//
		// i.e. item1 > item2
		//
		// Leave the original items unmerged, so they can be merged next time.
		// This case should be quite rare - if multiple data points are simultaneously inserted
		// into the same new time series from multiple concurrent goroutines.
		dstData = append(dstData[:0], tsm.dataCopy...)
		dstItems = append(dstItems[:0], tsm.itemsCopy...)
		if !checkItemsSorted(dstData, dstItems) {
			logger.Panicf("BUG: the original items weren't sorted; items=%q", dstItems)
		}
	}
	putTagToStreamIDsRowsMerger(tsm)
	return dstData, dstItems
}

// maxStreamIDsPerRow limits the number of streamIDs stored per tag-to-streamIDs row.
// This prevents individual rows from becoming too large, which would hurt index performance.
const maxStreamIDsPerRow = 32

// u128Sorter implements sort.Interface for 128-bit IDs.
type u128Sorter []u128

func (s u128Sorter) Len() int { return len(s) }
func (s u128Sorter) Less(i, j int) bool {
	return s[i].less(&s[j])
}
func (s u128Sorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// tagToStreamIDsRowsMerger holds state for merging tag-to-streamID entries.
type tagToStreamIDsRowsMerger struct {
	pendingStreamIDs u128Sorter
	sp               tagToStreamIDsRowParser
	spPrev           tagToStreamIDsRowParser

	itemsCopy []mergeset.Item
	dataCopy  []byte
}

func (tsm *tagToStreamIDsRowsMerger) Reset() {
	tsm.pendingStreamIDs = tsm.pendingStreamIDs[:0]
	tsm.sp.Reset()
	tsm.spPrev.Reset()

	tsm.itemsCopy = tsm.itemsCopy[:0]
	tsm.dataCopy = tsm.dataCopy[:0]
}

// flushPendingStreamIDs writes accumulated stream IDs as a merged index entry.
func (tsm *tagToStreamIDsRowsMerger) flushPendingStreamIDs(dstData []byte, dstItems []mergeset.Item, sp *tagToStreamIDsRowParser) ([]byte, []mergeset.Item) {
	if len(tsm.pendingStreamIDs) == 0 {
		// Nothing to flush
		return dstData, dstItems
	}
	// Use sort.Sort instead of sort.Slice in order to reduce memory allocations.
	sort.Sort(&tsm.pendingStreamIDs)
	tsm.pendingStreamIDs = removeDuplicateStreamIDs(tsm.pendingStreamIDs)

	// Marshal pendingStreamIDs
	dstDataLen := len(dstData)
	dstData = sp.MarshalPrefix(dstData)
	pendingStreamIDs := tsm.pendingStreamIDs
	for i := range pendingStreamIDs {
		dstData = pendingStreamIDs[i].marshal(dstData)
	}
	dstItems = append(dstItems, mergeset.Item{
		Start: uint32(dstDataLen),
		End:   uint32(len(dstData)),
	})
	tsm.pendingStreamIDs = tsm.pendingStreamIDs[:0]
	return dstData, dstItems
}

// removeDuplicateStreamIDs removes duplicates from a sorted slice of stream IDs.
func removeDuplicateStreamIDs(sortedStreamIDs []u128) []u128 {
	if len(sortedStreamIDs) < 2 {
		return sortedStreamIDs
	}
	hasDuplicates := false
	for i := 1; i < len(sortedStreamIDs); i++ {
		if sortedStreamIDs[i-1] == sortedStreamIDs[i] {
			hasDuplicates = true
			break
		}
	}
	if !hasDuplicates {
		return sortedStreamIDs
	}
	dstStreamIDs := sortedStreamIDs[:1]
	for i := 1; i < len(sortedStreamIDs); i++ {
		if sortedStreamIDs[i-1] == sortedStreamIDs[i] {
			continue
		}
		dstStreamIDs = append(dstStreamIDs, sortedStreamIDs[i])
	}
	return dstStreamIDs
}

func getTagToStreamIDsRowsMerger() *tagToStreamIDsRowsMerger {
	v := tsmPool.Get()
	if v == nil {
		return &tagToStreamIDsRowsMerger{}
	}
	return v.(*tagToStreamIDsRowsMerger)
}

func putTagToStreamIDsRowsMerger(tsm *tagToStreamIDsRowsMerger) {
	tsm.Reset()
	tsmPool.Put(tsm)
}

var tsmPool sync.Pool

// tagToStreamIDsRowParser parses entries of the form:
// [tenantID][tagName][tagValue][streamID1][streamID2]...
type tagToStreamIDsRowParser struct {
	// TenantID contains TenantID of the parsed row
	TenantID TenantID

	// StreamIDs contains parsed StreamIDs after ParseStreamIDs call
	StreamIDs []u128

	// streamIDsParsed is set to true after ParseStreamIDs call
	streamIDsParsed bool

	// Tag contains parsed tag after Init call
	Tag streamTag

	// tail contains the remaining unparsed streamIDs
	tail []byte
}

func (sp *tagToStreamIDsRowParser) Reset() {
	sp.TenantID.Reset()
	sp.StreamIDs = sp.StreamIDs[:0]
	sp.streamIDsParsed = false
	sp.Tag.reset()
	sp.tail = nil
}

// Init initializes sp from b, which should contain encoded tenantID:name:value -> streamIDs row.
func (sp *tagToStreamIDsRowParser) Init(b []byte) error {
	tail, nsPrefix, err := unmarshalCommonPrefix(&sp.TenantID, b)
	if err != nil {
		return fmt.Errorf("invalid tenantID:name:value -> streamIDs row %q: %w", b, err)
	}
	if nsPrefix != nsPrefixTagToStreamIDs {
		return fmt.Errorf("invalid prefix for tenantID:name:value -> streamIDs row %q; got %d; want %d", b, nsPrefix, nsPrefixTagToStreamIDs)
	}
	tail, err = sp.Tag.indexdbUnmarshal(tail)
	if err != nil {
		return fmt.Errorf("cannot unmarshal tag from tenantID:name:value -> streamIDs row %q: %w", b, err)
	}
	if err = sp.InitOnlyTail(tail); err != nil {
		return fmt.Errorf("cannot initialize tail from tenantID:name:value -> streamIDs row %q: %w", b, err)
	}
	return nil
}

// MarshalPrefix marshals row prefix (tenantID + tag) without streamIDs.
func (sp *tagToStreamIDsRowParser) MarshalPrefix(dst []byte) []byte {
	dst = marshalCommonPrefix(dst, nsPrefixTagToStreamIDs, sp.TenantID)
	dst = sp.Tag.indexdbMarshal(dst)
	return dst
}

// InitOnlyTail initializes sp.tail from a byte slice containing just streamIDs.
func (sp *tagToStreamIDsRowParser) InitOnlyTail(tail []byte) error {
	if len(tail) == 0 {
		return fmt.Errorf("missing streamID in the tenantID:name:value -> streamIDs row")
	}
	if len(tail)%16 != 0 {
		return fmt.Errorf("invalid tail length in the tenantID:name:value -> streamIDs row; got %d bytes; must be multiple of 16 bytes", len(tail))
	}
	sp.tail = tail
	sp.streamIDsParsed = false
	return nil
}

// EqualPrefix returns true if two rows have the same (tenantID, tagName, tagValue).
func (sp *tagToStreamIDsRowParser) EqualPrefix(x *tagToStreamIDsRowParser) bool {
	if !sp.TenantID.Equal(&x.TenantID) {
		return false
	}
	if !sp.Tag.equal(&x.Tag) {
		return false
	}
	return true
}

// StreamIDsLen returns the number of streamIDs in the row (without parsing them).
func (sp *tagToStreamIDsRowParser) StreamIDsLen() int {
	return len(sp.tail) / 16
}

// ParseStreamIDs parses the stream IDs from tail into sp.StreamIDs.
func (sp *tagToStreamIDsRowParser) ParseStreamIDs() {
	if sp.streamIDsParsed {
		return
	}
	tail := sp.tail
	n := len(tail) / 16
	sp.StreamIDs = slicesutil.SetLength(sp.StreamIDs, n)
	streamIDs := sp.StreamIDs
	_ = streamIDs[n-1]
	for i := 0; i < n; i++ {
		var err error
		tail, err = streamIDs[i].unmarshal(tail)
		if err != nil {
			logger.Panicf("FATAL: cannot unmarshal streamID: %s", err)
		}
	}
	sp.streamIDsParsed = true
}

// UpdateStreamIDs adds stream IDs from tail to the provided map.
func (sp *tagToStreamIDsRowParser) UpdateStreamIDs(ids map[u128]struct{}, tail []byte) {
	sp.Reset()
	if err := sp.InitOnlyTail(tail); err != nil {
		logger.Panicf("FATAL: cannot parse '(date, tag) -> streamIDs' row: %s", err)
	}
	sp.ParseStreamIDs()
	for _, id := range sp.StreamIDs {
		ids[id] = struct{}{}
	}
}

// ==================== Key Encoding Utilities ====================

// commonPrefixLen is the length of the common prefix for all indexdb rows.
// Format: [1 byte namespace prefix] + [8 bytes tenant ID]
const commonPrefixLen = 1 + 8

// marshalCommonPrefix writes the namespace prefix and tenant ID to dst.
func marshalCommonPrefix(dst []byte, nsPrefix byte, tenantID TenantID) []byte {
	dst = append(dst, nsPrefix)
	dst = tenantID.marshal(dst)
	return dst
}

// unmarshalCommonPrefix extracts the namespace prefix and tenant ID from src.
func unmarshalCommonPrefix(dstTenantID *TenantID, src []byte) ([]byte, byte, error) {
	if len(src) < commonPrefixLen {
		return nil, 0, fmt.Errorf("cannot unmarshal common prefix from %d bytes; need at least %d bytes; data=%X", len(src), commonPrefixLen, src)
	}
	prefix := src[0]
	src = src[1:]
	tail, err := dstTenantID.unmarshal(src)
	if err != nil {
		return nil, 0, fmt.Errorf("cannot unmarshal tenantID: %w", err)
	}
	return tail, prefix, nil
}

// checkItemsSorted verifies that items are in sorted order.
func checkItemsSorted(data []byte, items []mergeset.Item) bool {
	if len(items) == 0 {
		return true
	}
	prevItem := items[0].String(data)
	for _, it := range items[1:] {
		currItem := it.String(data)
		if prevItem > currItem {
			return false
		}
		prevItem = currItem
	}
	return true
}
