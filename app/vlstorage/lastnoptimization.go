// Package vlstorage provides the storage abstraction layer for VictoriaLogs.
// This file implements an optimization for "last N results" queries.
//
// Last N Results Optimization:
//
// When a query requests the last N log entries (a very common pattern like
// "show me the most recent 100 logs"), scanning all data would be extremely
// expensive for large time ranges. Instead, this optimization uses binary
// search over the time range to find the optimal window.
//
// Triggering Conditions:
//
// The optimization is triggered when:
//   - The query ends with | sort by (_time) desc | limit N
//   - Or the equivalent implicit pattern for "last N logs"
//
// This is detected by query.GetLastNResultsQuery() in the Query type.
//
// Algorithm Overview:
//
//  1. Fast path: Query 2×limit rows. If fewer than that, we're done.
//  2. Slow path: Binary search over the time range:
//     - Start with the more recent half of the time range
//     - If too many rows, narrow to more recent half
//     - If too few rows, expand to older half
//     - Repeat until ~limit rows found
//
// Why Binary Search Works:
//
// Log data is ordered by timestamp within each partition. By probing different
// time ranges, we can quickly find the time boundary that contains approximately
// N results, avoiding a full scan of potentially terabytes of data.
//
// Example:
//
//	Query: * | sort by (_time) desc | limit 100
//	Time range: 30 days
//
//	Instead of scanning 30 days of data:
//	1. Query last 15 days -> 500 rows (too many)
//	2. Query last 7.5 days -> 200 rows (too many)
//	3. Query last 3.75 days -> 80 rows (too few, keep these)
//	4. Query 3.75-7.5 days -> 150 rows (combine with step 3)
//	5. Return top 100 from combined ~230 rows
package vlstorage

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// runOptimizedLastNResultsQuery executes a query optimized for fetching the last N results.
//
// Instead of scanning all data and sorting, this uses binary search over the time
// range to find the optimal window containing approximately (offset + limit) rows.
//
// Parameters:
//   - qctx: Query context with the query to execute
//   - offset: Number of rows to skip (from pagination)
//   - limit: Maximum number of rows to return
//   - writeBlock: Callback to receive result rows
func runOptimizedLastNResultsQuery(qctx *logstorage.QueryContext, offset, limit uint64, writeBlock logstorage.WriteDataBlockFunc) error {
	rows, err := getLastNQueryResults(qctx, offset+limit)
	if err != nil {
		return err
	}
	// Apply offset
	if uint64(len(rows)) > offset {
		rows = rows[offset:]
	}

	// Stream results as DataBlocks
	var db logstorage.DataBlock
	var columns []logstorage.BlockColumn
	var values []string
	for _, r := range rows {
		columns = slicesutil.SetLength(columns, len(r.fields))
		values = slicesutil.SetLength(values, len(r.fields))
		for j, f := range r.fields {
			values[j] = f.Value
			columns[j].Name = f.Name
			columns[j].Values = values[j : j+1]
		}
		db.Columns = columns
		writeBlock(0, &db)
	}
	return nil
}

// getLastNQueryResults returns the last N log rows for the query using binary search.
//
// Algorithm:
//  1. Fast path: Query 2×limit. If ≤2×limit rows exist, we have all data.
//  2. Slow path: Binary search over time range to find ~limit rows
//
// The binary search maintains:
//   - rowsFound: Rows collected so far (from older time windows)
//   - lastNonEmptyRows: Rows from the most recent non-empty window (for merging at end)
//   - start, end: Current search window boundaries
func getLastNQueryResults(qctx *logstorage.QueryContext, limit uint64) ([]logRow, error) {
	timestamp := qctx.Query.GetTimestamp()

	// Try fast path: query 2×limit to check if time range is small
	q := qctx.Query.Clone(timestamp)
	q.AddPipeOffsetLimit(0, 2*limit)
	qctxLocal := qctx.WithQuery(q)
	rows, err := getQueryResults(qctxLocal)
	if err != nil {
		return nil, err
	}

	if uint64(len(rows)) < 2*limit {
		// Fast path: the entire time range has fewer than 2×limit rows
		rows = getLastNRows(rows, limit)
		return rows, nil
	}

	// Slow path: binary search over the time range
	start, end := q.GetFilterTimeRange()
	if end < math.MaxInt64 {
		end++
	}
	// Start search from the more recent half
	start += end/2 - start/2
	n := limit

	var rowsFound []logRow
	var lastNonEmptyRows []logRow

	for {
		q = qctx.Query.CloneWithTimeFilter(timestamp, start, end-1)
		q.AddPipeOffsetLimit(0, 2*n)
		qctxLocal = qctx.WithQuery(q)
		rows, err := getQueryResults(qctxLocal)
		if err != nil {
			return nil, err
		}

		if end/2-start/2 <= 0 {
			// Time range is now ≤1ns, can't narrow further
			// Combine all found rows and return
			rowsFound = append(rowsFound, lastNonEmptyRows...)
			rowsFound = append(rowsFound, rows...)
			rowsFound = getLastNRows(rowsFound, limit)
			return rowsFound, nil
		}

		if uint64(len(rows)) >= 2*n {
			// Too many rows: search in the more recent half
			if !logstorage.CanApplyLastNResultsOptimization(start, end) {
				// For very small time ranges, direct query is faster than binary search
				rows, err := getLogRowsLastN(qctx, start, end, n)
				if err != nil {
					return nil, err
				}
				rowsFound = append(rowsFound, rows...)
				rowsFound = getLastNRows(rowsFound, limit)
				return rowsFound, nil
			}
			start += end/2 - start/2
			lastNonEmptyRows = rows // Keep in case we need to merge later
			continue
		}
		if uint64(len(rowsFound)+len(rows)) >= limit {
			// Found enough rows
			rowsFound = append(rowsFound, rows...)
			rowsFound = getLastNRows(rowsFound, limit)
			return rowsFound, nil
		}

		// Too few rows: expand to older half
		rowsFound = append(rowsFound, rows...)
		n -= uint64(len(rows))

		d := end/2 - start/2
		end = start
		start -= d
	}
}

// getLogRowsLastN directly fetches the last N rows from a small time range.
// Used when the time range is small enough that binary search overhead isn't worth it.
func getLogRowsLastN(qctx *logstorage.QueryContext, start, end int64, n uint64) ([]logRow, error) {
	timestamp := qctx.Query.GetTimestamp()
	q := qctx.Query.CloneWithTimeFilter(timestamp, start, end)
	q.AddPipeSortByTimeDesc()
	q.AddPipeOffsetLimit(0, n)
	qctxLocal := qctx.WithQuery(q)
	return getQueryResults(qctxLocal)
}

// getQueryResults executes a query and collects all results into logRow slices.
// This is used by the optimization to collect intermediate results during binary search.
func getQueryResults(qctx *logstorage.QueryContext) ([]logRow, error) {
	var rowsLock sync.Mutex
	var rows []logRow

	var errLocal error
	var errLocalLock sync.Mutex

	writeBlock := func(_ uint, db *logstorage.DataBlock) {
		rowsLocal, err := getLogRowsFromDataBlock(db)
		if err != nil {
			errLocalLock.Lock()
			errLocal = err
			errLocalLock.Unlock()
		}

		rowsLock.Lock()
		rows = append(rows, rowsLocal...)
		rowsLock.Unlock()
	}

	err := RunQuery(qctx, writeBlock)
	if errLocal != nil {
		return nil, errLocal
	}

	return rows, err
}

// getLogRowsFromDataBlock converts a DataBlock to a slice of logRow structs.
// Each logRow contains a timestamp and all field name/value pairs.
func getLogRowsFromDataBlock(db *logstorage.DataBlock) ([]logRow, error) {
	timestamps, ok := db.GetTimestamps(nil)
	if !ok {
		return nil, fmt.Errorf("missing _time field in the query results")
	}

	columnNames := make([]string, len(db.Columns))
	for i, c := range db.Columns {
		columnNames[i] = strings.Clone(c.Name)
	}

	lrs := make([]logRow, 0, len(timestamps))
	// Pre-allocate field buffer for efficiency
	fieldsBuf := make([]logstorage.Field, 0, len(columnNames)*len(timestamps))

	for i, timestamp := range timestamps {
		fieldsBufLen := len(fieldsBuf)
		for j, c := range db.Columns {
			fieldsBuf = append(fieldsBuf, logstorage.Field{
				Name:  columnNames[j],
				Value: strings.Clone(c.Values[i]),
			})
		}
		lrs = append(lrs, logRow{
			timestamp: timestamp,
			fields:    fieldsBuf[fieldsBufLen:],
		})
	}

	return lrs, nil
}

// logRow represents a single log entry with timestamp and fields.
// Used internally by the last N optimization for sorting and filtering.
type logRow struct {
	timestamp int64
	fields    []logstorage.Field
}

// getLastNRows returns the last N rows (by timestamp, descending) from the input.
func getLastNRows(rows []logRow, limit uint64) []logRow {
	sortLogRows(rows)
	if uint64(len(rows)) > limit {
		rows = rows[:limit]
	}
	return rows
}

// sortLogRows sorts log rows by timestamp in descending order (newest first).
func sortLogRows(rows []logRow) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].timestamp > rows[j].timestamp
	})
}
