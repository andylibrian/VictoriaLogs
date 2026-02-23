// Package logstorage provides the core storage engine and query language implementation.
//
// This file implements the local part of the query_stats pipe for cluster mode.
//
// Local Query Stats Pipe Overview:
//
// In cluster mode, the query_stats pipe is split into two coordinated parts:
//   - Remote (pipe_query_stats.go): Runs on each vlstorage node
//   - Local (this file): Runs on the vlselect frontend after merging results
//
// Why this split exists:
//
// Query statistics are collected per-node during query execution, not passed
// through DataBlocks. Instead, they're accumulated in the QueryStats object
// which is passed as a side channel through the QueryContext.
//
// The flow is:
//  1. Remote: Each vlstorage node collects its local stats during search
//  2. Stats are aggregated into QueryStats (atomic updates)
//  3. Local: Frontend outputs the aggregated stats as the final result
//
// The local processor doesn't receive any DataBlocks - it only outputs the
// accumulated stats during flush().
package logstorage

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeQueryStatsLocal processes the local part of query_stats in cluster mode.
//
// This pipe only exists after splitting for cluster execution. It's never
// parsed directly from LogsQL - it's created by pipeQueryStats.splitToRemoteAndLocal().
type pipeQueryStatsLocal struct {
}

func (ps *pipeQueryStatsLocal) String() string {
	return "query_stats_local"
}

// splitToRemoteAndLocal should never be called on the local part.
// The local part is already the result of a split - it can't be split further.
func (ps *pipeQueryStatsLocal) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	logger.Panicf("BUG: unexpected call for %T", ps)
	return nil, nil
}

func (ps *pipeQueryStatsLocal) canLiveTail() bool {
	return false
}

func (ps *pipeQueryStatsLocal) canReturnLastNResults() bool {
	return false
}

func (ps *pipeQueryStatsLocal) updateNeededFields(_ *prefixfilter.Filter) {
	// Nothing to do - query_stats doesn't need any input fields
}

func (ps *pipeQueryStatsLocal) hasFilterInWithQuery() bool {
	return false
}

func (ps *pipeQueryStatsLocal) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc, _ bool) (pipe, error) {
	return ps, nil
}

func (ps *pipeQueryStatsLocal) visitSubqueries(_ func(q *Query)) {
	// nothing to do
}

func (ps *pipeQueryStatsLocal) newPipeProcessor(_ int, stopCh <-chan struct{}, _ func(), ppNext pipeProcessor) pipeProcessor {
	psp := &pipeQueryStatsLocalProcessor{
		ppNext: ppNext,
	}
	return psp
}

// pipeQueryStatsLocalProcessor is the runtime processor for the local query_stats part.
//
// This processor receives no data blocks - all stats are passed via the QueryStats
// side channel. During flush(), it outputs the accumulated stats.
type pipeQueryStatsLocalProcessor struct {
	ppNext pipeProcessor

	// qs must be set before flush() call via setQueryStats()
	// This is populated by runPipes after all remote searches complete
	qs *QueryStats

	// queryDurationNsecs must be set before flush() call via setQueryStats()
	queryDurationNsecs int64
}

// setQueryStats is called by runPipes to inject the aggregated query stats.
func (psp *pipeQueryStatsLocalProcessor) setQueryStats(qs *QueryStats, queryDurationNsecs int64) {
	psp.qs = qs
	psp.queryDurationNsecs = queryDurationNsecs
}

// writeBlock does nothing - stats are passed via QueryStats, not DataBlocks.
//
// In cluster mode, the remote nodes have already collected their stats.
// Those stats are aggregated into QueryStats which we output during flush().
func (psp *pipeQueryStatsLocalProcessor) writeBlock(_ uint, _ *blockResult) {
	// Nothing to do - query stats is passed from the remote storage nodes via a side channel.
}

// flush outputs the aggregated query stats as a DataBlock.
//
// This is called after all remote searches complete and their stats have been
// aggregated into QueryStats. We output these stats as a single result row.
func (psp *pipeQueryStatsLocalProcessor) flush() error {
	psp.qs.writeToPipeProcessor(psp.ppNext, psp.queryDurationNsecs)
	return nil
}
