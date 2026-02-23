// Package logstorage provides the core storage engine and query language implementation.
//
// This file implements the '| query_stats' pipe, which returns query execution statistics.
//
// Query Stats Pipe Overview:
//
// The query_stats pipe is unique because it doesn't process log data - it returns
// metadata about the query execution itself. This is useful for:
//   - Performance analysis: How many rows/blocks were scanned?
//   - Query optimization: Which parts of the query are slow?
//   - Monitoring: Track query resource usage over time
//
// Usage:
//
//   - | query_stats
//
// Returns columns like:
//   - rows_processed: Total rows examined during search
//   - rows_found: Rows that matched the filter
//   - blocks_processed: Number of data blocks read
//   - duration_nsecs: Query execution time
//
// Cluster Mode Split:
//
// In cluster mode, query_stats is split into two parts:
//   - Remote (this file): Runs on each vlstorage node, drains input blocks
//   - Local (pipe_query_stats_local.go): Runs on frontend, aggregates stats
//
// This split is necessary because:
//   - Stats are collected per-node during remote execution
//   - Stats are passed via a side channel (QueryStats) not through DataBlocks
//   - The local part aggregates stats from all nodes and outputs the final result
//
// See https://docs.victoriametrics.com/victorialogs/logsql/#query_stats-pipe
package logstorage

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipeQueryStats implements '| query_stats' pipe.
//
// This pipe is special - it doesn't transform data, it collects statistics.
// During execution, it reads all blocks (to simulate normal processing) and
// then outputs the accumulated QueryStats during flush().
type pipeQueryStats struct {
}

func (ps *pipeQueryStats) String() string {
	return "query_stats"
}

// splitToRemoteAndLocal splits query_stats for cluster execution.
//
// Returns:
//   - Remote: This pipe (pipeQueryStats) - drains input blocks on vlstorage nodes
//   - Local: pipeQueryStatsLocal - outputs the final stats
//
// The actual stats are passed via the QueryStats side channel, not through
// DataBlocks. This is why the remote part is essentially a no-op for data.
func (ps *pipeQueryStats) splitToRemoteAndLocal(_ int64) (pipe, []pipe) {
	psLocal := &pipeQueryStatsLocal{}
	return ps, []pipe{psLocal}
}

func (ps *pipeQueryStats) canLiveTail() bool {
	return false
}

func (ps *pipeQueryStats) canReturnLastNResults() bool {
	return false
}

func (ps *pipeQueryStats) updateNeededFields(pf *prefixfilter.Filter) {
	pf.AddAllowFilter("*")
}

func (ps *pipeQueryStats) hasFilterInWithQuery() bool {
	return false
}

func (ps *pipeQueryStats) initFilterInValues(_ *inValuesCache, _ getFieldValuesFunc, _ bool) (pipe, error) {
	return ps, nil
}

func (ps *pipeQueryStats) visitSubqueries(_ func(q *Query)) {
	// nothing to do
}

func (ps *pipeQueryStats) newPipeProcessor(_ int, _ <-chan struct{}, _ func(), ppNext pipeProcessor) pipeProcessor {
	psp := &pipeQueryStatsProcessor{
		ps:     ps,
		ppNext: ppNext,
	}
	return psp
}

// pipeQueryStatsProcessor is the runtime processor for query_stats pipe.
//
// During writeBlock, it reads all data (to ensure upstream processing happens)
// but doesn't output anything. The actual stats output happens during flush()
// after the QueryStats have been populated.
type pipeQueryStatsProcessor struct {
	ps     *pipeQueryStats
	ppNext pipeProcessor

	// Per-worker shards for tracking data read
	shards atomicutil.Slice[pipeQueryStatsProcessorShard]

	// qs must be set via setQueryStats() before flush() call.
	// This is populated by runPipes after search completes.
	qs *QueryStats

	// queryDurationNsecs must be set via setQueryStats() before flush() call.
	queryDurationNsecs int64
}

// pipeQueryStatsProcessorShard holds per-worker state.
// The sink field prevents compiler optimization from eliminating the read loop.
type pipeQueryStatsProcessorShard struct {
	// sink is used for preventing from the elimination of the loop inside writeBlock by too smart compiler
	sink int
}

// setQueryStats is called by runPipes to inject the query stats before flush.
func (psp *pipeQueryStatsProcessor) setQueryStats(qs *QueryStats, queryDurationNsecs int64) {
	psp.qs = qs
	psp.queryDurationNsecs = queryDurationNsecs
}

// writeBlock reads all data from the block without outputting anything.
//
// This is necessary to ensure upstream processing (filtering, aggregation, etc.)
// actually happens, which populates the QueryStats. We read the data but don't
// pass it through - the stats will be output during flush().
func (psp *pipeQueryStatsProcessor) writeBlock(workerID uint, br *blockResult) {
	// Read all the data from br in order to emulate the default behaviour
	// when this data is returned back to the client if there is no query_stats pipe at the end of the query.
	shard := psp.shards.Get(workerID)

	cs := br.getColumns()
	for _, c := range cs {
		values := c.getValues(br)
		shard.sink += len(values)
	}
}

// flush outputs the accumulated query stats as a DataBlock.
//
// This is called after all search work is complete and QueryStats are populated.
// The stats are written as a single row with columns for each statistic.
func (psp *pipeQueryStatsProcessor) flush() error {
	psp.qs.writeToPipeProcessor(psp.ppNext, psp.queryDurationNsecs)
	return nil
}

// parsePipeQueryStats parses the query_stats pipe from the lexer.
func parsePipeQueryStats(lex *lexer) (pipe, error) {
	if !lex.isKeyword("query_stats") {
		return nil, fmt.Errorf("expecting 'query_stats'; got %q", lex.token)
	}
	lex.nextToken()

	ps := &pipeQueryStats{}

	return ps, nil
}
