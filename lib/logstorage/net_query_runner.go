// Package logstorage provides the core storage engine and query execution for VictoriaLogs.
// This file (net_query_runner.go) implements distributed query splitting and execution.
//
// Query Splitting Overview:
//
// In cluster mode, a LogsQL query is automatically split into two parts:
//  1. Remote pipes: Executed in parallel on all vlstorage nodes
//  2. Local pipes: Executed on the vlselect frontend after merging results
//
// The splitting is determined by each pipe type's splitToRemoteAndLocal() method:
//   - Remotable pipes (filter, stats, fields, delete, limit): Can run on storage nodes
//   - Local-only pipes (sort, uniq final, join, union): Must run on frontend after merge
//
// Example Query Splitting:
//
//	Query: * | filter level:error | stats count() by host | sort by count desc
//
//	Split result:
//	- Remote (runs on each vlstorage):
//	    * | filter level:error | stats count() by host
//	- Local (runs on frontend after merging):
//	    merge stats results | sort by count desc
//
// Why Split Queries?
//
//  1. Performance: Filters run on storage nodes, reducing data transfer
//  2. Correctness: Some operations (sort, limit) must see all data to be correct
//  3. Resource efficiency: Aggregate early (stats), merge once (local pipes)
//
// Field Selection Optimization:
//
// After splitting, the remote query is modified to select only the fields needed
// by local pipes. This reduces network transfer for queries like:
//
//   - | filter level:error | fields message
//
// Only the "message" field is transferred from storage nodes.
package logstorage

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// RunNetQueryFunc is the signature for a function that executes a distributed query.
// It runs the query in qctx and streams results to writeBlock.
// This is implemented by netselect.Storage.RunQuery.
type RunNetQueryFunc func(qctx *QueryContext, writeBlock WriteDataBlockFunc) error

// NetQueryRunner orchestrates distributed query execution across cluster nodes.
// It manages the split between remote and local pipe execution.
type NetQueryRunner struct {
	// qctx is the original query context from the client
	qctx *QueryContext

	// qRemote is the query to execute on remote storage nodes.
	// It contains only pipes that can be distributed (filter, stats, etc.)
	// and is limited to fields needed by local pipes.
	qRemote *Query

	// pipesLocal contains pipes that must execute locally after receiving
	// results from all storage nodes (sort, uniq final, join, etc.)
	pipesLocal []pipe

	// writeBlock is the final output function that receives merged results
	writeBlock writeBlockResultFunc
}

// NewNetQueryRunner creates a NetQueryRunner for distributed query execution.
//
// The creation process:
//  1. Initialize subqueries for pipes that need parallel execution (e.g., stats by bucket)
//  2. Split the query into remote and local pipes using splitQueryToRemoteAndLocal()
//  3. Prepare the writeBlock wrapper for result streaming
//
// Parameters:
//   - qctx: The query context containing the original query, tenant IDs, and stats
//   - runNetQuery: Function to execute the query on remote nodes (netselect.Storage.RunQuery)
//   - writeNetBlock: Function to receive final merged results
func NewNetQueryRunner(qctx *QueryContext, runNetQuery RunNetQueryFunc, writeNetBlock WriteDataBlockFunc) (*NetQueryRunner, error) {
	runQuery := func(qctx *QueryContext, writeBlock writeBlockResultFunc) error {
		writeNetBlock := writeBlock.newDataBlockWriter()
		return runNetQuery(qctx, writeNetBlock)
	}

	qNew, err := initSubqueries(qctx, runQuery, false)
	if err != nil {
		return nil, err
	}
	q := qNew

	qRemote, pipesLocal := splitQueryToRemoteAndLocal(q)

	writeBlock := writeNetBlock.newBlockResultWriter()

	nqr := &NetQueryRunner{
		qctx:       qctx,
		qRemote:    qRemote,
		pipesLocal: pipesLocal,
		writeBlock: writeBlock,
	}
	return nqr, nil
}

// Run executes the distributed query.
//
// Execution flow:
//  1. Create a search function that executes qRemote on all storage nodes via netSearch
//  2. Run the local pipes (pipesLocal) with the specified concurrency
//  3. Results from remote nodes are merged and passed through local pipes
//  4. Final results are written to nqr.writeBlock
//
// Parameters:
//   - ctx: Context for cancellation
//   - concurrency: Number of parallel goroutines for local pipe processing
//   - netSearch: Function that fans out qRemote to storage nodes (from netselect)
func (nqr *NetQueryRunner) Run(ctx context.Context, concurrency int, netSearch func(stopCh <-chan struct{}, q *Query, writeBlock WriteDataBlockFunc) error) error {
	search := func(stopCh <-chan struct{}, writeBlockToPipes writeBlockResultFunc) error {
		writeNetBlock := writeBlockToPipes.newDataBlockWriter()
		return netSearch(stopCh, nqr.qRemote, writeNetBlock)
	}

	qctxLocal := nqr.qctx.WithContext(ctx)
	return runPipes(qctxLocal, nqr.pipesLocal, search, nqr.writeBlock, concurrency)
}

// splitQueryToRemoteAndLocal divides a query into remotely and locally executed parts.
//
// The algorithm:
//  1. Clone the query and drop all existing pipes
//  2. Walk through original pipes, calling splitToRemoteAndLocal() on each
//  3. Add remotable pipes to qRemote until a non-remotable pipe is found
//  4. Add remaining pipes (and local parts of split pipes) to pipesLocal
//  5. Add field filters to qRemote to select only columns needed by pipesLocal
//
// This allows operations like filter and partial stats to run on storage nodes,
// while sort, final uniq, and merge operations run on the frontend.
func splitQueryToRemoteAndLocal(q *Query) (*Query, []pipe) {
	timestamp := q.GetTimestamp()
	qRemote := q.Clone(timestamp)
	qRemote.DropAllPipes()

	pipesRemote, pipesLocal := getRemoteAndLocalPipes(q)
	qRemote.pipes = pipesRemote

	// Limit fields to select at the remote storage.
	pf := getNeededColumns(pipesLocal)
	qRemote.addFieldsFilters(pf)

	return qRemote, pipesLocal
}

// getRemoteAndLocalPipes walks the pipe chain and splits it at the first non-remotable pipe.
//
// Each pipe implements splitToRemoteAndLocal() which returns:
//   - pRemote: The part that can run on storage nodes (nil if not remotable)
//   - psLocal: Parts that must run locally (empty if fully remotable)
//
// The split point is the first pipe where:
//   - pRemote is nil (pipe cannot run remotely), OR
//   - psLocal is non-empty (pipe has local-only components)
//
// All subsequent pipes are added to pipesLocal regardless of their remotability,
// since they need the output of a local-only pipe.
func getRemoteAndLocalPipes(q *Query) ([]pipe, []pipe) {
	timestamp := q.GetTimestamp()

	var pipesRemote []pipe
	var pipesLocal []pipe

	for i, p := range q.pipes {
		pRemote, psLocal := p.splitToRemoteAndLocal(timestamp)
		if pRemote != nil {
			pipesRemote = append(pipesRemote, pRemote)
			if len(psLocal) == 0 {
				continue
			}
		}

		if len(psLocal) == 0 {
			logger.Panicf("BUG: psLocal must be non non-empty here")
		}

		pipesLocal = append(pipesLocal, psLocal...)
		pipesLocal = append(pipesLocal, q.pipes[i+1:]...)
		break
	}

	return pipesRemote, pipesLocal
}
