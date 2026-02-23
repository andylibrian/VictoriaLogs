// Package logstorage provides the core storage engine and query language implementation.
//
// This file (pipe.go) defines the pipe interface and pipe execution infrastructure.
// Pipes are the transformation operators in LogsQL, connected via | (pipe) operator.
//
// Pipe Architecture Overview:
//
// In LogsQL, a query consists of a filter followed by zero or more pipes:
//
//	error | filter level:ERROR | stats count() by host | sort by count desc
//
// Each pipe transforms a stream of DataBlocks:
//   - Input: DataBlocks from the previous stage (filter or previous pipe)
//   - Output: DataBlocks to the next stage (next pipe or final result)
//
// Key Design Patterns:
//
// 1. AST + Runtime Separation
//   - pipe interface: Represents parsed pipe semantics (immutable AST node)
//   - pipeProcessor interface: Runtime execution stage (mutable state)
//   - This separation allows pipes to be serialized, cloned, and split for distribution
//
// 2. Reverse Chain Assembly
//   - Pipes are created from tail to head during execution
//   - Each pipe wraps the next pipe's processor
//   - This allows each pipe to control flow to downstream pipes
//
// 3. Remote/Local Split
//   - In cluster mode, pipes can be split between remote storage nodes and local frontend
//   - splitToRemoteAndLocal() determines where each pipe executes
//   - Example: | filter runs remotely, | sort runs locally
//
// See onboarding/onboarding-logsql-parser-pipes.md for detailed documentation.
package logstorage

import (
	"fmt"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"
)

// pipe is the interface that all LogsQL pipe types must implement.
//
// A pipe represents a transformation stage in the query pipeline. Examples:
//   - | filter level:ERROR - filters rows
//   - | stats count() by host - aggregates rows
//   - | sort by _time desc - sorts rows
//   - | fields message - projects columns
//
// Pipes are created during parsing and remain immutable. Runtime state is
// managed by pipeProcessor instances created via newPipeProcessor().
type pipe interface {
	// String returns string representation of the pipe.
	// This should produce valid LogsQL that can be re-parsed.
	String() string

	// splitToRemoteAndLocal must return pipes for remote and local execution.
	//
	// In cluster mode, queries are split between:
	//   - Remote: Runs on each vlstorage node (push-down optimization)
	//   - Local: Runs on the vlselect frontend after merging remote results
	//
	// Return values:
	//   - pRemote != nil, psLocal empty: Pipe runs entirely remotely
	//   - pRemote == nil, psLocal non-empty: Pipe runs entirely locally
	//   - Both non-empty: Split execution (e.g., stats runs remotely with local merge)
	//
	// The timestamp is the query execution timestamp (for relative time expressions).
	splitToRemoteAndLocal(timestamp int64) (pipe, []pipe)

	// canLiveTail must return true if the given pipe can be used in live tailing.
	//
	// Live tailing continuously polls for new log entries. Pipes that modify
	// or delete the _time field cannot be used for live tailing because
	// the deduplication mechanism relies on timestamps.
	//
	// See https://docs.victoriametrics.com/victorialogs/querying/#live-tailing
	canLiveTail() bool

	// canReturnLastNResults must return true if the given pipe can return last N results ordered by _time desc.
	//
	// The pipe can return last N results if it doesn't modify the _time field.
	// This is used to determine if the last-N optimization can be applied.
	canReturnLastNResults() bool

	// updateNeededFields must update pf with fields the pipe needs and doesn't need at the input.
	//
	// This is used for column pruning optimization: if a pipe doesn't need
	// certain columns, they don't need to be read from storage.
	// The filter is updated in reverse order (tail pipe first).
	updateNeededFields(pf *prefixfilter.Filter)

	// newPipeProcessor must return new pipeProcessor, which writes data to the given ppNext.
	//
	// The processor is the runtime representation of the pipe during query execution.
	// Each processor:
	//   - Receives DataBlocks via writeBlock() from upstream
	//   - Transforms the data according to pipe semantics
	//   - Writes transformed blocks to ppNext (the next pipe's processor)
	//
	// Parameters:
	//   - concurrency: Number of goroutines for parallel processing
	//   - stopCh: Closed when query should stop (cancellation)
	//   - cancel: Call to cancel the entire query on error
	//   - ppNext: The next pipe's processor (downstream)
	//
	// The returned pipeProcessor may call cancel() at any time to notify
	// the caller to stop sending new data.
	newPipeProcessor(concurrency int, stopCh <-chan struct{}, cancel func(), ppNext pipeProcessor) pipeProcessor

	// hasFilterInWithQuery must return true of pipe contains 'in(subquery)' filter (recursively).
	// This is used to determine if subquery initialization is needed.
	hasFilterInWithQuery() bool

	// initFilterInValues must return new pipe with the initialized values for 'in(subquery)' filters (recursively).
	//
	// This materializes subquery results before main query execution.
	// The cache avoids re-executing the same subquery multiple times.
	//
	// If keepSubquery is false, then the returned pipe must completely replace subquery with the subquery results.
	// It is OK to return the pipe itself if it doesn't contain 'in(subquery)' filters.
	initFilterInValues(cache *inValuesCache, getFieldValuesFunc getFieldValuesFunc, keepSubquery bool) (pipe, error)

	// visitSubqueries must call visitFunc for all the subqueries, which exist at the pipe (recursively).
	// This is used for subquery initialization and optimization propagation.
	visitSubqueries(visitFunc func(q *Query))
}

// pipeProcessor must process a single pipe during query execution.
//
// While pipe represents the static AST, pipeProcessor represents the runtime
// execution state. Each query execution creates new pipeProcessor instances.
//
// Execution flow:
//  1. Storage workers call writeBlock() with matching DataBlocks
//  2. Processor transforms data and writes to downstream processor
//  3. After all workers complete, flush() is called to emit final results
//
// Thread Safety:
//   - writeBlock() is called concurrently from multiple workers
//   - Each worker has a unique workerID for per-worker state
//   - flush() is called after all workers complete (sequentially)
type pipeProcessor interface {
	// writeBlock must write the given block of data to the given pipeProcessor.
	//
	// writeBlock is called concurrently from worker goroutines.
	// The workerID is the id of the worker goroutine, which calls the writeBlock.
	// It is in the range 0 ... workersCount-1 , where workersCount is the number of worker goroutines.
	// The number of worker goroutines is unknown beforehand (but is usually limited by the number of CPU cores),
	// so the pipe must dynamically adapt to it. It is recommended using lib/atomicutil.Slice for maintaining per-worker state.
	//
	// It is OK to modify br contents inside writeBlock. The caller mustn't rely on br contents after writeBlock call.
	// It is forbidden to hold references to br after returning from writeBlock, since the caller may reuse it.
	//
	// If any error occurs at writeBlock, then cancel() must be called by pipeProcessor in order to notify worker goroutines
	// to stop sending new data. The occurred error must be returned from flush().
	//
	// cancel() may be called also when the pipeProcessor decides to stop accepting new data, even if there is no any error.
	writeBlock(workerID uint, br *blockResult)

	// flush must flush all the data accumulated in the pipeProcessor to the next pipeProcessor.
	//
	// flush is called after all the worker goroutines are stopped.
	//
	// It is guaranteed that flush() is called for every pipeProcessor returned from pipe.newPipeProcessor().
	flush() error
}

type noopPipeProcessor struct {
	stopCh          <-chan struct{}
	writeBlockFinal func(workerID uint, br *blockResult)
}

func newNoopPipeProcessor(stopCh <-chan struct{}, writeBlock func(workerID uint, br *blockResult)) pipeProcessor {
	return &noopPipeProcessor{
		stopCh:          stopCh,
		writeBlockFinal: writeBlock,
	}
}

func (npp *noopPipeProcessor) writeBlock(workerID uint, br *blockResult) {
	if needStop(npp.stopCh) {
		return
	}
	npp.writeBlockFinal(workerID, br)
}

func (npp *noopPipeProcessor) flush() error {
	logger.Panicf("BUG: mustn't be called!")
	return nil
}

// parsePipes parses a chain of pipes separated by '|'.
//
// Grammar: pipes = pipe ('|' pipe)*
//
// The function continues parsing until it hits ')' (end of subquery) or
// end of input. Each pipe is parsed by parsePipe() which dispatches
// to the appropriate pipe-specific parser.
func parsePipes(lex *lexer) ([]pipe, error) {
	var pipes []pipe
	for {
		p, err := parsePipe(lex)
		if err != nil {
			return nil, err
		}
		pipes = append(pipes, p)

		switch {
		case lex.isKeyword("|"):
			// Continue parsing the next pipe in the chain
			lex.nextToken()
		case lex.isKeyword(")", ""):
			// End of pipes - either closing paren or end of query
			return pipes, nil
		default:
			return nil, fmt.Errorf("unexpected token after [%s]: %q; expecting '|' or ')'", pipes[len(pipes)-1], lex.token)
		}
	}
}

// parsePipe parses a single pipe from the current lexer position.
//
// The parser first looks up the token in the pipe registry (initPipeParsers).
// If not found, it tries parsing without the leading keyword:
//   - Stats pipe can omit "stats" if it starts with aggregation function
//   - Filter pipe can omit "filter" keyword (bare filter expression)
//
// This allows more concise query syntax like:
//   - "| count() by host" instead of "| stats count() by host"
//   - "| level:ERROR" instead of "| filter level:ERROR"
func parsePipe(lex *lexer) (pipe, error) {
	pps := getPipeParsers()
	for pipeName, parseFunc := range pps {
		if !lex.isKeyword(pipeName) {
			continue
		}
		p, err := parseFunc(lex)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q pipe: %w", pipeName, err)
		}
		return p, nil
	}

	// Try parsing without explicit keyword
	lexState := lex.backupState()

	// Try parsing stats pipe without 'stats' keyword
	// Example: "| count() by host" instead of "| stats count() by host"
	ps, err := parsePipeStatsNoStatsKeyword(lex)
	if err == nil {
		return ps, nil
	}
	lex.restoreState(lexState)

	// Try parsing filter pipe without 'filter' keyword
	// Example: "| level:ERROR" instead of "| filter level:ERROR"
	pf, err := parsePipeFilterNoFilterKeyword(lex)
	if err == nil {
		return pf, nil
	}
	lex.restoreState(lexState)

	return nil, fmt.Errorf("unexpected pipe %q", lex.token)
}

// pipeParsers maps pipe keyword names to their parsing functions.
// Initialized lazily via sync.Once to avoid circular dependencies during package init.
var pipeParsers map[string]pipeParseFunc
var pipeParsersOnce sync.Once

// pipeParseFunc is the signature for functions that parse a specific pipe type.
type pipeParseFunc func(lex *lexer) (pipe, error)

// getPipeParsers returns the pipe parser registry, initializing it on first call.
func getPipeParsers() map[string]pipeParseFunc {
	pipeParsersOnce.Do(initPipeParsers)
	return pipeParsers
}

// initPipeParsers registers all available pipe types with their keywords.
//
// Each entry maps a keyword to its parser function. The keywords are case-insensitive.
// Aliases are registered separately (e.g., "rm" and "delete" both map to parsePipeDelete).
//
// Pipe categories:
//   - Filtering: filter, where
//   - Aggregation: stats, block_stats, total_stats, running_stats, query_stats
//   - Transformation: fields, keep, delete, copy, rename, format, extract
//   - Ordering: sort, order
//   - Limiting: limit, head, offset, skip, first, last, sample, top
//   - Joining: join, union
//   - Utility: uniq, facets, field_names, field_values, len
func initPipeParsers() {
	pipeParsers = map[string]pipeParseFunc{
		// Stats and aggregation pipes
		"block_stats":   parsePipeBlockStats,
		"blocks_count":  parsePipeBlocksCount,
		"facets":        parsePipeFacets,
		"field_names":   parsePipeFieldNames,
		"field_values":  parsePipeFieldValues,
		"query_stats":   parsePipeQueryStats,
		"running_stats": parsePipeRunningStats,
		"stats":         parsePipeStats,
		"stats_remote":  parsePipeStats, // Alias for distributed stats
		"total_stats":   parsePipeTotalStats,
		"uniq":          parsePipeUniq,

		// Filtering pipes
		"filter": parsePipeFilter,
		"where":  parsePipeFilter, // SQL-style alias

		// Field manipulation pipes
		"copy":              parsePipeCopy,
		"cp":                parsePipeCopy, // Short alias
		"delete":            parsePipeDelete,
		"del":               parsePipeDelete, // Short alias
		"drop":              parsePipeDelete, // SQL-style alias
		"rm":                parsePipeDelete, // Unix-style alias
		"drop_empty_fields": parsePipeDropEmptyFields,
		"fields":            parsePipeFields,
		"keep":              parsePipeFields, // Alias for fields
		"rename":            parsePipeRename,
		"mv":                parsePipeRename, // Unix-style alias

		// Extraction and parsing pipes
		"extract":        parsePipeExtract,
		"extract_regexp": parsePipeExtractRegexp,
		"unpack_json":    parsePipeUnpackJSON,
		"unpack_logfmt":  parsePipeUnpackLogfmt,
		"unpack_syslog":  parsePipeUnpackSyslog,
		"unpack_words":   parsePipeUnpackWords,
		"unroll":         parsePipeUnroll,

		// Formatting pipes
		"decolorize":     parsePipeDecolorize,
		"format":         parsePipeFormat,
		"hash":           parsePipeHash,
		"json_array_len": parsePipeJSONArrayLen,
		"len":            parsePipeLen,
		"pack_json":      parsePipePackJSON,
		"pack_logfmt":    parsePipePackLogfmt,
		"replace":        parsePipeReplace,
		"replace_regexp": parsePipeReplaceRegexp,
		"split":          parsePipeSplit,
		"time_add":       parsePipeTimeAdd,

		// Sorting and limiting pipes
		"first":  parsePipeFirst,
		"last":   parsePipeLast,
		"limit":  parsePipeLimit,
		"head":   parsePipeLimit, // Unix-style alias
		"offset": parsePipeOffset,
		"skip":   parsePipeOffset, // Unix-style alias
		"sort":   parsePipeSort,
		"order":  parsePipeSort, // SQL-style alias
		"top":    parsePipeTop,
		"sample": parsePipeSample,

		// Math and evaluation pipes
		"eval":              parsePipeMath, // Alias for math
		"math":              parsePipeMath,
		"collapse_nums":     parsePipeCollapseNums,
		"generate_sequence": parsePipeGenerateSequence,

		// Join and union pipes
		"join":           parsePipeJoin,
		"union":          parsePipeUnion,
		"stream_context": parsePipeStreamContext,

		// Stream field management
		"set_stream_fields": parsePipeSetStreamFields,
	}
}

// isPipeName checks if s is a reserved pipe keyword.
// This is used during parsing to prevent keywords from being used as field names
// without quoting, and to determine if a token starts a pipe expression.
func isPipeName(s string) bool {
	pps := getPipeParsers()
	sLower := strings.ToLower(s)
	return pps[sLower] != nil
}

func mustParsePipes(s string, timestamp int64) []pipe {
	lex := newLexer(s, timestamp)
	pipes, err := parsePipes(lex)
	if err != nil {
		logger.Panicf("BUG: cannot parse [%s]: %s", s, err)
	}
	if !lex.isEnd() {
		logger.Panicf("BUG: unexpected tail left after parsing [%s]: %s", s, lex.context())
	}
	return pipes
}

func mustParsePipe(s string, timestamp int64) pipe {
	lex := newLexer(s, timestamp)
	p, err := parsePipe(lex)
	if err != nil {
		logger.Panicf("BUG: cannot parse [%s]: %s", s, err)
	}
	if !lex.isEnd() {
		logger.Panicf("BUG: unexpected tail left after parsing [%s]: %s", s, lex.context())
	}
	return p
}
