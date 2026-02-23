// Package insertutil provides shared utilities for log ingestion endpoints.
//
// LineReader is a utility for reading newline-delimited data from HTTP request bodies.
// It handles:
//   - Buffered reading for efficiency
//   - Line size limits (oversized lines are skipped, not rejected)
//   - Graceful handling of various line endings
//
// WHY SKIP INSTEAD OF REJECT?
// For ingestion protocols like Elasticsearch bulk import, the request may contain
// thousands of log entries. Rejecting the entire request because one line is too
// long would cause data loss for all the valid entries. Instead, LineReader skips
// oversized lines and returns an empty line in their place, allowing the protocol
// handler to maintain line-number synchronization (e.g., action line → source line
// pairs in Elasticsearch bulk format).
package insertutil

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/VictoriaMetrics/metrics"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/stringsutil"
)

// LineReader reads newline-delimited lines from an underlying io.Reader.
// It handles buffering, line size limits, and graceful EOF handling.
//
// USAGE:
//
//	lr := insertutil.NewLineReader("jsonline", r.Body)
//	for lr.NextLine() {
//	    line := lr.Line  // Valid until next NextLine() call
//	    // process line
//	}
//	if err := lr.Err(); err != nil {
//	    // handle error
//	}
//
// OVERSIZED LINE HANDLING:
// If a line exceeds -insert.maxLineSizeBytes, it is:
//  1. Skipped (read and discarded)
//  2. An empty line is returned instead (to maintain line count for protocols
//     like Elasticsearch bulk that expect paired lines)
//  3. A warning is logged with a snippet of the oversized line
type LineReader struct {
	// Line contains the current line after NextLine() returns true.
	// The contents are valid only until the next call to NextLine().
	// Do not retain references to Line after calling NextLine().
	Line []byte

	// name identifies this reader for logging and error messages
	name string

	// r is the underlying reader (typically HTTP request body)
	r io.Reader

	// buf is the internal buffer for reading data
	buf []byte

	// bufOffset is the position in buf where the next line starts
	bufOffset int

	// err stores any error encountered during reading
	err error

	// eofReached indicates whether the underlying reader has returned EOF
	eofReached bool
}

// NewLineReader creates a LineReader for the given reader.
// The name is used in error messages and logs.
func NewLineReader(name string, r io.Reader) *LineReader {
	return &LineReader{
		name: name,
		r:    r,
	}
}

// NextLine reads the next line into lr.Line.
// Returns true if a line was read, false if there are no more lines or an error occurred.
//
// LINE SIZE LIMIT:
// If a line exceeds MaxLineSizeBytes, it is skipped and an empty line is returned.
// This behavior is important for protocols like Elasticsearch bulk import where
// line numbers must stay synchronized (action line → source line pairs).
//
// After NextLine returns false, call Err() to check if an error occurred.
func (lr *LineReader) NextLine() bool {
	for {
		lr.Line = nil

		// Check if we need more data
		if lr.bufOffset >= len(lr.buf) {
			if lr.err != nil || lr.eofReached {
				return false
			}
			if !lr.readMoreData() {
				return false
			}
			// Check again after reading
			if lr.bufOffset >= len(lr.buf) && lr.eofReached {
				return false
			}
		}

		// Look for newline in remaining buffer
		buf := lr.buf[lr.bufOffset:]
		if n := bytes.IndexByte(buf, '\n'); n >= 0 {
			// Found newline - extract the line (excluding the \n)
			lr.Line = buf[:n]
			lr.bufOffset += n + 1
			return true
		}

		// No newline found
		if lr.eofReached {
			// End of input - return remaining data as final line
			lr.Line = buf
			lr.bufOffset += len(buf)
			return true
		}

		// Need more data to complete the line
		if !lr.readMoreData() {
			return false
		}
	}
}

// Err returns any error that occurred during reading.
// Returns nil if reading completed successfully.
func (lr *LineReader) Err() error {
	if lr.err == nil {
		return nil
	}
	return fmt.Errorf("%s: %s", lr.name, lr.err)
}

// readMoreData reads more data from the underlying reader into the buffer.
// Returns true if more data was read or EOF was reached, false on error.
//
// LINE SIZE ENFORCEMENT:
// If the buffer reaches MaxLineSizeBytes without finding a newline, the line
// is considered too long and is skipped via skipUntilNextLine().
func (lr *LineReader) readMoreData() bool {
	// Compact buffer: move unread data to the beginning
	if lr.bufOffset > 0 {
		lr.buf = append(lr.buf[:0], lr.buf[lr.bufOffset:]...)
		lr.bufOffset = 0
	}

	bufLen := len(lr.buf)

	// Check if line is too long (no newline found within max size)
	if bufLen >= MaxLineSizeBytes.IntN() {
		lineSnippet := stringsutil.LimitStringLen(string(lr.buf), 1024)
		ok, skippedBytes := lr.skipUntilNextLine()
		logger.Warnf("%s: the line length exceeds -insert.maxLineSizeBytes=%d; skipping it; total skipped bytes=%d; the line snippet=%q",
			lr.name, MaxLineSizeBytes.IntN(), skippedBytes, lineSnippet)
		tooLongLinesSkipped.Inc()
		return ok
	}

	// Grow buffer to max size and read more data
	lr.buf = slicesutil.SetLength(lr.buf, MaxLineSizeBytes.IntN())
	n, err := lr.r.Read(lr.buf[bufLen:])
	lr.buf = lr.buf[:bufLen+n]

	if err != nil {
		if errors.Is(err, io.EOF) {
			lr.eofReached = true
			return true
		}
		lr.err = fmt.Errorf("cannot read the next line: %s", err)
	}
	return n > 0
}

// tooLongLinesSkipped tracks the number of oversized lines that were skipped.
var tooLongLinesSkipped = metrics.NewCounter("vl_too_long_lines_skipped_total")

// skipUntilNextLine reads and discards data until a newline is found.
// This is used when a line exceeds MaxLineSizeBytes.
//
// RETURNS:
//   - bool: true if a newline was found (or EOF), false on read error
//   - int:  total number of bytes skipped (for logging)
//
// SYNC MAINTENANCE:
// After skipping, the buffer is set to contain just the newline character.
// This ensures the next NextLine() call returns an empty line, maintaining
// line-number synchronization for protocols like Elasticsearch bulk import.
func (lr *LineReader) skipUntilNextLine() (bool, int) {
	// Start with MaxLineSizeBytes since we've already read that many bytes
	skipSizeBytes := MaxLineSizeBytes.IntN()

	for {
		lr.buf = slicesutil.SetLength(lr.buf, MaxLineSizeBytes.IntN())
		n, err := lr.r.Read(lr.buf)
		skipSizeBytes += n
		lr.buf = lr.buf[:n]

		if err != nil {
			if errors.Is(err, io.EOF) {
				lr.eofReached = true
				lr.buf = lr.buf[:0]
				return true, skipSizeBytes
			}
			lr.err = fmt.Errorf("cannot skip the current line: %s", err)
			return false, skipSizeBytes
		}

		// Look for newline in the data we just read
		if n := bytes.IndexByte(lr.buf, '\n'); n >= 0 {
			// Adjust skip count: we skipped bytes before the newline
			skipSizeBytes += n + 1 - len(lr.buf)

			// Keep just the newline in the buffer so NextLine returns an empty line.
			// This maintains line synchronization for protocols with paired lines
			// (e.g., Elasticsearch bulk: action line, source line).
			lr.buf = append(lr.buf[:0], lr.buf[n:]...)
			return true, skipSizeBytes
		}
	}
}
