// json_prettifier implements streaming JSON-to-formatted-output conversion.
//
// Architecture Overview:
// This file implements the output formatting layer of vlogscli. It uses an io.Pipe
// to decouple JSON parsing (background goroutine) from output consumption (main goroutine).
//
// Streaming Pipeline:
//
//	HTTP response body (NDJSON) → json.Decoder → prettifyJSONLines() → formatter → bufio.Writer → io.Pipe → less/stdout
//
// Why io.Pipe?
// The pipe provides natural backpressure: if the consumer (less) stops reading (e.g., user
// is viewing a page), the pipe write blocks, which pauses the formatter, which pauses the
// HTTP response read. This prevents unbounded memory growth for large result sets.
//
// Why flush after each JSON object?
// Results appear immediately as they arrive from the server. Users see streaming output
// rather than waiting for the entire response to buffer.
//
// See onboarding/onboarding-vlogscli.md for comprehensive documentation.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// outputMode defines the formatting style for query results.
// Each mode is optimized for different use cases:
//   - JSONMultiline: Human-readable JSON with indentation (default for interactive use)
//   - JSONSingleline: Compact JSON on one line (good for piping to jq)
//   - Logfmt: key="value" format (common in logging ecosystems)
//   - Compact: Minimal output for single-field results (e.g., just the _msg value)
type outputMode int

const (
	outputModeJSONMultiline  = outputMode(0)
	outputModeJSONSingleline = outputMode(1)
	outputModeLogfmt         = outputMode(2)
	outputModeCompact        = outputMode(3)
)

// getOutputFormatter returns the appropriate formatting function for the given output mode.
// The returned function writes a single formatted log entry to the writer.
func getOutputFormatter(outputMode outputMode) func(w io.Writer, fields []logstorage.Field) error {
	switch outputMode {
	case outputModeJSONMultiline:
		return func(w io.Writer, fields []logstorage.Field) error {
			return writeJSONObject(w, fields, true)
		}
	case outputModeJSONSingleline:
		return func(w io.Writer, fields []logstorage.Field) error {
			return writeJSONObject(w, fields, false)
		}
	case outputModeLogfmt:
		return writeLogfmtObject
	case outputModeCompact:
		return writeCompactObject
	default:
		panic(fmt.Errorf("BUG: unexpected outputMode=%d", outputMode))
	}
}

// jsonPrettifier wraps an NDJSON stream and provides formatted output via io.Pipe.
//
// The prettifier runs a background goroutine that:
//  1. Reads JSON objects one at a time from the HTTP response
//  2. Sorts fields alphabetically for consistent output
//  3. Formats according to the selected output mode
//  4. Flushes after each object for immediate display
//
// Concurrency Model:
//   - Background goroutine: reads from HTTP response, writes to PipeWriter
//   - Main goroutine: reads from PipeReader (via Read method), passes to less/stdout
//   - io.Pipe provides thread-safe communication between the two
type jsonPrettifier struct {
	// r is the original HTTP response body, kept for cleanup in Close().
	r io.ReadCloser

	// formatter is the selected output formatting function.
	formatter func(w io.Writer, fields []logstorage.Field) error

	// d is the JSON decoder for incremental parsing.
	// Using json.Decoder instead of json.Unmarshal allows streaming without buffering the entire response.
	d *json.Decoder

	// pr and pw form the io.Pipe that decouples formatting from consumption.
	// The background goroutine writes formatted output to pw.
	// The main goroutine reads from pr (via the Read method).
	pr *io.PipeReader
	pw *io.PipeWriter

	// bw is a buffered writer wrapping pw.
	// Buffering reduces syscalls, but we flush after each JSON object for immediate display.
	bw *bufio.Writer

	// wg tracks the background goroutine for clean shutdown.
	wg sync.WaitGroup
}

// newJSONPrettifier creates a streaming JSON prettifier.
//
// The constructor immediately starts a background goroutine that reads from the
// HTTP response and writes formatted output to the pipe. This design allows the
// consumer to start reading immediately without waiting for the entire response.
func newJSONPrettifier(r io.ReadCloser, outputMode outputMode) *jsonPrettifier {
	// Create JSON decoder for streaming parse.
	d := json.NewDecoder(r)

	// Create the pipe that will connect the background goroutine to the consumer.
	pr, pw := io.Pipe()

	// Buffer writes to reduce syscalls. Flush after each object for streaming display.
	bw := bufio.NewWriter(pw)

	formatter := getOutputFormatter(outputMode)

	jp := &jsonPrettifier{
		r:         r,
		formatter: formatter,

		d: d,

		pr: pr,
		pw: pw,
		bw: bw,
	}

	// Start background formatting goroutine.
	// This goroutine will run until the response is exhausted or an error occurs.
	jp.wg.Go(func() {
		err := jp.prettifyJSONLines()
		// Close pipes with error to signal completion/failure to the reader.
		jp.closePipesWithError(err)
	})

	return jp
}

// closePipesWithError closes both ends of the pipe with the given error.
// This unblocks any blocked Read or Write calls on the pipe.
func (jp *jsonPrettifier) closePipesWithError(err error) {
	_ = jp.pr.CloseWithError(err)
	_ = jp.pw.CloseWithError(err)
}

// prettifyJSONLines is the main loop of the background formatting goroutine.
//
// It reads JSON objects one at a time, formats them, and writes to the pipe.
// The loop continues until the HTTP response is exhausted or an error occurs.
//
// Field Sorting:
// Fields are sorted alphabetically for consistent output. This makes it easier
// to visually scan results and compare entries. Special fields like _time and _msg
// are sorted along with other fields (not given special positioning).
func (jp *jsonPrettifier) prettifyJSONLines() error {
	// d.More() returns true while there are more JSON values to decode.
	// VictoriaLogs returns NDJSON (newline-delimited JSON), one object per log entry.
	for jp.d.More() {
		fields, err := readNextJSONObject(jp.d)
		if err != nil {
			return err
		}

		// Sort fields alphabetically for consistent, predictable output.
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Name < fields[j].Name
		})

		// Format and write the log entry.
		if err := jp.formatter(jp.bw, fields); err != nil {
			return err
		}

		// Flush bw after every output line in order to show results as soon as they appear.
		// This is critical for streaming display: users see results incrementally
		// rather than waiting for the entire response to buffer.
		if err := jp.bw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close stops the prettifier and releases resources.
// It signals the background goroutine to stop and waits for it to complete.
func (jp *jsonPrettifier) Close() error {
	// Close pipes with io.ErrUnexpectedEOF to unblock any waiting reads/writes.
	jp.closePipesWithError(io.ErrUnexpectedEOF)

	// Close the underlying HTTP response body.
	err := jp.r.Close()

	// Wait for the background goroutine to finish.
	jp.wg.Wait()
	return err
}

// Read implements io.Reader, allowing the prettifier to be used wherever a reader is expected.
// It reads formatted output from the pipe (which is fed by the background goroutine).
func (jp *jsonPrettifier) Read(p []byte) (int, error) {
	return jp.pr.Read(p)
}

// readNextJSONObject parses a single JSON object from the stream.
//
// Why token-level parsing instead of json.Unmarshal into a map?
//   - Memory efficiency: No map allocation overhead for each object
//   - Streaming: Can process objects as they arrive without buffering
//   - Type specificity: VictoriaLogs only returns string values, so we optimize for that
//
// The function expects NDJSON format: one complete JSON object per line/object.
// Each object is a flat map of string keys to string values (VictoriaLogs query results).
func readNextJSONObject(d *json.Decoder) ([]logstorage.Field, error) {
	// Read the opening brace of the JSON object.
	t, err := d.Token()
	if err != nil {
		return nil, fmt.Errorf("cannot read '{': %w", err)
	}
	delim, ok := t.(json.Delim)
	if !ok || delim.String() != "{" {
		return nil, fmt.Errorf("unexpected token read; got %q; want '{'", delim)
	}

	var fields []logstorage.Field
	for {
		// Read object key
		t, err := d.Token()
		if err != nil {
			return nil, fmt.Errorf("cannot read JSON object key or closing brace: %w", err)
		}

		// Check for closing brace (end of object).
		delim, ok := t.(json.Delim)
		if ok {
			if delim.String() == "}" {
				return fields, nil
			}
			return nil, fmt.Errorf("unexpected delimiter read; got %q; want '}'", delim)
		}

		// The token should be a string key.
		key, ok := t.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected token read for object key: %v; want string or '}'", t)
		}

		// read object value
		t, err = d.Token()
		if err != nil {
			return nil, fmt.Errorf("cannot read JSON object value: %w", err)
		}

		// VictoriaLogs query results only contain string values.
		// If a non-string value is encountered, it's a protocol error.
		value, ok := t.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected token read for object value: %v; want string", t)
		}

		fields = append(fields, logstorage.Field{
			Name:  key,
			Value: value,
		})
	}
}

// writeLogfmtObject writes fields in logfmt format.
// Logfmt is a key="value" format commonly used in logging systems (e.g., Loki, Prometheus).
//
// Example output:
//
//	_time="2026-02-14T10:30:00Z" _msg="request completed" level="info"
func writeLogfmtObject(w io.Writer, fields []logstorage.Field) error {
	// Delegate to logstorage's logfmt implementation for proper escaping.
	data := logstorage.MarshalFieldsToLogfmt(nil, fields)
	_, err := fmt.Fprintf(w, "%s\n", data)
	return err
}

// writeCompactObject writes a minimal representation optimized for single-field queries.
//
// Compact Mode Logic:
//   - 1 field: Output just the value (no key)
//   - 2 fields with _time: Output "timestamp\tvalue" (tab-separated)
//   - Otherwise: Fall back to logfmt format
//
// This mode is useful for queries that select specific fields, making output more readable.
//
// Example outputs:
//   - 1 field (_msg): "request completed"
//   - 2 fields (_time, _msg): "2026-02-14T10:30:00Z\trequest completed"
//   - 3+ fields: Falls back to logfmt
func writeCompactObject(w io.Writer, fields []logstorage.Field) error {
	if len(fields) == 1 {
		// Just write field value as is without name
		_, err := fmt.Fprintf(w, "%s\n", fields[0].Value)
		return err
	}

	// Special case for _time + one other field: show as timestamp\tvalue
	// This is a common query pattern when viewing logs with time context.
	if len(fields) == 2 && (fields[0].Name == "_time" || fields[1].Name == "_time") {
		// Write _time\tfieldValue as is
		if fields[0].Name == "_time" {
			_, err := fmt.Fprintf(w, "%s\t%s\n", fields[0].Value, fields[1].Value)
			return err
		}
		_, err := fmt.Fprintf(w, "%s\t%s\n", fields[1].Value, fields[0].Value)
		return err
	}

	// Fall back to logfmt
	return writeLogfmtObject(w, fields)
}

// writeJSONObject writes fields as a JSON object.
// The isMultiline parameter controls formatting:
//   - true: Pretty-printed with newlines and indentation (default mode)
//   - false: Single-line compact JSON (good for piping to jq or other tools)
//
// Example multiline output:
//
//	{
//	  "_msg": "request completed",
//	  "_time": "2026-02-14T10:30:00Z",
//	  "level": "info"
//	}
//
// Example singleline output:
//
//	{"_msg":"request completed","_time":"2026-02-14T10:30:00Z","level":"info"}
func writeJSONObject(w io.Writer, fields []logstorage.Field, isMultiline bool) error {
	if len(fields) == 0 {
		fmt.Fprintf(w, "{}\n")
		return nil
	}

	fmt.Fprintf(w, "{")
	writeNewlineIfNeeded(w, isMultiline)

	// Write first field (no leading comma).
	if err := writeJSONObjectKeyValue(w, fields[0], isMultiline); err != nil {
		return err
	}

	// Write remaining fields with comma separators.
	for _, f := range fields[1:] {
		fmt.Fprintf(w, ",")
		writeNewlineIfNeeded(w, isMultiline)
		if err := writeJSONObjectKeyValue(w, f, isMultiline); err != nil {
			return err
		}
	}

	writeNewlineIfNeeded(w, isMultiline)
	fmt.Fprintf(w, "}\n")
	return nil
}

// writeNewlineIfNeeded writes a newline if in multiline mode.
func writeNewlineIfNeeded(w io.Writer, isMultiline bool) {
	if isMultiline {
		fmt.Fprintf(w, "\n")
	}
}

// writeJSONObjectKeyValue writes a single key-value pair in JSON format.
// In multiline mode, includes 2-space indentation for readability.
func writeJSONObjectKeyValue(w io.Writer, f logstorage.Field, isMultiline bool) error {
	key := getJSONString(f.Name)
	value := getJSONString(f.Value)
	if isMultiline {
		_, err := fmt.Fprintf(w, "  %s: %s", key, value)
		return err
	}
	_, err := fmt.Fprintf(w, "%s:%s", key, value)
	return err
}

// getJSONString returns a properly escaped and quoted JSON string.
// It also unescapes HTML entities (<, >, &) that Go's json.Marshal encodes by default.
//
// Why unescape HTML entities?
// Go's json.Marshal encodes <, >, and & as \u003c, \u003e, \u0026 for safety in HTML contexts.
// For log output, we want these characters displayed literally for readability.
func getJSONString(s string) string {
	data, err := json.Marshal(s)
	if err != nil {
		panic(fmt.Errorf("unexpected error when marshaling string to JSON: %w", err))
	}
	// Replace HTML escape sequences with literal characters.
	return jsonHTMLReplacer.Replace(string(data))
}

// jsonHTMLReplacer converts JSON's HTML escape sequences back to literal characters.
// This makes log output more readable by showing <, >, and & directly.
var jsonHTMLReplacer = strings.NewReplacer(
	`\u003c`, "\u003c", // < (less than)
	`\u003e`, "\u003e", // > (greater than)
	`\u0026`, "\u0026", // & (ampersand)
)
