// vlogscli is VictoriaLogs' interactive command-line query tool.
//
// Architecture Overview:
// This file implements the main entry point, REPL loop, and HTTP query execution.
// The tool uses a streaming pipeline architecture:
//
//	User input → REPL loop → HTTP query → jsonPrettifier (goroutine) → io.Pipe → less/stdout
//
// Key Design Decisions:
//   - Queries must end with ";" to execute, enabling multi-line query editing
//   - Client-side query validation via logstorage.ParseQuery() before sending to server
//   - Streaming response handling via io.Pipe to avoid buffering entire responses
//   - Signal-aware context management for graceful Ctrl+C handling
//
// See onboarding/onboarding-vlogscli.md for comprehensive documentation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"github.com/ergochat/readline"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Connection and authentication flags.
// These follow VictoriaMetrics ecosystem conventions for consistency with vmagent, vmalert, etc.
var (
	// datasourceURL is the VictoriaLogs query endpoint.
	// Default points to local VictoriaLogs instance's LogsQL query handler.
	// The /select/logsql/query path corresponds to vlselect's query endpoint.
	datasourceURL = flag.String("datasource.url", "http://localhost:9428/select/logsql/query", "URL for querying VictoriaLogs; "+
		"see https://docs.victoriametrics.com/victorialogs/querying/#querying-logs . See also -tail.url")

	// tailURL is the live tailing endpoint for streaming new log entries.
	// Auto-detected from datasourceURL by replacing /query with /tail if not explicitly set.
	tailURL = flag.String("tail.url", "", "URL for live tailing queries to VictoriaLogs; see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing ."+
		"The url is automatically detected from -datasource.url by replacing /query with /tail at the end if -tail.url is empty")

	// historyFile stores query history for persistence across sessions.
	// Uses quoted string format (one per line) for safe escaping of special characters.
	historyFile = flag.String("historyFile", "vlogscli-history", "Path to file with command history")

	// header allows custom HTTP headers for advanced use cases like cookies or custom auth.
	// Parsed as "Name: value" pairs and applied to every request.
	header = flagutil.NewArrayString("header", "Optional header to pass in request -datasource.url in the form 'HeaderName: value'")

	// accountID and projectID enable multi-tenancy support.
	// VictoriaLogs uses (AccountID, ProjectID) tuple for tenant isolation.
	accountID = flag.Int("accountID", 0, "Account ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy")
	projectID = flag.Int("projectID", 0, "Project ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy")

	// Authentication options - mutually compatible (can use basic auth + TLS client certs together).
	username    = flag.String("username", "", "Optional basic auth username to use for the -datasource.url")
	password    = flagutil.NewPassword("password", "Optional basic auth password to use for the -datasource.url")
	bearerToken = flagutil.NewPassword("bearerToken", "Optional bearer auth token to use for the -datasource.url")

	// TLS configuration for secure connections.
	tlsCAFile     = flag.String("tlsCAFile", "", "Optional path to TLS CA file to use for verifying connections to the -datasource.url. By default, system CA is used")
	tlsCertFile   = flag.String("tlsCertFile", "", "Optional path to client-side TLS certificate file to use when connecting to the -datasource.url")
	tlsKeyFile    = flag.String("tlsKeyFile", "", "Optional path to client-side TLS certificate key to use when connecting to the -datasource.url")
	tlsServerName = flag.String("tlsServerName", "", "Optional TLS server name to use for connections to the -datasource.url. "+
		"By default, the server name from -datasource.url is used")
	tlsInsecureSkipVerify = flag.Bool("tlsInsecureSkipVerify", false, "Whether to skip tls verification when connecting to the -datasource.url")
)

// Prompt strings for the REPL.
// The first line prompt ";> " reminds users that queries must end with ";" to execute.
// Continuation lines have no prompt to keep multi-line queries visually clean.
const (
	firstLinePrompt = ";> "
	nextLinePrompt  = ""
)

// main is the entry point for vlogscli.
// Initialization order matters:
//  1. Parse flags first (envflag.Parse handles both CLI flags and env vars)
//  2. Parse custom headers before creating HTTP client
//  3. Create HTTP client with auth configuration
//  4. Initialize readline with custom listener for Ctrl+C handling
//  5. Enter the REPL loop
func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = usage

	// Parse command-line flags; env vars are applied only when -envflag.enable is set.
	// Env var names are flag names with dots replaced by underscores and optional -envflag.prefix.
	envflag.Parse()

	// Initialize build info (version, commit, etc.) for --version flag.
	buildinfo.Init()

	// Initialize logger without log-related flags to avoid confusion.
	logger.InitNoLogFlags()

	// Parse custom HTTP headers from -header flags.
	// Headers are stored globally and applied to every request.
	hes, err := parseHeaders(*header)
	if err != nil {
		fatalf("cannot parse -header command-line flag: %s", err)
	}
	headers = hes

	// Create HTTP client with authentication and TLS configuration.
	// These are package-level globals for simplicity since they're used throughout.
	authConfig, httpClient = newHTTPClient()

	// incompleteLine tracks the current input buffer for Ctrl+C handling.
	// When user presses Ctrl+C mid-line, we need to save this incomplete text to history.
	incompleteLine := ""

	// Configure readline for interactive use.
	// DisableAutoSaveHistory is set because we manage history ourselves with deduplication and limits.
	cfg := &readline.Config{
		Prompt:                 firstLinePrompt,
		DisableAutoSaveHistory: true,
		// Listener is called on every keystroke, allowing us to track incomplete input.
		// This enables saving the partial line when user presses Ctrl+C.
		Listener: func(line []rune, pos int, _ rune) ([]rune, int, bool) {
			incompleteLine = string(line)
			return line, pos, false
		},
	}
	rl, err := readline.NewFromConfig(cfg)
	if err != nil {
		fatalf("cannot initialize readline: %s", err)
	}

	// Display startup message with the target URL.
	fmt.Fprintf(rl, "sending queries to -datasource.url=%s\n", *datasourceURL)
	fmt.Fprintf(rl, `type ? and press enter to see available commands`+"\n")

	// Enter the main REPL loop.
	runReadlineLoop(rl, &incompleteLine)

	// Clean up readline resources on exit.
	if err := rl.Close(); err != nil {
		fatalf("cannot close readline: %s", err)
	}

}

// runReadlineLoop is the main REPL (Read-Eval-Print Loop) for vlogscli.
//
// The loop handles three types of input:
//  1. Special commands (\q, \h, \s, \m, \c, \logfmt, \tail, etc.)
//  2. Multi-line query accumulation (waiting for ";" terminator)
//  3. Query execution when ";" is encountered
//
// Signal handling (Ctrl+C):
//   - Empty prompt → exit with code 130 (128 + SIGINT, shell convention)
//   - Mid-line input → save incomplete text to history, clear prompt
//   - During query execution → cancel via context (handled in executeQuery)
//
// The incompleteLine pointer allows Ctrl+C handler to access the current input buffer.
func runReadlineLoop(rl *readline.Instance, incompleteLine *string) {
	// Load query history from disk and populate readline's history buffer.
	// This enables up-arrow navigation through previous queries.
	historyLines, err := loadFromHistory(*historyFile)
	if err != nil {
		fatalf("cannot load query history: %s", err)
	}
	for _, line := range historyLines {
		if err := rl.SaveToHistory(line); err != nil {
			fatalf("cannot initialize query history: %s", err)
		}
	}

	// Output formatting state - persists across queries in the session.
	outputMode := outputModeJSONMultiline
	disableColors := true
	wrapLongLines := false

	// s accumulates multi-line queries until ";" is encountered.
	s := ""
	for {
		line, err := rl.ReadLine()
		if err != nil {
			switch err {
			case io.EOF:
				// EOF indicates non-interactive mode (piped input).
				// Execute any accumulated query before exiting.
				if s != "" {
					// This is non-interactive query execution.
					executeQuery(context.Background(), rl, s, outputMode, disableColors, wrapLongLines)
				}
				return
			case readline.ErrInterrupt:
				// User pressed Ctrl+C.
				if s == "" && *incompleteLine == "" {
					// Empty prompt - user wants to exit.
					// Exit code 130 follows shell convention (128 + signal number).
					fmt.Fprintf(rl, "interrupted\n")
					os.Exit(128 + int(syscall.SIGINT))
				}
				// Default value for Ctrl+C - clear the prompt and store the incompletely entered line into history
				// Save the incomplete query to history so user can recall it later.
				s += *incompleteLine
				historyLines = pushToHistory(rl, historyLines, s)
				s = ""
				rl.SetPrompt(firstLinePrompt)
				continue
			default:
				fatalf("unexpected error in readline: %s", err)
			}
		}

		s += line
		if s == "" {
			// Skip empty lines
			continue
		}

		// Check for special commands before query logic.
		// Commands are executed immediately without requiring ";".
		if isQuitCommand(s) {
			fmt.Fprintf(rl, "bye!\n")
			_ = pushToHistory(rl, historyLines, s)
			return
		}
		if isHelpCommand(s) {
			printCommandsHelp(rl)
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}
		// Output mode commands - switch formatter for subsequent queries.
		if s == `\s` {
			fmt.Fprintf(rl, "singleline json output mode\n")
			outputMode = outputModeJSONSingleline
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}
		if s == `\m` {
			fmt.Fprintf(rl, "multiline json output mode\n")
			outputMode = outputModeJSONMultiline
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}
		if s == `\c` {
			fmt.Fprintf(rl, "compact output mode\n")
			outputMode = outputModeCompact
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}
		if s == `\logfmt` {
			fmt.Fprintf(rl, "logfmt output mode\n")
			outputMode = outputModeLogfmt
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}
		// Toggle commands for pager behavior.
		if s == `\wrap_long_lines` {
			if wrapLongLines {
				wrapLongLines = false
				fmt.Fprintf(rl, "wrapping of long lines is disabled\n")
			} else {
				wrapLongLines = true
				fmt.Fprintf(rl, "wrapping of long lines is enabled\n")
			}
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}
		if s == `\disable_colors` {
			if !disableColors {
				disableColors = true
				fmt.Fprintf(rl, `disabled colors in compact output mode; enter \enable_colors for enabling it`+"\n")
			}
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}
		if s == `\enable_colors` {
			if disableColors {
				disableColors = false
				fmt.Fprintf(rl, `enabled colors in compact output mode; type \disable_colors for disabling it`+"\n")
			}
			historyLines = pushToHistory(rl, historyLines, s)
			s = ""
			continue
		}

		// Multi-line query handling: wait for ";" before executing.
		// This allows users to break long queries across multiple lines for readability.
		if line != "" && !strings.HasSuffix(line, ";") {
			// Assume the query is incomplete and allow the user finishing the query on the next line
			s += "\n"
			rl.SetPrompt(nextLinePrompt)
			continue
		}

		// Execute the query
		// Create a context that cancels on Ctrl+C, allowing in-flight requests to be aborted.
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
		executeQuery(ctx, rl, s, outputMode, disableColors, wrapLongLines)
		cancel()

		// Save successful query to history and reset for next input.
		historyLines = pushToHistory(rl, historyLines, s)
		s = ""
		rl.SetPrompt(firstLinePrompt)
	}
}

// pushToHistory adds a command/query to history with deduplication and size limits.
//
// History Management Strategy:
//   - Only adds entry if different from the most recent (avoids consecutive duplicates)
//   - Maintains maximum 500 entries (trims oldest when exceeded)
//   - Persists to disk immediately after each addition
//   - Also updates readline's in-memory history for up-arrow navigation
func pushToHistory(rl *readline.Instance, historyLines []string, s string) []string {
	s = strings.TrimSpace(s)
	// Skip if same as last entry (consecutive deduplication).
	if len(historyLines) == 0 || historyLines[len(historyLines)-1] != s {
		historyLines = append(historyLines, s)
		// Enforce 500-entry limit by keeping only the most recent.
		if len(historyLines) > 500 {
			historyLines = historyLines[len(historyLines)-500:]
		}
		// Persist to disk for survival across sessions.
		mustSaveToHistory(*historyFile, historyLines)
	}
	// Update readline's internal history for immediate up-arrow access.
	if err := rl.SaveToHistory(s); err != nil {
		fatalf("cannot update query history: %s", err)
	}
	return historyLines
}

// loadFromHistory reads query history from disk.
//
// History File Format:
//   - One quoted string per line (using Go's strconv.Quote format)
//   - Quoting ensures safe handling of newlines, quotes, and special characters within queries
//   - Example line: "_time:1h\n| stats count()"
//
// The quoted format was chosen over plain text because queries can contain newlines
// in multi-line form, and we need to preserve the exact structure.
func loadFromHistory(filePath string) ([]string, error) {
	if !fs.IsPathExist(filePath) {
		return nil, nil
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	linesQuoted := strings.Split(string(data), "\n")
	lines := make([]string, 0, len(linesQuoted))
	i := 0
	for _, lineQuoted := range linesQuoted {
		i++
		if lineQuoted == "" {
			continue
		}
		// Unquote converts the Go-quoted string back to its original form.
		line, err := strconv.Unquote(lineQuoted)
		if err != nil {
			return nil, fmt.Errorf("cannot parse line #%d at %s: %w; line: [%s]", i, filePath, err, line)
		}
		lines = append(lines, line)
	}
	return lines, nil
}

// mustSaveToHistory persists query history to disk.
// Uses fs.MustWriteSync for atomic write with fsync for durability.
func mustSaveToHistory(filePath string, lines []string) {
	linesQuoted := make([]string, len(lines))
	for i, line := range lines {
		// Quote each line to safely encode newlines and special characters.
		lineQuoted := strconv.Quote(line)
		linesQuoted[i] = lineQuoted
	}
	data := strings.Join(linesQuoted, "\n")
	fs.MustWriteSync(filePath, []byte(data))
}

// isQuitCommand checks if the input is a quit command.
// Multiple aliases are supported for user convenience.
func isQuitCommand(s string) bool {
	switch s {
	case `\q`, "q", "quit", "exit":
		return true
	default:
		return false
	}
}

// isHelpCommand checks if the input is a help command.
// Multiple aliases are supported for user convenience.
func isHelpCommand(s string) bool {
	switch s {
	case `\h`, "h", "help", "?":
		return true
	default:
		return false
	}
}

// printCommandsHelp displays the available commands to the user.
func printCommandsHelp(w io.Writer) {
	fmt.Fprintf(w, "%s", `Available commands:
\q - quit
\h - show this help
\s - singleline json output mode
\m - multiline json output mode
\c - compact output mode
\logfmt - logfmt output mode
\wrap_long_lines - toggles wrapping long lines
\enable_colors - enable ANSI colors in compact output mode
\disable_colors - disable ANSI colors in compact output mode
\tail <query> - live tail <query> results

See https://docs.victoriametrics.com/victorialogs/querying/vlogscli/ for more details
`)
}

// executeQuery routes query execution to the appropriate handler.
//
// Two execution paths:
//  1. Regular query → getQueryResponse → readWithLess (paged output)
//  2. Live tail (\tail prefix) → tailQuery (streaming output, no pager)
//
// The context enables cancellation via Ctrl+C during long-running queries.
func executeQuery(ctx context.Context, output io.Writer, qStr string, outputMode outputMode, disableColors, wrapLongLines bool) {
	// Check for live tailing command prefix.
	if strings.HasPrefix(qStr, `\tail `) {
		tailQuery(ctx, output, qStr, outputMode)
		return
	}

	// Execute regular query and wrap response in prettifier.
	respBody := getQueryResponse(ctx, output, qStr, outputMode, *datasourceURL)
	if respBody == nil {
		return
	}
	defer func() {
		_ = respBody.Close()
	}()

	// Pipe formatted output through less pager (or directly to stdout in non-TTY mode).
	if err := readWithLess(respBody, disableColors, wrapLongLines); err != nil {
		fmt.Fprintf(output, "error when reading query response: %s\n", err)
		return
	}
}

// tailQuery executes a live tailing query that streams new log entries.
//
// Key differences from regular queries:
//   - Uses /select/logsql/tail endpoint instead of /query
//   - No paging (output goes directly to terminal) since tailing is indefinite
//   - Continues until user presses Ctrl+C (context cancellation)
//
// Live tailing is useful for monitoring logs in real-time, similar to `tail -f`.
func tailQuery(ctx context.Context, output io.Writer, qStr string, outputMode outputMode) {
	// Strip the \tail command prefix to get the actual query.
	qStr = strings.TrimPrefix(qStr, `\tail `)
	qURL, err := getTailURL()
	if err != nil {
		fmt.Fprintf(output, "%s\n", err)
		return
	}

	// Reuse the same HTTP request path as regular queries.
	respBody := getQueryResponse(ctx, output, qStr, outputMode, qURL)
	if respBody == nil {
		return
	}
	defer func() {
		_ = respBody.Close()
	}()

	// Stream directly to output without paging.
	// The tail endpoint keeps the connection open and streams new matches.
	if _, err := io.Copy(output, respBody); err != nil {
		// Ignore context cancellation (user pressed Ctrl+C) and broken pipe errors.
		if !errors.Is(err, context.Canceled) && !isErrPipe(err) {
			fmt.Fprintf(output, "error when live tailing query response: %s\n", err)
		}
		fmt.Fprintf(output, "\n")
		return
	}
}

// getTailURL determines the URL for live tailing.
// If -tail.url flag is not set, auto-detects by replacing /query with /tail in datasourceURL.
func getTailURL() (string, error) {
	if *tailURL != "" {
		return *tailURL, nil
	}

	// Parse datasource URL and replace /query suffix with /tail.
	u, err := url.Parse(*datasourceURL)
	if err != nil {
		return "", fmt.Errorf("cannot parse -datasource.url=%q: %w", *datasourceURL, err)
	}
	if !strings.HasSuffix(u.Path, "/query") {
		return "", fmt.Errorf("cannot find /query suffix in -datasource.url=%q", *datasourceURL)
	}
	u.Path = u.Path[:len(u.Path)-len("/query")] + "/tail"
	return u.String(), nil
}

// getQueryResponse sends a LogsQL query to VictoriaLogs and returns a streaming prettified response.
//
// Query Processing Pipeline:
//  1. Parse query client-side for validation and canonicalization
//  2. Build POST request with form-encoded query parameter
//  3. Apply authentication headers and custom headers
//  4. Execute HTTP request with timing measurement
//  5. Wrap response body in jsonPrettifier for streaming output
//
// Returns nil on error (error already printed to output).
func getQueryResponse(ctx context.Context, output io.Writer, qStr string, outputMode outputMode, qURL string) io.ReadCloser {
	// Parse the query and convert it to canonical view.
	// Client-side parsing provides immediate feedback on syntax errors without server round-trip.
	// It also normalizes the query (e.g., whitespace, ordering) for consistent display.
	qStr = strings.TrimSuffix(qStr, ";")
	q, err := logstorage.ParseQuery(qStr)
	if err != nil {
		fmt.Fprintf(output, "cannot parse query: %s\n", err)
		return nil
	}
	qStr = q.String()
	fmt.Fprintf(output, "executing [%s]...", qStr)

	// Prepare HTTP request for qURL
	// VictoriaLogs expects the query as a form-encoded POST body.
	args := make(url.Values)
	args.Set("query", qStr)
	data := strings.NewReader(args.Encode())

	req, err := http.NewRequestWithContext(ctx, "POST", qURL, data)
	if err != nil {
		// This should never happen with validated inputs.
		panic(fmt.Errorf("BUG: cannot prepare request to server: %w", err))
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	// Apply custom headers from -header flags.
	for _, h := range headers {
		req.Header.Set(h.Name, h.Value)
	}

	// Set multi-tenancy headers for tenant-scoped queries.
	req.Header.Set("AccountID", strconv.Itoa(*accountID))
	req.Header.Set("ProjectID", strconv.Itoa(*projectID))

	// Apply authentication headers (basic auth, bearer token, etc.).
	if err := authConfig.SetHeaders(req, true); err != nil {
		fmt.Fprintf(output, "prepare auth info fail: %s\n", err)
		return nil
	}

	// Execute HTTP request at qURL
	startTime := time.Now()
	resp, err := httpClient.Do(req)

	// Report query duration.
	// Prefer server-reported duration header when available (more accurate).
	queryDuration := fmt.Sprintf("client %.3f", time.Since(startTime).Seconds())
	if err == nil {
		qd := resp.Header.Get("VL-Request-Duration-Seconds")
		if qd != "" {
			queryDuration = "server " + qd
		}
	}
	fmt.Fprintf(output, "; duration: %ss\n", queryDuration)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintf(output, "\n")
		} else {
			fmt.Fprintf(output, "cannot execute query: %s\n", err)
		}
		return nil
	}

	// Verify response code
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			body = []byte(fmt.Sprintf("cannot read response body: %s", err))
		}
		fmt.Fprintf(output, "unexpected status code: %d; response body:\n%s\n", resp.StatusCode, body)
		_ = resp.Body.Close()
		return nil
	}

	// Prettify the response body
	// The prettifier runs in a background goroutine and streams formatted output via io.Pipe.
	jp := newJSONPrettifier(resp.Body, outputMode)

	return jp
}

// newHTTPClient creates an HTTP client with authentication and TLS configuration.
// Returns both the auth config (for setting request headers) and the client (for executing requests).
func newHTTPClient() (*promauth.Config, *http.Client) {
	ac := newAuthConfig()
	// Create transport with connection pooling and timeouts.
	tr := httputil.NewTransport(true, "vlogscli")
	c := &http.Client{
		// Wrap transport with auth handling (adds auth headers to each request).
		Transport: ac.NewRoundTripper(tr),
	}

	return ac, c
}

// newAuthConfig builds the authentication configuration from command-line flags.
// Supports multiple auth methods simultaneously (e.g., TLS client certs + bearer token).
func newAuthConfig() *promauth.Config {
	username := *username
	password := password.Get()
	var basicAuthCfg *promauth.BasicAuthConfig
	if username != "" || password != "" {
		basicAuthCfg = &promauth.BasicAuthConfig{
			Username: username,
			Password: promauth.NewSecret(password),
		}
	}

	tlsCfg := &promauth.TLSConfig{
		CAFile:             *tlsCAFile,
		CertFile:           *tlsCertFile,
		KeyFile:            *tlsKeyFile,
		ServerName:         *tlsServerName,
		InsecureSkipVerify: *tlsInsecureSkipVerify,
	}

	opts := &promauth.Options{
		BasicAuth:   basicAuthCfg,
		BearerToken: bearerToken.Get(),
		TLSConfig:   tlsCfg,
	}
	ac, err := opts.NewConfig()
	if err != nil {
		logger.Panicf("FATAL: cannot populate auth config: %s", err)
	}

	return ac
}

// Package-level globals for HTTP client and authentication.
// These are initialized once at startup and reused for all requests.
var httpClient *http.Client
var authConfig *promauth.Config

// headers stores custom HTTP headers parsed from -header flags.
var headers []headerEntry

// headerEntry represents a parsed custom HTTP header.
type headerEntry struct {
	Name  string
	Value string
}

// parseHeaders converts -header flag values into headerEntry structs.
// Expected format: "HeaderName: value" (colon-separated with optional whitespace).
func parseHeaders(a []string) ([]headerEntry, error) {
	hes := make([]headerEntry, len(a))
	for i, s := range a {
		// SplitN with n=2 ensures only the first colon is used as delimiter.
		// This allows header values to contain colons (e.g., dates).
		a := strings.SplitN(s, ":", 2)
		if len(a) != 2 {
			return nil, fmt.Errorf("cannot parse header=%q; it must contain at least one ':'; for example, 'Cookie: foo'", s)
		}
		hes[i] = headerEntry{
			Name:  strings.TrimSpace(a[0]),
			Value: strings.TrimSpace(a[1]),
		}
	}
	return hes, nil
}

// fatalf prints an error message to stderr and exits with code 1.
// Used for unrecoverable errors during initialization.
func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// usage prints the command-line help message.
func usage() {
	const s = `
vlogscli is a command-line tool for querying VictoriaLogs.

See the docs at https://docs.victoriametrics.com/victorialogs/querying/vlogscli/
`
	flagutil.Usage(s)
}
