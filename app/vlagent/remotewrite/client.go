package remotewrite

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/persistentqueue"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/ratelimiter"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
	"github.com/VictoriaMetrics/metrics"
)

// Configuration flags for HTTP client behavior.
var (
	// Rate limiting flags.
	rateLimit = flagutil.NewArrayInt("remoteWrite.rateLimit", 0, "Optional rate limit in bytes per second for data sent to the corresponding -remoteWrite.url. "+
		"By default, the rate limit is disabled. It can be useful for limiting load on remote storage when big amounts of buffered data ")

	// Timeout and retry flags.
	sendTimeout      = flagutil.NewArrayDuration("remoteWrite.sendTimeout", time.Minute, "Timeout for sending a single block of data to the corresponding -remoteWrite.url")
	retryMinInterval = flagutil.NewArrayDuration("remoteWrite.retryMinInterval", time.Second, "The minimum delay between retry attempts to send a block of data to the corresponding -remoteWrite.url. Every next retry attempt will double the delay to prevent hammering of remote database. See also -remoteWrite.retryMaxTime")
	retryMaxTime     = flagutil.NewArrayDuration("remoteWrite.retryMaxTime", time.Minute, "The max time spent on retry attempts to send a block of data to the corresponding -remoteWrite.url. Change this value if it is expected for -remoteWrite.url to be unreachable for more than -remoteWrite.retryMaxTime. See also -remoteWrite.retryMinInterval")

	// Proxy configuration.
	proxyURL = flagutil.NewArrayString("remoteWrite.proxyURL", "Optional proxy URL for writing data to the corresponding -remoteWrite.url. "+
		"Supported proxies: http, https, socks5. Example: -remoteWrite.proxyURL=socks5://proxy:1234")

	// TLS configuration flags.
	tlsHandshakeTimeout   = flagutil.NewArrayDuration("remoteWrite.tlsHandshakeTimeout", 20*time.Second, "The timeout for establishing tls connections to the corresponding -remoteWrite.url")
	tlsInsecureSkipVerify = flagutil.NewArrayBool("remoteWrite.tlsInsecureSkipVerify", "Whether to skip tls verification when connecting to the corresponding -remoteWrite.url")
	tlsCertFile           = flagutil.NewArrayString("remoteWrite.tlsCertFile", "Optional path to client-side TLS certificate file to use when connecting "+
		"to the corresponding -remoteWrite.url")
	tlsKeyFile = flagutil.NewArrayString("remoteWrite.tlsKeyFile", "Optional path to client-side TLS certificate key to use when connecting to the corresponding -remoteWrite.url")
	tlsCAFile  = flagutil.NewArrayString("remoteWrite.tlsCAFile", "Optional path to TLS CA file to use for verifying connections to the corresponding -remoteWrite.url. "+
		"By default, system CA is used")
	tlsServerName = flagutil.NewArrayString("remoteWrite.tlsServerName", "Optional TLS server name to use for connections to the corresponding -remoteWrite.url. "+
		"By default, the server name from -remoteWrite.url is used")

	// Custom headers.
	headers = flagutil.NewArrayString("remoteWrite.headers", "Optional HTTP headers to send with each request to the corresponding -remoteWrite.url. "+
		"For example, -remoteWrite.headers='My-Auth:foobar' would send 'My-Auth: foobar' HTTP header with every request to the corresponding -remoteWrite.url. "+
		"Multiple headers must be delimited by '^^': -remoteWrite.headers='header1:value1^^header2:value2'")

	// Authentication flags - Basic Auth.
	basicAuthUsername     = flagutil.NewArrayString("remoteWrite.basicAuth.username", "Optional basic auth username to use for the corresponding -remoteWrite.url")
	basicAuthPassword     = flagutil.NewArrayString("remoteWrite.basicAuth.password", "Optional basic auth password to use for the corresponding -remoteWrite.url")
	basicAuthPasswordFile = flagutil.NewArrayString("remoteWrite.basicAuth.passwordFile", "Optional path to basic auth password to use for the corresponding -remoteWrite.url. "+
		"The file is re-read every second")

	// Authentication flags - Bearer Token.
	bearerToken     = flagutil.NewArrayString("remoteWrite.bearerToken", "Optional bearer auth token to use for the corresponding -remoteWrite.url")
	bearerTokenFile = flagutil.NewArrayString("remoteWrite.bearerTokenFile", "Optional path to bearer token file to use for the corresponding -remoteWrite.url. "+
		"The token is re-read from the file every second")

	// Authentication flags - OAuth2.
	oauth2ClientID         = flagutil.NewArrayString("remoteWrite.oauth2.clientID", "Optional OAuth2 clientID to use for the corresponding -remoteWrite.url")
	oauth2ClientSecret     = flagutil.NewArrayString("remoteWrite.oauth2.clientSecret", "Optional OAuth2 clientSecret to use for the corresponding -remoteWrite.url")
	oauth2ClientSecretFile = flagutil.NewArrayString("remoteWrite.oauth2.clientSecretFile", "Optional OAuth2 clientSecretFile to use for the corresponding -remoteWrite.url")
	oauth2EndpointParams   = flagutil.NewArrayString("remoteWrite.oauth2.endpointParams", "Optional OAuth2 endpoint parameters to use for the corresponding -remoteWrite.url . "+
		`The endpoint parameters must be set in JSON format: {"param1":"value1",...,"paramN":"valueN"}`)
	oauth2TokenURL = flagutil.NewArrayString("remoteWrite.oauth2.tokenUrl", "Optional OAuth2 tokenURL to use for the corresponding -remoteWrite.url")
	oauth2Scopes   = flagutil.NewArrayString("remoteWrite.oauth2.scopes", "Optional OAuth2 scopes to use for the corresponding -remoteWrite.url. Scopes must be delimited by ';'")
)

// client manages HTTP communication with a single remote VictoriaLogs instance.
//
// It maintains a pool of worker goroutines that:
//  1. Read blocks from the persistent queue
//  2. Send them via HTTP POST to the remote URL
//  3. Handle retries with exponential backoff on failures
//  4. Return unsent blocks to the queue if retries are exhausted
//
// The client supports:
//   - Multiple authentication methods (basic auth, bearer token, OAuth2)
//   - TLS client certificates
//   - Custom headers
//   - HTTP/HTTPS/SOCKS5 proxies
//   - Rate limiting
//   - Automatic retry with exponential backoff
type client struct {
	// sanitizedURL is the URL for metrics/logging (may hide credentials).
	sanitizedURL string

	// remoteWriteURL is the actual URL with ?version=v1 query param.
	remoteWriteURL string

	// fq is the source persistent queue to read blocks from.
	fq *persistentqueue.FastQueue

	// hc is the HTTP client with authentication transport.
	hc *http.Client

	// Retry configuration.
	retryMinInterval time.Duration
	retryMaxTime     time.Duration

	// sendBlock is the function that sends a block.
	// Pluggable for testing, defaults to sendBlockHTTP.
	sendBlock func(block []byte) bool

	// authCfg contains authentication configuration.
	authCfg *promauth.Config

	// rl is an optional rate limiter.
	rl *ratelimiter.RateLimiter

	// Metrics for monitoring.
	bytesSent       *metrics.Counter
	blocksSent      *metrics.Counter
	requestDuration *metrics.Histogram
	requestsOKCount *metrics.Counter
	errorsCount     *metrics.Counter
	packetsDropped  *metrics.Counter
	rateLimit       *metrics.Gauge
	retriesCount    *metrics.Counter
	sendDuration    *metrics.FloatCounter

	// Lifecycle management.
	wg     sync.WaitGroup
	stopCh chan struct{}
}

// newHTTPClient creates a new HTTP client for a remote URL.
//
// This function:
//  1. Parses authentication configuration
//  2. Configures the HTTP transport (TLS, proxy, connection pooling)
//  3. Creates the HTTP client with timeout
func newHTTPClient(argIdx int, remoteWriteURL, sanitizedURL string, fq *persistentqueue.FastQueue, concurrency int) *client {
	authCfg, err := getAuthConfig(argIdx)
	if err != nil {
		logger.Fatalf("cannot initialize auth config for -remoteWrite.url=%q: %s", remoteWriteURL, err)
	}

	// Configure the HTTP transport.
	tr := httputil.NewTransport(false, "vlagent_remotewrite")
	tr.TLSHandshakeTimeout = tlsHandshakeTimeout.GetOptionalArg(argIdx)
	tr.MaxConnsPerHost = 2 * concurrency     // Allow 2 connections per worker
	tr.MaxIdleConnsPerHost = 2 * concurrency // Keep connections alive
	tr.IdleConnTimeout = time.Minute         // Close idle connections after 1 minute
	tr.WriteBufferSize = 64 * 1024           // 64KB write buffer

	// Configure proxy if specified.
	pURL := proxyURL.GetOptionalArg(argIdx)
	if len(pURL) > 0 {
		if !strings.Contains(pURL, "://") {
			logger.Fatalf("cannot parse -remoteWrite.proxyURL=%q: it must start with `http://`, `https://` or `socks5://`", pURL)
		}
		pu, err := url.Parse(pURL)
		if err != nil {
			logger.Fatalf("cannot parse -remoteWrite.proxyURL=%q: %s", pURL, err)
		}
		tr.Proxy = http.ProxyURL(pu)
	}

	// Wrap transport with authentication.
	hc := &http.Client{
		Transport: authCfg.NewRoundTripper(tr),
		Timeout:   sendTimeout.GetOptionalArg(argIdx),
	}

	c := &client{
		sanitizedURL:     sanitizedURL,
		remoteWriteURL:   remoteWriteURL,
		authCfg:          authCfg,
		fq:               fq,
		hc:               hc,
		retryMinInterval: retryMinInterval.GetOptionalArg(argIdx),
		retryMaxTime:     retryMaxTime.GetOptionalArg(argIdx),
		stopCh:           make(chan struct{}),
	}
	c.sendBlock = c.sendBlockHTTP
	return c
}

// init starts the worker goroutines and initializes metrics.
//
// Each worker runs independently, reading blocks from the queue and sending them.
// Multiple workers allow parallel HTTP requests for higher throughput.
func (c *client) init(argIdx, concurrency int, sanitizedURL string) {
	// Initialize rate limiter if configured.
	limitReached := metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_rate_limit_reached_total{url=%q}`, c.sanitizedURL))
	if bytesPerSec := rateLimit.GetOptionalArg(argIdx); bytesPerSec > 0 {
		logger.Infof("applying %d bytes per second rate limit for -remoteWrite.url=%q", bytesPerSec, sanitizedURL)
		c.rl = ratelimiter.New(int64(bytesPerSec), limitReached, c.stopCh)
	}

	// Initialize metrics.
	c.bytesSent = metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_bytes_sent_total{url=%q}`, c.sanitizedURL))
	c.blocksSent = metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_blocks_sent_total{url=%q}`, c.sanitizedURL))
	c.rateLimit = metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_rate_limit{url=%q}`, c.sanitizedURL), func() float64 {
		return float64(rateLimit.GetOptionalArg(argIdx))
	})
	c.requestDuration = metrics.GetOrCreateHistogram(fmt.Sprintf(`vlagent_remotewrite_duration_seconds{url=%q}`, c.sanitizedURL))
	c.requestsOKCount = metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_requests_total{url=%q, status_code="2XX"}`, c.sanitizedURL))
	c.errorsCount = metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_errors_total{url=%q}`, c.sanitizedURL))
	c.packetsDropped = metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_packets_dropped_total{url=%q}`, c.sanitizedURL))
	c.retriesCount = metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_retries_count_total{url=%q}`, c.sanitizedURL))
	c.sendDuration = metrics.GetOrCreateFloatCounter(fmt.Sprintf(`vlagent_remotewrite_send_duration_seconds_total{url=%q}`, c.sanitizedURL))
	metrics.GetOrCreateGauge(fmt.Sprintf(`vlagent_remotewrite_queues{url=%q}`, c.sanitizedURL), func() float64 {
		return float64(*queues)
	})

	// Start worker goroutines.
	for i := 0; i < concurrency; i++ {
		c.wg.Go(c.runWorker)
	}
	logger.Infof("initialized client for -remoteWrite.url=%q", c.sanitizedURL)
}

// MustStop gracefully shuts down the client.
func (c *client) MustStop() {
	close(c.stopCh)
	c.wg.Wait()
	logger.Infof("stopped client for -remoteWrite.url=%q", c.sanitizedURL)
}

// getAuthConfig builds the authentication configuration from flags.
//
// Supports multiple authentication methods:
//   - Basic Auth (username/password)
//   - Bearer Token
//   - OAuth2 (client credentials flow)
//   - TLS Client Certificates
//   - Custom Headers
func getAuthConfig(argIdx int) (*promauth.Config, error) {
	// Parse custom headers (delimited by ^^).
	headersValue := headers.GetOptionalArg(argIdx)
	var hdrs []string
	if headersValue != "" {
		hdrs = strings.Split(headersValue, "^^")
	}

	// Build basic auth configuration.
	username := basicAuthUsername.GetOptionalArg(argIdx)
	password := basicAuthPassword.GetOptionalArg(argIdx)
	passwordFile := basicAuthPasswordFile.GetOptionalArg(argIdx)
	var basicAuthCfg *promauth.BasicAuthConfig
	if username != "" || password != "" || passwordFile != "" {
		basicAuthCfg = &promauth.BasicAuthConfig{
			Username:     username,
			Password:     promauth.NewSecret(password),
			PasswordFile: passwordFile,
		}
	}

	// Bearer token configuration.
	token := bearerToken.GetOptionalArg(argIdx)
	tokenFile := bearerTokenFile.GetOptionalArg(argIdx)

	// OAuth2 configuration.
	var oauth2Cfg *promauth.OAuth2Config
	clientSecret := oauth2ClientSecret.GetOptionalArg(argIdx)
	clientSecretFile := oauth2ClientSecretFile.GetOptionalArg(argIdx)
	if clientSecretFile != "" || clientSecret != "" {
		endpointParamsJSON := oauth2EndpointParams.GetOptionalArg(argIdx)
		endpointParams, err := flagutil.ParseJSONMap(endpointParamsJSON)
		if err != nil {
			return nil, fmt.Errorf("cannot parse JSON for -remoteWrite.oauth2.endpointParams=%s: %w", endpointParamsJSON, err)
		}
		oauth2Cfg = &promauth.OAuth2Config{
			ClientID:         oauth2ClientID.GetOptionalArg(argIdx),
			ClientSecret:     promauth.NewSecret(clientSecret),
			ClientSecretFile: clientSecretFile,
			EndpointParams:   endpointParams,
			TokenURL:         oauth2TokenURL.GetOptionalArg(argIdx),
			Scopes:           strings.Split(oauth2Scopes.GetOptionalArg(argIdx), ";"),
		}
	}

	// TLS configuration.
	tlsCfg := &promauth.TLSConfig{
		CAFile:             tlsCAFile.GetOptionalArg(argIdx),
		CertFile:           tlsCertFile.GetOptionalArg(argIdx),
		KeyFile:            tlsKeyFile.GetOptionalArg(argIdx),
		ServerName:         tlsServerName.GetOptionalArg(argIdx),
		InsecureSkipVerify: tlsInsecureSkipVerify.GetOptionalArg(argIdx),
	}

	opts := &promauth.Options{
		BasicAuth:       basicAuthCfg,
		BearerToken:     token,
		BearerTokenFile: tokenFile,
		OAuth2:          oauth2Cfg,
		TLSConfig:       tlsCfg,
		Headers:         hdrs,
	}
	authCfg, err := opts.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot populate auth config for remoteWrite idx: %d, err: %w", argIdx, err)
	}
	return authCfg, nil
}

// runWorker is the main loop for a worker goroutine.
//
// Each worker:
//  1. Reads a block from the persistent queue (blocking)
//  2. Sends the block via HTTP in a background goroutine
//  3. Waits for the send to complete (or stop signal)
//  4. Returns unsent blocks to the queue if needed
//
// The async send pattern allows the worker to start reading the next block
// while the previous block is being sent, improving throughput.
func (c *client) runWorker() {
	var ok bool
	var block []byte
	ch := make(chan bool, 1)

	for {
		// Read a block from the queue (blocking).
		block, ok = c.fq.MustReadBlock(block[:0])
		if !ok {
			// Queue is closed - exit.
			return
		}
		if len(block) == 0 {
			// Skip empty blocks.
			continue
		}

		// Send the block asynchronously.
		// This allows reading the next block while sending.
		go func() {
			startTime := time.Now()
			ch <- c.sendBlock(block)
			c.sendDuration.Add(time.Since(startTime).Seconds())
		}()

		// Wait for send to complete or stop signal.
		select {
		case ok := <-ch:
			if ok {
				// Block sent successfully - continue.
				continue
			}
			// Send failed after all retries - return block to queue and exit.
			c.fq.MustWriteBlockIgnoreDisabledPQ(block)
			return
		case <-c.stopCh:
			// Shutdown requested - wait briefly for in-flight send.
			graceDuration := 5 * time.Second
			select {
			case ok := <-ch:
				if !ok {
					// Return unsent block to queue.
					c.fq.MustWriteBlockIgnoreDisabledPQ(block)
				}
			case <-time.After(graceDuration):
				// Timeout - return block to queue.
				c.fq.MustWriteBlockIgnoreDisabledPQ(block)
			}
			return
		}
	}
}

// doRequest performs an HTTP request with automatic retry on stale connections.
//
// If the first request fails with EOF/UnexpectedEOF, it's retried once.
// This handles the case where a connection was reused but became stale
// (e.g., the server closed it between requests).
//
// See: https://github.com/VictoriaMetrics/VictoriaMetrics/issues/4139
func (c *client) doRequest(url string, body []byte) (*http.Response, error) {
	req, err := c.newRequest(url, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err == nil {
		return resp, nil
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	// It is likely the connection became stale or timed out during the first request.
	// Make another attempt in hope the request will succeed.
	req, err = c.newRequest(url, body)
	if err != nil {
		return nil, fmt.Errorf("second attempt: %w", err)
	}
	resp, err = c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("second attempt: %w", err)
	}
	return resp, nil
}

// newRequest creates an HTTP POST request with appropriate headers.
func (c *client) newRequest(url string, body []byte) (*http.Request, error) {
	reqBody := bytes.NewBuffer(body)
	req, err := http.NewRequest(http.MethodPost, url, reqBody)
	if err != nil {
		logger.Panicf("BUG: unexpected error from http.NewRequest(%q): %s", url, err)
	}
	err = c.authCfg.SetHeaders(req, true)
	if err != nil {
		return nil, err
	}
	h := req.Header
	h.Set("User-Agent", "vlagent")
	h.Set("Content-Encoding", "zstd")
	h.Set("Content-Type", "application/octet-stream")

	return req, nil
}

// sendBlockHTTP sends a block to the remote URL with retry logic.
//
// Retry behavior:
//   - Connection errors: Retry indefinitely with exponential backoff
//   - HTTP 2XX: Success - block acknowledged
//   - HTTP 400/404: Client error - block dropped (not retried)
//   - HTTP 5XX/other: Retry with exponential backoff
//
// The Retry-After header (RFC 7231) is respected if present.
//
// Returns false only if stopCh is closed. Otherwise retries indefinitely.
func (c *client) sendBlockHTTP(block []byte) bool {
	// Apply rate limiting if configured.
	c.rl.Register(len(block))

	// Initialize retry parameters with jitter.
	maxRetryDuration := timeutil.AddJitterToDuration(c.retryMaxTime)
	retryDuration := timeutil.AddJitterToDuration(c.retryMinInterval)
	retriesCount := 0

again:
	startTime := time.Now()
	resp, err := c.doRequest(c.remoteWriteURL, block)
	c.requestDuration.UpdateDuration(startTime)

	if err != nil {
		c.errorsCount.Inc()

		// Exponential backoff: double the delay, cap at max.
		retryDuration *= 2
		if retryDuration > maxRetryDuration {
			retryDuration = maxRetryDuration
		}

		remoteWriteRetryLogger.Warnf("couldn't send a block with size %d bytes to %q: %s; re-sending the block in %.3f seconds",
			len(block), c.sanitizedURL, err, retryDuration.Seconds())

		// Wait before retry, or exit if stopped.
		t := timerpool.Get(retryDuration)
		select {
		case <-c.stopCh:
			timerpool.Put(t)
			return false
		case <-t.C:
			timerpool.Put(t)
		}
		c.retriesCount.Inc()
		goto again
	}

	statusCode := resp.StatusCode
	if statusCode/100 == 2 {
		// Success!
		_ = resp.Body.Close()
		c.requestsOKCount.Inc()
		c.bytesSent.Add(len(block))
		c.blocksSent.Inc()
		return true
	}

	// Track non-2XX status codes.
	metrics.GetOrCreateCounter(fmt.Sprintf(`vlagent_remotewrite_requests_total{url=%q, status_code="%d"}`, c.sanitizedURL, statusCode)).Inc()

	// Client errors (400, 404) are not retried - the block is invalid.
	if statusCode == 400 || statusCode == 404 {
		logBlockRejected(block, c.sanitizedURL, resp)
		_ = resp.Body.Close()
		c.packetsDropped.Inc()
		return true // Return true to acknowledge and move on.
	}

	// Server errors and other status codes are retried.
	retriesCount++
	retryAfterHeader := parseRetryAfterHeader(resp.Header.Get("Retry-After"))
	retryDuration = getRetryDuration(retryAfterHeader, retryDuration, maxRetryDuration)

	// Read and log the response body for debugging.
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		logger.Errorf("cannot read response body from %q during retry #%d: %s", c.sanitizedURL, retriesCount, err)
	} else {
		logger.Errorf("unexpected status code received after sending a block with size %d bytes to %q during retry #%d: %d; response body=%q; "+
			"re-sending the block in %.3f seconds", len(block), c.sanitizedURL, retriesCount, statusCode, body, retryDuration.Seconds())
	}

	// Wait before retry.
	t := timerpool.Get(retryDuration)
	select {
	case <-c.stopCh:
		timerpool.Put(t)
		return false
	case <-t.C:
		timerpool.Put(t)
	}
	c.retriesCount.Inc()
	goto again
}

// Throttled loggers to prevent log spam during outages.
var remoteWriteRejectedLogger = logger.WithThrottler("remoteWriteRejected", 5*time.Second)
var remoteWriteRetryLogger = logger.WithThrottler("remoteWriteRetry", 5*time.Second)

// getRetryDuration calculates the retry delay.
//
// Priority:
//  1. Retry-After header (if present and valid)
//  2. Exponential backoff (double the previous delay, capped at max)
//
// Jitter is added to prevent synchronized retries across multiple vlagent instances.
func getRetryDuration(retryAfterDuration, retryDuration, maxRetryDuration time.Duration) time.Duration {
	// Retry-After header has highest priority.
	if retryAfterDuration > 0 {
		return timeutil.AddJitterToDuration(retryAfterDuration)
	}

	// Exponential backoff.
	retryDuration *= 2
	if retryDuration > maxRetryDuration {
		retryDuration = maxRetryDuration
	}

	return retryDuration
}

// logBlockRejected logs when a block is rejected by the remote server.
// This happens for 400 and 404 responses, indicating the block is invalid.
func logBlockRejected(block []byte, sanitizedURL string, resp *http.Response) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		remoteWriteRejectedLogger.Errorf("sending a block with size %d bytes to %q was rejected (skipping the block): status code %d; "+
			"failed to read response body: %s",
			len(block), sanitizedURL, resp.StatusCode, err)
	} else {
		remoteWriteRejectedLogger.Errorf("sending a block with size %d bytes to %q was rejected (skipping the block): status code %d; response body: %s",
			len(block), sanitizedURL, resp.StatusCode, string(body))
	}
}

// parseRetryAfterHeader parses the Retry-After HTTP header.
//
// The value can be either:
//   - A number of seconds (e.g., "120")
//   - An HTTP date (e.g., "Fri, 31 Dec 1999 23:59:59 GMT")
//
// Returns 0 if the header is missing or invalid.
// See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
func parseRetryAfterHeader(retryAfterString string) (retryAfterDuration time.Duration) {
	if retryAfterString == "" {
		return retryAfterDuration
	}

	defer func() {
		v := retryAfterDuration.Seconds()
		logger.Infof("'Retry-After: %s' parsed into %.2f second(s)", retryAfterString, v)
	}()

	// Try HTTP date format first.
	if parsedTime, err := time.Parse(http.TimeFormat, retryAfterString); err == nil {
		return time.Duration(time.Until(parsedTime).Seconds()) * time.Second
	}

	// Try seconds format.
	if seconds, err := strconv.Atoi(retryAfterString); err == nil {
		return time.Duration(seconds) * time.Second
	}

	return 0
}
