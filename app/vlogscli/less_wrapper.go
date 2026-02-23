// less_wrapper implements terminal-aware output paging using the 'less' pager.
//
// Architecture Overview:
// This file handles the final stage of vlogscli's output pipeline. It detects whether
// output is going to a terminal and either:
//   - Terminal: Spawns 'less' pager for interactive browsing
//   - Non-terminal: Writes directly to stdout (for piping to other commands)
//
// Signal Handling Complexity:
// When 'less' is running, Ctrl+C must be handled by 'less' itself (for interrupting
// searches, etc.), not by vlogscli. This requires the parent process to ignore SIGINT
// while 'less' is active. The ignoreSignals function manages this temporary signal masking.
//
// Why use 'less' instead of implementing our own pager?
//   - Less is ubiquitous and well-tested
//   - Users are familiar with its keybindings
//   - It handles terminal resizing, search, and navigation efficiently
//
// See onboarding/onboarding-vlogscli.md for comprehensive documentation.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	"github.com/mattn/go-isatty"
)

// isTerminal checks if both stdout and stderr are connected to a terminal.
//
// Why check both?
// The 'less' pager writes to stdout and may emit diagnostics to stderr.
// If either stream is redirected (e.g., vlogscli 2>errors.log), skip pager mode
// and stream directly to stdout.
//
// This check enables scripting support: when output is piped to another command,
// we bypass 'less' and write directly to stdout.
func isTerminal() bool {
	return isatty.IsTerminal(os.Stdout.Fd()) && isatty.IsTerminal(os.Stderr.Fd())
}

// readWithLess pipes output through the 'less' pager for interactive viewing,
// or writes directly to stdout in non-terminal mode.
//
// Terminal Mode (interactive):
// Spawns 'less' with appropriate flags and pipes formatted output to it.
// The function blocks until the user exits 'less'.
//
// Non-Terminal Mode (piped/redirected):
// Writes output directly to stdout without paging, enabling use in scripts and pipes.
//
// Less Flags:
//   - -F: Quit immediately if output fits on one screen (auto-exit for small results)
//   - -X: Don't clear the screen on exit (preserves output in terminal scrollback)
//   - -R: Preserve ANSI color codes (needed when colors are enabled)
//   - -S: Chop long lines instead of wrapping (when wrapLongLines is false)
//
// The LESSCHARSET=utf-8 environment variable ensures proper Unicode display.
func readWithLess(r io.Reader, disableColors, wrapLongLines bool) error {
	if !isTerminal() {
		// Just write everything to stdout if no terminal is available.
		// This path is used when output is piped to another command.
		_, err := io.Copy(os.Stdout, r)
		if err != nil && !isErrPipe(err) {
			return fmt.Errorf("error when forwarding data to stdout: %w", err)
		}
		// Sync ensures all data is written to the underlying file descriptor.
		if err := os.Stdout.Sync(); err != nil {
			return fmt.Errorf("cannot sync data to stdout: %w", err)
		}
		return nil
	}

	// Create a pipe to connect our output to 'less' input.
	// pr will be 'less' stdin, pw will be our write end.
	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("cannot create pipe: %w", err)
	}
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()

	// Temporarily ignore Ctrl+C in vlogscli so interactive SIGINT behavior is handled by 'less'.
	// This keeps pager behavior predictable (e.g. interrupt search, then continue paging).
	cancel := ignoreSignals(os.Interrupt)
	defer cancel()

	// Start 'less' process
	path, err := exec.LookPath("less")
	if err != nil {
		return fmt.Errorf("cannot find 'less' command: %w", err)
	}

	// Build 'less' command-line arguments.
	// -F and -X are always included for better UX.
	opts := []string{"less", "-F", "-X"}
	if !disableColors {
		// -R preserves ANSI color escape sequences in the output.
		opts = append(opts, "-R")
	}
	if !wrapLongLines {
		// -S chops long lines at screen width instead of wrapping.
		// Users can press left/right in less to scroll horizontally.
		opts = append(opts, "-S")
	}

	// Spawn 'less' as a separate process.
	// We connect:
	//   - stdin (Files[0]) → pr (our pipe reader)
	//   - stdout (Files[1]) → os.Stdout (user's terminal)
	//   - stderr (Files[2]) → os.Stderr (user's terminal)
	p, err := os.StartProcess(path, opts, &os.ProcAttr{
		Env:   append(os.Environ(), "LESSCHARSET=utf-8"),
		Files: []*os.File{pr, os.Stdout, os.Stderr},
	})
	if err != nil {
		return fmt.Errorf("cannot start 'less' process: %w", err)
	}

	// Close pr after 'less' finishes in a parallel goroutine
	// in order to unblock forwarding data to stopped 'less' below.
	// This is important for proper cleanup: if 'less' exits (e.g., user presses q),
	// we need to close pr to unblock the io.Copy below.
	waitch := make(chan *os.ProcessState)
	go func() {
		// Wait for 'less' process to finish.
		ps, err := p.Wait()
		if err != nil {
			fatalf("unexpected error when waiting for 'less' process: %w", err)
		}
		_ = pr.Close()
		waitch <- ps
	}()

	// Forward data from r to 'less'
	// This copies formatted output from the jsonPrettifier into the pipe
	// that 'less' is reading from.
	_, err = io.Copy(pw, r)
	_ = pw.Sync()
	_ = pw.Close()

	// Wait until 'less' finished
	ps := <-waitch

	// Verify 'less' status.
	// A non-zero exit could indicate 'less' was killed or encountered an error.
	if !ps.Success() {
		return fmt.Errorf("'less' finished with unexpected code %d", ps.ExitCode())
	}

	// Check for errors during the copy operation.
	// Ignore pipe errors (EPIPE) which occur when 'less' exits before we finish writing.
	if err != nil && !isErrPipe(err) {
		return fmt.Errorf("error when forwarding data to 'less': %w", err)
	}

	return nil
}

// isErrPipe checks if the error is a broken pipe error.
//
// Broken pipe errors occur when:
//   - The reading process ('less') exits before we finish writing
//   - The pipe is closed while we're still writing
//
// These errors are expected and should not be treated as failures.
// They're a normal part of the lifecycle when a user exits 'less' early.
func isErrPipe(err error) bool {
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, io.ErrClosedPipe)
}

// ignoreSignals temporarily ignores the specified signals in the current process.
//
// Why is this needed?
// When 'less' is running as a child process, we want Ctrl+C (SIGINT) to be handled
// by 'less', not by vlogscli. Without this, Ctrl+C would terminate vlogscli instead of
// letting 'less' apply its own interactive SIGINT behavior.
//
// How it works:
//  1. Creates a buffered channel and registers it with signal.Notify
//  2. Spawns a goroutine that drains the channel (ignoring the signals)
//  3. Returns a cancel function that stops notification and closes the channel
//
// The returned cancel function restores normal signal handling when called.
// This must be called after 'less' exits to restore Ctrl+C behavior for future queries.
func ignoreSignals(sigs ...os.Signal) func() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sigs...)

	var wg sync.WaitGroup
	wg.Go(func() {
		// Drain the signal channel, effectively ignoring the signals.
		// The goroutine exits when the channel is closed.
		for {
			_, ok := <-ch
			if !ok {
				return
			}
		}
	})

	// Return cleanup function to restore normal signal handling.
	return func() {
		signal.Stop(ch)
		close(ch)
		wg.Wait()
	}
}
