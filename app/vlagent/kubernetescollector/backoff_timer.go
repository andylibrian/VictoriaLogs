package kubernetescollector

import (
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timerpool"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/timeutil"
)

// backoffTimer implements an exponential backoff timer with jitter.
//
// This is used in three places in vlagent:
//   - Kubernetes API watch reconnection: 200ms-30s
//   - File polling when no new data: 100ms-10s
//   - HTTP retry in remotewrite client: configurable min/max
//
// The exponential backoff prevents:
//   - Hammering the Kubernetes API during outages
//   - Busy-waiting on idle log files
//   - Overwhelming remote storage during network issues
//
// Jitter (up to +10%, capped at +10s) is added to prevent synchronized
// retry storms across multiple vlagent instances.
type backoffTimer struct {
	// min is the minimum/initial delay.
	min time.Duration

	// max is the maximum delay cap.
	max time.Duration

	// current is the current delay (doubled after each wait until max).
	current time.Duration

	// timer is a reusable timer from timerpool.
	timer *time.Timer
}

// newBackoffTimer creates a new backoff timer with the specified range.
//
// The delay starts at minDelay and doubles after each wait() call
// until it reaches maxDelay. Call reset() to return to minDelay.
//
// The caller must call stop() when the backoffTimer is no longer needed
// to return the timer to the pool.
func newBackoffTimer(minDelay, maxDelay time.Duration) backoffTimer {
	return backoffTimer{
		min:     minDelay,
		max:     maxDelay,
		current: minDelay,
	}
}

// wait sleeps for the current delay with jitter, then doubles the delay.
//
// The actual sleep duration is current + jitter (up to +10%, max +10s).
// After sleeping, the delay is doubled for the next call, capped at max.
//
// If stopCh is closed during the wait, the function returns immediately.
// Use currentDelay() to get the delay that will be used (before jitter).
func (bt *backoffTimer) wait(stopCh <-chan struct{}) {
	// Add jitter to prevent synchronized retries.
	v := timeutil.AddJitterToDuration(bt.current)

	// Double the delay for next time, capped at max.
	bt.current *= 2
	if bt.current > bt.max {
		bt.current = bt.max
	}

	// Use timerpool for efficient timer reuse.
	if bt.timer == nil {
		bt.timer = timerpool.Get(v)
	} else {
		bt.timer.Reset(v)
	}

	// Wait for either the timer or stop signal.
	select {
	case <-stopCh:
		bt.timer.Stop()
	case <-bt.timer.C:
	}
}

// currentDelay returns the current backoff duration (before jitter).
// This is useful for logging how long the next wait will be.
func (bt *backoffTimer) currentDelay() time.Duration {
	return bt.current
}

// reset returns the backoff delay to its minimum.
// Call this after a successful operation to reset the backoff.
func (bt *backoffTimer) reset() {
	bt.current = bt.min
}

// stop releases the timer back to the pool.
// Call this when the backoffTimer is no longer needed.
func (bt *backoffTimer) stop() {
	if bt.timer != nil {
		timerpool.Put(bt.timer)
		bt.timer = nil
	}
}
