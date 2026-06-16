// Package wait offers small event-driven helpers for tests that need
// to observe an asynchronous condition without sprinkling time.Sleep
// across the codebase. Sleep-based waits hide flakiness behind padded
// timeouts and slow the suite for no reason; the helpers here poll
// cheaply, exit as soon as the condition is met, and fail with a
// useful message when it isn't.
package wait

import (
	"testing"
	"time"
)

// Eventually polls cond every interval until it returns true or
// timeout elapses. The test fails (via t.Fatalf) if the condition
// never becomes true. It is safe to call cond concurrently with other
// goroutines as long as cond itself is.
//
// Use this in place of time.Sleep(duration) followed by an assertion
// — it shortens the suite when the condition is met quickly and
// produces a self-describing failure when it isn't.
func Eventually(t testing.TB, cond func() bool, timeout, interval time.Duration, msgAndArgs ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			if len(msgAndArgs) == 0 {
				t.Fatalf("wait.Eventually: condition not satisfied after %s", timeout)
			}
			fail(t, "wait.Eventually: condition not satisfied after "+timeout.String(), msgAndArgs)
			return
		}
		time.Sleep(interval)
	}
}

// Never asserts that cond stays false for at least duration. Useful
// for catching premature firing of an asynchronous event (e.g. a
// leader election that should not happen before a quorum is healthy).
func Never(t testing.TB, cond func() bool, duration, interval time.Duration, msgAndArgs ...any) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if cond() {
			if len(msgAndArgs) == 0 {
				t.Fatalf("wait.Never: condition became true within %s", duration)
			}
			fail(t, "wait.Never: condition became true within "+duration.String(), msgAndArgs)
			return
		}
		time.Sleep(interval)
	}
}

// For sleeps for d as a deliberate, documented pacing primitive — use
// this only when the test genuinely needs to elapse wall-clock time
// (e.g. validating a TTL or a rate limiter). Calling time.Sleep
// directly in tests is forbidden by .golangci.yml in later phases;
// going through wait.For surfaces the intent in code review.
func For(d time.Duration) { time.Sleep(d) }

func fail(t testing.TB, prefix string, msgAndArgs []any) {
	t.Helper()
	switch v := msgAndArgs[0].(type) {
	case string:
		t.Fatalf(prefix+": "+v, msgAndArgs[1:]...)
	default:
		args := make([]any, 0, len(msgAndArgs)+1)
		args = append(args, prefix)
		args = append(args, v)
		args = append(args, msgAndArgs[1:]...)
		t.Fatalf("%s %v", args...)
	}
}
