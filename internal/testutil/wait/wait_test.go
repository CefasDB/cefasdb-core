package wait_test

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/testutil/wait"
)

func TestEventuallyReturnsWhenConditionFires(t *testing.T) {
	t.Parallel()
	var flag atomic.Bool
	go func() {
		time.Sleep(5 * time.Millisecond)
		flag.Store(true)
	}()
	wait.Eventually(t, flag.Load, 200*time.Millisecond, 1*time.Millisecond)
}

func TestEventuallyFailsWhenConditionNeverFires(t *testing.T) {
	t.Parallel()
	tb := &recordingT{}
	wait.Eventually(tb, func() bool { return false }, 5*time.Millisecond, 1*time.Millisecond, "polling %s", "counter")
	if !tb.failed {
		t.Fatalf("expected wait.Eventually to fail when condition never fires")
	}
	if tb.msg == "" || !contains(tb.msg, "polling counter") {
		t.Fatalf("failure message lost format args: %q", tb.msg)
	}
}

func TestNeverPassesWhenConditionStaysFalse(t *testing.T) {
	t.Parallel()
	wait.Never(t, func() bool { return false }, 5*time.Millisecond, 1*time.Millisecond)
}

func TestNeverFailsWhenConditionFiresEarly(t *testing.T) {
	t.Parallel()
	tb := &recordingT{}
	var flag atomic.Bool
	go func() {
		time.Sleep(2 * time.Millisecond)
		flag.Store(true)
	}()
	wait.Never(tb, flag.Load, 50*time.Millisecond, 1*time.Millisecond, "leader=%q", "node-7")
	if !tb.failed {
		t.Fatalf("expected wait.Never to fail when condition fires")
	}
	if !contains(tb.msg, "leader=\"node-7\"") {
		t.Fatalf("failure message lost format args: %q", tb.msg)
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}

// recordingT is the minimal testing.TB subset Eventually / Never use.
// It captures Fatalf without aborting the goroutine so the wrapper
// tests can assert on the failure message.
type recordingT struct {
	testing.TB
	failed bool
	msg    string
}

func (r *recordingT) Helper() {}

func (r *recordingT) Fatalf(format string, args ...any) {
	r.failed = true
	r.msg = fmt.Sprintf(format, args...)
}
