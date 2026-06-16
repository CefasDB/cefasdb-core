package runner

import (
	"context"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{"empty slice", nil, 50, 0},
		{"p0 returns min", []time.Duration{1, 2, 3}, 0, 1},
		{"p100 returns max", []time.Duration{1, 2, 3}, 100, 3},
		{"p50 mid", []time.Duration{10, 20, 30, 40, 50}, 50, 30},
		{"p95 high", []time.Duration{
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		}, 95, 19},
		{"negative p clamps to min", []time.Duration{5, 6, 7}, -10, 5},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := percentile(tc.sorted, tc.p)
			if got != tc.want {
				t.Fatalf("percentile(%v, %v) = %v, want %v", tc.sorted, tc.p, got, tc.want)
			}
		})
	}
}

func TestShouldSample(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		n    int64
		rate int64
		want bool
	}{
		{"rate 0 always samples", 7, 0, true},
		{"rate 1 always samples", 1, 1, true},
		{"rate 10 hits on 10", 10, 10, true},
		{"rate 10 misses on 1", 1, 10, false},
		{"rate 10 misses on 9", 9, 10, false},
		{"rate 10 hits on 20", 20, 10, true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldSample(tc.n, tc.rate); got != tc.want {
				t.Fatalf("shouldSample(%d,%d) = %v, want %v", tc.n, tc.rate, got, tc.want)
			}
		})
	}
}

func TestThrottleReturnsTrueWhenUncapped(t *testing.T) {
	t.Parallel()
	if !throttle(context.Background(), time.Now(), 100, 0) {
		t.Fatal("throttle with rate=0 should return true")
	}
	if !throttle(context.Background(), time.Now(), 0, 1000) {
		t.Fatal("throttle with emitted=0 should return true")
	}
}

func TestThrottleSleepsToHitTarget(t *testing.T) {
	t.Parallel()
	// rate=1000/s, emitted=10 → target = started + 10ms.
	// If "now" is started, throttle should sleep ~10ms then return true.
	started := time.Now()
	ok := throttle(context.Background(), started, 10, 1000)
	if !ok {
		t.Fatal("throttle should return true on normal completion")
	}
	if elapsed := time.Since(started); elapsed < 5*time.Millisecond {
		t.Fatalf("throttle returned too quickly: %v", elapsed)
	}
}

func TestThrottleAbortsOnCancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Target far in the future. Cancellation should make throttle return false.
	if throttle(ctx, time.Now(), 1, 1) {
		t.Fatal("throttle should return false when context cancelled")
	}
}

func TestDurationMillis(t *testing.T) {
	t.Parallel()
	if got := durationMillis(2 * time.Millisecond); got != 2 {
		t.Fatalf("durationMillis(2ms) = %v, want 2", got)
	}
	if got := durationMillis(500 * time.Microsecond); got != 0.5 {
		t.Fatalf("durationMillis(500us) = %v, want 0.5", got)
	}
}

func TestDurationString(t *testing.T) {
	t.Parallel()
	if got := durationString(0); got != "" {
		t.Fatalf("durationString(0) = %q, want empty", got)
	}
	if got := durationString(2 * time.Second); got != "2s" {
		t.Fatalf("durationString(2s) = %q, want 2s", got)
	}
}

func TestApproximateItemKB(t *testing.T) {
	t.Parallel()
	if got := approximateItemKB(0); got <= 0 {
		t.Fatalf("approximateItemKB(0) should be > 0, got %v", got)
	}
	low := approximateItemKB(0)
	high := approximateItemKB(1024)
	if !(high > low) {
		t.Fatalf("approximateItemKB should grow with payload: low=%v high=%v", low, high)
	}
}

func TestCollectLatenciesSortsAscending(t *testing.T) {
	t.Parallel()
	ch := make(chan workerResult, 3)
	ch <- workerResult{latencies: []time.Duration{5, 1, 3}}
	ch <- workerResult{latencies: []time.Duration{4, 2}}
	ch <- workerResult{latencies: nil}
	close(ch)
	got := collectLatencies(ch)
	want := []time.Duration{1, 2, 3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("collectLatencies len = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("collectLatencies[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
