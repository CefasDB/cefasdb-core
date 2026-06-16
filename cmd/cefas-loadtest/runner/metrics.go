package runner

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

func collectLatencies(results <-chan workerResult) []time.Duration {
	var latencies []time.Duration
	for result := range results {
		latencies = append(latencies, result.latencies...)
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	return latencies
}

func shouldSample(n, rate int64) bool {
	return rate <= 1 || n%rate == 0
}

func throttle(ctx context.Context, started time.Time, emitted, rate int64) bool {
	if rate <= 0 || emitted <= 0 {
		return true
	}
	target := started.Add(time.Duration(float64(emitted) / float64(rate) * float64(time.Second)))
	sleep := time.Until(target)
	if sleep <= 0 {
		return true
	}
	timer := time.NewTimer(sleep)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func durationString(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}

func approximateItemKB(payloadBytes int) float64 {
	// Rough payload size for comparing runs; exact storage footprint includes key
	// encoding, indexes, log records, and Pebble metadata.
	return float64(payloadBytes+220) / 1024
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := int((p / 100) * float64(len(sorted)-1))
	return sorted[idx]
}

func startProgress(name string, interval time.Duration, current *atomic.Int64, total int64, started time.Time) func() {
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var last int64
		var lastAt = started
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				cur := current.Load()
				delta := cur - last
				window := now.Sub(lastAt).Seconds()
				totalRate := float64(cur) / now.Sub(started).Seconds()
				windowRate := float64(delta) / window
				if total > 0 {
					fmt.Printf("%s progress: %d/%d total=%.0f/s window=%.0f/s\n", name, cur, total, totalRate, windowRate)
				} else {
					fmt.Printf("%s progress: %d total=%.0f/s window=%.0f/s\n", name, cur, totalRate, windowRate)
				}
				last = cur
				lastAt = now
			}
		}
	}()
	return func() { close(done) }
}
