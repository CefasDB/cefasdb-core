package audience_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/plugin/audience"
)

// Benchmarks exist so the PR can justify the "≤2x slowdown" target
// from issue #243. Run with: go test -bench=. -run=^$ ./pkg/plugin/audience/.

func BenchmarkDedupEphemeral(b *testing.B) {
	now := time.Unix(1, 0)
	s := audience.NewMemoryStore(func() time.Time { return now })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.CheckDedup("c1", strconv.Itoa(i), time.Hour)
	}
}

func BenchmarkDedupDurable(b *testing.B) {
	now := time.Unix(1, 0)
	be := audience.NewMemoryBackend()
	s, err := audience.NewDurableStore(audience.StoreOptions{
		ID:            "ads",
		Backend:       be,
		Now:           func() time.Time { return now },
		SweepInterval: time.Hour,
	})
	if err != nil {
		b.Fatalf("durable: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.CheckDedup("c1", strconv.Itoa(i), time.Hour)
	}
}

func BenchmarkFreqCapEphemeral(b *testing.B) {
	now := time.Unix(1, 0)
	s := audience.NewMemoryStore(func() time.Time { return now })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.CheckFreqCap("c1", "u"+strconv.Itoa(i%128), 1000, time.Hour)
	}
}

func BenchmarkFreqCapDurable(b *testing.B) {
	now := time.Unix(1, 0)
	be := audience.NewMemoryBackend()
	s, err := audience.NewDurableStore(audience.StoreOptions{
		ID:            "ads",
		Backend:       be,
		Now:           func() time.Time { return now },
		SweepInterval: time.Hour,
	})
	if err != nil {
		b.Fatalf("durable: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.CheckFreqCap("c1", "u"+strconv.Itoa(i%128), 1000, time.Hour)
	}
}
