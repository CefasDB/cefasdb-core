package hll_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/hll"
)

func TestEstimateWithinErrorBound(t *testing.T) {
	const n = 100_000
	s := hll.New(14) // precision 14 → std error ≈ 0.81%
	for i := 0; i < n; i++ {
		s.Observe(fmt.Appendf(nil, "user-%d", i))
	}
	est := s.Estimate()
	relErr := math.Abs(est-float64(n)) / float64(n)
	if relErr > 0.05 {
		t.Fatalf("relErr = %.3f, want <= 0.05 (got est=%.0f, want %d)", relErr, est, n)
	}
}

func TestDuplicateObservationsNoOp(t *testing.T) {
	s := hll.New(12)
	for i := 0; i < 10_000; i++ {
		s.Observe([]byte("repeated-value"))
	}
	if est := s.Estimate(); est > 5 {
		t.Fatalf("est = %.2f, want ≈ 1", est)
	}
}

func TestMergeSymmetric(t *testing.T) {
	a, b := hll.New(12), hll.New(12)
	for i := 0; i < 50_000; i++ {
		a.Observe(fmt.Appendf(nil, "a-%d", i))
		b.Observe(fmt.Appendf(nil, "b-%d", i))
	}
	merged := hll.New(12)
	_ = merged.Merge(a)
	_ = merged.Merge(b)
	est := merged.Estimate()
	if est < 90_000 || est > 110_000 {
		t.Fatalf("merge estimate = %.0f, want ≈ 100k ±10%%", est)
	}
}

func TestSerializeRoundTrip(t *testing.T) {
	s := hll.New(10)
	for i := 0; i < 1000; i++ {
		s.Observe(fmt.Appendf(nil, "v-%d", i))
	}
	g, err := hll.Deserialize(s.Serialize())
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if math.Abs(s.Estimate()-g.Estimate()) > 1 {
		t.Fatalf("estimates diverged: %.2f vs %.2f", s.Estimate(), g.Estimate())
	}
}

func TestPluginObserveAndEstimate(t *testing.T) {
	p := hll.NewPlugin()
	for i := 0; i < 10_000; i++ {
		_ = p.Observe("users", model.AttributeValue{T: model.AttrS, S: fmt.Sprintf("u-%d", i)})
	}
	est, err := p.Estimate("users")
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if est < 9_000 || est > 11_000 {
		t.Fatalf("est = %.0f, want ≈ 10000 ±10%%", est)
	}
}

func TestMergePrecisionMismatchErrors(t *testing.T) {
	a, b := hll.New(12), hll.New(14)
	if err := a.Merge(b); err == nil {
		t.Fatal("expected precision-mismatch error")
	}
}
