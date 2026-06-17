package cms_test

import (
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin/cms"
)

func TestFrequencyIsAtLeastTrue(t *testing.T) {
	s, err := cms.New(0.001, 0.001)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for i := 0; i < 5000; i++ {
		s.Observe([]byte("hot"))
	}
	for i := 0; i < 50; i++ {
		s.Observe([]byte("warm"))
	}
	if got := s.Frequency([]byte("hot")); got < 5000 {
		t.Fatalf("hot freq = %d, want >= 5000", got)
	}
	if got := s.Frequency([]byte("warm")); got < 50 {
		t.Fatalf("warm freq = %d, want >= 50", got)
	}
	if got := s.Frequency([]byte("cold")); got > 5 {
		t.Fatalf("cold freq = %d, want ~0", got)
	}
}

func TestMergeSumsCounters(t *testing.T) {
	a, _ := cms.New(0.01, 0.01)
	b, _ := cms.New(0.01, 0.01)
	for i := 0; i < 100; i++ {
		a.Observe([]byte("k"))
		b.Observe([]byte("k"))
	}
	if err := a.Merge(b); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got := a.Frequency([]byte("k")); got < 200 {
		t.Fatalf("merged freq = %d, want >= 200", got)
	}
}

func TestSerializeRoundTrip(t *testing.T) {
	s, _ := cms.NewSized(4, 256)
	for i := 0; i < 100; i++ {
		s.Observe(fmt.Appendf(nil, "v-%d", i%10))
	}
	g, err := cms.Deserialize(s.Serialize())
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	for i := 0; i < 10; i++ {
		want := s.Frequency(fmt.Appendf(nil, "v-%d", i))
		got := g.Frequency(fmt.Appendf(nil, "v-%d", i))
		if want != got {
			t.Fatalf("freq[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestPluginFrequencyAndEstimate(t *testing.T) {
	p := cms.NewPlugin()
	for i := 0; i < 1000; i++ {
		_ = p.Observe("clicks", model.AttributeValue{T: model.AttrS, S: "campaign-1"})
	}
	if f := p.Frequency("clicks", model.AttributeValue{T: model.AttrS, S: "campaign-1"}); f < 1000 {
		t.Fatalf("freq = %d, want >= 1000", f)
	}
	if est, _ := p.Estimate("clicks"); est < 1000 {
		t.Fatalf("est = %.0f, want >= 1000", est)
	}
}

func TestEpsilonDeltaValidation(t *testing.T) {
	if _, err := cms.New(0, 0.01); err == nil {
		t.Fatal("expected epsilon error")
	}
	if _, err := cms.New(0.01, 1.5); err == nil {
		t.Fatal("expected delta error")
	}
}
