package query_test

import (
	"math"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/core/query"
)

// absDiff is a tiny stand-in distance: |a.N - b.N| as float64. Lets
// us exercise the TopK engine without a real distance plugin.
type absDiff struct{}

func (absDiff) Name() string                      { return "absdiff" }
func (absDiff) Supports(a, b model.AttrType) bool { return a == model.AttrN && b == model.AttrN }
func (absDiff) Eval(a, b model.AttributeValue) (float64, error) {
	var av, bv float64
	if _, err := fmtSscanf(a.N, &av); err != nil {
		return 0, err
	}
	if _, err := fmtSscanf(b.N, &bv); err != nil {
		return 0, err
	}
	return math.Abs(av - bv), nil
}

// avoid a fmt import dance by inlining a tiny parser.
func fmtSscanf(s string, out *float64) (int, error) {
	var sign float64 = 1
	i := 0
	if i < len(s) && s[i] == '-' {
		sign = -1
		i++
	}
	var v float64
	var frac float64 = 1
	dot := false
	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			dot = true
			continue
		}
		if c < '0' || c > '9' {
			break
		}
		d := float64(c - '0')
		if dot {
			frac /= 10
			v += d * frac
		} else {
			v = v*10 + d
		}
	}
	*out = sign * v
	return i, nil
}

func num(s string) model.AttributeValue { return model.AttributeValue{T: model.AttrN, N: s} }

func TestTopKKeepsKSmallest(t *testing.T) {
	eng, err := query.NewTopK(absDiff{}, "score", num("10"), 3)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for _, s := range []string{"1", "5", "9", "10", "12", "20", "100"} {
		if err := eng.Observe(model.Item{"score": num(s)}); err != nil {
			t.Fatalf("observe %s: %v", s, err)
		}
	}
	got := eng.Result()
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	want := []float64{0, 1, 2}
	for i, w := range want {
		if got[i].Distance != w {
			t.Fatalf("[%d] distance = %g, want %g (item=%v)", i, got[i].Distance, w, got[i].Item)
		}
	}
}

func TestTopKSkipsMissingAttribute(t *testing.T) {
	eng, _ := query.NewTopK(absDiff{}, "score", num("0"), 2)
	_ = eng.Observe(model.Item{"other": num("1")})
	_ = eng.Observe(model.Item{"score": num("2")})
	got := eng.Result()
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1 (missing-attr should be skipped)", len(got))
	}
}

func TestTopKRejectsBadConfig(t *testing.T) {
	if _, err := query.NewTopK(nil, "x", num("0"), 1); err == nil {
		t.Fatal("expected nil-op error")
	}
	if _, err := query.NewTopK(absDiff{}, "", num("0"), 1); err == nil {
		t.Fatal("expected empty-attr error")
	}
	if _, err := query.NewTopK(absDiff{}, "x", num("0"), 0); err == nil {
		t.Fatal("expected k=0 error")
	}
}

func TestTopKResultIsSortedAscending(t *testing.T) {
	eng, _ := query.NewTopK(absDiff{}, "v", num("0"), 4)
	for _, s := range []string{"7", "2", "5", "3", "1"} {
		_ = eng.Observe(model.Item{"v": num(s)})
	}
	got := eng.Result()
	last := -1.0
	for _, r := range got {
		if r.Distance < last {
			t.Fatalf("not ascending: %v then %v", last, r.Distance)
		}
		last = r.Distance
	}
}
