package query_test

import (
	"errors"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/core/query"
)

type fakeOp struct {
	name     string
	supports bool
	out      float64
	err      error
}

func (f *fakeOp) Name() string                      { return f.name }
func (f *fakeOp) Supports(a, b model.AttrType) bool { return f.supports }
func (f *fakeOp) Eval(a, b model.AttributeValue) (float64, error) {
	return f.out, f.err
}

func TestRegistryRegisterAndLookup(t *testing.T) {
	r := query.NewDistanceRegistry()
	op := &fakeOp{name: "cosine", supports: true}
	if err := r.Register(op); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, ok := r.Lookup("cosine")
	if !ok || got != op {
		t.Fatalf("lookup = %v / ok=%v, want fakeOp / true", got, ok)
	}
}

func TestRegistryRejectsDuplicates(t *testing.T) {
	r := query.NewDistanceRegistry()
	_ = r.Register(&fakeOp{name: "cosine"})
	if err := r.Register(&fakeOp{name: "cosine"}); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestRegistryRejectsEmptyAndNil(t *testing.T) {
	r := query.NewDistanceRegistry()
	if err := r.Register(nil); err == nil {
		t.Fatal("expected error on nil op")
	}
	if err := r.Register(&fakeOp{name: ""}); err == nil {
		t.Fatal("expected error on empty name")
	}
}

func TestRegistryListIsSorted(t *testing.T) {
	r := query.NewDistanceRegistry()
	for _, n := range []string{"cosine", "haversine", "euclidean"} {
		_ = r.Register(&fakeOp{name: n})
	}
	got := r.List()
	want := []string{"cosine", "euclidean", "haversine"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, n := range want {
		if got[i].Name() != n {
			t.Fatalf("[%d] = %q, want %q", i, got[i].Name(), n)
		}
	}
}

func TestRegistryLookupMissing(t *testing.T) {
	r := query.NewDistanceRegistry()
	if _, ok := r.Lookup("ghost"); ok {
		t.Fatal("ghost lookup returned ok=true")
	}
}

// ensures DistanceOp errors propagate cleanly through the engine
func TestDistanceOpErrorsPropagate(t *testing.T) {
	r := query.NewDistanceRegistry()
	want := errors.New("dim mismatch")
	op := &fakeOp{name: "x", err: want}
	_ = r.Register(op)
	got, _ := r.Lookup("x")
	if _, err := got.Eval(model.AttributeValue{}, model.AttributeValue{}); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}
