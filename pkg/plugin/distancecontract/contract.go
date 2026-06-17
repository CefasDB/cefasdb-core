// Package distancecontract exposes a shared contract test suite for
// every implementation of query.DistanceOp (cosine, euclidean,
// hamming, jaccard, levenshtein, manhattan, jarowinkler, damerau,
// haversine, …).
//
// The playbook §2 LSP rule requires that every interface with ≥ 2
// implementations be covered by a single contract test that each
// implementation must pass. distancecontract.Run is that contract:
// callers supply the operator under test and a handful of valid
// (supported) input pairs, and the helper asserts the invariants
// every distance operator promises (determinism, symmetry, identity,
// non-negativity).
//
// A new distance plugin is wired in by adding a one-liner test that
// calls distancecontract.Run with the operator's typical inputs — the
// shared suite then guarantees the new plugin behaves like every
// other distance operator without each plugin re-inventing the
// assertion shape.
package distancecontract

import (
	"math"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/core/query"
)

// Case is one (a, b) input pair the contract uses to exercise the
// operator. The pair must satisfy Op.Supports(a.T, b.T); otherwise
// the test fails with a descriptive message instead of silently
// skipping the case.
type Case struct {
	Name string
	A, B model.AttributeValue
}

// Spec parametrises one contract run.
type Spec struct {
	// Op is the implementation under test.
	Op query.DistanceOp
	// ExpectedName is the canonical operator name the implementation
	// must self-report via Op.Name().
	ExpectedName string
	// Cases lists at least one valid input pair. Implementations
	// with multiple supported attribute kinds (e.g. cosine accepts
	// AttrVec, AttrL, AttrNS) should supply one case per kind.
	Cases []Case
	// Epsilon is the absolute tolerance for the symmetry and
	// identity equality checks. Defaults to 1e-9 when zero.
	Epsilon float64
}

// Run executes every contract subtest for the operator described by
// spec. Each invariant runs as a parallel t.Run so a failure points
// at exactly the case + invariant that broke.
func Run(t *testing.T, spec Spec) {
	t.Helper()
	if spec.Op == nil {
		t.Fatalf("distancecontract: spec.Op is nil")
	}
	if spec.ExpectedName == "" {
		t.Fatalf("distancecontract: spec.ExpectedName is empty")
	}
	if len(spec.Cases) == 0 {
		t.Fatalf("distancecontract: spec.Cases is empty — every contract needs at least one valid input pair")
	}
	eps := spec.Epsilon
	if eps == 0 {
		eps = 1e-9
	}

	t.Run("name", func(t *testing.T) {
		t.Parallel()
		if got := spec.Op.Name(); got != spec.ExpectedName {
			t.Fatalf("Name() = %q, want %q", got, spec.ExpectedName)
		}
	})

	for _, tc := range spec.Cases {
		t.Run("supports/"+tc.Name, func(t *testing.T) {
			t.Parallel()
			if !spec.Op.Supports(tc.A.T, tc.B.T) {
				t.Fatalf("Supports(%v, %v) = false; case %q is not a valid input pair for %s", tc.A.T, tc.B.T, tc.Name, spec.ExpectedName)
			}
		})

		t.Run("deterministic/"+tc.Name, func(t *testing.T) {
			t.Parallel()
			first, err := spec.Op.Eval(tc.A, tc.B)
			if err != nil {
				t.Fatalf("first Eval: %v", err)
			}
			second, err := spec.Op.Eval(tc.A, tc.B)
			if err != nil {
				t.Fatalf("second Eval: %v", err)
			}
			if first != second {
				t.Fatalf("Eval not deterministic: %v vs %v", first, second)
			}
			if math.IsNaN(first) {
				t.Fatalf("Eval produced NaN for a supported input pair")
			}
		})

		t.Run("nonnegative/"+tc.Name, func(t *testing.T) {
			t.Parallel()
			got, err := spec.Op.Eval(tc.A, tc.B)
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}
			if got < 0 {
				t.Fatalf("distance %v is negative", got)
			}
		})

		t.Run("symmetric/"+tc.Name, func(t *testing.T) {
			t.Parallel()
			ab, err := spec.Op.Eval(tc.A, tc.B)
			if err != nil {
				t.Fatalf("Eval(a,b): %v", err)
			}
			ba, err := spec.Op.Eval(tc.B, tc.A)
			if err != nil {
				t.Fatalf("Eval(b,a): %v", err)
			}
			if math.Abs(ab-ba) > eps {
				t.Fatalf("not symmetric: Eval(a,b)=%v Eval(b,a)=%v (diff %v > eps %v)", ab, ba, math.Abs(ab-ba), eps)
			}
		})

		t.Run("identity/"+tc.Name, func(t *testing.T) {
			t.Parallel()
			d, err := spec.Op.Eval(tc.A, tc.A)
			if err != nil {
				t.Fatalf("Eval(a,a): %v", err)
			}
			if math.Abs(d) > eps {
				t.Fatalf("Eval(a,a) = %v, want 0 (within eps %v)", d, eps)
			}
		})
	}
}
