package query

import (
	"fmt"

	"github.com/osvaldoandrade/cefas/internal/core/model"
)

// ANNFilterStrategy is the rule-based execution mode the planner
// picks for a hybrid ANN + WHERE query.
//
// Selectivity is the fraction of rows in the table the predicate is
// estimated to retain (1.0 = keep everything, 0.0 = keep nothing).
// The planner chooses:
//
//   - StrategyFilterFirst when the predicate is highly selective —
//     intersect the candidate set with an indexed-attribute bitmap
//     before re-ranking. Avoids ranking rows that will be filtered
//     out.
//   - StrategyANNFirstOverscan when the predicate is loose — pull
//     `k * overscan_factor` candidates from ANN and post-filter. The
//     overscan factor is auto-tuned from selectivity so we still
//     return k rows in expectation.
type ANNFilterStrategy uint8

const (
	// StrategyUnset means the planner has not picked a strategy yet
	// (e.g. there is no WHERE clause). The executor treats it as the
	// plain ANN scan.
	StrategyUnset ANNFilterStrategy = iota
	// StrategyFilterFirst restricts candidates before ranking.
	StrategyFilterFirst
	// StrategyANNFirstOverscan ranks first, post-filters after.
	StrategyANNFirstOverscan
)

// String returns the canonical EXPLAIN label for the strategy.
func (s ANNFilterStrategy) String() string {
	switch s {
	case StrategyFilterFirst:
		return "filter-first"
	case StrategyANNFirstOverscan:
		return "ann-first-overscan"
	default:
		return "ann-only"
	}
}

// FilterFirstSelectivityThreshold is the rule-based cutoff: when the
// estimated selectivity is at or below this value, the planner picks
// StrategyFilterFirst; otherwise StrategyANNFirstOverscan.
//
// 0.20 matches the rule we tested against the recommendation workload:
// once more than ~20 % of rows are eligible, the bitmap-intersection
// cost dominates and overscan wins.
const FilterFirstSelectivityThreshold = 0.20

// MaxOverscanFactor caps the overscan multiplier so a near-zero
// selectivity does not produce an unbounded scan. The planner falls
// back to filter-first when this cap would be exceeded.
const MaxOverscanFactor = 16

// ChooseStrategy picks the execution strategy from the estimated
// selectivity. `indexedColumn` is true when the WHERE predicate
// touches an attribute the storage layer can intersect with a
// roaring-bitmap cohort; without that index we always fall back to
// overscan, since filter-first has nothing to intersect against.
func ChooseStrategy(selectivity float64, indexedColumn bool) ANNFilterStrategy {
	if selectivity <= 0 {
		// Degenerate: estimator was certain nothing matches. Fall
		// through as filter-first only when the index can short-circuit
		// the scan; otherwise overscan (the executor will simply return
		// fewer than k rows with a warning).
		if indexedColumn {
			return StrategyFilterFirst
		}
		return StrategyANNFirstOverscan
	}
	if indexedColumn && selectivity <= FilterFirstSelectivityThreshold {
		return StrategyFilterFirst
	}
	return StrategyANNFirstOverscan
}

// OverscanFactor returns the multiplier the executor should apply to
// k when pulling candidates from ANN, given the predicate's
// selectivity. With selectivity s we expect k/s ANN candidates to
// satisfy the predicate, so the factor is 1/s — capped at
// MaxOverscanFactor so the executor never balloons the scan.
func OverscanFactor(selectivity float64) int {
	if selectivity >= 1 {
		return 1
	}
	if selectivity <= 0 {
		return MaxOverscanFactor
	}
	f := int(1.0/selectivity + 0.999) // round up
	if f < 1 {
		f = 1
	}
	if f > MaxOverscanFactor {
		f = MaxOverscanFactor
	}
	return f
}

// Predicate is the eligibility test the planner pushes into the ANN
// scan. The executor evaluates Eligible against each candidate row;
// implementations are expected to be cheap (no I/O, no allocations
// per call) since they sit on the hot path.
type Predicate interface {
	// Eligible reports whether the row should be admitted into the
	// TopK heap. Returning an error stops the scan.
	Eligible(item model.Item) (bool, error)
}

// PredicateFunc adapts a function literal to Predicate.
type PredicateFunc func(model.Item) (bool, error)

// Eligible runs the wrapped function.
func (f PredicateFunc) Eligible(it model.Item) (bool, error) { return f(it) }

// Selectivity records the planner's prediction and the executor's
// observed value, so EXPLAIN can surface drift. Both are fractions of
// the rows the predicate kept.
type Selectivity struct {
	// Predicted is the value the planner used to pick the strategy.
	Predicted float64
	// Actual is set by the executor after the scan completes. Zero
	// when the executor has not run yet.
	Actual float64
	// CandidateRows is how many ANN candidates the executor produced.
	CandidateRows int
	// KeptRows is how many rows survived the predicate.
	KeptRows int
}

// String renders Selectivity for the EXPLAIN detail column.
func (s Selectivity) String() string {
	if s.CandidateRows == 0 {
		return fmt.Sprintf("predicted=%.3f", s.Predicted)
	}
	return fmt.Sprintf("predicted=%.3f actual=%.3f kept=%d/%d",
		s.Predicted, s.Actual, s.KeptRows, s.CandidateRows)
}

// ANNFilterPlan is the planner-side description of a hybrid
// ANN + WHERE node. The SQL planner builds one of these per
// `ORDER BY <vec> ANN OF ... WHERE <predicate>` query and the
// executor reads it to drive the scan.
//
// The struct is intentionally a plain value type so it can travel
// through the SQL `Plan` interface without dragging in a heavy
// hierarchy. The executor attaches the runtime Predicate and updates
// `Selectivity.Actual` after the scan.
type ANNFilterPlan struct {
	// Strategy is the rule chosen by the planner.
	Strategy ANNFilterStrategy
	// IndexUsed names the index whose roaring bitmap backs the
	// filter-first cohort intersection, or "" when no index was
	// available (overscan fallback).
	IndexUsed string
	// IndexedColumn is the attribute the predicate restricts. Empty
	// when the predicate touches no indexed column.
	IndexedColumn string
	// OverscanFactor is the multiplier applied to k for the
	// ANN-first strategy. 1 for filter-first.
	OverscanFactor int
	// Selectivity carries predicted and (after execution) actual
	// fractions for EXPLAIN.
	Selectivity Selectivity
	// PredicateDescription is the human-readable form of the WHERE
	// clause, surfaced in EXPLAIN.
	PredicateDescription string
	// Warning is set by the executor when the strategy returned
	// fewer than k rows (typically because overscan still couldn't
	// satisfy the predicate). Empty otherwise.
	Warning string
}

// ExplainNode returns the PlanNode the SQL planner stitches under its
// own ANN node so explain output reports the hybrid strategy.
func (p ANNFilterPlan) ExplainNode() PlanNode {
	detail := fmt.Sprintf("strategy=%s", p.Strategy)
	if p.IndexUsed != "" {
		detail += " index=" + p.IndexUsed
	}
	if p.IndexedColumn != "" {
		detail += " column=" + p.IndexedColumn
	}
	if p.OverscanFactor > 1 {
		detail += fmt.Sprintf(" overscan=%d", p.OverscanFactor)
	}
	detail += " selectivity={" + p.Selectivity.String() + "}"
	if p.PredicateDescription != "" {
		detail += " predicate=" + p.PredicateDescription
	}
	if p.Warning != "" {
		detail += " warning=" + p.Warning
	}
	return PlanNode{Op: "ANNFilter", Detail: detail}
}

// ApplyPredicate streams candidates through the predicate into the
// TopK engine and records the observed selectivity. Both strategies
// (filter-first and ANN-first-with-overscan) collapse to the same
// inner loop once the candidate set is in hand — what differs is how
// the caller assembled `candidates`. Filter-first hands in a
// cohort-intersected slice; ANN-first-with-overscan hands in the
// pre-ranked top k*overscan.
func ApplyPredicate(eng *TopKEngine, pred Predicate, candidates []model.Item) (*Selectivity, error) {
	sel := &Selectivity{CandidateRows: len(candidates)}
	for _, it := range candidates {
		ok, err := pred.Eligible(it)
		if err != nil {
			return sel, err
		}
		if !ok {
			continue
		}
		sel.KeptRows++
		if err := eng.Observe(it); err != nil {
			return sel, err
		}
	}
	if sel.CandidateRows > 0 {
		sel.Actual = float64(sel.KeptRows) / float64(sel.CandidateRows)
	}
	return sel, nil
}

// FewerThanKWarning is the documented warning string the executor
// surfaces when the overscan strategy could not satisfy the LIMIT.
const FewerThanKWarning = "fewer than k available"
