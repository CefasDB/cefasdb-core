package query_test

import (
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/core/query"
)

func TestChooseStrategyHighSelectivityWithIndexPicksFilterFirst(t *testing.T) {
	got := query.ChooseStrategy(0.05, true)
	if got != query.StrategyFilterFirst {
		t.Fatalf("got %v, want filter-first", got)
	}
}

func TestChooseStrategyLooseSelectivityPicksOverscan(t *testing.T) {
	got := query.ChooseStrategy(0.50, true)
	if got != query.StrategyANNFirstOverscan {
		t.Fatalf("got %v, want overscan", got)
	}
}

func TestChooseStrategyNoIndexFallsBackToOverscan(t *testing.T) {
	// Even a tight predicate falls back when no bitmap is available.
	got := query.ChooseStrategy(0.01, false)
	if got != query.StrategyANNFirstOverscan {
		t.Fatalf("got %v, want overscan when no index available", got)
	}
}

func TestOverscanFactorRoundsUpAndCaps(t *testing.T) {
	if f := query.OverscanFactor(1.0); f != 1 {
		t.Fatalf("selectivity=1.0 should not overscan, got %d", f)
	}
	if f := query.OverscanFactor(0.5); f != 2 {
		t.Fatalf("selectivity=0.5 should overscan x2, got %d", f)
	}
	if f := query.OverscanFactor(0.001); f != query.MaxOverscanFactor {
		t.Fatalf("selectivity~0 should cap at %d, got %d", query.MaxOverscanFactor, f)
	}
}

func TestApplyPredicateReportsSelectivity(t *testing.T) {
	eng, err := query.NewTopK(absDiff{}, "score", num("0"), 3)
	if err != nil {
		t.Fatalf("topk: %v", err)
	}
	pred := query.PredicateFunc(func(it model.Item) (bool, error) {
		v := it["region"]
		return v.S == "us", nil
	})
	candidates := []model.Item{
		{"score": num("1"), "region": model.AttributeValue{T: model.AttrS, S: "us"}},
		{"score": num("2"), "region": model.AttributeValue{T: model.AttrS, S: "eu"}},
		{"score": num("3"), "region": model.AttributeValue{T: model.AttrS, S: "us"}},
		{"score": num("4"), "region": model.AttributeValue{T: model.AttrS, S: "eu"}},
	}
	sel, err := query.ApplyPredicate(eng, pred, candidates)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if sel.KeptRows != 2 || sel.CandidateRows != 4 {
		t.Fatalf("counts wrong: %+v", sel)
	}
	if sel.Actual != 0.5 {
		t.Fatalf("actual selectivity = %v, want 0.5", sel.Actual)
	}
	results := eng.Result()
	if len(results) != 2 {
		t.Fatalf("topk got %d results, want 2", len(results))
	}
}

func TestANNFilterPlanExplainNodeIncludesStrategyAndIndex(t *testing.T) {
	plan := query.ANNFilterPlan{
		Strategy:             query.StrategyFilterFirst,
		IndexUsed:            "by_region",
		IndexedColumn:        "region",
		OverscanFactor:       1,
		Selectivity:          query.Selectivity{Predicted: 0.05},
		PredicateDescription: "region = 'us'",
	}
	node := plan.ExplainNode()
	if node.Op != "ANNFilter" {
		t.Fatalf("op = %q, want ANNFilter", node.Op)
	}
	for _, want := range []string{"filter-first", "by_region", "region", "predicted=0.050", "region = 'us'"} {
		if !strings.Contains(node.Detail, want) {
			t.Errorf("detail missing %q: %s", want, node.Detail)
		}
	}
}

func TestANNFilterPlanExplainNodeIncludesOverscan(t *testing.T) {
	plan := query.ANNFilterPlan{
		Strategy:       query.StrategyANNFirstOverscan,
		OverscanFactor: 4,
		Selectivity:    query.Selectivity{Predicted: 0.25, Actual: 0.30, CandidateRows: 40, KeptRows: 12},
	}
	detail := plan.ExplainNode().Detail
	if !strings.Contains(detail, "overscan=4") {
		t.Errorf("missing overscan: %s", detail)
	}
	if !strings.Contains(detail, "actual=0.300") || !strings.Contains(detail, "kept=12/40") {
		t.Errorf("missing actual selectivity: %s", detail)
	}
}
