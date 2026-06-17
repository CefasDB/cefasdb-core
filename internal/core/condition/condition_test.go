package condition_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/condition"
	"github.com/CefasDb/cefasdb/internal/core/model"
)

// A trivial in-memory evaluator proves the interface is satisfiable
// without dragging in any storage internals.
type stubEvaluator struct{ truthy bool }

func (s stubEvaluator) Evaluate(expr string, item model.Item, binds map[string]model.AttributeValue) (bool, error) {
	if expr == "" {
		return true, nil
	}
	return s.truthy, nil
}

func TestEvaluatorInterfaceSatisfied(t *testing.T) {
	var e condition.Evaluator = stubEvaluator{truthy: true}
	got, err := e.Evaluate("attribute_exists(id)", model.Item{"id": {T: model.AttrS, S: "x"}}, nil)
	if err != nil || !got {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestEvaluatorEmptyExprAlwaysTrue(t *testing.T) {
	var e condition.Evaluator = stubEvaluator{truthy: false}
	got, _ := e.Evaluate("", nil, nil)
	if !got {
		t.Fatal("empty expression must evaluate to true by contract")
	}
}
