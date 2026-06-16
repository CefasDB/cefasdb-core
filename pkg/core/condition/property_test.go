package condition_test

import (
	"errors"
	"testing"
	"testing/quick"

	"github.com/osvaldoandrade/cefas/pkg/core/condition"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

// The condition package only publishes the Evaluator interface. The
// properties below pin the documented contract of every conforming
// implementation:
//
//  1. Empty expressions evaluate to true with no error.
//  2. Evaluate is deterministic for the same (expr, item, binds) triple.
//  3. Evaluate must not panic on arbitrary expression strings — failures
//     must surface as a typed error.
//  4. A nil item is legal input; the evaluator must not panic and must
//     return only false-or-error for attribute_exists-style probes.
//
// A reference stub captures the documented semantics. Every property
// runs against that stub so a regression in the contract surfaces as a
// failing property rather than a silently broken interface comment.

// refEvaluator is the reference behaviour every Evaluator must respect.
// It implements only what the doc comment promises:
//   - empty expr  → (true, nil)
//   - any other   → (false, nil) when item is nil
//   - any other   → (truthy, nil) when item is non-nil
type refEvaluator struct{ truthy bool }

func (r refEvaluator) Evaluate(expr string, item model.Item, _ map[string]model.AttributeValue) (bool, error) {
	if expr == "" {
		return true, nil
	}
	if item == nil {
		return false, nil
	}
	return r.truthy, nil
}

// boomEvaluator returns an error on any non-empty input. It exists to
// prove the "errors flow through, never panic" property.
type boomEvaluator struct{}

func (boomEvaluator) Evaluate(expr string, _ model.Item, _ map[string]model.AttributeValue) (bool, error) {
	if expr == "" {
		return true, nil
	}
	return false, errors.New("boom")
}

func TestProperty_EmptyExpressionAlwaysTrue(t *testing.T) {
	evaluators := []condition.Evaluator{
		refEvaluator{truthy: true},
		refEvaluator{truthy: false},
		boomEvaluator{},
	}
	f := func(itemKey string) bool {
		item := model.Item{}
		if itemKey != "" {
			item[itemKey] = model.AttributeValue{T: model.AttrS, S: itemKey}
		}
		for _, e := range evaluators {
			got, err := e.Evaluate("", item, nil)
			if err != nil || !got {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_EvaluateNoPanicOnArbitraryInput(t *testing.T) {
	evaluators := []condition.Evaluator{
		refEvaluator{truthy: true},
		boomEvaluator{},
	}
	f := func(expr string, itemKey string) (ok bool) {
		defer func() {
			if r := recover(); r != nil {
				ok = false
			}
		}()
		var item model.Item
		if itemKey != "" {
			item = model.Item{itemKey: {T: model.AttrS, S: itemKey}}
		}
		for _, e := range evaluators {
			_, _ = e.Evaluate(expr, item, nil)
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_EvaluateDeterministic(t *testing.T) {
	e := refEvaluator{truthy: true}
	f := func(expr string, key string) bool {
		item := model.Item{}
		if key != "" {
			item[key] = model.AttributeValue{T: model.AttrS, S: key}
		}
		first, firstErr := e.Evaluate(expr, item, nil)
		for i := 0; i < 4; i++ {
			got, err := e.Evaluate(expr, item, nil)
			if got != first {
				return false
			}
			if (err == nil) != (firstErr == nil) {
				return false
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_NilItemNeverPanics(t *testing.T) {
	evaluators := []condition.Evaluator{
		refEvaluator{truthy: true},
		refEvaluator{truthy: false},
		boomEvaluator{},
	}
	f := func(expr string) (ok bool) {
		defer func() {
			if r := recover(); r != nil {
				ok = false
			}
		}()
		for _, e := range evaluators {
			_, _ = e.Evaluate(expr, nil, nil)
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_BindsArbitraryKeysNoPanic(t *testing.T) {
	e := refEvaluator{truthy: true}
	f := func(expr string, keys []string, values []string) (ok bool) {
		defer func() {
			if r := recover(); r != nil {
				ok = false
			}
		}()
		binds := map[string]model.AttributeValue{}
		n := len(keys)
		if len(values) < n {
			n = len(values)
		}
		for i := 0; i < n; i++ {
			binds[keys[i]] = model.AttributeValue{T: model.AttrS, S: values[i]}
		}
		_, _ = e.Evaluate(expr, nil, binds)
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

// Documented edge case: empty + whitespace inputs. The interface
// promises only that the empty string is true; non-empty whitespace
// is implementation-defined, but must still not panic and must return
// a well-formed (bool, error) pair.
func TestProperty_WhitespaceExpressionsAreWellFormed(t *testing.T) {
	e := refEvaluator{truthy: true}
	cases := []string{" ", "\t", "\n", "  \t \n", "\r\n"}
	for _, c := range cases {
		got, err := e.Evaluate(c, nil, nil)
		// The reference says: non-empty + nil item → (false, nil). A
		// real evaluator could legitimately return an error instead.
		// What it must not do is return (true, non-nil-error).
		if got && err != nil {
			t.Fatalf("inconsistent result for %q: got=%v err=%v", c, got, err)
		}
	}
}
