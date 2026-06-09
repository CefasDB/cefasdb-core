package storage

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestConditionEvaluate(t *testing.T) {
	item := types.Item{
		"name":  {T: types.AttrS, S: "alice"},
		"score": {T: types.AttrN, N: "42"},
		"ok":    {T: types.AttrBOOL, BOOL: true},
	}
	binds := map[string]types.AttributeValue{
		"alice":  {T: types.AttrS, S: "alice"},
		"bob":    {T: types.AttrS, S: "bob"},
		"forty":  {T: types.AttrN, N: "40"},
		"fifty":  {T: types.AttrN, N: "50"},
		"forty2": {T: types.AttrN, N: "42"},
	}

	cases := []struct {
		expr string
		want bool
	}{
		{"attribute_exists(name)", true},
		{"attribute_not_exists(missing)", true},
		{"attribute_exists(missing)", false},
		{"name = :alice", true},
		{"name = :bob", false},
		{"name <> :bob", true},
		{"score = :forty2", true},
		{"score > :forty", true},
		{"score < :forty", false},
		{"score >= :forty2", true},
		{"score BETWEEN :forty AND :fifty", true},
		{"score BETWEEN :fifty AND :forty", false},
		{"attribute_exists(name) AND score > :forty", true},
		{"attribute_exists(missing) OR score = :forty2", true},
		{"NOT attribute_exists(name)", false},
		{"(name = :alice OR name = :bob) AND attribute_exists(score)", true},
	}

	for _, c := range cases {
		cond, err := ParseCondition(c.expr)
		if err != nil {
			t.Fatalf("ParseCondition(%q): %v", c.expr, err)
		}
		got, err := cond.Evaluate(item, binds)
		if err != nil {
			t.Fatalf("Evaluate(%q): %v", c.expr, err)
		}
		if got != c.want {
			t.Fatalf("Evaluate(%q) = %v, want %v", c.expr, got, c.want)
		}
	}
}

func TestConditionNilItemAttributeNotExistsTrue(t *testing.T) {
	cond, err := ParseCondition("attribute_not_exists(id)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := cond.Evaluate(nil, nil)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if !got {
		t.Fatalf("attribute_not_exists on nil item should be true")
	}
}

func TestConditionParseErrors(t *testing.T) {
	bad := []string{
		"name =",                    // missing rhs
		"= :v",                      // missing lhs
		"name BETWEEN :a OR :b",     // wrong keyword
		"attribute_exists)",         // missing (
		"attribute_exists(name",     // missing )
		"name @ :v",                 // unknown char
		"name AND",                  // dangling op
	}
	for _, b := range bad {
		if _, err := ParseCondition(b); err == nil {
			t.Errorf("ParseCondition(%q) returned nil error", b)
		}
	}
}
