package query_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/core/query"
)

func sampleTree() query.PlanNode {
	return query.PlanNode{
		Op: "TopK", Cost: 25, Detail: "k=20",
		Children: []query.PlanNode{
			{
				Op: "PostFilter", Plugin: "cosine", Cost: 20,
				Children: []query.PlanNode{
					{Op: "CandidateSet", Plugin: "vectorlsh", Cost: 5, Detail: "L=8 buckets"},
				},
			},
		},
	}
}

func TestExplainTextRenders(t *testing.T) {
	got := query.RenderExplain(sampleTree(), query.ExplainText)
	for _, want := range []string{
		"- TopK",
		"[plugin=cosine]",
		"[plugin=vectorlsh]",
		"cost=5",
		"k=20",
		"L=8 buckets",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("text output missing %q\n%s", want, got)
		}
	}
}

func TestExplainJSONRoundTrips(t *testing.T) {
	got := query.RenderExplain(sampleTree(), query.ExplainJSON)
	var back query.PlanNode
	if err := json.Unmarshal([]byte(got), &back); err != nil {
		t.Fatalf("json unmarshal: %v\n%s", err, got)
	}
	if back.Op != "TopK" || back.Cost != 25 || len(back.Children) != 1 {
		t.Fatalf("round-trip lost data: %+v", back)
	}
	leaf := back.Children[0].Children[0]
	if leaf.Op != "CandidateSet" || leaf.Plugin != "vectorlsh" {
		t.Fatalf("leaf wrong: %+v", leaf)
	}
}
