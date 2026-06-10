package query

import (
	"encoding/json"
	"fmt"
	"strings"
)

// PlanNode is the shape every plan emits for explain rendering. Plans
// build a tree of PlanNodes then hand them to RenderExplain to get
// either a text outline or a JSON document — same data, two formats.
type PlanNode struct {
	Op       string     `json:"op"`
	Plugin   string     `json:"plugin,omitempty"`
	Cost     float64    `json:"cost,omitempty"`
	Detail   string     `json:"detail,omitempty"`
	Children []PlanNode `json:"children,omitempty"`
}

// RenderExplain turns a plan tree into its on-the-wire explain output.
// fmtKind selects ExplainText (indented outline) or ExplainJSON
// (machine-readable document with the same fields).
func RenderExplain(root PlanNode, fmtKind ExplainFormat) string {
	switch fmtKind {
	case ExplainJSON:
		b, _ := json.MarshalIndent(root, "", "  ")
		return string(b)
	default:
		var b strings.Builder
		renderText(&b, root, 0)
		return b.String()
	}
}

func renderText(b *strings.Builder, n PlanNode, depth int) {
	indent := strings.Repeat("  ", depth)
	fmt.Fprintf(b, "%s- %s", indent, n.Op)
	if n.Plugin != "" {
		fmt.Fprintf(b, " [plugin=%s]", n.Plugin)
	}
	if n.Cost > 0 {
		fmt.Fprintf(b, " cost=%.3g", n.Cost)
	}
	if n.Detail != "" {
		fmt.Fprintf(b, " %s", n.Detail)
	}
	b.WriteByte('\n')
	for _, c := range n.Children {
		renderText(b, c, depth+1)
	}
}
