// Package levenshtein is the classic edit-distance operator plugin.
// Counts the minimum number of single-character insertions,
// deletions, or substitutions to turn one string into another.
//
// Supported kinds: S vs S (compared as []rune so multi-byte chars
// count once).
package levenshtein

import (
	"fmt"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "levenshtein",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Levenshtein edit distance over S values",
	}
}

func (Op) Name() string { return "levenshtein" }

func (Op) Supports(a, b model.AttrType) bool {
	return a == model.AttrS && b == model.AttrS
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	if a.T != model.AttrS || b.T != model.AttrS {
		return 0, fmt.Errorf("levenshtein: need S values, got (%v, %v)", a.T, b.T)
	}
	return float64(Distance(a.S, b.S)), nil
}

// Distance returns the Levenshtein distance between s and t. Exposed
// so other plugins (Damerau, fuzzy index post-filters) can reuse it.
func Distance(s, t string) int {
	sr, tr := []rune(s), []rune(t)
	n, m := len(sr), len(tr)
	if n == 0 {
		return m
	}
	if m == 0 {
		return n
	}
	// Two-row DP keeps memory at O(min(n,m)).
	if n > m {
		sr, tr = tr, sr
		n, m = m, n
	}
	prev := make([]int, n+1)
	curr := make([]int, n+1)
	for i := 0; i <= n; i++ {
		prev[i] = i
	}
	for j := 1; j <= m; j++ {
		curr[0] = j
		for i := 1; i <= n; i++ {
			cost := 1
			if sr[i-1] == tr[j-1] {
				cost = 0
			}
			ins := curr[i-1] + 1
			del := prev[i] + 1
			sub := prev[i-1] + cost
			curr[i] = min3(ins, del, sub)
		}
		prev, curr = curr, prev
	}
	return prev[n]
}

func min3(a, b, c int) int {
	if a < b {
		if a < c {
			return a
		}
		return c
	}
	if b < c {
		return b
	}
	return c
}

func init() { plugin.Default.MustRegister(Op{}) }
