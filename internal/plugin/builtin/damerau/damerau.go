// Package damerau is the Damerau-Levenshtein operator plugin —
// Levenshtein plus a single-edit transposition of adjacent characters.
// Same input shape as Levenshtein; just one extra term in the DP
// recurrence.
package damerau

import (
	"fmt"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "damerau",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Damerau-Levenshtein edit distance (with adjacent transposition)",
	}
}

func (Op) Name() string { return "damerau" }

func (Op) Supports(a, b model.AttrType) bool {
	return a == model.AttrS && b == model.AttrS
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	if a.T != model.AttrS || b.T != model.AttrS {
		return 0, fmt.Errorf("damerau: need S values, got (%v, %v)", a.T, b.T)
	}
	return float64(Distance(a.S, b.S)), nil
}

// Distance returns the optimal-string-alignment Damerau distance —
// adjacent transposition counts as one edit but each pair can only be
// transposed once. Matches the common Wagner-Fischer + OSA variant.
func Distance(s, t string) int {
	sr, tr := []rune(s), []rune(t)
	n, m := len(sr), len(tr)
	if n == 0 {
		return m
	}
	if m == 0 {
		return n
	}
	d := make([][]int, n+1)
	for i := range d {
		d[i] = make([]int, m+1)
		d[i][0] = i
	}
	for j := 0; j <= m; j++ {
		d[0][j] = j
	}
	for i := 1; i <= n; i++ {
		for j := 1; j <= m; j++ {
			cost := 1
			if sr[i-1] == tr[j-1] {
				cost = 0
			}
			d[i][j] = min3(d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1]+cost)
			if i > 1 && j > 1 && sr[i-1] == tr[j-2] && sr[i-2] == tr[j-1] {
				if alt := d[i-2][j-2] + 1; alt < d[i][j] {
					d[i][j] = alt
				}
			}
		}
	}
	return d[n][m]
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
