// Package jarowinkler is the Jaro-Winkler similarity operator
// plugin. Returns a value in [0, 1] where 1 means identical — the
// natural shape for name / spelling matching workloads.
//
// To stay consistent with the rest of the distance-plugin family
// (smaller = closer), Eval returns 1 - similarity so the planner can
// compose with thresholds the same way as Levenshtein <= K.
package jarowinkler

import (
	"fmt"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "jaro_winkler",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Jaro-Winkler distance (1 - similarity) over S values",
	}
}

func (Op) Name() string { return "jaro_winkler" }

func (Op) Supports(a, b model.AttrType) bool {
	return a == model.AttrS && b == model.AttrS
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	if a.T != model.AttrS || b.T != model.AttrS {
		return 0, fmt.Errorf("jaro_winkler: need S values, got (%v, %v)", a.T, b.T)
	}
	return 1 - Similarity(a.S, b.S), nil
}

// Similarity returns the Jaro-Winkler similarity in [0, 1]; 1 means
// identical, 0 means no characters in common. Exposed so callers
// that prefer the higher-is-better convention can use it directly.
func Similarity(s, t string) float64 {
	if s == t {
		return 1
	}
	sr, tr := []rune(s), []rune(t)
	if len(sr) == 0 || len(tr) == 0 {
		return 0
	}
	matchWindow := max(len(sr), len(tr))/2 - 1
	if matchWindow < 0 {
		matchWindow = 0
	}
	sMatched := make([]bool, len(sr))
	tMatched := make([]bool, len(tr))
	matches := 0
	for i, sc := range sr {
		lo := i - matchWindow
		if lo < 0 {
			lo = 0
		}
		hi := i + matchWindow + 1
		if hi > len(tr) {
			hi = len(tr)
		}
		for j := lo; j < hi; j++ {
			if tMatched[j] || tr[j] != sc {
				continue
			}
			sMatched[i] = true
			tMatched[j] = true
			matches++
			break
		}
	}
	if matches == 0 {
		return 0
	}
	transpositions := 0
	k := 0
	for i := 0; i < len(sr); i++ {
		if !sMatched[i] {
			continue
		}
		for !tMatched[k] {
			k++
		}
		if sr[i] != tr[k] {
			transpositions++
		}
		k++
	}
	mf := float64(matches)
	jaro := (mf/float64(len(sr)) + mf/float64(len(tr)) + (mf-float64(transpositions)/2)/mf) / 3
	// Winkler prefix bonus: up to 4 matching leading chars, scale 0.1.
	prefix := 0
	for i := 0; i < min2(len(sr), len(tr)) && i < 4; i++ {
		if sr[i] != tr[i] {
			break
		}
		prefix++
	}
	return jaro + float64(prefix)*0.1*(1-jaro)
}

func min2(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() { plugin.Default.MustRegister(Op{}) }
