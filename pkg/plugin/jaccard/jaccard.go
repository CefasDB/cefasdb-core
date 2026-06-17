// Package jaccard is the Jaccard-distance operator plugin. Set
// inputs (SS) compare directly; string inputs (S) are shingled into
// trigrams before comparing. Returns 1 - Jaccard so smaller means
// closer (consistent with the other distance plugins).
package jaccard

import (
	"fmt"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "jaccard",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Jaccard distance (1 - |A∩B|/|A∪B|) over SS or shingled S",
	}
}

func (Op) Name() string { return "jaccard" }

func (Op) Supports(a, b model.AttrType) bool {
	if a == model.AttrSS && b == model.AttrSS {
		return true
	}
	if a == model.AttrS && b == model.AttrS {
		return true
	}
	return false
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	switch {
	case a.T == model.AttrSS && b.T == model.AttrSS:
		return distanceSets(a.SS, b.SS), nil
	case a.T == model.AttrS && b.T == model.AttrS:
		return distanceSets(trigramShingles(a.S), trigramShingles(b.S)), nil
	}
	return 0, fmt.Errorf("jaccard: unsupported pair (%v, %v)", a.T, b.T)
}

func distanceSets(a, b []string) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 0
	}
	setA := make(map[string]struct{}, len(a))
	for _, s := range a {
		setA[s] = struct{}{}
	}
	intersect := 0
	setB := make(map[string]struct{}, len(b))
	for _, s := range b {
		if _, dup := setB[s]; dup {
			continue
		}
		setB[s] = struct{}{}
		if _, in := setA[s]; in {
			intersect++
		}
	}
	union := len(setA) + len(setB) - intersect
	if union == 0 {
		return 0
	}
	return 1 - float64(intersect)/float64(union)
}

// trigramShingles produces all overlapping 3-rune shingles in s.
// Inputs shorter than 3 runes hash as a single shingle of the whole
// string so very short names still produce a non-empty set.
func trigramShingles(s string) []string {
	r := []rune(s)
	if len(r) < 3 {
		return []string{string(r)}
	}
	out := make([]string, 0, len(r)-2)
	for i := 0; i <= len(r)-3; i++ {
		out = append(out, string(r[i:i+3]))
	}
	return out
}

func init() { plugin.Default.MustRegister(Op{}) }
