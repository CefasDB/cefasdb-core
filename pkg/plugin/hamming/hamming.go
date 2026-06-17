// Package hamming is the Hamming-distance operator plugin. Counts
// position-wise differences between equal-length strings or byte
// slices.
//
// Supported kinds: S vs S (Unicode code points compared 1-to-1) and
// B vs B (raw bytes compared 1-to-1). Lengths must match.
package hamming

import (
	"fmt"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

// Op satisfies plugin.DistancePlugin.
type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "hamming",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Hamming distance over equal-length S or B values",
	}
}

func (Op) Name() string { return "hamming" }

func (Op) Supports(a, b model.AttrType) bool {
	return (a == model.AttrS && b == model.AttrS) || (a == model.AttrB && b == model.AttrB)
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	if a.T != b.T {
		return 0, fmt.Errorf("hamming: type mismatch (%v vs %v)", a.T, b.T)
	}
	switch a.T {
	case model.AttrS:
		ar, br := []rune(a.S), []rune(b.S)
		if len(ar) != len(br) {
			return 0, fmt.Errorf("hamming: length mismatch (%d vs %d)", len(ar), len(br))
		}
		n := 0
		for i := range ar {
			if ar[i] != br[i] {
				n++
			}
		}
		return float64(n), nil
	case model.AttrB:
		if len(a.B) != len(b.B) {
			return 0, fmt.Errorf("hamming: length mismatch (%d vs %d)", len(a.B), len(b.B))
		}
		n := 0
		for i := range a.B {
			if a.B[i] != b.B[i] {
				n++
			}
		}
		return float64(n), nil
	}
	return 0, fmt.Errorf("hamming: unsupported kind %v", a.T)
}

func init() { plugin.Default.MustRegister(Op{}) }
