// Package euclidean is the L2-distance operator plugin over numeric
// vectors.
package euclidean

import (
	"fmt"
	"math"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/internal/vecattr"
)

type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "euclidean",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Euclidean (L2) distance over numeric vectors (V, L of N, or NS)",
	}
}

func (Op) Name() string { return "euclidean" }

func (Op) Supports(a, b model.AttrType) bool {
	return numericVec(a) && numericVec(b)
}

func numericVec(t model.AttrType) bool {
	return t == model.AttrVec || t == model.AttrL || t == model.AttrNS
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	av, err := vecattr.AsFloats(a)
	if err != nil {
		return 0, fmt.Errorf("euclidean: %w", err)
	}
	bv, err := vecattr.AsFloats(b)
	if err != nil {
		return 0, fmt.Errorf("euclidean: %w", err)
	}
	if err := vecattr.Same(av, bv); err != nil {
		return 0, fmt.Errorf("euclidean: %w", err)
	}
	var s float64
	for i := range av {
		d := av[i] - bv[i]
		s += d * d
	}
	return math.Sqrt(s), nil
}

func init() { plugin.Default.MustRegister(Op{}) }
