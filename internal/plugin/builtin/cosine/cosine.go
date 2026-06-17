// Package cosine is the cosine-distance operator plugin. Inputs are
// numeric vectors (AttrVec, AttrL of AttrN, or AttrNS); Eval returns the
// distance 1 - similarity so smaller means closer.
package cosine

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
		Name:        "cosine",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Cosine distance (1 - similarity) over numeric vectors (V, L of N, or NS)",
	}
}

func (Op) Name() string { return "cosine" }

func (Op) Supports(a, b model.AttrType) bool {
	return numericVec(a) && numericVec(b)
}

func numericVec(t model.AttrType) bool {
	return t == model.AttrVec || t == model.AttrL || t == model.AttrNS
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	av, err := vecattr.AsFloats(a)
	if err != nil {
		return 0, fmt.Errorf("cosine: %w", err)
	}
	bv, err := vecattr.AsFloats(b)
	if err != nil {
		return 0, fmt.Errorf("cosine: %w", err)
	}
	if err := vecattr.Same(av, bv); err != nil {
		return 0, fmt.Errorf("cosine: %w", err)
	}
	var dot, na, nb float64
	for i := range av {
		dot += av[i] * bv[i]
		na += av[i] * av[i]
		nb += bv[i] * bv[i]
	}
	if na == 0 || nb == 0 {
		return 1, nil
	}
	sim := dot / (math.Sqrt(na) * math.Sqrt(nb))
	// Clamp away from float drift past the theoretical range.
	if sim > 1 {
		sim = 1
	}
	if sim < -1 {
		sim = -1
	}
	return 1 - sim, nil
}

func init() { plugin.Default.MustRegister(Op{}) }
