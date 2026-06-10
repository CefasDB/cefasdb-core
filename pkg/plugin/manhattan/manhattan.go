// Package manhattan is the L1-distance operator plugin over numeric
// vectors. Sum of absolute differences per dimension.
package manhattan

import (
	"fmt"
	"math"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/internal/vecattr"
)

type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "manhattan",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Manhattan (L1) distance over numeric vectors (L of N, or NS)",
	}
}

func (Op) Name() string { return "manhattan" }

func (Op) Supports(a, b model.AttrType) bool {
	return (a == model.AttrL || a == model.AttrNS) && (b == model.AttrL || b == model.AttrNS)
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	av, err := vecattr.AsFloats(a)
	if err != nil {
		return 0, fmt.Errorf("manhattan: %w", err)
	}
	bv, err := vecattr.AsFloats(b)
	if err != nil {
		return 0, fmt.Errorf("manhattan: %w", err)
	}
	if err := vecattr.Same(av, bv); err != nil {
		return 0, fmt.Errorf("manhattan: %w", err)
	}
	var s float64
	for i := range av {
		s += math.Abs(av[i] - bv[i])
	}
	return s, nil
}

func init() { plugin.Default.MustRegister(Op{}) }
