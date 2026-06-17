// Package vecattr extracts a numeric vector from an
// AttributeValue. Lives under pkg/plugin/internal so it stays out of
// the public plugin API; vector-distance plugins reuse it instead of
// each rolling their own AttrL / AttrNS parser.
package vecattr

import (
	"fmt"
	"strconv"

	"github.com/CefasDb/cefasdb/pkg/core/model"
)

// AsFloats returns av's numeric vector in float64 form. Supported
// kinds:
//   - AttrVec (native vector)
//   - AttrL (list of AttrN entries)
//   - AttrNS (number set on the wire, []string)
//
// Other kinds — including AttrL with non-numeric entries — return a
// clearly-typed error so the planner can reject the operator at plan
// time instead of at row time.
func AsFloats(av model.AttributeValue) ([]float64, error) {
	switch av.T {
	case model.AttrVec:
		return append([]float64(nil), av.Vec...), nil
	case model.AttrL:
		out := make([]float64, 0, len(av.L))
		for i, e := range av.L {
			if e.T != model.AttrN {
				return nil, fmt.Errorf("vecattr: L[%d] is %v, want N", i, e.T)
			}
			v, err := strconv.ParseFloat(e.N, 64)
			if err != nil {
				return nil, fmt.Errorf("vecattr: L[%d] parse %q: %w", i, e.N, err)
			}
			out = append(out, v)
		}
		return out, nil
	case model.AttrNS:
		out := make([]float64, 0, len(av.NS))
		for i, s := range av.NS {
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return nil, fmt.Errorf("vecattr: NS[%d] parse %q: %w", i, s, err)
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, fmt.Errorf("vecattr: kind %v is not a numeric vector (need V, L or NS)", av.T)
}

// Same returns true iff a and b have identical lengths. The vector
// distance plugins call this before iterating so dim-mismatch error
// messages stay uniform.
func Same(a, b []float64) error {
	if len(a) != len(b) {
		return fmt.Errorf("vecattr: dimension mismatch (%d vs %d)", len(a), len(b))
	}
	return nil
}
