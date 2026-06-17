package cosine_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin/cosine"
	"github.com/CefasDb/cefasdb/pkg/plugin/distancecontract"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           cosine.Op{},
		ExpectedName: "cosine",
		Cases: []distancecontract.Case{
			{Name: "orthogonal", A: vec(1, 0), B: vec(0, 1)},
			{Name: "parallel", A: vec(1, 2, 3), B: vec(2, 4, 6)},
			{Name: "diagonal", A: vec(1, 1, 1), B: vec(0.5, 0.5, 0.5)},
		},
	})
}
