package euclidean_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin/distancecontract"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/euclidean"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           euclidean.Op{},
		ExpectedName: "euclidean",
		Cases: []distancecontract.Case{
			{Name: "axis", A: vec("1", "0", "0"), B: vec("0", "1", "0")},
			{Name: "diagonal", A: vec("1", "2", "3"), B: vec("4", "5", "6")},
			{Name: "negative", A: vec("-1", "0"), B: vec("1", "0")},
		},
	})
}
