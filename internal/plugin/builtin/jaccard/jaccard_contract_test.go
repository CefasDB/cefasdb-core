package jaccard_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin/distancecontract"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/jaccard"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           jaccard.Op{},
		ExpectedName: "jaccard",
		Cases: []distancecontract.Case{
			{Name: "overlap", A: ss("a", "b"), B: ss("a", "b", "c")},
			{Name: "disjoint", A: ss("a"), B: ss("b")},
			{Name: "identical", A: ss("a", "b", "c"), B: ss("a", "b", "c")},
		},
	})
}
