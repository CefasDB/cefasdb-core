package manhattan_test

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/plugin/distancecontract"
	"github.com/osvaldoandrade/cefas/pkg/plugin/manhattan"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           manhattan.Op{},
		ExpectedName: "manhattan",
		Cases: []distancecontract.Case{
			{Name: "axis", A: vec("1", "0", "0"), B: vec("0", "1", "0")},
			{Name: "diagonal", A: vec("1", "2", "3"), B: vec("4", "5", "6")},
			{Name: "negative", A: vec("-3", "0"), B: vec("3", "0")},
		},
	})
}
