package jarowinkler_test

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/plugin/distancecontract"
	"github.com/osvaldoandrade/cefas/pkg/plugin/jarowinkler"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           jarowinkler.Op{},
		ExpectedName: "jaro_winkler",
		Cases: []distancecontract.Case{
			{Name: "martha", A: s("MARTHA"), B: s("MARHTA")},
			{Name: "dwayne", A: s("DWAYNE"), B: s("DUANE")},
			{Name: "identical", A: s("hello"), B: s("hello")},
		},
	})
}
