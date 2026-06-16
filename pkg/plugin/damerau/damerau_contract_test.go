package damerau_test

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/plugin/damerau"
	"github.com/osvaldoandrade/cefas/pkg/plugin/distancecontract"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           damerau.Op{},
		ExpectedName: "damerau",
		Cases: []distancecontract.Case{
			{Name: "transposition", A: s("ab"), B: s("ba")},
			{Name: "typo", A: s("recieve"), B: s("receive")},
			{Name: "identical", A: s("hello"), B: s("hello")},
		},
	})
}
