package damerau_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/internal/plugin/builtin/damerau"
	"github.com/CefasDb/cefasdb/pkg/plugin/distancecontract"
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
