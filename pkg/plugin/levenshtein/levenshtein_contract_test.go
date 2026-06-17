package levenshtein_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin/distancecontract"
	"github.com/CefasDb/cefasdb/pkg/plugin/levenshtein"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           levenshtein.Op{},
		ExpectedName: "levenshtein",
		Cases: []distancecontract.Case{
			{Name: "kitten-sitting", A: s("kitten"), B: s("sitting")},
			{Name: "flaw-lawn", A: s("flaw"), B: s("lawn")},
			{Name: "identical", A: s("hello"), B: s("hello")},
		},
	})
}
