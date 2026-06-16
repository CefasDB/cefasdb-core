package haversine_test

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/plugin/distancecontract"
	"github.com/osvaldoandrade/cefas/pkg/plugin/haversine"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           haversine.Op{},
		ExpectedName: "haversine",
		// Distances on Earth are in metres; default 1e-9 epsilon is
		// too tight for symmetry checks at km-scale floats.
		Epsilon: 1e-3,
		Cases: []distancecontract.Case{
			{Name: "sp-santos", A: loc("-23.5505", "-46.6333"), B: loc("-23.9608", "-46.3336")},
			{Name: "equator-prime", A: loc("0", "0"), B: loc("0", "10")},
			{Name: "antipodal", A: loc("0", "0"), B: loc("0", "180")},
		},
	})
}
