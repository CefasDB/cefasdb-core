package hamming_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin/distancecontract"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/hamming"
)

func TestDistanceContract(t *testing.T) {
	distancecontract.Run(t, distancecontract.Spec{
		Op:           hamming.Op{},
		ExpectedName: "hamming",
		Cases: []distancecontract.Case{
			{Name: "string", A: s("karolin"), B: s("kathrin")},
			{Name: "string-equal", A: s("abcd"), B: s("abcd")},
			{Name: "bytes", A: b([]byte{0x00, 0x11, 0x22}), B: b([]byte{0x00, 0x10, 0x22})},
		},
	})
}
