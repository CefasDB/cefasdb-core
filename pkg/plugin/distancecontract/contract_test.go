package distancecontract_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin/distancecontract"
)

// fakeOp is a hand-rolled DistanceOp that returns a constant 0 for
// every input, satisfying every invariant. The smoke test below
// confirms the contract helper itself wires up correctly so a failure
// in a real plugin's contract test points at the plugin, not the
// helper.
type fakeOp struct{}

func (fakeOp) Name() string                      { return "fake" }
func (fakeOp) Supports(_, _ model.AttrType) bool { return true }
func (fakeOp) Eval(_, _ model.AttributeValue) (float64, error) {
	return 0, nil
}

func TestContractHelperSmoke(t *testing.T) {
	t.Parallel()
	v := model.AttributeValue{T: model.AttrS, S: "x"}
	distancecontract.Run(t, distancecontract.Spec{
		Op:           fakeOp{},
		ExpectedName: "fake",
		Cases: []distancecontract.Case{
			{Name: "ok", A: v, B: v},
		},
	})
}
