package model_test

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// TestTypeAliasesRefersToPkgTypes pins the contract that the model
// package is a pure facade — every promoted symbol must be the same
// underlying type as the canonical one in pkg/types. If someone later
// adds an *adapter* type (struct wrapper) here, this test fires.
func TestTypeAliasesRefersToPkgTypes(t *testing.T) {
	var (
		_ model.Item           = types.Item{}
		_ model.KeySchema      = types.KeySchema{}
		_ model.AttributeValue = types.AttributeValue{}
		_ model.TableDescriptor = types.TableDescriptor{}
	)
	// Spot-check one attribute kind alias is wire-compatible.
	if model.AttrS != types.AttrS {
		t.Fatalf("AttrS alias drifted: model=%v types=%v", model.AttrS, types.AttrS)
	}
}
