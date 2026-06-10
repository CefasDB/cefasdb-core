package core_test

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

// Sanity contract: pkg/core's Descriptor carries enough fields to
// describe every built-in index kind plus a plugin-backed one. If a
// later refactor drops a field, this test fires.
func TestIndexDescriptorCarriesPluginPlumbing(t *testing.T) {
	d := index.Descriptor{
		Table:        "Users",
		Name:         "by_email",
		PluginName:   "trigram",
		PluginConfig: []byte(`{"field":"email","n":3}`),
		KeySchema:    model.KeySchema{PK: "email"},
		Projection:   []string{"id", "name"},
	}
	if d.PluginName == "" || len(d.PluginConfig) == 0 || d.KeySchema.PK == "" {
		t.Fatalf("descriptor missing required fields: %+v", d)
	}
}
