package plugin_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin"
)

func TestManifestKindString(t *testing.T) {
	cases := map[plugin.Kind]string{
		plugin.KindIndex:     "index",
		plugin.KindDistance:  "distance",
		plugin.KindEstimator: "estimator",
		plugin.KindAudience:  "audience",
		plugin.Kind(99):      "unspecified",
	}
	for k, want := range cases {
		if got := k.String(); got != want {
			t.Errorf("Kind(%d).String() = %q, want %q", k, got, want)
		}
	}
}

func TestManifestValidate(t *testing.T) {
	good := plugin.Manifest{Name: "trigram", Kind: plugin.KindIndex, Version: "1.0"}
	if err := good.Validate(); err != nil {
		t.Errorf("good manifest rejected: %v", err)
	}
	bads := []plugin.Manifest{
		{Kind: plugin.KindIndex, Version: "1"},                  // no name
		{Name: "x", Version: "1"},                               // no kind
		{Name: "x", Kind: plugin.KindIndex},                     // no version
		{Name: "x", Kind: plugin.KindUnspecified, Version: "1"}, // explicit unspecified
	}
	for i, m := range bads {
		if err := m.Validate(); err == nil {
			t.Errorf("[%d] expected validation error for %+v", i, m)
		}
	}
}
