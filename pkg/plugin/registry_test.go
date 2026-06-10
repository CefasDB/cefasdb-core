package plugin_test

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

type stubPlugin struct{ name string; kind plugin.Kind }

func (s *stubPlugin) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: s.name, Kind: s.kind, Version: "1"}
}

func TestRegisterAndLookup(t *testing.T) {
	r := plugin.NewRegistry()
	p := &stubPlugin{name: "trigram", kind: plugin.KindIndex}
	if err := r.Register(p); err != nil {
		t.Fatalf("register: %v", err)
	}
	got, ok := r.Lookup("trigram")
	if !ok || got != p {
		t.Fatalf("lookup = %v / ok=%v", got, ok)
	}
}

func TestRegisterRejectsNilAndInvalid(t *testing.T) {
	r := plugin.NewRegistry()
	if err := r.Register(nil); err == nil {
		t.Fatal("expected nil-plugin error")
	}
	if err := r.Register(&stubPlugin{name: "", kind: plugin.KindIndex}); err == nil {
		t.Fatal("expected empty-name error")
	}
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlugin{name: "trigram", kind: plugin.KindIndex})
	if err := r.Register(&stubPlugin{name: "trigram", kind: plugin.KindIndex}); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestDisableEnableLookup(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlugin{name: "trigram", kind: plugin.KindIndex})

	if err := r.Disable("trigram"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if !r.IsDisabled("trigram") {
		t.Fatal("IsDisabled = false, want true")
	}
	if _, ok := r.Lookup("trigram"); ok {
		t.Fatal("disabled plugin returned ok=true from Lookup")
	}
	// Disable should not erase the plugin — re-enable restores it.
	if err := r.Enable("trigram"); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if _, ok := r.Lookup("trigram"); !ok {
		t.Fatal("re-enabled plugin missing from Lookup")
	}
}

func TestDisableUnknown(t *testing.T) {
	r := plugin.NewRegistry()
	if err := r.Disable("ghost"); err == nil {
		t.Fatal("expected error for unknown plugin")
	}
}

func TestListIsSortedAcrossKinds(t *testing.T) {
	r := plugin.NewRegistry()
	for _, p := range []*stubPlugin{
		{name: "trigram", kind: plugin.KindIndex},
		{name: "cosine", kind: plugin.KindDistance},
		{name: "hll", kind: plugin.KindEstimator},
		{name: "bloom", kind: plugin.KindIndex},
	} {
		_ = r.Register(p)
	}
	got := r.List()
	want := []string{"bloom", "cosine", "hll", "trigram"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, n := range want {
		if got[i].Manifest().Name != n {
			t.Fatalf("[%d] = %q, want %q", i, got[i].Manifest().Name, n)
		}
	}
}

func TestLookupByKind(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlugin{name: "trigram", kind: plugin.KindIndex})
	_ = r.Register(&stubPlugin{name: "cosine", kind: plugin.KindDistance})
	_ = r.Register(&stubPlugin{name: "bloom", kind: plugin.KindIndex})

	idx := r.LookupByKind(plugin.KindIndex)
	if len(idx) != 2 || idx[0].Manifest().Name != "bloom" || idx[1].Manifest().Name != "trigram" {
		t.Fatalf("KindIndex = %v", idx)
	}
	dist := r.LookupByKind(plugin.KindDistance)
	if len(dist) != 1 || dist[0].Manifest().Name != "cosine" {
		t.Fatalf("KindDistance = %v", dist)
	}
}
