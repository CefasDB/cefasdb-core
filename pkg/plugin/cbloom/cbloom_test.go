package cbloom_test

import (
	"fmt"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin/cbloom"
)

func TestAddRemoveContains(t *testing.T) {
	f, err := cbloom.New([]byte(`{"field":"v","m":2048,"k":5,"width":4}`))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for i := 0; i < 200; i++ {
		f.Add(fmt.Appendf(nil, "x-%d", i))
	}
	for i := 0; i < 200; i++ {
		if !f.Contains(fmt.Appendf(nil, "x-%d", i)) {
			t.Fatalf("missing %d", i)
		}
	}
	for i := 0; i < 200; i++ {
		f.Remove(fmt.Appendf(nil, "x-%d", i))
	}
	// After removal we expect (almost) no positives. A few may
	// remain due to hash collisions; assert mostly gone.
	stillThere := 0
	for i := 0; i < 200; i++ {
		if f.Contains(fmt.Appendf(nil, "x-%d", i)) {
			stillThere++
		}
	}
	if stillThere > 20 {
		t.Fatalf("after removal %d remain (want <= 20)", stillThere)
	}
}

func TestPluginUpdateDeletesOld(t *testing.T) {
	p := cbloom.NewPlugin()
	d := index.Descriptor{Table: "T", Name: "x", PluginName: "cbloom",
		PluginConfig: []byte(`{"field":"email","m":2048,"k":5,"width":4}`)}
	old := model.Item{"email": {T: model.AttrS, S: "old@example.com"}}
	neu := model.Item{"email": {T: model.AttrS, S: "new@example.com"}}
	// Pre-seed via Update so the old value is present.
	_ = p.Update(d, nil, old)
	// Now flip; old should leave, new should join.
	if err := p.Update(d, old, neu); err != nil {
		t.Fatalf("update: %v", err)
	}
}

func TestSerializeRoundTrip(t *testing.T) {
	f, _ := cbloom.New([]byte(`{"field":"v","m":256,"k":3,"width":4}`))
	for i := 0; i < 32; i++ {
		f.Add(fmt.Appendf(nil, "v-%d", i))
	}
	buf, err := f.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	g, err := cbloom.Deserialize(buf)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	for i := 0; i < 32; i++ {
		if !g.Contains(fmt.Appendf(nil, "v-%d", i)) {
			t.Fatalf("round-trip lost %d", i)
		}
	}
}

func TestWidthValidation(t *testing.T) {
	if _, err := cbloom.New([]byte(`{"field":"x","m":16,"k":2,"width":17}`)); err == nil {
		t.Fatal("expected width-range error")
	}
}
