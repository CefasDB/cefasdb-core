package plugin_test

import (
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

type spStub struct {
	name string
	st   plugin.Status
}

func (s *spStub) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: s.name, Kind: plugin.KindIndex, Version: "1"}
}
func (s *spStub) Status() plugin.Status { return s.st }

func TestSnapshotPrefersStatusProvider(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&spStub{name: "p", st: plugin.Status{Name: "p", Kind: "index", ItemsIndexed: 1234}})
	_ = r.Register(&stubPlugin{name: "q", kind: plugin.KindDistance})

	snap := plugin.Snapshot(r, func(name string) plugin.State {
		if name == "p" {
			return plugin.StateRunning
		}
		return plugin.StateLoaded
	}, nil)
	if len(snap) != 2 {
		t.Fatalf("len = %d, want 2", len(snap))
	}
	// p comes first (sorted by name).
	if snap[0].Name != "p" || snap[0].ItemsIndexed != 1234 || snap[0].State != "running" {
		t.Fatalf("p = %+v", snap[0])
	}
	if snap[1].Name != "q" || snap[1].Kind != "distance" || snap[1].State != "loaded" {
		t.Fatalf("q = %+v", snap[1])
	}
}

func TestSnapshotIncludesLastError(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlugin{name: "p", kind: plugin.KindIndex})
	when := time.Unix(1717_000_000, 0)
	snap := plugin.Snapshot(r, nil, func(name string) (string, time.Time) {
		return "boom", when
	})
	if snap[0].LastError != "boom" || snap[0].LastErrorAtUnix != when.Unix() {
		t.Fatalf("lastErr = %+v", snap[0])
	}
}
