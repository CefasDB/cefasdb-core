package plugin_test

import (
	"context"
	"errors"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin"
)

type lcPlugin struct {
	name        string
	startErr    error
	stopErr     error
	startCalled int
	stopCalled  int
}

func (p *lcPlugin) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: p.name, Kind: plugin.KindIndex, Version: "1"}
}
func (p *lcPlugin) Start(context.Context) error { p.startCalled++; return p.startErr }
func (p *lcPlugin) Stop(context.Context) error  { p.stopCalled++; return p.stopErr }

func TestManagerStartStopSymmetric(t *testing.T) {
	r := plugin.NewRegistry()
	a, b := &lcPlugin{name: "a"}, &lcPlugin{name: "b"}
	_ = r.Register(a)
	_ = r.Register(b)
	mgr := plugin.NewManager(r)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if a.startCalled != 1 || b.startCalled != 1 {
		t.Fatalf("starts: a=%d b=%d", a.startCalled, b.startCalled)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if a.stopCalled != 1 || b.stopCalled != 1 {
		t.Fatalf("stops: a=%d b=%d", a.stopCalled, b.stopCalled)
	}
}

func TestManagerStartHaltsOnFirstError(t *testing.T) {
	r := plugin.NewRegistry()
	a := &lcPlugin{name: "a"}
	bad := &lcPlugin{name: "b", startErr: errors.New("boom")}
	c := &lcPlugin{name: "c"}
	_ = r.Register(a)
	_ = r.Register(bad)
	_ = r.Register(c)
	mgr := plugin.NewManager(r)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	// a started, bad attempted, c never reached.
	if a.startCalled != 1 || bad.startCalled != 1 || c.startCalled != 0 {
		t.Fatalf("a=%d bad=%d c=%d", a.startCalled, bad.startCalled, c.startCalled)
	}
}

func TestManagerStopRunsBestEffort(t *testing.T) {
	r := plugin.NewRegistry()
	a := &lcPlugin{name: "a", stopErr: errors.New("a-stop")}
	b := &lcPlugin{name: "b"}
	_ = r.Register(a)
	_ = r.Register(b)
	mgr := plugin.NewManager(r)
	_ = mgr.Start(context.Background())
	err := mgr.Stop(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if a.stopCalled != 1 || b.stopCalled != 1 {
		t.Fatalf("a=%d b=%d", a.stopCalled, b.stopCalled)
	}
}

func TestManagerSkipsNonLifecyclePlugins(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlugin{name: "nolifecycle", kind: plugin.KindIndex})
	mgr := plugin.NewManager(r)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}
