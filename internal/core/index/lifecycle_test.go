package index_test

import (
	"errors"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/core/index"
	"github.com/osvaldoandrade/cefas/internal/core/model"
)

type stubLifecycle struct {
	state map[string]index.Descriptor
}

func newStub() *stubLifecycle { return &stubLifecycle{state: map[string]index.Descriptor{}} }

func k(table, name string) string { return table + "/" + name }

func (s *stubLifecycle) Create(d index.Descriptor) error {
	if _, dup := s.state[k(d.Table, d.Name)]; dup {
		return errors.New("dup")
	}
	s.state[k(d.Table, d.Name)] = d
	return nil
}
func (s *stubLifecycle) Describe(t, n string) (index.Descriptor, error) {
	d, ok := s.state[k(t, n)]
	if !ok {
		return index.Descriptor{}, errors.New("not found")
	}
	return d, nil
}
func (s *stubLifecycle) Rebuild(t, n string) error {
	if _, ok := s.state[k(t, n)]; !ok {
		return errors.New("not found")
	}
	return nil
}
func (s *stubLifecycle) Drop(t, n string) error {
	if _, ok := s.state[k(t, n)]; !ok {
		return errors.New("not found")
	}
	delete(s.state, k(t, n))
	return nil
}

func TestLifecycleRoundTrip(t *testing.T) {
	var l index.Lifecycle = newStub()
	desc := index.Descriptor{
		Table: "Merchants", Name: "name_trigram",
		PluginName: "trigram", PluginConfig: []byte(`{"field":"name"}`),
		KeySchema: model.KeySchema{PK: "id"},
	}
	if err := l.Create(desc); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := l.Describe(desc.Table, desc.Name)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.PluginName != "trigram" || string(got.PluginConfig) != `{"field":"name"}` {
		t.Fatalf("descriptor mangled: %+v", got)
	}
	if err := l.Rebuild(desc.Table, desc.Name); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := l.Drop(desc.Table, desc.Name); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := l.Describe(desc.Table, desc.Name); err == nil {
		t.Fatal("expected describe-after-drop to error")
	}
}
