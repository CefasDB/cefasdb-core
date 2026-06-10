package ttl_test

import (
	"sync"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/core/ttl"
)

type recObserver struct {
	mu    sync.Mutex
	calls []string
}

func (r *recObserver) OnExpire(table string, key model.Item) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, table)
}

type stubTTL struct {
	attr      map[string]string
	mu        sync.Mutex
	observers []ttl.Observer
}

func (s *stubTTL) Attribute(table string) string { return s.attr[table] }
func (s *stubTTL) Subscribe(o ttl.Observer) (cancel func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observers = append(s.observers, o)
	idx := len(s.observers) - 1
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.observers = append(s.observers[:idx], s.observers[idx+1:]...)
	}
}
func (s *stubTTL) Fire(table string, key model.Item) {
	s.mu.Lock()
	obs := append([]ttl.Observer(nil), s.observers...)
	s.mu.Unlock()
	for _, o := range obs {
		o.OnExpire(table, key)
	}
}

func TestSubscribeAndUnsubscribe(t *testing.T) {
	svc := &stubTTL{attr: map[string]string{"Sessions": "expires_at"}}
	if svc.Attribute("Sessions") != "expires_at" {
		t.Fatal("attribute lookup wrong")
	}
	obs := &recObserver{}
	cancel := svc.Subscribe(obs)
	svc.Fire("Sessions", model.Item{"pk": {T: model.AttrS, S: "u1"}})
	if len(obs.calls) != 1 {
		t.Fatalf("observer not called: %v", obs.calls)
	}
	cancel()
	svc.Fire("Sessions", nil)
	if len(obs.calls) != 1 {
		t.Fatalf("observer called after cancel: %v", obs.calls)
	}
}
