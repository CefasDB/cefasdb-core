package stream_test

import (
	"sync"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/core/model"
	"github.com/osvaldoandrade/cefas/internal/core/stream"
)

type recSub struct {
	mu     sync.Mutex
	events []stream.Event
}

func (r *recSub) OnChange(e stream.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

type stubStream struct {
	subs []stream.Subscriber
}

func (s *stubStream) Subscribe(sub stream.Subscriber) (func(), error) {
	s.subs = append(s.subs, sub)
	idx := len(s.subs) - 1
	return func() { s.subs = append(s.subs[:idx], s.subs[idx+1:]...) }, nil
}
func (s *stubStream) Publish(e stream.Event) {
	for _, sub := range s.subs {
		_ = sub.OnChange(e)
	}
}

func TestStreamSubscriberReceivesEvents(t *testing.T) {
	st := &stubStream{}
	sub := &recSub{}
	cancel, err := st.Subscribe(sub)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cancel()
	st.Publish(stream.Event{RaftIndex: 1, Op: stream.OpPut, Table: "T", Key: model.Item{"pk": {T: model.AttrS, S: "a"}}})
	st.Publish(stream.Event{RaftIndex: 2, Op: stream.OpDelete, Table: "T", Key: model.Item{"pk": {T: model.AttrS, S: "b"}}})
	if len(sub.events) != 2 {
		t.Fatalf("events = %d, want 2", len(sub.events))
	}
	if sub.events[0].Op != stream.OpPut || sub.events[1].Op != stream.OpDelete {
		t.Fatalf("ops wrong: %v", sub.events)
	}
}
