package replication

import (
	"context"
	"sync"

	pebbledb "github.com/cockroachdb/pebble"
)

// ChangeOp is the verb on a ChangeEvent.
type ChangeOp uint8

const (
	OpPut ChangeOp = iota + 1
	OpDelete
)

// ChangeEvent is one (key, op) pair emitted by the FSM after a
// committed raft log entry applies. Subscribers consume the stream
// to build materialised views, search indexes, cache invalidations,
// or any other downstream that needs to react to writes.
//
// The cefas/raft/* keyspace is filtered out — subscribers only see
// cefas/ user data keys. RaftIndex is the log index of the entry
// that produced the event; resumable subscribers persist the last
// observed index and pass it back via Subscribe.
type ChangeEvent struct {
	RaftIndex uint64
	Op        ChangeOp
	Key       []byte
	// Value is set on OpPut. Empty on OpDelete.
	Value []byte
}

// Publisher is the FSM-side write surface. Each Apply call gets a
// snapshot of the batch's mutations and emits one event per mutation
// to every active subscriber. Subscribers that fall behind by more
// than ringSize events lose the oldest entries — at-most-once for
// stalled consumers, exactly-once-in-order for live ones.
type Publisher struct {
	mu          sync.RWMutex
	subscribers map[*subscription]struct{}
	ringSize    int
}

// NewPublisher returns a ready Publisher.
func NewPublisher(ringSize int) *Publisher {
	if ringSize <= 0 {
		ringSize = 1024
	}
	return &Publisher{
		subscribers: make(map[*subscription]struct{}),
		ringSize:    ringSize,
	}
}

type subscription struct {
	ch   chan ChangeEvent
	done chan struct{}
}

// Subscribe returns a read channel that emits every change event
// committed after the call. The caller cancels by closing the
// returned cancel function or by ctx cancellation; either path
// drains and frees the subscription slot.
func (p *Publisher) Subscribe(ctx context.Context) (<-chan ChangeEvent, func()) {
	sub := &subscription{
		ch:   make(chan ChangeEvent, p.ringSize),
		done: make(chan struct{}),
	}
	p.mu.Lock()
	p.subscribers[sub] = struct{}{}
	p.mu.Unlock()

	cancel := func() {
		p.mu.Lock()
		if _, ok := p.subscribers[sub]; ok {
			delete(p.subscribers, sub)
			close(sub.done)
			close(sub.ch)
		}
		p.mu.Unlock()
	}
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return sub.ch, cancel
}

// publish fans an event out to every subscriber. Backpressure is
// non-blocking: when a subscriber's ring is full, the event drops
// for that subscriber. Live consumers see the full ordered stream.
func (p *Publisher) publish(ev ChangeEvent) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for sub := range p.subscribers {
		select {
		case sub.ch <- ev:
		default:
			// Drop; slow consumer.
		}
	}
}

// SubscriberCount reports the number of live subscribers (used by
// tests and metrics).
func (p *Publisher) SubscriberCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.subscribers)
}

// applyAndPublish replays a serialized batch against the local
// Pebble store while collecting the (key, op) pairs the FSM emits to
// its publisher. Used by fsm.Apply when a publisher is attached.
func applyAndPublish(db *pebbledb.DB, repr []byte, raftIndex uint64, pub *Publisher) error {
	batch := db.NewBatch()
	defer batch.Close()
	if err := batch.SetRepr(repr); err != nil {
		return err
	}

	// Collect events before commit so a failed Commit doesn't leak
	// half-published changes. We iterate the batch via Reader().
	reader := batch.Reader()
	var events []ChangeEvent
	for {
		kind, k, v, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		// Filter raft state-keeping keys; only emit cefas/ user data.
		if len(k) >= 5 && string(k[:5]) == "raft/" {
			continue
		}
		switch kind {
		case pebbledb.InternalKeyKindSet:
			events = append(events, ChangeEvent{
				RaftIndex: raftIndex,
				Op:        OpPut,
				Key:       append([]byte(nil), k...),
				Value:     append([]byte(nil), v...),
			})
		case pebbledb.InternalKeyKindDelete:
			events = append(events, ChangeEvent{
				RaftIndex: raftIndex,
				Op:        OpDelete,
				Key:       append([]byte(nil), k...),
			})
		}
	}

	if err := batch.Commit(pebbledb.NoSync); err != nil {
		return err
	}
	for _, ev := range events {
		pub.publish(ev)
	}
	return nil
}
