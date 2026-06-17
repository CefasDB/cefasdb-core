package stream

import "github.com/CefasDb/cefasdb/internal/core/model"

// Op classifies a change event.
type Op uint8

// OpUnspecified / OpPut / OpDelete classify a change event: the
// zero value when the source has not set Op, a row insertion or
// update, and a row deletion.
const (
	OpUnspecified Op = iota
	OpPut
	OpDelete
)

// Event describes one committed mutation in raft-log order.
type Event struct {
	RaftIndex uint64
	Op        Op
	Table     string
	Key       model.Item
	NewItem   model.Item // populated on OpPut
	OldItem   model.Item // populated when known (best-effort)
}

// Subscriber receives change events. Implementations must return
// quickly; slow subscribers will be dropped from the live ring buffer
// and must fall back to a snapshot replay.
type Subscriber interface {
	OnChange(Event) error
}

// ChangeStream is the engine-side seam plugins subscribe against.
type ChangeStream interface {
	Subscribe(Subscriber) (cancel func(), err error)
}
