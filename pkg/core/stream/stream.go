// Package stream defines the change-stream surface plugins use to
// maintain derived state (e.g. SimHash dedup, MinHash signatures)
// without polling the base table.
package stream

import "github.com/osvaldoandrade/cefas/pkg/core/model"

// Op classifies a change event.
type Op uint8

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
