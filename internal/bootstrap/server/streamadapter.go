// Package server hosts construction-time wiring for cmd/cefasdb.
// It keeps the binary entry point thin by hosting the small adapters
// and helpers that bridge internal packages without forcing them to
// depend on each other.
package server

import (
	"context"

	apiserver "github.com/CefasDb/cefasdb/internal/server"
	craft "github.com/CefasDb/cefasdb/internal/replication"
)

// raftSource is the minimal raft surface StreamAdapter needs. It lets
// tests substitute an in-memory fake without spinning up a real raft
// node. *raft.DB satisfies it implicitly.
type raftSource interface {
	Publisher() *craft.Publisher
	ListSnapshots() ([]craft.SnapshotMetadata, error)
}

// StreamAdapter bridges the raft package's CDC types to the api
// package's wire-agnostic shape. Lives here so neither package needs
// to import the other.
type StreamAdapter struct {
	raft raftSource
}

// NewStreamAdapter wires a StreamAdapter onto raftDB. raftDB may be
// nil; in that case SubscribeChanges returns an already-closed channel
// and ListSnapshots returns (nil, nil).
func NewStreamAdapter(raftDB *craft.DB) *StreamAdapter {
	if raftDB == nil {
		return &StreamAdapter{}
	}
	return &StreamAdapter{raft: raftDB}
}

// SubscribeChanges implements apiserver.ChangeStream. It fans the raft
// publisher's events through a buffered channel and translates the
// raft ChangeOp enum into the wire string ("PUT" / "DELETE").
func (a *StreamAdapter) SubscribeChanges(ctx context.Context) (<-chan apiserver.ChangeEvent, func()) {
	var pub *craft.Publisher
	if a != nil && a.raft != nil {
		pub = a.raft.Publisher()
	}
	if pub == nil {
		out := make(chan apiserver.ChangeEvent)
		close(out)
		return out, func() {}
	}
	src, cancel := pub.Subscribe(ctx)
	out := make(chan apiserver.ChangeEvent, 64)
	go func() {
		defer close(out)
		for ev := range src {
			op := "PUT"
			if ev.Op == craft.OpDelete {
				op = "DELETE"
			}
			select {
			case out <- apiserver.ChangeEvent{RaftIndex: ev.RaftIndex, Op: op, Key: ev.Key, Value: ev.Value}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, cancel
}

// ListSnapshots implements apiserver.ChangeStream by projecting the raft
// SnapshotMetadata onto the api-side mirror type.
func (a *StreamAdapter) ListSnapshots() ([]apiserver.SnapshotMetadata, error) {
	if a == nil || a.raft == nil {
		return nil, nil
	}
	metas, err := a.raft.ListSnapshots()
	if err != nil {
		return nil, err
	}
	out := make([]apiserver.SnapshotMetadata, 0, len(metas))
	for _, m := range metas {
		out = append(out, apiserver.SnapshotMetadata{
			ID:        m.ID,
			Index:     m.Index,
			Term:      m.Term,
			SizeBytes: m.SizeBytes,
		})
	}
	return out, nil
}
