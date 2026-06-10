// Package raft replicates cefas writes across a cluster using
// hashicorp/raft. Every write to storage.DB flows through Replicate as
// an opaque pebble.Batch.Repr; raft replicates the bytes; followers
// apply them to their local Pebble via the FSM. Reads stay local.
package raft

import (
	"fmt"
	"io"

	pebbledb "github.com/cockroachdb/pebble"
	hraft "github.com/hashicorp/raft"
)

// fsm is the hashicorp/raft.FSM for a cefas node. Apply replays a
// pebble.Batch.Repr committed through the log; Snapshot/Restore handle
// log compaction over the cefas/ keyspace (raft/ state is excluded —
// the new node rebuilds its own log/stable views from the snapshot
// install protocol).
//
// raft serializes Apply calls. Snapshot may run concurrently with
// Apply because it captures a point-in-time pebble.Snapshot inside
// Snapshot() before returning the FSMSnapshot — the iterator sees a
// stable view even as new entries land.
type fsm struct {
	pebble *pebbledb.DB
}

func newFSM(pebble *pebbledb.DB) *fsm { return &fsm{pebble: pebble} }

// Apply replays the committed log entry into Pebble. The Data slice is
// produced by storage.DB.CommitBatch → Replicator.Replicate(batch.Repr).
// We copy it before handing to Pebble because raft reuses its log
// buffer on the next dispatch.
//
// Non-command entries and empty payloads are no-ops — raft's runFSM
// already filters most of them but defensive code costs nothing.
func (f *fsm) Apply(log *hraft.Log) any {
	if log == nil || log.Type != hraft.LogCommand || len(log.Data) == 0 {
		return nil
	}
	repr := make([]byte, len(log.Data))
	copy(repr, log.Data)

	batch := f.pebble.NewBatch()
	defer batch.Close()
	if err := batch.SetRepr(repr); err != nil {
		return fmt.Errorf("fsm apply: SetRepr: %w", err)
	}
	if err := batch.Commit(pebbledb.NoSync); err != nil {
		return fmt.Errorf("fsm apply: commit: %w", err)
	}
	return nil
}

// Snapshot grabs a point-in-time pebble.Snapshot. The snapshot stays
// pinned until raft calls Persist or Release on the returned
// FSMSnapshot.
func (f *fsm) Snapshot() (hraft.FSMSnapshot, error) {
	snap := f.pebble.NewSnapshot()
	return &fsmSnapshot{snap: snap}, nil
}

// Restore wipes the local cefas/ range and re-populates it from rc.
// Called by raft on startup or after an install_snapshot RPC.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := readSnapshot(rc, f.pebble); err != nil {
		return fmt.Errorf("fsm restore: %w", err)
	}
	return nil
}

type fsmSnapshot struct {
	snap *pebbledb.Snapshot
}

func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	if err := writeSnapshot(sink, s.snap); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("fsm snapshot persist: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {
	if s.snap != nil {
		_ = s.snap.Close()
		s.snap = nil
	}
}
