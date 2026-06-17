package server

import (
	"context"
	"errors"
	"testing"
	"time"

	craft "github.com/CefasDb/cefasdb/internal/replication"
)

// fakeRaft implements raftSource without spinning up Pebble or hraft.
type fakeRaft struct {
	publisher *craft.Publisher
	metas     []craft.SnapshotMetadata
	err       error
}

func (f *fakeRaft) Publisher() *craft.Publisher { return f.publisher }

func (f *fakeRaft) ListSnapshots() ([]craft.SnapshotMetadata, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.metas, nil
}

func TestNewStreamAdapterNilSafe(t *testing.T) {
	a := NewStreamAdapter(nil)
	if a == nil {
		t.Fatal("constructor must not return nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, stop := a.SubscribeChanges(ctx)
	if ch == nil || stop == nil {
		t.Fatal("nil-safe SubscribeChanges must return non-nil channel and cancel")
	}
	stop()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel for nil raft")
		}
	case <-time.After(time.Second):
		t.Fatal("nil-raft channel must be closed immediately")
	}
	metas, err := a.ListSnapshots()
	if err != nil {
		t.Fatalf("ListSnapshots on nil raft: %v", err)
	}
	if metas != nil {
		t.Fatalf("expected nil metas, got %v", metas)
	}
}

func TestStreamAdapterSubscribeChanges(t *testing.T) {
	pub := craft.NewPublisher(16)
	a := &StreamAdapter{raft: &fakeRaft{publisher: pub}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, stop := a.SubscribeChanges(ctx)
	if ch == nil || stop == nil {
		t.Fatal("SubscribeChanges returned nil")
	}

	stop()
	select {
	case _, ok := <-ch:
		if ok {
			// Drained but still open; consume until close.
			for range ch {
			}
		}
	case <-time.After(time.Second):
		t.Fatal("channel did not close after cancel")
	}
}

func TestStreamAdapterSubscribeChangesNilPublisher(t *testing.T) {
	a := &StreamAdapter{raft: &fakeRaft{publisher: nil}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, stop := a.SubscribeChanges(ctx)
	if ch == nil || stop == nil {
		t.Fatal("SubscribeChanges must return non-nil channel and cancel")
	}
	stop()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel when publisher is nil")
		}
	case <-time.After(time.Second):
		t.Fatal("nil-publisher channel must be closed immediately")
	}
}

func TestStreamAdapterListSnapshotsRoundTrip(t *testing.T) {
	in := []craft.SnapshotMetadata{
		{ID: "snap-1", Index: 10, Term: 2, SizeBytes: 1024},
		{ID: "snap-2", Index: 42, Term: 3, SizeBytes: 4096},
	}
	a := &StreamAdapter{raft: &fakeRaft{metas: in}}
	out, err := a.ListSnapshots()
	if err != nil {
		t.Fatalf("ListSnapshots: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("len mismatch: want %d got %d", len(in), len(out))
	}
	for i, want := range in {
		got := out[i]
		if got.ID != want.ID || got.Index != want.Index ||
			got.Term != want.Term || got.SizeBytes != want.SizeBytes {
			t.Fatalf("entry %d mismatch: want %+v got %+v", i, want, got)
		}
	}
}

func TestStreamAdapterListSnapshotsError(t *testing.T) {
	boom := errors.New("raft closed")
	a := &StreamAdapter{raft: &fakeRaft{err: boom}}
	out, err := a.ListSnapshots()
	if !errors.Is(err, boom) {
		t.Fatalf("expected %v, got %v", boom, err)
	}
	if out != nil {
		t.Fatalf("expected nil metas on error, got %v", out)
	}
}
