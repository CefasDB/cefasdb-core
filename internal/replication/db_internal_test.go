package replication

import (
	"errors"
	"testing"
)

func TestEnqueueAsyncApplyDropsWhenQueueIsFull(t *testing.T) {
	d := &DB{
		stopCh:  make(chan struct{}),
		applyCh: make(chan *applyReq, 1),
	}

	if err := d.enqueueAsyncApply([]byte("batch-1")); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	err := d.enqueueAsyncApply([]byte("batch-2"))
	if !errors.Is(err, ErrAsyncApplyQueueFull) {
		t.Fatalf("second enqueue err = %v, want ErrAsyncApplyQueueFull", err)
	}

	stats := d.AsyncReplicationStats()
	if stats.Submitted != 1 || stats.Dropped != 1 || stats.QueueDepth != 1 || stats.QueueCapacity != 1 {
		t.Fatalf("stats = %+v, want submitted=1 dropped=1 queue=1/1", stats)
	}
}

func TestCompleteApplyCountsAsyncErrorsWithoutDoneChannel(t *testing.T) {
	d := &DB{}
	d.completeApply(&applyReq{}, ErrNotLeader)

	stats := d.AsyncReplicationStats()
	if stats.ApplyErrors != 1 {
		t.Fatalf("ApplyErrors = %d, want 1", stats.ApplyErrors)
	}
}
