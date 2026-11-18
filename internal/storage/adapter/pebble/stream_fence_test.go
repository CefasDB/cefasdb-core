package pebble

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// TestStreamFenceWatermarkRandomizedModel drives the reservation primitive
// through random interleavings of register / allocate / commit / abandon and
// checks the invariant after every step: the watermark is below every
// allocated-but-uncommitted native sequence, and it never moves backwards.
func TestStreamFenceWatermarkRandomizedModel(t *testing.T) {
	td := textTable()
	td.StreamSpecification = &types.StreamSpecification{StreamEnabled: true}
	for seed := int64(1); seed <= 200; seed++ {
		rng := rand.New(rand.NewSource(seed))
		d := &DB{}
		d.changeIndex.Store(uint64(rng.Intn(1000)))
		type writer struct {
			release   func()
			allocated []uint64
		}
		var live []*writer
		uncommitted := map[uint64]bool{}
		var last uint64
		for step := 0; step < 400; step++ {
			switch op := rng.Intn(5); {
			case op == 0 || len(live) == 0:
				live = append(live, &writer{release: d.beginNativeStreamWrite(td)})
			case op == 1:
				// A non-native writer allocates without a reservation.
				d.changeIndex.Add(1)
			case op == 2:
				w := live[rng.Intn(len(live))]
				for n := rng.Intn(3) + 1; n > 0; n-- {
					idx := d.changeIndex.Add(1)
					w.allocated = append(w.allocated, idx)
					uncommitted[idx] = true
				}
			default:
				i := rng.Intn(len(live))
				w := live[i]
				// Commit or abandon: either way the sequences are settled
				// before the reservation is released.
				for _, idx := range w.allocated {
					delete(uncommitted, idx)
				}
				w.release()
				w.release() // idempotent
				live = append(live[:i], live[i+1:]...)
			}
			wm := d.nativeStreamWatermark()
			if wm < last {
				t.Fatalf("seed %d step %d: watermark regressed %d -> %d", seed, step, last, wm)
			}
			last = wm
			for idx := range uncommitted {
				if idx <= wm {
					t.Fatalf("seed %d step %d: watermark %d covers uncommitted native sequence %d", seed, step, wm, idx)
				}
			}
		}
	}
}

// jitterReplicator commits through ApplyCommittedBatch after a random delay,
// so concurrent writers commit out of allocation order.
type jitterReplicator struct {
	db  *DB
	mu  sync.Mutex
	rng *rand.Rand
}

func (*jitterReplicator) IsLeader() bool              { return true }
func (*jitterReplicator) LeaderHTTPAddr() string      { return "" }
func (*jitterReplicator) Barrier(time.Duration) error { return nil }
func (r *jitterReplicator) Replicate(raw []byte) error {
	r.mu.Lock()
	delay := time.Duration(r.rng.Intn(1500)) * time.Microsecond
	r.mu.Unlock()
	time.Sleep(delay)
	return r.db.ApplyCommittedBatch(raw)
}

// TestNativeStreamConcurrentCursorsNeverSkipAllocatedUncommitted runs
// concurrent native writers, a concurrent writer on another stream table
// (whose later sequences commit first), and concurrent cursors. Every
// cursor must eventually observe every native record exactly once: a cursor
// that advanced past an allocated-but-uncommitted native sequence would miss
// it forever.
func TestNativeStreamConcurrentCursorsNeverSkipAllocatedUncommitted(t *testing.T) {
	for _, mode := range []string{"local", "jitter"} {
		t.Run(mode, func(t *testing.T) {
			db, td := nativeStreamDB(t)
			if mode == "jitter" {
				db.AttachReplicator(&jitterReplicator{db: db, rng: rand.New(rand.NewSource(7))})
			}
			other := streamTestTableWithView(types.StreamViewTypeNewImage)
			const writers, perWriter, readers = 8, 40, 4
			expiry := time.Now().Add(time.Hour).Unix()
			var wg sync.WaitGroup
			done := make(chan struct{})
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						item := textEvent("t", fmt.Sprintf("%d-%d", w, i), "payload", expiry)
						var err error
						if i%5 == 4 {
							err = db.BatchWriteItem(td, []BatchOp{{Op: BatchOpPut, Item: item}, {Op: BatchOpPut, Item: textEvent("t", fmt.Sprintf("%d-%d-b", w, i), "payload", expiry)}})
						} else {
							err = db.PutItemWith(td, item, PutOptions{})
						}
						if err != nil {
							t.Error(err)
							return
						}
					}
				}(w)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < writers*perWriter/2; i++ {
					if err := db.PutItemWith(other, types.Item{"id": streamS(fmt.Sprint(i))}, PutOptions{}); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			type cursorResult struct {
				seen map[uint64]int
				err  error
			}
			results := make([]cursorResult, readers)
			var rwg sync.WaitGroup
			for r := 0; r < readers; r++ {
				rwg.Add(1)
				go func(r int) {
					defer rwg.Done()
					seen := map[uint64]int{}
					next := uint64(1)
					drain := func() error {
						for {
							recs, n, err := db.StreamRecordsCtx(context.Background(), td.Name, next, 0, 7, 1<<20)
							if err != nil {
								return err
							}
							for _, rec := range recs {
								seen[rec.Index]++
							}
							progressed := n != next
							next = n
							if !progressed {
								return nil
							}
						}
					}
					for {
						select {
						case <-done:
							results[r] = cursorResult{seen: seen, err: drain()}
							return
						default:
						}
						if err := drain(); err != nil {
							results[r] = cursorResult{err: err}
							return
						}
					}
				}(r)
			}
			wg.Wait()
			close(done)
			rwg.Wait()

			want := map[uint64]bool{}
			all, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range all {
				want[rec.Index] = true
			}
			if len(want) != writers*perWriter+writers*perWriter/5 {
				t.Fatalf("committed native records = %d", len(want))
			}
			for r, res := range results {
				if res.err != nil {
					t.Fatalf("reader %d: %v", r, res.err)
				}
				for idx := range want {
					if res.seen[idx] != 1 {
						t.Fatalf("reader %d observed native sequence %d %d times (skipped or duplicated)", r, idx, res.seen[idx])
					}
				}
				if len(res.seen) != len(want) {
					t.Fatalf("reader %d observed %d records, want %d", r, len(res.seen), len(want))
				}
			}
			if s := db.StreamFenceStats(); s.InFlight != 0 || s.Watermark != s.Allocated {
				t.Fatalf("reservations leaked: %+v", s)
			}
		})
	}
}

// TestNativeStreamReadersAndWritersDoNotQueueBehindSlowCommit holds one
// native commit open: other native writers still commit, and stream readers
// return promptly with the cursor parked below the uncommitted sequence.
func TestNativeStreamReadersAndWritersDoNotQueueBehindSlowCommit(t *testing.T) {
	db, td := nativeStreamDB(t)
	r := &blockedNativeCommit{db: db, first: make(chan struct{}), release: make(chan struct{})}
	db.AttachReplicator(r)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(r.release) }) }
	// Registered after nativeStreamDB's Close cleanup, so it runs first and a
	// failed assertion cannot leave the writer (and Close) blocked.
	t.Cleanup(release)
	expiry := time.Now().Add(time.Hour).Unix()
	firstDone := make(chan error, 1)
	go func() { firstDone <- db.PutItemWith(td, textEvent("a", "slow", "first", expiry), PutOptions{}) }()
	<-r.first
	// Writers on other keys are not serialized behind the slow commit. Keys
	// sharing the slow key's primary-key lock shard would legitimately wait,
	// so pick keys on other shards.
	shard := func(item types.Item) uint64 {
		pk, sk, err := extractKeyBytes(item, td.KeySchema)
		if err != nil {
			t.Fatal(err)
		}
		return atomicLockIndex(storage.KeyPrimary(td.Name, pk, sk))
	}
	slowShard := shard(textEvent("a", "slow", "", 0))
	var others []types.Item
	for i := 0; len(others) < 5; i++ {
		if item := textEvent("a", fmt.Sprint(i), "later", expiry); shard(item) != slowShard {
			others = append(others, item)
		}
	}
	for _, item := range others {
		start := time.Now()
		if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("native writer queued behind a slow commit")
		}
	}
	// Readers return promptly and expose nothing past the slow sequence.
	start := time.Now()
	recs, next, err := db.StreamRecordsCtx(context.Background(), td.Name, 1, 0, 100, 1<<20)
	if err != nil || len(recs) != 0 || next != 1 {
		t.Fatalf("reader crossed the uncommitted sequence: recs=%d next=%d err=%v", len(recs), next, err)
	}
	committed, err := db.CommittedChangeIndex()
	if err != nil || committed != 0 {
		t.Fatalf("committed index crossed the uncommitted sequence: %d %v", committed, err)
	}
	if after, _ := db.changeRecordsAfter(td.Name, 0, 0, 0); len(after) != 0 {
		t.Fatalf("changeRecordsAfter crossed the uncommitted sequence: %d", len(after))
	}
	if time.Since(start) > time.Second {
		t.Fatal("reader waited for a writer")
	}
	if s := db.StreamFenceStats(); s.InFlight != 1 || s.Watermark != 0 || s.Allocated != 6 {
		t.Fatalf("fence stats = %+v", s)
	}
	release()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	recs, next, err = db.StreamRecordsCtx(context.Background(), td.Name, next, 0, 100, 1<<20)
	if err != nil || len(recs) != 6 || next != 7 {
		t.Fatalf("after release: recs=%d next=%d err=%v", len(recs), next, err)
	}
	if committed, err := db.CommittedChangeIndex(); err != nil || committed != 6 {
		t.Fatalf("committed index after release = %d %v", committed, err)
	}
}

// slowBarrier blocks Barrier until released, like a Raft barrier waiting
// behind a slow fsync.
type slowBarrier struct {
	db      *DB
	release chan struct{}
	calls   atomic.Int32
}

func (*slowBarrier) IsLeader() bool         { return true }
func (*slowBarrier) LeaderHTTPAddr() string { return "" }
func (b *slowBarrier) Barrier(timeout time.Duration) error {
	b.calls.Add(1)
	select {
	case <-b.release:
		return nil
	case <-time.After(timeout):
		return errors.New("timed out enqueuing operation")
	}
}
func (b *slowBarrier) Replicate(raw []byte) error { return b.db.ApplyCommittedBatch(raw) }

func TestNativeStreamReadHonoursContextWhileBarrierIsSlow(t *testing.T) {
	db, td := nativeStreamDB(t)
	if err := db.PutItemWith(td, textEvent("a", "1", "x", time.Now().Add(time.Hour).Unix()), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	b := &slowBarrier{db: db, release: make(chan struct{})}
	db.AttachReplicator(b)
	defer close(b.release)
	reads := map[string]func(context.Context) error{
		"StreamRecords": func(ctx context.Context) error {
			_, _, err := db.StreamRecordsCtx(ctx, td.Name, 1, 0, 10, 1<<20)
			return err
		},
		"CommittedChangeIndex": func(ctx context.Context) error {
			_, err := db.CommittedChangeIndexCtx(ctx)
			return err
		},
		"ReadBarrier": func(ctx context.Context) error { return db.ReadBarrierCtx(ctx, 5*time.Second) },
	}
	for name, read := range reads {
		t.Run(name+"/deadline", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := read(ctx)
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("read ignored its deadline: %v", elapsed)
			}
			if !errors.Is(err, ErrReadUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, want ErrReadUnavailable wrapping DeadlineExceeded", err)
			}
		})
		t.Run(name+"/cancel", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			go func() { time.Sleep(20 * time.Millisecond); cancel() }()
			start := time.Now()
			err := read(ctx)
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("read ignored cancellation: %v", elapsed)
			}
			if !errors.Is(err, ErrReadUnavailable) || !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want ErrReadUnavailable wrapping Canceled", err)
			}
		})
	}
	if _, _, err := db.StreamRecordsCtx(canceledContext(), td.Name, 1, 0, 10, 1<<20); !errors.Is(err, ErrReadUnavailable) {
		t.Fatalf("already-cancelled read = %v", err)
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
