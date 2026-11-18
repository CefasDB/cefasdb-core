package pebble

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// raftLogModel is a test Replicator with an explicit Raft log, unlike the
// fixtures whose Barrier is a no-op. Entries (data and barriers) are appended
// in call order and applied, in log order, only when the test commits them.
// It models the FU-01 review P2-1 interleaving:
//   - parkNext holds the next data entry before it is appended, so a writer
//     can allocate its sequence first and append behind a reader's barrier;
//   - loseLeadership fails every unapplied future with a leadership-lost
//     error while the entries stay in the log;
//   - elect re-establishes a leader that keeps (or truncates) the log tail,
//     and its first commit applies the abandoned entries, as a new Raft
//     leader commits earlier-term entries together with its first entry.
type raftLogModel struct {
	db *DB

	mu      sync.Mutex
	leader  bool
	auto    bool
	log     []*raftEntry
	applied int
	changed chan struct{}

	gate   chan struct{}
	parked chan struct{}

	afterBarrier func()
}

type raftEntry struct {
	data     []byte
	barrier  bool
	done     chan error
	resolved bool
}

var errModelLeadershipLost = fmt.Errorf("raft apply: leadership lost while committing log: %w", ErrNotLeader)

func newRaftLogModel(db *DB) *raftLogModel {
	return &raftLogModel{db: db, leader: true, changed: make(chan struct{})}
}

func (r *raftLogModel) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leader
}

func (*raftLogModel) LeaderHTTPAddr() string { return "" }

func (r *raftLogModel) Replicate(raw []byte) error {
	r.mu.Lock()
	gate := r.gate
	r.gate = nil
	r.mu.Unlock()
	if gate != nil {
		close(r.parked)
		<-gate
	}
	e, err := r.append(&raftEntry{data: append([]byte(nil), raw...)})
	if err != nil {
		return err
	}
	return <-e.done
}

func (r *raftLogModel) Barrier(timeout time.Duration) error {
	e, err := r.append(&raftEntry{barrier: true})
	if err != nil {
		return err
	}
	select {
	case err = <-e.done:
	case <-time.After(timeout):
		return errors.New("timed out waiting for barrier")
	}
	if err != nil {
		return err
	}
	r.mu.Lock()
	hook := r.afterBarrier
	r.afterBarrier = nil
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}

func (r *raftLogModel) append(e *raftEntry) (*raftEntry, error) {
	e.done = make(chan error, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.leader {
		return nil, ErrNotLeader
	}
	r.log = append(r.log, e)
	close(r.changed)
	r.changed = make(chan struct{})
	if r.auto {
		if err := r.commitLocked(len(r.log)); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// parkNext makes the next data Replicate wait, before it is appended, until
// the returned release is called. parked is closed once it is waiting.
func (r *raftLogModel) parkNext() (parked <-chan struct{}, release func()) {
	gate := make(chan struct{})
	r.mu.Lock()
	r.gate = gate
	r.parked = make(chan struct{})
	p := r.parked
	r.mu.Unlock()
	var once sync.Once
	return p, func() { once.Do(func() { close(gate) }) }
}

func (r *raftLogModel) waitLen(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		r.mu.Lock()
		got, ch := len(r.log), r.changed
		r.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("raft log length %d, want %d", got, n)
		}
	}
}

func (r *raftLogModel) commitThrough(t *testing.T, n int) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.commitLocked(n); err != nil {
		t.Fatal(err)
	}
}

func (r *raftLogModel) commitLocked(n int) error {
	for ; r.applied < n && r.applied < len(r.log); r.applied++ {
		e := r.log[r.applied]
		if !e.barrier {
			if err := r.db.ApplyCommittedBatch(e.data); err != nil {
				return err
			}
		}
		if !e.resolved {
			e.resolved = true
			e.done <- nil
		}
	}
	return nil
}

func (r *raftLogModel) loseLeadership() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.leader = false
	for _, e := range r.log[r.applied:] {
		if !e.resolved {
			e.resolved = true
			e.done <- errModelLeadershipLost
		}
	}
}

// elect makes this node leader again. keep is the log length the new term
// retains (entries after it were never replicated and are lost); -1 keeps
// the whole log. From now on every append commits immediately, which also
// commits the retained earlier-term entries in log order.
func (r *raftLogModel) elect(keep int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if keep >= 0 && keep < len(r.log) {
		r.log = r.log[:keep]
	}
	r.leader = true
	r.auto = true
}

func (r *raftLogModel) setAfterBarrier(fn func()) {
	r.mu.Lock()
	r.afterBarrier = fn
	r.mu.Unlock()
}

// rawChangeIndexes reads the changelog of table directly, without the read
// barrier or the watermark, so it works while the model has no leader.
func rawChangeIndexes(t *testing.T, db *DB, table string) []uint64 {
	t.Helper()
	lower, upper := storage.PrefixChangeLog()
	it, err := db.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out []uint64
	for ok := it.First(); ok; ok = it.Next() {
		rec, err := decodeChangeRecord(it.Value())
		if err != nil {
			t.Fatal(err)
		}
		if rec.Table == table {
			out = append(out, rec.Index)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func otherShardItem(t *testing.T, td types.TableDescriptor, avoid types.Item, prefix string, expiry int64) types.Item {
	t.Helper()
	shard := func(item types.Item) uint64 {
		pk, sk, err := extractKeyBytes(item, td.KeySchema)
		if err != nil {
			t.Fatal(err)
		}
		return atomicLockIndex(storage.KeyPrimary(td.Name, pk, sk))
	}
	for i := 0; ; i++ {
		item := textEvent("a", fmt.Sprintf("%s-%d", prefix, i), "second", expiry)
		if shard(item) != shard(avoid) {
			return item
		}
	}
}

// TestNativeStreamReadBoundAbandonedEntryAndLeaderChange is the FU-01
// review P2-1 regression. Log order: [W2(s+1), R-barrier, W(s)]. W2 and the
// barrier commit; the leader then loses leadership before the reader
// continues, so W's future fails and W releases its reservation; finally a
// new leader commits W's entry at s (or truncates it). A reader that took
// its watermark after the barrier would expose s+1 without s and never see s.
// The watermark must be read before the barrier.
func TestNativeStreamReadBoundAbandonedEntryAndLeaderChange(t *testing.T) {
	readers := []string{"StreamRecords", "changeRecordsAfter", "CommittedChangeIndex"}
	fates := []string{"committed-by-next-leader", "truncated-by-next-leader"}
	for _, reader := range readers {
		for _, fate := range fates {
			t.Run(reader+"/"+fate, func(t *testing.T) {
				db, td := nativeStreamDB(t)
				r := newRaftLogModel(db)
				db.AttachReplicator(r)
				expiry := time.Now().Add(time.Hour).Unix()
				slow := textEvent("a", "slow", "first", expiry)
				other := otherShardItem(t, td, slow, "other", expiry)

				// W allocates sequence 1 and parks before its entry is appended.
				parked, releaseW := r.parkNext()
				t.Cleanup(releaseW)
				wDone := make(chan error, 1)
				go func() { wDone <- db.PutItemWith(td, slow, PutOptions{}) }()
				<-parked
				// W2 allocates sequence 2; its entry is log index 0.
				w2Done := make(chan error, 1)
				go func() { w2Done <- db.PutItemWith(td, other, PutOptions{}) }()
				r.waitLen(t, 1)

				var wErr, w2Err error
				r.setAfterBarrier(func() {
					// Leadership is lost after the barrier applied and
					// before the reader continues: W's future fails and both
					// writers release their reservations.
					r.loseLeadership()
					wErr, w2Err = <-wDone, <-w2Done
				})

				type readResult struct {
					recs      []ChangeRecord
					next      uint64
					committed uint64
					err       error
				}
				readDone := make(chan readResult, 1)
				go func() {
					var res readResult
					switch reader {
					case "StreamRecords":
						res.recs, res.next, res.err = db.StreamRecordsCtx(context.Background(), td.Name, 1, 0, 100, 1<<20)
					case "changeRecordsAfter":
						res.recs, res.err = db.changeRecordsAfterCtx(context.Background(), td.Name, 0, 0, 0)
						res.next = 1
						for _, rec := range res.recs {
							res.next = rec.Index + 1
						}
					case "CommittedChangeIndex":
						res.committed, res.err = db.CommittedChangeIndexCtx(context.Background())
						res.next = res.committed + 1
					}
					readDone <- res
				}()
				// The reader's barrier is log index 1; W's entry then lands
				// behind it at index 2.
				r.waitLen(t, 2)
				releaseW()
				r.waitLen(t, 3)
				r.commitThrough(t, 2)

				var res readResult
				select {
				case res = <-readDone:
				case <-time.After(5 * time.Second):
					t.Fatal("reader did not return")
				}
				if res.err != nil {
					t.Fatalf("read: %v", res.err)
				}
				if !errors.Is(wErr, ErrNotLeader) || w2Err != nil {
					t.Fatalf("writers: abandoned=%v committed=%v", wErr, w2Err)
				}
				if s := db.StreamFenceStats(); s.InFlight != 0 {
					t.Fatalf("reservations leaked: %+v", s)
				}
				committedAtRead := map[uint64]bool{}
				for _, idx := range rawChangeIndexes(t, db, td.Name) {
					committedAtRead[idx] = true
				}

				// A new leader commits (or drops) the abandoned entry.
				if fate == "committed-by-next-leader" {
					r.elect(-1)
				} else {
					r.elect(2)
				}
				seen := map[uint64]int{}
				for _, rec := range res.recs {
					seen[rec.Index]++
				}
				next := res.next
				for {
					recs, n, err := db.StreamRecordsCtx(context.Background(), td.Name, next, 0, 100, 1<<20)
					if err != nil {
						t.Fatal(err)
					}
					for _, rec := range recs {
						seen[rec.Index]++
					}
					if n == next {
						break
					}
					next = n
				}
				final := rawChangeIndexes(t, db, td.Name)
				want := 2
				if fate == "truncated-by-next-leader" {
					want = 1
				}
				if len(final) != want {
					t.Fatalf("committed native records = %v, want %d", final, want)
				}
				for _, idx := range final {
					if reader == "CommittedChangeIndex" {
						// A LATEST iterator starts after the committed index;
						// nothing committed later may sit at or below it.
						if idx <= res.committed && !committedAtRead[idx] {
							t.Fatalf("committed index %d covers sequence %d committed afterwards by the next leader", res.committed, idx)
						}
						if idx <= res.committed {
							continue
						}
					}
					if seen[idx] != 1 {
						t.Fatalf("cursor observed native sequence %d %d times (first read next=%d, final next=%d)", idx, seen[idx], res.next, next)
					}
				}
			})
		}
	}
}

// TestNativeStreamReadBoundWatermarkPrecedesBarrier pins the order directly:
// a reservation released while the barrier runs must not raise the bound.
func TestNativeStreamReadBoundWatermarkPrecedesBarrier(t *testing.T) {
	db, td := nativeStreamDB(t)
	r := newRaftLogModel(db)
	r.auto = true
	db.AttachReplicator(r)
	release := db.beginNativeStreamWrite(td)
	db.changeIndex.Add(1)
	r.setAfterBarrier(release)
	bound, fenced, err := db.nativeStreamReadBound(context.Background(), td.Name)
	if err != nil || !fenced {
		t.Fatalf("read bound: fenced=%v err=%v", fenced, err)
	}
	if bound != 0 {
		t.Fatalf("bound %d includes a sequence released during the barrier", bound)
	}
	if wm := db.nativeStreamWatermark(); wm != 1 {
		t.Fatalf("watermark after release = %d", wm)
	}
}
