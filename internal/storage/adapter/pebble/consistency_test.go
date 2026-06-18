package pebble

import (
	"errors"
	"testing"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/CefasDb/cefasdb/pkg/types"
)

type consistencyTestReplicator struct {
	db         *DB
	leader     bool
	syncCalls  int
	asyncCalls int
}

func (r *consistencyTestReplicator) IsLeader() bool { return r.leader }

func (r *consistencyTestReplicator) Replicate(repr []byte) error {
	r.syncCalls++
	return applyReprForConsistencyTest(r.db, repr)
}

func (r *consistencyTestReplicator) ReplicateAsync(repr []byte) error {
	r.asyncCalls++
	return nil
}

func (r *consistencyTestReplicator) LeaderHTTPAddr() string { return "http://leader" }

func applyReprForConsistencyTest(db *DB, repr []byte) error {
	b := db.Raw().NewBatch()
	defer b.Close()
	if err := b.SetRepr(append([]byte(nil), repr...)); err != nil {
		return err
	}
	return b.Commit(pebbledb.NoSync)
}

func consistencySAttr(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func TestWriteConsistencyOneCommitsLocallyAndReplicatesAsync(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir(), WriteConsistency: "one"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repl := &consistencyTestReplicator{db: db, leader: true}
	db.AttachReplicator(repl)

	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	item := types.Item{"id": consistencySAttr("1"), "v": consistencySAttr("local")}
	if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
		t.Fatalf("PutItemWith: %v", err)
	}

	got, err := db.GetItem("events", td.KeySchema, types.Item{"id": consistencySAttr("1")})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got["v"].S != "local" {
		t.Fatalf("value = %q, want local", got["v"].S)
	}
	if repl.syncCalls != 0 {
		t.Fatalf("sync replication calls = %d, want 0", repl.syncCalls)
	}
	if repl.asyncCalls != 1 {
		t.Fatalf("async replication calls = %d, want 1", repl.asyncCalls)
	}
}

func TestWriteConsistencyQuorumUsesSynchronousReplicator(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir(), WriteConsistency: "quorum"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repl := &consistencyTestReplicator{db: db, leader: true}
	db.AttachReplicator(repl)

	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	if err := db.PutItemWith(td, types.Item{"id": consistencySAttr("1"), "v": consistencySAttr("strong")}, PutOptions{}); err != nil {
		t.Fatalf("PutItemWith: %v", err)
	}
	if repl.syncCalls != 1 {
		t.Fatalf("sync replication calls = %d, want 1", repl.syncCalls)
	}
	if repl.asyncCalls != 0 {
		t.Fatalf("async replication calls = %d, want 0", repl.asyncCalls)
	}
	got, err := db.GetItem("events", td.KeySchema, types.Item{"id": consistencySAttr("1")})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got["v"].S != "strong" {
		t.Fatalf("value = %q, want strong", got["v"].S)
	}
}

func TestWriteConsistencyOneStillRejectsFollowerWrites(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir(), WriteConsistency: "one"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repl := &consistencyTestReplicator{db: db, leader: false}
	db.AttachReplicator(repl)

	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	err = db.PutItemWith(td, types.Item{"id": consistencySAttr("1")}, PutOptions{})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v, want ErrNotLeader", err)
	}
	if repl.syncCalls != 0 || repl.asyncCalls != 0 {
		t.Fatalf("replication calls sync=%d async=%d, want 0/0", repl.syncCalls, repl.asyncCalls)
	}
}
