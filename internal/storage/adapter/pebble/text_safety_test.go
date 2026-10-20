package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestNativeTextOverflowShortUnicodeAndBounds(t *testing.T) {
	db, td := textDB(t)
	expiry := time.Now().Add(time.Hour).Unix()
	for _, entry := range []struct{ id, message string }{{"1", "needle éáã"}, {"2", strings.Repeat("large ", 4000) + " NEEDLE éáã"}, {"3", "needle éáã"}} {
		if err := db.PutItemWith(td, textEvent("a", entry.id, entry.message, expiry), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, descending := range []bool{false, true} {
		q := types.TextQuery{IndexName: "message", PKValue: types.AttributeValue{T: types.AttrS, S: "a"}, Search: "NEEDLE", Limit: 1, Descending: descending}
		want := []string{"1", "2", "3"}
		if descending {
			want = []string{"3", "2", "1"}
		}
		for i, id := range want {
			page, err := db.TextQuery(context.Background(), td, q)
			if err != nil || len(page.Items) != 1 || page.Items[0]["orderKey"].S != id {
				t.Fatalf("merged iterator page %d: rows=%d err=%v", i, len(page.Items), err)
			}
			q.Cursor = page.NextCursor
			if (i < 2) != (q.Cursor != "") {
				t.Fatalf("page %d cursor mismatch", i)
			}
		}
	}
	for _, search := range []string{"é", "ÉÁÃ", ""} {
		q := types.TextQuery{IndexName: "message", PKValue: types.AttributeValue{T: types.AttrS, S: "a"}, Search: search, SKLow: types.AttributeValue{T: types.AttrS, S: "2"}, SKHigh: types.AttributeValue{T: types.AttrS, S: "3"}, MaxValues: map[string]types.AttributeValue{"fields.attempt": {T: types.AttrN, N: "2"}}}
		page, err := db.TextQuery(context.Background(), td, q)
		if err != nil || len(page.Items) != 1 || page.Items[0]["orderKey"].S != "2" {
			t.Fatalf("short/unicode/range search: rows=%d err=%v", len(page.Items), err)
		}
		q.MaxValues["fields.attempt"] = types.AttributeValue{T: types.AttrN, N: "1.99"}
		page, err = db.TextQuery(context.Background(), td, q)
		if err != nil || len(page.Items) != 0 {
			t.Fatalf("scalar maximum: %v", err)
		}
	}
	q := types.TextQuery{IndexName: "message", PKValue: types.AttributeValue{T: types.AttrS, S: "a"}, Search: "needle"}
	agg, err := db.TextAggregate(context.Background(), td, types.TextAggregate{TextQuery: q, IntervalSeconds: 60})
	if err != nil || len(agg.Buckets) != 1 || agg.Buckets[0].Count != 3 {
		t.Fatalf("overflow aggregate: %+v %v", agg, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.TextQuery(ctx, td, q); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query: %v", err)
	}
	q.Limit = 1001
	if _, err := db.TextQuery(context.Background(), td, q); !errors.Is(err, ErrTextQueryInvalid) {
		t.Fatalf("unbounded query accepted: %v", err)
	}
}

type textReplication struct {
	primary, replica      *DB
	leader                bool
	failBefore, failAfter bool
	batches               int
}

func (r *textReplication) IsLeader() bool            { return r.leader }
func (*textReplication) LeaderHTTPAddr() string      { return "" }
func (*textReplication) Barrier(time.Duration) error { return nil }
func (r *textReplication) Replicate(raw []byte) error {
	if r.failBefore {
		return errors.New("fixture unavailable")
	}
	if err := r.primary.ApplyCommittedBatch(raw); err != nil {
		return err
	}
	if err := r.replica.ApplyCommittedBatch(raw); err != nil {
		return err
	}
	r.batches++
	if r.failAfter {
		return errors.New("fixture lost acknowledgement")
	}
	return nil
}

func TestNativeTextReplicatedBatchLostACKAndExpiry(t *testing.T) {
	primary, td := textDB(t)
	replica, err := Open(Options{Path: t.TempDir(), FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	repl := &textReplication{primary: primary, replica: replica, leader: true}
	primary.AttachReplicator(repl)
	raw, _ := json.Marshal(td)
	if err := primary.Set(storage.KeyCatalog(td.Name), raw); err != nil {
		t.Fatal(err)
	}
	item := textEvent("a", "1", "durable needle", time.Now().Add(time.Hour).Unix())
	opts := PutOptions{Condition: "attribute_not_exists(tenantId)"}
	repl.failAfter = true
	if err := primary.PutItemWith(td, item, opts); err == nil {
		t.Fatal("lost ACK was not surfaced")
	}
	repl.failAfter = false
	batches := repl.batches
	if err := primary.PutItemWith(td, item, opts); !errors.Is(err, storage.ErrConditionFailed) {
		t.Fatalf("replay overwrote committed row: %v", err)
	}
	if repl.batches != batches {
		t.Fatal("duplicate replay mutated expiration or index")
	}
	q := types.TextQuery{IndexName: "message", PKValue: item["tenantId"], Search: "needle"}
	page, err := replica.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 1 || page.Items[0]["expiresAt"].N != item["expiresAt"].N {
		t.Fatalf("replicated row/index missing: %v", err)
	}
	repl.failBefore = true
	changed := textEvent("a", "1", "uncommitted replacement", time.Now().Add(time.Hour).Unix())
	if err := primary.PutItemWith(td, changed, PutOptions{}); err == nil {
		t.Fatal("failed commit not reported")
	}
	repl.failBefore = false
	page, err = replica.TextQuery(context.Background(), td, q)
	if err != nil || len(page.Items) != 1 {
		t.Fatal("failed mutation changed replica index")
	}
	item["expiresAt"] = types.AttributeValue{T: types.AttrN, N: "1"}
	if err := primary.PutItemWith(td, item, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	repl.leader = false
	r := NewReaper(primary, retentionCatalog{td}, repl, ReaperConfig{})
	if err := r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if primary.TTLStatus().Deleted != 0 || primary.TTLStatus().Leader {
		t.Fatal("follower deleted TTL")
	}
	repl.leader = true
	if err := r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, db := range []*DB{primary, replica} {
		low, high := storage.PrefixTable(td.Name)
		it, err := db.Iter(low, high)
		if err != nil {
			t.Fatal(err)
		}
		if it.First() {
			_ = it.Close()
			t.Fatal("replicated expiry left primary/TTL/text data")
		}
		if err := it.Error(); err != nil {
			t.Fatal(err)
		}
		_ = it.Close()
	}
}

func TestRetentionBudgetAndFailuresAreObservable(t *testing.T) {
	db, td := textDB(t)
	for _, id := range []string{"1", "2", "3"} {
		if err := db.PutItemWith(td, textEvent("a", id, "expired", 1), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	r := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{BatchSize: 1})
	if err := r.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := db.TTLStatus(); status.Deleted != 1 || !status.BacklogPresent {
		t.Fatalf("bounded backlog missing: %+v", status)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Tick(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup cancellation: %v", err)
	}
	if status := db.TTLStatus(); status.Failures != 1 || status.Deleted != 1 {
		t.Fatalf("cleanup failure not recorded: %+v", status)
	}
	for i := 0; i < 2; i++ {
		if err := r.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if status := db.TTLStatus(); status.Deleted != 3 || status.BacklogPresent {
		t.Fatalf("backlog not drained: %+v", status)
	}
}
