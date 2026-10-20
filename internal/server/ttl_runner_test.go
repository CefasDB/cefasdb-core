package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestRuntimeTTLRunnerUsesCatalogAndStopsBeforeClose(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	td := types.TableDescriptor{Name: "logs", KeySchema: types.KeySchema{PK: "tenant", SK: "time"}, TTLAttribute: "expiresAt", TextIndexes: []types.TextIndexDescriptor{{Name: "message", Field: "message"}}}
	if err := cat.Create(td); err != nil {
		t.Fatal(err)
	}
	item := types.Item{"tenant": {T: types.AttrS, S: "a"}, "time": {T: types.AttrS, S: "1"}, "message": {T: types.AttrS, S: "expired event"}, "expiresAt": {T: types.AttrN, N: fmt.Sprint(time.Now().Add(-time.Hour).Unix())}}
	if err := db.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	stop := New(db, cat).StartTTLReapers(context.Background(), pebble.ReaperConfig{Interval: time.Millisecond * 5, BatchSize: 8})
	deadline := time.Now().Add(time.Second * 2)
	for db.TTLStatus().Deleted != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond * 5)
	}
	stop()
	status := db.TTLStatus()
	if !status.Enabled || !status.Leader || status.Deleted != 1 || status.LastSuccessAt.IsZero() || status.Failures != 0 {
		t.Fatalf("cleanup did not execute: %+v", status)
	}
	rows, err := db.TextQuery(context.Background(), td, types.TextQuery{IndexName: "message", PKValue: item["tenant"], Search: "expired"})
	if err != nil || len(rows.Items) != 0 {
		t.Fatalf("cleanup text result: %v", err)
	}
}
