package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestHTTPNativeTextStorageRoundTrip(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir(), FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	server.New(db, cat).Routes(mux)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	c, err := client.NewHTTPClient(httpServer.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	td := types.TableDescriptor{Name: "logs-events", KeySchema: types.KeySchema{PK: "tenantId", SK: "orderKey"}, TTLAttribute: "expiresAt", TextIndexes: []types.TextIndexDescriptor{{Name: "message", Field: "message"}}}
	td.GSIs = []types.GSIDescriptor{{Name: "accepted-time", KeySchema: types.KeySchema{PK: "tenantId", SK: "acceptedKey"}, Projection: types.IndexProjection{Mode: "KEYS_ONLY"}}}
	created, err := c.CreateTable(ctx, td)
	if err != nil {
		t.Fatal(err)
	}
	if created.TTLAttribute != "expiresAt" || len(created.TextIndexes) != 1 {
		t.Fatal("schema was lost")
	}
	described, err := c.DescribeTable(ctx, td.Name)
	if err != nil || described.TextIndexes[0].Field != "message" {
		t.Fatalf("describe: %v", err)
	}
	if _, err := c.UpdateTimeToLive(ctx, td.Name, "expiresAt", true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateTimeToLive(ctx, td.Name, "otherExpiry", true); err == nil {
		t.Fatal("conflicting expiry accepted")
	}
	item := types.Item{"tenantId": {T: types.AttrS, S: "a"}, "orderKey": {T: types.AttrS, S: "2026-10-19T12:00:00.000000000Z#1"}, "message": {T: types.AttrS, S: "Raw connection failure"}, "timestamp": {T: types.AttrS, S: "2026-10-19T12:00:00Z"}, "level": {T: types.AttrS, S: "error"}, "expiresAt": {T: types.AttrN, N: fmt.Sprint(time.Now().Add(time.Hour).Unix())}, "fields": {T: types.AttrM, M: map[string]types.AttributeValue{"attempt": {T: types.AttrN, N: "2"}}}}
	item["acceptedKey"] = types.AttributeValue{T: types.AttrS, S: "2026-10-19T12:00:01.000000000Z#1"}
	item["payloadHash"] = types.AttributeValue{T: types.AttrS, S: "fixture-hash"}
	opts := client.PutOptions{Condition: "attribute_not_exists(tenantId)"}
	if err := c.PutItem(ctx, td.Name, item, opts); err != nil {
		t.Fatal(err)
	}
	err = c.PutItem(ctx, td.Name, item, opts)
	var condition *client.HTTPError
	if !errors.As(err, &condition) || condition.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("conditional retry: %v", err)
	}
	got, err := c.GetItem(ctx, td.Name, item, true)
	if err != nil || got["message"].S != item["message"].S || got["fields"].M["attempt"].N != "2" {
		t.Fatalf("message/fields were not independent: %v", err)
	}
	q := types.TextQuery{Table: td.Name, IndexName: "message", PKValue: item["tenantId"], Search: "FAILURE", Fields: map[string]types.AttributeValue{"fields.attempt": {T: types.AttrN, N: "2"}}, Consistency: "strong"}
	page, err := c.TextQuery(ctx, q)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("query: rows=%d err=%v", len(page.Items), err)
	}
	aggregate, err := c.TextAggregate(ctx, types.TextAggregate{TextQuery: q, IntervalSeconds: 60, GroupBy: "level"})
	if err != nil || len(aggregate.Buckets) != 1 || aggregate.Buckets[0].Errors != 1 {
		t.Fatalf("aggregate: %+v err=%v", aggregate, err)
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/Query", strings.NewReader(`{"table":"logs-events","indexName":"accepted-time","pkValue":{"S":"a"},"skHigh":{"S":"2026-10-19T12:01:00.000000000Z"},"limit":10}`)))
	var indexed struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &indexed); err != nil || response.Code != 200 || len(indexed.Items) != 1 {
		t.Fatalf("acceptance-time GSI bootstrap/query: status=%d rows=%d err=%v", response.Code, len(indexed.Items), err)
	}
	q.PKValue.S = "b"
	page, err = c.TextQuery(ctx, q)
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("foreign partition: rows=%d err=%v", len(page.Items), err)
	}
	deleteOpts := client.DeleteOptions{Condition: "tenantId = :tenant AND payloadHash = :hash", Binds: map[string]types.AttributeValue{"tenant": item["tenantId"], "hash": {T: types.AttrS, S: "foreign-hash"}}}
	if err := c.DeleteItem(ctx, td.Name, item, deleteOpts); err == nil {
		t.Fatal("conditional delete ignored immutable hash")
	}
	deleteOpts.Binds["hash"] = item["payloadHash"]
	if err := c.DeleteItem(ctx, td.Name, item, deleteOpts); err != nil {
		t.Fatal(err)
	}
	got, err = c.GetItem(ctx, td.Name, item, true)
	if err != nil || got != nil {
		t.Fatalf("conditional cleanup retained row: %v", err)
	}
}

func TestHTTPClientNeverFollowsCredentialRedirectsOrLogsBodies(t *testing.T) {
	var followed bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed = true; w.WriteHeader(200) }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
		_, _ = w.Write([]byte("sensitive fixture body"))
	}))
	defer origin.Close()
	c, err := client.NewHTTPClient(origin.URL, nil, func(context.Context) (string, error) { return "test-only-credential", nil })
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeTable(context.Background(), "logs")
	if followed || err == nil || err.Error() != "cefas HTTP status 307" {
		t.Fatalf("unsafe redirect/error behavior: followed=%t err=%v", followed, err)
	}
}
