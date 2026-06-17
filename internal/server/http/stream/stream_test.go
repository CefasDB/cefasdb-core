package stream_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	streamhttp "github.com/CefasDb/cefasdb/internal/server/http/stream"
	"github.com/CefasDb/cefasdb/internal/server/streamcore"
	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// newHandlers spins up a tempdir-backed storage + catalog and returns
// a Handlers instance with no ChangeStream and no metric observers
// (the SSE-off and metrics-off paths are exercised separately).
func newHandlers(t *testing.T) (*streamhttp.Handlers, *pebble.DB, *catalog.Catalog, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	return streamhttp.New(cat, db, nil, nil, nil), db, cat, func() { _ = db.Close() }
}

func createEventsTable(t *testing.T, cat *catalog.Catalog) types.TableDescriptor {
	t.Helper()
	if err := cat.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
	}); err != nil {
		t.Fatalf("create events: %v", err)
	}
	td, err := cat.Describe("Events")
	if err != nil {
		t.Fatalf("describe events: %v", err)
	}
	return td
}

func TestHandleListStreamsReturnsDescriptors(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()

	td := createEventsTable(t, cat)

	body := bytes.NewBufferString(`{"tableName":"Events"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/ListStreams", body)
	rec := httptest.NewRecorder()
	h.HandleListStreams(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Streams []struct {
			StreamArn string `json:"streamArn"`
			TableName string `json:"tableName"`
		} `json:"streams"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Streams) != 1 ||
		resp.Streams[0].StreamArn != td.LatestStreamArn ||
		resp.Streams[0].TableName != "Events" {
		t.Fatalf("unexpected list response: %+v (want stream %q)", resp, td.LatestStreamArn)
	}
}

func TestHandleDescribeStreamUnknownARNReturns4xx(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	body := bytes.NewBufferString(`{"streamArn":"arn:does-not-exist"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/DescribeStream", body)
	rec := httptest.NewRecorder()
	h.HandleDescribeStream(rec, req)

	if rec.Code < 400 || rec.Code >= 500 {
		t.Fatalf("status = %d, want 4xx; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleGetShardIteratorHappyPath(t *testing.T) {
	t.Parallel()
	h, _, cat, cleanup := newHandlers(t)
	defer cleanup()

	td := createEventsTable(t, cat)

	body := bytes.NewBufferString(
		`{"streamArn":"` + td.LatestStreamArn +
			`","shardId":"` + types.StreamShardIDSingle +
			`","shardIteratorType":"TRIM_HORIZON"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/GetShardIterator", body)
	rec := httptest.NewRecorder()
	h.HandleGetShardIterator(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		ShardIterator string `json:"shardIterator"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ShardIterator == "" {
		t.Fatalf("empty shardIterator in response")
	}
}

func TestHandleGetRecordsHappyPath(t *testing.T) {
	t.Parallel()
	h, db, cat, cleanup := newHandlers(t)
	defer cleanup()

	td := createEventsTable(t, cat)

	if err := db.PutItemWith(td, types.Item{
		"pk": {T: types.AttrS, S: "event-1"},
		"v":  {T: types.AttrS, S: "payload"},
	}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put item: %v", err)
	}

	iterBody := bytes.NewBufferString(
		`{"streamArn":"` + td.LatestStreamArn +
			`","shardId":"` + types.StreamShardIDSingle +
			`","shardIteratorType":"TRIM_HORIZON"}`)
	iterReq := httptest.NewRequest(http.MethodPost, "/v1/GetShardIterator", iterBody)
	iterRec := httptest.NewRecorder()
	h.HandleGetShardIterator(iterRec, iterReq)
	if iterRec.Code != http.StatusOK {
		t.Fatalf("iterator status = %d body=%s", iterRec.Code, iterRec.Body.String())
	}
	var iterResp struct {
		ShardIterator string `json:"shardIterator"`
	}
	if err := json.NewDecoder(iterRec.Body).Decode(&iterResp); err != nil {
		t.Fatalf("decode iterator: %v", err)
	}

	body := bytes.NewBufferString(`{"shardIterator":"` + iterResp.ShardIterator + `","limit":10}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/GetRecords", body)
	rec := httptest.NewRecorder()
	h.HandleGetRecords(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Records []struct {
			EventName string `json:"eventName"`
		} `json:"records"`
		NextShardIterator string `json:"nextShardIterator"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Records) == 0 {
		t.Fatalf("expected at least 1 record, got 0; body=%s", rec.Body.String())
	}
	if resp.NextShardIterator == "" {
		t.Fatalf("expected non-empty nextShardIterator for open shard")
	}
}

func TestHandleGetRecordsInvalidIteratorReturns4xx(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	body := bytes.NewBufferString(`{"shardIterator":"not-a-real-iterator","limit":1}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/GetRecords", body)
	rec := httptest.NewRecorder()
	h.HandleGetRecords(rec, req)

	if rec.Code < 400 || rec.Code >= 500 {
		t.Fatalf("status = %d, want 4xx; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleStreamWithoutChangeStreamReturns4xx(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/Stream", nil)
	rec := httptest.NewRecorder()
	h.HandleStream(rec, req)

	if rec.Code < 400 || rec.Code >= 500 {
		t.Fatalf("status = %d, want 4xx; body=%s", rec.Code, rec.Body.String())
	}
}

// stubChangeStream lets TestHandleStream verify HandleStream forwards
// events as SSE data: lines while a ChangeStream is attached.
type stubChangeStream struct {
	events []streamcore.ChangeEvent
}

func (s stubChangeStream) SubscribeChanges(ctx context.Context) (<-chan streamcore.ChangeEvent, func()) {
	out := make(chan streamcore.ChangeEvent, len(s.events))
	for _, ev := range s.events {
		out <- ev
	}
	close(out)
	return out, func() {}
}

func TestHandleStreamForwardsEvents(t *testing.T) {
	t.Parallel()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	stream := stubChangeStream{events: []streamcore.ChangeEvent{
		{RaftIndex: 1, Op: "PUT", Key: []byte("k"), Value: []byte("v")},
	}}
	h := streamhttp.New(cat, db, stream, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/v1/Stream", nil)
	rec := httptest.NewRecorder()
	h.HandleStream(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"Op":"PUT"`)) {
		t.Fatalf("body missing PUT event: %s", rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}
}
