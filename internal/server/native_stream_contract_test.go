package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/cluster"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nativeStreamHTTP(t *testing.T, shards int) (*http.ServeMux, types.TableDescriptor) {
	t.Helper()
	m, err := cluster.Open(context.Background(), cluster.Config{Root: t.TempDir(), Shards: shards, SelfID: "n1", FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	sh, _ := m.Shard(0)
	cat, err := catalog.New(sh.Storage)
	if err != nil {
		t.Fatal(err)
	}
	s := New(sh.Storage, cat)
	s.AttachManager(m)
	mux := http.NewServeMux()
	s.Routes(mux)
	w := nativeStreamPost(t, mux, "/v1/tables", map[string]any{"name": "logs-events", "keySchema": map[string]string{"pk": "tenantId", "sk": "orderKey"}, "ttlAttribute": "expiresAt", "textIndexes": []map[string]string{{"name": "message", "field": "message"}}, "streamSpecification": map[string]any{"streamEnabled": true, "streamViewType": "NEW_IMAGE", "retentionSeconds": 86400}})
	if w.Code != http.StatusCreated {
		t.Fatalf("bootstrap status %d", w.Code)
	}
	td, err := cat.Describe("logs-events")
	if err != nil {
		t.Fatal(err)
	}
	return mux, td
}

func nativeStreamPost(t *testing.T, mux *http.ServeMux, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw)))
	return w
}

func TestNativeStreamHTTPCommittedCheckpointAndFreshZero(t *testing.T) {
	mux, td := nativeStreamHTTP(t, 1)
	w := nativeStreamPost(t, mux, "/v1/DescribeStream", map[string]any{"streamArn": td.LatestStreamArn})
	var desc struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"shardId"`
				Latest  string `json:"latestSequenceNumber"`
			} `json:"shards"`
		} `json:"streamDescription"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &desc); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || len(desc.StreamDescription.Shards) != 1 || desc.StreamDescription.Shards[0].Latest != "0" {
		t.Fatalf("missing real empty committed checkpoint: status %d %+v", w.Code, desc)
	}
	w = nativeStreamPost(t, mux, "/v1/GetShardIterator", map[string]any{"streamArn": td.LatestStreamArn, "shardId": desc.StreamDescription.Shards[0].ShardID, "shardIteratorType": "AFTER_SEQUENCE_NUMBER", "sequenceNumber": "0"})
	if w.Code != http.StatusOK {
		t.Fatalf("fresh AFTER0 refused: %d", w.Code)
	}
	var iterator struct {
		ShardIterator string `json:"shardIterator"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &iterator); err != nil {
		t.Fatal(err)
	}
	w = nativeStreamPost(t, mux, "/v1/GetRecords", map[string]any{"shardIterator": iterator.ShardIterator})
	var page struct {
		Last string `json:"lastScannedSequenceNumber"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || page.Last != "0" {
		t.Fatalf("missing compact next checkpoint: status %d, checkpoint %q", w.Code, page.Last)
	}
}

func TestNativeStreamHTTPRefusesUnsupportedMultipleDataStores(t *testing.T) {
	mux, td := nativeStreamHTTP(t, 2)
	w := nativeStreamPost(t, mux, "/v1/DescribeStream", map[string]any{"streamArn": td.LatestStreamArn})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsupported topology returned successful partial stream: %d", w.Code)
	}
	w = nativeStreamPost(t, mux, "/v1/GetShardIterator", map[string]any{"streamArn": td.LatestStreamArn, "shardId": "shardId-000000000000", "shardIteratorType": "TRIM_HORIZON"})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsupported topology minted a partial iterator: %d", w.Code)
	}
	w = nativeStreamPost(t, mux, "/v1/GetRecords", map[string]any{"shardIterator": "unused-on-unsupported-topology"})
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsupported topology consumed a partial stream: %d", w.Code)
	}
}

type nativeStreamFollower struct{}

func (nativeStreamFollower) IsLeader() bool              { return false }
func (nativeStreamFollower) LeaderHTTPAddr() string      { return "https://trusted-cefas.example" }
func (nativeStreamFollower) Replicate([]byte) error      { return errors.New("fixture cannot write") }
func (nativeStreamFollower) Barrier(time.Duration) error { return nil }

func TestNativeStreamCursorCannotAuthorizeFollowerReads(t *testing.T) {
	srv, db, cat, stop := newStreamIteratorTestServer(t)
	defer stop()
	arn := createIteratorStream(t, srv)
	desc, err := srv.DescribeStream(context.Background(), &cefaspb.DescribeStreamRequest{StreamArn: arn})
	if err != nil || desc.GetStreamDescription().GetShards()[0].GetLatestSequenceNumber() != "0" {
		t.Fatalf("gRPC empty committed checkpoint: %v", err)
	}
	iter, err := srv.GetShardIterator(context.Background(), &cefaspb.GetShardIteratorRequest{StreamArn: arn, ShardId: "shardId-000000000000", ShardIteratorType: "AFTER_SEQUENCE_NUMBER", SequenceNumber: "0"})
	if err != nil {
		t.Fatal(err)
	}
	page, err := srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{ShardIterator: iter.GetShardIterator()})
	if err != nil || page.GetLastScannedSequenceNumber() != "0" {
		t.Fatalf("gRPC empty compact checkpoint: %v", err)
	}
	db.AttachReplicator(nativeStreamFollower{})
	_, err = srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{ShardIterator: iter.GetShardIterator()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("valid cursor authorized follower response: %v", err)
	}
	if _, err := db.CommittedChangeIndex(); !errors.Is(err, pebble.ErrNotLeader) {
		t.Fatalf("follower checkpoint passed: %v", err)
	}
	mux := http.NewServeMux()
	New(db, cat).Routes(mux)
	w := nativeStreamPost(t, mux, "/v1/GetRecords", map[string]any{"shardIterator": iter.GetShardIterator()})
	if w.Code != http.StatusTemporaryRedirect || w.Header().Get("Location") != "https://trusted-cefas.example/v1/GetRecords" {
		t.Fatalf("follower did not direct trusted-client retry: %d", w.Code)
	}
}
