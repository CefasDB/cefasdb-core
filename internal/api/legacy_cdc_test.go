package api_test

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/internal/api"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

type legacyCDCStream struct {
	events []api.ChangeEvent
}

func (s legacyCDCStream) SubscribeChanges(ctx context.Context) (<-chan api.ChangeEvent, func()) {
	ch := make(chan api.ChangeEvent, len(s.events))
	for _, ev := range s.events {
		ch <- ev
	}
	close(ch)
	return ch, func() {}
}

func (s legacyCDCStream) ListSnapshots() ([]api.SnapshotMetadata, error) {
	return nil, nil
}

func TestLegacyHTTPChangeStreamStillEmitsSSEEvents(t *testing.T) {
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := api.New(db, catStore)
	srv.AttachChangeStream(legacyCDCStream{events: []api.ChangeEvent{
		{RaftIndex: 7, Op: "PUT", Key: []byte("cefas/table/Events/item/1"), Value: []byte(`{"pk":{"S":"1"}}`)},
	}})
	mux := http.NewServeMux()
	srv.Routes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/Stream", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", got)
	}

	scanner := bufio.NewScanner(strings.NewReader(rec.Body.String()))
	if !scanner.Scan() {
		t.Fatalf("missing SSE data line: %q", rec.Body.String())
	}
	line := scanner.Text()
	if !strings.HasPrefix(line, "data: ") {
		t.Fatalf("line = %q, want SSE data prefix", line)
	}
	var event struct {
		RaftIndex uint64 `json:"RaftIndex"`
		Op        string `json:"Op"`
		Key       string `json:"Key"`
		Value     string `json:"Value"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if event.RaftIndex != 7 || event.Op != "PUT" {
		t.Fatalf("event metadata = %+v", event)
	}
	key, err := base64.StdEncoding.DecodeString(event.Key)
	if err != nil {
		t.Fatalf("decode key: %v", err)
	}
	if string(key) != "cefas/table/Events/item/1" {
		t.Fatalf("key = %q", key)
	}
	value, err := base64.StdEncoding.DecodeString(event.Value)
	if err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if string(value) != `{"pk":{"S":"1"}}` {
		t.Fatalf("value = %q", value)
	}
}

func TestLegacyGRPCStreamChangesStillFiltersFromIndex(t *testing.T) {
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	handler := api.NewGRPCServer(db, catStore, nil)
	handler.AttachChangeStream(legacyCDCStream{events: []api.ChangeEvent{
		{RaftIndex: 1, Op: "PUT", Key: []byte("old"), Value: []byte("old-value")},
		{RaftIndex: 2, Op: "PUT", Key: []byte("current"), Value: []byte("current-value")},
		{RaftIndex: 3, Op: "DELETE", Key: []byte("deleted")},
	}})
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, handler)
	go func() { _ = gsrv.Serve(ln) }()
	t.Cleanup(func() {
		gsrv.GracefulStop()
		_ = ln.Close()
	})

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	stream, err := cefaspb.NewCefasClient(conn).StreamChanges(context.Background(), &cefaspb.StreamChangesRequest{FromIndex: 2})
	if err != nil {
		t.Fatalf("stream changes: %v", err)
	}

	var got []*cefaspb.ChangeEvent
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		got = append(got, ev)
	}
	if len(got) != 2 {
		t.Fatalf("events = %d, want 2: %+v", len(got), got)
	}
	if got[0].GetRaftIndex() != 2 ||
		got[0].GetOp() != cefaspb.ChangeEvent_OP_PUT ||
		string(got[0].GetKey()) != "current" ||
		string(got[0].GetValue()) != "current-value" {
		t.Fatalf("first event = %+v", got[0])
	}
	if got[1].GetRaftIndex() != 3 ||
		got[1].GetOp() != cefaspb.ChangeEvent_OP_DELETE ||
		string(got[1].GetKey()) != "deleted" ||
		len(got[1].GetValue()) != 0 {
		t.Fatalf("second event = %+v", got[1])
	}
}
