package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	apiserver "github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

func newProbeMux(t *testing.T) (*apiserver.Server, *http.ServeMux) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := apiserver.New(db, cat)
	mux := http.NewServeMux()
	srv.Routes(mux)
	return srv, mux
}

func probeStatus(mux http.Handler, path string) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Code
}

func TestStartupAndReadinessLifecycle(t *testing.T) {
	srv, mux := newProbeMux(t)

	if got := probeStatus(mux, "/startupz"); got != http.StatusServiceUnavailable {
		t.Fatalf("startup before MarkStarted = %d, want 503", got)
	}
	if got := probeStatus(mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready before MarkStarted = %d, want 503", got)
	}

	srv.MarkStarted()
	if got := probeStatus(mux, "/startupz"); got != http.StatusOK {
		t.Fatalf("startup after MarkStarted = %d, want 200", got)
	}
	if got := probeStatus(mux, "/readyz"); got != http.StatusOK {
		t.Fatalf("ready after MarkStarted = %d, want 200", got)
	}

	srv.StartDraining("test")
	if got := probeStatus(mux, "/livez"); got != http.StatusOK {
		t.Fatalf("live while draining = %d, want 200", got)
	}
	if got := probeStatus(mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready while draining = %d, want 503", got)
	}
}

func TestReadinessCheckFailure(t *testing.T) {
	srv, mux := newProbeMux(t)
	srv.MarkStarted()
	srv.AddReadinessCheck("lease", func(context.Context) error {
		return fmt.Errorf("lost")
	})

	if got := probeStatus(mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready with failing check = %d, want 503", got)
	}
}

func TestRaftReadinessRequiresKnownLeader(t *testing.T) {
	srv, mux := newProbeMux(t)
	cluster := &fakeProbeCluster{selfID: "n1", bindAddr: "127.0.0.1:9001"}
	srv.AttachCluster(cluster)
	srv.MarkStarted()

	if got := probeStatus(mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("ready without raft leader = %d, want 503", got)
	}
	if got := probeStatus(mux, "/raftz"); got != http.StatusOK {
		t.Fatalf("raftz without raft leader = %d, want 200", got)
	}

	cluster.leaderID = "n2"
	cluster.leaderAddr = "127.0.0.1:9002"
	if got := probeStatus(mux, "/readyz"); got != http.StatusOK {
		t.Fatalf("ready with raft leader = %d, want 200", got)
	}
}

type fakeProbeCluster struct {
	leader     bool
	selfID     string
	bindAddr   string
	leaderID   string
	leaderAddr string
	leaderHTTP string
}

func (f *fakeProbeCluster) IsLeader() bool { return f.leader }

func (f *fakeProbeCluster) LeaderInfo() (string, string) { return f.leaderID, f.leaderAddr }

func (f *fakeProbeCluster) LeaderHTTPAddr() string { return f.leaderHTTP }

func (f *fakeProbeCluster) AddVoter(string, string, time.Duration) error { return nil }

func (f *fakeProbeCluster) RemoveServer(string, time.Duration) error { return nil }

func (f *fakeProbeCluster) Barrier(time.Duration) error { return nil }

func (f *fakeProbeCluster) SelfID() string { return f.selfID }

func (f *fakeProbeCluster) BindAddr() string { return f.bindAddr }
