package api_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/api"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

// stubPlug satisfies plugin.Plugin so we can pre-seed a registry
// without dragging in a real plugin implementation.
type stubPlug struct {
	name string
	kind plugin.Kind
}

func (s *stubPlug) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: s.name, Kind: s.kind, Version: "1"}
}

// fixtureWithRegistry mirrors startUnsecuredFixture but injects an
// explicit plugin registry so the test doesn't rely on plugin.Default
// state (which other init() functions may have touched).
func fixtureWithRegistry(t *testing.T, r *plugin.Registry) (cefaspb.CefasClient, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	cat, _ := catalog.New(db)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := api.NewGRPCServer(db, cat, nil)
	srv.AttachPluginRegistry(r)
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, srv)
	go func() { _ = gsrv.Serve(ln) }()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return cefaspb.NewCefasClient(conn), func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = db.Close()
	}
}

func TestListPluginsReturnsRegistered(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlug{name: "trigram", kind: plugin.KindIndex})
	_ = r.Register(&stubPlug{name: "cosine", kind: plugin.KindDistance})
	stub, cleanup := fixtureWithRegistry(t, r)
	defer cleanup()
	resp, err := stub.ListPlugins(context.Background(), &cefaspb.ListPluginsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.GetPlugins()) != 2 {
		t.Fatalf("plugins = %d, want 2", len(resp.GetPlugins()))
	}
	// sorted by name
	if resp.GetPlugins()[0].GetName() != "cosine" || resp.GetPlugins()[1].GetName() != "trigram" {
		t.Fatalf("order: %v / %v", resp.GetPlugins()[0].GetName(), resp.GetPlugins()[1].GetName())
	}
}

func TestListPluginsFilterByKind(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlug{name: "trigram", kind: plugin.KindIndex})
	_ = r.Register(&stubPlug{name: "cosine", kind: plugin.KindDistance})
	stub, cleanup := fixtureWithRegistry(t, r)
	defer cleanup()
	resp, err := stub.ListPlugins(context.Background(), &cefaspb.ListPluginsRequest{Kind: "distance"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(resp.GetPlugins()) != 1 || resp.GetPlugins()[0].GetName() != "cosine" {
		t.Fatalf("filtered = %v", resp.GetPlugins())
	}
}

func TestListPluginsRejectsBadKind(t *testing.T) {
	r := plugin.NewRegistry()
	stub, cleanup := fixtureWithRegistry(t, r)
	defer cleanup()
	_, err := stub.ListPlugins(context.Background(), &cefaspb.ListPluginsRequest{Kind: "bogus"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestDescribePluginReturnsPlugin(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlug{name: "trigram", kind: plugin.KindIndex})
	stub, cleanup := fixtureWithRegistry(t, r)
	defer cleanup()
	resp, err := stub.DescribePlugin(context.Background(), &cefaspb.DescribePluginRequest{Name: "trigram"})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if resp.GetPlugin().GetName() != "trigram" || resp.GetPlugin().GetKind() != "index" {
		t.Fatalf("got = %v", resp.GetPlugin())
	}
}

func TestDescribePluginUnknownReturnsNotFound(t *testing.T) {
	r := plugin.NewRegistry()
	stub, cleanup := fixtureWithRegistry(t, r)
	defer cleanup()
	_, err := stub.DescribePlugin(context.Background(), &cefaspb.DescribePluginRequest{Name: "ghost"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestDescribePluginRequiresName(t *testing.T) {
	r := plugin.NewRegistry()
	stub, cleanup := fixtureWithRegistry(t, r)
	defer cleanup()
	_, err := stub.DescribePlugin(context.Background(), &cefaspb.DescribePluginRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}
