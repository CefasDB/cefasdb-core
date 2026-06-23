package server_test

import (
	"context"
	"net"
	"testing"

	identitymw "github.com/codecompany/identity-middleware/pkg/identitymiddleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestResolveServiceLevel_HeaderTakesPrecedence(t *testing.T) {
	md := metadata.New(map[string]string{
		auth.ServiceLevelMetadataKey: "olap",
	})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	if got := server.ResolveServiceLevel(ctx); got != "olap" {
		t.Errorf("got %q, want %q", got, "olap")
	}
}

func TestResolveServiceLevel_ClaimFallback(t *testing.T) {
	claims := &identitymw.Claims{Raw: map[string]any{
		auth.ServiceLevelClaimKey: "batch",
	}}
	ctx := identitymw.WithClaims(context.Background(), claims)
	if got := server.ResolveServiceLevel(ctx); got != "batch" {
		t.Errorf("got %q, want %q", got, "batch")
	}
}

func TestResolveServiceLevel_HeaderBeatsClaim(t *testing.T) {
	claims := &identitymw.Claims{Raw: map[string]any{
		auth.ServiceLevelClaimKey: "batch",
	}}
	ctx := identitymw.WithClaims(context.Background(), claims)
	md := metadata.New(map[string]string{
		auth.ServiceLevelMetadataKey: "oltp",
	})
	ctx = metadata.NewIncomingContext(ctx, md)
	if got := server.ResolveServiceLevel(ctx); got != "oltp" {
		t.Errorf("got %q, want oltp", got)
	}
}

func TestResolveServiceLevel_DefaultFallback(t *testing.T) {
	if got := server.ResolveServiceLevel(context.Background()); got != types.DefaultServiceLevelName {
		t.Errorf("got %q, want default", got)
	}
}

func TestServiceLevelInterceptor_AttachesToContext(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := server.NewGRPCServer(db, cat, nil)

	var observed string
	captured := false
	tapUnary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		observed = auth.ServiceLevelFromContext(ctx)
		captured = true
		return handler(ctx, req)
	}

	slUnary, _ := server.ServiceLevelInterceptor()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer(grpc.ChainUnaryInterceptor(slUnary, tapUnary))
	cefaspb.RegisterCefasServer(gsrv, srv)
	go func() { _ = gsrv.Serve(ln) }()
	defer gsrv.GracefulStop()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	stub := cefaspb.NewCefasClient(conn)

	ctx := metadata.AppendToOutgoingContext(context.Background(), auth.ServiceLevelMetadataKey, "olap")
	if _, err := stub.ListServiceLevels(ctx, &cefaspb.ListServiceLevelsRequest{}); err != nil {
		t.Fatalf("ListServiceLevels: %v", err)
	}
	if !captured {
		t.Fatal("tap interceptor never observed the request")
	}
	if observed != "olap" {
		t.Errorf("observed SL = %q, want olap", observed)
	}
}
