package server_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/osvaldoandrade/cefas/internal/server"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/protocol"
)

// startUnsecuredFixture spins up a gRPC server with auth disabled — the
// per-request scope checks then short-circuit on missing claims (single
// node dev contract). Good enough for handler-shape tests.
func startUnsecuredFixture(t *testing.T) (cefaspb.CefasClient, func()) {
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
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, server.NewGRPCServer(db, cat, nil))
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

func TestUpdateAndDescribeTimeToLive(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Sessions",
			KeySchema: &cefaspb.KeySchema{Pk: "pk"},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dt, err := stub.DescribeTimeToLive(ctx, &cefaspb.DescribeTimeToLiveRequest{TableName: "Sessions"})
	if err != nil {
		t.Fatalf("describe initial: %v", err)
	}
	if dt.GetStatus() != "DISABLED" {
		t.Fatalf("initial status = %q, want DISABLED", dt.GetStatus())
	}

	up, err := stub.UpdateTimeToLive(ctx, &cefaspb.UpdateTimeToLiveRequest{
		TableName: "Sessions",
		TimeToLiveSpecification: &cefaspb.TimeToLiveSpecification{
			Enabled:       true,
			AttributeName: "expires_at",
		},
	})
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !up.GetTimeToLiveSpecification().GetEnabled() || up.GetTimeToLiveSpecification().GetAttributeName() != "expires_at" {
		t.Fatalf("enable response = %+v", up.GetTimeToLiveSpecification())
	}

	dt, err = stub.DescribeTimeToLive(ctx, &cefaspb.DescribeTimeToLiveRequest{TableName: "Sessions"})
	if err != nil {
		t.Fatalf("describe after enable: %v", err)
	}
	if dt.GetStatus() != "ENABLED" || dt.GetAttributeName() != "expires_at" {
		t.Fatalf("describe after enable = %+v", dt)
	}

	if _, err := stub.UpdateTimeToLive(ctx, &cefaspb.UpdateTimeToLiveRequest{
		TableName:               "Sessions",
		TimeToLiveSpecification: &cefaspb.TimeToLiveSpecification{Enabled: false},
	}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	dt, _ = stub.DescribeTimeToLive(ctx, &cefaspb.DescribeTimeToLiveRequest{TableName: "Sessions"})
	if dt.GetStatus() != "DISABLED" {
		t.Fatalf("status after disable = %q", dt.GetStatus())
	}
}

func TestUpdateTimeToLiveRejectsEnabledWithoutAttribute(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "T",
			KeySchema: &cefaspb.KeySchema{Pk: "pk"},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err := stub.UpdateTimeToLive(ctx, &cefaspb.UpdateTimeToLiveRequest{
		TableName: "T",
		TimeToLiveSpecification: &cefaspb.TimeToLiveSpecification{
			Enabled:       true,
			AttributeName: "",
		},
	})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
