package server_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

func startUpdateStreamFixture(t *testing.T) (cefaspb.CefasClient, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := server.NewGRPCServer(db, cat, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, srv)
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = db.Close()
	}
	return cefaspb.NewCefasClient(conn), cleanup
}

func TestUpdateStreamSpecification_EnableThenDisable(t *testing.T) {
	stub, cleanup := startUpdateStreamFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Orders",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Enable streams.
	enabled, err := stub.UpdateStreamSpecification(ctx, &cefaspb.UpdateStreamSpecificationRequest{
		TableName: "Orders",
		StreamSpecification: &cefaspb.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: "NEW_AND_OLD_IMAGES",
		},
	})
	if err != nil {
		t.Fatalf("UpdateStreamSpecification enable: %v", err)
	}
	if !enabled.GetStreamSpecification().GetStreamEnabled() {
		t.Error("expected stream_enabled = true after enable")
	}

	// DescribeTable reflects the update.
	desc, err := stub.DescribeTable(ctx, &cefaspb.DescribeTableRequest{Name: "Orders"})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	if !desc.GetDescriptor_().GetStreamSpecification().GetStreamEnabled() {
		t.Error("DescribeTable did not see the enabled stream spec")
	}

	// Disable.
	disabled, err := stub.UpdateStreamSpecification(ctx, &cefaspb.UpdateStreamSpecificationRequest{
		TableName:           "Orders",
		StreamSpecification: &cefaspb.StreamSpecification{StreamEnabled: false},
	})
	if err != nil {
		t.Fatalf("UpdateStreamSpecification disable: %v", err)
	}
	if disabled.GetStreamSpecification() != nil && disabled.GetStreamSpecification().GetStreamEnabled() {
		t.Error("expected stream_enabled = false after disable")
	}
}

func TestUpdateStreamSpecification_ChangeViewType(t *testing.T) {
	stub, cleanup := startUpdateStreamFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Orders",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			StreamSpecification: &cefaspb.StreamSpecification{
				StreamEnabled:  true,
				StreamViewType: "NEW_IMAGE",
			},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	resp, err := stub.UpdateStreamSpecification(ctx, &cefaspb.UpdateStreamSpecificationRequest{
		TableName: "Orders",
		StreamSpecification: &cefaspb.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: "NEW_AND_OLD_IMAGES",
		},
	})
	if err != nil {
		t.Fatalf("UpdateStreamSpecification: %v", err)
	}
	if resp.GetStreamSpecification().GetStreamViewType() != "NEW_AND_OLD_IMAGES" {
		t.Errorf("view = %q, want NEW_AND_OLD_IMAGES", resp.GetStreamSpecification().GetStreamViewType())
	}
}

func TestUpdateStreamSpecification_PerTableRetention(t *testing.T) {
	stub, cleanup := startUpdateStreamFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Orders",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	resp, err := stub.UpdateStreamSpecification(ctx, &cefaspb.UpdateStreamSpecificationRequest{
		TableName: "Orders",
		StreamSpecification: &cefaspb.StreamSpecification{
			StreamEnabled:    true,
			StreamViewType:   "NEW_AND_OLD_IMAGES",
			RetentionSeconds: 3600,
		},
	})
	if err != nil {
		t.Fatalf("UpdateStreamSpecification: %v", err)
	}
	if got := resp.GetStreamSpecification().GetRetentionSeconds(); got != 3600 {
		t.Errorf("RetentionSeconds = %d, want 3600", got)
	}
}

func TestUpdateStreamSpecification_RejectsMissingTable(t *testing.T) {
	stub, cleanup := startUpdateStreamFixture(t)
	defer cleanup()
	_, err := stub.UpdateStreamSpecification(context.Background(), &cefaspb.UpdateStreamSpecificationRequest{
		TableName: "Missing",
		StreamSpecification: &cefaspb.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: "KEYS_ONLY",
		},
	})
	if err == nil {
		t.Fatal("expected error for missing table")
	}
}
