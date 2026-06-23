package server_test

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

func startCDCFixture(t *testing.T) (cefaspb.CefasClient, func()) {
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

func TestCDC_ScanReturnsChangelogRows(t *testing.T) {
	stub, cleanup := startCDCFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Orders",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			StreamSpecification: &cefaspb.StreamSpecification{
				StreamEnabled:  true,
				StreamViewType: "NEW_AND_OLD_IMAGES",
			},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for i, pk := range []string{"o1", "o2", "o3"} {
		if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: "Orders",
			Item: map[string]*cefaspb.AttributeValue{
				"id":     {Value: &cefaspb.AttributeValue_S{S: pk}},
				"status": {Value: &cefaspb.AttributeValue_S{S: "open"}},
			},
		}); err != nil {
			t.Fatalf("PutItem %d: %v", i, err)
		}
	}

	stream, err := stub.Scan(ctx, &cefaspb.ScanRequest{
		Table: "Orders_cdc",
	})
	if err != nil {
		t.Fatalf("Scan Orders_cdc: %v", err)
	}
	var rows []map[string]*cefaspb.AttributeValue
	for {
		it, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if it == nil {
			break
		}
		rows = append(rows, it.GetAttributes())
	}
	if len(rows) != 3 {
		t.Fatalf("CDC rows = %d, want 3", len(rows))
	}
	for i, row := range rows {
		if row["table"].GetS() != "Orders" {
			t.Errorf("row %d table = %q, want Orders", i, row["table"].GetS())
		}
		if row["op"].GetS() != "put" {
			t.Errorf("row %d op = %q, want put", i, row["op"].GetS())
		}
		if row["index"].GetN() == "" {
			t.Errorf("row %d missing index", i)
		}
		if row["event_time"].GetN() == "" {
			t.Errorf("row %d missing event_time", i)
		}
	}
}

func TestCDC_DescribeFallsThroughToAlias(t *testing.T) {
	stub, cleanup := startCDCFixture(t)
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
	resp, err := stub.DescribeTable(ctx, &cefaspb.DescribeTableRequest{Name: "Orders_cdc"})
	if err != nil {
		t.Fatalf("DescribeTable Orders_cdc: %v", err)
	}
	if resp.GetDescriptor_().GetName() != "Orders_cdc" {
		t.Errorf("Name = %q", resp.GetDescriptor_().GetName())
	}
}

func TestCDC_ScanUnknownBaseFails(t *testing.T) {
	stub, cleanup := startCDCFixture(t)
	defer cleanup()
	ctx := context.Background()
	stream, err := stub.Scan(ctx, &cefaspb.ScanRequest{Table: "Missing_cdc"})
	if err != nil {
		// Server-side Describe failure surfaces here.
		return
	}
	// Otherwise the stream itself should error on Recv.
	if _, err := stream.Recv(); err == nil {
		t.Error("expected error scanning CDC alias over missing base")
	}
}
