package api_test

import (
	"context"
	"errors"
	"io"
	"testing"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

func putString(t *testing.T, stub cefaspb.CefasClient, table, pk, v string) {
	t.Helper()
	_, err := stub.PutItem(context.Background(), &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: pk}},
			"v":  {Value: &cefaspb.AttributeValue_S{S: v}},
		},
	})
	if err != nil {
		t.Fatalf("put %s: %v", pk, err)
	}
}

func drainScan(t *testing.T, stream cefaspb.Cefas_ScanClient) []map[string]string {
	t.Helper()
	out := []map[string]string{}
	for {
		it, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		row := map[string]string{}
		for k, av := range it.GetAttributes() {
			if s := av.GetS(); s != "" {
				row[k] = s
			}
		}
		out = append(out, row)
	}
}

func TestScanReturnsEveryItem(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "T",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	putString(t, stub, "T", "a", "alpha")
	putString(t, stub, "T", "b", "beta")
	putString(t, stub, "T", "c", "gamma")

	stream, err := stub.Scan(ctx, &cefaspb.ScanRequest{Table: "T"})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	rows := drainScan(t, stream)
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (%v)", len(rows), rows)
	}
}

func TestScanFilterExpression(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "T",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	putString(t, stub, "T", "a", "alpha")
	putString(t, stub, "T", "b", "beta")
	putString(t, stub, "T", "c", "alpha")

	stream, err := stub.Scan(ctx, &cefaspb.ScanRequest{
		Table:            "T",
		FilterExpression: "v = :want",
		Binds: map[string]*cefaspb.AttributeValue{
			":want": {Value: &cefaspb.AttributeValue_S{S: "alpha"}},
		},
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	rows := drainScan(t, stream)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (%v)", len(rows), rows)
	}
	for _, r := range rows {
		if r["v"] != "alpha" {
			t.Fatalf("unexpected row %v", r)
		}
	}
}

func TestScanRespectsLimit(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "T",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		putString(t, stub, "T", id, id)
	}
	stream, err := stub.Scan(ctx, &cefaspb.ScanRequest{Table: "T", Limit: 2})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	rows := drainScan(t, stream)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
}
