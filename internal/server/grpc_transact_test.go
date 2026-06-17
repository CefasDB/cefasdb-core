package server_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/protocol"
)

func TestTransactWriteItemsAppliesPutAndDelete(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	putString(t, stub, "T", "delete-me", "v")

	_, err := stub.TransactWriteItems(ctx, &cefaspb.TransactWriteItemsRequest{
		Ops: []*cefaspb.TransactWriteOp{
			{Op: &cefaspb.TransactWriteOp_Put_{Put: &cefaspb.TransactWriteOp_Put{
				Table: "T",
				Item: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
					"v":  {Value: &cefaspb.AttributeValue_S{S: "alpha"}},
				},
			}}},
			{Op: &cefaspb.TransactWriteOp_Delete_{Delete: &cefaspb.TransactWriteOp_Delete{
				Table: "T",
				Key: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "delete-me"}},
				},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("transact: %v", err)
	}

	// The Put committed.
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{Table: "T", Key: map[string]*cefaspb.AttributeValue{
		"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
	}})
	if err != nil || got.GetItem()["v"].GetS() != "alpha" {
		t.Fatalf("put missing: %v %v", err, got)
	}
	// The Delete committed.
	got, err = stub.GetItem(ctx, &cefaspb.GetItemRequest{Table: "T", Key: map[string]*cefaspb.AttributeValue{
		"id": {Value: &cefaspb.AttributeValue_S{S: "delete-me"}},
	}})
	if err != nil {
		t.Fatalf("get delete-me: %v", err)
	}
	if got.GetFound() {
		t.Fatalf("delete-me still present")
	}
}

func TestTransactWriteItemsConditionCheckBlocks(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	// The check requires u1 to exist; it doesn't, so the whole txn fails.
	_, err := stub.TransactWriteItems(ctx, &cefaspb.TransactWriteItemsRequest{
		Ops: []*cefaspb.TransactWriteOp{
			{Op: &cefaspb.TransactWriteOp_ConditionCheck_{ConditionCheck: &cefaspb.TransactWriteOp_ConditionCheck{
				Table: "T",
				Key: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
				},
			}}, ConditionExpression: "attribute_exists(id)"},
			{Op: &cefaspb.TransactWriteOp_Put_{Put: &cefaspb.TransactWriteOp_Put{
				Table: "T",
				Item: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "u2"}},
				},
			}}},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	// The Put never landed.
	got, _ := stub.GetItem(ctx, &cefaspb.GetItemRequest{Table: "T", Key: map[string]*cefaspb.AttributeValue{
		"id": {Value: &cefaspb.AttributeValue_S{S: "u2"}},
	}})
	if got.GetFound() {
		t.Fatalf("u2 leaked through despite condition failure")
	}
}

func TestTransactWriteItemsRejectsCrossTable(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "A")
	createTable(t, stub, "B")
	_, err := stub.TransactWriteItems(ctx, &cefaspb.TransactWriteItemsRequest{
		Ops: []*cefaspb.TransactWriteOp{
			{Op: &cefaspb.TransactWriteOp_Put_{Put: &cefaspb.TransactWriteOp_Put{
				Table: "A",
				Item:  map[string]*cefaspb.AttributeValue{"id": {Value: &cefaspb.AttributeValue_S{S: "a"}}},
			}}},
			{Op: &cefaspb.TransactWriteOp_Put_{Put: &cefaspb.TransactWriteOp_Put{
				Table: "B",
				Item:  map[string]*cefaspb.AttributeValue{"id": {Value: &cefaspb.AttributeValue_S{S: "b"}}},
			}}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for cross-table, got %v", err)
	}
}

func TestTransactGetItemsRoundTrips(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	putString(t, stub, "T", "a", "alpha")
	putString(t, stub, "T", "b", "beta")

	resp, err := stub.TransactGetItems(ctx, &cefaspb.TransactGetItemsRequest{
		Items: []*cefaspb.TransactGet{
			{Table: "T", Key: map[string]*cefaspb.AttributeValue{
				"id": {Value: &cefaspb.AttributeValue_S{S: "a"}},
			}},
			{Table: "T", Key: map[string]*cefaspb.AttributeValue{
				"id": {Value: &cefaspb.AttributeValue_S{S: "missing"}},
			}},
			{Table: "T", Key: map[string]*cefaspb.AttributeValue{
				"id": {Value: &cefaspb.AttributeValue_S{S: "b"}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("transact get: %v", err)
	}
	if len(resp.GetItems()) != 3 {
		t.Fatalf("got %d items, want 3", len(resp.GetItems()))
	}
	if resp.GetItems()[0].GetAttributes()["v"].GetS() != "alpha" {
		t.Fatalf("a = %v", resp.GetItems()[0])
	}
	if len(resp.GetItems()[1].GetAttributes()) != 0 {
		t.Fatalf("missing should render empty: %v", resp.GetItems()[1])
	}
	if resp.GetItems()[2].GetAttributes()["v"].GetS() != "beta" {
		t.Fatalf("b = %v", resp.GetItems()[2])
	}
}
