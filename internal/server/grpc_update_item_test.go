package server_test

import (
	"context"
	"testing"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

func createTable(t *testing.T, stub cefaspb.CefasClient, name string) {
	t.Helper()
	if _, err := stub.CreateTable(context.Background(), &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      name,
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}

func TestUpdateItemSetAndAdd(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "T",
		Item: map[string]*cefaspb.AttributeValue{
			"id":    {Value: &cefaspb.AttributeValue_S{S: "u1"}},
			"name":  {Value: &cefaspb.AttributeValue_S{S: "old"}},
			"score": {Value: &cefaspb.AttributeValue_N{N: "5"}},
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := stub.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: "T",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		},
		UpdateExpression:         "SET #n = :name, ADD score :inc",
		ExpressionAttributeNames: map[string]string{"n": "name"},
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":name": {Value: &cefaspb.AttributeValue_S{S: "new"}},
			":inc":  {Value: &cefaspb.AttributeValue_N{N: "2"}},
		},
		ReturnValues: cefaspb.ReturnValues_RETURN_VALUES_ALL_NEW,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	got := resp.GetAttributes()
	if got["name"].GetS() != "new" {
		t.Fatalf("name = %q, want new", got["name"].GetS())
	}
	if got["score"].GetN() != "7" {
		t.Fatalf("score = %q, want 7", got["score"].GetN())
	}
}

func TestUpdateItemReturnOld(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "T",
		Item: map[string]*cefaspb.AttributeValue{
			"id":   {Value: &cefaspb.AttributeValue_S{S: "u1"}},
			"name": {Value: &cefaspb.AttributeValue_S{S: "alpha"}},
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := stub.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: "T",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		},
		UpdateExpression: "SET name = :v",
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":v": {Value: &cefaspb.AttributeValue_S{S: "beta"}},
		},
		ReturnValues: cefaspb.ReturnValues_RETURN_VALUES_ALL_OLD,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.GetAttributes()["name"].GetS() != "alpha" {
		t.Fatalf("old name = %q, want alpha", resp.GetAttributes()["name"].GetS())
	}
}

func TestUpdateItemConditionExpression(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "T",
		Item: map[string]*cefaspb.AttributeValue{
			"id":   {Value: &cefaspb.AttributeValue_S{S: "u1"}},
			"tier": {Value: &cefaspb.AttributeValue_S{S: "bronze"}},
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Condition fails: tier is bronze, not gold.
	_, err := stub.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: "T",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		},
		UpdateExpression:    "SET tier = :next",
		ConditionExpression: "tier = :req",
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":next": {Value: &cefaspb.AttributeValue_S{S: "silver"}},
			":req":  {Value: &cefaspb.AttributeValue_S{S: "gold"}},
		},
	})
	if err == nil {
		t.Fatalf("expected condition failure, got nil")
	}
}

func TestUpdateItemRemove(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "T",
		Item: map[string]*cefaspb.AttributeValue{
			"id":      {Value: &cefaspb.AttributeValue_S{S: "u1"}},
			"deleted": {Value: &cefaspb.AttributeValue_BoolVal{BoolVal: true}},
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := stub.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: "T",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		},
		UpdateExpression: "REMOVE deleted",
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	g, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "T",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, exists := g.GetItem()["deleted"]; exists {
		t.Fatalf("deleted attribute still present after REMOVE: %v", g.GetItem())
	}
}
