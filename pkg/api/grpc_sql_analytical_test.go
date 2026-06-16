package api_test

import (
	"context"
	"testing"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

func TestGRPCSqlAnalyticalQueries(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	createSQLTable(t, ctx, stub, "Users", "id")
	createSQLTable(t, ctx, stub, "Orders", "order_id")
	putSQLItem(t, ctx, stub, "Users", map[string]*cefaspb.AttributeValue{
		"id":     {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		"status": {Value: &cefaspb.AttributeValue_S{S: "active"}},
		"score":  {Value: &cefaspb.AttributeValue_N{N: "90"}},
	})
	putSQLItem(t, ctx, stub, "Users", map[string]*cefaspb.AttributeValue{
		"id":     {Value: &cefaspb.AttributeValue_S{S: "u2"}},
		"status": {Value: &cefaspb.AttributeValue_S{S: "inactive"}},
		"score":  {Value: &cefaspb.AttributeValue_N{N: "40"}},
	})
	putSQLItem(t, ctx, stub, "Users", map[string]*cefaspb.AttributeValue{
		"id":     {Value: &cefaspb.AttributeValue_S{S: "u3"}},
		"status": {Value: &cefaspb.AttributeValue_S{S: "active"}},
		"score":  {Value: &cefaspb.AttributeValue_N{N: "95"}},
	})
	putSQLItem(t, ctx, stub, "Orders", map[string]*cefaspb.AttributeValue{
		"order_id": {Value: &cefaspb.AttributeValue_S{S: "o1"}},
		"user_id":  {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		"status":   {Value: &cefaspb.AttributeValue_S{S: "paid"}},
	})
	putSQLItem(t, ctx, stub, "Orders", map[string]*cefaspb.AttributeValue{
		"order_id": {Value: &cefaspb.AttributeValue_S{S: "o2"}},
		"user_id":  {Value: &cefaspb.AttributeValue_S{S: "u3"}},
		"status":   {Value: &cefaspb.AttributeValue_S{S: "paid"}},
	})

	ordered, err := stub.Sql(ctx, &cefaspb.SqlRequest{Query: "SELECT id FROM Users ALLOW SCAN WHERE status = 'active' ORDER BY score DESC LIMIT 2"})
	if err != nil {
		t.Fatalf("sql order: %v", err)
	}
	if got := sqlStringAttr(ordered.GetRows()[0], "id") + "," + sqlStringAttr(ordered.GetRows()[1], "id"); got != "u3,u1" {
		t.Fatalf("ordered ids = %s, want u3,u1", got)
	}

	grouped, err := stub.Sql(ctx, &cefaspb.SqlRequest{Query: "SELECT status, COUNT(*) FROM Users ALLOW SCAN GROUP BY status"})
	if err != nil {
		t.Fatalf("sql group: %v", err)
	}
	assertSQLGroupCount(t, grouped.GetRows(), "active", "2")
	assertSQLGroupCount(t, grouped.GetRows(), "inactive", "1")

	joined, err := stub.Sql(ctx, &cefaspb.SqlRequest{Query: "SELECT u.id, o.order_id FROM Users u INNER JOIN Orders o ON u.id = o.user_id ALLOW SCAN WHERE o.status = 'paid' LIMIT 10"})
	if err != nil {
		t.Fatalf("sql join: %v", err)
	}
	pairs := map[string]bool{}
	for _, row := range joined.GetRows() {
		pairs[sqlStringAttr(row, "u.id")+"/"+sqlStringAttr(row, "o.order_id")] = true
	}
	if !pairs["u1/o1"] || !pairs["u3/o2"] || len(pairs) != 2 {
		t.Fatalf("join pairs = %+v", pairs)
	}
}

func createSQLTable(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, table, pk string) {
	t.Helper()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      table,
			KeySchema: &cefaspb.KeySchema{Pk: pk},
		},
	}); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
}

func putSQLItem(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, table string, item map[string]*cefaspb.AttributeValue) {
	t.Helper()
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{Table: table, Item: item}); err != nil {
		t.Fatalf("put %s: %v", table, err)
	}
}

func sqlStringAttr(row *cefaspb.Item, name string) string {
	return row.GetAttributes()[name].GetS()
}

func assertSQLGroupCount(t *testing.T, rows []*cefaspb.Item, status, count string) {
	t.Helper()
	for _, row := range rows {
		if sqlStringAttr(row, "status") == status {
			if got := row.GetAttributes()["count"].GetN(); got != count {
				t.Fatalf("count for %s = %s, want %s", status, got, count)
			}
			return
		}
	}
	t.Fatalf("status %s not found in %+v", status, rows)
}
