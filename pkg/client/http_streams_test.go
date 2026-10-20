package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestHTTPNativeStreamCompactCheckpointRoundTrip(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir(), FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	server.New(db, cat).Routes(mux)
	h := httptest.NewServer(mux)
	defer h.Close()
	c, err := client.NewHTTPClient(h.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	td, err := c.CreateTable(ctx, types.TableDescriptor{Name: "logs-events", KeySchema: types.KeySchema{PK: "tenantId", SK: "orderKey"}, TTLAttribute: "expiresAt", TextIndexes: []types.TextIndexDescriptor{{Name: "message", Field: "message"}}, StreamSpecification: &types.StreamSpecification{StreamEnabled: true, StreamViewType: types.StreamViewTypeNewImage, RetentionSeconds: 86400}})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := c.DescribeStream(ctx, td.LatestStreamArn, client.DescribeStreamOptions{})
	if err != nil || len(desc.Shards) != 1 || desc.Shards[0].LatestSequenceNumber != "0" {
		t.Fatalf("describe committed latest: %+v %v", desc, err)
	}
	iter, err := c.GetShardIterator(ctx, client.GetShardIteratorOptions{StreamArn: td.LatestStreamArn, ShardID: desc.Shards[0].ShardID, ShardIteratorType: "AFTER_SEQUENCE_NUMBER", SequenceNumber: "0"})
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately old event time, persisted after the live bootstrap.
	item := types.Item{"tenantId": {T: types.AttrS, S: "a"}, "orderKey": {T: types.AttrS, S: "2020-01-01T00:00:00.000000000Z#1"}, "message": {T: types.AttrS, S: "late durable body"}, "expiresAt": {T: types.AttrN, N: strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)}}
	if err := c.PutItem(ctx, td.Name, item, client.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	page, err := c.GetRecords(ctx, iter, client.GetRecordsOptions{})
	if err != nil || len(page.Records) != 1 || page.Records[0].DynamoDB.NewImage["message"].S != "late durable body" || page.LastScannedSequenceNumber != "1" {
		t.Fatalf("late commit lost: %+v %v", page, err)
	}
	if err := c.DeleteItem(ctx, td.Name, item, client.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	page, err = c.GetRecords(ctx, iter, client.GetRecordsOptions{})
	if err != nil || page.LastScannedSequenceNumber != "2" {
		t.Fatalf("withdrawal failed to advance: %+v %v", page, err)
	}
	for _, rec := range page.Records {
		if len(rec.DynamoDB.NewImage)+len(rec.DynamoDB.OldImage) != 0 {
			t.Fatal("deleted image served through canonical SDK")
		}
	}
}
