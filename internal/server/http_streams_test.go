package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CefasDb/cefasdb/internal/server"
	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestHTTPStreamsDiscovery(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := catStore.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
	}); err != nil {
		t.Fatalf("create events: %v", err)
	}
	td, err := catStore.Describe("Events")
	if err != nil {
		t.Fatalf("describe events: %v", err)
	}
	if err := db.PutItemWith(td, types.Item{
		"pk": {T: types.AttrS, S: "event-1"},
		"v":  {T: types.AttrS, S: "payload"},
	}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put stream row: %v", err)
	}

	srv := server.New(db, catStore)
	mux := http.NewServeMux()
	srv.Routes(mux)

	listReq := httptest.NewRequest(http.MethodPost, "/v1/ListStreams", bytes.NewBufferString(`{"tableName":"Events"}`))
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d body=%s", listRec.Code, listRec.Body.String())
	}
	var listResp struct {
		Streams []struct {
			StreamArn   string `json:"streamArn"`
			StreamLabel string `json:"streamLabel"`
			TableName   string `json:"tableName"`
		} `json:"streams"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResp.Streams) != 1 || listResp.Streams[0].StreamArn != td.LatestStreamArn || listResp.Streams[0].TableName != "Events" {
		t.Fatalf("list response = %+v, want Events stream %q", listResp, td.LatestStreamArn)
	}

	describeReq := httptest.NewRequest(http.MethodPost, "/v1/DescribeStream", bytes.NewBufferString(`{"streamArn":"`+td.LatestStreamArn+`"}`))
	describeRec := httptest.NewRecorder()
	mux.ServeHTTP(describeRec, describeReq)
	if describeRec.Code != http.StatusOK {
		t.Fatalf("describe status = %d body=%s", describeRec.Code, describeRec.Body.String())
	}
	var describeResp struct {
		StreamDescription struct {
			StreamArn    string                        `json:"streamArn"`
			StreamStatus string                        `json:"streamStatus"`
			TableName    string                        `json:"tableName"`
			Shards       []types.StreamShardDescriptor `json:"shards"`
		} `json:"streamDescription"`
	}
	if err := json.NewDecoder(describeRec.Body).Decode(&describeResp); err != nil {
		t.Fatalf("decode describe: %v", err)
	}
	if describeResp.StreamDescription.StreamArn != td.LatestStreamArn ||
		describeResp.StreamDescription.StreamStatus != types.StreamStatusEnabled ||
		describeResp.StreamDescription.TableName != "Events" ||
		len(describeResp.StreamDescription.Shards) != 1 {
		t.Fatalf("describe response = %+v", describeResp)
	}

	iteratorReq := httptest.NewRequest(http.MethodPost, "/v1/GetShardIterator", bytes.NewBufferString(`{"streamArn":"`+td.LatestStreamArn+`","shardId":"`+types.StreamShardIDSingle+`","shardIteratorType":"TRIM_HORIZON"}`))
	iteratorRec := httptest.NewRecorder()
	mux.ServeHTTP(iteratorRec, iteratorReq)
	if iteratorRec.Code != http.StatusOK {
		t.Fatalf("iterator status = %d body=%s", iteratorRec.Code, iteratorRec.Body.String())
	}
	var iteratorResp struct {
		ShardIterator string `json:"shardIterator"`
	}
	if err := json.NewDecoder(iteratorRec.Body).Decode(&iteratorResp); err != nil {
		t.Fatalf("decode iterator: %v", err)
	}
	if iteratorResp.ShardIterator == "" {
		t.Fatal("empty shard iterator")
	}

	recordsReq := httptest.NewRequest(http.MethodPost, "/v1/GetRecords", bytes.NewBufferString(`{"shardIterator":"`+iteratorResp.ShardIterator+`","limit":1}`))
	recordsRec := httptest.NewRecorder()
	mux.ServeHTTP(recordsRec, recordsReq)
	if recordsRec.Code != http.StatusOK {
		t.Fatalf("records status = %d body=%s", recordsRec.Code, recordsRec.Body.String())
	}
	var recordsResp struct {
		Records []struct {
			EventName      string `json:"eventName"`
			EventSourceARN string `json:"eventSourceARN"`
			DynamoDB       struct {
				Keys map[string]struct {
					S string `json:"S,omitempty"`
				} `json:"keys"`
				SequenceNumber string `json:"sequenceNumber"`
				StreamViewType string `json:"streamViewType"`
			} `json:"dynamodb"`
		} `json:"records"`
		NextShardIterator string `json:"nextShardIterator,omitempty"`
	}
	if err := json.NewDecoder(recordsRec.Body).Decode(&recordsResp); err != nil {
		t.Fatalf("decode records: %v", err)
	}
	if len(recordsResp.Records) != 1 ||
		recordsResp.Records[0].EventName != "INSERT" ||
		recordsResp.Records[0].EventSourceARN != td.LatestStreamArn ||
		recordsResp.Records[0].DynamoDB.Keys["pk"].S != "event-1" ||
		recordsResp.Records[0].DynamoDB.SequenceNumber != "1" ||
		recordsResp.Records[0].DynamoDB.StreamViewType != types.StreamViewTypeKeysOnly ||
		recordsResp.NextShardIterator == "" {
		t.Fatalf("records response = %+v", recordsResp)
	}
}

func TestHTTPDescribeStreamMissingReturnsNotFound(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := server.New(db, catStore)
	mux := http.NewServeMux()
	srv.Routes(mux)

	req := httptest.NewRequest(http.MethodPost, "/v1/DescribeStream", bytes.NewBufferString(`{"streamArn":"arn:missing"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}
