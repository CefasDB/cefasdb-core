package api

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func newStreamIteratorTestServer(t *testing.T) (*GRPCServer, *storage.DB, *catalog.Catalog, func()) {
	t.Helper()
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	return NewGRPCServer(db, catStore, nil), db, catStore, func() {
		_ = db.Close()
	}
}

func createIteratorStream(t *testing.T, srv *GRPCServer) string {
	return createIteratorStreamWithView(t, srv, "Events", types.StreamViewTypeNewAndOldImages)
}

func createIteratorStreamWithView(t *testing.T, srv *GRPCServer, table, view string) string {
	t.Helper()
	resp, err := srv.CreateTable(context.Background(), &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      table,
			KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
			StreamSpecification: &cefaspb.StreamSpecification{
				StreamEnabled:  true,
				StreamViewType: view,
			},
		},
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	return resp.GetDescriptor_().GetLatestStreamArn()
}

func appendIteratorStreamRecords(t *testing.T, db *storage.DB, catStore *catalog.Catalog, count int) {
	appendIteratorStreamRecordsForTable(t, db, catStore, "Events", count)
}

func appendIteratorStreamRecordsForTable(t *testing.T, db *storage.DB, catStore *catalog.Catalog, table string, count int) {
	t.Helper()
	td, err := catStore.Describe(table)
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	for i := 1; i <= count; i++ {
		item := types.Item{
			"pk": {T: types.AttrS, S: "account"},
			"sk": {T: types.AttrS, S: fmt.Sprintf("%03d", i)},
			"v":  {T: types.AttrN, N: fmt.Sprint(i)},
		}
		if err := db.PutItemWith(td, item, storage.PutOptions{}); err != nil {
			t.Fatalf("put stream record %d: %v", i, err)
		}
	}
}

func appendInsertModifyRemove(t *testing.T, db *storage.DB, catStore *catalog.Catalog, table string) {
	t.Helper()
	td, err := catStore.Describe(table)
	if err != nil {
		t.Fatalf("describe table: %v", err)
	}
	first := types.Item{
		"pk":     {T: types.AttrS, S: "account"},
		"sk":     {T: types.AttrS, S: "001"},
		"status": {T: types.AttrS, S: "new"},
	}
	if err := db.PutItemWith(td, first, storage.PutOptions{}); err != nil {
		t.Fatalf("put first: %v", err)
	}
	updated := types.Item{
		"pk":     {T: types.AttrS, S: "account"},
		"sk":     {T: types.AttrS, S: "001"},
		"status": {T: types.AttrS, S: "updated"},
	}
	if err := db.PutItemWith(td, updated, storage.PutOptions{}); err != nil {
		t.Fatalf("put updated: %v", err)
	}
	if err := db.DeleteItemWith(td, types.Item{
		"pk": {T: types.AttrS, S: "account"},
		"sk": {T: types.AttrS, S: "001"},
	}, storage.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func iteratorPayloadFromResponse(t *testing.T, resp *cefaspb.GetShardIteratorResponse) streamShardIteratorPayload {
	t.Helper()
	payload, err := decodeStreamShardIterator(resp.GetShardIterator(), time.Now())
	if err != nil {
		t.Fatalf("decode iterator: %v", err)
	}
	return payload
}

func TestGetShardIteratorStartPositions(t *testing.T) {
	srv, db, catStore, cleanup := newStreamIteratorTestServer(t)
	defer cleanup()
	arn := createIteratorStream(t, srv)
	appendIteratorStreamRecords(t, db, catStore, 2)

	cases := []struct {
		name           string
		iteratorType   string
		sequenceNumber string
		wantNext       string
	}{
		{name: "trim horizon", iteratorType: streamIteratorTypeTrimHorizon, wantNext: "1"},
		{name: "latest", iteratorType: streamIteratorTypeLatest, wantNext: "3"},
		{name: "at sequence", iteratorType: streamIteratorTypeAtSequenceNumber, sequenceNumber: "2", wantNext: "2"},
		{name: "after sequence", iteratorType: streamIteratorTypeAfterSequenceNumber, sequenceNumber: "2", wantNext: "3"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := srv.GetShardIterator(context.Background(), &cefaspb.GetShardIteratorRequest{
				StreamArn:         arn,
				ShardId:           types.StreamShardIDSingle,
				ShardIteratorType: tc.iteratorType,
				SequenceNumber:    tc.sequenceNumber,
			})
			if err != nil {
				t.Fatalf("get iterator: %v", err)
			}
			payload := iteratorPayloadFromResponse(t, resp)
			if payload.StreamArn != arn || payload.ShardID != types.StreamShardIDSingle || payload.IteratorType != tc.iteratorType {
				t.Fatalf("payload identity = %+v", payload)
			}
			if payload.NextSequenceNumber != tc.wantNext {
				t.Fatalf("next sequence = %q, want %q", payload.NextSequenceNumber, tc.wantNext)
			}
			if payload.ExpiresUnixNano <= time.Now().UnixNano() {
				t.Fatalf("expiry is not in the future: %+v", payload)
			}
		})
	}
}

func TestGetShardIteratorValidationErrors(t *testing.T) {
	srv, _, _, cleanup := newStreamIteratorTestServer(t)
	defer cleanup()
	arn := createIteratorStream(t, srv)

	tests := []struct {
		name string
		req  *cefaspb.GetShardIteratorRequest
		code codes.Code
	}{
		{
			name: "missing sequence number",
			req: &cefaspb.GetShardIteratorRequest{
				StreamArn:         arn,
				ShardId:           types.StreamShardIDSingle,
				ShardIteratorType: streamIteratorTypeAtSequenceNumber,
			},
			code: codes.InvalidArgument,
		},
		{
			name: "unknown stream",
			req: &cefaspb.GetShardIteratorRequest{
				StreamArn:         "arn:missing",
				ShardId:           types.StreamShardIDSingle,
				ShardIteratorType: streamIteratorTypeTrimHorizon,
			},
			code: codes.NotFound,
		},
		{
			name: "unknown shard",
			req: &cefaspb.GetShardIteratorRequest{
				StreamArn:         arn,
				ShardId:           "missing-shard",
				ShardIteratorType: streamIteratorTypeTrimHorizon,
			},
			code: codes.NotFound,
		},
		{
			name: "trimmed sequence",
			req: &cefaspb.GetShardIteratorRequest{
				StreamArn:         arn,
				ShardId:           types.StreamShardIDSingle,
				ShardIteratorType: streamIteratorTypeAtSequenceNumber,
				SequenceNumber:    "0",
			},
			code: codes.FailedPrecondition,
		},
		{
			name: "invalid iterator type",
			req: &cefaspb.GetShardIteratorRequest{
				StreamArn:         arn,
				ShardId:           types.StreamShardIDSingle,
				ShardIteratorType: "FROM_NOW",
			},
			code: codes.InvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := srv.GetShardIterator(context.Background(), tt.req)
			if status.Code(err) != tt.code {
				t.Fatalf("code = %v, want %v: %v", status.Code(err), tt.code, err)
			}
		})
	}
}

func TestStreamShardIteratorTokenRejectsExpiredAndMalformedTokens(t *testing.T) {
	now := time.Unix(100, 0)
	token, err := encodeStreamShardIterator(streamShardIteratorPayload{
		Version:            1,
		StreamArn:          "arn:cefas:dynamodb:local:000000000000:table/Events/stream/label",
		ShardID:            types.StreamShardIDSingle,
		NextSequenceNumber: "1",
		IteratorType:       streamIteratorTypeTrimHorizon,
		ExpiresUnixNano:    now.Add(-time.Nanosecond).UnixNano(),
	})
	if err != nil {
		t.Fatalf("encode expired iterator: %v", err)
	}
	if _, err := decodeStreamShardIterator(token, now); !errors.Is(err, types.ErrStreamIteratorExpired) {
		t.Fatalf("expired token error = %v, want ErrStreamIteratorExpired", err)
	}
	if _, err := decodeStreamShardIterator("not-a-valid-token", now); !errors.Is(err, types.ErrStreamIteratorInvalid) {
		t.Fatalf("malformed token error = %v, want ErrStreamIteratorInvalid", err)
	}
}

func TestGetRecordsReturnsDynamoDBStreamRecordShape(t *testing.T) {
	srv, db, catStore, cleanup := newStreamIteratorTestServer(t)
	defer cleanup()
	arn := createIteratorStream(t, srv)
	appendInsertModifyRemove(t, db, catStore, "Events")

	iter, err := srv.GetShardIterator(context.Background(), &cefaspb.GetShardIteratorRequest{
		StreamArn:         arn,
		ShardId:           types.StreamShardIDSingle,
		ShardIteratorType: streamIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	resp, err := srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{
		ShardIterator: iter.GetShardIterator(),
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("get records: %v", err)
	}
	if len(resp.GetRecords()) != 3 {
		t.Fatalf("record count = %d, want 3", len(resp.GetRecords()))
	}
	if resp.GetNextShardIterator() == "" {
		t.Fatal("open shard returned empty next iterator")
	}

	insert := resp.GetRecords()[0]
	if insert.GetEventId() != arn+":1" ||
		insert.GetEventName() != "INSERT" ||
		insert.GetEventVersion() != streamEventVersion ||
		insert.GetEventSource() != streamEventSource ||
		insert.GetEventSourceArn() != arn ||
		insert.GetAwsRegion() != streamAWSRegion {
		t.Fatalf("insert envelope = %+v", insert)
	}
	if insert.GetDynamodb().GetSequenceNumber() != "1" ||
		insert.GetDynamodb().GetApproximateCreationDateTime() == 0 ||
		insert.GetDynamodb().GetSizeBytes() <= 0 ||
		insert.GetDynamodb().GetStreamViewType() != types.StreamViewTypeNewAndOldImages {
		t.Fatalf("insert dynamodb metadata = %+v", insert.GetDynamodb())
	}
	if insert.GetDynamodb().GetKeys()["pk"].GetS() != "account" ||
		insert.GetDynamodb().GetKeys()["sk"].GetS() != "001" {
		t.Fatalf("insert keys = %+v", insert.GetDynamodb().GetKeys())
	}
	if insert.GetDynamodb().GetNewImage()["status"].GetS() != "new" {
		t.Fatalf("insert new image = %+v", insert.GetDynamodb().GetNewImage())
	}
	if len(insert.GetDynamodb().GetOldImage()) != 0 {
		t.Fatalf("insert old image = %+v, want empty", insert.GetDynamodb().GetOldImage())
	}

	modify := resp.GetRecords()[1]
	if modify.GetEventName() != "MODIFY" ||
		modify.GetDynamodb().GetOldImage()["status"].GetS() != "new" ||
		modify.GetDynamodb().GetNewImage()["status"].GetS() != "updated" {
		t.Fatalf("modify record = %+v", modify)
	}
	remove := resp.GetRecords()[2]
	if remove.GetEventName() != "REMOVE" ||
		remove.GetDynamodb().GetOldImage()["status"].GetS() != "updated" ||
		len(remove.GetDynamodb().GetNewImage()) != 0 {
		t.Fatalf("remove record = %+v", remove)
	}
}

func TestGetRecordsProjectsImagesByStreamViewType(t *testing.T) {
	cases := []struct {
		view          string
		wantModifyOld bool
		wantModifyNew bool
	}{
		{view: types.StreamViewTypeKeysOnly},
		{view: types.StreamViewTypeNewImage, wantModifyNew: true},
		{view: types.StreamViewTypeOldImage, wantModifyOld: true},
		{view: types.StreamViewTypeNewAndOldImages, wantModifyOld: true, wantModifyNew: true},
	}
	for i, tc := range cases {
		t.Run(tc.view, func(t *testing.T) {
			srv, db, catStore, cleanup := newStreamIteratorTestServer(t)
			defer cleanup()
			table := fmt.Sprintf("Events%d", i)
			arn := createIteratorStreamWithView(t, srv, table, tc.view)
			appendInsertModifyRemove(t, db, catStore, table)
			iter, err := srv.GetShardIterator(context.Background(), &cefaspb.GetShardIteratorRequest{
				StreamArn:         arn,
				ShardId:           types.StreamShardIDSingle,
				ShardIteratorType: streamIteratorTypeTrimHorizon,
			})
			if err != nil {
				t.Fatalf("iterator: %v", err)
			}
			resp, err := srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{ShardIterator: iter.GetShardIterator()})
			if err != nil {
				t.Fatalf("records: %v", err)
			}
			if len(resp.GetRecords()) != 3 {
				t.Fatalf("record count = %d, want 3", len(resp.GetRecords()))
			}
			insert := resp.GetRecords()[0].GetDynamodb()
			if len(insert.GetOldImage()) != 0 {
				t.Fatalf("insert old image present for %s: %+v", tc.view, insert.GetOldImage())
			}
			remove := resp.GetRecords()[2].GetDynamodb()
			if len(remove.GetNewImage()) != 0 {
				t.Fatalf("remove new image present for %s: %+v", tc.view, remove.GetNewImage())
			}
			modify := resp.GetRecords()[1].GetDynamodb()
			if got := len(modify.GetOldImage()) > 0; got != tc.wantModifyOld {
				t.Fatalf("modify old present = %v, want %v for %s", got, tc.wantModifyOld, tc.view)
			}
			if got := len(modify.GetNewImage()) > 0; got != tc.wantModifyNew {
				t.Fatalf("modify new present = %v, want %v for %s", got, tc.wantModifyNew, tc.view)
			}
		})
	}
}

func TestGetRecordsLimitPaginationAndEmptyPoll(t *testing.T) {
	srv, db, catStore, cleanup := newStreamIteratorTestServer(t)
	defer cleanup()
	arn := createIteratorStream(t, srv)
	appendIteratorStreamRecords(t, db, catStore, 3)

	iter, err := srv.GetShardIterator(context.Background(), &cefaspb.GetShardIteratorRequest{
		StreamArn:         arn,
		ShardId:           types.StreamShardIDSingle,
		ShardIteratorType: streamIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	page1, err := srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{
		ShardIterator: iter.GetShardIterator(),
		Limit:         2,
	})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1.GetRecords()) != 2 ||
		page1.GetRecords()[0].GetDynamodb().GetSequenceNumber() != "1" ||
		page1.GetRecords()[1].GetDynamodb().GetSequenceNumber() != "2" ||
		page1.GetNextShardIterator() == "" {
		t.Fatalf("page1 = %+v", page1)
	}
	page2, err := srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{
		ShardIterator: page1.GetNextShardIterator(),
		Limit:         2,
	})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2.GetRecords()) != 1 || page2.GetRecords()[0].GetDynamodb().GetSequenceNumber() != "3" {
		t.Fatalf("page2 = %+v", page2)
	}
	empty, err := srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{
		ShardIterator: page2.GetNextShardIterator(),
	})
	if err != nil {
		t.Fatalf("empty poll: %v", err)
	}
	if len(empty.GetRecords()) != 0 || empty.GetNextShardIterator() == "" {
		t.Fatalf("empty poll = %+v, want no records with next iterator", empty)
	}
}

func TestGetRecordsClosedShardExhaustionReturnsNoNextIterator(t *testing.T) {
	srv, db, catStore, cleanup := newStreamIteratorTestServer(t)
	defer cleanup()
	arn := createIteratorStream(t, srv)
	appendIteratorStreamRecords(t, db, catStore, 1)
	td, err := catStore.Describe("Events")
	if err != nil {
		t.Fatalf("describe events: %v", err)
	}
	td.StreamSpecification = nil
	if err := catStore.UpdateTable(td); err != nil {
		t.Fatalf("disable stream: %v", err)
	}
	iter, err := srv.GetShardIterator(context.Background(), &cefaspb.GetShardIteratorRequest{
		StreamArn:         arn,
		ShardId:           types.StreamShardIDSingle,
		ShardIteratorType: streamIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	resp, err := srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{ShardIterator: iter.GetShardIterator()})
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(resp.GetRecords()) != 1 || resp.GetNextShardIterator() != "" {
		t.Fatalf("closed shard response = %+v, want one record and empty next", resp)
	}
}

func TestGetRecordsRejectsExpiredMalformedAndTrimmedIterators(t *testing.T) {
	srv, _, _, cleanup := newStreamIteratorTestServer(t)
	defer cleanup()
	arn := createIteratorStream(t, srv)
	now := time.Now()
	expired, err := encodeStreamShardIterator(streamShardIteratorPayload{
		Version:            1,
		StreamArn:          arn,
		ShardID:            types.StreamShardIDSingle,
		NextSequenceNumber: "1",
		IteratorType:       streamIteratorTypeTrimHorizon,
		ExpiresUnixNano:    now.Add(-time.Nanosecond).UnixNano(),
	})
	if err != nil {
		t.Fatalf("encode expired: %v", err)
	}
	_, err = srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{ShardIterator: expired})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expired code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}
	_, err = srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{ShardIterator: "bad-token"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
	trimmed, err := encodeStreamShardIterator(streamShardIteratorPayload{
		Version:            1,
		StreamArn:          arn,
		ShardID:            types.StreamShardIDSingle,
		NextSequenceNumber: "0",
		IteratorType:       streamIteratorTypeTrimHorizon,
		ExpiresUnixNano:    now.Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatalf("encode trimmed: %v", err)
	}
	_, err = srv.GetRecords(context.Background(), &cefaspb.GetRecordsRequest{ShardIterator: trimmed})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("trimmed code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}
}
