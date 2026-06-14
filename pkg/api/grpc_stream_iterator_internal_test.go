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
	t.Helper()
	resp, err := srv.CreateTable(context.Background(), &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Events",
			KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
			StreamSpecification: &cefaspb.StreamSpecification{
				StreamEnabled:  true,
				StreamViewType: types.StreamViewTypeNewAndOldImages,
			},
		},
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	return resp.GetDescriptor_().GetLatestStreamArn()
}

func appendIteratorStreamRecords(t *testing.T, db *storage.DB, catStore *catalog.Catalog, count int) {
	t.Helper()
	td, err := catStore.Describe("Events")
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
