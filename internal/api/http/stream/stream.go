// Package stream owns the HTTP handlers for the DynamoDB-Streams-
// compatible endpoints (/v1/ListStreams, /v1/DescribeStream,
// /v1/GetShardIterator, /v1/GetRecords) and the SSE endpoint
// /v1/Stream.
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses
// for every other route. HandleStream is deliberately registered
// without that wrapper — it is a long-lived SSE connection that
// must bypass per-request instrumentation.
//
// The package depends only on:
//
//   - internal/api/streamcore             — shared stream helpers and types
//   - internal/auth                       — scope checks
//   - internal/catalog                    — stream descriptor lookup
//   - internal/storage                    — change-record retrieval
//   - internal/api/http/httpx             — JSON write helpers
//   - pkg/core/model                      — shard-id parsing
//   - pkg/ddbjson                         — attribute encoding
//   - pkg/types                           — wire types + sentinel errors
//
// It deliberately does not import internal/api so the import graph
// stays one-way (api → stream, never the reverse).
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/osvaldoandrade/cefas/internal/api/http/httpx"
	"github.com/osvaldoandrade/cefas/internal/api/streamcore"
	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// ChangeStream is the CDC subset the SSE /v1/Stream handler needs.
// It is the same surface internal/api.ChangeStream exposes; we
// re-declare a minimal interface here so the import direction stays
// one-way (api → stream).
type ChangeStream interface {
	SubscribeChanges(ctx context.Context) (<-chan streamcore.ChangeEvent, func())
}

// IteratorFailureObserver records a GetShardIterator failure for
// metrics. table may be empty when the ARN failed to resolve. nil is
// honoured (no-op).
type IteratorFailureObserver func(table string, err error)

// GetRecordsObserver records a GetRecords outcome for metrics. err is
// nil on success; result.TableName carries the table label.
type GetRecordsObserver func(result streamcore.StreamRecordsResult, err error)

// Handlers carries the dependencies every stream handler needs. Build
// it once during internal/api.Server.Routes and let the server
// register the methods with its existing middleware stack.
type Handlers struct {
	cat                *catalog.Catalog
	db                 *storage.DB
	stream             ChangeStream
	observeIterFailure IteratorFailureObserver
	observeGetRecords  GetRecordsObserver
}

// New constructs the handler set. stream may be nil (HandleStream
// then returns 400 "change stream not configured"). The observer
// callbacks may be nil; the handlers skip recording in that case.
func New(
	cat *catalog.Catalog,
	db *storage.DB,
	stream ChangeStream,
	observeIterFailure IteratorFailureObserver,
	observeGetRecords GetRecordsObserver,
) *Handlers {
	return &Handlers{
		cat:                cat,
		db:                 db,
		stream:             stream,
		observeIterFailure: observeIterFailure,
		observeGetRecords:  observeGetRecords,
	}
}

// ---------- request / response types ----------

type listStreamsRequest struct {
	TableName               string `json:"tableName,omitempty"`
	Limit                   int32  `json:"limit,omitempty"`
	ExclusiveStartStreamArn string `json:"exclusiveStartStreamArn,omitempty"`
}

type streamSummaryResponse struct {
	StreamArn   string `json:"streamArn"`
	StreamLabel string `json:"streamLabel"`
	TableName   string `json:"tableName"`
}

type listStreamsResponse struct {
	Streams                []streamSummaryResponse `json:"streams"`
	LastEvaluatedStreamArn string                  `json:"lastEvaluatedStreamArn,omitempty"`
}

type describeStreamRequest struct {
	StreamArn             string `json:"streamArn"`
	Limit                 int32  `json:"limit,omitempty"`
	ExclusiveStartShardID string `json:"exclusiveStartShardId,omitempty"`
}

type streamDescriptionResponse struct {
	StreamArn               string                        `json:"streamArn"`
	StreamLabel             string                        `json:"streamLabel"`
	StreamStatus            string                        `json:"streamStatus"`
	StreamViewType          string                        `json:"streamViewType"`
	TableName               string                        `json:"tableName"`
	CreationRequestDateTime int64                         `json:"creationRequestDateTime"`
	KeySchema               types.KeySchema               `json:"keySchema"`
	Shards                  []types.StreamShardDescriptor `json:"shards"`
	LastEvaluatedShardID    string                        `json:"lastEvaluatedShardId,omitempty"`
}

type describeStreamResponse struct {
	StreamDescription streamDescriptionResponse `json:"streamDescription"`
}

type getShardIteratorRequest struct {
	StreamArn         string `json:"streamArn"`
	ShardID           string `json:"shardId"`
	ShardIteratorType string `json:"shardIteratorType"`
	SequenceNumber    string `json:"sequenceNumber,omitempty"`
}

type getShardIteratorResponse struct {
	ShardIterator string `json:"shardIterator"`
}

type getRecordsRequest struct {
	ShardIterator string `json:"shardIterator"`
	Limit         int32  `json:"limit,omitempty"`
}

type getRecordsResponse struct {
	Records           []streamRecordHTTP `json:"records"`
	NextShardIterator string             `json:"nextShardIterator,omitempty"`
}

type streamRecordHTTP struct {
	EventID        string               `json:"eventID"`
	EventName      string               `json:"eventName"`
	EventVersion   string               `json:"eventVersion"`
	EventSource    string               `json:"eventSource"`
	EventSourceARN string               `json:"eventSourceARN"`
	AWSRegion      string               `json:"awsRegion"`
	DynamoDB       streamRecordDataHTTP `json:"dynamodb"`
}

type streamRecordDataHTTP struct {
	ApproximateCreationDateTime int64                        `json:"approximateCreationDateTime"`
	Keys                        map[string]ddbjson.Attribute `json:"keys,omitempty"`
	NewImage                    map[string]ddbjson.Attribute `json:"newImage,omitempty"`
	OldImage                    map[string]ddbjson.Attribute `json:"oldImage,omitempty"`
	SequenceNumber              string                       `json:"sequenceNumber"`
	SizeBytes                   int64                        `json:"sizeBytes,omitempty"`
	StreamViewType              string                       `json:"streamViewType"`
}

// ---------- handlers ----------

// HandleListStreams serves POST /v1/ListStreams.
func (h *Handlers) HandleListStreams(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
		return
	}
	var req listStreamsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	streams, err := h.cat.ListStreams(req.TableName)
	if err != nil {
		httpx.WriteErr(w, mapStreamErr(err), err)
		return
	}
	page, lastEvaluated, err := streamcore.PaginateStreamDescriptors(
		streams,
		streamcore.NormalizeStreamAPILimit(req.Limit),
		req.ExclusiveStartStreamArn,
	)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	resp := listStreamsResponse{LastEvaluatedStreamArn: lastEvaluated}
	for _, stream := range page {
		resp.Streams = append(resp.Streams, streamSummaryResponse{
			StreamArn:   stream.StreamArn,
			StreamLabel: stream.StreamLabel,
			TableName:   stream.TableName,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// HandleDescribeStream serves POST /v1/DescribeStream.
func (h *Handlers) HandleDescribeStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
		return
	}
	var req describeStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if req.StreamArn == "" {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("streamArn required"))
		return
	}
	desc, err := h.cat.DescribeStream(req.StreamArn)
	if err != nil {
		httpx.WriteErr(w, mapStreamErr(err), err)
		return
	}
	shards, lastEvaluated, err := streamcore.PaginateStreamShards(
		desc.Shards,
		streamcore.NormalizeStreamAPILimit(req.Limit),
		req.ExclusiveStartShardID,
	)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, describeStreamResponse{
		StreamDescription: streamDescriptionResponse{
			StreamArn:               desc.StreamArn,
			StreamLabel:             desc.StreamLabel,
			StreamStatus:            desc.StreamStatus,
			StreamViewType:          desc.StreamViewType,
			TableName:               desc.TableName,
			CreationRequestDateTime: desc.CreationRequestDateTime,
			KeySchema:               desc.KeySchema,
			Shards:                  shards,
			LastEvaluatedShardID:    lastEvaluated,
		},
	})
}

// HandleGetShardIterator serves POST /v1/GetShardIterator.
func (h *Handlers) HandleGetShardIterator(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
		return
	}
	var req getShardIteratorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	shardID, err := model.NewStreamShardID(req.ShardID)
	if err != nil {
		err = fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
		h.recordIteratorFailure(req.StreamArn, err)
		httpx.WriteErr(w, mapStreamErr(err), err)
		return
	}
	token, err := streamcore.CreateStreamShardIterator(h.cat, h.db, streamcore.CreateIteratorRequest{
		StreamArn:      req.StreamArn,
		ShardID:        shardID,
		IteratorType:   req.ShardIteratorType,
		SequenceNumber: req.SequenceNumber,
	}, time.Now())
	if err != nil {
		h.recordIteratorFailure(req.StreamArn, err)
		httpx.WriteErr(w, mapStreamErr(err), err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, getShardIteratorResponse{ShardIterator: token})
}

// HandleGetRecords serves POST /v1/GetRecords.
func (h *Handlers) HandleGetRecords(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
		return
	}
	var req getRecordsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := streamcore.GetStreamRecords(h.cat, h.db, req.ShardIterator, req.Limit, time.Now())
	if err != nil {
		h.recordGetRecords(result, err)
		httpx.WriteErr(w, mapStreamErr(err), err)
		return
	}
	h.recordGetRecords(result, nil)
	resp := getRecordsResponse{NextShardIterator: result.NextShardIterator}
	for _, record := range result.Records {
		resp.Records = append(resp.Records, streamRecordToHTTP(record))
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// HandleStream is the HTTP/SSE variant of the CDC stream. Clients
// receive `data:` lines with one JSON ChangeEvent each.
//
// It is registered via mux.HandleFunc (not the shared register
// helper) so the long-lived connection bypasses per-request
// instrumentation.
func (h *Handlers) HandleStream(w http.ResponseWriter, r *http.Request) {
	if h.stream == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("change stream not configured"))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpx.WriteErr(w, http.StatusInternalServerError, fmt.Errorf("server does not support streaming"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events, cancel := h.stream.SubscribeChanges(r.Context())
	defer cancel()

	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			fmt.Fprint(w, "data: ")
			_ = enc.Encode(ev) // writes JSON + newline
			flusher.Flush()
		}
	}
}

// ---------- internals ----------

func (h *Handlers) recordIteratorFailure(streamArn string, err error) {
	if h.observeIterFailure == nil {
		return
	}
	h.observeIterFailure(streamcore.StreamTableForARN(h.cat, streamArn), err)
}

func (h *Handlers) recordGetRecords(result streamcore.StreamRecordsResult, err error) {
	if h.observeGetRecords == nil {
		return
	}
	h.observeGetRecords(result, err)
}

func streamRecordToHTTP(record streamcore.StreamRecordEntry) streamRecordHTTP {
	return streamRecordHTTP{
		EventID:        record.EventID,
		EventName:      record.EventName,
		EventVersion:   record.EventVersion,
		EventSource:    record.EventSource,
		EventSourceARN: record.EventSourceARN,
		AWSRegion:      record.AWSRegion,
		DynamoDB: streamRecordDataHTTP{
			ApproximateCreationDateTime: record.DynamoDB.ApproximateCreationDateTime,
			Keys:                        ddbjson.EncodeItem(record.DynamoDB.Keys),
			NewImage:                    ddbjson.EncodeItem(record.DynamoDB.NewImage),
			OldImage:                    ddbjson.EncodeItem(record.DynamoDB.OldImage),
			SequenceNumber:              record.DynamoDB.SequenceNumber,
			SizeBytes:                   record.DynamoDB.SizeBytes,
			StreamViewType:              record.DynamoDB.StreamViewType,
		},
	}
}

// mapStreamErr maps the typed errors the stream helpers return into
// HTTP status codes. It mirrors the relevant arms of the central
// mapWriteErr in internal/api/server.go — stream-only errors live
// here so this package doesn't depend on the full server-side
// mapping (which knows about cluster, raft, storage write errors).
func mapStreamErr(err error) int {
	switch {
	case errors.Is(err, types.ErrStreamNotFound),
		errors.Is(err, types.ErrStreamShardNotFound):
		return http.StatusNotFound
	case errors.Is(err, types.ErrStreamIteratorInvalid):
		return http.StatusBadRequest
	case errors.Is(err, types.ErrStreamIteratorExpired),
		errors.Is(err, types.ErrStreamTrimmed):
		return http.StatusPreconditionFailed
	default:
		return http.StatusInternalServerError
	}
}
