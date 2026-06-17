// streams.go used to own the shared DynamoDB-Streams iterator codec
// and the wire-agnostic record types. Those helpers now live in
// internal/api/streamcore so the new HTTP handler package
// (internal/api/http/stream) and the gRPC server in this package
// can share a single source of truth without an import cycle.
//
// This file keeps a thin set of in-package aliases so existing call
// sites in internal/api (grpc_streams.go, grpc_codec.go,
// stream_metrics.go, grpc_stream_iterator_internal_test.go) can
// reference the helpers under their familiar exported names while
// they're being refactored.
package server

import (
	"github.com/CefasDb/cefasdb/internal/server/streamcore"
)

// Iterator-type string constants are part of the wire vocabulary
// shared by the in-package gRPC iterator test suite. They forward to
// streamcore so the canonical definitions stay in one place.
const (
	streamIteratorTypeTrimHorizon         = streamcore.IteratorTypeTrimHorizon
	streamIteratorTypeLatest              = streamcore.IteratorTypeLatest
	streamIteratorTypeAtSequenceNumber    = streamcore.IteratorTypeAtSequenceNumber
	streamIteratorTypeAfterSequenceNumber = streamcore.IteratorTypeAfterSequenceNumber

	streamEventVersion = streamcore.EventVersion
	streamEventSource  = streamcore.EventSource
	streamAWSRegion    = streamcore.AWSRegion
)

// CreateIteratorRequest is the streamcore type, re-exported so
// in-package gRPC code reads naturally.
type CreateIteratorRequest = streamcore.CreateIteratorRequest

// StreamRecordEntry is the streamcore type, re-exported.
type StreamRecordEntry = streamcore.StreamRecordEntry

// StreamRecordData is the streamcore type, re-exported.
type StreamRecordData = streamcore.StreamRecordData

// StreamRecordsResult is the streamcore type, re-exported.
type StreamRecordsResult = streamcore.StreamRecordsResult

// streamShardIteratorPayload is the legacy in-package alias used by
// the internal gRPC iterator test. Kept unexported because external
// users go through streamcore.
type streamShardIteratorPayload = streamcore.StreamShardIteratorPayload

// NormalizeStreamAPILimit forwards to streamcore.
func NormalizeStreamAPILimit(limit int32) int {
	return streamcore.NormalizeStreamAPILimit(limit)
}

// PaginateStreamDescriptors forwards to streamcore.
var PaginateStreamDescriptors = streamcore.PaginateStreamDescriptors

// PaginateStreamShards forwards to streamcore.
var PaginateStreamShards = streamcore.PaginateStreamShards

// CreateStreamShardIterator forwards to streamcore.
var CreateStreamShardIterator = streamcore.CreateStreamShardIterator

// GetStreamRecords forwards to streamcore.
var GetStreamRecords = streamcore.GetStreamRecords

// StreamTableForARN forwards to streamcore.
var StreamTableForARN = streamcore.StreamTableForARN

// streamErrorReason forwards to streamcore (used by stream_metrics.go).
var streamErrorReason = streamcore.StreamErrorReason

// encodeStreamShardIterator and decodeStreamShardIterator are kept
// as in-package aliases so grpc_stream_iterator_internal_test.go,
// which constructs hostile payloads to exercise error paths, doesn't
// need to import streamcore directly.
var (
	encodeStreamShardIterator = streamcore.EncodeStreamShardIterator
	decodeStreamShardIterator = streamcore.DecodeStreamShardIterator
)
