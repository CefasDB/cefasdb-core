// Package streamcore is the shared, wire-agnostic core of the
// DynamoDB-Streams-compatible surface: iterator-token codec, shard
// pagination, retention math, and the request/response value types
// the HTTP and gRPC layers both project into their respective wire
// shapes.
//
// Both internal/api (gRPC) and internal/api/http/stream (HTTP)
// depend on this package. Neither depends on the other, so this
// package owns the single source of truth for iterator semantics.
package streamcore

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// ChangeEvent is the wire-agnostic shape of a single CDC entry used
// by the SSE /v1/Stream surface. Adapters from raft.ChangeEvent or
// other sources project into this type.
type ChangeEvent struct {
	RaftIndex uint64
	Op        string // "PUT" | "DELETE"
	Key       []byte
	Value     []byte
}

// CreateIteratorRequest packs every input CreateStreamShardIterator
// needs. Replaces the seven-parameter call that violated the §9
// argument-limit cap; also gives StreamShardID a single validation
// site at the boundary instead of repeating "if shardID == ”" in
// every caller.
type CreateIteratorRequest struct {
	StreamArn      string
	ShardID        model.StreamShardID
	IteratorType   string
	SequenceNumber string
}

const (
	maxStreamAPILimit          = 100
	maxGetRecordsLimit         = 1000
	maxGetRecordsResponseBytes = 1 << 20

	// IteratorType* are the DynamoDB-Streams shard-iterator-type
	// strings. Exported so the gRPC iterator test suite can construct
	// requests with the canonical labels.
	IteratorTypeTrimHorizon         = "TRIM_HORIZON"
	IteratorTypeLatest              = "LATEST"
	IteratorTypeAtSequenceNumber    = "AT_SEQUENCE_NUMBER"
	IteratorTypeAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"

	tokenPrefix = "cefas-stream-iterator-v1"
	tokenTTL    = 15 * time.Minute

	// EventVersion, EventSource and AWSRegion are the constant
	// per-record envelope values DynamoDB Streams clients expect to
	// find on every entry. Exported so test code can assert exact
	// matches against them.
	EventVersion = "1.1"
	EventSource  = "cefas:dynamodb-compatible"
	AWSRegion    = "local"
)

var signingKey = []byte("cefas-stream-iterator-signing-key-v1")

// StreamShardIteratorPayload is the decoded body of a shard-iterator
// token. Exported for the internal gRPC test suite, which constructs
// expired/trimmed payloads to assert error mapping.
type StreamShardIteratorPayload struct {
	Version            int                 `json:"version"`
	StreamArn          string              `json:"streamArn"`
	ShardID            model.StreamShardID `json:"shardId"`
	NextSequenceNumber string              `json:"nextSequenceNumber"`
	IteratorType       string              `json:"iteratorType"`
	ExpiresUnixNano    int64               `json:"expiresUnixNano"`
}

// StreamRecordEntry is the wire-agnostic shape of a single CDC entry.
// HTTP and gRPC handlers project this into their respective response
// types.
type StreamRecordEntry struct {
	EventID        string
	EventName      string
	EventVersion   string
	EventSource    string
	EventSourceARN string
	AWSRegion      string
	DynamoDB       StreamRecordData
}

// StreamRecordData mirrors DynamoDB Streams' "dynamodb" sub-object
// for a single record.
type StreamRecordData struct {
	ApproximateCreationDateTime int64
	Keys                        types.Item
	NewImage                    types.Item
	OldImage                    types.Item
	SequenceNumber              string
	SizeBytes                   int64
	StreamViewType              string
}

// StreamRecordsResult is the output of GetStreamRecords: the records
// page plus the next shard iterator (empty when the shard is closed
// and exhausted). TableName lets metric observers tag failures by
// table without re-resolving from the iterator.
type StreamRecordsResult struct {
	TableName         string
	Records           []StreamRecordEntry
	NextShardIterator string
}

// NormalizeStreamAPILimit clamps a wire limit to [1, maxStreamAPILimit].
// A non-positive limit defaults to maxStreamAPILimit so callers don't
// need to special-case "unset".
func NormalizeStreamAPILimit(limit int32) int {
	if limit <= 0 || limit > maxStreamAPILimit {
		return maxStreamAPILimit
	}
	return int(limit)
}

func normalizeGetRecordsLimit(limit int32) int {
	if limit <= 0 || limit > maxGetRecordsLimit {
		return maxGetRecordsLimit
	}
	return int(limit)
}

// PaginateStreamDescriptors slices streams to a limit window starting
// just after exclusiveStartStreamARN. An empty exclusiveStart returns
// the first page. The returned last-evaluated arn is non-empty only
// when more pages exist.
func PaginateStreamDescriptors(streams []types.StreamDescriptor, limit int, exclusiveStartStreamARN string) ([]types.StreamDescriptor, string, error) {
	start := 0
	if exclusiveStartStreamARN != "" {
		found := false
		for i, stream := range streams {
			if stream.StreamArn == exclusiveStartStreamARN {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, "", fmt.Errorf("exclusive_start_stream_arn %q not found", exclusiveStartStreamARN)
		}
	}
	if start >= len(streams) {
		return nil, "", nil
	}
	end := start + limit
	if end >= len(streams) {
		return streams[start:], "", nil
	}
	return streams[start:end], streams[end-1].StreamArn, nil
}

// PaginateStreamShards slices shards the same way
// PaginateStreamDescriptors slices streams.
func PaginateStreamShards(shards []types.StreamShardDescriptor, limit int, exclusiveStartShardID string) ([]types.StreamShardDescriptor, string, error) {
	start := 0
	if exclusiveStartShardID != "" {
		found := false
		for i, shard := range shards {
			if shard.ShardID == exclusiveStartShardID {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, "", fmt.Errorf("exclusive_start_shard_id %q not found", exclusiveStartShardID)
		}
	}
	if start >= len(shards) {
		return nil, "", nil
	}
	end := start + limit
	if end >= len(shards) {
		return shards[start:], "", nil
	}
	return shards[start:end], shards[end-1].ShardID, nil
}

// CreateStreamShardIterator validates req, resolves the shard against
// the catalog and current retention, and returns a signed iterator
// token. Stream-specific failures (unknown ARN, unknown shard,
// trimmed/invalid iterator) return typed errors callers can map to
// the wire status they need.
func CreateStreamShardIterator(cat *catalog.Catalog, db *pebble.DB, req CreateIteratorRequest, now time.Time) (string, error) {
	if req.StreamArn == "" {
		return "", fmt.Errorf("%w: stream_arn required", types.ErrStreamIteratorInvalid)
	}
	iteratorType := normalizeIteratorType(req.IteratorType)
	if iteratorType == "" {
		return "", fmt.Errorf("%w: shard_iterator_type required", types.ErrStreamIteratorInvalid)
	}
	if !validIteratorType(iteratorType) {
		return "", fmt.Errorf("%w: shard_iterator_type %q must be one of %s, %s, %s, %s",
			types.ErrStreamIteratorInvalid,
			iteratorType,
			IteratorTypeTrimHorizon,
			IteratorTypeLatest,
			IteratorTypeAtSequenceNumber,
			IteratorTypeAfterSequenceNumber)
	}
	desc, err := cat.DescribeStream(req.StreamArn)
	if err != nil {
		return "", err
	}
	shard, ok := findStreamShard(desc, req.ShardID)
	if !ok {
		return "", types.ErrStreamShardNotFound
	}
	retention, err := db.PreviewStreamRetention(desc.TableName, now)
	if err != nil {
		return "", err
	}
	nextSequence, err := resolveShardIteratorNextSequence(db, shard, iteratorType, req.SequenceNumber, retention)
	if err != nil {
		return "", err
	}
	return EncodeStreamShardIterator(StreamShardIteratorPayload{
		Version:            1,
		StreamArn:          req.StreamArn,
		ShardID:            req.ShardID,
		NextSequenceNumber: strconv.FormatUint(nextSequence, 10),
		IteratorType:       iteratorType,
		ExpiresUnixNano:    now.Add(tokenTTL).UnixNano(),
	})
}

// GetStreamRecords decodes shardIterator, fetches up to limit records
// from the underlying store, and returns the next iterator token (or
// "" when the shard is closed and exhausted). Stream-specific
// failures return typed errors.
func GetStreamRecords(cat *catalog.Catalog, db *pebble.DB, shardIterator string, limit int32, now time.Time) (StreamRecordsResult, error) {
	if shardIterator == "" {
		return StreamRecordsResult{}, fmt.Errorf("%w: shard_iterator required", types.ErrStreamIteratorInvalid)
	}
	payload, err := DecodeStreamShardIterator(shardIterator, now)
	if err != nil {
		return StreamRecordsResult{}, err
	}
	desc, err := cat.DescribeStream(payload.StreamArn)
	if err != nil {
		return StreamRecordsResult{}, err
	}
	result := StreamRecordsResult{TableName: desc.TableName}
	shard, ok := findStreamShard(desc, payload.ShardID)
	if !ok {
		return result, types.ErrStreamShardNotFound
	}
	retention, err := db.PreviewStreamRetention(desc.TableName, now)
	if err != nil {
		return result, err
	}
	nextSequence, err := parseStreamSequenceNumber(payload.NextSequenceNumber)
	if err != nil {
		return result, fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
	}
	startSequence, err := parseStreamSequenceNumber(shard.SequenceNumberRange.StartingSequenceNumber)
	if err != nil {
		return result, fmt.Errorf("%w: shard starting sequence: %v", types.ErrStreamIteratorInvalid, err)
	}
	trimFloor := streamTrimFloor(startSequence, retention)
	if nextSequence < trimFloor {
		return result, types.ErrStreamTrimmed
	}
	var endingSequence uint64
	if shard.SequenceNumberRange.EndingSequenceNumber != "" {
		endingSequence, err = parseStreamSequenceNumber(shard.SequenceNumberRange.EndingSequenceNumber)
		if err != nil {
			return result, fmt.Errorf("%w: shard ending sequence: %v", types.ErrStreamIteratorInvalid, err)
		}
		if nextSequence > endingSequence {
			return result, nil
		}
	}
	records, nextSequence, err := db.StreamRecords(desc.TableName, nextSequence, endingSequence, normalizeGetRecordsLimit(limit), maxGetRecordsResponseBytes)
	if err != nil {
		return result, err
	}
	out := make([]StreamRecordEntry, 0, len(records))
	for _, rec := range records {
		out = append(out, streamRecordFromChange(payload.StreamArn, rec))
	}
	result.Records = out
	if endingSequence > 0 && nextSequence > endingSequence {
		return result, nil
	}
	payload.NextSequenceNumber = strconv.FormatUint(nextSequence, 10)
	payload.ExpiresUnixNano = now.Add(tokenTTL).UnixNano()
	nextIterator, err := EncodeStreamShardIterator(payload)
	if err != nil {
		return result, err
	}
	result.NextShardIterator = nextIterator
	return result, nil
}

// StreamTableForARN resolves the table that owns streamArn. Used by
// metric observers to tag errors with the correct table label
// without re-decoding the iterator. Returns "" when cat is nil or the
// ARN doesn't resolve.
func StreamTableForARN(cat *catalog.Catalog, streamArn string) string {
	if cat == nil || streamArn == "" {
		return ""
	}
	desc, err := cat.DescribeStream(streamArn)
	if err != nil {
		return ""
	}
	return desc.TableName
}

// StreamErrorReason maps a stream-related error to a short label for
// metrics ("trimmed", "expired", "invalid", "stream_not_found",
// "shard_not_found", "err").
func StreamErrorReason(err error) string {
	switch {
	case errors.Is(err, types.ErrStreamTrimmed):
		return "trimmed"
	case errors.Is(err, types.ErrStreamIteratorExpired):
		return "expired"
	case errors.Is(err, types.ErrStreamIteratorInvalid):
		return "invalid"
	case errors.Is(err, types.ErrStreamNotFound):
		return "stream_not_found"
	case errors.Is(err, types.ErrStreamShardNotFound):
		return "shard_not_found"
	default:
		return "err"
	}
}

func streamRecordFromChange(streamArn string, rec pebble.ChangeRecord) StreamRecordEntry {
	sequence := rec.SequenceNumber
	if sequence == "" {
		sequence = strconv.FormatUint(rec.Index, 10)
	}
	return StreamRecordEntry{
		EventID:        streamArn + ":" + sequence,
		EventName:      string(rec.EventName),
		EventVersion:   EventVersion,
		EventSource:    EventSource,
		EventSourceARN: streamArn,
		AWSRegion:      AWSRegion,
		DynamoDB: StreamRecordData{
			ApproximateCreationDateTime: rec.UnixNano,
			Keys:                        rec.Key,
			NewImage:                    rec.NewItem,
			OldImage:                    rec.OldItem,
			SequenceNumber:              sequence,
			SizeBytes:                   rec.SizeBytes,
			StreamViewType:              rec.StreamViewType,
		},
	}
}

func normalizeIteratorType(iteratorType string) string {
	return strings.ToUpper(strings.TrimSpace(iteratorType))
}

func validIteratorType(iteratorType string) bool {
	switch iteratorType {
	case IteratorTypeTrimHorizon, IteratorTypeLatest, IteratorTypeAtSequenceNumber, IteratorTypeAfterSequenceNumber:
		return true
	default:
		return false
	}
}

func findStreamShard(desc types.StreamDescriptor, shardID model.StreamShardID) (types.StreamShardDescriptor, bool) {
	wanted := shardID.String()
	for _, shard := range desc.Shards {
		if shard.ShardID == wanted {
			return shard, true
		}
	}
	return types.StreamShardDescriptor{}, false
}

func resolveShardIteratorNextSequence(db *pebble.DB, shard types.StreamShardDescriptor, iteratorType, sequenceNumber string, retention pebble.StreamRetentionStats) (uint64, error) {
	start, err := parseStreamSequenceNumber(shard.SequenceNumberRange.StartingSequenceNumber)
	if err != nil {
		return 0, fmt.Errorf("stream shard starting sequence: %w", err)
	}
	trimFloor := streamTrimFloor(start, retention)
	switch iteratorType {
	case IteratorTypeTrimHorizon:
		return trimFloor, nil
	case IteratorTypeLatest:
		return latestStreamSequence(db, shard)
	case IteratorTypeAtSequenceNumber, IteratorTypeAfterSequenceNumber:
		if sequenceNumber == "" {
			return 0, fmt.Errorf("%w: sequence_number required for %s", types.ErrStreamIteratorInvalid, iteratorType)
		}
		seq, err := parseStreamSequenceNumber(sequenceNumber)
		if err != nil {
			return 0, fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
		}
		if seq < trimFloor {
			return 0, types.ErrStreamTrimmed
		}
		if iteratorType == IteratorTypeAfterSequenceNumber {
			return seq + 1, nil
		}
		return seq, nil
	default:
		return 0, fmt.Errorf("unsupported iterator type %q", iteratorType)
	}
}

func streamTrimFloor(shardStart uint64, retention pebble.StreamRetentionStats) uint64 {
	if retention.OldestSequence > shardStart {
		return retention.OldestSequence
	}
	return shardStart
}

func latestStreamSequence(db *pebble.DB, shard types.StreamShardDescriptor) (uint64, error) {
	if shard.SequenceNumberRange.EndingSequenceNumber != "" {
		ending, err := parseStreamSequenceNumber(shard.SequenceNumberRange.EndingSequenceNumber)
		if err != nil {
			return 0, fmt.Errorf("stream shard ending sequence: %w", err)
		}
		return ending + 1, nil
	}
	current, err := db.CurrentChangeIndex()
	if err != nil {
		return 0, err
	}
	return current + 1, nil
}

func parseStreamSequenceNumber(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("sequence_number required")
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("sequence_number %q must be a base-10 uint64", raw)
	}
	return n, nil
}

// EncodeStreamShardIterator marshals payload and signs it with the
// package-level HMAC key. Exported so tests can construct expired or
// otherwise hostile tokens without going through CreateStreamShardIterator.
func EncodeStreamShardIterator(payload StreamShardIteratorPayload) (string, error) {
	if payload.Version == 0 {
		payload.Version = 1
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal stream iterator: %w", err)
	}
	sig := signStreamShardIterator(raw)
	return tokenPrefix + "." +
		base64.RawURLEncoding.EncodeToString(raw) + "." +
		base64.RawURLEncoding.EncodeToString(sig), nil
}

// DecodeStreamShardIterator verifies the HMAC signature, unmarshals
// the payload, and validates it (version, required fields,
// expiration). Returns types.ErrStreamIteratorInvalid for malformed
// tokens and types.ErrStreamIteratorExpired when the token's TTL has
// passed.
func DecodeStreamShardIterator(token string, now time.Time) (StreamShardIteratorPayload, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != tokenPrefix {
		return StreamShardIteratorPayload{}, types.ErrStreamIteratorInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return StreamShardIteratorPayload{}, fmt.Errorf("%w: payload", types.ErrStreamIteratorInvalid)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return StreamShardIteratorPayload{}, fmt.Errorf("%w: signature", types.ErrStreamIteratorInvalid)
	}
	if !hmac.Equal(sig, signStreamShardIterator(raw)) {
		return StreamShardIteratorPayload{}, types.ErrStreamIteratorInvalid
	}
	var payload StreamShardIteratorPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return StreamShardIteratorPayload{}, fmt.Errorf("%w: payload json", types.ErrStreamIteratorInvalid)
	}
	if payload.Version != 1 ||
		payload.StreamArn == "" ||
		payload.ShardID.String() == "" ||
		payload.NextSequenceNumber == "" ||
		!validIteratorType(payload.IteratorType) ||
		payload.ExpiresUnixNano == 0 {
		return StreamShardIteratorPayload{}, types.ErrStreamIteratorInvalid
	}
	if _, err := parseStreamSequenceNumber(payload.NextSequenceNumber); err != nil {
		return StreamShardIteratorPayload{}, fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
	}
	if now.UnixNano() > payload.ExpiresUnixNano {
		return StreamShardIteratorPayload{}, types.ErrStreamIteratorExpired
	}
	return payload, nil
}

func signStreamShardIterator(raw []byte) []byte {
	mac := hmac.New(sha256.New, signingKey)
	_, _ = mac.Write(raw)
	return mac.Sum(nil)
}
