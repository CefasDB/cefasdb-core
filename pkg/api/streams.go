package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

const (
	maxStreamAPILimit          = 100
	maxGetRecordsLimit         = 1000
	maxGetRecordsResponseBytes = 1 << 20

	streamIteratorTypeTrimHorizon         = "TRIM_HORIZON"
	streamIteratorTypeLatest              = "LATEST"
	streamIteratorTypeAtSequenceNumber    = "AT_SEQUENCE_NUMBER"
	streamIteratorTypeAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"

	streamShardIteratorTokenPrefix = "cefas-stream-iterator-v1"
	streamShardIteratorTTL         = 15 * time.Minute

	streamEventVersion = "1.1"
	streamEventSource  = "cefas:dynamodb-compatible"
	streamAWSRegion    = "local"
)

var streamShardIteratorKey = []byte("cefas-stream-iterator-signing-key-v1")

type streamShardIteratorPayload struct {
	Version            int    `json:"version"`
	StreamArn          string `json:"streamArn"`
	ShardID            string `json:"shardId"`
	NextSequenceNumber string `json:"nextSequenceNumber"`
	IteratorType       string `json:"iteratorType"`
	ExpiresUnixNano    int64  `json:"expiresUnixNano"`
}

type streamRecordEntry struct {
	EventID        string
	EventName      string
	EventVersion   string
	EventSource    string
	EventSourceARN string
	AWSRegion      string
	DynamoDB       streamRecordData
}

type streamRecordData struct {
	ApproximateCreationDateTime int64
	Keys                        types.Item
	NewImage                    types.Item
	OldImage                    types.Item
	SequenceNumber              string
	SizeBytes                   int64
	StreamViewType              string
}

func normalizeStreamAPILimit(limit int32) int {
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

func paginateStreamDescriptors(streams []types.StreamDescriptor, limit int, exclusiveStartStreamARN string) ([]types.StreamDescriptor, string, error) {
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

func paginateStreamShards(shards []types.StreamShardDescriptor, limit int, exclusiveStartShardID string) ([]types.StreamShardDescriptor, string, error) {
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

func createStreamShardIterator(cat *catalog.Catalog, db *storage.DB, streamArn, shardID, iteratorType, sequenceNumber string, now time.Time) (string, error) {
	if streamArn == "" {
		return "", fmt.Errorf("%w: stream_arn required", types.ErrStreamIteratorInvalid)
	}
	if shardID == "" {
		return "", fmt.Errorf("%w: shard_id required", types.ErrStreamIteratorInvalid)
	}
	iteratorType = normalizeStreamIteratorType(iteratorType)
	if iteratorType == "" {
		return "", fmt.Errorf("%w: shard_iterator_type required", types.ErrStreamIteratorInvalid)
	}
	if !validStreamIteratorType(iteratorType) {
		return "", fmt.Errorf("%w: shard_iterator_type %q must be one of %s, %s, %s, %s",
			types.ErrStreamIteratorInvalid,
			iteratorType,
			streamIteratorTypeTrimHorizon,
			streamIteratorTypeLatest,
			streamIteratorTypeAtSequenceNumber,
			streamIteratorTypeAfterSequenceNumber)
	}
	desc, err := cat.DescribeStream(streamArn)
	if err != nil {
		return "", err
	}
	shard, ok := findStreamShard(desc, shardID)
	if !ok {
		return "", types.ErrStreamShardNotFound
	}
	nextSequence, err := resolveShardIteratorNextSequence(db, shard, iteratorType, sequenceNumber)
	if err != nil {
		return "", err
	}
	return encodeStreamShardIterator(streamShardIteratorPayload{
		Version:            1,
		StreamArn:          streamArn,
		ShardID:            shardID,
		NextSequenceNumber: strconv.FormatUint(nextSequence, 10),
		IteratorType:       iteratorType,
		ExpiresUnixNano:    now.Add(streamShardIteratorTTL).UnixNano(),
	})
}

func getStreamRecords(cat *catalog.Catalog, db *storage.DB, shardIterator string, limit int32, now time.Time) ([]streamRecordEntry, string, error) {
	if shardIterator == "" {
		return nil, "", fmt.Errorf("%w: shard_iterator required", types.ErrStreamIteratorInvalid)
	}
	payload, err := decodeStreamShardIterator(shardIterator, now)
	if err != nil {
		return nil, "", err
	}
	desc, err := cat.DescribeStream(payload.StreamArn)
	if err != nil {
		return nil, "", err
	}
	shard, ok := findStreamShard(desc, payload.ShardID)
	if !ok {
		return nil, "", types.ErrStreamShardNotFound
	}
	nextSequence, err := parseStreamSequenceNumber(payload.NextSequenceNumber)
	if err != nil {
		return nil, "", fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
	}
	startSequence, err := parseStreamSequenceNumber(shard.SequenceNumberRange.StartingSequenceNumber)
	if err != nil {
		return nil, "", fmt.Errorf("%w: shard starting sequence: %v", types.ErrStreamIteratorInvalid, err)
	}
	if nextSequence < startSequence {
		return nil, "", types.ErrStreamTrimmed
	}
	var endingSequence uint64
	if shard.SequenceNumberRange.EndingSequenceNumber != "" {
		endingSequence, err = parseStreamSequenceNumber(shard.SequenceNumberRange.EndingSequenceNumber)
		if err != nil {
			return nil, "", fmt.Errorf("%w: shard ending sequence: %v", types.ErrStreamIteratorInvalid, err)
		}
		if nextSequence > endingSequence {
			return nil, "", nil
		}
	}
	records, nextSequence, err := db.StreamRecords(desc.TableName, nextSequence, endingSequence, normalizeGetRecordsLimit(limit), maxGetRecordsResponseBytes)
	if err != nil {
		return nil, "", err
	}
	out := make([]streamRecordEntry, 0, len(records))
	for _, rec := range records {
		out = append(out, streamRecordFromChange(payload.StreamArn, rec))
	}
	if endingSequence > 0 && nextSequence > endingSequence {
		return out, "", nil
	}
	payload.NextSequenceNumber = strconv.FormatUint(nextSequence, 10)
	payload.ExpiresUnixNano = now.Add(streamShardIteratorTTL).UnixNano()
	nextIterator, err := encodeStreamShardIterator(payload)
	if err != nil {
		return nil, "", err
	}
	return out, nextIterator, nil
}

func streamRecordFromChange(streamArn string, rec storage.ChangeRecord) streamRecordEntry {
	sequence := rec.SequenceNumber
	if sequence == "" {
		sequence = strconv.FormatUint(rec.Index, 10)
	}
	return streamRecordEntry{
		EventID:        streamArn + ":" + sequence,
		EventName:      string(rec.EventName),
		EventVersion:   streamEventVersion,
		EventSource:    streamEventSource,
		EventSourceARN: streamArn,
		AWSRegion:      streamAWSRegion,
		DynamoDB: streamRecordData{
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

func normalizeStreamIteratorType(iteratorType string) string {
	return strings.ToUpper(strings.TrimSpace(iteratorType))
}

func validStreamIteratorType(iteratorType string) bool {
	switch iteratorType {
	case streamIteratorTypeTrimHorizon, streamIteratorTypeLatest, streamIteratorTypeAtSequenceNumber, streamIteratorTypeAfterSequenceNumber:
		return true
	default:
		return false
	}
}

func findStreamShard(desc types.StreamDescriptor, shardID string) (types.StreamShardDescriptor, bool) {
	for _, shard := range desc.Shards {
		if shard.ShardID == shardID {
			return shard, true
		}
	}
	return types.StreamShardDescriptor{}, false
}

func resolveShardIteratorNextSequence(db *storage.DB, shard types.StreamShardDescriptor, iteratorType, sequenceNumber string) (uint64, error) {
	start, err := parseStreamSequenceNumber(shard.SequenceNumberRange.StartingSequenceNumber)
	if err != nil {
		return 0, fmt.Errorf("stream shard starting sequence: %w", err)
	}
	switch iteratorType {
	case streamIteratorTypeTrimHorizon:
		return start, nil
	case streamIteratorTypeLatest:
		return latestStreamSequence(db, shard)
	case streamIteratorTypeAtSequenceNumber, streamIteratorTypeAfterSequenceNumber:
		if sequenceNumber == "" {
			return 0, fmt.Errorf("%w: sequence_number required for %s", types.ErrStreamIteratorInvalid, iteratorType)
		}
		seq, err := parseStreamSequenceNumber(sequenceNumber)
		if err != nil {
			return 0, fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
		}
		if seq < start {
			return 0, types.ErrStreamTrimmed
		}
		if iteratorType == streamIteratorTypeAfterSequenceNumber {
			return seq + 1, nil
		}
		return seq, nil
	default:
		return 0, fmt.Errorf("unsupported iterator type %q", iteratorType)
	}
}

func latestStreamSequence(db *storage.DB, shard types.StreamShardDescriptor) (uint64, error) {
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

func encodeStreamShardIterator(payload streamShardIteratorPayload) (string, error) {
	if payload.Version == 0 {
		payload.Version = 1
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal stream iterator: %w", err)
	}
	sig := signStreamShardIterator(raw)
	return streamShardIteratorTokenPrefix + "." +
		base64.RawURLEncoding.EncodeToString(raw) + "." +
		base64.RawURLEncoding.EncodeToString(sig), nil
}

func decodeStreamShardIterator(token string, now time.Time) (streamShardIteratorPayload, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != streamShardIteratorTokenPrefix {
		return streamShardIteratorPayload{}, types.ErrStreamIteratorInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return streamShardIteratorPayload{}, fmt.Errorf("%w: payload", types.ErrStreamIteratorInvalid)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return streamShardIteratorPayload{}, fmt.Errorf("%w: signature", types.ErrStreamIteratorInvalid)
	}
	if !hmac.Equal(sig, signStreamShardIterator(raw)) {
		return streamShardIteratorPayload{}, types.ErrStreamIteratorInvalid
	}
	var payload streamShardIteratorPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return streamShardIteratorPayload{}, fmt.Errorf("%w: payload json", types.ErrStreamIteratorInvalid)
	}
	if payload.Version != 1 ||
		payload.StreamArn == "" ||
		payload.ShardID == "" ||
		payload.NextSequenceNumber == "" ||
		!validStreamIteratorType(payload.IteratorType) ||
		payload.ExpiresUnixNano == 0 {
		return streamShardIteratorPayload{}, types.ErrStreamIteratorInvalid
	}
	if _, err := parseStreamSequenceNumber(payload.NextSequenceNumber); err != nil {
		return streamShardIteratorPayload{}, fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
	}
	if now.UnixNano() > payload.ExpiresUnixNano {
		return streamShardIteratorPayload{}, types.ErrStreamIteratorExpired
	}
	return payload, nil
}

func signStreamShardIterator(raw []byte) []byte {
	mac := hmac.New(sha256.New, streamShardIteratorKey)
	_, _ = mac.Write(raw)
	return mac.Sum(nil)
}
