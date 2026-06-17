package client

import (
	"context"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// ListStreamsOptions mirrors DynamoDB Streams ListStreams pagination.
type ListStreamsOptions struct {
	TableName               string
	Limit                   int
	ExclusiveStartStreamARN string
}

// StreamSummary is the compact stream reference returned by ListStreams.
type StreamSummary struct {
	StreamArn   string
	StreamLabel string
	TableName   string
}

// ListStreamsResult is the typed SDK envelope for ListStreams.
type ListStreamsResult struct {
	Streams                []StreamSummary
	LastEvaluatedStreamARN string
}

// DescribeStreamOptions mirrors DynamoDB Streams DescribeStream pagination.
type DescribeStreamOptions struct {
	Limit                 int
	ExclusiveStartShardID string
}

// StreamDescription includes the public stream descriptor plus shard pagination.
type StreamDescription struct {
	StreamArn               string
	StreamLabel             string
	StreamStatus            string
	StreamViewType          string
	TableName               string
	CreationRequestDateTime int64
	KeySchema               types.KeySchema
	Shards                  []types.StreamShardDescriptor
	LastEvaluatedShardID    string
}

// GetShardIteratorOptions identifies the stream shard position to read from.
type GetShardIteratorOptions struct {
	StreamArn         string
	ShardID           string
	ShardIteratorType string
	SequenceNumber    string
}

// GetRecordsOptions controls the number of stream records returned by one poll.
type GetRecordsOptions struct {
	Limit int
}

// StreamRecordData mirrors the DynamoDB Streams record payload.
type StreamRecordData struct {
	ApproximateCreationDateTime int64
	Keys                        types.Item
	NewImage                    types.Item
	OldImage                    types.Item
	SequenceNumber              string
	SizeBytes                   int64
	StreamViewType              string
}

// StreamRecord is a DynamoDB Streams compatible change event.
type StreamRecord struct {
	EventID        string
	EventName      string
	EventVersion   string
	EventSource    string
	EventSourceARN string
	AWSRegion      string
	DynamoDB       StreamRecordData
}

// GetRecordsResult is the typed SDK envelope for GetRecords.
type GetRecordsResult struct {
	Records           []StreamRecord
	NextShardIterator string
}

// ListStreams returns stream summaries, optionally filtered by table.
func (c *Client) ListStreams(ctx context.Context, opts ListStreamsOptions) (ListStreamsResult, error) {
	resp, err := c.stub.ListStreams(c.withAuth(ctx), &cefaspb.ListStreamsRequest{
		TableName:               opts.TableName,
		Limit:                   int32(opts.Limit),
		ExclusiveStartStreamArn: opts.ExclusiveStartStreamARN,
	})
	if err != nil {
		return ListStreamsResult{}, err
	}
	out := ListStreamsResult{
		Streams:                make([]StreamSummary, 0, len(resp.GetStreams())),
		LastEvaluatedStreamARN: resp.GetLastEvaluatedStreamArn(),
	}
	for _, stream := range resp.GetStreams() {
		out.Streams = append(out.Streams, streamSummaryFromPB(stream))
	}
	return out, nil
}

// DescribeStream returns stream metadata and shard page data for streamArn.
func (c *Client) DescribeStream(ctx context.Context, streamArn string, opts DescribeStreamOptions) (StreamDescription, error) {
	resp, err := c.stub.DescribeStream(c.withAuth(ctx), &cefaspb.DescribeStreamRequest{
		StreamArn:             streamArn,
		Limit:                 int32(opts.Limit),
		ExclusiveStartShardId: opts.ExclusiveStartShardID,
	})
	if err != nil {
		return StreamDescription{}, err
	}
	return streamDescriptionFromPB(resp.GetStreamDescription()), nil
}

// GetShardIterator returns a signed iterator token for a stream shard.
func (c *Client) GetShardIterator(ctx context.Context, opts GetShardIteratorOptions) (string, error) {
	resp, err := c.stub.GetShardIterator(c.withAuth(ctx), &cefaspb.GetShardIteratorRequest{
		StreamArn:         opts.StreamArn,
		ShardId:           opts.ShardID,
		ShardIteratorType: opts.ShardIteratorType,
		SequenceNumber:    opts.SequenceNumber,
	})
	if err != nil {
		return "", err
	}
	return resp.GetShardIterator(), nil
}

// GetRecords reads change records from a shard iterator.
func (c *Client) GetRecords(ctx context.Context, shardIterator string, opts GetRecordsOptions) (GetRecordsResult, error) {
	resp, err := c.stub.GetRecords(c.withAuth(ctx), &cefaspb.GetRecordsRequest{
		ShardIterator: shardIterator,
		Limit:         int32(opts.Limit),
	})
	if err != nil {
		return GetRecordsResult{}, err
	}
	out := GetRecordsResult{
		Records:           make([]StreamRecord, 0, len(resp.GetRecords())),
		NextShardIterator: resp.GetNextShardIterator(),
	}
	for _, record := range resp.GetRecords() {
		out.Records = append(out.Records, streamRecordFromPB(record))
	}
	return out, nil
}

func streamSummaryFromPB(pb *cefaspb.StreamSummary) StreamSummary {
	if pb == nil {
		return StreamSummary{}
	}
	return StreamSummary{
		StreamArn:   pb.GetStreamArn(),
		StreamLabel: pb.GetStreamLabel(),
		TableName:   pb.GetTableName(),
	}
}

func streamDescriptionFromPB(pb *cefaspb.StreamDescription) StreamDescription {
	if pb == nil {
		return StreamDescription{}
	}
	out := StreamDescription{
		StreamArn:               pb.GetStreamArn(),
		StreamLabel:             pb.GetStreamLabel(),
		StreamStatus:            pb.GetStreamStatus(),
		StreamViewType:          pb.GetStreamViewType(),
		TableName:               pb.GetTableName(),
		CreationRequestDateTime: pb.GetCreationRequestDateTime(),
		LastEvaluatedShardID:    pb.GetLastEvaluatedShardId(),
	}
	if ks := pb.GetKeySchema(); ks != nil {
		out.KeySchema = types.KeySchema{PK: ks.GetPk(), SK: ks.GetSk()}
	}
	for _, shard := range pb.GetShards() {
		out.Shards = append(out.Shards, streamShardFromPB(shard))
	}
	return out
}

func streamShardFromPB(pb *cefaspb.StreamShard) types.StreamShardDescriptor {
	if pb == nil {
		return types.StreamShardDescriptor{}
	}
	out := types.StreamShardDescriptor{ShardID: pb.GetShardId()}
	if r := pb.GetSequenceNumberRange(); r != nil {
		out.SequenceNumberRange = types.StreamSequenceNumberRange{
			StartingSequenceNumber: r.GetStartingSequenceNumber(),
			EndingSequenceNumber:   r.GetEndingSequenceNumber(),
		}
	}
	return out
}

func streamRecordFromPB(pb *cefaspb.StreamRecordEntry) StreamRecord {
	if pb == nil {
		return StreamRecord{}
	}
	return StreamRecord{
		EventID:        pb.GetEventId(),
		EventName:      pb.GetEventName(),
		EventVersion:   pb.GetEventVersion(),
		EventSource:    pb.GetEventSource(),
		EventSourceARN: pb.GetEventSourceArn(),
		AWSRegion:      pb.GetAwsRegion(),
		DynamoDB:       streamRecordDataFromPB(pb.GetDynamodb()),
	}
}

func streamRecordDataFromPB(pb *cefaspb.StreamRecordData) StreamRecordData {
	if pb == nil {
		return StreamRecordData{}
	}
	return StreamRecordData{
		ApproximateCreationDateTime: pb.GetApproximateCreationDateTime(),
		Keys:                        itemFromPB(pb.GetKeys()),
		NewImage:                    itemFromPB(pb.GetNewImage()),
		OldImage:                    itemFromPB(pb.GetOldImage()),
		SequenceNumber:              pb.GetSequenceNumber(),
		SizeBytes:                   pb.GetSizeBytes(),
		StreamViewType:              pb.GetStreamViewType(),
	}
}
