package client

import (
	"context"
	"net/http"

	"github.com/CefasDb/cefasdb/pkg/encoding/ddbjson"
)

func (c *HTTPClient) ListStreams(ctx context.Context, opts ListStreamsOptions) (ListStreamsResult, error) {
	var out ListStreamsResult
	err := c.request(ctx, http.MethodPost, "/v1/ListStreams", map[string]any{"tableName": opts.TableName, "limit": opts.Limit, "exclusiveStartStreamArn": opts.ExclusiveStartStreamARN}, &out)
	return out, err
}

func (c *HTTPClient) DescribeStream(ctx context.Context, arn string, opts DescribeStreamOptions) (StreamDescription, error) {
	var out struct {
		StreamDescription StreamDescription `json:"streamDescription"`
	}
	err := c.request(ctx, http.MethodPost, "/v1/DescribeStream", map[string]any{"streamArn": arn, "limit": opts.Limit, "exclusiveStartShardId": opts.ExclusiveStartShardID}, &out)
	return out.StreamDescription, err
}

func (c *HTTPClient) GetShardIterator(ctx context.Context, opts GetShardIteratorOptions) (string, error) {
	var out struct {
		ShardIterator string `json:"shardIterator"`
	}
	err := c.request(ctx, http.MethodPost, "/v1/GetShardIterator", map[string]any{"streamArn": opts.StreamArn, "shardId": opts.ShardID, "shardIteratorType": opts.ShardIteratorType, "sequenceNumber": opts.SequenceNumber}, &out)
	return out.ShardIterator, err
}

func (c *HTTPClient) GetRecords(ctx context.Context, iterator string, opts GetRecordsOptions) (GetRecordsResult, error) {
	var wire struct {
		Records []struct {
			EventID        string `json:"eventID"`
			EventName      string `json:"eventName"`
			EventVersion   string `json:"eventVersion"`
			EventSource    string `json:"eventSource"`
			EventSourceARN string `json:"eventSourceARN"`
			AWSRegion      string `json:"awsRegion"`
			DynamoDB       struct {
				ApproximateCreationDateTime int64                        `json:"approximateCreationDateTime"`
				Keys                        map[string]ddbjson.Attribute `json:"keys"`
				NewImage                    map[string]ddbjson.Attribute `json:"newImage"`
				OldImage                    map[string]ddbjson.Attribute `json:"oldImage"`
				SequenceNumber              string                       `json:"sequenceNumber"`
				SizeBytes                   int64                        `json:"sizeBytes"`
				StreamViewType              string                       `json:"streamViewType"`
			} `json:"dynamodb"`
		} `json:"records"`
		NextShardIterator         string `json:"nextShardIterator"`
		LastScannedSequenceNumber string `json:"lastScannedSequenceNumber"`
	}
	if err := c.request(ctx, http.MethodPost, "/v1/GetRecords", map[string]any{"shardIterator": iterator, "limit": opts.Limit}, &wire); err != nil {
		return GetRecordsResult{}, err
	}
	out := GetRecordsResult{NextShardIterator: wire.NextShardIterator, LastScannedSequenceNumber: wire.LastScannedSequenceNumber}
	for _, rec := range wire.Records {
		keys, err := ddbjson.DecodeItem(rec.DynamoDB.Keys)
		if err != nil {
			return GetRecordsResult{}, err
		}
		newImage, err := ddbjson.DecodeItem(rec.DynamoDB.NewImage)
		if err != nil {
			return GetRecordsResult{}, err
		}
		oldImage, err := ddbjson.DecodeItem(rec.DynamoDB.OldImage)
		if err != nil {
			return GetRecordsResult{}, err
		}
		out.Records = append(out.Records, StreamRecord{EventID: rec.EventID, EventName: rec.EventName, EventVersion: rec.EventVersion, EventSource: rec.EventSource, EventSourceARN: rec.EventSourceARN, AWSRegion: rec.AWSRegion, DynamoDB: StreamRecordData{ApproximateCreationDateTime: rec.DynamoDB.ApproximateCreationDateTime, Keys: keys, NewImage: newImage, OldImage: oldImage, SequenceNumber: rec.DynamoDB.SequenceNumber, SizeBytes: rec.DynamoDB.SizeBytes, StreamViewType: rec.DynamoDB.StreamViewType}})
	}
	return out, nil
}
