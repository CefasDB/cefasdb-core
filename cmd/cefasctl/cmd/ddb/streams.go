package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func registerListStreams(root *cobra.Command) {
	var (
		tableName               string
		limit                   int
		exclusiveStartStreamARN string
	)
	c := &cobra.Command{
		Use:   "list-streams",
		Short: "List DynamoDB-compatible table streams",
		Long: `Mirrors aws dynamodbstreams list-streams.

Example:
  cefas list-streams --table-name Events --limit 25`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			resp, err := cli.ListStreams(ctx, client.ListStreamsOptions{
				TableName:               tableName,
				Limit:                   limit,
				ExclusiveStartStreamARN: exclusiveStartStreamARN,
			})
			if err != nil {
				return fmt.Errorf("list streams: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(listStreamsOutputFromSDK(resp))
		},
	}
	f := c.Flags()
	f.StringVar(&tableName, "table-name", "", "Filter streams by table name")
	f.IntVar(&limit, "limit", 0, "Maximum stream summaries to return")
	f.StringVar(&exclusiveStartStreamARN, "exclusive-start-stream-arn", "", "Stream ARN to continue after")
	root.AddCommand(c)
}

func registerDescribeStream(root *cobra.Command) {
	var (
		streamARN             string
		limit                 int
		exclusiveStartShardID string
	)
	c := &cobra.Command{
		Use:   "describe-stream",
		Short: "Describe a DynamoDB-compatible table stream",
		Long: `Mirrors aws dynamodbstreams describe-stream.

Example:
  cefas describe-stream --stream-arn arn:cefas:dynamodb:local:000000000000:table/Events/stream/123`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if streamARN == "" {
				return fmt.Errorf("--stream-arn is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			desc, err := cli.DescribeStream(ctx, streamARN, client.DescribeStreamOptions{
				Limit:                 limit,
				ExclusiveStartShardID: exclusiveStartShardID,
			})
			if err != nil {
				return fmt.Errorf("describe stream: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(describeStreamOutputFromSDK(desc))
		},
	}
	f := c.Flags()
	f.StringVar(&streamARN, "stream-arn", "", "Target stream ARN (required)")
	f.IntVar(&limit, "limit", 0, "Maximum shards to return")
	f.StringVar(&exclusiveStartShardID, "exclusive-start-shard-id", "", "Shard ID to continue after")
	_ = c.MarkFlagRequired("stream-arn")
	root.AddCommand(c)
}

func registerGetShardIterator(root *cobra.Command) {
	var (
		streamARN         string
		shardID           string
		shardIteratorType string
		sequenceNumber    string
	)
	c := &cobra.Command{
		Use:   "get-shard-iterator",
		Short: "Create a DynamoDB-compatible stream shard iterator",
		Long: `Mirrors aws dynamodbstreams get-shard-iterator.

Example:
  cefas get-shard-iterator \
    --stream-arn arn:cefas:dynamodb:local:000000000000:table/Events/stream/123 \
    --shard-id shardId-000000000000 \
    --shard-iterator-type TRIM_HORIZON`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if streamARN == "" {
				return fmt.Errorf("--stream-arn is required")
			}
			if shardID == "" {
				return fmt.Errorf("--shard-id is required")
			}
			if shardIteratorType == "" {
				return fmt.Errorf("--shard-iterator-type is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			iterator, err := cli.GetShardIterator(ctx, client.GetShardIteratorOptions{
				StreamArn:         streamARN,
				ShardID:           shardID,
				ShardIteratorType: shardIteratorType,
				SequenceNumber:    sequenceNumber,
			})
			if err != nil {
				return fmt.Errorf("get shard iterator: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"ShardIterator": iterator,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&streamARN, "stream-arn", "", "Target stream ARN (required)")
	f.StringVar(&shardID, "shard-id", "", "Target shard ID (required)")
	f.StringVar(&shardIteratorType, "shard-iterator-type", "", "TRIM_HORIZON | LATEST | AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER (required)")
	f.StringVar(&sequenceNumber, "sequence-number", "", "Sequence number for AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER")
	_ = c.MarkFlagRequired("stream-arn")
	_ = c.MarkFlagRequired("shard-id")
	_ = c.MarkFlagRequired("shard-iterator-type")
	root.AddCommand(c)
}

func registerGetRecords(root *cobra.Command) {
	var (
		shardIterator string
		limit         int
	)
	c := &cobra.Command{
		Use:   "get-records",
		Short: "Read records from a DynamoDB-compatible stream shard iterator",
		Long: `Mirrors aws dynamodbstreams get-records.

Example:
  cefas get-records --shard-iterator <token> --limit 100`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if shardIterator == "" {
				return fmt.Errorf("--shard-iterator is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			resp, err := cli.GetRecords(ctx, shardIterator, client.GetRecordsOptions{Limit: limit})
			if err != nil {
				return fmt.Errorf("get records: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(getRecordsOutputFromSDK(resp))
		},
	}
	f := c.Flags()
	f.StringVar(&shardIterator, "shard-iterator", "", "Shard iterator token (required)")
	f.IntVar(&limit, "limit", 0, "Maximum records to return")
	_ = c.MarkFlagRequired("shard-iterator")
	root.AddCommand(c)
}

type listStreamsOutput struct {
	Streams                []streamSummaryOutput `json:"Streams"`
	LastEvaluatedStreamArn string                `json:"LastEvaluatedStreamArn,omitempty"`
}

type streamSummaryOutput struct {
	StreamArn   string `json:"StreamArn"`
	StreamLabel string `json:"StreamLabel"`
	TableName   string `json:"TableName"`
}

type describeStreamOutput struct {
	StreamDescription streamDescriptionOutput `json:"StreamDescription"`
}

type streamDescriptionOutput struct {
	StreamArn               string              `json:"StreamArn"`
	StreamLabel             string              `json:"StreamLabel"`
	StreamStatus            string              `json:"StreamStatus"`
	StreamViewType          string              `json:"StreamViewType"`
	TableName               string              `json:"TableName"`
	CreationRequestDateTime int64               `json:"CreationRequestDateTime"`
	KeySchema               []map[string]string `json:"KeySchema"`
	Shards                  []streamShardOutput `json:"Shards"`
	LastEvaluatedShardId    string              `json:"LastEvaluatedShardId,omitempty"`
}

type streamShardOutput struct {
	ShardId             string                    `json:"ShardId"`
	SequenceNumberRange streamSequenceRangeOutput `json:"SequenceNumberRange"`
}

type streamSequenceRangeOutput struct {
	StartingSequenceNumber string `json:"StartingSequenceNumber,omitempty"`
	EndingSequenceNumber   string `json:"EndingSequenceNumber,omitempty"`
}

type getRecordsOutput struct {
	Records           []streamRecordOutput `json:"Records"`
	NextShardIterator string               `json:"NextShardIterator,omitempty"`
}

type streamRecordOutput struct {
	EventID        string                 `json:"eventID"`
	EventName      string                 `json:"eventName"`
	EventVersion   string                 `json:"eventVersion"`
	EventSource    string                 `json:"eventSource"`
	EventSourceARN string                 `json:"eventSourceARN"`
	AWSRegion      string                 `json:"awsRegion"`
	DynamoDB       streamRecordDataOutput `json:"dynamodb"`
}

type streamRecordDataOutput struct {
	ApproximateCreationDateTime int64                        `json:"ApproximateCreationDateTime"`
	Keys                        map[string]ddbjson.Attribute `json:"Keys,omitempty"`
	NewImage                    map[string]ddbjson.Attribute `json:"NewImage,omitempty"`
	OldImage                    map[string]ddbjson.Attribute `json:"OldImage,omitempty"`
	SequenceNumber              string                       `json:"SequenceNumber"`
	SizeBytes                   int64                        `json:"SizeBytes,omitempty"`
	StreamViewType              string                       `json:"StreamViewType"`
}

func listStreamsOutputFromSDK(resp client.ListStreamsResult) listStreamsOutput {
	out := listStreamsOutput{
		Streams:                make([]streamSummaryOutput, 0, len(resp.Streams)),
		LastEvaluatedStreamArn: resp.LastEvaluatedStreamARN,
	}
	for _, stream := range resp.Streams {
		out.Streams = append(out.Streams, streamSummaryOutput{
			StreamArn:   stream.StreamArn,
			StreamLabel: stream.StreamLabel,
			TableName:   stream.TableName,
		})
	}
	return out
}

func describeStreamOutputFromSDK(desc client.StreamDescription) describeStreamOutput {
	out := streamDescriptionOutput{
		StreamArn:               desc.StreamArn,
		StreamLabel:             desc.StreamLabel,
		StreamStatus:            desc.StreamStatus,
		StreamViewType:          desc.StreamViewType,
		TableName:               desc.TableName,
		CreationRequestDateTime: desc.CreationRequestDateTime,
		KeySchema:               keySchemaWire(desc.KeySchema.PK, desc.KeySchema.SK),
		Shards:                  make([]streamShardOutput, 0, len(desc.Shards)),
		LastEvaluatedShardId:    desc.LastEvaluatedShardID,
	}
	for _, shard := range desc.Shards {
		out.Shards = append(out.Shards, streamShardOutputFromSDK(shard))
	}
	return describeStreamOutput{StreamDescription: out}
}

func streamShardOutputFromSDK(shard types.StreamShardDescriptor) streamShardOutput {
	return streamShardOutput{
		ShardId: shard.ShardID,
		SequenceNumberRange: streamSequenceRangeOutput{
			StartingSequenceNumber: shard.SequenceNumberRange.StartingSequenceNumber,
			EndingSequenceNumber:   shard.SequenceNumberRange.EndingSequenceNumber,
		},
	}
}

func getRecordsOutputFromSDK(resp client.GetRecordsResult) getRecordsOutput {
	out := getRecordsOutput{
		Records:           make([]streamRecordOutput, 0, len(resp.Records)),
		NextShardIterator: resp.NextShardIterator,
	}
	for _, record := range resp.Records {
		out.Records = append(out.Records, streamRecordOutputFromSDK(record))
	}
	return out
}

func streamRecordOutputFromSDK(record client.StreamRecord) streamRecordOutput {
	return streamRecordOutput{
		EventID:        record.EventID,
		EventName:      record.EventName,
		EventVersion:   record.EventVersion,
		EventSource:    record.EventSource,
		EventSourceARN: record.EventSourceARN,
		AWSRegion:      record.AWSRegion,
		DynamoDB: streamRecordDataOutput{
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
