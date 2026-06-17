package ddb

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/internal/compat/ddbjson"
)

// batchWriteRequest mirrors a single entry in the aws-cli
// --request-items array. Exactly one of Put / Delete is set.
type batchWriteRequest struct {
	PutRequest *struct {
		Item map[string]ddbjson.Attribute `json:"Item"`
	} `json:"PutRequest,omitempty"`
	DeleteRequest *struct {
		Key map[string]ddbjson.Attribute `json:"Key"`
	} `json:"DeleteRequest,omitempty"`
}

func registerBatchWriteItem(root *cobra.Command) {
	var (
		table       string
		requestsArg string
	)
	c := &cobra.Command{
		Use:   "batch-write-item",
		Short: "Apply many put/delete operations against one table",
		Long: `Applies a mix of PutRequest and DeleteRequest entries against a
single table. The cefas gRPC surface is single-table, so --table-name
is required (aws-cli wraps the same shape in a per-table map).

Example:
  cefas batch-write-item --table-name Users --request-items '[
    {"PutRequest": {"Item": {"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"},"name":{"S":"Ova"}}}},
    {"DeleteRequest": {"Key": {"pk":{"S":"USER#2"},"sk":{"S":"PROFILE"}}}}
  ]'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if requestsArg == "" {
				return fmt.Errorf("--request-items is required")
			}
			raw, err := fileloader.Load(requestsArg)
			if err != nil {
				return err
			}
			ops, err := parseBatchRequests(raw)
			if err != nil {
				return fmt.Errorf("--request-items: %w", err)
			}
			if len(ops) == 0 {
				return fmt.Errorf("--request-items: empty array")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.BatchWriteItem(ctx, table, ops); err != nil {
				return fmt.Errorf("batch write item: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			// aws-cli emits an UnprocessedItems map; cefas commits the
			// batch atomically, so the map is always empty on success.
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"UnprocessedItems": map[string]any{},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&requestsArg, "request-items", "", "JSON array of PutRequest/DeleteRequest entries (required; inline or file://path)")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("request-items")
	root.AddCommand(c)
}

func parseBatchRequests(raw []byte) ([]client.BatchWriteOp, error) {
	var entries []batchWriteRequest
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("expected a JSON array of {PutRequest|DeleteRequest}: %w", err)
	}
	out := make([]client.BatchWriteOp, 0, len(entries))
	for i, e := range entries {
		switch {
		case e.PutRequest != nil && e.DeleteRequest != nil:
			return nil, fmt.Errorf("request-items[%d]: set exactly one of PutRequest or DeleteRequest", i)
		case e.PutRequest != nil:
			it, err := ddbjson.DecodeItem(e.PutRequest.Item)
			if err != nil {
				return nil, fmt.Errorf("request-items[%d].PutRequest.Item: %w", i, err)
			}
			out = append(out, client.BatchWriteOp{Put: it})
		case e.DeleteRequest != nil:
			k, err := ddbjson.DecodeItem(e.DeleteRequest.Key)
			if err != nil {
				return nil, fmt.Errorf("request-items[%d].DeleteRequest.Key: %w", i, err)
			}
			out = append(out, client.BatchWriteOp{Delete: k})
		default:
			return nil, fmt.Errorf("request-items[%d]: missing PutRequest or DeleteRequest", i)
		}
	}
	return out, nil
}
