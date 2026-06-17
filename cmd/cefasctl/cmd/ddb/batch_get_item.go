package ddb

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/ddbjson"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func registerBatchGetItem(root *cobra.Command) {
	var (
		table   string
		keysArg string
	)
	c := &cobra.Command{
		Use:   "batch-get-item",
		Short: "Fetch many items by primary key in a single call",
		Long: `Fetches one or more items by primary key. The cefas gRPC surface
batches against a single table, so --table-name is required (unlike
aws dynamodb batch-get-item which accepts a multi-table RequestItems
map).

Example:
  cefas batch-get-item \
    --table-name Users \
    --keys '[{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"}},
            {"pk":{"S":"USER#2"},"sk":{"S":"PROFILE"}}]'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if keysArg == "" {
				return fmt.Errorf("--keys is required")
			}
			raw, err := fileloader.Load(keysArg)
			if err != nil {
				return err
			}
			keys, err := parseKeyArray(raw)
			if err != nil {
				return fmt.Errorf("--keys: %w", err)
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			items, err := cli.BatchGetItem(ctx, table, keys)
			if err != nil {
				return fmt.Errorf("batch get item: %w", err)
			}
			// Drop the (nil) slots for not-found keys so the renderer
			// emits a clean Items slice; the wire still preserves order
			// for callers that need it via repeated get-item.
			out := make([]types.Item, 0, len(items))
			for _, it := range items {
				if it != nil {
					out = append(out, it)
				}
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Items(out)
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&keysArg, "keys", "", "JSON array of DynamoDB-JSON keys (required; inline or file://path)")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("keys")
	root.AddCommand(c)
}

// parseKeyArray decodes a JSON array of DDB-JSON keys into typed Items.
func parseKeyArray(raw []byte) ([]types.Item, error) {
	var wire []map[string]ddbjson.Attribute
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("expected a JSON array of items: %w", err)
	}
	out := make([]types.Item, 0, len(wire))
	for i, m := range wire {
		it, err := ddbjson.DecodeItem(m)
		if err != nil {
			return nil, fmt.Errorf("keys[%d]: %w", i, err)
		}
		out = append(out, it)
	}
	return out, nil
}
