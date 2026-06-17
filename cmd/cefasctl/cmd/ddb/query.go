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

func registerQuery(root *cobra.Command) {
	var (
		table          string
		pkArg          string
		skLowArg       string
		skHighArg      string
		indexName      string
		limit          int
		consistentRead bool
	)
	c := &cobra.Command{
		Use:   "query",
		Short: "Run a partition-key query (optionally bounded by sort-key range)",
		Long: `Mirrors aws dynamodb query, restricted to the cefas gRPC surface:
the partition key is exact-match, and the sort key supports a BETWEEN
range via --sk-low / --sk-high.

Example:
  cefas query \
    --table-name Users \
    --pk-value '{"S":"USER#1"}' \
    --sk-low '{"S":"A"}' --sk-high '{"S":"Z"}' \
    --limit 25`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if pkArg == "" {
				return fmt.Errorf("--pk-value is required")
			}
			pk, err := parseAttr(pkArg)
			if err != nil {
				return fmt.Errorf("--pk-value: %w", err)
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()

			qb := cli.Query(ctx, table).PK(pk)
			if indexName != "" {
				qb = qb.Index(indexName)
			}
			if limit > 0 {
				qb = qb.Limit(limit)
			}
			if consistentRead {
				qb = qb.Strong()
			}
			if skLowArg != "" || skHighArg != "" {
				lo := nullAttr()
				hi := nullAttr()
				if skLowArg != "" {
					v, err := parseAttr(skLowArg)
					if err != nil {
						return fmt.Errorf("--sk-low: %w", err)
					}
					lo = v
				}
				if skHighArg != "" {
					v, err := parseAttr(skHighArg)
					if err != nil {
						return fmt.Errorf("--sk-high: %w", err)
					}
					hi = v
				}
				qb = qb.SKBetween(lo, hi)
			}

			items, err := qb.Run(ctx)
			if err != nil {
				return fmt.Errorf("query: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Items(items)
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&pkArg, "pk-value", "", "DynamoDB-JSON attribute value for the partition key (required)")
	f.StringVar(&skLowArg, "sk-low", "", "DynamoDB-JSON attribute value (inclusive lower bound on sort key)")
	f.StringVar(&skHighArg, "sk-high", "", "DynamoDB-JSON attribute value (inclusive upper bound on sort key)")
	f.StringVar(&indexName, "index-name", "", "Query a secondary index by name")
	f.IntVar(&limit, "limit", 0, "Cap the number of items returned (0 = no cap)")
	f.BoolVar(&consistentRead, "consistent-read", false, "Strong consistency: routes through the leader + barrier")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("pk-value")
	root.AddCommand(c)
}

// parseAttr accepts either an inline DDB-JSON attribute value
// (`{"S":"x"}`) or `file://path` and returns the storage-layer
// AttributeValue.
func parseAttr(arg string) (types.AttributeValue, error) {
	raw, err := fileloader.Load(arg)
	if err != nil {
		return types.AttributeValue{}, err
	}
	var wire ddbjson.Attribute
	if err := json.Unmarshal(raw, &wire); err != nil {
		return types.AttributeValue{}, fmt.Errorf("not valid DDB-JSON attribute: %w", err)
	}
	return wire.ToAttr()
}

func nullAttr() types.AttributeValue {
	return types.AttributeValue{T: types.AttrNull}
}
