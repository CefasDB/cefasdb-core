package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/fileloader"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
)

func registerGetItem(root *cobra.Command) {
	var (
		table          string
		keyArg         string
		consistentRead bool
	)
	c := &cobra.Command{
		Use:   "get-item",
		Short: "Fetch a single item by primary key",
		Long: `Mirrors aws dynamodb get-item.

Example:
  cefas get-item \
    --table-name Users \
    --key '{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"}}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if keyArg == "" {
				return fmt.Errorf("--key is required")
			}
			keyBytes, err := fileloader.Load(keyArg)
			if err != nil {
				return err
			}
			key, err := ddbjson.ParseItem(keyBytes)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			item, err := cli.GetItem(ctx, table, key, client.GetOptions{Strong: consistentRead})
			if err != nil {
				return fmt.Errorf("get item: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Item(item)
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&keyArg, "key", "", "DynamoDB-JSON primary key (required; inline or file://path)")
	f.BoolVar(&consistentRead, "consistent-read", false, "Strong consistency: routes through the leader + barrier")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("key")
	root.AddCommand(c)
}
