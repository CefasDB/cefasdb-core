package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/ddbjson"
)

func registerDeleteItem(root *cobra.Command) {
	var (
		table     string
		keyArg    string
		condition string
		bindsArg  string
	)
	c := &cobra.Command{
		Use:   "delete-item",
		Short: "Remove a single item by primary key",
		Long: `Mirrors aws dynamodb delete-item.

Example:
  cefas delete-item \
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
			opts := client.DeleteOptions{Condition: condition}
			if bindsArg != "" {
				bindBytes, err := fileloader.Load(bindsArg)
				if err != nil {
					return err
				}
				binds, err := ddbjson.ParseBinds(bindBytes)
				if err != nil {
					return err
				}
				opts.Binds = binds
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.DeleteItem(ctx, table, key, opts); err != nil {
				return fmt.Errorf("delete item: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(struct{}{})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&keyArg, "key", "", "DynamoDB-JSON primary key (required; inline or file://path)")
	f.StringVar(&condition, "condition-expression", "", "Optional ConditionExpression")
	f.StringVar(&bindsArg, "expression-attribute-values", "", "DynamoDB-JSON bind map (inline or file://path)")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("key")
	root.AddCommand(c)
}
