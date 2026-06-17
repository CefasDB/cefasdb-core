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

type transactGetEntry struct {
	Get struct {
		TableName string                       `json:"TableName"`
		Key       map[string]ddbjson.Attribute `json:"Key"`
	} `json:"Get"`
}

func registerTransactGetItems(root *cobra.Command) {
	var argsArg string
	c := &cobra.Command{
		Use:   "transact-get-items",
		Short: "Fetch many items as a consistent batch (single-table v1)",
		Long: `Mirrors aws dynamodb transact-get-items. v1 single-table; every Get
must reference the same TableName.

Example:
  cefas transact-get-items --transact-items '[
    {"Get":{"TableName":"Users","Key":{"pk":{"S":"u1"}}}},
    {"Get":{"TableName":"Users","Key":{"pk":{"S":"u2"}}}}
  ]'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if argsArg == "" {
				return fmt.Errorf("--transact-items is required")
			}
			raw, err := fileloader.Load(argsArg)
			if err != nil {
				return err
			}
			var entries []transactGetEntry
			if err := json.Unmarshal(raw, &entries); err != nil {
				return fmt.Errorf("--transact-items: %w", err)
			}
			gets := make([]client.TransactGet, 0, len(entries))
			for i, e := range entries {
				if e.Get.TableName == "" {
					return fmt.Errorf("transact-items[%d].Get.TableName required", i)
				}
				key, err := ddbjson.DecodeItem(e.Get.Key)
				if err != nil {
					return fmt.Errorf("transact-items[%d].Get.Key: %w", i, err)
				}
				gets = append(gets, client.TransactGet{Table: e.Get.TableName, Key: key})
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			items, err := cli.TransactGetItems(ctx, gets)
			if err != nil {
				return fmt.Errorf("transact get items: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			// AWS wire shape: Responses is an array aligned with the
			// request. Missing items render as an empty object.
			resp := make([]map[string]any, len(items))
			for i, it := range items {
				if it == nil {
					resp[i] = map[string]any{}
					continue
				}
				resp[i] = map[string]any{"Item": ddbjson.EncodeItem(it)}
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Responses": resp,
			})
		},
	}
	c.Flags().StringVar(&argsArg, "transact-items", "", "JSON array of {Get:{TableName,Key}} entries (required; inline or file://path)")
	_ = c.MarkFlagRequired("transact-items")
	root.AddCommand(c)
}
