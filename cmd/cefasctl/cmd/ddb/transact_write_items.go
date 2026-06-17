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

// transactWriteEntry mirrors one entry in the aws --transact-items array.
type transactWriteEntry struct {
	Put *struct {
		TableName                 string                       `json:"TableName"`
		Item                      map[string]ddbjson.Attribute `json:"Item"`
		ConditionExpression       string                       `json:"ConditionExpression,omitempty"`
		ExpressionAttributeValues map[string]ddbjson.Attribute `json:"ExpressionAttributeValues,omitempty"`
	} `json:"Put,omitempty"`
	Delete *struct {
		TableName                 string                       `json:"TableName"`
		Key                       map[string]ddbjson.Attribute `json:"Key"`
		ConditionExpression       string                       `json:"ConditionExpression,omitempty"`
		ExpressionAttributeValues map[string]ddbjson.Attribute `json:"ExpressionAttributeValues,omitempty"`
	} `json:"Delete,omitempty"`
	ConditionCheck *struct {
		TableName                 string                       `json:"TableName"`
		Key                       map[string]ddbjson.Attribute `json:"Key"`
		ConditionExpression       string                       `json:"ConditionExpression"`
		ExpressionAttributeValues map[string]ddbjson.Attribute `json:"ExpressionAttributeValues,omitempty"`
	} `json:"ConditionCheck,omitempty"`
}

func registerTransactWriteItems(root *cobra.Command) {
	var argsArg string
	c := &cobra.Command{
		Use:   "transact-write-items",
		Short: "Apply up to 100 Put/Delete/ConditionCheck ops atomically",
		Long: `Mirrors aws dynamodb transact-write-items. v1 restricts the batch
to a single table — every op must reference the same TableName. Update
is not yet supported inside a transaction; call UpdateItem outside or
use a Put with a ConditionExpression.

Example:
  cefas transact-write-items --transact-items '[
    {"Put":{"TableName":"Users","Item":{"pk":{"S":"u1"},"name":{"S":"Ova"}}}},
    {"Delete":{"TableName":"Users","Key":{"pk":{"S":"u2"}}}},
    {"ConditionCheck":{"TableName":"Users","Key":{"pk":{"S":"u3"}},
                       "ConditionExpression":"attribute_exists(pk)"}}
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
			var entries []transactWriteEntry
			if err := json.Unmarshal(raw, &entries); err != nil {
				return fmt.Errorf("--transact-items: %w", err)
			}
			ops, err := buildTransactWriteOps(entries)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.TransactWriteItems(ctx, ops); err != nil {
				return fmt.Errorf("transact write items: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(struct{}{})
		},
	}
	c.Flags().StringVar(&argsArg, "transact-items", "", "JSON array of {Put|Delete|ConditionCheck} entries (required; inline or file://path)")
	_ = c.MarkFlagRequired("transact-items")
	root.AddCommand(c)
}

func buildTransactWriteOps(entries []transactWriteEntry) ([]client.TransactWriteOp, error) {
	out := make([]client.TransactWriteOp, 0, len(entries))
	for i, e := range entries {
		set := 0
		if e.Put != nil {
			set++
		}
		if e.Delete != nil {
			set++
		}
		if e.ConditionCheck != nil {
			set++
		}
		if set != 1 {
			return nil, fmt.Errorf("transact-items[%d]: set exactly one of Put / Delete / ConditionCheck", i)
		}
		switch {
		case e.Put != nil:
			item, err := ddbjson.DecodeItem(e.Put.Item)
			if err != nil {
				return nil, fmt.Errorf("transact-items[%d].Put.Item: %w", i, err)
			}
			binds, err := ddbjson.DecodeBinds(e.Put.ExpressionAttributeValues)
			if err != nil {
				return nil, fmt.Errorf("transact-items[%d].Put binds: %w", i, err)
			}
			out = append(out, client.TransactWriteOp{
				Kind: client.TransactPut, Table: e.Put.TableName, Item: item,
				ConditionExpression: e.Put.ConditionExpression, Binds: binds,
			})
		case e.Delete != nil:
			key, err := ddbjson.DecodeItem(e.Delete.Key)
			if err != nil {
				return nil, fmt.Errorf("transact-items[%d].Delete.Key: %w", i, err)
			}
			binds, err := ddbjson.DecodeBinds(e.Delete.ExpressionAttributeValues)
			if err != nil {
				return nil, fmt.Errorf("transact-items[%d].Delete binds: %w", i, err)
			}
			out = append(out, client.TransactWriteOp{
				Kind: client.TransactDelete, Table: e.Delete.TableName, Key: key,
				ConditionExpression: e.Delete.ConditionExpression, Binds: binds,
			})
		case e.ConditionCheck != nil:
			key, err := ddbjson.DecodeItem(e.ConditionCheck.Key)
			if err != nil {
				return nil, fmt.Errorf("transact-items[%d].ConditionCheck.Key: %w", i, err)
			}
			binds, err := ddbjson.DecodeBinds(e.ConditionCheck.ExpressionAttributeValues)
			if err != nil {
				return nil, fmt.Errorf("transact-items[%d].ConditionCheck binds: %w", i, err)
			}
			out = append(out, client.TransactWriteOp{
				Kind: client.TransactConditionCheck, Table: e.ConditionCheck.TableName, Key: key,
				ConditionExpression: e.ConditionCheck.ConditionExpression, Binds: binds,
			})
		}
	}
	return out, nil
}
