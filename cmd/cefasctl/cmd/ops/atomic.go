// Atomic read-modify-write CLI surface (issue #242).
//
//	cefas atomic incr --table T --key '{"id":{"S":"page"}}' --attribute count --by 1
//	cefas atomic apply --table T --key '{"id":{"S":"arm1"}}' --attribute beta \
//	    --expr 'clamp(beta + 1 - reward, 0, 1)'
//
// Mirrors aws dynamodb update-item ergonomics but funnels through the
// CefasAtomic gRPC service so contended counters never need a retry loop.
package ops

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/clicfg"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func registerAtomic(root *cobra.Command) {
	grp := &cobra.Command{
		Use:   "atomic",
		Short: "Atomic read-modify-write primitives (counter / CAS)",
		Long: `Atomic operations against a single item under contention without a
client-side retry loop. The post-image of the row is always returned
so callers never need a follow-up get-item.

Subcommands:
  incr   — atomic add-with-return (INCR_RETURN / ADD_RETURN)
  apply  — evaluate a whitelisted expression and assign the result
  set    — atomic SET (with optional ConditionExpression)`,
	}
	grp.AddCommand(atomicIncrCmd())
	grp.AddCommand(atomicApplyCmd())
	grp.AddCommand(atomicSetCmd())
	root.AddCommand(grp)
}

func atomicIncrCmd() *cobra.Command {
	var (
		table, keyJSON, attribute, by, condition string
	)
	c := &cobra.Command{
		Use:   "incr",
		Short: "Atomically add --by to --attribute, returning the new value",
		Long: `Atomically increments --attribute by --by (numeric) on the row
keyed by --key (DynamoDB-JSON). Returns the new value plus the
post-image of the row.

Example:
  cefas atomic incr \
    --table Counters \
    --key '{"id":{"S":"page_views"}}' \
    --attribute count \
    --by 1`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || keyJSON == "" || attribute == "" || by == "" {
				return fmt.Errorf("--table, --key, --attribute, --by are required")
			}
			key, err := parseDDBKey(keyJSON)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			res, err := cli.AtomicUpdate(ctx, table, key, client.AtomicOptions{
				Condition: condition,
				Actions: []client.AtomicAction{{
					Kind:      client.AtomicIncrReturn,
					Attribute: attribute,
					Value:     types.AttributeValue{T: types.AttrN, N: by},
				}},
			})
			if err != nil {
				return fmt.Errorf("atomic incr: %w", err)
			}
			return renderAtomicResult(cmd, profile, res)
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&keyJSON, "key", "", "Primary key as DynamoDB-JSON, e.g. '{\"id\":{\"S\":\"x\"}}' (required)")
	f.StringVar(&attribute, "attribute", "", "Numeric attribute to increment (required)")
	f.StringVar(&by, "by", "", "Numeric delta as canonical decimal text (required)")
	f.StringVar(&condition, "condition", "", "Optional ConditionExpression to gate the write")
	return c
}

func atomicApplyCmd() *cobra.Command {
	var (
		table, keyJSON, attribute, expr, condition string
	)
	c := &cobra.Command{
		Use:   "apply",
		Short: "Apply a whitelisted expression and assign the result to --attribute",
		Long: `Evaluates --expr server-side against the prior item state and
assigns the result to --attribute. Whitelisted grammar:
numeric arithmetic (+ - * /), min(a,b), max(a,b), clamp(x,lo,hi).

Bandit posterior example (issue #242 canonical):
  cefas atomic apply \
    --table BanditArms \
    --key '{"id":{"S":"arm1"}}' \
    --attribute beta \
    --expr 'clamp(beta + 1, 0, 1000)'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || keyJSON == "" || attribute == "" || expr == "" {
				return fmt.Errorf("--table, --key, --attribute, --expr are required")
			}
			key, err := parseDDBKey(keyJSON)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			res, err := cli.AtomicUpdate(ctx, table, key, client.AtomicOptions{
				Condition: condition,
				Actions: []client.AtomicAction{{
					Kind:       client.AtomicApply,
					Attribute:  attribute,
					Expression: expr,
				}},
			})
			if err != nil {
				return fmt.Errorf("atomic apply: %w", err)
			}
			return renderAtomicResult(cmd, profile, res)
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&keyJSON, "key", "", "Primary key as DynamoDB-JSON (required)")
	f.StringVar(&attribute, "attribute", "", "Attribute to assign the result to (required)")
	f.StringVar(&expr, "expr", "", "Whitelisted expression (required)")
	f.StringVar(&condition, "condition", "", "Optional ConditionExpression to gate the write")
	return c
}

func atomicSetCmd() *cobra.Command {
	var (
		table, keyJSON, attribute, valueJSON, condition string
	)
	c := &cobra.Command{
		Use:   "set",
		Short: "Atomically SET an attribute, returning the post-image",
		Long: `Atomically overwrites --attribute with --value (DynamoDB-JSON).
Mirrors PutItem semantics but returns the resulting item in the
same round-trip, with an optional ConditionExpression to gate the
write.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || keyJSON == "" || attribute == "" || valueJSON == "" {
				return fmt.Errorf("--table, --key, --attribute, --value are required")
			}
			key, err := parseDDBKey(keyJSON)
			if err != nil {
				return err
			}
			var av ddbjson.Attribute
			if err := json.Unmarshal([]byte(valueJSON), &av); err != nil {
				return fmt.Errorf("--value: %w", err)
			}
			attr, err := av.ToAttr()
			if err != nil {
				return fmt.Errorf("--value: %w", err)
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			res, err := cli.AtomicUpdate(ctx, table, key, client.AtomicOptions{
				Condition: condition,
				Actions: []client.AtomicAction{{
					Kind:      client.AtomicSet,
					Attribute: attribute,
					Value:     attr,
				}},
			})
			if err != nil {
				return fmt.Errorf("atomic set: %w", err)
			}
			return renderAtomicResult(cmd, profile, res)
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&keyJSON, "key", "", "Primary key as DynamoDB-JSON (required)")
	f.StringVar(&attribute, "attribute", "", "Attribute to overwrite (required)")
	f.StringVar(&valueJSON, "value", "", "New value as DynamoDB-JSON, e.g. '{\"N\":\"42\"}' (required)")
	f.StringVar(&condition, "condition", "", "Optional ConditionExpression to gate the write")
	return c
}

func parseDDBKey(raw string) (types.Item, error) {
	out, err := ddbjson.ParseItem([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("--key: %w", err)
	}
	return out, nil
}

func renderAtomicResult(cmd *cobra.Command, profile clicfg.Profile, res client.AtomicResult) error {
	fm, err := output.Validate(profile.Output)
	if err != nil {
		return err
	}
	returned := make([]ddbjson.Attribute, 0, len(res.Returned))
	for _, av := range res.Returned {
		returned = append(returned, ddbjson.FromAttr(av))
	}
	return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
		"Item":     ddbjson.EncodeItem(res.Item),
		"Returned": returned,
		"Created":  res.Created,
	})
}
