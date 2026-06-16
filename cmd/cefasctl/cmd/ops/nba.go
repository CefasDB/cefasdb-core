package ops

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
)

func registerNBA(root *cobra.Command) {
	grp := &cobra.Command{
		Use:   "nba",
		Short: "Next-best-action pipeline",
	}
	grp.AddCommand(nbaDecideCmd())
	grp.AddCommand(nbaRewardCmd())
	grp.AddCommand(nbaGetCmd())
	root.AddCommand(grp)
}

func nbaDecideCmd() *cobra.Command {
	var (
		banditID, userID, actionsArg, fallback, contextArg string
		capScope                                           string
		capLimit                                           int
		capWindow, decisionTTL                             int64
	)
	cmd := &cobra.Command{
		Use:   "decide",
		Short: "Choose one eligible action and log the decision",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if banditID == "" || userID == "" || actionsArg == "" {
				return fmt.Errorf("--bandit-id, --user-id, and --actions are required")
			}
			actions, err := parseNBAActionsArg(actionsArg)
			if err != nil {
				return err
			}
			ctxMap, err := parseContextArg(contextArg)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			resp, err := cli.NextBestAction(ctx, client.NextBestActionRequest{
				BanditID:           banditID,
				UserID:             userID,
				Actions:            actions,
				FallbackActionID:   fallback,
				Context:            ctxMap,
				CapScope:           capScope,
				CapLimit:           capLimit,
				CapWindowSeconds:   capWindow,
				DecisionTTLSeconds: decisionTTL,
			})
			if err != nil {
				return fmt.Errorf("nba decide: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"DecisionID":  resp.DecisionID,
				"ActionID":    resp.ActionID,
				"Fallback":    resp.Fallback,
				"ReasonCodes": resp.ReasonCodes,
				"Stages":      pipelineStagesForOutput(resp.Stages),
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&banditID, "bandit-id", "", "Bandit identifier (required)")
	f.StringVar(&userID, "user-id", "", "User identifier (required)")
	f.StringVar(&actionsArg, "actions", "", "Comma-separated action IDs or JSON array of action objects")
	f.StringVar(&fallback, "fallback", "", "Fallback action ID")
	f.StringVar(&contextArg, "context", "", "Optional context as JSON object")
	f.StringVar(&capScope, "cap-scope", "", "Frequency-cap scope (default: bandit/action)")
	f.IntVar(&capLimit, "cap-limit", 0, "Frequency-cap limit for the chosen action")
	f.Int64Var(&capWindow, "cap-window-seconds", 0, "Frequency-cap window in seconds")
	f.Int64Var(&decisionTTL, "decision-ttl-seconds", 0, "Decision log TTL (default: 86400)")
	return cmd
}

func nbaRewardCmd() *cobra.Command {
	var (
		decisionID, banditID, actionID, contextArg string
		reward                                     float64
	)
	cmd := &cobra.Command{
		Use:   "reward",
		Short: "Record reward for a logged NBA decision",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if decisionID == "" && (banditID == "" || actionID == "") {
				return fmt.Errorf("--decision-id or --bandit-id/--action-id is required")
			}
			ctxMap, err := parseContextArg(contextArg)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			resp, err := cli.RecordReward(ctx, client.RecordRewardRequest{
				DecisionID: decisionID,
				BanditID:   banditID,
				ActionID:   actionID,
				Reward:     reward,
				Context:    ctxMap,
			})
			if err != nil {
				return fmt.Errorf("nba reward: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BanditID": resp.BanditID,
				"ActionID": resp.ActionID,
				"Reward":   reward,
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&decisionID, "decision-id", "", "Logged decision identifier")
	f.StringVar(&banditID, "bandit-id", "", "Bandit identifier for direct rewards")
	f.StringVar(&actionID, "action-id", "", "Action identifier for direct rewards")
	f.Float64Var(&reward, "reward", 0, "Reward value")
	f.StringVar(&contextArg, "context", "", "Optional context as JSON object")
	return cmd
}

func nbaGetCmd() *cobra.Command {
	var decisionID string
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Fetch a logged NBA decision",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if decisionID == "" {
				return fmt.Errorf("--decision-id is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			rec, found, err := cli.GetDecision(ctx, decisionID)
			if err != nil {
				return fmt.Errorf("nba get: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			if !found {
				return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{"Found": false})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Found":         true,
				"DecisionID":    rec.DecisionID,
				"BanditID":      rec.BanditID,
				"UserID":        rec.UserID,
				"ActionID":      rec.ActionID,
				"Fallback":      rec.Fallback,
				"ReasonCodes":   rec.ReasonCodes,
				"Context":       rec.Context,
				"CreatedAtUnix": rec.CreatedAtUnix,
				"ExpiresAtUnix": rec.ExpiresAtUnix,
			})
		},
	}
	cmd.Flags().StringVar(&decisionID, "decision-id", "", "Logged decision identifier")
	return cmd
}

type nbaActionWire struct {
	ActionID string            `json:"action_id"`
	Disabled bool              `json:"disabled"`
	Reason   string            `json:"reason"`
	Context  map[string]string `json:"context"`
}

func parseNBAActionsArg(arg string) ([]client.NBAAction, error) {
	arg = strings.TrimSpace(arg)
	if strings.HasPrefix(arg, "[") {
		var wire []nbaActionWire
		if err := json.Unmarshal([]byte(arg), &wire); err != nil {
			return nil, fmt.Errorf("--actions JSON: %w", err)
		}
		out := make([]client.NBAAction, 0, len(wire))
		for _, a := range wire {
			if a.ActionID == "" {
				return nil, fmt.Errorf("--actions JSON: action_id required")
			}
			out = append(out, client.NBAAction{
				ActionID: a.ActionID,
				Disabled: a.Disabled,
				Reason:   a.Reason,
				Context:  a.Context,
			})
		}
		return out, nil
	}
	parts := strings.Split(arg, ",")
	out := make([]client.NBAAction, 0, len(parts))
	for _, p := range parts {
		id := strings.TrimSpace(p)
		if id == "" {
			continue
		}
		out = append(out, client.NBAAction{ActionID: id})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--actions: no action IDs")
	}
	return out, nil
}
