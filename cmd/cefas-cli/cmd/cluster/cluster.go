// Package cluster hosts the `cefas cluster <verb>` subcommands —
// status, add-voter, remove-server. These wrap the SDK's cluster
// helpers and inherit every global flag from the root command.
package cluster

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

// Register installs the cluster subcommand tree onto root.
func Register(root *cobra.Command) {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster membership and status",
	}
	c.AddCommand(statusCmd())
	c.AddCommand(addVoterCmd())
	c.AddCommand(removeServerCmd())
	root.AddCommand(c)
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the cluster mode, self ID, and current leader",
		Long: `Fetches the cluster status. Public — no token required.

Example:
  cefas cluster status`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			st, err := cli.Status(ctx)
			if err != nil {
				return fmt.Errorf("cluster status: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Mode":       st.Mode,
				"IsLeader":   st.IsLeader,
				"SelfID":     st.SelfID,
				"BindAddr":   st.BindAddr,
				"LeaderHTTP": st.LeaderHTTP,
			})
		},
	}
}

func addVoterCmd() *cobra.Command {
	var (
		id   string
		addr string
	)
	c := &cobra.Command{
		Use:   "add-voter",
		Short: "Add a voting peer to the Raft configuration",
		Long: `Asks the leader to add a voter at the supplied Raft address.
Requires the cefas:cluster:admin scope on the bearer token.

Example:
  cefas cluster add-voter --id node-3 --addr 10.0.0.13:9000`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if id == "" {
				return fmt.Errorf("--id is required")
			}
			if addr == "" {
				return fmt.Errorf("--addr is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.AddVoter(ctx, id, addr); err != nil {
				return fmt.Errorf("add voter: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Added": map[string]string{"ID": id, "Addr": addr},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&id, "id", "", "Raft node ID (required)")
	f.StringVar(&addr, "addr", "", "Raft transport address host:port (required)")
	_ = c.MarkFlagRequired("id")
	_ = c.MarkFlagRequired("addr")
	return c
}

func removeServerCmd() *cobra.Command {
	var id string
	c := &cobra.Command{
		Use:   "remove-server",
		Short: "Evict a peer from the Raft configuration",
		Long: `Removes the peer with the given ID from the cluster. Requires
the cefas:cluster:admin scope on the bearer token.

Example:
  cefas cluster remove-server --id node-3`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if id == "" {
				return fmt.Errorf("--id is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.RemoveServer(ctx, id); err != nil {
				return fmt.Errorf("remove server: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Removed": map[string]string{"ID": id},
			})
		},
	}
	c.Flags().StringVar(&id, "id", "", "Raft node ID to remove (required)")
	_ = c.MarkFlagRequired("id")
	return c
}
