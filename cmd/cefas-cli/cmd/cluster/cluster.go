// Package cluster hosts the `cefas cluster <verb>` subcommands.
// PR 5 fills the actual status / add-voter / remove-server commands;
// for now this is a registered-but-empty group so root.go compiles.
package cluster

import "github.com/spf13/cobra"

// Register installs the cluster subcommand tree onto root.
func Register(root *cobra.Command) {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster membership and status",
	}
	root.AddCommand(c)
}
