// Package plugin hosts the `cefas list-plugins` and
// `cefas describe-plugin` subcommands. Both flow through the standard
// runtime.Dial + output renderer so json / table / text work out of
// the box.
package plugin

import "github.com/spf13/cobra"

// Register installs the plugin-introspection subcommands onto root.
func Register(root *cobra.Command) {
	registerListPlugins(root)
	registerDescribePlugin(root)
}
