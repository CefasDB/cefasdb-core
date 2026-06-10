// Package cmd implements the cefas-cli root command surface. Each
// subcommand lives in the cmd/ddb or cmd/cluster sub-package; this
// file just registers the global flag surface every command
// inherits and dispatches Execute.
package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/cmd/cluster"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/cmd/ddb"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

// Version is the cefas-cli build version. Set via -ldflags at build
// time; falls back to "dev".
var Version = "dev"

// Root returns the root Cobra command with every subcommand
// registered.
func Root() *cobra.Command {
	root := &cobra.Command{
		Use:   "cefas",
		Short: "cefas — distributed multi-model database CLI",
		Long: `cefas is the command-line interface for the cefas database.
The surface mirrors AWS DynamoDB CLI so scripts written against
'aws dynamodb' can be ported by replacing the command name.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	pf := root.PersistentFlags()
	pf.StringVar(&runtime.Flags.ConfigPath, "config", "", "Config file path (defaults to ~/.cefas/config.yaml)")
	pf.StringVar(&runtime.Flags.ProfileName, "profile", "", "Named profile from the config file (default: 'default')")
	pf.StringVar(&runtime.Flags.Endpoint, "endpoint", "", "gRPC endpoint host:port (overrides config + env)")
	pf.StringVar(&runtime.Flags.Token, "token", "", "Bearer token (overrides --token-file and env)")
	pf.StringVar(&runtime.Flags.TokenFile, "token-file", "", "Path to a file containing the bearer token")
	pf.StringVar(&runtime.Flags.TLSCAPath, "ca", "", "Path to a TLS CA bundle for server verification")
	pf.BoolVar(&runtime.Flags.Insecure, "insecure", false, "Disable TLS (plaintext)")
	pf.StringVar(&runtime.Flags.Output, "output", "", "Output format: json (default) | table | text")
	pf.DurationVar(&runtime.Flags.Timeout, "timeout", 0, "Per-call timeout (e.g. 30s)")
	pf.BoolVar(&runtime.Flags.NoStream, "no-stream", false, "Buffer streaming results into a single response")

	root.AddCommand(versionCmd())

	ddb.Register(root)
	cluster.Register(root)

	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the cefas-cli version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), Version)
			return nil
		},
	}
}

// Execute is the entry point main.go calls. Sets up a cancellable
// context and exits with a non-zero status on error.
func Execute() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	root := Root()
	root.SetContext(ctx)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "cefas:", err)
		os.Exit(1)
	}
}
