// Package cmd implements the cefasctl root command surface. Each
// subcommand lives in the cmd/ddb or cmd/cluster sub-package; this
// file registers the global flag surface, dispatches Execute, and
// starts the interactive shell when no subcommand is provided.
package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/cmd/cluster"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/cmd/ddb"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/cmd/ops"
	plugincmd "github.com/osvaldoandrade/cefas/cmd/cefasctl/cmd/plugin"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

// Version is the cefasctl build version. Set via -ldflags at build
// time; falls back to "dev".
var Version = "dev"

type rootMode int

const (
	rootModeCLI rootMode = iota
	rootModeCommand
)

// Root returns the root Cobra command with every subcommand
// registered.
func Root() *cobra.Command {
	session := runtime.NewSession(runtime.Flags)
	root := rootWithSession(session, rootModeCLI)
	root.SetContext(runtime.WithSession(context.Background(), session))
	return root
}

func rootWithSession(session *runtime.Session, mode rootMode) *cobra.Command {
	if session == nil {
		session = runtime.NewSession(runtime.Options{})
	}
	root := &cobra.Command{
		Use:   "cefas",
		Short: "cefas — distributed multi-model database CLI",
		Long: `cefas is the command-line interface for the cefas database.
The surface mirrors AWS DynamoDB CLI so scripts written against
'aws dynamodb' can be ported by replacing the command name.

Run cefas without a command to start the interactive shell.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	if mode == rootModeCLI {
		root.Args = cobra.NoArgs
		root.RunE = func(cmd *cobra.Command, _ []string) error {
			return runREPL(cmd.Context(), session, cmd.InOrStdin(), cmd.OutOrStdout(), cmd.ErrOrStderr())
		}
	}

	flags := session.BindTarget()
	pf := root.PersistentFlags()
	pf.StringVar(&flags.ConfigPath, "config", flags.ConfigPath, "Config file path (defaults to ~/.cefas/config.yaml)")
	pf.StringVar(&flags.ProfileName, "profile", flags.ProfileName, "Named profile from the config file (default: 'default')")
	pf.StringVar(&flags.Endpoint, "endpoint", flags.Endpoint, "gRPC endpoint host:port (overrides config + env)")
	pf.StringVar(&flags.Token, "token", flags.Token, "Bearer token (overrides --token-file and env)")
	pf.StringVar(&flags.TokenFile, "token-file", flags.TokenFile, "Path to a file containing the bearer token")
	pf.StringVar(&flags.TLSCAPath, "ca", flags.TLSCAPath, "Path to a TLS CA bundle for server verification")
	pf.BoolVar(&flags.Insecure, "insecure", flags.Insecure, "Disable TLS (plaintext)")
	pf.StringVar(&flags.Output, "output", flags.Output, "Output format: json (default) | table | text")
	pf.DurationVar(&flags.Timeout, "timeout", flags.Timeout, "Per-call timeout (e.g. 30s)")
	pf.BoolVar(&flags.NoStream, "no-stream", flags.NoStream, "Buffer streaming results into a single response")

	root.AddCommand(versionCmd())

	ddb.Register(root)
	cluster.Register(root)
	plugincmd.Register(root)
	ops.Register(root)

	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the cefasctl version",
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

	session := runtime.NewSession(runtime.Options{})
	root := rootWithSession(session, rootModeCLI)
	root.SetContext(runtime.WithSession(ctx, session))

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "cefas:", err)
		os.Exit(1)
	}
}
