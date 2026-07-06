package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	mgr "github.com/CefasDb/cefasdb/internal/manager"
	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/pkg/client"
)

type config struct {
	endpoint       string
	httpEndpoint   string
	token          string
	tokenFile      string
	insecure       bool
	timeout        time.Duration
	namespace      string
	selector       string
	kubeDisabled   bool
	output         string
	managerID      string
	leaderElection bool
	leaderLease    string
	leaderTTL      time.Duration
	auditLog       string
	approveFencing bool
	interval       time.Duration
	repairMode     string
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	cfg := defaultConfig()
	cmd := &cobra.Command{Use: "cefas-manager", Short: "CefasDB health, repair, and reconciliation manager"}
	cmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		if cfg.token == "" && cfg.tokenFile != "" {
			data, err := os.ReadFile(cfg.tokenFile)
			if err != nil {
				return err
			}
			cfg.token = strings.TrimSpace(string(data))
		}
		return nil
	}
	f := cmd.PersistentFlags()
	f.StringVar(&cfg.endpoint, "endpoint", cfg.endpoint, "Cefas gRPC endpoint")
	f.StringVar(&cfg.httpEndpoint, "http-endpoint", cfg.httpEndpoint, "Cefas HTTP endpoint for placement audit")
	f.StringVar(&cfg.token, "token", cfg.token, "bearer token for cluster-admin APIs")
	f.StringVar(&cfg.tokenFile, "token-file", cfg.tokenFile, "file containing bearer token")
	f.BoolVar(&cfg.insecure, "insecure", cfg.insecure, "use plaintext gRPC")
	f.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "per-operation timeout")
	f.StringVar(&cfg.namespace, "namespace", cfg.namespace, "Kubernetes namespace")
	f.StringVar(&cfg.selector, "selector", cfg.selector, "Kubernetes label selector for Cefas resources")
	f.BoolVar(&cfg.kubeDisabled, "kube-disabled", cfg.kubeDisabled, "skip Kubernetes snapshot")
	f.StringVar(&cfg.output, "output", cfg.output, "output format: json or text")
	f.StringVar(&cfg.managerID, "manager-id", cfg.managerID, "leader-election holder identity")
	f.BoolVar(&cfg.leaderElection, "leader-election", cfg.leaderElection, "acquire Kubernetes leader lease before active operations")
	f.StringVar(&cfg.leaderLease, "leader-lease-name", cfg.leaderLease, "Kubernetes lease name for manager leadership")
	f.DurationVar(&cfg.leaderTTL, "leader-lease-ttl", cfg.leaderTTL, "Kubernetes leader lease TTL")
	f.StringVar(&cfg.auditLog, "audit-log", cfg.auditLog, "JSONL audit log path for repair execution")
	cmd.AddCommand(doctorCmd(&cfg), repairCmd(&cfg), controllerCmd(&cfg))
	return cmd
}

func doctorCmd(cfg *config) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Report CefasDB and Kubernetes health",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.timeout)
			defer cancel()
			clients, err := buildClients(ctx, cfg)
			if err != nil {
				return err
			}
			defer clients.close()
			report, err := mgr.Doctor(ctx, mgr.DoctorOptions{
				Cefas:      clients.cefas,
				Kubernetes: clients.kube,
				Kube:       mgr.KubeSnapshotOptions{Namespace: cfg.namespace, Selector: cfg.selector},
				Audit:      placement.PlacementAuditRequest{IncludeRepairPlan: true},
			})
			if err != nil {
				return err
			}
			return writeOutput(cmd, cfg.output, report)
		},
	}
}

func repairCmd(cfg *config) *cobra.Command {
	var dryRun bool
	var execute bool
	c := &cobra.Command{
		Use:   "repair",
		Short: "Plan or execute guarded repairs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dryRun && execute {
				return fmt.Errorf("--dry-run and --execute are mutually exclusive")
			}
			if !dryRun && !execute {
				dryRun = true
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.timeout)
			defer cancel()
			clients, err := buildClients(ctx, cfg)
			if err != nil {
				return err
			}
			defer clients.close()
			report, err := mgr.Doctor(ctx, mgr.DoctorOptions{
				Cefas:      clients.cefas,
				Kubernetes: clients.kube,
				Kube:       mgr.KubeSnapshotOptions{Namespace: cfg.namespace, Selector: cfg.selector},
				Audit:      placement.PlacementAuditRequest{IncludeRepairPlan: true},
			})
			if err != nil {
				return err
			}
			leader := mgr.LeaderLease{}
			if execute {
				if !cfg.leaderElection {
					return fmt.Errorf("repair --execute requires --leader-election")
				}
				leader, err = clients.acquireLeader(ctx)
				if err != nil {
					return err
				}
			}
			opts := mgr.RepairOptions{Cefas: clients.cefas, Report: report, Leader: leader, ApproveFencing: cfg.approveFencing, AuditLogPath: cfg.auditLog, Timeout: cfg.timeout}
			var result mgr.RepairResult
			if execute {
				result, err = mgr.ExecuteRepair(ctx, opts)
				if err != nil {
					_ = writeOutput(cmd, cfg.output, result)
					return err
				}
			} else {
				result = mgr.DryRunRepair(opts)
			}
			return writeOutput(cmd, cfg.output, result)
		},
	}
	c.Flags().BoolVar(&dryRun, "dry-run", false, "emit an ordered repair plan without mutations")
	c.Flags().BoolVar(&execute, "execute", false, "execute supported repair actions after all guards pass")
	c.Flags().BoolVar(&cfg.approveFencing, "approve-fencing", cfg.approveFencing, "confirm fencing/provider state for sensitive actions")
	return c
}

func controllerCmd(cfg *config) *cobra.Command {
	c := &cobra.Command{
		Use:   "controller",
		Short: "Run the leader-elected manager loop",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg.leaderElection = true
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			clients, err := buildClients(ctx, cfg)
			if err != nil {
				return err
			}
			defer clients.close()
			ticker := time.NewTicker(cfg.interval)
			defer ticker.Stop()
			for {
				if err := runControllerTick(ctx, cmd, cfg, clients); err != nil {
					fmt.Fprintf(cmd.ErrOrStderr(), "controller tick: %v\n", err)
				}
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
				}
			}
		},
	}
	c.Flags().DurationVar(&cfg.interval, "interval", cfg.interval, "controller reconciliation interval")
	c.Flags().StringVar(&cfg.repairMode, "repair-mode", cfg.repairMode, "observe, dry-run, or execute")
	c.Flags().BoolVar(&cfg.approveFencing, "approve-fencing", cfg.approveFencing, "confirm fencing/provider state for sensitive actions in execute mode")
	return c
}

func runControllerTick(ctx context.Context, cmd *cobra.Command, cfg *config, clients runtimeClients) error {
	leader, err := clients.acquireLeader(ctx)
	if err != nil {
		return err
	}
	if !leader.Acquired {
		return writeJSONLine(cmd.OutOrStdout(), map[string]any{"at": time.Now().UTC(), "event": "standby", "holder": leader.Holder})
	}
	tickCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	report, err := mgr.Doctor(tickCtx, mgr.DoctorOptions{
		Cefas:      clients.cefas,
		Kubernetes: clients.kube,
		Kube:       mgr.KubeSnapshotOptions{Namespace: cfg.namespace, Selector: cfg.selector},
		Audit:      placement.PlacementAuditRequest{IncludeRepairPlan: true},
	})
	if err != nil {
		return err
	}
	switch cfg.repairMode {
	case "observe":
		return writeJSONLine(cmd.OutOrStdout(), map[string]any{"at": time.Now().UTC(), "event": "doctor", "classification": report.Classification, "signals": len(report.Signals)})
	case "dry-run":
		return writeJSONLine(cmd.OutOrStdout(), mgr.DryRunRepair(mgr.RepairOptions{Report: report, Leader: leader, AuditLogPath: cfg.auditLog}))
	case "execute":
		result, err := mgr.ExecuteRepair(tickCtx, mgr.RepairOptions{Cefas: clients.cefas, Report: report, Leader: leader, ApproveFencing: cfg.approveFencing, AuditLogPath: cfg.auditLog, Timeout: cfg.timeout})
		_ = writeJSONLine(cmd.OutOrStdout(), result)
		return err
	default:
		return fmt.Errorf("invalid --repair-mode %q", cfg.repairMode)
	}
}

type runtimeClients struct {
	grpc  *client.Client
	cefas *mgr.SDKCefas
	kube  mgr.Kubernetes
	lead  mgr.LeaderElector
}

func (c runtimeClients) close() {
	if c.grpc != nil {
		_ = c.grpc.Close()
	}
}

func (c runtimeClients) acquireLeader(ctx context.Context) (mgr.LeaderLease, error) {
	if c.lead == nil {
		return mgr.LeaderLease{}, fmt.Errorf("leader election is not configured")
	}
	return c.lead.Acquire(ctx)
}

func buildClients(ctx context.Context, cfg *config) (runtimeClients, error) {
	opts := []client.Option{}
	if cfg.insecure {
		opts = append(opts, client.WithPlaintext())
	}
	if cfg.token != "" {
		opts = append(opts, client.WithBearer(cfg.token))
	}
	grpcClient, err := client.Dial(ctx, cfg.endpoint, opts...)
	if err != nil {
		return runtimeClients{}, err
	}
	audit, err := mgr.NewHTTPAuditClient(cfg.httpEndpoint, cfg.token, http.DefaultClient)
	if err != nil {
		_ = grpcClient.Close()
		return runtimeClients{}, err
	}
	clients := runtimeClients{grpc: grpcClient, cefas: &mgr.SDKCefas{GRPC: grpcClient, Audit: audit}}
	if !cfg.kubeDisabled || cfg.leaderElection {
		kubeClient, namespace, err := mgr.NewInClusterKubeClient()
		if err != nil {
			if cfg.leaderElection {
				_ = grpcClient.Close()
				return runtimeClients{}, err
			}
		} else {
			if cfg.namespace == "" {
				cfg.namespace = namespace
			}
			clients.kube = kubeClient
			if cfg.leaderElection {
				clients.lead = &mgr.KubeLeaderElector{
					Client: kubeClient,
					Opts: mgr.LeaderElectionOptions{
						Namespace: cfg.namespace,
						Name:      cfg.leaderLease,
						HolderID:  cfg.managerID,
						TTL:       cfg.leaderTTL,
						Labels:    map[string]string{"app.kubernetes.io/component": "cefas-manager"},
					},
				}
			}
		}
	}
	return clients, nil
}

func defaultConfig() config {
	host, _ := os.Hostname()
	if host == "" {
		host = "cefas-manager"
	}
	return config{
		endpoint:       envDefault("CEFAS_ENDPOINT", "127.0.0.1:9090"),
		httpEndpoint:   envDefault("CEFAS_HTTP_ENDPOINT", "http://127.0.0.1:8080"),
		token:          os.Getenv("CEFAS_TOKEN"),
		tokenFile:      os.Getenv("CEFAS_TOKEN_FILE"),
		insecure:       envBoolDefault("CEFAS_INSECURE", true),
		timeout:        envDurationDefault("CEFAS_MANAGER_TIMEOUT", 30*time.Second),
		namespace:      os.Getenv("POD_NAMESPACE"),
		selector:       envDefault("CEFAS_KUBE_SELECTOR", "app.kubernetes.io/name=cefas"),
		output:         "json",
		managerID:      envDefault("CEFAS_MANAGER_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		leaderElection: envBoolDefault("CEFAS_MANAGER_LEADER_ELECTION", false),
		leaderLease:    envDefault("CEFAS_MANAGER_LEADER_LEASE", "cefas-manager"),
		leaderTTL:      envDurationDefault("CEFAS_MANAGER_LEADER_TTL", 30*time.Second),
		auditLog:       os.Getenv("CEFAS_MANAGER_AUDIT_LOG"),
		interval:       envDurationDefault("CEFAS_MANAGER_INTERVAL", 30*time.Second),
		repairMode:     envDefault("CEFAS_MANAGER_REPAIR_MODE", "observe"),
	}
}

func writeOutput(cmd *cobra.Command, format string, v any) error {
	switch format {
	case "", "json":
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	case "text":
		return writeText(cmd, v)
	default:
		return fmt.Errorf("invalid --output %q", format)
	}
}

func writeText(cmd *cobra.Command, v any) error {
	switch t := v.(type) {
	case mgr.DoctorReport:
		fmt.Fprintf(cmd.OutOrStdout(), "classification: %s\n", t.Classification)
		for _, sig := range t.Signals {
			fmt.Fprintf(cmd.OutOrStdout(), "- %s %s %s: %s\n", sig.Class, sig.Component, sig.Name, sig.Status)
			if sig.Detail != "" {
				fmt.Fprintf(cmd.OutOrStdout(), "  %s\n", sig.Detail)
			}
		}
		return nil
	case mgr.RepairResult:
		fmt.Fprintf(cmd.OutOrStdout(), "mode: %s\nclassification: %s\nactions: %d\n", t.Plan.Mode, t.Plan.Classification, len(t.Plan.Actions))
		if t.Error != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "error: %s\n", t.Error)
		}
		for _, action := range t.Plan.Actions {
			fmt.Fprintf(cmd.OutOrStdout(), "- %s %s supported=%t sensitive=%t\n", action.ID, action.Type, action.Supported, action.Sensitive)
		}
		return nil
	default:
		return writeOutput(cmd, "json", v)
	}
}

func writeJSONLine(w interface{ Write([]byte) (int, error) }, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBoolDefault(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func envDurationDefault(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return v
}
