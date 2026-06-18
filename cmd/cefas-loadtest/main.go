package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"time"

	"google.golang.org/grpc"

	runner "github.com/CefasDb/cefasdb/cmd/cefas-loadtest/runner"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

var logger = slog.New(slog.NewTextHandler(os.Stderr, nil))

func fatal(msg string, args ...any) {
	logger.Error(msg, args...)
	os.Exit(1)
}

type config struct {
	addr        string
	addrs       string
	token       string
	plaintext   bool
	createTable bool
	runner.Config
}

func main() {
	cfg := parseFlags()
	startedAt := time.Now()
	var phases []runner.PhaseSummary

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := []client.Option{
		client.WithDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(128<<20),
			grpc.MaxCallRecvMsgSize(128<<20),
		)),
	}
	if cfg.plaintext {
		opts = append(opts, client.WithPlaintext())
	}
	if cfg.token != "" {
		opts = append(opts, client.WithBearer(cfg.token))
	}

	addr := cfg.addr
	if cfg.addrs != "" {
		var err error
		addr, err = selectLeader(ctx, cfg.addrs, opts)
		if err != nil {
			fatal("select leader", "err", err)
		}
	}
	fmt.Printf("target: %s\n", addr)

	cli, err := client.Dial(ctx, addr, opts...)
	if err != nil {
		fatal("dial", "err", err)
	}
	defer cli.Close()

	if cfg.createTable {
		if err := ensureTable(ctx, cli, cfg.Table); err != nil {
			fatal("create table", "err", err)
		}
	}

	var wrote int64
	if cfg.Items > 0 || cfg.WriteDuration > 0 {
		stats, err := runner.RunWritePhase(ctx, cfg.Config, cli)
		runner.PrintStats(stats)
		phases = append(phases, runner.SummarizeStats(stats))
		if err != nil {
			fatal("write load failed", "err", err)
		}
		wrote = stats.Units
	}

	if cfg.Reads > 0 || cfg.ReadDuration > 0 {
		keyspace := cfg.Keyspace
		if keyspace == 0 {
			keyspace = wrote
		}
		if keyspace == 0 {
			fatal("invalid flags", "reason", "--reads requires --keyspace when --items is 0")
		}
		stats, found, err := runner.RunReadPhase(ctx, cfg.Config, cli, keyspace)
		stats.Found = found
		runner.PrintStats(stats)
		fmt.Printf("read found: %d/%d\n", found, stats.Units)
		phases = append(phases, runner.SummarizeStats(stats))
		if err != nil {
			fatal("read load failed", "err", err)
		}
	}

	if cfg.MixedDuration > 0 {
		keyspace := cfg.Keyspace
		if keyspace == 0 {
			keyspace = wrote
		}
		if keyspace == 0 {
			fatal("invalid flags", "reason", "--mixed-duration requires --keyspace or a preceding write phase")
		}
		writeStartID := cfg.StartID + keyspace
		writeStats, readStats, found, err := runner.RunMixedPhase(ctx, cfg.Config, cli, keyspace, writeStartID)
		readStats.Found = found
		runner.PrintStats(writeStats)
		runner.PrintStats(readStats)
		fmt.Printf("mixed read found: %d/%d\n", found, readStats.Units)
		phases = append(phases, runner.SummarizeStats(writeStats), runner.SummarizeStats(readStats))
		if err != nil {
			fatal("mixed load failed", "err", err)
		}
	}

	if cfg.JSONOutput != "" {
		if err := runner.WriteReport(cfg.Config, addr, startedAt, time.Now(), phases); err != nil {
			fatal("write json report", "err", err)
		}
		fmt.Printf("json report: %s\n", cfg.JSONOutput)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "localhost:9090", "gRPC address")
	flag.StringVar(&cfg.addrs, "addrs", "", "comma-separated gRPC addresses; when set, the current leader is selected automatically")
	flag.StringVar(&cfg.Table, "table", "MassiveLoad", "table name")
	flag.StringVar(&cfg.token, "token", "", "bearer token")
	flag.BoolVar(&cfg.plaintext, "plaintext", true, "use plaintext gRPC")
	flag.BoolVar(&cfg.createTable, "create-table", true, "create the table if it does not exist")
	flag.Int64Var(&cfg.Items, "items", 1_000_000, "items to write")
	flag.Int64Var(&cfg.StartID, "start-id", 0, "first numeric item id to write")
	flag.Int64Var(&cfg.Keyspace, "keyspace", 0, "existing keyspace for read-only tests; defaults to --items")
	flag.Int64Var(&cfg.Reads, "reads", 100_000, "get-item reads after the write phase")
	flag.DurationVar(&cfg.WriteDuration, "write-duration", 0, "write for this duration instead of stopping at --items")
	flag.DurationVar(&cfg.ReadDuration, "read-duration", 0, "read for this duration instead of stopping at --reads")
	flag.DurationVar(&cfg.MixedDuration, "mixed-duration", 0, "run reads and writes concurrently for this duration after any write/read phases")
	flag.IntVar(&cfg.BatchSize, "batch-size", 500, "items per BatchWriteItem RPC")
	flag.IntVar(&cfg.Workers, "workers", 64, "concurrent write workers")
	flag.IntVar(&cfg.ReadWorkers, "read-workers", 64, "concurrent read workers")
	flag.Int64Var(&cfg.WriteRate, "write-rate", 0, "target write units per second; 0 means uncapped")
	flag.Int64Var(&cfg.ReadRate, "read-rate", 0, "target read units per second; 0 means uncapped")
	flag.Int64Var(&cfg.Users, "users", 100_000, "distinct partition keys")
	flag.IntVar(&cfg.PayloadBytes, "payload-bytes", 256, "bytes in the payload attribute")
	flag.StringVar(&cfg.PayloadMode, "payload-mode", runner.PayloadModeRepeat, "payload generation mode: repeat or random")
	flag.DurationVar(&cfg.RPCTimeout, "rpc-timeout", 30*time.Second, "timeout per RPC")
	flag.DurationVar(&cfg.Progress, "progress", 5*time.Second, "progress print interval; 0 disables")
	flag.Int64Var(&cfg.LatencySampleRate, "latency-sample-rate", 1, "record one latency sample every N RPCs")
	flag.StringVar(&cfg.JSONOutput, "json-output", "", "write benchmark summary to this JSON file")
	flag.StringVar(&cfg.Label, "label", "", "label stored in the JSON report")
	flag.BoolVar(&cfg.StrongRead, "strong-read", false, "use strong consistency for get-item reads")
	flag.Parse()

	if cfg.Items < 0 || cfg.Reads < 0 {
		fatal("invalid flags", "reason", "--items and --reads must be >= 0")
	}
	if cfg.WriteDuration < 0 || cfg.ReadDuration < 0 {
		fatal("invalid flags", "reason", "--write-duration and --read-duration must be >= 0")
	}
	if cfg.MixedDuration < 0 {
		fatal("invalid flags", "reason", "--mixed-duration must be >= 0")
	}
	if cfg.StartID < 0 {
		fatal("invalid flags", "reason", "--start-id must be >= 0")
	}
	if cfg.BatchSize <= 0 {
		fatal("invalid flags", "reason", "--batch-size must be > 0")
	}
	if cfg.Workers <= 0 || cfg.ReadWorkers <= 0 {
		fatal("invalid flags", "reason", "--workers and --read-workers must be > 0")
	}
	if cfg.WriteRate < 0 || cfg.ReadRate < 0 {
		fatal("invalid flags", "reason", "--write-rate and --read-rate must be >= 0")
	}
	if cfg.Users <= 0 {
		fatal("invalid flags", "reason", "--users must be > 0")
	}
	if cfg.PayloadBytes < 0 {
		fatal("invalid flags", "reason", "--payload-bytes must be >= 0")
	}
	payloadMode, err := runner.NormalizePayloadMode(cfg.PayloadMode)
	if err != nil {
		fatal("invalid flags", "reason", err)
	}
	cfg.PayloadMode = payloadMode
	if cfg.LatencySampleRate <= 0 {
		fatal("invalid flags", "reason", "--latency-sample-rate must be > 0")
	}
	return cfg
}

func selectLeader(ctx context.Context, addrs string, opts []client.Option) (string, error) {
	candidates := strings.Split(addrs, ",")
	var firstReachable string
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		for _, raw := range candidates {
			addr := strings.TrimSpace(raw)
			if addr == "" {
				continue
			}
			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			cli, err := client.Dial(probeCtx, addr, opts...)
			if err != nil {
				cancel()
				lastErr = err
				continue
			}
			status, err := cli.Status(probeCtx)
			_ = cli.Close()
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			if firstReachable == "" {
				firstReachable = addr
			}
			fmt.Printf("probe %s: mode=%s self=%s leader=%v\n", addr, status.Mode, status.SelfID, status.IsLeader)
			if status.IsLeader {
				return addr, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if firstReachable != "" {
		return firstReachable, fmt.Errorf("no leader reported by %s", addrs)
	}
	return "", fmt.Errorf("no reachable node in %s: %w", addrs, lastErr)
}

func ensureTable(ctx context.Context, cli *client.Client, table string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err := cli.CreateTable(rpcCtx, types.TableDescriptor{
		Name:      table,
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
	})
	if err == nil {
		fmt.Printf("created table: %s\n", table)
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "already") || strings.Contains(msg, "exist") {
		fmt.Printf("using existing table: %s\n", table)
		return nil
	}
	return err
}
