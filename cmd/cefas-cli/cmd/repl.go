package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/chzyer/readline"
	"github.com/mattn/go-isatty"
	"github.com/mattn/go-shellwords"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func runREPL(ctx context.Context, session *runtime.Session, in io.Reader, out, errOut io.Writer) error {
	if session == nil {
		session = runtime.NewSession(runtime.Options{})
	}
	ctx = runtime.WithSession(ctx, session)
	if isTerminal(in, out) {
		applyInteractiveDefaults(session)
		return runInteractiveREPL(ctx, session, out, errOut)
	}
	return runScriptedREPL(ctx, session, in, out, errOut)
}

func runInteractiveREPL(ctx context.Context, session *runtime.Session, out, errOut io.Writer) error {
	printREPLBanner(ctx, session, out)
	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 replPrompt(ctx, session),
		HistoryFile:            replHistoryPath(),
		HistoryLimit:           1000,
		DisableAutoSaveHistory: true,
		HistorySearchFold:      true,
		AutoComplete:           replCompleter(),
		InterruptPrompt:        "^C",
		EOFPrompt:              "exit",
		Stdout:                 out,
		Stderr:                 errOut,
	})
	if err != nil {
		return err
	}
	defer rl.Close()

	for {
		rl.SetPrompt(replPrompt(ctx, session))
		line, err := rl.Readline()
		switch {
		case errors.Is(err, readline.ErrInterrupt):
			continue
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		}
		line = strings.TrimSpace(line)
		if shouldSaveHistory(line) {
			_ = rl.SaveHistory(line)
		}
		start := time.Now()
		result, err := executeREPLLine(ctx, session, line, out, errOut)
		if err != nil {
			printREPLError(errOut, err, time.Since(start))
			continue
		}
		if result.Exit {
			return nil
		}
		if !result.Empty && !result.Builtin {
			printREPLStatus(errOut, time.Since(start))
		}
	}
}

func runScriptedREPL(ctx context.Context, session *runtime.Session, in io.Reader, out, errOut io.Writer) error {
	reader := bufio.NewReader(in)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			result, execErr := executeREPLLine(ctx, session, line, out, errOut)
			if execErr != nil {
				fmt.Fprintln(errOut, "cefas:", execErr)
				return execErr
			}
			if result.Exit {
				return nil
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

type replLineResult struct {
	Exit    bool
	Empty   bool
	Builtin bool
}

func executeREPLLine(ctx context.Context, session *runtime.Session, line string, out, errOut io.Writer) (replLineResult, error) {
	args, err := parseREPLArgs(line)
	if err != nil {
		return replLineResult{}, err
	}
	if len(args) == 0 {
		return replLineResult{Empty: true}, nil
	}
	if args[0] == "?" {
		args[0] = "help"
	}
	handled, exit, err := handleREPLBuiltin(ctx, session, args, out)
	if handled {
		return replLineResult{Exit: exit, Builtin: true}, err
	}
	return replLineResult{}, runCommand(ctx, session, args, strings.NewReader(""), out, errOut)
}

func handleREPLBuiltin(ctx context.Context, session *runtime.Session, args []string, out io.Writer) (bool, bool, error) {
	switch strings.ToLower(args[0]) {
	case "help":
		if len(args) == 1 || strings.EqualFold(args[1], "repl") {
			return true, false, showREPLHelp(out)
		}
		if len(args) == 2 && (strings.EqualFold(args[1], "shortcuts") || strings.EqualFold(args[1], "settings")) {
			return true, false, showREPLHelp(out)
		}
		return false, false, nil
	case "shortcuts":
		if len(args) != 1 {
			return true, false, fmt.Errorf("usage: shortcuts")
		}
		return true, false, showREPLHelp(out)
	case "exit", "quit":
		if len(args) != 1 {
			return true, false, fmt.Errorf("usage: %s", args[0])
		}
		return true, true, nil
	case "clear":
		if len(args) != 1 {
			return true, false, fmt.Errorf("usage: clear")
		}
		fmt.Fprint(out, "\033[H\033[2J")
		return true, false, nil
	case "show":
		if len(args) != 1 {
			return true, false, fmt.Errorf("usage: show")
		}
		return true, false, showREPLSession(ctx, session, out)
	case "use":
		if len(args) != 2 {
			return true, false, fmt.Errorf("usage: use <profile>")
		}
		return true, false, useREPLProfile(ctx, session, args[1], out)
	case "set":
		return true, false, setREPLSessionOption(session, args, out)
	default:
		return false, false, nil
	}
}

func showREPLHelp(out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "Cefas interactive shell")
	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "Shortcuts\t")
	fmt.Fprintln(tw, "  TABLES\tList tables")
	fmt.Fprintln(tw, "  DESC <table>\tDescribe a table")
	fmt.Fprintln(tw, "  GET <table> <key-json> [CONSISTENT]\tFetch one item")
	fmt.Fprintln(tw, "  PUT <table> <item-json>\tInsert or replace one item")
	fmt.Fprintln(tw, "  DELETE <table> <key-json>\tDelete one item")
	fmt.Fprintln(tw, "  QUERY <table> <pk-json> [SK <low> <high>] [LIMIT n] [INDEX name] [CONSISTENT]\tRun a key query")
	fmt.Fprintln(tw, "  SCAN <table> [FILTER expr] [VALUES ddb-json] [LIMIT n] [CONSISTENT]\tScan a table")
	fmt.Fprintln(tw, "  SQL <statement> [PARAMS ddb-json-array]\tRun PartiQL")
	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "Session\t")
	fmt.Fprintln(tw, "  show\tShow active profile, endpoint, output, and transport")
	fmt.Fprintln(tw, "  use <profile>\tSwitch profile for this REPL session")
	fmt.Fprintln(tw, "  set endpoint <host:port>\tChange target endpoint")
	fmt.Fprintln(tw, "  set output table|json|text\tChange output format")
	fmt.Fprintln(tw, "  set insecure [true|false]\tToggle plaintext transport")
	fmt.Fprintln(tw, "  set token-file <path>\tUse a token file")
	fmt.Fprintln(tw, "  set ca <path>\tUse a TLS CA bundle")
	fmt.Fprintln(tw, "  clear\tClear the screen")
	fmt.Fprintln(tw, "  exit\tLeave the shell")
	fmt.Fprintln(tw)
	fmt.Fprintln(tw, "Tips\t")
	fmt.Fprintln(tw, "  Existing CLI commands still work unchanged.\tExample: list-tables --output json")
	fmt.Fprintln(tw, "  Use file://path for large JSON payloads.\tExample: PUT Users file://item.json")
	fmt.Fprintln(tw, "  Help for a CLI command still works.\tExample: help create-table")
	return tw.Flush()
}

func showREPLSession(ctx context.Context, session *runtime.Session, out io.Writer) error {
	profile, err := runtime.ResolveProfile(runtime.WithSession(ctx, session))
	if err != nil {
		return err
	}
	opts := session.Options()
	profileName := opts.ProfileName
	if profileName == "" {
		profileName = "default"
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "Setting\tValue")
	fmt.Fprintf(tw, "profile\t%s\n", profileName)
	fmt.Fprintf(tw, "endpoint\t%s\n", profile.Endpoint)
	fmt.Fprintf(tw, "output\t%s\n", profile.Output)
	fmt.Fprintf(tw, "transport\t%s\n", transportLabel(profile.Insecure))
	if opts.ConfigPath != "" {
		fmt.Fprintf(tw, "config\t%s\n", opts.ConfigPath)
	}
	if profile.TLSCAPath != "" {
		fmt.Fprintf(tw, "ca\t%s\n", profile.TLSCAPath)
	}
	if profile.TokenFile != "" {
		fmt.Fprintf(tw, "tokenFile\t%s\n", profile.TokenFile)
	} else if profile.Token != "" {
		fmt.Fprintln(tw, "token\t<configured>")
	}
	if opts.NoStream {
		fmt.Fprintln(tw, "noStream\ttrue")
	}
	return tw.Flush()
}

func useREPLProfile(ctx context.Context, session *runtime.Session, name string, out io.Writer) error {
	old := session.Options()
	next := old
	next.ProfileName = name
	session.Update(next)
	if _, err := runtime.ResolveProfile(runtime.WithSession(ctx, session)); err != nil {
		session.Update(old)
		return err
	}
	fmt.Fprintf(out, "profile = %s\n", name)
	return nil
}

func setREPLSessionOption(session *runtime.Session, args []string, out io.Writer) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: set endpoint|output|insecure|token-file|ca|no-stream <value>")
	}
	key := strings.ToLower(strings.ReplaceAll(args[1], "_", "-"))
	opts := session.Options()
	switch key {
	case "endpoint":
		value, err := singleSetValue(args, key)
		if err != nil {
			return err
		}
		opts.Endpoint = value
	case "output":
		value, err := singleSetValue(args, key)
		if err != nil {
			return err
		}
		if _, err := output.Validate(value); err != nil {
			return err
		}
		opts.Output = value
	case "insecure":
		value, err := optionalBoolSetValue(args, key)
		if err != nil {
			return err
		}
		opts.Insecure = value
	case "token-file":
		value, err := singleSetValue(args, key)
		if err != nil {
			return err
		}
		opts.TokenFile = value
	case "ca":
		value, err := singleSetValue(args, key)
		if err != nil {
			return err
		}
		opts.TLSCAPath = value
	case "no-stream":
		value, err := optionalBoolSetValue(args, key)
		if err != nil {
			return err
		}
		opts.NoStream = value
	default:
		return fmt.Errorf("unknown setting %q", args[1])
	}
	session.Update(opts)
	fmt.Fprintf(out, "%s = %s\n", key, replSettingValue(opts, key))
	return nil
}

func singleSetValue(args []string, key string) (string, error) {
	if len(args) != 3 {
		return "", fmt.Errorf("usage: set %s <value>", key)
	}
	if args[2] == "" {
		return "", fmt.Errorf("set %s requires a non-empty value", key)
	}
	return args[2], nil
}

func optionalBoolSetValue(args []string, key string) (bool, error) {
	switch len(args) {
	case 2:
		return true, nil
	case 3:
		value, err := strconv.ParseBool(args[2])
		if err == nil {
			return value, nil
		}
		switch strings.ToLower(args[2]) {
		case "on", "yes":
			return true, nil
		case "off", "no":
			return false, nil
		default:
			return false, fmt.Errorf("set %s expects true|false", key)
		}
	default:
		return false, fmt.Errorf("usage: set %s [true|false]", key)
	}
}

func replPrompt(ctx context.Context, session *runtime.Session) string {
	profile, err := runtime.ResolveProfile(runtime.WithSession(ctx, session))
	opts := session.Options()
	name := opts.ProfileName
	if name == "" {
		name = "default"
	}
	endpoint := profile.Endpoint
	if err != nil || endpoint == "" {
		endpoint = "unknown"
	}
	return fmt.Sprintf("\033[36mcefas\033[0m \033[2m%s@%s %s %s\033[0m\n> ",
		name,
		endpoint,
		profile.Output,
		transportLabel(profile.Insecure),
	)
}

func replHistoryPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	dir := filepath.Join(home, ".cefas")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return ""
	}
	return filepath.Join(dir, "repl_history")
}

func shouldSaveHistory(line string) bool {
	if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
		return false
	}
	parser := shellwordsParser()
	args, err := parser.Parse(line)
	if err != nil {
		return !strings.Contains(strings.ToLower(line), "--token")
	}
	for i, arg := range args {
		lower := strings.ToLower(arg)
		if lower == "--token" || strings.HasPrefix(lower, "--token=") {
			return false
		}
		if strings.EqualFold(arg, "set") && i+1 < len(args) && strings.EqualFold(args[i+1], "token") {
			return false
		}
	}
	return true
}

func shellwordsParser() *shellwords.Parser {
	parser := shellwords.NewParser()
	parser.ParseEnv = false
	parser.ParseBacktick = false
	return parser
}

func applyInteractiveDefaults(session *runtime.Session) {
	opts := session.Options()
	if opts.Output == "" {
		opts.Output = "table"
		session.Update(opts)
	}
}

func printREPLBanner(ctx context.Context, session *runtime.Session, out io.Writer) {
	profile, err := runtime.ResolveProfile(runtime.WithSession(ctx, session))
	opts := session.Options()
	name := opts.ProfileName
	if name == "" {
		name = "default"
	}
	endpoint := profile.Endpoint
	if err != nil || endpoint == "" {
		endpoint = "unknown"
	}
	fmt.Fprintf(out, "\033[36mCefas interactive shell\033[0m\n")
	fmt.Fprintf(out, "session: %s@%s  output=%s  transport=%s\n",
		name,
		endpoint,
		profile.Output,
		transportLabel(profile.Insecure),
	)
	fmt.Fprintln(out, "type help for shortcuts, show for session, exit to quit")
	fmt.Fprintln(out)
}

func printREPLStatus(out io.Writer, elapsed time.Duration) {
	fmt.Fprintf(out, "\033[32mok\033[0m %s\n", formatElapsed(elapsed))
}

func printREPLError(out io.Writer, err error, elapsed time.Duration) {
	fmt.Fprintf(out, "\033[31merror\033[0m %s: %v\n", formatElapsed(elapsed), err)
}

func formatElapsed(d time.Duration) string {
	if d < time.Millisecond {
		us := d.Microseconds()
		if us < 1 {
			us = 1
		}
		return fmt.Sprintf("%dus", us)
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(10 * time.Millisecond).String()
}

func replSettingValue(opts runtime.Options, key string) string {
	switch key {
	case "endpoint":
		return opts.Endpoint
	case "output":
		return opts.Output
	case "insecure":
		return strconv.FormatBool(opts.Insecure)
	case "token-file":
		return opts.TokenFile
	case "ca":
		return opts.TLSCAPath
	case "no-stream":
		return strconv.FormatBool(opts.NoStream)
	default:
		return ""
	}
}

func transportLabel(insecure bool) string {
	if insecure {
		return "plain"
	}
	return "tls"
}

func replCompleter() readline.AutoCompleter {
	return readline.NewPrefixCompleter(
		readline.PcItem("help"),
		readline.PcItem("shortcuts"),
		readline.PcItem("show"),
		readline.PcItem("use"),
		readline.PcItem("set",
			readline.PcItem("endpoint"),
			readline.PcItem("output",
				readline.PcItem("table"),
				readline.PcItem("json"),
				readline.PcItem("text"),
			),
			readline.PcItem("insecure"),
			readline.PcItem("token-file"),
			readline.PcItem("ca"),
			readline.PcItem("no-stream"),
		),
		readline.PcItem("clear"),
		readline.PcItem("exit"),
		readline.PcItem("quit"),
		readline.PcItem("TABLES"),
		readline.PcItem("DESC"),
		readline.PcItem("GET"),
		readline.PcItem("PUT"),
		readline.PcItem("DELETE"),
		readline.PcItem("QUERY"),
		readline.PcItem("SCAN"),
		readline.PcItem("SQL"),
		readline.PcItem("list-tables"),
		readline.PcItem("describe-table"),
		readline.PcItem("get-item"),
		readline.PcItem("put-item"),
		readline.PcItem("delete-item"),
		readline.PcItem("query"),
		readline.PcItem("scan"),
		readline.PcItem("execute-statement"),
	)
}

func isTerminal(in io.Reader, out io.Writer) bool {
	inFile, ok := in.(*os.File)
	if !ok {
		return false
	}
	outFile, ok := out.(*os.File)
	if !ok {
		return false
	}
	return isatty.IsTerminal(inFile.Fd()) && isatty.IsTerminal(outFile.Fd())
}
