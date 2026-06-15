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
		return runInteractiveREPL(ctx, session, out, errOut)
	}
	return runScriptedREPL(ctx, session, in, out, errOut)
}

func runInteractiveREPL(ctx context.Context, session *runtime.Session, out, errOut io.Writer) error {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:                 replPrompt(ctx, session),
		HistoryFile:            replHistoryPath(),
		HistoryLimit:           1000,
		DisableAutoSaveHistory: true,
		HistorySearchFold:      true,
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
		exit, err := executeREPLLine(ctx, session, line, out, errOut)
		if err != nil {
			fmt.Fprintln(errOut, "cefas:", err)
			continue
		}
		if exit {
			return nil
		}
	}
}

func runScriptedREPL(ctx context.Context, session *runtime.Session, in io.Reader, out, errOut io.Writer) error {
	reader := bufio.NewReader(in)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			exit, execErr := executeREPLLine(ctx, session, line, out, errOut)
			if execErr != nil {
				fmt.Fprintln(errOut, "cefas:", execErr)
				return execErr
			}
			if exit {
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

func executeREPLLine(ctx context.Context, session *runtime.Session, line string, out, errOut io.Writer) (bool, error) {
	args, err := parseREPLArgs(line)
	if err != nil {
		return false, err
	}
	if len(args) == 0 {
		return false, nil
	}
	if args[0] == "?" {
		args[0] = "help"
	}
	handled, exit, err := handleREPLBuiltin(ctx, session, args, out)
	if handled {
		return exit, err
	}
	return false, runCommand(ctx, session, args, strings.NewReader(""), out, errOut)
}

func handleREPLBuiltin(ctx context.Context, session *runtime.Session, args []string, out io.Writer) (bool, bool, error) {
	switch strings.ToLower(args[0]) {
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
	fmt.Fprintf(out, "profile: %s\n", profileName)
	fmt.Fprintf(out, "endpoint: %s\n", profile.Endpoint)
	fmt.Fprintf(out, "output: %s\n", profile.Output)
	fmt.Fprintf(out, "insecure: %t\n", profile.Insecure)
	if opts.ConfigPath != "" {
		fmt.Fprintf(out, "config: %s\n", opts.ConfigPath)
	}
	if profile.TLSCAPath != "" {
		fmt.Fprintf(out, "ca: %s\n", profile.TLSCAPath)
	}
	if profile.TokenFile != "" {
		fmt.Fprintf(out, "tokenFile: %s\n", profile.TokenFile)
	} else if profile.Token != "" {
		fmt.Fprintln(out, "token: <configured>")
	}
	if opts.NoStream {
		fmt.Fprintln(out, "noStream: true")
	}
	return nil
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
	fmt.Fprintln(out, "OK")
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
	fmt.Fprintln(out, "OK")
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
	return fmt.Sprintf("cefas[%s@%s]> ", name, endpoint)
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
