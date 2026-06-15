package cmd

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func TestParseREPLArgsExpandsDDBShortcuts(t *testing.T) {
	tests := []struct {
		name string
		line string
		want []string
	}{
		{
			name: "tables",
			line: "TABLES",
			want: []string{"list-tables"},
		},
		{
			name: "get consistent",
			line: `GET Users '{"pk":{"S":"USER#1"}}' CONSISTENT`,
			want: []string{"get-item", "--table-name", "Users", "--key", `{"pk":{"S":"USER#1"}}`, "--consistent-read"},
		},
		{
			name: "query",
			line: `QUERY Events '{"S":"USER#1"}' SK '{"S":"A"}' '{"S":"Z"}' LIMIT 25 INDEX by_status CONSISTENT`,
			want: []string{
				"query",
				"--table-name", "Events",
				"--pk-value", `{"S":"USER#1"}`,
				"--sk-low", `{"S":"A"}`,
				"--sk-high", `{"S":"Z"}`,
				"--limit", "25",
				"--index-name", "by_status",
				"--consistent-read",
			},
		},
		{
			name: "scan expression",
			line: `SCAN Users FILTER status = :active VALUES '{":active":{"S":"active"}}' LIMIT 10`,
			want: []string{
				"scan",
				"--table-name", "Users",
				"--filter-expression", "status = :active",
				"--expression-attribute-values", `{":active":{"S":"active"}}`,
				"--limit", "10",
			},
		},
		{
			name: "sql joins statement tokens",
			line: `SQL SELECT * FROM Users WHERE pk = ? PARAMS '[{"S":"USER#1"}]'`,
			want: []string{
				"execute-statement",
				"--statement", "SELECT * FROM Users WHERE pk = ?",
				"--parameters", `[{"S":"USER#1"}]`,
			},
		},
		{
			name: "normal command unchanged",
			line: `list-tables --output table`,
			want: []string{"list-tables", "--output", "table"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseREPLArgs(tt.line)
			if err != nil {
				t.Fatalf("parseREPLArgs: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("args = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestREPLBuiltinsMutateSession(t *testing.T) {
	session := runtime.NewSession(runtime.Options{
		ConfigPath: "/path/that/does/not/exist.yaml",
		Output:     "json",
	})
	var out bytes.Buffer

	exit, err := executeREPLLine(context.Background(), session, "set output table", &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("set output: %v", err)
	}
	if exit {
		t.Fatal("set output requested exit")
	}
	if got := session.Options().Output; got != "table" {
		t.Fatalf("session output = %q, want table", got)
	}

	out.Reset()
	exit, err = executeREPLLine(context.Background(), session, "set insecure false", &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("set insecure: %v", err)
	}
	if exit {
		t.Fatal("set insecure requested exit")
	}
	if got := session.Options().Insecure; got {
		t.Fatalf("session insecure = %v, want false", got)
	}

	out.Reset()
	exit, err = executeREPLLine(context.Background(), session, "show", &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("show: %v", err)
	}
	if exit {
		t.Fatal("show requested exit")
	}
	if !strings.Contains(out.String(), "output: table") {
		t.Fatalf("show output = %q, want output table", out.String())
	}
}

func TestRunCommandDoesNotLeakPersistentFlags(t *testing.T) {
	session := runtime.NewSession(runtime.Options{Output: "json"})
	var out bytes.Buffer
	var errOut bytes.Buffer
	if err := runCommand(context.Background(), session, []string{"--output", "text", "version"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatalf("runCommand: %v\nstderr: %s", err, errOut.String())
	}
	if got := strings.TrimSpace(out.String()); got != Version {
		t.Fatalf("version output = %q, want %q", got, Version)
	}
	if got := session.Options().Output; got != "json" {
		t.Fatalf("base session output = %q, want json", got)
	}
}

func TestRootWithoutArgsRunsScriptedREPL(t *testing.T) {
	session := runtime.NewSession(runtime.Options{
		ConfigPath: "/path/that/does/not/exist.yaml",
	})
	root := rootWithSession(session, rootModeCLI)
	var out bytes.Buffer
	var errOut bytes.Buffer
	root.SetContext(runtime.WithSession(context.Background(), session))
	root.SetArgs([]string{"--endpoint", "localhost:19090"})
	root.SetIn(strings.NewReader("show\nexit\n"))
	root.SetOut(&out)
	root.SetErr(&errOut)
	if err := root.Execute(); err != nil {
		t.Fatalf("root Execute: %v\nstderr: %s", err, errOut.String())
	}
	if got := out.String(); !strings.Contains(got, "endpoint: localhost:19090") {
		t.Fatalf("repl output = %q, want endpoint from root flag", got)
	}
}

func TestShouldSaveHistoryRejectsInlineTokens(t *testing.T) {
	tests := []struct {
		line string
		want bool
	}{
		{line: "", want: false},
		{line: "# comment", want: false},
		{line: "GET Users '{}'", want: true},
		{line: "list-tables --token secret", want: false},
		{line: "list-tables --token=secret", want: false},
		{line: "set token secret", want: false},
		{line: "set token-file /tmp/token", want: true},
	}

	for _, tt := range tests {
		if got := shouldSaveHistory(tt.line); got != tt.want {
			t.Fatalf("shouldSaveHistory(%q) = %v, want %v", tt.line, got, tt.want)
		}
	}
}
