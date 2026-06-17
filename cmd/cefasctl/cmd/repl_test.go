package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
	"github.com/osvaldoandrade/cefas/internal/server"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/protocol"
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

	result, err := executeREPLLine(context.Background(), session, "set output table", &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("set output: %v", err)
	}
	if result.Exit {
		t.Fatal("set output requested exit")
	}
	if got := session.Options().Output; got != "table" {
		t.Fatalf("session output = %q, want table", got)
	}

	out.Reset()
	result, err = executeREPLLine(context.Background(), session, "set insecure false", &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("set insecure: %v", err)
	}
	if result.Exit {
		t.Fatal("set insecure requested exit")
	}
	if got := session.Options().Insecure; got {
		t.Fatalf("session insecure = %v, want false", got)
	}

	out.Reset()
	result, err = executeREPLLine(context.Background(), session, "show", &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("show: %v", err)
	}
	if result.Exit {
		t.Fatal("show requested exit")
	}
	if !strings.Contains(out.String(), "output") || !strings.Contains(out.String(), "table") {
		t.Fatalf("show output = %q, want output table", out.String())
	}
}

func TestREPLHelpIsShellSpecific(t *testing.T) {
	session := runtime.NewSession(runtime.Options{
		ConfigPath: "/path/that/does/not/exist.yaml",
	})
	var out bytes.Buffer
	result, err := executeREPLLine(context.Background(), session, "help", &out, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("help: %v", err)
	}
	if !result.Builtin {
		t.Fatal("help should be handled by REPL")
	}
	got := out.String()
	for _, want := range []string{"Cefas interactive shell", "put <table>", "set output table|json|text", "Existing CLI commands"} {
		if !strings.Contains(got, want) {
			t.Fatalf("help output = %q, missing %q", got, want)
		}
	}
}

func TestParseREPLTokenDetailsPreservesQuotedValues(t *testing.T) {
	tokens, err := parseREPLTokenDetails(`put Users 1 age=42 code="42" name="Ana Maria" tags=[vip,beta]`)
	if err != nil {
		t.Fatalf("parseREPLTokenDetails: %v", err)
	}
	got := replTokenTexts(tokens)
	want := []string{"put", "Users", "1", "age=42", "code=42", "name=Ana Maria", "tags=[vip,beta]"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens = %#v, want %#v", got, want)
	}
	if !tokens[4].Quoted || !tokens[5].Quoted {
		t.Fatalf("quoted value flags = %#v", tokens)
	}
	if tokens[3].Quoted || tokens[6].Quoted {
		t.Fatalf("unquoted value flags = %#v", tokens)
	}
}

func TestSimpleValueAttrInfersTypes(t *testing.T) {
	tests := []struct {
		token replToken
		want  string
	}{
		{token: replToken{Text: "42"}, want: `{"N":"42"}`},
		{token: replToken{Text: "42", Quoted: true}, want: `{"S":"42"}`},
		{token: replToken{Text: "true"}, want: `{"BOOL":true}`},
		{token: replToken{Text: "null"}, want: `{"NULL":true}`},
		{token: replToken{Text: "[a,2,false]"}, want: `{"L":[{"S":"a"},{"N":"2"},{"BOOL":false}]}`},
	}
	for _, tt := range tests {
		attr, err := simpleValueAttr(tt.token)
		if err != nil {
			t.Fatalf("simpleValueAttr(%#v): %v", tt.token, err)
		}
		raw, err := marshalDDBAttr(attr)
		if err != nil {
			t.Fatalf("marshalDDBAttr: %v", err)
		}
		if raw != tt.want {
			t.Fatalf("attr = %s, want %s", raw, tt.want)
		}
	}
}

func TestSimpleCreateExpansion(t *testing.T) {
	tokens, err := parseREPLTokenDetails("create Users user_id ts")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := expandREPLCommand(context.Background(), runtime.NewSession(runtime.Options{}), tokens)
	if err != nil {
		t.Fatalf("expandREPLCommand: %v", err)
	}
	want := []string{
		"create-table",
		"--table-name", "Users",
		"--attribute-definitions", "AttributeName=user_id,AttributeType=S",
		"--key-schema", "AttributeName=user_id,KeyType=HASH",
		"--attribute-definitions", "AttributeName=ts,AttributeType=S",
		"--key-schema", "AttributeName=ts,KeyType=RANGE",
	}
	if !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("args = %#v, want %#v", got.Args, want)
	}
}

func TestSimpleScanWithConsistentExpansion(t *testing.T) {
	tokens, err := parseREPLTokenDetails("scan picpay_match_keys where source=picpay limit 10 consistent")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := expandREPLCommand(context.Background(), runtime.NewSession(runtime.Options{}), tokens)
	if err != nil {
		t.Fatalf("expandREPLCommand: %v", err)
	}
	want := []string{
		"scan",
		"--table-name", "picpay_match_keys",
		"--filter-expression", "source = :v0",
		"--expression-attribute-values", `{":v0":{"S":"picpay"}}`,
		"--limit", "10",
		"--consistent-read",
	}
	if !reflect.DeepEqual(got.Args, want) {
		t.Fatalf("args = %#v, want %#v", got.Args, want)
	}
}

func TestInteractiveDefaultsPreferTableOutput(t *testing.T) {
	session := runtime.NewSession(runtime.Options{})
	applyInteractiveDefaults(session)
	if got := session.Options().Output; got != "table" {
		t.Fatalf("interactive output default = %q, want table", got)
	}

	session = runtime.NewSession(runtime.Options{Output: "json"})
	applyInteractiveDefaults(session)
	if got := session.Options().Output; got != "json" {
		t.Fatalf("explicit output = %q, want json", got)
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
	if got := out.String(); !strings.Contains(got, "endpoint") || !strings.Contains(got, "localhost:19090") {
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

type replCLIFixture struct {
	addr string
}

func newREPLCLIFixture(t *testing.T) replCLIFixture {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("storage open: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, server.NewGRPCServer(db, cat, nil))
	go func() { _ = gsrv.Serve(ln) }()
	t.Cleanup(func() {
		gsrv.GracefulStop()
		_ = ln.Close()
		_ = db.Close()
	})
	return replCLIFixture{addr: ln.Addr().String()}
}

func TestSimpleREPLEndToEnd(t *testing.T) {
	fx := newREPLCLIFixture(t)
	session := runtime.NewSession(runtime.Options{
		ConfigPath: "/path/that/does/not/exist.yaml",
	})
	root := rootWithSession(session, rootModeCLI)
	var out bytes.Buffer
	var errOut bytes.Buffer
	root.SetContext(runtime.WithSession(context.Background(), session))
	root.SetArgs([]string{"--endpoint", fx.addr, "--insecure", "--output", "json"})
	root.SetIn(strings.NewReader(strings.Join([]string{
		"create Users",
		`put Users user-1 name=Ana age=42 active=true code="42" tags=[vip,beta]`,
		"get Users user-1 consistent",
		"query Users user-1 limit 10",
		"delete Users user-1",
		"drop Users",
		"exit",
	}, "\n") + "\n"))
	root.SetOut(&out)
	root.SetErr(&errOut)
	if err := root.Execute(); err != nil {
		t.Fatalf("root Execute: %v\nstderr: %s", err, errOut.String())
	}
	got := out.String()
	for _, want := range []string{`"TableName": "Users"`, `"S": "Ana"`, `"N": "42"`, `"BOOL": true`, `"S": "42"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("REPL output missing %q:\n%s", want, got)
		}
	}
	var decoded struct {
		Item map[string]json.RawMessage `json:"Item"`
	}
	if err := json.Unmarshal([]byte(extractLastJSONObjectContaining(got, `"Item"`)), &decoded); err != nil {
		t.Fatalf("decode get item output: %v\n%s", err, got)
	}
	if decoded.Item["id"] == nil || decoded.Item["name"] == nil {
		t.Fatalf("decoded item = %+v", decoded.Item)
	}
}

func extractLastJSONObjectContaining(s, marker string) string {
	idx := strings.LastIndex(s, marker)
	if idx < 0 {
		return ""
	}
	start := strings.LastIndex(s[:idx], "{")
	end := strings.Index(s[idx:], "\n}")
	if start < 0 || end < 0 {
		return ""
	}
	return s[start : idx+end+2]
}
