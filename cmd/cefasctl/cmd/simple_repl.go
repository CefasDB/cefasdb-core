package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/internal/compat/ddbjson"
	"github.com/CefasDb/cefasdb/pkg/types"
)

var simpleNumberRE = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

type replExpansion struct {
	Args            []string
	Quiet           bool
	InvalidateTable string
}

func expandREPLCommand(ctx context.Context, session *runtime.Session, tokens []replToken) (replExpansion, error) {
	args := replTokenTexts(tokens)
	if len(args) == 0 {
		return replExpansion{}, nil
	}
	if commandHasFlags(args) {
		expanded, err := expandShortcut(args)
		return replExpansion{Args: expanded}, err
	}
	if expanded, ok, err := expandSimpleREPLCommand(ctx, session, tokens); ok || err != nil {
		return expanded, err
	}
	expanded, err := expandShortcut(args)
	return replExpansion{Args: expanded}, err
}

func expandSimpleREPLCommand(ctx context.Context, session *runtime.Session, tokens []replToken) (replExpansion, bool, error) {
	args := replTokenTexts(tokens)
	cmd := strings.ToLower(args[0])
	switch cmd {
	case "create", "create-table":
		if cmd == "create-table" && looksLikeAdvancedCreate(args) {
			return replExpansion{}, false, nil
		}
		expanded, err := expandSimpleCreate(args)
		table := ""
		if len(args) > 1 {
			table = args[1]
		}
		return replExpansion{Args: expanded, InvalidateTable: table}, true, err
	case "drop":
		expanded, err := expandSimpleDrop(args)
		table := ""
		if len(args) > 1 {
			table = args[1]
		}
		return replExpansion{Args: expanded, Quiet: true, InvalidateTable: table}, true, err
	case "delete-table":
		if len(args) != 2 {
			return replExpansion{}, false, nil
		}
		expanded, err := expandSimpleDrop(args)
		table := ""
		if len(args) > 1 {
			table = args[1]
		}
		return replExpansion{Args: expanded, Quiet: true, InvalidateTable: table}, true, err
	case "put", "put-item":
		if isLegacyPut(args) {
			return replExpansion{}, false, nil
		}
		expanded, err := expandSimplePut(ctx, session, tokens)
		return replExpansion{Args: expanded, Quiet: true}, true, err
	case "get", "get-item":
		if isLegacyKeyCommand(args) {
			return replExpansion{}, false, nil
		}
		expanded, err := expandSimpleGet(ctx, session, tokens)
		return replExpansion{Args: expanded}, true, err
	case "delete", "del", "delete-item":
		if isLegacyKeyCommand(args) {
			return replExpansion{}, false, nil
		}
		expanded, err := expandSimpleDelete(ctx, session, tokens)
		return replExpansion{Args: expanded, Quiet: true}, true, err
	case "query":
		if isLegacyQuery(args) {
			return replExpansion{}, false, nil
		}
		expanded, err := expandSimpleQuery(ctx, session, tokens)
		return replExpansion{Args: expanded}, true, err
	case "scan":
		if isLegacyScan(args) {
			return replExpansion{}, false, nil
		}
		expanded, err := expandSimpleScan(tokens)
		return replExpansion{Args: expanded}, true, err
	default:
		return replExpansion{}, false, nil
	}
}

func expandSimpleCreate(args []string) ([]string, error) {
	if len(args) < 2 || len(args) > 4 {
		return nil, fmt.Errorf("usage: create <table> [pk] [sk]")
	}
	table := args[1]
	pk := "id"
	if len(args) >= 3 {
		pk = args[2]
	}
	out := []string{
		"create-table",
		"--table-name", table,
		"--attribute-definitions", "AttributeName=" + pk + ",AttributeType=S",
		"--key-schema", "AttributeName=" + pk + ",KeyType=HASH",
	}
	if len(args) == 4 {
		sk := args[3]
		out = append(out,
			"--attribute-definitions", "AttributeName="+sk+",AttributeType=S",
			"--key-schema", "AttributeName="+sk+",KeyType=RANGE",
		)
	}
	return out, nil
}

func expandSimpleDrop(args []string) ([]string, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("usage: drop <table>")
	}
	return []string{"delete-table", "--table-name", args[1]}, nil
}

func expandSimplePut(ctx context.Context, session *runtime.Session, tokens []replToken) ([]string, error) {
	if len(tokens) < 3 {
		return nil, fmt.Errorf("usage: put <table> <pk> [sk] field=value...")
	}
	table := tokens[1].Text
	schema, err := loadREPLTableSchema(ctx, session, table)
	if err != nil {
		return nil, err
	}
	keyEnd, item, err := simpleKeyItem(schema, tokens, 2)
	if err != nil {
		return nil, err
	}
	if keyEnd >= len(tokens) {
		return nil, fmt.Errorf("usage: put %s <pk>%s field=value...", table, sortKeyUsage(schema))
	}
	for _, token := range tokens[keyEnd:] {
		name, value, err := parseSimpleAssignment(token)
		if err != nil {
			return nil, err
		}
		if name == schema.KeySchema.PK || name == schema.KeySchema.SK {
			return nil, fmt.Errorf("%q is a key attribute; pass it positionally", name)
		}
		item[name], err = simpleValueAttr(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	raw, err := marshalDDBItem(item)
	if err != nil {
		return nil, err
	}
	return []string{"put-item", "--table-name", table, "--item", raw}, nil
}

func expandSimpleGet(ctx context.Context, session *runtime.Session, tokens []replToken) ([]string, error) {
	if len(tokens) < 3 {
		return nil, fmt.Errorf("usage: get <table> <pk> [sk]")
	}
	table := tokens[1].Text
	schema, err := loadREPLTableSchema(ctx, session, table)
	if err != nil {
		return nil, err
	}
	keyEnd, key, err := simpleKeyItem(schema, tokens, 2)
	if err != nil {
		return nil, err
	}
	raw, err := marshalDDBItem(key)
	if err != nil {
		return nil, err
	}
	out := []string{"get-item", "--table-name", table, "--key", raw}
	if keyEnd < len(tokens) {
		if keyEnd+1 != len(tokens) || !strings.EqualFold(tokens[keyEnd].Text, "consistent") {
			return nil, fmt.Errorf("usage: get %s <pk>%s [consistent]", table, sortKeyUsage(schema))
		}
		out = append(out, "--consistent-read")
	}
	return out, nil
}

func expandSimpleDelete(ctx context.Context, session *runtime.Session, tokens []replToken) ([]string, error) {
	if len(tokens) < 3 {
		return nil, fmt.Errorf("usage: delete <table> <pk> [sk]")
	}
	table := tokens[1].Text
	schema, err := loadREPLTableSchema(ctx, session, table)
	if err != nil {
		return nil, err
	}
	keyEnd, key, err := simpleKeyItem(schema, tokens, 2)
	if err != nil {
		return nil, err
	}
	if keyEnd != len(tokens) {
		return nil, fmt.Errorf("usage: delete %s <pk>%s", table, sortKeyUsage(schema))
	}
	raw, err := marshalDDBItem(key)
	if err != nil {
		return nil, err
	}
	return []string{"delete-item", "--table-name", table, "--key", raw}, nil
}

func expandSimpleQuery(ctx context.Context, session *runtime.Session, tokens []replToken) ([]string, error) {
	if len(tokens) < 3 {
		return nil, fmt.Errorf("usage: query <table> <pk> [between <low> <high>] [limit n] [consistent]")
	}
	table := tokens[1].Text
	schema, err := loadREPLTableSchema(ctx, session, table)
	if err != nil {
		return nil, err
	}
	pk, err := simpleKeyValueAttr(schema, schema.KeySchema.PK, tokens[2])
	if err != nil {
		return nil, err
	}
	pkRaw, err := marshalDDBAttr(pk)
	if err != nil {
		return nil, err
	}
	out := []string{"query", "--table-name", table, "--pk-value", pkRaw}
	for i := 3; i < len(tokens); i++ {
		switch strings.ToLower(tokens[i].Text) {
		case "between":
			if schema.KeySchema.SK == "" {
				return nil, fmt.Errorf("table %s has no sort key; between is unavailable", table)
			}
			if i+2 >= len(tokens) {
				return nil, fmt.Errorf("query between requires <low> <high>")
			}
			lo, err := simpleKeyValueAttr(schema, schema.KeySchema.SK, tokens[i+1])
			if err != nil {
				return nil, err
			}
			hi, err := simpleKeyValueAttr(schema, schema.KeySchema.SK, tokens[i+2])
			if err != nil {
				return nil, err
			}
			loRaw, _ := marshalDDBAttr(lo)
			hiRaw, _ := marshalDDBAttr(hi)
			out = append(out, "--sk-low", loRaw, "--sk-high", hiRaw)
			i += 2
		case "limit":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("query limit requires a value")
			}
			if _, err := strconv.Atoi(tokens[i+1].Text); err != nil {
				return nil, fmt.Errorf("query limit must be an integer")
			}
			out = append(out, "--limit", tokens[i+1].Text)
			i++
		case "consistent":
			out = append(out, "--consistent-read")
		default:
			return nil, fmt.Errorf("query: unknown option %q", tokens[i].Text)
		}
	}
	return out, nil
}

func expandSimpleScan(tokens []replToken) ([]string, error) {
	if len(tokens) < 2 {
		return nil, fmt.Errorf("usage: scan <table> [where field=value] [limit n] [consistent]")
	}
	table := tokens[1].Text
	out := []string{"scan", "--table-name", table}
	for i := 2; i < len(tokens); i++ {
		switch strings.ToLower(tokens[i].Text) {
		case "where":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("scan where requires field=value")
			}
			name, value, next, err := parseSimpleWhere(tokens, i+1)
			if err != nil {
				return nil, err
			}
			attr, err := simpleValueAttr(value)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			bindsRaw, err := marshalDDBBinds(map[string]ddbjson.Attribute{":v0": attr})
			if err != nil {
				return nil, err
			}
			out = append(out, "--filter-expression", name+" = :v0", "--expression-attribute-values", bindsRaw)
			i = next
		case "limit":
			if i+1 >= len(tokens) {
				return nil, fmt.Errorf("scan limit requires a value")
			}
			if _, err := strconv.Atoi(tokens[i+1].Text); err != nil {
				return nil, fmt.Errorf("scan limit must be an integer")
			}
			out = append(out, "--limit", tokens[i+1].Text)
			i++
		case "consistent":
			out = append(out, "--consistent-read")
		default:
			return nil, fmt.Errorf("scan: unknown option %q", tokens[i].Text)
		}
	}
	return out, nil
}

func loadREPLTableSchema(ctx context.Context, session *runtime.Session, table string) (types.TableDescriptor, error) {
	if td, ok := session.CachedTable(table); ok {
		return td, nil
	}
	cli, _, err := runtime.Dial(runtime.WithSession(ctx, session))
	if err != nil {
		return types.TableDescriptor{}, err
	}
	defer cli.Close()
	td, err := cli.DescribeTable(ctx, table)
	if err != nil {
		return types.TableDescriptor{}, fmt.Errorf("describe table %s: %w", table, err)
	}
	session.CacheTable(td)
	return td, nil
}

func simpleKeyItem(schema types.TableDescriptor, tokens []replToken, start int) (int, map[string]ddbjson.Attribute, error) {
	if schema.KeySchema.PK == "" {
		return start, nil, fmt.Errorf("table %s has no partition key", schema.Name)
	}
	if start >= len(tokens) {
		return start, nil, fmt.Errorf("missing partition key value")
	}
	item := map[string]ddbjson.Attribute{}
	pk, err := simpleKeyValueAttr(schema, schema.KeySchema.PK, tokens[start])
	if err != nil {
		return start, nil, err
	}
	item[schema.KeySchema.PK] = pk
	next := start + 1
	if schema.KeySchema.SK != "" {
		if next >= len(tokens) {
			return next, nil, fmt.Errorf("table %s has sort key %s; use: %s <table> <pk> <sk>", schema.Name, schema.KeySchema.SK, tokens[0].Text)
		}
		sk, err := simpleKeyValueAttr(schema, schema.KeySchema.SK, tokens[next])
		if err != nil {
			return next, nil, err
		}
		item[schema.KeySchema.SK] = sk
		next++
	}
	return next, item, nil
}

func simpleKeyValueAttr(schema types.TableDescriptor, name string, token replToken) (ddbjson.Attribute, error) {
	switch keyAttrType(schema, name) {
	case "", "S":
		value := token.Text
		return ddbjson.Attribute{S: &value}, nil
	case "N":
		if !simpleNumberRE.MatchString(token.Text) {
			return ddbjson.Attribute{}, fmt.Errorf("%s key expects a number", name)
		}
		value := token.Text
		return ddbjson.Attribute{N: &value}, nil
	default:
		return ddbjson.Attribute{}, fmt.Errorf("%s key type is not supported by simple REPL commands; use DDB-JSON", name)
	}
}

func keyAttrType(schema types.TableDescriptor, name string) string {
	for _, def := range schema.AttributeDefinitions {
		if def.Name == name {
			return strings.ToUpper(def.Type)
		}
	}
	return ""
}

func simpleValueAttr(token replToken) (ddbjson.Attribute, error) {
	if token.Quoted {
		value := token.Text
		return ddbjson.Attribute{S: &value}, nil
	}
	raw := strings.TrimSpace(token.Text)
	switch strings.ToLower(raw) {
	case "null":
		value := true
		return ddbjson.Attribute{NULL: &value}, nil
	case "true", "false":
		value := strings.EqualFold(raw, "true")
		return ddbjson.Attribute{BOOL: &value}, nil
	}
	if simpleNumberRE.MatchString(raw) {
		return ddbjson.Attribute{N: &raw}, nil
	}
	if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
		attrs, err := simpleListAttrs(raw[1 : len(raw)-1])
		if err != nil {
			return ddbjson.Attribute{}, err
		}
		return ddbjson.Attribute{L: attrs}, nil
	}
	value := raw
	return ddbjson.Attribute{S: &value}, nil
}

func simpleListAttrs(raw string) ([]ddbjson.Attribute, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []ddbjson.Attribute{}, nil
	}
	parts := strings.Split(raw, ",")
	attrs := make([]ddbjson.Attribute, 0, len(parts))
	for _, part := range parts {
		attr, err := simpleValueAttr(replToken{Text: strings.TrimSpace(part)})
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, attr)
	}
	return attrs, nil
}

func parseSimpleAssignment(token replToken) (string, replToken, error) {
	name, value, ok := strings.Cut(token.Text, "=")
	if !ok || strings.TrimSpace(name) == "" {
		return "", replToken{}, fmt.Errorf("expected field=value, got %q", token.Text)
	}
	return strings.TrimSpace(name), replToken{Text: value, Quoted: token.Quoted}, nil
}

func parseSimpleWhere(tokens []replToken, start int) (string, replToken, int, error) {
	if strings.Contains(tokens[start].Text, "=") {
		name, value, err := parseSimpleAssignment(tokens[start])
		return name, value, start, err
	}
	if start+2 < len(tokens) && tokens[start+1].Text == "=" {
		return tokens[start].Text, tokens[start+2], start + 2, nil
	}
	return "", replToken{}, start, fmt.Errorf("scan where requires field=value")
}

func marshalDDBItem(item map[string]ddbjson.Attribute) (string, error) {
	raw, err := json.Marshal(item)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func marshalDDBAttr(attr ddbjson.Attribute) (string, error) {
	raw, err := json.Marshal(attr)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func marshalDDBBinds(binds map[string]ddbjson.Attribute) (string, error) {
	raw, err := json.Marshal(binds)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func commandHasFlags(args []string) bool {
	for _, arg := range args {
		if strings.HasPrefix(arg, "--") {
			return true
		}
	}
	return false
}

func looksLikeAdvancedCreate(args []string) bool {
	return len(args) > 1 && strings.HasPrefix(args[1], "--")
}

func isLegacyPut(args []string) bool {
	return len(args) == 3 && looksLikeDDBJSON(args[2])
}

func isLegacyKeyCommand(args []string) bool {
	return len(args) >= 3 && looksLikeDDBJSON(args[2])
}

func isLegacyQuery(args []string) bool {
	return len(args) >= 3 && looksLikeDDBJSON(args[2])
}

func isLegacyScan(args []string) bool {
	for _, arg := range args[2:] {
		switch strings.ToUpper(arg) {
		case "FILTER", "VALUES":
			return true
		}
	}
	return false
}

func looksLikeDDBJSON(s string) bool {
	s = strings.TrimSpace(s)
	return strings.HasPrefix(s, "{") || strings.HasPrefix(s, "file://")
}

func sortKeyUsage(schema types.TableDescriptor) string {
	if schema.KeySchema.SK == "" {
		return ""
	}
	return " <sk>"
}
