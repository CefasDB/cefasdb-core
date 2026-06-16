package cmd

import (
	"fmt"
	"strings"
)

type replToken struct {
	Text   string
	Quoted bool
}

func parseREPLArgs(line string) ([]string, error) {
	tokens, err := parseREPLTokenDetails(line)
	if err != nil {
		return nil, err
	}
	args := replTokenTexts(tokens)
	if len(args) == 0 {
		return nil, nil
	}
	return expandShortcut(args)
}

func parseREPLTokens(line string) ([]string, error) {
	tokens, err := parseREPLTokenDetails(line)
	if err != nil {
		return nil, err
	}
	return replTokenTexts(tokens), nil
}

func parseREPLTokenDetails(line string) ([]replToken, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return nil, nil
	}
	var tokens []replToken
	var b strings.Builder
	var quote rune
	quoted := false
	escaped := false
	inToken := false

	flush := func() {
		if !inToken {
			return
		}
		tokens = append(tokens, replToken{Text: b.String(), Quoted: quoted})
		b.Reset()
		quoted = false
		inToken = false
	}

	for _, r := range line {
		if escaped {
			switch r {
			case 'n':
				b.WriteRune('\n')
			case 't':
				b.WriteRune('\t')
			default:
				b.WriteRune(r)
			}
			inToken = true
			escaped = false
			continue
		}
		if r == '\\' && quote != '\'' {
			escaped = true
			inToken = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
				quoted = true
				inToken = true
				continue
			}
			b.WriteRune(r)
			inToken = true
			continue
		}
		switch r {
		case '\t', '\n', '\r', ' ':
			flush()
		case '\'', '"':
			quote = r
			quoted = true
			inToken = true
		default:
			b.WriteRune(r)
			inToken = true
		}
	}
	if escaped {
		b.WriteRune('\\')
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	flush()
	return tokens, nil
}

func replTokenTexts(tokens []replToken) []string {
	args := make([]string, 0, len(tokens))
	for _, token := range tokens {
		args = append(args, token.Text)
	}
	return args
}

func expandShortcut(args []string) ([]string, error) {
	switch strings.ToUpper(args[0]) {
	case "TABLES":
		if len(args) != 1 {
			return nil, fmt.Errorf("usage: TABLES")
		}
		return []string{"list-tables"}, nil
	case "DESC", "DESCRIBE":
		if len(args) != 2 {
			return nil, fmt.Errorf("usage: DESC <table>")
		}
		return []string{"describe-table", "--table-name", args[1]}, nil
	case "GET":
		return expandGetShortcut(args)
	case "PUT":
		return expandPutShortcut(args)
	case "DEL", "DELETE":
		return expandDeleteShortcut(args)
	case "QUERY":
		return expandQueryShortcut(args)
	case "SCAN":
		return expandScanShortcut(args)
	case "SQL":
		return expandSQLShortcut(args)
	default:
		return args, nil
	}
}

func expandGetShortcut(args []string) ([]string, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("usage: GET <table> <key-json> [CONSISTENT]")
	}
	out := []string{"get-item", "--table-name", args[1], "--key", args[2]}
	for _, arg := range args[3:] {
		switch strings.ToUpper(arg) {
		case "CONSISTENT":
			out = append(out, "--consistent-read")
		default:
			return nil, fmt.Errorf("GET: unknown option %q", arg)
		}
	}
	return out, nil
}

func expandPutShortcut(args []string) ([]string, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("usage: PUT <table> <item-json>")
	}
	return []string{"put-item", "--table-name", args[1], "--item", args[2]}, nil
}

func expandDeleteShortcut(args []string) ([]string, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("usage: DELETE <table> <key-json>")
	}
	return []string{"delete-item", "--table-name", args[1], "--key", args[2]}, nil
}

func expandQueryShortcut(args []string) ([]string, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("usage: QUERY <table> <pk-json> [SK <low-json> <high-json>] [LIMIT n] [INDEX name] [CONSISTENT]")
	}
	out := []string{"query", "--table-name", args[1], "--pk-value", args[2]}
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "SK":
			if i+2 >= len(args) {
				return nil, fmt.Errorf("QUERY SK requires <low-json> <high-json>")
			}
			out = append(out, "--sk-low", args[i+1], "--sk-high", args[i+2])
			i += 2
		case "LIMIT":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("QUERY LIMIT requires a value")
			}
			out = append(out, "--limit", args[i+1])
			i++
		case "INDEX":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("QUERY INDEX requires a value")
			}
			out = append(out, "--index-name", args[i+1])
			i++
		case "CONSISTENT":
			out = append(out, "--consistent-read")
		default:
			return nil, fmt.Errorf("QUERY: unknown option %q", args[i])
		}
	}
	return out, nil
}

func expandScanShortcut(args []string) ([]string, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("usage: SCAN <table> [FILTER expr] [VALUES ddb-json] [LIMIT n] [CONSISTENT]")
	}
	out := []string{"scan", "--table-name", args[1]}
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "FILTER":
			value, next := collectUntilKeyword(args, i+1, "VALUES", "LIMIT", "CONSISTENT")
			if value == "" {
				return nil, fmt.Errorf("SCAN FILTER requires an expression")
			}
			out = append(out, "--filter-expression", value)
			i = next - 1
		case "VALUES":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("SCAN VALUES requires a DDB-JSON bind map")
			}
			out = append(out, "--expression-attribute-values", args[i+1])
			i++
		case "LIMIT":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("SCAN LIMIT requires a value")
			}
			out = append(out, "--limit", args[i+1])
			i++
		case "CONSISTENT":
			out = append(out, "--consistent-read")
		default:
			return nil, fmt.Errorf("SCAN: unknown option %q", args[i])
		}
	}
	return out, nil
}

func expandSQLShortcut(args []string) ([]string, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("usage: SQL <statement> [PARAMS ddb-json-array]")
	}
	paramsAt := -1
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(args[i], "PARAMS") {
			paramsAt = i
			break
		}
	}
	if paramsAt == 1 {
		return nil, fmt.Errorf("SQL requires a statement before PARAMS")
	}
	if paramsAt == -1 {
		return []string{"execute-statement", "--statement", strings.Join(args[1:], " ")}, nil
	}
	if paramsAt+1 >= len(args) {
		return nil, fmt.Errorf("SQL PARAMS requires a JSON array")
	}
	if paramsAt+2 != len(args) {
		return nil, fmt.Errorf("SQL PARAMS accepts exactly one JSON array argument")
	}
	return []string{
		"execute-statement",
		"--statement", strings.Join(args[1:paramsAt], " "),
		"--parameters", args[paramsAt+1],
	}, nil
}

func collectUntilKeyword(args []string, start int, keywords ...string) (string, int) {
	var parts []string
	for i := start; i < len(args); i++ {
		for _, keyword := range keywords {
			if strings.EqualFold(args[i], keyword) {
				return strings.Join(parts, " "), i
			}
		}
		parts = append(parts, args[i])
	}
	return strings.Join(parts, " "), len(args)
}
