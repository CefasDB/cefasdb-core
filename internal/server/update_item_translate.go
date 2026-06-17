package server

import (
	"fmt"
	"strings"

	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// translateUpdateItem turns an aws-shaped UpdateItem request into a
// cefas SQL UPDATE statement so it can ride the existing executor
// (which already handles SET / ADD / REMOVE / DELETE + GSI + LSI + TTL
// maintenance). #names get textually substituted; :values get inlined
// as SQL literals — cefas SQL does not yet accept colon-binds inside
// UPDATE value positions.
//
// Returns the SQL string. The returning flag also tells the caller
// which image (NEW vs OLD) to read out of the executor's Result.Rows.
func translateUpdateItem(
	table string,
	key types.Item,
	ks types.KeySchema,
	updateExpr string,
	condExpr string,
	names map[string]string,
	values map[string]types.AttributeValue,
	returnKind string, // "" | "NONE" | "ALL_NEW" | "ALL_OLD" | "UPDATED_NEW" | "UPDATED_OLD"
) (sql string, wantImage string, err error) {
	if strings.TrimSpace(updateExpr) == "" {
		return "", "", fmt.Errorf("update_expression required")
	}
	ue, err := substituteNames(updateExpr, names)
	if err != nil {
		return "", "", fmt.Errorf("update_expression: %w", err)
	}
	ue, err = substituteValues(ue, values)
	if err != nil {
		return "", "", fmt.Errorf("update_expression: %w", err)
	}

	ce := strings.TrimSpace(condExpr)
	if ce != "" {
		ce, err = substituteNames(ce, names)
		if err != nil {
			return "", "", fmt.Errorf("condition_expression: %w", err)
		}
		ce, err = substituteValues(ce, values)
		if err != nil {
			return "", "", fmt.Errorf("condition_expression: %w", err)
		}
	}

	// WHERE pk = <lit> [AND sk = <lit>]
	if ks.PK == "" {
		return "", "", fmt.Errorf("table has no partition key")
	}
	pkVal, ok := key[ks.PK]
	if !ok {
		return "", "", fmt.Errorf("key missing partition attribute %q", ks.PK)
	}
	pkLit, err := cefassql.LiteralFromAttr(pkVal)
	if err != nil {
		return "", "", fmt.Errorf("partition key literal: %w", err)
	}
	where := fmt.Sprintf("%s = %s", ks.PK, pkLit)
	if ks.SK != "" {
		skVal, ok := key[ks.SK]
		if !ok {
			return "", "", fmt.Errorf("key missing sort attribute %q", ks.SK)
		}
		skLit, err := cefassql.LiteralFromAttr(skVal)
		if err != nil {
			return "", "", fmt.Errorf("sort key literal: %w", err)
		}
		where += fmt.Sprintf(" AND %s = %s", ks.SK, skLit)
	}

	out := fmt.Sprintf("UPDATE %s %s WHERE %s", table, ue, where)
	if ce != "" {
		out += " IF " + ce
	}

	switch strings.ToUpper(returnKind) {
	case "", "NONE":
		// no RETURNING
	case "ALL_NEW", "UPDATED_NEW":
		out += " RETURNING NEW"
		wantImage = "NEW"
	case "ALL_OLD", "UPDATED_OLD":
		out += " RETURNING OLD"
		wantImage = "OLD"
	default:
		return "", "", fmt.Errorf("unsupported return_values %q", returnKind)
	}
	return out, wantImage, nil
}

// substituteNames replaces every `#ident` token in `src` with the
// matching entry from `names`. Placeholders inside single-quoted
// strings are left untouched.
func substituteNames(src string, names map[string]string) (string, error) {
	var b strings.Builder
	b.Grow(len(src))
	r := []rune(src)
	for i := 0; i < len(r); {
		c := r[i]
		if c == '\'' {
			j := scanQuoted(r, i)
			b.WriteString(string(r[i:j]))
			i = j
			continue
		}
		if c == '#' {
			j := i + 1
			for j < len(r) && isIdentRune(r[j]) {
				j++
			}
			if j == i+1 {
				return "", fmt.Errorf("# at offset %d missing name", i)
			}
			ph := string(r[i+1 : j])
			real, ok := names[ph]
			if !ok {
				return "", fmt.Errorf("ExpressionAttributeNames missing %q (referenced as #%s)", ph, ph)
			}
			b.WriteString(real)
			i = j
			continue
		}
		b.WriteRune(c)
		i++
	}
	return b.String(), nil
}

// substituteValues replaces every `:ident` token in `src` with the SQL
// literal form of the matching entry from `values`. Placeholders
// inside single-quoted strings are left untouched.
func substituteValues(src string, values map[string]types.AttributeValue) (string, error) {
	var b strings.Builder
	b.Grow(len(src))
	r := []rune(src)
	for i := 0; i < len(r); {
		c := r[i]
		if c == '\'' {
			j := scanQuoted(r, i)
			b.WriteString(string(r[i:j]))
			i = j
			continue
		}
		if c == ':' {
			j := i + 1
			for j < len(r) && isIdentRune(r[j]) {
				j++
			}
			if j == i+1 {
				return "", fmt.Errorf(": at offset %d missing name", i)
			}
			ph := string(r[i+1 : j])
			av, ok := values[ph]
			if !ok {
				// Tolerate the colon-prefixed form too — aws-cli sends
				// `:name` as the map key.
				if v, ok2 := values[":"+ph]; ok2 {
					av = v
					ok = true
				}
			}
			if !ok {
				return "", fmt.Errorf("ExpressionAttributeValues missing %q (referenced as :%s)", ph, ph)
			}
			lit, err := cefassql.LiteralFromAttr(av)
			if err != nil {
				return "", fmt.Errorf("value :%s: %w", ph, err)
			}
			b.WriteString(lit)
			i = j
			continue
		}
		b.WriteRune(c)
		i++
	}
	return b.String(), nil
}

// scanQuoted returns the index past the closing quote of a single-
// quoted SQL string that starts at r[i]. Handles `”` escapes.
func scanQuoted(r []rune, i int) int {
	j := i + 1
	for j < len(r) {
		if r[j] == '\'' {
			if j+1 < len(r) && r[j+1] == '\'' {
				j += 2
				continue
			}
			return j + 1
		}
		j++
	}
	return j
}

func isIdentRune(r rune) bool {
	return r == '_' ||
		(r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9')
}
