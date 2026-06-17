package sql

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// PartiQLParameter is the wire shape of a parameter on the AWS
// ExecuteStatement request. We accept the same letter-tagged
// AttributeValue every other endpoint uses; only the fields that
// matter for SQL substitution are populated.
type PartiQLParameter struct {
	S    *string `json:"S,omitempty"`
	N    *string `json:"N,omitempty"`
	B    *string `json:"B,omitempty"` // base64
	BOOL *bool   `json:"BOOL,omitempty"`
	NULL *bool   `json:"NULL,omitempty"`
}

// BindPartiQL substitutes `?` placeholders in `statement` with the
// supplied parameter values, producing a plain cefas SQL string. This
// keeps the PartiQL endpoint thin: clients write the AWS PartiQL
// grammar that our parser already supports (modulo features), and the
// server applies the standard cefas pipeline downstream.
//
// String / binary values become single-quoted literals (with proper
// `”` escaping). Numbers stay unquoted. Booleans become TRUE/FALSE.
// NULL becomes the SQL keyword NULL.
func BindPartiQL(statement string, params []PartiQLParameter) (string, error) {
	if !strings.Contains(statement, "?") {
		if len(params) > 0 {
			return "", fmt.Errorf("statement has no ? placeholders but %d parameters were provided", len(params))
		}
		return statement, nil
	}
	var b strings.Builder
	b.Grow(len(statement) + 16*len(params))
	idx := 0
	for i := 0; i < len(statement); i++ {
		c := statement[i]
		// Don't substitute ? inside single-quoted strings. Cheap
		// state machine — toggles on every unescaped quote.
		if c == '\'' {
			b.WriteByte(c)
			i++
			for ; i < len(statement); i++ {
				b.WriteByte(statement[i])
				if statement[i] == '\'' {
					if i+1 < len(statement) && statement[i+1] == '\'' {
						b.WriteByte('\'')
						i++
						continue
					}
					break
				}
			}
			continue
		}
		if c == '?' {
			if idx >= len(params) {
				return "", fmt.Errorf("not enough parameters: statement uses %d, only %d provided", idx+1, len(params))
			}
			lit, err := paramLiteral(params[idx])
			if err != nil {
				return "", fmt.Errorf("parameter %d: %w", idx, err)
			}
			b.WriteString(lit)
			idx++
			continue
		}
		b.WriteByte(c)
	}
	if idx != len(params) {
		return "", fmt.Errorf("statement uses %d placeholders, %d parameters provided", idx, len(params))
	}
	return b.String(), nil
}

// LiteralFromAttr renders a storage-layer AttributeValue as a cefas
// SQL literal token. Used by callers that splice DDB-typed values
// directly into a generated SQL statement (UpdateItem RPC, batch
// translation, etc.). Returns an error for set / list / map / binary
// values — cefas SQL doesn't have literal syntax for those.
func LiteralFromAttr(av types.AttributeValue) (string, error) {
	switch av.T {
	case types.AttrS:
		return "'" + strings.ReplaceAll(av.S, "'", "''") + "'", nil
	case types.AttrN:
		return av.N, nil
	case types.AttrBOOL:
		if av.BOOL {
			return "TRUE", nil
		}
		return "FALSE", nil
	case types.AttrNull:
		return "NULL", nil
	case types.AttrVec:
		parts := make([]string, len(av.Vec))
		for i, v := range av.Vec {
			parts[i] = strconv.FormatFloat(v, 'g', -1, 64)
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	}
	return "", fmt.Errorf("cefas SQL has no literal form for attribute type %v", av.T)
}

func paramLiteral(p PartiQLParameter) (string, error) {
	switch {
	case p.S != nil:
		return "'" + strings.ReplaceAll(*p.S, "'", "''") + "'", nil
	case p.N != nil:
		return *p.N, nil
	case p.B != nil:
		raw, err := base64.StdEncoding.DecodeString(*p.B)
		if err != nil {
			return "", fmt.Errorf("base64: %w", err)
		}
		return "'" + strings.ReplaceAll(string(raw), "'", "''") + "'", nil
	case p.BOOL != nil:
		if *p.BOOL {
			return "TRUE", nil
		}
		return "FALSE", nil
	case p.NULL != nil && *p.NULL:
		return "NULL", nil
	}
	return "", fmt.Errorf("parameter has no field set")
}
