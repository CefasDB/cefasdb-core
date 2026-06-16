// Package sql implements a small, hand-written SQL surface over cefas.
//
// Supported statements:
//
//	SELECT * | col,... | COUNT(*)  FROM <table> [USE INDEX (<idx>)]
//	  [ALLOW SCAN] [WHERE <pred>] [GROUP BY col,...]
//	  [ORDER BY <sk> ASC|DESC] [LIMIT <n>]
//
//	INSERT INTO <table> (col,...) VALUES (v,...)
//
//	UPDATE <table> SET col = v, ... WHERE <pred>
//
//	DELETE FROM <table> WHERE <pred>
//
//	CREATE TABLE <name> ( PRIMARY KEY ( <pk> [, <sk>] ) )
//
//	DROP TABLE <name>
//
// Predicates support equality and range comparisons, AND, OR, NOT,
// BETWEEN, and the spatial functions ST_Within(loc, BBox(...)) and
// ST_DWithin(loc, Point(...), meters). The planner uses the predicate
// shape to pick a primary, GSI, or spatial index path.
package sql

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenKind identifies the lexical class of a Token.
type TokenKind uint8

const (
	tEOF TokenKind = iota

	// Punctuation.
	tLParen
	tRParen
	tLBracket
	tRBracket
	tComma
	tDot
	tStar
	tSemicolon

	// Operators.
	tEq
	tNeq
	tLt
	tLte
	tGt
	tGte
	tPlus
	tMinus

	// Literals.
	tIdent
	tNumber // unquoted; lexer keeps original text
	tString // single-quoted; the value is the unescaped contents

	// Keywords.
	tSelect
	tFrom
	tWhere
	tInsert
	tInto
	tValues
	tUpdate
	tSet
	tDelete
	tCreate
	tTable
	tPrimary
	tKey
	tDrop
	tOrder
	tGroup
	tBy
	tInner
	tJoin
	tOn
	tAs
	tAsc
	tDesc
	tLimit
	tAnd
	tOr
	tNot
	tBetween
	tIs
	tNull
	tTrue
	tFalse
	tUse
	tIndex
	tIf
	tExists
	tAdd
	tRemove
	tCount
	tReturning
	tNew
	tOld
	tAnn
	tOf
	tDiversify
	tTo
	tWith
	tStorage
	tAllow
	tScan
)

// Token is a single lexer output. Lit carries the original source
// text (useful for error messages and for re-emitting identifiers
// with their case preserved).
type Token struct {
	Kind TokenKind
	Lit  string
	Pos  int // byte offset into the source
}

var keywords = map[string]TokenKind{
	"SELECT":    tSelect,
	"FROM":      tFrom,
	"WHERE":     tWhere,
	"INSERT":    tInsert,
	"INTO":      tInto,
	"VALUES":    tValues,
	"UPDATE":    tUpdate,
	"SET":       tSet,
	"DELETE":    tDelete,
	"CREATE":    tCreate,
	"TABLE":     tTable,
	"PRIMARY":   tPrimary,
	"KEY":       tKey,
	"DROP":      tDrop,
	"ORDER":     tOrder,
	"GROUP":     tGroup,
	"BY":        tBy,
	"INNER":     tInner,
	"JOIN":      tJoin,
	"ON":        tOn,
	"AS":        tAs,
	"ASC":       tAsc,
	"DESC":      tDesc,
	"LIMIT":     tLimit,
	"AND":       tAnd,
	"OR":        tOr,
	"NOT":       tNot,
	"BETWEEN":   tBetween,
	"IS":        tIs,
	"NULL":      tNull,
	"TRUE":      tTrue,
	"FALSE":     tFalse,
	"USE":       tUse,
	"INDEX":     tIndex,
	"IF":        tIf,
	"EXISTS":    tExists,
	"ADD":       tAdd,
	"REMOVE":    tRemove,
	"COUNT":     tCount,
	"RETURNING": tReturning,
	"NEW":       tNew,
	"OLD":       tOld,
	"ANN":       tAnn,
	"OF":        tOf,
	"DIVERSIFY": tDiversify,
	"TO":        tTo,
	"WITH":      tWith,
	"STORAGE":   tStorage,
	"ALLOW":     tAllow,
	"SCAN":      tScan,
}

// Tokenize turns src into a slice of Tokens. Comments (-- to end of
// line) and whitespace are skipped silently. Identifiers and keywords
// are returned with their original case but lookups against the
// keyword table are case-insensitive (SQL convention).
func Tokenize(src string) ([]Token, error) {
	var out []Token
	r := []rune(src)
	for i := 0; i < len(r); {
		c := r[i]
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '-' && i+1 < len(r) && r[i+1] == '-':
			for i < len(r) && r[i] != '\n' {
				i++
			}
		case c == '(':
			out = append(out, Token{Kind: tLParen, Lit: "(", Pos: i})
			i++
		case c == ')':
			out = append(out, Token{Kind: tRParen, Lit: ")", Pos: i})
			i++
		case c == '[':
			out = append(out, Token{Kind: tLBracket, Lit: "[", Pos: i})
			i++
		case c == ']':
			out = append(out, Token{Kind: tRBracket, Lit: "]", Pos: i})
			i++
		case c == ',':
			out = append(out, Token{Kind: tComma, Lit: ",", Pos: i})
			i++
		case c == ';':
			out = append(out, Token{Kind: tSemicolon, Lit: ";", Pos: i})
			i++
		case c == '*':
			out = append(out, Token{Kind: tStar, Lit: "*", Pos: i})
			i++
		case c == '+':
			out = append(out, Token{Kind: tPlus, Lit: "+", Pos: i})
			i++
		case c == '=':
			out = append(out, Token{Kind: tEq, Lit: "=", Pos: i})
			i++
		case c == '!':
			if i+1 < len(r) && r[i+1] == '=' {
				out = append(out, Token{Kind: tNeq, Lit: "!=", Pos: i})
				i += 2
				continue
			}
			return nil, fmt.Errorf("unexpected '!' at %d", i)
		case c == '<':
			switch {
			case i+1 < len(r) && r[i+1] == '=':
				out = append(out, Token{Kind: tLte, Lit: "<=", Pos: i})
				i += 2
			case i+1 < len(r) && r[i+1] == '>':
				out = append(out, Token{Kind: tNeq, Lit: "<>", Pos: i})
				i += 2
			default:
				out = append(out, Token{Kind: tLt, Lit: "<", Pos: i})
				i++
			}
		case c == '>':
			if i+1 < len(r) && r[i+1] == '=' {
				out = append(out, Token{Kind: tGte, Lit: ">=", Pos: i})
				i += 2
			} else {
				out = append(out, Token{Kind: tGt, Lit: ">", Pos: i})
				i++
			}
		case c == '\'':
			// Single-quoted string. '' inside the literal stands for a
			// single quote (standard SQL).
			var b strings.Builder
			start := i
			i++
			for i < len(r) {
				if r[i] == '\'' {
					if i+1 < len(r) && r[i+1] == '\'' {
						b.WriteRune('\'')
						i += 2
						continue
					}
					i++
					out = append(out, Token{Kind: tString, Lit: b.String(), Pos: start})
					goto next
				}
				b.WriteRune(r[i])
				i++
			}
			return nil, fmt.Errorf("unterminated string at %d", start)
		case c == '"':
			// Double-quoted identifier (column/table with reserved
			// word or special characters). Same escape rules.
			var b strings.Builder
			start := i
			i++
			for i < len(r) {
				if r[i] == '"' {
					if i+1 < len(r) && r[i+1] == '"' {
						b.WriteRune('"')
						i += 2
						continue
					}
					i++
					out = append(out, Token{Kind: tIdent, Lit: b.String(), Pos: start})
					goto next
				}
				b.WriteRune(r[i])
				i++
			}
			return nil, fmt.Errorf("unterminated quoted identifier at %d", start)
		case c == '-' && (i+1 >= len(r) || !unicode.IsDigit(r[i+1]) || !precedingAcceptsUnary(out)):
			out = append(out, Token{Kind: tMinus, Lit: "-", Pos: i})
			i++
		case unicode.IsDigit(c) || (c == '.' && i+1 < len(r) && unicode.IsDigit(r[i+1])) ||
			(c == '-' && i+1 < len(r) && unicode.IsDigit(r[i+1]) && precedingAcceptsUnary(out)):
			start := i
			if c == '-' {
				i++
			}
			for i < len(r) && (unicode.IsDigit(r[i]) || r[i] == '.') {
				i++
			}
			// Exponent.
			if i < len(r) && (r[i] == 'e' || r[i] == 'E') {
				i++
				if i < len(r) && (r[i] == '+' || r[i] == '-') {
					i++
				}
				for i < len(r) && unicode.IsDigit(r[i]) {
					i++
				}
			}
			out = append(out, Token{Kind: tNumber, Lit: string(r[start:i]), Pos: start})
		case c == '.':
			out = append(out, Token{Kind: tDot, Lit: ".", Pos: i})
			i++
		case unicode.IsLetter(c) || c == '_':
			start := i
			for i < len(r) && (unicode.IsLetter(r[i]) || unicode.IsDigit(r[i]) || r[i] == '_') {
				i++
			}
			word := string(r[start:i])
			if kw, ok := keywords[strings.ToUpper(word)]; ok {
				out = append(out, Token{Kind: kw, Lit: word, Pos: start})
			} else {
				out = append(out, Token{Kind: tIdent, Lit: word, Pos: start})
			}
		default:
			return nil, fmt.Errorf("unexpected character %q at %d", string(c), i)
		}
	next:
	}
	out = append(out, Token{Kind: tEOF, Lit: "", Pos: len(r)})
	return out, nil
}

// precedingAcceptsUnary reports whether the previous token suggests
// the current '-' is a unary minus on a numeric literal (i.e. we are
// parsing the start of an expression). When the previous token is a
// value-producer (number, ident, string, ')', '*'), '-' is the binary
// subtraction operator — but the lexer doesn't tokenize subtraction
// today, so we treat that case as an error rather than swallow it.
func precedingAcceptsUnary(toks []Token) bool {
	if len(toks) == 0 {
		return true
	}
	switch toks[len(toks)-1].Kind {
	case tNumber, tIdent, tString, tRParen, tStar:
		return false
	}
	return true
}
