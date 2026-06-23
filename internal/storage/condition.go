package storage

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// ErrConditionFailed is returned by PutItem / DeleteItem when the
// caller supplied a ConditionExpression and the expression evaluated
// to false against the prior item state. This is the optimistic-
// concurrency signal callers compare via errors.Is.
var ErrConditionFailed = errors.New("cefas/storage: condition failed")

// ErrInvalidCounterMutation is returned when a regular write path
// attempts to set, overwrite, or remove a schema-level counter column.
var ErrInvalidCounterMutation = errors.New("cefas/storage: counter columns must be mutated via AtomicUpdate")

// Condition is a parsed expression ready for evaluation. Empty
// expressions are valid and always succeed.
//
// Supported grammar (DynamoDB ConditionExpression subset):
//
//	expr        = orExpr
//	orExpr      = andExpr ("OR" andExpr)*
//	andExpr     = notExpr ("AND" notExpr)*
//	notExpr     = "NOT" notExpr | primary
//	primary     = funcCall | "(" expr ")" | comparison
//	funcCall    = ("attribute_exists" | "attribute_not_exists") "(" ident ")"
//	comparison  = operand op operand
//	            | operand "BETWEEN" operand "AND" operand
//	op          = "=" | "<>" | "<" | "<=" | ">" | ">="
//	operand     = ident | ":name"        // values come from a bind map
//
// Identifiers refer to top-level attribute names; bind variables map
// to an AttributeValue supplied alongside the expression.
type Condition struct {
	root node
}

// EmptyCondition is the no-op condition that always evaluates to true.
var EmptyCondition = Condition{}

// IsZero reports whether c carries no expression. Internal callers use
// it to skip parsing the prior item.
func (c Condition) IsZero() bool { return c.root == nil }

// ParseCondition compiles src into a Condition. Returns an empty
// Condition for empty / whitespace-only input so callers can pass
// optional expressions straight through.
func ParseCondition(src string) (Condition, error) {
	if strings.TrimSpace(src) == "" {
		return EmptyCondition, nil
	}
	toks, err := tokenize(src)
	if err != nil {
		return Condition{}, fmt.Errorf("tokenize: %w", err)
	}
	p := &parser{tokens: toks}
	root, err := p.parseExpr()
	if err != nil {
		return Condition{}, err
	}
	if p.pos < len(p.tokens) {
		return Condition{}, fmt.Errorf("unexpected token %q at position %d", p.tokens[p.pos].lit, p.pos)
	}
	return Condition{root: root}, nil
}

// Evaluate returns whether the condition holds for `item`. `item` may
// be nil to mean "no prior version" — `attribute_exists(x)` is then
// false for every x. Bind variables are resolved from `binds` (keys
// without the leading ':').
func (c Condition) Evaluate(item types.Item, binds map[string]types.AttributeValue) (bool, error) {
	if c.root == nil {
		return true, nil
	}
	return c.root.eval(item, binds)
}

// ---------- token / lexer ----------

type tokenKind uint8

const (
	tIdent tokenKind = iota + 1
	tBind
	tLParen
	tRParen
	tComma
	tEq
	tNeq
	tLt
	tLte
	tGt
	tGte
	tAnd
	tOr
	tNot
	tBetween
	tAttrExists
	tAttrNotExists
)

type token struct {
	kind tokenKind
	lit  string
}

func tokenize(src string) ([]token, error) {
	var out []token
	r := []rune(src)
	for i := 0; i < len(r); {
		c := r[i]
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '(':
			out = append(out, token{kind: tLParen, lit: "("})
			i++
		case c == ')':
			out = append(out, token{kind: tRParen, lit: ")"})
			i++
		case c == ',':
			out = append(out, token{kind: tComma, lit: ","})
			i++
		case c == '=':
			out = append(out, token{kind: tEq, lit: "="})
			i++
		case c == '<':
			if i+1 < len(r) && r[i+1] == '=' {
				out = append(out, token{kind: tLte, lit: "<="})
				i += 2
			} else if i+1 < len(r) && r[i+1] == '>' {
				out = append(out, token{kind: tNeq, lit: "<>"})
				i += 2
			} else {
				out = append(out, token{kind: tLt, lit: "<"})
				i++
			}
		case c == '>':
			if i+1 < len(r) && r[i+1] == '=' {
				out = append(out, token{kind: tGte, lit: ">="})
				i += 2
			} else {
				out = append(out, token{kind: tGt, lit: ">"})
				i++
			}
		case c == ':':
			j := i + 1
			for j < len(r) && (unicode.IsLetter(r[j]) || unicode.IsDigit(r[j]) || r[j] == '_') {
				j++
			}
			if j == i+1 {
				return nil, fmt.Errorf("bind variable at %d missing name", i)
			}
			out = append(out, token{kind: tBind, lit: string(r[i+1 : j])})
			i = j
		case unicode.IsLetter(c) || c == '_':
			j := i
			for j < len(r) && (unicode.IsLetter(r[j]) || unicode.IsDigit(r[j]) || r[j] == '_') {
				j++
			}
			word := string(r[i:j])
			switch strings.ToUpper(word) {
			case "AND":
				out = append(out, token{kind: tAnd, lit: word})
			case "OR":
				out = append(out, token{kind: tOr, lit: word})
			case "NOT":
				out = append(out, token{kind: tNot, lit: word})
			case "BETWEEN":
				out = append(out, token{kind: tBetween, lit: word})
			case "ATTRIBUTE_EXISTS":
				out = append(out, token{kind: tAttrExists, lit: word})
			case "ATTRIBUTE_NOT_EXISTS":
				out = append(out, token{kind: tAttrNotExists, lit: word})
			default:
				out = append(out, token{kind: tIdent, lit: word})
			}
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q at position %d", string(c), i)
		}
	}
	return out, nil
}

// ---------- parser ----------

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) peek() (token, bool) {
	if p.pos >= len(p.tokens) {
		return token{}, false
	}
	return p.tokens[p.pos], true
}

func (p *parser) consume() (token, bool) {
	t, ok := p.peek()
	if ok {
		p.pos++
	}
	return t, ok
}

func (p *parser) expect(k tokenKind, want string) (token, error) {
	t, ok := p.peek()
	if !ok || t.kind != k {
		return token{}, fmt.Errorf("expected %s at position %d", want, p.pos)
	}
	p.pos++
	return t, nil
}

func (p *parser) parseExpr() (node, error) { return p.parseOr() }

func (p *parser) parseOr() (node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		t, ok := p.peek()
		if !ok || t.kind != tOr {
			return left, nil
		}
		p.pos++
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = boolNode{op: "OR", l: left, r: right}
	}
}

func (p *parser) parseAnd() (node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for {
		t, ok := p.peek()
		if !ok || t.kind != tAnd {
			return left, nil
		}
		p.pos++
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = boolNode{op: "AND", l: left, r: right}
	}
}

func (p *parser) parseNot() (node, error) {
	if t, ok := p.peek(); ok && t.kind == tNot {
		p.pos++
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notNode{inner: inner}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (node, error) {
	t, ok := p.peek()
	if !ok {
		return nil, fmt.Errorf("unexpected end of expression")
	}
	switch t.kind {
	case tLParen:
		p.pos++
		inner, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
		return inner, nil
	case tAttrExists, tAttrNotExists:
		p.pos++
		negate := t.kind == tAttrNotExists
		if _, err := p.expect(tLParen, "("); err != nil {
			return nil, err
		}
		idt, err := p.expect(tIdent, "attribute name")
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tRParen, ")"); err != nil {
			return nil, err
		}
		return existsNode{attr: idt.lit, negate: negate}, nil
	}
	// comparison: operand op operand  |  operand BETWEEN operand AND operand
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	op, ok := p.consume()
	if !ok {
		return nil, fmt.Errorf("expected operator after operand")
	}
	switch op.kind {
	case tBetween:
		lo, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tAnd, "AND"); err != nil {
			return nil, err
		}
		hi, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return betweenNode{value: left, lo: lo, hi: hi}, nil
	case tEq, tNeq, tLt, tLte, tGt, tGte:
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		return cmpNode{op: op.lit, l: left, r: right}, nil
	}
	return nil, fmt.Errorf("unsupported operator %q", op.lit)
}

func (p *parser) parseOperand() (operand, error) {
	t, ok := p.consume()
	if !ok {
		return operand{}, fmt.Errorf("expected operand")
	}
	switch t.kind {
	case tIdent:
		return operand{kind: opAttr, name: t.lit}, nil
	case tBind:
		return operand{kind: opBind, name: t.lit}, nil
	}
	return operand{}, fmt.Errorf("expected identifier or bind variable, got %q", t.lit)
}

// ---------- AST ----------

type node interface {
	eval(item types.Item, binds map[string]types.AttributeValue) (bool, error)
}

type operandKind uint8

const (
	opAttr operandKind = iota + 1
	opBind
)

type operand struct {
	kind operandKind
	name string
}

func (o operand) resolve(item types.Item, binds map[string]types.AttributeValue) (types.AttributeValue, bool, error) {
	switch o.kind {
	case opAttr:
		av, ok := item[o.name]
		return av, ok, nil
	case opBind:
		av, ok := binds[o.name]
		if !ok {
			return types.AttributeValue{}, false, fmt.Errorf("bind variable :%s not provided", o.name)
		}
		return av, true, nil
	}
	return types.AttributeValue{}, false, fmt.Errorf("operand has unknown kind")
}

type existsNode struct {
	attr   string
	negate bool
}

func (n existsNode) eval(item types.Item, _ map[string]types.AttributeValue) (bool, error) {
	_, ok := item[n.attr]
	if n.negate {
		return !ok, nil
	}
	return ok, nil
}

type notNode struct{ inner node }

func (n notNode) eval(item types.Item, b map[string]types.AttributeValue) (bool, error) {
	v, err := n.inner.eval(item, b)
	return !v, err
}

type boolNode struct {
	op   string
	l, r node
}

func (n boolNode) eval(item types.Item, b map[string]types.AttributeValue) (bool, error) {
	lv, err := n.l.eval(item, b)
	if err != nil {
		return false, err
	}
	if n.op == "AND" && !lv {
		return false, nil
	}
	if n.op == "OR" && lv {
		return true, nil
	}
	return n.r.eval(item, b)
}

type cmpNode struct {
	op   string
	l, r operand
}

func (n cmpNode) eval(item types.Item, b map[string]types.AttributeValue) (bool, error) {
	lv, lok, err := n.l.resolve(item, b)
	if err != nil {
		return false, err
	}
	rv, rok, err := n.r.resolve(item, b)
	if err != nil {
		return false, err
	}
	// Attribute absence makes the comparison fail (matches DynamoDB
	// semantics: missing attributes evaluate to false in scalar
	// comparisons). The bind side always resolves (or errors).
	if !lok || !rok {
		return false, nil
	}
	c, err := compareAV(lv, rv)
	if err != nil {
		return false, err
	}
	switch n.op {
	case "=":
		return c == 0, nil
	case "<>":
		return c != 0, nil
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	case ">":
		return c > 0, nil
	case ">=":
		return c >= 0, nil
	}
	return false, fmt.Errorf("unknown comparison operator %q", n.op)
}

type betweenNode struct {
	value, lo, hi operand
}

func (n betweenNode) eval(item types.Item, b map[string]types.AttributeValue) (bool, error) {
	v, vok, err := n.value.resolve(item, b)
	if err != nil {
		return false, err
	}
	if !vok {
		return false, nil
	}
	lo, _, err := n.lo.resolve(item, b)
	if err != nil {
		return false, err
	}
	hi, _, err := n.hi.resolve(item, b)
	if err != nil {
		return false, err
	}
	cl, err := compareAV(v, lo)
	if err != nil {
		return false, err
	}
	ch, err := compareAV(v, hi)
	if err != nil {
		return false, err
	}
	return cl >= 0 && ch <= 0, nil
}

// compareAV returns -1 / 0 / +1 mirroring bytes.Compare semantics. The
// two values must share a comparable type (S/N/B). Mixing types is
// an error because comparing "abc" < 5 has no defined answer in this
// data model. For numbers we compare lexicographically on the
// canonical decimal text — callers feeding numbers MUST normalize
// upstream (e.g. always send "5" not "05").
func compareAV(a, b types.AttributeValue) (int, error) {
	if a.T != b.T {
		return 0, fmt.Errorf("cannot compare attribute types %d and %d", a.T, b.T)
	}
	switch a.T {
	case types.AttrS:
		return strings.Compare(a.S, b.S), nil
	case types.AttrN:
		return strings.Compare(a.N, b.N), nil
	case types.AttrB:
		return bytesCompare(a.B, b.B), nil
	case types.AttrBOOL:
		ai := boolToInt(a.BOOL)
		bi := boolToInt(b.BOOL)
		switch {
		case ai < bi:
			return -1, nil
		case ai > bi:
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("comparison not supported on attribute type %d", a.T)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func bytesCompare(a, b []byte) int {
	switch {
	case len(a) < len(b) && commonPrefixCmp(a, b) == 0:
		return -1
	case len(a) > len(b) && commonPrefixCmp(a, b) == 0:
		return 1
	}
	return commonPrefixCmp(a, b)
}

func commonPrefixCmp(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}
