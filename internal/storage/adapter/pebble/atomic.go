// Server-side atomic read-modify-write (issue #242).
//
// AtomicUpdate composes the existing condition-expression evaluator
// with a small whitelisted mutator so callers can perform "increment
// a counter and return the new value", "clamp a posterior parameter",
// and similar operations in one RPC. The whole flow lives inside a
// single pebble.Batch + per-key mutex so the post-image returned to
// the caller is the one the next reader will observe.
//
// Whitelisted expression grammar (APPLY action):
//
//	expr     = term (("+" | "-") term)*
//	term     = factor (("*" | "/") factor)*
//	factor   = number | ident | funcCall | "(" expr ")" | "-" factor
//	funcCall = ("min" | "max" | "clamp") "(" args ")"
//	args     = expr ("," expr)*
//	ident    = top-level item attribute name (resolves to its number)
//	number   = signed decimal literal
//
// Identifiers refer to the prior item state. The set of functions is
// intentionally small — see the issue for the canonical bandit example:
//
//	alpha = alpha + reward                  // ADD_RETURN with delta=reward
//	beta  = clamp(beta + 1 - reward, 0, 1)  // APPLY beta = clamp(beta + 1 - reward, 0, 1)
package pebble

import (
	"errors"
	"fmt"
	"hash/maphash"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/CefasDb/cefasdb/internal/storage"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// AtomicActionKind / AtomicAction / AtomicOptions / AtomicResult and
// the AtomicAction kind constants are aliased here so existing
// `pebble.X` references keep compiling. Canonical declarations live
// in internal/storage.
type (
	AtomicActionKind = storage.AtomicActionKind
	AtomicAction     = storage.AtomicAction
	AtomicOptions    = storage.AtomicOptions
	AtomicResult     = storage.AtomicResult
)

const (
	AtomicActionSet        = storage.AtomicActionSet
	AtomicActionIncrReturn = storage.AtomicActionIncrReturn
	AtomicActionAddReturn  = storage.AtomicActionAddReturn
	AtomicActionApply      = storage.AtomicActionApply
)

// ErrAtomicUnsupported is returned for action kinds / expression
// forms the engine does not allow. The whitelist is intentionally
// tight; bigger mutators should compose multiple actions instead.
var ErrAtomicUnsupported = errors.New("cefas/storage: atomic action not supported")

// atomicMutexShards is the fanout of the per-key mutex pool. 64 buckets
// is plenty for counter workloads: contention only matters when two
// writers land on the same shard *and* the same bucket, which collapses
// to a Mutex.Lock anyway.
const atomicMutexShards = 64

var (
	atomicMu   [atomicMutexShards]sync.Mutex
	atomicSeed = maphash.MakeSeed()
)

func atomicLock(key []byte) *sync.Mutex {
	var h maphash.Hash
	h.SetSeed(atomicSeed)
	_, _ = h.Write(key)
	return &atomicMu[h.Sum64()%atomicMutexShards]
}

// AtomicUpdate performs a single read-modify-write against the row
// identified by keyAttrs. The precondition (if any), the action set,
// and the index maintenance all land in one pebble.Batch so the
// returned post-image is the one the next reader sees.
//
// Concurrency: writers contending on the same primary key serialize
// on a per-key mutex; writers on different keys run in parallel.
// Multi-shard deployments serialize naturally via the Raft FSM.
func (d *DB) AtomicUpdate(td types.TableDescriptor, keyAttrs types.Item, opts AtomicOptions) (AtomicResult, error) {
	if err := d.checkWritePressure(); err != nil {
		return AtomicResult{}, err
	}
	if len(opts.Actions) == 0 {
		return AtomicResult{}, fmt.Errorf("atomic: at least one action required")
	}
	pk, sk, err := extractKeyBytes(keyAttrs, td.KeySchema)
	if err != nil {
		return AtomicResult{}, err
	}
	primaryKey := storage.KeyPrimary(td.Name, pk, sk)

	cond, err := storage.ParseCondition(opts.Condition)
	if err != nil {
		return AtomicResult{}, fmt.Errorf("condition: %w", err)
	}

	mu := atomicLock(primaryKey)
	mu.Lock()
	defer mu.Unlock()

	prior, err := d.snapshotGet(primaryKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return AtomicResult{}, fmt.Errorf("read prior: %w", err)
	}
	var priorItem types.Item
	created := prior == nil
	if prior != nil {
		priorItem, err = storage.DecodeItem(prior)
		if err != nil {
			return AtomicResult{}, fmt.Errorf("decode prior: %w", err)
		}
	}

	if !cond.IsZero() {
		ok, err := cond.Evaluate(priorItem, opts.Binds)
		if err != nil {
			return AtomicResult{}, fmt.Errorf("evaluate condition: %w", err)
		}
		if !ok {
			return AtomicResult{}, storage.ErrConditionFailed
		}
	}

	// Compose the new item by cloning prior and applying each action.
	newItem := cloneItem(priorItem)
	if newItem == nil {
		newItem = types.Item{}
	}
	// Always preserve key attributes (covers create-on-write).
	for k, v := range keyAttrs {
		if _, ok := newItem[k]; !ok {
			newItem[k] = v
		}
	}

	returned := make([]types.AttributeValue, len(opts.Actions))
	for i, act := range opts.Actions {
		if act.Attribute == "" {
			return AtomicResult{}, fmt.Errorf("action %d: attribute required", i)
		}
		switch act.Kind {
		case AtomicActionSet:
			newItem[act.Attribute] = act.Value
			returned[i] = act.Value
		case AtomicActionIncrReturn, AtomicActionAddReturn:
			if act.Value.T != types.AttrN {
				return AtomicResult{}, fmt.Errorf("action %d: %w: INCR/ADD value must be N", i, ErrAtomicUnsupported)
			}
			cur := newItem[act.Attribute]
			// AttrNull and the zero-value AttrType (unset attribute)
			// both mean "treat base as 0". Any other type is an error.
			if cur.T != types.AttrN && cur.T != types.AttrNull {
				return AtomicResult{}, fmt.Errorf("action %d: target %q is not numeric", i, act.Attribute)
			}
			base := 0.0
			if cur.T == types.AttrN {
				base, err = strconv.ParseFloat(cur.N, 64)
				if err != nil {
					return AtomicResult{}, fmt.Errorf("action %d: parse base: %w", i, err)
				}
			}
			delta, err := strconv.ParseFloat(act.Value.N, 64)
			if err != nil {
				return AtomicResult{}, fmt.Errorf("action %d: parse delta: %w", i, err)
			}
			next := base + delta
			av := types.AttributeValue{T: types.AttrN, N: formatAtomicNumber(next)}
			newItem[act.Attribute] = av
			returned[i] = av
		case AtomicActionApply:
			result, err := evalAtomicExpr(act.Expression, newItem)
			if err != nil {
				return AtomicResult{}, fmt.Errorf("action %d: apply: %w", i, err)
			}
			av := types.AttributeValue{T: types.AttrN, N: formatAtomicNumber(result)}
			newItem[act.Attribute] = av
			returned[i] = av
		default:
			return AtomicResult{}, fmt.Errorf("action %d: %w: unknown kind", i, ErrAtomicUnsupported)
		}
	}

	if err := validateDescriptorItem(td, newItem); err != nil {
		return AtomicResult{}, err
	}

	encoded, err := storage.EncodeItem(newItem)
	if err != nil {
		return AtomicResult{}, fmt.Errorf("encode item: %w", err)
	}

	gsiOps, err := storage.PlanGSI(td.Name, td.KeySchema, td.GSIs, priorItem, newItem)
	if err != nil {
		return AtomicResult{}, fmt.Errorf("plan gsi: %w", err)
	}
	lsiOps, err := storage.PlanLSI(td.Name, td.KeySchema, td.LSIs, priorItem, newItem)
	if err != nil {
		return AtomicResult{}, fmt.Errorf("plan lsi: %w", err)
	}
	spatialOps, err := planSpatial(td.Name, td.KeySchema, td.SpatialIndexes, priorItem, newItem)
	if err != nil {
		return AtomicResult{}, fmt.Errorf("plan spatial: %w", err)
	}
	ttlOps, err := planTTL(td.Name, td.KeySchema, td.TTLAttribute, priorItem, newItem)
	if err != nil {
		return AtomicResult{}, fmt.Errorf("plan ttl: %w", err)
	}

	b := d.Batch()
	defer b.Close()
	if err := b.Set(primaryKey, encoded, nil); err != nil {
		return AtomicResult{}, fmt.Errorf("batch set primary: %w", err)
	}
	if err := applyIndexOps(b, gsiOps); err != nil {
		return AtomicResult{}, err
	}
	if err := applyIndexOps(b, lsiOps); err != nil {
		return AtomicResult{}, err
	}
	if err := applyIndexOps(b, spatialOps); err != nil {
		return AtomicResult{}, err
	}
	if err := applyIndexOps(b, ttlOps); err != nil {
		return AtomicResult{}, err
	}
	if d.shouldAppendChangeRecord(td) {
		rec := newChangeRecord(td, ChangePut, keyItemFromItem(newItem, td.KeySchema), priorItem, newItem)
		rec.BatchID = d.nextBatchID()
		if _, err := d.appendChangeRecord(b, rec); err != nil {
			return AtomicResult{}, fmt.Errorf("change log: %w", err)
		}
	}
	if err := d.CommitBatch(b); err != nil {
		return AtomicResult{}, err
	}
	if isMemoryTable(td) {
		d.memorySet(td.Name, primaryKey, encoded)
	}
	return AtomicResult{Item: newItem, OldItem: priorItem, Returned: returned, Created: created}, nil
}

func cloneItem(in types.Item) types.Item {
	if in == nil {
		return nil
	}
	out := make(types.Item, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// formatAtomicNumber mirrors pkg/sql.formatNumber so atomic post-images
// share canonical decimal text with SQL UPDATE results.
func formatAtomicNumber(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// ---------- whitelisted expression evaluator ----------

// evalAtomicExpr parses src under the APPLY grammar and evaluates it
// against the current item state. The grammar admits only numeric
// arithmetic and the three named functions; anything else returns
// ErrAtomicUnsupported with offset diagnostics.
func evalAtomicExpr(src string, item types.Item) (float64, error) {
	src = strings.TrimSpace(src)
	if src == "" {
		return 0, fmt.Errorf("apply: empty expression")
	}
	// Strip "<attr> = " prefix when present so callers can write either
	// "amount + 1" or "amount = amount + 1" — both are unambiguous since
	// the LHS attribute is already supplied via AtomicAction.Attribute.
	if eq := strings.Index(src, "="); eq > 0 {
		lhs := strings.TrimSpace(src[:eq])
		if isAtomicIdent(lhs) {
			src = strings.TrimSpace(src[eq+1:])
		}
	}
	toks, err := atomicTokenize(src)
	if err != nil {
		return 0, err
	}
	p := &atomicParser{tokens: toks, item: item}
	v, err := p.parseExpr()
	if err != nil {
		return 0, err
	}
	if p.pos < len(p.tokens) {
		return 0, fmt.Errorf("apply: unexpected token %q at position %d", p.tokens[p.pos].lit, p.tokens[p.pos].pos)
	}
	return v, nil
}

type atomicTokKind uint8

const (
	atLParen atomicTokKind = iota + 1
	atRParen
	atComma
	atPlus
	atMinus
	atStar
	atSlash
	atNumber
	atIdent
)

type atomicToken struct {
	kind atomicTokKind
	lit  string
	pos  int
}

func atomicTokenize(src string) ([]atomicToken, error) {
	var out []atomicToken
	r := []rune(src)
	for i := 0; i < len(r); {
		c := r[i]
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '(':
			out = append(out, atomicToken{kind: atLParen, lit: "(", pos: i})
			i++
		case c == ')':
			out = append(out, atomicToken{kind: atRParen, lit: ")", pos: i})
			i++
		case c == ',':
			out = append(out, atomicToken{kind: atComma, lit: ",", pos: i})
			i++
		case c == '+':
			out = append(out, atomicToken{kind: atPlus, lit: "+", pos: i})
			i++
		case c == '-':
			out = append(out, atomicToken{kind: atMinus, lit: "-", pos: i})
			i++
		case c == '*':
			out = append(out, atomicToken{kind: atStar, lit: "*", pos: i})
			i++
		case c == '/':
			out = append(out, atomicToken{kind: atSlash, lit: "/", pos: i})
			i++
		case unicode.IsDigit(c) || (c == '.' && i+1 < len(r) && unicode.IsDigit(r[i+1])):
			j := i
			for j < len(r) && (unicode.IsDigit(r[j]) || r[j] == '.') {
				j++
			}
			// Scientific notation: 1e3 / 1.5e-2.
			if j < len(r) && (r[j] == 'e' || r[j] == 'E') {
				j++
				if j < len(r) && (r[j] == '+' || r[j] == '-') {
					j++
				}
				for j < len(r) && unicode.IsDigit(r[j]) {
					j++
				}
			}
			out = append(out, atomicToken{kind: atNumber, lit: string(r[i:j]), pos: i})
			i = j
		case unicode.IsLetter(c) || c == '_':
			j := i
			for j < len(r) && (unicode.IsLetter(r[j]) || unicode.IsDigit(r[j]) || r[j] == '_') {
				j++
			}
			out = append(out, atomicToken{kind: atIdent, lit: string(r[i:j]), pos: i})
			i = j
		default:
			return nil, fmt.Errorf("apply: unexpected character %q at position %d", string(c), i)
		}
	}
	return out, nil
}

type atomicParser struct {
	tokens []atomicToken
	pos    int
	item   types.Item
}

func (p *atomicParser) peek() (atomicToken, bool) {
	if p.pos >= len(p.tokens) {
		return atomicToken{}, false
	}
	return p.tokens[p.pos], true
}

func (p *atomicParser) consume() (atomicToken, bool) {
	t, ok := p.peek()
	if ok {
		p.pos++
	}
	return t, ok
}

func (p *atomicParser) parseExpr() (float64, error) {
	left, err := p.parseTerm()
	if err != nil {
		return 0, err
	}
	for {
		t, ok := p.peek()
		if !ok || (t.kind != atPlus && t.kind != atMinus) {
			return left, nil
		}
		p.pos++
		right, err := p.parseTerm()
		if err != nil {
			return 0, err
		}
		if t.kind == atPlus {
			left += right
		} else {
			left -= right
		}
	}
}

func (p *atomicParser) parseTerm() (float64, error) {
	left, err := p.parseFactor()
	if err != nil {
		return 0, err
	}
	for {
		t, ok := p.peek()
		if !ok || (t.kind != atStar && t.kind != atSlash) {
			return left, nil
		}
		p.pos++
		right, err := p.parseFactor()
		if err != nil {
			return 0, err
		}
		if t.kind == atStar {
			left *= right
		} else {
			if right == 0 {
				return 0, fmt.Errorf("apply: division by zero at position %d", t.pos)
			}
			left /= right
		}
	}
}

func (p *atomicParser) parseFactor() (float64, error) {
	t, ok := p.peek()
	if !ok {
		return 0, fmt.Errorf("apply: unexpected end of expression")
	}
	switch t.kind {
	case atMinus:
		p.pos++
		v, err := p.parseFactor()
		if err != nil {
			return 0, err
		}
		return -v, nil
	case atPlus:
		p.pos++
		return p.parseFactor()
	case atLParen:
		p.pos++
		v, err := p.parseExpr()
		if err != nil {
			return 0, err
		}
		closer, ok := p.consume()
		if !ok || closer.kind != atRParen {
			return 0, fmt.Errorf("apply: expected ')' at position %d", t.pos)
		}
		return v, nil
	case atNumber:
		p.pos++
		f, err := strconv.ParseFloat(t.lit, 64)
		if err != nil {
			return 0, fmt.Errorf("apply: invalid number %q at position %d", t.lit, t.pos)
		}
		return f, nil
	case atIdent:
		p.pos++
		// Function call?
		if nxt, ok := p.peek(); ok && nxt.kind == atLParen {
			return p.parseFuncCall(t)
		}
		// Bare identifier — resolve from item.
		av, ok := p.item[t.lit]
		if !ok {
			return 0, fmt.Errorf("apply: unknown attribute %q at position %d", t.lit, t.pos)
		}
		if av.T != types.AttrN {
			return 0, fmt.Errorf("apply: attribute %q is not numeric", t.lit)
		}
		f, err := strconv.ParseFloat(av.N, 64)
		if err != nil {
			return 0, fmt.Errorf("apply: attribute %q parse: %w", t.lit, err)
		}
		return f, nil
	}
	return 0, fmt.Errorf("apply: unexpected token %q at position %d", t.lit, t.pos)
}

func (p *atomicParser) parseFuncCall(name atomicToken) (float64, error) {
	// Consume '('.
	p.pos++
	args, err := p.parseArgs()
	if err != nil {
		return 0, err
	}
	closer, ok := p.consume()
	if !ok || closer.kind != atRParen {
		return 0, fmt.Errorf("apply: expected ')' for %s at position %d", name.lit, name.pos)
	}
	switch strings.ToLower(name.lit) {
	case "min":
		if len(args) < 2 {
			return 0, fmt.Errorf("apply: min(...) needs at least 2 args")
		}
		m := args[0]
		for _, v := range args[1:] {
			if v < m {
				m = v
			}
		}
		return m, nil
	case "max":
		if len(args) < 2 {
			return 0, fmt.Errorf("apply: max(...) needs at least 2 args")
		}
		m := args[0]
		for _, v := range args[1:] {
			if v > m {
				m = v
			}
		}
		return m, nil
	case "clamp":
		if len(args) != 3 {
			return 0, fmt.Errorf("apply: clamp(x, lo, hi) needs exactly 3 args")
		}
		x, lo, hi := args[0], args[1], args[2]
		if lo > hi {
			return 0, fmt.Errorf("apply: clamp lo > hi")
		}
		if x < lo {
			return lo, nil
		}
		if x > hi {
			return hi, nil
		}
		return x, nil
	}
	return 0, fmt.Errorf("apply: %w: function %q", ErrAtomicUnsupported, name.lit)
}

func (p *atomicParser) parseArgs() ([]float64, error) {
	if t, ok := p.peek(); ok && t.kind == atRParen {
		return nil, nil
	}
	var out []float64
	for {
		v, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		t, ok := p.peek()
		if !ok || t.kind != atComma {
			return out, nil
		}
		p.pos++
	}
}

func isAtomicIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 && !(unicode.IsLetter(r) || r == '_') {
			return false
		}
		if i > 0 && !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return true
}
