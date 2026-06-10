package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// EvalBool evaluates a boolean expression against an item. Used both
// by the executor (post-filter on SELECT rows) and by IF / WHERE
// checks on INSERT / UPDATE / DELETE.
//
// Supported nodes:
//   - BinaryExpr (AND/OR/comparisons)
//   - NotExpr
//   - BetweenExpr
//   - FuncCall: ATTRIBUTE_EXISTS / ATTRIBUTE_NOT_EXISTS /
//     ATTRIBUTE_TYPE / SIZE / BEGINS_WITH / CONTAINS. Spatial
//     functions evaluate to true (handled at the planner level).
//   - ColumnRef + Literal as comparison operands.
//
// `item` may be nil — attribute_exists(x) returns false on nil items,
// attribute_not_exists(x) returns true, comparisons fail.
func EvalBool(e Expr, item types.Item, binds map[string]types.AttributeValue) (bool, error) {
	switch n := e.(type) {
	case nil:
		return true, nil
	case *BinaryExpr:
		return evalBinary(n, item, binds)
	case *NotExpr:
		v, err := EvalBool(n.Inner, item, binds)
		return !v, err
	case *BetweenExpr:
		return evalBetween(n, item, binds)
	case *FuncCall:
		return evalFunc(n, item, binds)
	}
	return false, fmt.Errorf("unsupported expression in boolean context: %T", e)
}

func evalBinary(n *BinaryExpr, item types.Item, binds map[string]types.AttributeValue) (bool, error) {
	switch n.Op {
	case BinAnd:
		l, err := EvalBool(n.Left, item, binds)
		if err != nil {
			return false, err
		}
		if !l {
			return false, nil
		}
		return EvalBool(n.Right, item, binds)
	case BinOr:
		l, err := EvalBool(n.Left, item, binds)
		if err != nil {
			return false, err
		}
		if l {
			return true, nil
		}
		return EvalBool(n.Right, item, binds)
	}
	lv, lOK, err := evalOperand(n.Left, item, binds)
	if err != nil {
		return false, err
	}
	rv, rOK, err := evalOperand(n.Right, item, binds)
	if err != nil {
		return false, err
	}
	if !lOK || !rOK {
		// Missing attributes always fail comparisons — DynamoDB
		// semantics carry over from the storage condition evaluator.
		return false, nil
	}
	c, err := compareAV(lv, rv)
	if err != nil {
		return false, err
	}
	switch n.Op {
	case BinEq:
		return c == 0, nil
	case BinNeq:
		return c != 0, nil
	case BinLt:
		return c < 0, nil
	case BinLte:
		return c <= 0, nil
	case BinGt:
		return c > 0, nil
	case BinGte:
		return c >= 0, nil
	}
	return false, fmt.Errorf("unknown binary op %d", n.Op)
}

func evalBetween(n *BetweenExpr, item types.Item, binds map[string]types.AttributeValue) (bool, error) {
	v, ok, err := evalOperand(n.Value, item, binds)
	if err != nil || !ok {
		return false, err
	}
	lo, _, err := evalOperand(n.Lo, item, binds)
	if err != nil {
		return false, err
	}
	hi, _, err := evalOperand(n.Hi, item, binds)
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

// evalFunc dispatches the scalar boolean functions. Spatial helpers
// (ST_Within etc.) return true here so post-filters do not further
// constrain the rows — the planner has already converted them into
// the storage spatial query.
func evalFunc(n *FuncCall, item types.Item, binds map[string]types.AttributeValue) (bool, error) {
	switch strings.ToUpper(n.Name) {
	case "ATTRIBUTE_EXISTS":
		if len(n.Args) != 1 {
			return false, fmt.Errorf("attribute_exists arity")
		}
		col, ok := n.Args[0].(*ColumnRef)
		if !ok {
			return false, fmt.Errorf("attribute_exists expects a column reference")
		}
		_, has := item[col.Name]
		return has, nil
	case "ATTRIBUTE_NOT_EXISTS":
		if len(n.Args) != 1 {
			return false, fmt.Errorf("attribute_not_exists arity")
		}
		col, ok := n.Args[0].(*ColumnRef)
		if !ok {
			return false, fmt.Errorf("attribute_not_exists expects a column reference")
		}
		_, has := item[col.Name]
		return !has, nil
	case "ATTRIBUTE_TYPE":
		if len(n.Args) != 2 {
			return false, fmt.Errorf("attribute_type arity")
		}
		col, ok := n.Args[0].(*ColumnRef)
		if !ok {
			return false, fmt.Errorf("attribute_type expects a column reference as first arg")
		}
		want, _, err := evalOperand(n.Args[1], item, binds)
		if err != nil {
			return false, err
		}
		av, has := item[col.Name]
		if !has {
			return false, nil
		}
		return strings.EqualFold(want.S, attrTypeName(av.T)), nil
	case "SIZE":
		// size() in a boolean context is rare; we keep it as a value
		// function and let an outer comparison consume it.
		return false, fmt.Errorf("size() must appear inside a comparison")
	case "BEGINS_WITH":
		if len(n.Args) != 2 {
			return false, fmt.Errorf("begins_with arity")
		}
		lv, lOK, err := evalOperand(n.Args[0], item, binds)
		if err != nil || !lOK {
			return false, err
		}
		rv, _, err := evalOperand(n.Args[1], item, binds)
		if err != nil {
			return false, err
		}
		return strings.HasPrefix(lv.S, rv.S), nil
	case "CONTAINS":
		if len(n.Args) != 2 {
			return false, fmt.Errorf("contains arity")
		}
		lv, lOK, err := evalOperand(n.Args[0], item, binds)
		if err != nil || !lOK {
			return false, err
		}
		rv, _, err := evalOperand(n.Args[1], item, binds)
		if err != nil {
			return false, err
		}
		switch lv.T {
		case types.AttrS:
			return strings.Contains(lv.S, rv.S), nil
		case types.AttrSS:
			for _, s := range lv.SS {
				if s == rv.S {
					return true, nil
				}
			}
			return false, nil
		case types.AttrNS:
			for _, s := range lv.NS {
				if s == rv.N {
					return true, nil
				}
			}
			return false, nil
		case types.AttrL:
			for _, v := range lv.L {
				if c, err := compareAV(v, rv); err == nil && c == 0 {
					return true, nil
				}
			}
			return false, nil
		}
		return false, nil
	case "ST_WITHIN", "ST_DWITHIN":
		// Planner already converted these. Post-filter pass-through.
		return true, nil
	}
	return false, fmt.Errorf("unknown function %s", n.Name)
}

// evalOperand resolves a column or literal as an AttributeValue. The
// second return reports presence on the item — missing columns
// short-circuit comparisons to false.
func evalOperand(e Expr, item types.Item, binds map[string]types.AttributeValue) (types.AttributeValue, bool, error) {
	switch n := e.(type) {
	case *Literal:
		v, err := evalLiteral(n)
		return v, true, err
	case *ColumnRef:
		av, ok := item[n.Name]
		return av, ok, nil
	case *FuncCall:
		// Functions used as values (e.g. size(col) in a comparison).
		if strings.EqualFold(n.Name, "SIZE") {
			if len(n.Args) != 1 {
				return types.AttributeValue{}, false, fmt.Errorf("size arity")
			}
			col, ok := n.Args[0].(*ColumnRef)
			if !ok {
				return types.AttributeValue{}, false, fmt.Errorf("size expects a column reference")
			}
			av, present := item[col.Name]
			if !present {
				return types.AttributeValue{}, false, nil
			}
			return types.AttributeValue{T: types.AttrN, N: strconv.Itoa(attrSize(av))}, true, nil
		}
	}
	return types.AttributeValue{}, false, fmt.Errorf("operand %T not usable as a value", e)
}

func attrSize(av types.AttributeValue) int {
	switch av.T {
	case types.AttrS:
		return len(av.S)
	case types.AttrN:
		return len(av.N)
	case types.AttrB:
		return len(av.B)
	case types.AttrL:
		return len(av.L)
	case types.AttrSS:
		return len(av.SS)
	case types.AttrNS:
		return len(av.NS)
	case types.AttrBS:
		return len(av.BS)
	case types.AttrM:
		return len(av.M)
	}
	return 0
}

func attrTypeName(t types.AttrType) string {
	switch t {
	case types.AttrS:
		return "S"
	case types.AttrN:
		return "N"
	case types.AttrB:
		return "B"
	case types.AttrBOOL:
		return "BOOL"
	case types.AttrNull:
		return "NULL"
	case types.AttrSS:
		return "SS"
	case types.AttrNS:
		return "NS"
	case types.AttrBS:
		return "BS"
	case types.AttrL:
		return "L"
	case types.AttrM:
		return "M"
	}
	return ""
}

func compareAV(a, b types.AttributeValue) (int, error) {
	if a.T != b.T {
		return 0, fmt.Errorf("cannot compare attribute types %d and %d", a.T, b.T)
	}
	switch a.T {
	case types.AttrS:
		return strings.Compare(a.S, b.S), nil
	case types.AttrN:
		// Numeric comparison: parse both sides to float so "11" > "5".
		// Falls back to lexicographic compare on parse failure (e.g.
		// numbers with leading zeros that the storage layer may not
		// canonicalise).
		af, errA := strconv.ParseFloat(a.N, 64)
		bf, errB := strconv.ParseFloat(b.N, 64)
		if errA != nil || errB != nil {
			return strings.Compare(a.N, b.N), nil
		}
		switch {
		case af < bf:
			return -1, nil
		case af > bf:
			return 1, nil
		}
		return 0, nil
	case types.AttrB:
		return bytesCompare(a.B, b.B), nil
	case types.AttrBOOL:
		ai, bi := 0, 0
		if a.BOOL {
			ai = 1
		}
		if b.BOOL {
			bi = 1
		}
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

func bytesCompare(a, b []byte) int {
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
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}
