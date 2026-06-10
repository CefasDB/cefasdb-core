package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// Result is the executor output. SELECT statements populate Rows;
// INSERT / UPDATE / DELETE / CREATE / DROP set AffectedRows.
type Result struct {
	Rows         []types.Item
	AffectedRows int
}

// Storage is the executor's view of the storage engine. The real
// *storage.DB satisfies it; tests can fake parts.
type Storage interface {
	PutItemWith(td types.TableDescriptor, item types.Item, opts storage.PutOptions) error
	DeleteItemWith(td types.TableDescriptor, key types.Item, opts storage.DeleteOptions) error
	GetItem(table string, ks types.KeySchema, key types.Item) (types.Item, error)
	QueryByPK(table string, ks types.KeySchema, pkAttr types.AttributeValue, limit int) ([]types.Item, error)
	QueryByPKRange(table string, ks types.KeySchema, pkAttr, skLow, skHigh types.AttributeValue, limit int) ([]types.Item, error)
	QueryByGSI(td types.TableDescriptor, idxName string, gsiPKVal types.AttributeValue, opts storage.QueryOptions) ([]types.Item, error)
	SpatialQueryItems(td types.TableDescriptor, idxName string, q storage.SpatialQuery) ([]types.Item, error)
}

// CatalogMutator is the schema-management surface the executor uses
// for CREATE / DROP TABLE.
type CatalogMutator interface {
	Create(td types.TableDescriptor) error
	Drop(name string) error
	Describe(name string) (types.TableDescriptor, error)
}

// Executor runs a compiled Plan against the storage + catalog.
type Executor struct {
	Storage Storage
	Catalog CatalogMutator
}

// Execute dispatches to the plan-specific path.
func (e *Executor) Execute(plan Plan) (*Result, error) {
	switch p := plan.(type) {
	case *PlanCreateTable:
		return e.execCreate(p)
	case *PlanDropTable:
		return e.execDrop(p)
	case *PlanPutItem:
		return e.execInsert(p)
	case *PlanUpdate:
		return e.execUpdate(p)
	case *PlanDelete:
		return e.execDelete(p)
	case *PlanGetItem:
		return e.execGet(p)
	case *PlanQuery:
		return e.execQuery(p)
	case *PlanSpatial:
		return e.execSpatial(p)
	}
	return nil, fmt.Errorf("unsupported plan type %T", plan)
}

func (e *Executor) execCreate(p *PlanCreateTable) (*Result, error) {
	if err := e.Catalog.Create(p.Descriptor); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execDrop(p *PlanDropTable) (*Result, error) {
	if err := e.Catalog.Drop(p.Name); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execInsert(p *PlanPutItem) (*Result, error) {
	if p.If != nil {
		prior, err := e.Storage.GetItem(p.Table, p.Descriptor.KeySchema, keyOnly(p.Item, p.Descriptor.KeySchema))
		if err != nil && !errors.Is(err, types.ErrItemNotFound) {
			return nil, err
		}
		ok, evalErr := EvalBool(p.If, prior, nil)
		if evalErr != nil {
			return nil, evalErr
		}
		if !ok {
			return nil, storage.ErrConditionFailed
		}
	}
	if err := e.Storage.PutItemWith(p.Descriptor, p.Item, storage.PutOptions{}); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execUpdate(p *PlanUpdate) (*Result, error) {
	prior, err := e.Storage.GetItem(p.Table, p.Descriptor.KeySchema, p.Key)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			return &Result{AffectedRows: 0}, nil
		}
		return nil, err
	}
	if p.If != nil {
		ok, err := EvalBool(p.If, prior, nil)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, storage.ErrConditionFailed
		}
	}
	for _, a := range p.Actions {
		if err := applyAction(prior, a); err != nil {
			return nil, fmt.Errorf("UPDATE %s %q: %w", actionKindName(a.Kind), a.Column, err)
		}
	}
	if err := e.Storage.PutItemWith(p.Descriptor, prior, storage.PutOptions{}); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execDelete(p *PlanDelete) (*Result, error) {
	if p.If != nil {
		prior, err := e.Storage.GetItem(p.Table, p.Descriptor.KeySchema, p.Key)
		if err != nil && !errors.Is(err, types.ErrItemNotFound) {
			return nil, err
		}
		ok, evalErr := EvalBool(p.If, prior, nil)
		if evalErr != nil {
			return nil, evalErr
		}
		if !ok {
			return nil, storage.ErrConditionFailed
		}
	}
	if err := e.Storage.DeleteItemWith(p.Descriptor, p.Key, storage.DeleteOptions{}); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func keyOnly(item types.Item, ks types.KeySchema) types.Item {
	out := types.Item{ks.PK: item[ks.PK]}
	if ks.SK != "" {
		out[ks.SK] = item[ks.SK]
	}
	return out
}

func actionKindName(k AssignKind) string {
	switch k {
	case AssignSet:
		return "SET"
	case AssignRemove:
		return "REMOVE"
	case AssignAdd:
		return "ADD"
	case AssignDelete:
		return "DELETE"
	}
	return "?"
}

// applyAction mutates `item` in place to reflect a single SET /
// REMOVE / ADD / DELETE action.
func applyAction(item types.Item, a Assignment) error {
	switch a.Kind {
	case AssignSet:
		v, err := evalAssignExpr(a.Value, item)
		if err != nil {
			return err
		}
		item[a.Column] = v
		return nil
	case AssignRemove:
		delete(item, a.Column)
		return nil
	case AssignAdd:
		v, err := evalLiteral(a.Value)
		if err != nil {
			return err
		}
		return numericIncrement(item, a.Column, v)
	case AssignDelete:
		v, err := evalLiteral(a.Value)
		if err != nil {
			return err
		}
		return setMemberRemove(item, a.Column, v)
	}
	return fmt.Errorf("unsupported action kind %d", a.Kind)
}

func evalAssignExpr(e Expr, item types.Item) (types.AttributeValue, error) {
	switch n := e.(type) {
	case *Literal:
		return evalLiteral(n)
	case *ColumnRef:
		return item[n.Name], nil
	case *ArithExpr:
		base, err := evalAssignExpr(n.Left, item)
		if err != nil {
			return types.AttributeValue{}, err
		}
		delta, err := evalAssignExpr(n.Right, item)
		if err != nil {
			return types.AttributeValue{}, err
		}
		if base.T == types.AttrNull {
			base = types.AttributeValue{T: types.AttrN, N: "0"}
		}
		if base.T != types.AttrN || delta.T != types.AttrN {
			return types.AttributeValue{}, fmt.Errorf("arithmetic on non-numeric attribute")
		}
		bv, err := parseNumber(base.N)
		if err != nil {
			return types.AttributeValue{}, err
		}
		dv, err := parseNumber(delta.N)
		if err != nil {
			return types.AttributeValue{}, err
		}
		var out float64
		switch n.Op {
		case ArithAdd:
			out = bv + dv
		case ArithSub:
			out = bv - dv
		}
		return types.AttributeValue{T: types.AttrN, N: formatNumber(out)}, nil
	case *FuncCall:
		if strings.EqualFold(n.Name, "LIST_APPEND") {
			return listAppend(item, n.Args, false)
		}
		if strings.EqualFold(n.Name, "LIST_PREPEND") {
			return listAppend(item, n.Args, true)
		}
	}
	return types.AttributeValue{}, fmt.Errorf("unsupported assignment expression %T", e)
}

func listAppend(item types.Item, args []Expr, prepend bool) (types.AttributeValue, error) {
	if len(args) != 2 {
		return types.AttributeValue{}, fmt.Errorf("list_append/prepend arity")
	}
	base, err := evalAssignExpr(args[0], item)
	if err != nil {
		return types.AttributeValue{}, err
	}
	add, err := evalAssignExpr(args[1], item)
	if err != nil {
		return types.AttributeValue{}, err
	}
	if base.T != types.AttrL && base.T != types.AttrNull {
		return types.AttributeValue{}, fmt.Errorf("list_append target must be a list (or absent)")
	}
	var listOut []types.AttributeValue
	if base.T == types.AttrL {
		listOut = base.L
	}
	if prepend {
		listOut = append([]types.AttributeValue{add}, listOut...)
	} else {
		listOut = append(listOut, add)
	}
	return types.AttributeValue{T: types.AttrL, L: listOut}, nil
}

func numericIncrement(item types.Item, col string, delta types.AttributeValue) error {
	if delta.T != types.AttrN {
		return fmt.Errorf("ADD non-numeric value")
	}
	dv, err := parseNumber(delta.N)
	if err != nil {
		return err
	}
	cur := item[col]
	if cur.T == types.AttrNull {
		item[col] = types.AttributeValue{T: types.AttrN, N: delta.N}
		return nil
	}
	if cur.T != types.AttrN {
		return fmt.Errorf("ADD target is not numeric")
	}
	bv, err := parseNumber(cur.N)
	if err != nil {
		return err
	}
	item[col] = types.AttributeValue{T: types.AttrN, N: formatNumber(bv + dv)}
	return nil
}

func setMemberRemove(item types.Item, col string, val types.AttributeValue) error {
	cur, ok := item[col]
	if !ok || cur.T == types.AttrNull {
		return nil
	}
	switch cur.T {
	case types.AttrSS:
		filtered := cur.SS[:0]
		for _, s := range cur.SS {
			if s != val.S {
				filtered = append(filtered, s)
			}
		}
		item[col] = types.AttributeValue{T: types.AttrSS, SS: filtered}
		return nil
	case types.AttrNS:
		filtered := cur.NS[:0]
		for _, s := range cur.NS {
			if s != val.N {
				filtered = append(filtered, s)
			}
		}
		item[col] = types.AttributeValue{T: types.AttrNS, NS: filtered}
		return nil
	}
	return fmt.Errorf("DELETE target must be a set")
}

func parseNumber(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse number %q: %w", s, err)
	}
	return f, nil
}

func formatNumber(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func (e *Executor) execGet(p *PlanGetItem) (*Result, error) {
	td, err := e.Catalog.Describe(p.Table)
	if err != nil {
		return nil, err
	}
	item, err := e.Storage.GetItem(p.Table, td.KeySchema, p.Key)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			return &Result{}, nil
		}
		return nil, err
	}
	return &Result{Rows: []types.Item{item}}, nil
}

func (e *Executor) execQuery(p *PlanQuery) (*Result, error) {
	var (
		items []types.Item
		err   error
	)
	openLow := p.SKLow.T == types.AttrNull
	openHigh := p.SKHigh.T == types.AttrNull

	switch {
	case p.IndexName != "":
		items, err = e.Storage.QueryByGSI(p.Descriptor, p.IndexName, p.PKValue, storage.QueryOptions{
			SKLow:  p.SKLow,
			SKHigh: p.SKHigh,
			Limit:  p.Limit,
		})
	case openLow && openHigh:
		items, err = e.Storage.QueryByPK(p.Table, p.Descriptor.KeySchema, p.PKValue, p.Limit)
	default:
		items, err = e.Storage.QueryByPKRange(p.Table, p.Descriptor.KeySchema, p.PKValue, p.SKLow, p.SKHigh, p.Limit)
	}
	if err != nil {
		return nil, err
	}
	if p.PostFilter != nil {
		filtered := items[:0]
		for _, it := range items {
			keep, err := EvalBool(p.PostFilter, it, nil)
			if err != nil {
				return nil, err
			}
			if keep {
				filtered = append(filtered, it)
			}
		}
		items = filtered
	}
	if p.OrderDesc {
		reverse(items)
	}
	if len(p.Project) > 0 {
		project(items, p.Project)
	}
	return &Result{Rows: items}, nil
}

func (e *Executor) execSpatial(p *PlanSpatial) (*Result, error) {
	items, err := e.Storage.SpatialQueryItems(p.Descriptor, p.IndexName, p.Query)
	if err != nil {
		return nil, err
	}
	if len(p.Project) > 0 {
		project(items, p.Project)
	}
	return &Result{Rows: items}, nil
}

func reverse(items []types.Item) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

func project(items []types.Item, cols []string) {
	keep := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		keep[c] = struct{}{}
	}
	for i, it := range items {
		out := make(types.Item, len(cols))
		for k, v := range it {
			if _, ok := keep[k]; ok {
				out[k] = v
			}
		}
		items[i] = out
	}
}
