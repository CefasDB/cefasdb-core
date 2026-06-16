package sql

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/osvaldoandrade/cefas/internal/storage"
	cquery "github.com/osvaldoandrade/cefas/pkg/core/query"
	"github.com/osvaldoandrade/cefas/pkg/core/query/mmr"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// Result is the executor output. SELECT statements populate Rows;
// INSERT / UPDATE / DELETE / CREATE / DROP set AffectedRows.
type Result struct {
	Rows         []types.Item
	AffectedRows int
}

// ItemMutation describes one row-level DML change after the storage
// mutation has succeeded. NewItem is nil for deletes; DeleteKey carries
// the caller-supplied key even when the row did not previously exist.
type ItemMutation struct {
	Table     string
	OldItem   types.Item
	NewItem   types.Item
	DeleteKey types.Item
}

// MutationHook lets API layers attach secondary maintenance to SQL DML
// without teaching the SQL package about plugin registries.
type MutationHook func(ItemMutation) error

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
	ScanTable(table string, limit int) ([]types.Item, error)
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
	Storage              Storage
	Catalog              CatalogMutator
	TableDropHook        func(table string) error
	DistanceResolver     func(table, field string, target types.AttributeValue) (cquery.DistanceOp, error)
	ANNCandidateResolver func(table, field string, target types.AttributeValue, limit int) ([]cquery.TopKResult, bool, error)
	MutationHook         MutationHook
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
	case *PlanScan:
		return e.execScan(p)
	case *PlanJoin:
		return e.execJoin(p)
	case *PlanSpatial:
		return e.execSpatial(p)
	case *PlanANN:
		return e.execANN(p)
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
	if e.TableDropHook != nil {
		if err := e.TableDropHook(p.Name); err != nil {
			return nil, err
		}
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execInsert(p *PlanPutItem) (*Result, error) {
	var prior types.Item
	if p.If != nil || e.MutationHook != nil {
		var err error
		prior, err = e.Storage.GetItem(p.Table, p.Descriptor.KeySchema, keyOnly(p.Item, p.Descriptor.KeySchema))
		if err != nil && !errors.Is(err, types.ErrItemNotFound) {
			return nil, err
		}
		if p.If != nil {
			ok, evalErr := EvalBool(p.If, prior, nil)
			if evalErr != nil {
				return nil, evalErr
			}
			if !ok {
				return nil, storage.ErrConditionFailed
			}
		}
	}
	if err := e.Storage.PutItemWith(p.Descriptor, p.Item, storage.PutOptions{}); err != nil {
		return nil, err
	}
	if e.MutationHook != nil {
		if err := e.MutationHook(ItemMutation{Table: p.Table, OldItem: cloneItem(prior), NewItem: cloneItem(p.Item)}); err != nil {
			return nil, err
		}
	}
	res := &Result{AffectedRows: 1}
	switch p.Returning {
	case ReturningAll, ReturningNew:
		res.Rows = []types.Item{cloneItem(p.Item)}
	}
	return res, nil
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
	oldImage := cloneItem(prior)
	for _, a := range p.Actions {
		if err := applyAction(prior, a); err != nil {
			return nil, fmt.Errorf("UPDATE %s %q: %w", actionKindName(a.Kind), a.Column, err)
		}
	}
	if err := e.Storage.PutItemWith(p.Descriptor, prior, storage.PutOptions{}); err != nil {
		return nil, err
	}
	if e.MutationHook != nil {
		if err := e.MutationHook(ItemMutation{Table: p.Table, OldItem: cloneItem(oldImage), NewItem: cloneItem(prior)}); err != nil {
			return nil, err
		}
	}
	res := &Result{AffectedRows: 1}
	switch p.Returning {
	case ReturningNew, ReturningAll:
		res.Rows = []types.Item{cloneItem(prior)}
	case ReturningOld:
		res.Rows = []types.Item{oldImage}
	}
	return res, nil
}

func (e *Executor) execDelete(p *PlanDelete) (*Result, error) {
	var oldImage types.Item
	if p.If != nil || p.Returning != ReturningNone || e.MutationHook != nil {
		prior, err := e.Storage.GetItem(p.Table, p.Descriptor.KeySchema, p.Key)
		if err != nil && !errors.Is(err, types.ErrItemNotFound) {
			return nil, err
		}
		oldImage = prior
		if p.If != nil {
			ok, evalErr := EvalBool(p.If, prior, nil)
			if evalErr != nil {
				return nil, evalErr
			}
			if !ok {
				return nil, storage.ErrConditionFailed
			}
		}
	}
	if err := e.Storage.DeleteItemWith(p.Descriptor, p.Key, storage.DeleteOptions{}); err != nil {
		return nil, err
	}
	if e.MutationHook != nil {
		if err := e.MutationHook(ItemMutation{Table: p.Table, OldItem: cloneItem(oldImage), DeleteKey: cloneItem(p.Key)}); err != nil {
			return nil, err
		}
	}
	res := &Result{AffectedRows: 1}
	if (p.Returning == ReturningOld || p.Returning == ReturningAll) && oldImage != nil {
		res.Rows = []types.Item{oldImage}
	}
	return res, nil
}

// cloneItem returns a shallow copy. Used so RETURNING doesn't echo
// back a map the caller could mutate over our internal state.
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
	case *VectorLiteral:
		return types.AttributeValue{T: types.AttrVec, Vec: append([]float64(nil), n.Values...)}, nil
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
	sourceLimit := p.Limit
	if len(p.GroupBy) > 0 {
		sourceLimit = 0
	}

	switch {
	case p.IndexName != "":
		items, err = e.Storage.QueryByGSI(p.Descriptor, p.IndexName, p.PKValue, storage.QueryOptions{
			SKLow:  p.SKLow,
			SKHigh: p.SKHigh,
			Limit:  sourceLimit,
		})
	case openLow && openHigh:
		items, err = e.Storage.QueryByPK(p.Table, p.Descriptor.KeySchema, p.PKValue, sourceLimit)
	default:
		items, err = e.Storage.QueryByPKRange(p.Table, p.Descriptor.KeySchema, p.PKValue, p.SKLow, p.SKHigh, sourceLimit)
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
	if p.Count {
		return &Result{AffectedRows: len(items)}, nil
	}
	if len(p.GroupBy) > 0 {
		return aggregateRows(items, p.GroupBy, p.Aggs, p.Limit), nil
	}
	if p.OrderDesc {
		reverse(items)
	}
	if len(p.Project) > 0 {
		project(items, p.Project)
	}
	return &Result{Rows: items}, nil
}

func (e *Executor) execScan(p *PlanScan) (*Result, error) {
	sourceLimit := p.Limit
	if p.Predicate != nil || p.Count || len(p.GroupBy) > 0 {
		sourceLimit = 0
	}
	items, err := e.Storage.ScanTable(p.Table, sourceLimit)
	if err != nil {
		return nil, err
	}
	if p.Predicate != nil {
		filtered := items[:0]
		for _, it := range items {
			keep, err := EvalBool(p.Predicate, it, nil)
			if err != nil {
				return nil, err
			}
			if keep {
				filtered = append(filtered, it)
			}
		}
		items = filtered
	}
	if p.Count {
		return &Result{AffectedRows: len(items)}, nil
	}
	if len(p.GroupBy) > 0 {
		return aggregateRows(items, p.GroupBy, p.Aggs, p.Limit), nil
	}
	if p.Limit > 0 && len(items) > p.Limit {
		items = items[:p.Limit]
	}
	if len(p.Project) > 0 {
		project(items, p.Project)
	}
	return &Result{Rows: items}, nil
}

func (e *Executor) execJoin(p *PlanJoin) (*Result, error) {
	leftItems, err := e.Storage.ScanTable(p.LeftTable, 0)
	if err != nil {
		return nil, err
	}
	rightItems, err := e.Storage.ScanTable(p.RightTable, 0)
	if err != nil {
		return nil, err
	}

	rightByKey := make(map[string][]types.Item)
	for _, right := range rightItems {
		av, ok := right[p.RightColumn]
		if !ok || av.T == types.AttrNull {
			continue
		}
		key := joinAttrKey(av)
		rightByKey[key] = append(rightByKey[key], right)
	}

	var rows []types.Item
	for _, left := range leftItems {
		av, ok := left[p.LeftColumn]
		if !ok || av.T == types.AttrNull {
			continue
		}
		for _, right := range rightByKey[joinAttrKey(av)] {
			row := joinItems(p.LeftAlias, left, p.RightAlias, right)
			if p.Predicate != nil {
				keep, err := EvalBool(p.Predicate, row, nil)
				if err != nil {
					return nil, err
				}
				if !keep {
					continue
				}
			}
			if len(p.Project) > 0 {
				projected := []types.Item{row}
				project(projected, p.Project)
				row = projected[0]
			}
			rows = append(rows, row)
			if p.Limit > 0 && len(rows) >= p.Limit {
				return &Result{Rows: rows}, nil
			}
		}
	}
	return &Result{Rows: rows}, nil
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

func (e *Executor) execANN(p *PlanANN) (*Result, error) {
	if e.DistanceResolver == nil {
		return nil, fmt.Errorf("ANN distance resolver not configured")
	}
	op, err := e.DistanceResolver(p.Table, p.Field, p.Target)
	if err != nil {
		return nil, err
	}
	if e.ANNCandidateResolver != nil {
		fanout := p.Limit
		if p.Predicate != nil && p.Filter.Strategy == cquery.StrategyANNFirstOverscan {
			overscan := p.Filter.OverscanFactor
			if overscan < 1 {
				overscan = 1
			}
			fanout = p.Limit * overscan
		}
		rows, ok, err := e.ANNCandidateResolver(p.Table, p.Field, p.Target, fanout)
		if err != nil {
			return nil, err
		}
		if ok {
			if p.Predicate != nil {
				var sel cquery.Selectivity
				rows, sel, err = filterANNRows(p, rows)
				if err != nil {
					return nil, err
				}
				p.Filter.Selectivity.Actual = sel.Actual
				p.Filter.Selectivity.CandidateRows = sel.CandidateRows
				p.Filter.Selectivity.KeptRows = sel.KeptRows
				if len(rows) < p.Limit {
					p.Filter.Warning = cquery.FewerThanKWarning
				}
			}
			return e.finishANN(p, rows, op)
		}
	}
	eng, err := cquery.NewTopK(op, p.Field, p.Target, p.Limit)
	if err != nil {
		return nil, err
	}
	items, err := e.Storage.ScanTable(p.Table, 0)
	if err != nil {
		return nil, err
	}

	if p.Predicate == nil {
		for _, item := range items {
			if err := eng.Observe(item); err != nil {
				return nil, err
			}
		}
		return e.finishANN(p, eng.Result(), op)
	}

	pred := cquery.PredicateFunc(func(it types.Item) (bool, error) {
		return EvalBool(p.Predicate, it, nil)
	})

	candidates := items
	if p.Filter.Strategy == cquery.StrategyANNFirstOverscan {
		// Without a streaming ANN index in storage today we rank the
		// full table here and slice the top k*overscan. A real ANN
		// engine would terminate its candidate stream once enough
		// survivors were collected.
		overscan := p.Filter.OverscanFactor
		if overscan < 1 {
			overscan = 1
		}
		ranker, rerr := cquery.NewTopK(op, p.Field, p.Target, p.Limit*overscan)
		if rerr != nil {
			return nil, rerr
		}
		for _, item := range items {
			if err := ranker.Observe(item); err != nil {
				return nil, err
			}
		}
		ranked := ranker.Result()
		candidates = make([]types.Item, len(ranked))
		for i, r := range ranked {
			candidates[i] = r.Item
		}
	}

	sel, err := cquery.ApplyPredicate(eng, pred, candidates)
	if err != nil {
		return nil, err
	}

	rows := eng.Result()
	if sel != nil {
		p.Filter.Selectivity.Actual = sel.Actual
		p.Filter.Selectivity.CandidateRows = sel.CandidateRows
		p.Filter.Selectivity.KeptRows = sel.KeptRows
	}
	if len(rows) < p.Limit {
		p.Filter.Warning = cquery.FewerThanKWarning
	}
	return e.finishANN(p, rows, op)
}

func filterANNRows(p *PlanANN, rows []cquery.TopKResult) ([]cquery.TopKResult, cquery.Selectivity, error) {
	sel := cquery.Selectivity{CandidateRows: len(rows)}
	capHint := len(rows)
	if capHint > p.Limit {
		capHint = p.Limit
	}
	out := make([]cquery.TopKResult, 0, capHint)
	for _, row := range rows {
		ok, err := EvalBool(p.Predicate, row.Item, nil)
		if err != nil {
			return nil, sel, err
		}
		if !ok {
			continue
		}
		sel.KeptRows++
		if len(out) < p.Limit {
			out = append(out, row)
		}
	}
	if sel.CandidateRows > 0 {
		sel.Actual = float64(sel.KeptRows) / float64(sel.CandidateRows)
	}
	return out, sel, nil
}

func (e *Executor) finishANN(p *PlanANN, rows []cquery.TopKResult, op cquery.DistanceOp) (*Result, error) {
	if p.Diversify != nil {
		cands := make([]mmr.Candidate, 0, len(rows))
		for _, row := range rows {
			cands = append(cands, mmr.Candidate{
				Item:     row.Item,
				Distance: row.Distance,
				Vector:   row.Item[p.Field],
			})
		}
		slate, err := mmr.Rerank(mmr.Request{
			Candidates: cands,
			Sim:        mmr.SimilarityFromDistance(op, p.Field),
			Lambda:     p.Diversify.Lambda,
			N:          p.Diversify.TargetSize,
		})
		if err != nil {
			return nil, err
		}
		rows = make([]cquery.TopKResult, 0, len(slate))
		for _, pick := range slate {
			rows = append(rows, cquery.TopKResult{Item: pick.Item, Distance: pick.Distance})
		}
	}
	out := make([]types.Item, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Item)
	}
	if len(p.Project) > 0 {
		project(out, p.Project)
	}
	return &Result{Rows: out}, nil
}

func reverse(items []types.Item) {
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
}

type aggregateGroup struct {
	key    string
	values []types.AttributeValue
	counts []int
}

func aggregateRows(items []types.Item, groupBy []string, aggs []AggregateExpr, limit int) *Result {
	groups := make(map[string]*aggregateGroup)
	for _, it := range items {
		values := groupValues(it, groupBy)
		key := groupKey(values)
		g, ok := groups[key]
		if !ok {
			g = &aggregateGroup{
				key:    key,
				values: values,
				counts: make([]int, len(aggs)),
			}
			groups[key] = g
		}
		for i, agg := range aggs {
			if countAggregate(agg, it) {
				g.counts[i]++
			}
		}
	}

	ordered := make([]*aggregateGroup, 0, len(groups))
	for _, g := range groups {
		ordered = append(ordered, g)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].key < ordered[j].key
	})
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}

	rows := make([]types.Item, 0, len(ordered))
	for _, g := range ordered {
		row := make(types.Item, len(groupBy)+len(aggs))
		for i, col := range groupBy {
			row[col] = g.values[i]
		}
		for i, agg := range aggs {
			row[agg.OutputName] = types.AttributeValue{T: types.AttrN, N: strconv.Itoa(g.counts[i])}
		}
		rows = append(rows, row)
	}
	return &Result{Rows: rows}
}

func groupValues(it types.Item, groupBy []string) []types.AttributeValue {
	values := make([]types.AttributeValue, 0, len(groupBy))
	for _, col := range groupBy {
		av, ok := it[col]
		if !ok {
			av = types.AttributeValue{T: types.AttrNull}
		}
		values = append(values, av)
	}
	return values
}

func countAggregate(agg AggregateExpr, it types.Item) bool {
	if strings.EqualFold(agg.Func, "COUNT") {
		if agg.Column == "*" {
			return true
		}
		av, ok := it[agg.Column]
		return ok && av.T != types.AttrNull
	}
	return false
}

func groupKey(values []types.AttributeValue) string {
	var b strings.Builder
	for _, av := range values {
		writeAttrStableKey(&b, av)
		b.WriteByte('|')
	}
	return b.String()
}

func joinItems(leftAlias string, left types.Item, rightAlias string, right types.Item) types.Item {
	out := make(types.Item, len(left)+len(right))
	for k, v := range left {
		out[leftAlias+"."+k] = v
	}
	for k, v := range right {
		out[rightAlias+"."+k] = v
	}
	return out
}

func joinAttrKey(av types.AttributeValue) string {
	var b strings.Builder
	writeAttrStableKey(&b, av)
	return b.String()
}

func writeAttrStableKey(b *strings.Builder, av types.AttributeValue) {
	b.WriteString(strconv.Itoa(int(av.T)))
	b.WriteByte(':')
	switch av.T {
	case types.AttrNull:
		return
	case types.AttrS:
		b.WriteString(strconv.Quote(av.S))
	case types.AttrN:
		b.WriteString(strconv.Quote(av.N))
	case types.AttrB:
		b.WriteString(strconv.Quote(string(av.B)))
	case types.AttrBOOL:
		if av.BOOL {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	case types.AttrSS:
		writeStringListGroupKey(b, sortedStringCopy(av.SS))
	case types.AttrNS:
		writeStringListGroupKey(b, sortedStringCopy(av.NS))
	case types.AttrBS:
		bs := append([][]byte(nil), av.BS...)
		sort.Slice(bs, func(i, j int) bool { return bytesCompare(bs[i], bs[j]) < 0 })
		for _, v := range bs {
			b.WriteString(strconv.Quote(string(v)))
			b.WriteByte(',')
		}
	case types.AttrL:
		for _, v := range av.L {
			writeAttrStableKey(b, v)
			b.WriteByte(',')
		}
	case types.AttrM:
		keys := make([]string, 0, len(av.M))
		for k := range av.M {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString(strconv.Quote(k))
			b.WriteByte('=')
			writeAttrStableKey(b, av.M[k])
			b.WriteByte(',')
		}
	case types.AttrVec:
		for _, v := range av.Vec {
			b.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
			b.WriteByte(',')
		}
	}
}

func sortedStringCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func writeStringListGroupKey(b *strings.Builder, values []string) {
	for _, v := range values {
		b.WriteString(strconv.Quote(v))
		b.WriteByte(',')
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
