package sql

import (
	"errors"
	"fmt"
	"strings"

	_ "unsafe" // for go:linkname placeholder; kept inert today

	cquery "github.com/CefasDb/cefasdb/internal/core/query"
	"github.com/CefasDb/cefasdb/internal/spatial"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// Catalog is the planner's view of the table catalog. The wider
// catalog package implements it; tests can fake it.
type Catalog interface {
	Describe(name string) (types.TableDescriptor, error)
}

// PlanStmt compiles `stmt` into a Plan against `cat`. The planner's
// job is to map a parsed statement to the lowest-cost storage
// operation; it refuses queries that would require a full table scan
// unless the caller has already opted in elsewhere.
func PlanStmt(stmt Stmt, cat Catalog) (Plan, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return planCreateTable(s)
	case *DropTableStmt:
		return &PlanDropTable{Name: s.Table}, nil
	case *InsertStmt:
		return planInsert(s, cat)
	case *UpdateStmt:
		return planUpdate(s, cat)
	case *DeleteStmt:
		return planDelete(s, cat)
	case *SelectStmt:
		return planSelect(s, cat)
	case *CreateMaterializedViewStmt:
		return planCreateMaterializedView(s)
	case *DropMaterializedViewStmt:
		return &PlanDropMaterializedView{Name: s.Name}, nil
	case *CreateServiceLevelStmt:
		return &PlanCreateServiceLevel{Descriptor: serviceLevelFromSpec(s.Name, s.Spec)}, nil
	case *AlterServiceLevelStmt:
		return &PlanAlterServiceLevel{Descriptor: serviceLevelFromSpec(s.Name, s.Spec)}, nil
	case *DropServiceLevelStmt:
		return &PlanDropServiceLevel{Name: s.Name}, nil
	case *ListServiceLevelsStmt:
		return &PlanListServiceLevels{}, nil
	case *CreateGlobalIndexStmt:
		return &PlanCreateGlobalIndex{Descriptor: types.GlobalIndexDescriptor{
			Name:              s.Name,
			BaseTable:         s.BaseTable,
			IndexedColumn:     s.IndexedColumn,
			ProjectedColumns:  append([]string(nil), s.Projected...),
			Shards:            s.Shards,
			ReplicationFactor: s.ReplicationFactor,
		}}, nil
	case *DropGlobalIndexStmt:
		return &PlanDropGlobalIndex{Name: s.Name}, nil
	}
	return nil, fmt.Errorf("unsupported statement type %T", stmt)
}

func serviceLevelFromSpec(name string, spec ServiceLevelSpec) types.ServiceLevelDescriptor {
	return types.ServiceLevelDescriptor{
		Name:           name,
		Shares:         spec.Shares,
		MaxInFlight:    spec.MaxInFlight,
		MaxRowsPerSec:  spec.MaxRowsPerSec,
		MaxBytesPerSec: spec.MaxBytesPerSec,
	}
}

func planCreateMaterializedView(s *CreateMaterializedViewStmt) (*PlanCreateMaterializedView, error) {
	mv := types.MaterializedViewDescriptor{
		Name:                s.Name,
		BaseTable:           s.BaseTable,
		KeySchema:           types.KeySchema{PK: s.PK, SK: s.SK},
		ProjectedAttributes: append([]string(nil), s.Projected...),
		RefreshPolicy: types.RefreshPolicy{
			Mode:            types.RefreshMode(s.Refresh.Mode),
			IntervalSeconds: s.Refresh.IntervalSeconds,
		},
	}
	return &PlanCreateMaterializedView{Descriptor: mv}, nil
}

func planCreateTable(s *CreateTableStmt) (*PlanCreateTable, error) {
	defs := make([]types.AttributeDefinition, 0, len(s.AttributeDefinitions))
	for _, def := range s.AttributeDefinitions {
		defs = append(defs, types.AttributeDefinition{
			Name:             def.Name,
			Type:             strings.ToUpper(def.Type),
			VectorDimensions: def.VectorDimensions,
		})
	}
	return &PlanCreateTable{
		Descriptor: types.TableDescriptor{
			Name:                 s.Table,
			KeySchema:            types.KeySchema{PK: s.PK, SK: s.SK},
			StorageClass:         s.StorageClass,
			AttributeDefinitions: defs,
		},
	}, nil
}

func planInsert(s *InsertStmt, cat Catalog) (*PlanPutItem, error) {
	td, err := cat.Describe(s.Table)
	if err != nil {
		return nil, err
	}
	item := make(types.Item, len(s.Columns))
	for i, col := range s.Columns {
		v, err := evalLiteral(s.Values[i])
		if err != nil {
			return nil, fmt.Errorf("INSERT %q: %w", col, err)
		}
		item[col] = v
	}
	if _, ok := item[td.KeySchema.PK]; !ok {
		return nil, fmt.Errorf("INSERT missing PK column %q", td.KeySchema.PK)
	}
	if td.KeySchema.SK != "" {
		if _, ok := item[td.KeySchema.SK]; !ok {
			return nil, fmt.Errorf("INSERT missing SK column %q", td.KeySchema.SK)
		}
	}
	// Refine IF NOT EXISTS — the parser stamped ColumnRef "*" because
	// it hadn't seen the schema yet; the planner picks the PK column
	// so attribute_not_exists evaluates correctly on the snapshot
	// prior item.
	cond := refinePKShortcut(s.If, td.KeySchema.PK)
	return &PlanPutItem{Table: s.Table, Item: item, Descriptor: td, If: cond, Returning: s.Returning}, nil
}

// refinePKShortcut replaces the placeholder ColumnRef "*" in an
// "IF NOT EXISTS" / "IF EXISTS" shortcut with the table's PK column.
func refinePKShortcut(e Expr, pk string) Expr {
	if fn, ok := e.(*FuncCall); ok && len(fn.Args) == 1 {
		if cr, isCol := fn.Args[0].(*ColumnRef); isCol && cr.Name == "*" {
			cr.Name = pk
		}
	}
	return e
}

func planUpdate(s *UpdateStmt, cat Catalog) (*PlanUpdate, error) {
	td, err := cat.Describe(s.Table)
	if err != nil {
		return nil, err
	}
	key, err := extractRowKey(s.Where, td.KeySchema)
	if err != nil {
		return nil, fmt.Errorf("UPDATE: %w", err)
	}
	for _, a := range s.Assignments {
		if a.Column == td.KeySchema.PK || a.Column == td.KeySchema.SK {
			return nil, fmt.Errorf("UPDATE cannot modify key column %q", a.Column)
		}
		if isCounterAttribute(td, a.Column) {
			return nil, fmt.Errorf("UPDATE cannot modify counter column %q; use AtomicUpdate", a.Column)
		}
	}
	cond := refinePKShortcut(s.If, td.KeySchema.PK)
	return &PlanUpdate{Table: s.Table, Key: key, Actions: s.Assignments, Descriptor: td, If: cond, Returning: s.Returning}, nil
}

func isCounterAttribute(td types.TableDescriptor, name string) bool {
	for _, def := range td.AttributeDefinitions {
		if def.Name == name && types.IsCounterAttributeType(def.Type) {
			return true
		}
	}
	return false
}

func planDelete(s *DeleteStmt, cat Catalog) (*PlanDelete, error) {
	td, err := cat.Describe(s.Table)
	if err != nil {
		return nil, err
	}
	key, err := extractRowKey(s.Where, td.KeySchema)
	if err != nil {
		return nil, fmt.Errorf("DELETE: %w", err)
	}
	cond := refinePKShortcut(s.If, td.KeySchema.PK)
	return &PlanDelete{Table: s.Table, Key: key, Descriptor: td, If: cond, Returning: s.Returning}, nil
}

// planSelect picks among GetItem, Query (primary or GSI), and
// SpatialQuery based on the predicate shape.
func planSelect(s *SelectStmt, cat Catalog) (Plan, error) {
	td, err := cat.Describe(s.Table)
	if err != nil {
		return nil, err
	}
	if s.OrderANN {
		if s.Limit <= 0 {
			return nil, fmt.Errorf("ANN ORDER BY requires LIMIT")
		}
		if dim, ok := vectorDimension(td, s.OrderBy); ok && dim != len(s.ANNTarget) {
			return nil, fmt.Errorf("ANN target dimension %d != declared dimension %d for %q", len(s.ANNTarget), dim, s.OrderBy)
		}
		if s.Diversify != nil {
			if !strings.EqualFold(s.Diversify.Method, "mmr") {
				return nil, fmt.Errorf("unsupported DIVERSIFY method %q", s.Diversify.Method)
			}
			if s.Diversify.Lambda < 0 || s.Diversify.Lambda > 1 {
				return nil, fmt.Errorf("DIVERSIFY lambda %.3f out of [0,1]", s.Diversify.Lambda)
			}
			if s.Diversify.TargetSize <= 0 {
				return nil, fmt.Errorf("DIVERSIFY target size must be > 0")
			}
			if s.Diversify.TargetSize > s.Limit {
				return nil, fmt.Errorf("DIVERSIFY target size %d cannot exceed LIMIT %d", s.Diversify.TargetSize, s.Limit)
			}
		}
		plan := &PlanANN{
			Table:      s.Table,
			Field:      s.OrderBy,
			Target:     types.AttributeValue{T: types.AttrVec, Vec: append([]float64(nil), s.ANNTarget...)},
			Limit:      s.Limit,
			Project:    s.Columns,
			Descriptor: td,
			Predicate:  s.Where,
			Diversify:  s.Diversify,
		}
		if s.Where != nil {
			plan.Filter = chooseANNFilterPlan(s.Where, td)
		}
		return plan, nil
	}

	if s.AllowScan {
		if s.IndexName != "" {
			return nil, fmt.Errorf("ALLOW SCAN cannot be combined with USE INDEX")
		}
		if s.OrderBy != "" {
			return nil, fmt.Errorf("scalar ORDER BY with ALLOW SCAN is not supported yet")
		}
		if !s.Count && s.Limit <= 0 {
			return nil, fmt.Errorf("ALLOW SCAN requires LIMIT for row-returning SELECT")
		}
		return &PlanScan{
			Table:      s.Table,
			Limit:      s.Limit,
			Project:    s.Columns,
			Predicate:  s.Where,
			Descriptor: td,
			Count:      s.Count,
		}, nil
	}

	// Spatial path: WHERE includes a ST_Within / ST_DWithin call.
	if spq, indexName, ok := extractSpatialQuery(s.Where, td, s.IndexName); ok {
		return &PlanSpatial{
			Table:      s.Table,
			IndexName:  indexName,
			Query:      spq,
			Project:    s.Columns,
			Descriptor: td,
		}, nil
	}

	// GSI / primary range path.
	pkAttr := td.KeySchema.PK
	skAttr := td.KeySchema.SK
	if s.IndexName != "" {
		// Index attribute resolution. The GSI declaration tells us
		// which attribute to look for in the WHERE clause.
		gsi, ok := findGSI(td, s.IndexName)
		if !ok {
			return nil, fmt.Errorf("table %q has no index %q", s.Table, s.IndexName)
		}
		pkAttr = gsi.KeySchema.PK
		skAttr = gsi.KeySchema.SK
	}

	pkVal, skLow, skHigh, hasExactSK, postFilter, err := extractAccessPath(s.Where, pkAttr, skAttr)
	if err != nil {
		return nil, err
	}

	// Single-row GetItem when PK + exact SK and no post-filter / no
	// LIMIT / no ORDER BY / no GSI.
	if s.IndexName == "" && hasExactSK && s.Limit == 0 && s.OrderBy == "" && postFilter == nil {
		key := types.Item{pkAttr: pkVal, skAttr: skLow}
		if isMinimalRowPredicate(s.Where, pkAttr, skAttr) {
			return &PlanGetItem{Table: s.Table, Key: key}, nil
		}
	}

	limit := s.Limit
	if limit == 0 {
		limit = 0 // unlimited; executor passes through
	}
	return &PlanQuery{
		Table:      s.Table,
		IndexName:  s.IndexName,
		PKValue:    pkVal,
		SKLow:      skLow,
		SKHigh:     skHigh,
		Limit:      limit,
		Project:    s.Columns,
		OrderDesc:  s.OrderDesc,
		Descriptor: td,
		PostFilter: postFilter,
		Count:      s.Count,
	}, nil
}

// chooseANNFilterPlan inspects the WHERE clause of an ANN query and
// produces the planner-side strategy description: which index (if
// any) backs the cohort, the estimated selectivity, and the chosen
// strategy.
//
// Predicate pushdown is rule-based: equality / IN / range comparisons
// against indexed columns (the table PK, any GSI PK) can be folded
// into a cohort intersection; everything else stays as a post-filter
// for the executor.
func chooseANNFilterPlan(where Expr, td types.TableDescriptor) cquery.ANNFilterPlan {
	plan := cquery.ANNFilterPlan{
		PredicateDescription: describePredicate(where),
	}
	col, indexName, indexed := indexedPredicateColumn(where, td)
	plan.IndexedColumn = col
	plan.IndexUsed = indexName

	sel := estimateSelectivity(where, td)
	plan.Selectivity.Predicted = sel

	plan.Strategy = cquery.ChooseStrategy(sel, indexed)
	switch plan.Strategy {
	case cquery.StrategyFilterFirst:
		plan.OverscanFactor = 1
	default:
		plan.OverscanFactor = cquery.OverscanFactor(sel)
	}
	return plan
}

// indexedPredicateColumn walks the WHERE conjuncts and returns the
// first column reference that the table has an index on, plus the
// resolved index name. The caller uses the name in EXPLAIN.
//
// Returned `indexed` is true only when at least one conjunct binds an
// indexed attribute to a literal via equality / IN / range. Without
// such a binding the filter-first strategy has nothing to intersect
// and the planner falls back to overscan.
func indexedPredicateColumn(where Expr, td types.TableDescriptor) (col, idx string, indexed bool) {
	for _, c := range flattenAnd(where) {
		name, ok := predicateColumn(c)
		if !ok {
			continue
		}
		if name == td.KeySchema.PK {
			return name, "primary", true
		}
		for _, gsi := range td.GSIs {
			if gsi.KeySchema.PK == name {
				return name, gsi.Name, true
			}
		}
	}
	return "", "", false
}

// predicateColumn extracts the column name from a single conjunct,
// when the conjunct is one of the pushdownable shapes (equality,
// range comparison, BETWEEN, or IN-style equality chain).
func predicateColumn(e Expr) (string, bool) {
	switch n := e.(type) {
	case *BinaryExpr:
		switch n.Op {
		case BinEq, BinNeq, BinLt, BinLte, BinGt, BinGte:
			if cr, ok := n.Left.(*ColumnRef); ok {
				return cr.Name, true
			}
			if cr, ok := n.Right.(*ColumnRef); ok {
				return cr.Name, true
			}
		}
	case *BetweenExpr:
		if cr, ok := n.Value.(*ColumnRef); ok {
			return cr.Name, true
		}
	}
	return "", false
}

// estimateSelectivity returns a rule-of-thumb fraction of rows the
// WHERE clause is expected to retain. We don't carry per-attribute
// histograms today; the heuristic is:
//
//   - equality on any column: 0.10
//   - IN-style equality chain (OR of equalities on the same column):
//     min(0.10 * N, 0.80)
//   - range comparison: 0.30
//   - BETWEEN: 0.20
//
// Conjuncts multiply (independence assumption). Unknown shapes
// contribute 1.0 so they don't accidentally drive selectivity to 0.
//
// The estimate is bounded to [0.001, 1.0] so the overscan factor
// stays finite.
func estimateSelectivity(where Expr, _ types.TableDescriptor) float64 {
	if where == nil {
		return 1.0
	}
	sel := 1.0
	for _, c := range flattenAnd(where) {
		sel *= conjunctSelectivity(c)
	}
	if sel < 0.001 {
		sel = 0.001
	}
	if sel > 1.0 {
		sel = 1.0
	}
	return sel
}

func conjunctSelectivity(e Expr) float64 {
	switch n := e.(type) {
	case *BinaryExpr:
		switch n.Op {
		case BinEq:
			return 0.10
		case BinNeq:
			return 0.90
		case BinLt, BinLte, BinGt, BinGte:
			return 0.30
		case BinOr:
			// OR-of-equalities on the same column is the closest we
			// get to an IN list. Conservative: 0.10 per disjunct,
			// capped at 0.80.
			n2 := countDisjuncts(n)
			s := 0.10 * float64(n2)
			if s > 0.80 {
				s = 0.80
			}
			return s
		case BinAnd:
			return conjunctSelectivity(n.Left) * conjunctSelectivity(n.Right)
		}
	case *BetweenExpr:
		return 0.20
	case *NotExpr:
		return 1 - conjunctSelectivity(n.Inner)
	}
	return 1.0
}

func countDisjuncts(e Expr) int {
	bin, ok := e.(*BinaryExpr)
	if !ok || bin.Op != BinOr {
		return 1
	}
	return countDisjuncts(bin.Left) + countDisjuncts(bin.Right)
}

// describePredicate renders a short, human-readable form of the
// WHERE clause for EXPLAIN output. It is best-effort: nodes the
// renderer does not know about fall through as "<expr>".
func describePredicate(e Expr) string {
	switch n := e.(type) {
	case nil:
		return ""
	case *BinaryExpr:
		return describePredicate(n.Left) + " " + binOpString(n.Op) + " " + describePredicate(n.Right)
	case *BetweenExpr:
		return describePredicate(n.Value) + " BETWEEN " + describePredicate(n.Lo) + " AND " + describePredicate(n.Hi)
	case *NotExpr:
		return "NOT " + describePredicate(n.Inner)
	case *ColumnRef:
		return n.Name
	case *Literal:
		if n.Kind == LitString {
			return "'" + n.Value + "'"
		}
		return n.Value
	case *FuncCall:
		args := ""
		for i, a := range n.Args {
			if i > 0 {
				args += ", "
			}
			args += describePredicate(a)
		}
		return n.Name + "(" + args + ")"
	}
	return "<expr>"
}

func binOpString(op BinOp) string {
	switch op {
	case BinAnd:
		return "AND"
	case BinOr:
		return "OR"
	case BinEq:
		return "="
	case BinNeq:
		return "!="
	case BinLt:
		return "<"
	case BinLte:
		return "<="
	case BinGt:
		return ">"
	case BinGte:
		return ">="
	}
	return "?"
}

func vectorDimension(td types.TableDescriptor, field string) (int, bool) {
	for _, def := range td.AttributeDefinitions {
		if def.Name == field && strings.EqualFold(def.Type, "V") && def.VectorDimensions > 0 {
			return def.VectorDimensions, true
		}
	}
	return 0, false
}

// ---------- WHERE-clause analysis ----------

// extractAccessPath walks the WHERE tree, peels off predicates the
// storage layer can consume (PK equality + SK range), and returns the
// remainder as a post-filter expression the executor evaluates on
// each candidate row.
//
//   - pkVal: required equality on the PK attribute
//   - skLow / skHigh: optional SK range
//   - hasExactSK: true when the predicate was sk = literal
//   - postFilter: AND of every clause we couldn't push down (nil if all
//     clauses were consumed)
//
// begins_with(sk, 'pref') is pushed down to a SK prefix range when it
// is the sole SK predicate; otherwise it stays in the post-filter.
func extractAccessPath(where Expr, pkAttr, skAttr string) (pkVal, skLow, skHigh types.AttributeValue, hasExactSK bool, postFilter Expr, err error) {
	if where == nil {
		err = errors.New("SELECT without WHERE would scan the whole table; refusing")
		return
	}
	clauses := flattenAnd(where)
	var residual []Expr
	for _, c := range clauses {
		switch e := c.(type) {
		case *BinaryExpr:
			col, lit, ok := extractColEqLit(e, pkAttr)
			if ok && e.Op == BinEq {
				pkVal = lit
				continue
			}
			if col == skAttr && skAttr != "" {
				v, errLit := evalLiteral(litFromOperand(e, col))
				if errLit != nil {
					err = errLit
					return
				}
				switch e.Op {
				case BinEq:
					skLow = v
					skHigh = nextLex(v)
					hasExactSK = true
				case BinGte, BinGt:
					skLow = v
				case BinLte:
					skHigh = nextLex(v)
				case BinLt:
					skHigh = v
				default:
					residual = append(residual, c)
				}
				continue
			}
			// Non-key column predicate: keep as post-filter.
			residual = append(residual, c)
		case *BetweenExpr:
			cr, isCol := e.Value.(*ColumnRef)
			if isCol && cr.Name == skAttr {
				lo, errLit := evalLiteral(e.Lo)
				if errLit != nil {
					err = errLit
					return
				}
				hi, errLit := evalLiteral(e.Hi)
				if errLit != nil {
					err = errLit
					return
				}
				skLow = lo
				skHigh = nextLex(hi)
				continue
			}
			residual = append(residual, c)
		case *FuncCall:
			// begins_with(<sk>, '<pref>') as the only SK predicate
			// pushes down. Other functions become post-filters.
			if strings.EqualFold(e.Name, "BEGINS_WITH") && len(e.Args) == 2 {
				if cr, isCol := e.Args[0].(*ColumnRef); isCol && cr.Name == skAttr {
					if lit, isLit := e.Args[1].(*Literal); isLit && lit.Kind == LitString {
						skLow = types.AttributeValue{T: types.AttrS, S: lit.Value}
						skHigh = types.AttributeValue{T: types.AttrS, S: lit.Value + "\xff"}
						continue
					}
				}
			}
			residual = append(residual, c)
		default:
			residual = append(residual, c)
		}
	}
	if pkVal.T == types.AttrNull {
		err = fmt.Errorf("WHERE must equate %q to a value", pkAttr)
		return
	}
	if len(residual) > 0 {
		postFilter = residual[0]
		for i := 1; i < len(residual); i++ {
			postFilter = &BinaryExpr{Op: BinAnd, Left: postFilter, Right: residual[i]}
		}
	}
	return
}

func extractColEqLit(e *BinaryExpr, pkAttr string) (col string, val types.AttributeValue, ok bool) {
	if cr, isCol := e.Left.(*ColumnRef); isCol {
		if v, err := evalLiteral(e.Right); err == nil {
			return cr.Name, v, cr.Name == pkAttr
		}
		return cr.Name, types.AttributeValue{}, false
	}
	if cr, isCol := e.Right.(*ColumnRef); isCol {
		if v, err := evalLiteral(e.Left); err == nil {
			return cr.Name, v, cr.Name == pkAttr
		}
	}
	return "", types.AttributeValue{}, false
}

func litFromOperand(e *BinaryExpr, col string) Expr {
	if cr, isCol := e.Left.(*ColumnRef); isCol && cr.Name == col {
		return e.Right
	}
	return e.Left
}

// flattenAnd returns the conjuncts of a top-level AND tree. OR-trees
// are returned as a single expression so the caller errors out (we
// only plan conjunctive predicates today).
func flattenAnd(e Expr) []Expr {
	bin, ok := e.(*BinaryExpr)
	if !ok || bin.Op != BinAnd {
		return []Expr{e}
	}
	return append(flattenAnd(bin.Left), flattenAnd(bin.Right)...)
}

func extractRowKey(where Expr, ks types.KeySchema) (types.Item, error) {
	clauses := flattenAnd(where)
	out := types.Item{}
	for _, c := range clauses {
		bin, ok := c.(*BinaryExpr)
		if !ok || bin.Op != BinEq {
			return nil, fmt.Errorf("only equality on key columns supported, got %T", c)
		}
		col, v, _ := extractColEqLit(bin, ks.PK)
		if col != ks.PK && col != ks.SK {
			return nil, fmt.Errorf("WHERE references non-key column %q", col)
		}
		// Reuse extractColEqLit's parse: if it didn't yield a literal
		// (col=col equality), reject.
		if v.T == 0 {
			parsed, err := evalLiteral(litFromOperand(bin, col))
			if err != nil {
				return nil, err
			}
			v = parsed
		}
		out[col] = v
	}
	if _, ok := out[ks.PK]; !ok {
		return nil, fmt.Errorf("WHERE must pin PK %q", ks.PK)
	}
	if ks.SK != "" {
		if _, ok := out[ks.SK]; !ok {
			return nil, fmt.Errorf("WHERE must pin SK %q for tables with a sort key", ks.SK)
		}
	}
	return out, nil
}

// isMinimalRowPredicate reports whether `where` only constrains the
// PK and SK columns with equality — the GetItem-eligible shape.
func isMinimalRowPredicate(where Expr, pkAttr, skAttr string) bool {
	for _, c := range flattenAnd(where) {
		bin, ok := c.(*BinaryExpr)
		if !ok || bin.Op != BinEq {
			return false
		}
		col, _, _ := extractColEqLit(bin, pkAttr)
		if col != pkAttr && col != skAttr {
			return false
		}
	}
	return true
}

// extractSpatialQuery looks for ST_Within / ST_DWithin in the WHERE
// tree. When present, returns the constructed SpatialQuery and the
// resolved spatial index name.
func extractSpatialQuery(where Expr, td types.TableDescriptor, hintIndex string) (pebble.SpatialQuery, string, bool) {
	if where == nil {
		return pebble.SpatialQuery{}, "", false
	}
	for _, c := range flattenAnd(where) {
		fn, ok := c.(*FuncCall)
		if !ok {
			continue
		}
		switch fn.Name {
		case "ST_WITHIN":
			if len(fn.Args) != 2 {
				return pebble.SpatialQuery{}, "", false
			}
			box, ok := bboxFromExpr(fn.Args[1])
			if !ok {
				return pebble.SpatialQuery{}, "", false
			}
			idx := resolveSpatialIndex(td, hintIndex, fn.Args[0])
			return pebble.SpatialQuery{BBox: &box}, idx, true
		case "ST_DWITHIN":
			if len(fn.Args) != 3 {
				return pebble.SpatialQuery{}, "", false
			}
			pt, ok := pointFromExpr(fn.Args[1])
			if !ok {
				return pebble.SpatialQuery{}, "", false
			}
			meters, err := numberFromExpr(fn.Args[2])
			if err != nil {
				return pebble.SpatialQuery{}, "", false
			}
			idx := resolveSpatialIndex(td, hintIndex, fn.Args[0])
			return pebble.SpatialQuery{Radius: &pebble.RadiusQuery{Lat: pt[0], Lon: pt[1], Meters: meters}}, idx, true
		}
	}
	return pebble.SpatialQuery{}, "", false
}

func bboxFromExpr(e Expr) (spatial.BBox, bool) {
	fn, ok := e.(*FuncCall)
	if !ok || fn.Name != "BBOX" || len(fn.Args) != 4 {
		return spatial.BBox{}, false
	}
	minLat, err1 := numberFromExpr(fn.Args[0])
	minLon, err2 := numberFromExpr(fn.Args[1])
	maxLat, err3 := numberFromExpr(fn.Args[2])
	maxLon, err4 := numberFromExpr(fn.Args[3])
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return spatial.BBox{}, false
	}
	return spatial.BBox{MinLat: minLat, MinLon: minLon, MaxLat: maxLat, MaxLon: maxLon}, true
}

func pointFromExpr(e Expr) ([2]float64, bool) {
	fn, ok := e.(*FuncCall)
	if !ok || fn.Name != "POINT" || len(fn.Args) != 2 {
		return [2]float64{}, false
	}
	lat, err1 := numberFromExpr(fn.Args[0])
	lon, err2 := numberFromExpr(fn.Args[1])
	if err1 != nil || err2 != nil {
		return [2]float64{}, false
	}
	return [2]float64{lat, lon}, true
}

func numberFromExpr(e Expr) (float64, error) {
	lit, ok := e.(*Literal)
	if !ok || lit.Kind != LitNumber {
		return 0, fmt.Errorf("expected numeric literal")
	}
	var f float64
	_, err := fmt.Sscanf(lit.Value, "%f", &f)
	if err != nil {
		return 0, err
	}
	return f, nil
}

func resolveSpatialIndex(td types.TableDescriptor, hint string, locExpr Expr) string {
	if hint != "" {
		return hint
	}
	// No hint: pick the first geohash index whose first attribute is
	// referenced by the location column (when present). Otherwise
	// return the first spatial index.
	colName := ""
	if cr, ok := locExpr.(*ColumnRef); ok {
		colName = cr.Name
	}
	for _, si := range td.SpatialIndexes {
		if len(si.Attributes) > 0 && (colName == "" || si.Attributes[0] == colName) {
			return si.Name
		}
	}
	if len(td.SpatialIndexes) > 0 {
		return td.SpatialIndexes[0].Name
	}
	return ""
}

func findGSI(td types.TableDescriptor, name string) (types.GSIDescriptor, bool) {
	for _, g := range td.GSIs {
		if g.Name == name {
			return g, true
		}
	}
	return types.GSIDescriptor{}, false
}

// nextLex returns the SK upper bound for an inclusive comparison —
// the storage layer uses [low, high) so we bump the supplied value
// one byte higher on the canonical text form.
func nextLex(v types.AttributeValue) types.AttributeValue {
	switch v.T {
	case types.AttrS:
		return types.AttributeValue{T: types.AttrS, S: v.S + "\x00"}
	case types.AttrN:
		return types.AttributeValue{T: types.AttrN, N: v.N + "\x00"}
	}
	return v
}

// evalLiteral resolves a SQL literal expression to a cefas
// AttributeValue. Numbers stay as canonical text to preserve
// arbitrary precision.
func evalLiteral(e Expr) (types.AttributeValue, error) {
	switch lit := e.(type) {
	case *Literal:
		switch lit.Kind {
		case LitString:
			return types.AttributeValue{T: types.AttrS, S: lit.Value}, nil
		case LitNumber:
			return types.AttributeValue{T: types.AttrN, N: strings.TrimSpace(lit.Value)}, nil
		case LitBool:
			return types.AttributeValue{T: types.AttrBOOL, BOOL: lit.Bool}, nil
		case LitNull:
			return types.AttributeValue{T: types.AttrNull}, nil
		}
		return types.AttributeValue{}, fmt.Errorf("unsupported literal kind %d", lit.Kind)
	case *VectorLiteral:
		return types.AttributeValue{T: types.AttrVec, Vec: append([]float64(nil), lit.Values...)}, nil
	}
	return types.AttributeValue{}, fmt.Errorf("expected literal, got %T", e)
}
