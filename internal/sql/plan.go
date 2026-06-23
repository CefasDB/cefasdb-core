package sql

import (
	"strconv"

	cquery "github.com/CefasDb/cefasdb/internal/core/query"
	"github.com/CefasDb/cefasdb/internal/spatial"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// Plan is the executor input — a self-contained description of what
// the storage layer should do. Concrete types document the planner
// branches.
type Plan interface {
	plan()
}

// PlanCreateTable is a CREATE TABLE.
type PlanCreateTable struct {
	Descriptor types.TableDescriptor
}

// PlanDropTable is a DROP TABLE.
type PlanDropTable struct {
	Name string
}

// PlanGetItem is the primary-key lookup path.
type PlanGetItem struct {
	Table string
	Key   types.Item
}

// PlanQuery is a primary or GSI range scan.
type PlanQuery struct {
	Table      string
	IndexName  string // "" → primary
	PKValue    types.AttributeValue
	SKLow      types.AttributeValue // T == AttrNull means open low
	SKHigh     types.AttributeValue // T == AttrNull means open high
	Limit      int
	Project    []string // nil → all attributes
	OrderDesc  bool
	Descriptor types.TableDescriptor // resolved by planner so executor doesn't reread it
	// PostFilter, when non-nil, is evaluated against each row the
	// iterator yields. Used for predicates the planner can't push
	// into the storage range scan (begins_with on non-key cols,
	// contains, attribute_*, size).
	PostFilter Expr
	// Count = true means the executor returns AffectedRows = N
	// instead of materialising row data.
	Count bool
}

// PlanScan is an explicit scan-backed SELECT. The planner only emits
// it when the statement opts in with ALLOW SCAN, keeping accidental
// full-table reads out of the default SQL path.
type PlanScan struct {
	Table      string
	Limit      int
	Project    []string
	Predicate  Expr
	Descriptor types.TableDescriptor
	Count      bool
}

// PlanSpatial is a geohash / Z-order / radius scan.
type PlanSpatial struct {
	Table      string
	IndexName  string
	Query      pebble.SpatialQuery
	Project    []string
	Descriptor types.TableDescriptor
}

// PlanANN ranks table rows by a vector distance resolved from an ann
// index in the execution environment.
//
// When the SELECT carries a WHERE clause, the planner populates
// Filter so the executor can pick between the two hybrid strategies
// (filter-first vs ANN-first-with-overscan). Filter.Strategy ==
// StrategyUnset means there was no WHERE clause and the executor
// runs the plain ANN scan.
type PlanANN struct {
	Table      string
	Field      string
	Target     types.AttributeValue
	Limit      int
	Project    []string
	Descriptor types.TableDescriptor
	// Predicate is the WHERE expression the executor must enforce on
	// each ANN candidate. nil when the SELECT has no WHERE clause.
	Predicate Expr
	// Filter carries the planner's chosen hybrid strategy plus the
	// selectivity estimate. The executor updates Filter.Selectivity
	// with the observed value after the scan.
	Filter cquery.ANNFilterPlan
	// Diversify optionally applies a post-rank MMR pass over the ANN
	// candidate set before projection.
	Diversify *DiversifyClause
}

// PlanPutItem is INSERT INTO ... VALUES (...) [IF expr]
// [RETURNING mode].
type PlanPutItem struct {
	Table      string
	Item       types.Item
	Descriptor types.TableDescriptor
	If         Expr
	Returning  ReturningMode
}

// PlanUpdate is the single-item UPDATE path. The executor reads the
// prior row, applies each Action in order, and writes the merged
// result back through storage.PutItemWith so GSI + LSI + spatial +
// TTL maintenance stay atomic.
type PlanUpdate struct {
	Table      string
	Key        types.Item
	Actions    []Assignment
	Descriptor types.TableDescriptor
	If         Expr
	Returning  ReturningMode
}

// PlanDelete is DELETE WHERE pk = ... [AND sk = ...] [IF expr]
// [RETURNING OLD].
type PlanDelete struct {
	Table      string
	Key        types.Item
	Descriptor types.TableDescriptor
	If         Expr
	Returning  ReturningMode
}

// PlanCreateMaterializedView is CREATE MATERIALIZED VIEW.
type PlanCreateMaterializedView struct {
	Descriptor types.MaterializedViewDescriptor
}

// PlanDropMaterializedView is DROP MATERIALIZED VIEW.
type PlanDropMaterializedView struct {
	Name string
}

// PlanCreateServiceLevel is CREATE SERVICE LEVEL.
type PlanCreateServiceLevel struct {
	Descriptor types.ServiceLevelDescriptor
}

// PlanAlterServiceLevel is ALTER SERVICE LEVEL.
type PlanAlterServiceLevel struct {
	Descriptor types.ServiceLevelDescriptor
}

// PlanDropServiceLevel is DROP SERVICE LEVEL.
type PlanDropServiceLevel struct {
	Name string
}

// PlanListServiceLevels is LIST SERVICE LEVELS.
type PlanListServiceLevels struct{}

// PlanCreateGlobalIndex is CREATE INDEX ... AS GLOBAL.
type PlanCreateGlobalIndex struct {
	Descriptor types.GlobalIndexDescriptor
}

// PlanDropGlobalIndex is DROP INDEX.
type PlanDropGlobalIndex struct {
	Name string
}

func (*PlanCreateTable) plan()            {}
func (*PlanDropTable) plan()              {}
func (*PlanGetItem) plan()                {}
func (*PlanQuery) plan()                  {}
func (*PlanScan) plan()                   {}
func (*PlanSpatial) plan()                {}
func (*PlanANN) plan()                    {}
func (*PlanPutItem) plan()                {}
func (*PlanUpdate) plan()                 {}
func (*PlanDelete) plan()                 {}
func (*PlanCreateMaterializedView) plan() {}
func (*PlanDropMaterializedView) plan()   {}
func (*PlanCreateServiceLevel) plan()     {}
func (*PlanAlterServiceLevel) plan()      {}
func (*PlanDropServiceLevel) plan()       {}
func (*PlanListServiceLevels) plan()      {}
func (*PlanCreateGlobalIndex) plan()      {}
func (*PlanDropGlobalIndex) plan()        {}

// Explain renders the ANN plan for EXPLAIN output. The hybrid
// filter node is stitched under the ANN scan so callers see both
// the chosen strategy and the index that backs the cohort.
func (p *PlanANN) Explain(fmtKind cquery.ExplainFormat) string {
	root := cquery.PlanNode{
		Op:     "ANN",
		Detail: "table=" + p.Table + " field=" + p.Field,
	}
	if p.Filter.Strategy != cquery.StrategyUnset {
		root.Children = append(root.Children, p.Filter.ExplainNode())
	}
	if p.Diversify != nil {
		root.Children = append(root.Children, cquery.PlanNode{
			Op:     "Diversify",
			Plugin: p.Diversify.Method,
			Detail: "lambda=" + p.DiversifyLambdaString() + " to=" + p.DiversifyTargetString(),
		})
	}
	return cquery.RenderExplain(root, fmtKind)
}

func (p *PlanANN) DiversifyLambdaString() string {
	if p == nil || p.Diversify == nil {
		return ""
	}
	return strconv.FormatFloat(p.Diversify.Lambda, 'f', -1, 64)
}

func (p *PlanANN) DiversifyTargetString() string {
	if p == nil || p.Diversify == nil {
		return ""
	}
	return strconv.Itoa(p.Diversify.TargetSize)
}

// _ is here so the import stays as a compile-time check that the
// spatial package is reachable from the SQL layer.
var _ = spatial.BBox{}
