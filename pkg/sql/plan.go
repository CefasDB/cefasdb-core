package sql

import (
	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
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
}

// PlanSpatial is a geohash / Z-order / radius scan.
type PlanSpatial struct {
	Table      string
	IndexName  string
	Query      storage.SpatialQuery
	Project    []string
	Descriptor types.TableDescriptor
}

// PlanPutItem is INSERT INTO ... VALUES (...).
type PlanPutItem struct {
	Table      string
	Item       types.Item
	Descriptor types.TableDescriptor
}

// PlanUpdate is the single-item UPDATE path. The planner resolves the
// item identity from the WHERE clause and packages the assignment
// delta — the executor reads the prior row and merges the changes
// inside a single storage.PutItemWith call.
type PlanUpdate struct {
	Table       string
	Key         types.Item            // PK [+ SK] of the row to update
	Assignments map[string]types.AttributeValue
	Descriptor  types.TableDescriptor
}

// PlanDelete is DELETE WHERE pk = ... [AND sk = ...].
type PlanDelete struct {
	Table      string
	Key        types.Item
	Descriptor types.TableDescriptor
}

func (*PlanCreateTable) plan() {}
func (*PlanDropTable) plan()   {}
func (*PlanGetItem) plan()     {}
func (*PlanQuery) plan()       {}
func (*PlanSpatial) plan()     {}
func (*PlanPutItem) plan()     {}
func (*PlanUpdate) plan()      {}
func (*PlanDelete) plan()      {}

// _ is here so the import stays as a compile-time check that the
// spatial package is reachable from the SQL layer.
var _ = spatial.BBox{}
