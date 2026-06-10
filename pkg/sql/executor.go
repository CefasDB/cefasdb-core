package sql

import (
	"errors"
	"fmt"

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
	if err := e.Storage.PutItemWith(p.Descriptor, p.Item, storage.PutOptions{}); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execUpdate(p *PlanUpdate) (*Result, error) {
	// Read prior, merge assignments, write back. The PutItemWith call
	// keeps GSI + spatial maintenance atomic; we never touch the
	// primary key columns so the indexes converge correctly.
	prior, err := e.Storage.GetItem(p.Table, p.Descriptor.KeySchema, p.Key)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			return &Result{AffectedRows: 0}, nil
		}
		return nil, err
	}
	for k, v := range p.Assignments {
		prior[k] = v
	}
	if err := e.Storage.PutItemWith(p.Descriptor, prior, storage.PutOptions{}); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
}

func (e *Executor) execDelete(p *PlanDelete) (*Result, error) {
	if err := e.Storage.DeleteItemWith(p.Descriptor, p.Key, storage.DeleteOptions{}); err != nil {
		return nil, err
	}
	return &Result{AffectedRows: 1}, nil
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
