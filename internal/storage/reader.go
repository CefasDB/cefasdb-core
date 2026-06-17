package storage

import "github.com/CefasDb/cefasdb/pkg/types"

// Reader is the read surface of the cefas storage engine. The
// Pebble-backed adapter at internal/storage/adapter/pebble.DB
// implements it; tests can substitute a smaller fake without
// pulling in the Pebble engine.
//
// Justification: six methods, each a distinct query type the
// planner dispatches on parsed statement shape (point, PK range,
// GSI, spatial, scan). Splitting further would scatter the
// planner-to-storage vocabulary across many tiny interfaces with
// no consumer benefit. Every method operates on items fetched from
// the same storage namespace.
type Reader interface {
	GetItem(table string, ks types.KeySchema, key types.Item) (types.Item, error)
	QueryByPK(table string, ks types.KeySchema, pkAttr types.AttributeValue, limit int) ([]types.Item, error)
	QueryByPKRange(table string, ks types.KeySchema, pkAttr, skLow, skHigh types.AttributeValue, limit int) ([]types.Item, error)
	QueryByGSI(td types.TableDescriptor, idxName string, gsiPKVal types.AttributeValue, opts QueryOptions) ([]types.Item, error)
	SpatialQueryItems(td types.TableDescriptor, idxName string, q SpatialQuery) ([]types.Item, error)
	ScanTable(table string, limit int) ([]types.Item, error)
}
