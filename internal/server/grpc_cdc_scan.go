package server

import (
	"strconv"
	"strings"

	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// scanCDCStream streams the CDC alias rows to the Scan response.
// Applies the request's FilterExpression + Limit per row before
// emitting.
func (s *GRPCServer) scanCDCStream(req *cefaspb.ScanRequest, baseTable string, stream cefaspb.Cefas_ScanServer) error {
	cond, err := storage.ParseCondition(req.GetFilterExpression())
	if err != nil {
		return mapStorageErr(err)
	}
	rawBinds, err := pbToItem(req.GetBinds())
	if err != nil {
		return mapStorageErr(err)
	}
	binds := make(map[string]types.AttributeValue, len(rawBinds))
	for k, v := range rawBinds {
		binds[strings.TrimPrefix(k, ":")] = v
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.scanCDCAlias(baseTable, limit)
	if err != nil {
		return mapStorageErr(err)
	}
	sent := 0
	for _, it := range rows {
		if !cond.IsZero() {
			ok, evErr := cond.Evaluate(it, binds)
			if evErr != nil {
				return mapStorageErr(evErr)
			}
			if !ok {
				continue
			}
		}
		if err := stream.Send(&cefaspb.Item{Attributes: itemToPB(it)}); err != nil {
			return err
		}
		sent++
		if req.GetLimit() > 0 && sent >= int(req.GetLimit()) {
			break
		}
	}
	return nil
}

// cdcAliasBase strips the CDC suffix from name when present.
// Mirrors catalog.cdcAliasBase so the server layer can detect the
// alias without taking a catalog lock on every Scan / Query call.
func cdcAliasBase(name string) (string, bool) {
	if !strings.HasSuffix(name, types.CDCTableSuffix) {
		return "", false
	}
	base := strings.TrimSuffix(name, types.CDCTableSuffix)
	if base == "" {
		return "", false
	}
	return base, true
}

// scanCDCAlias drains the changelog for the base table behind the
// CDC alias and projects each ChangeRecord into a row the existing
// Scan / Query handlers can stream back. Order is changelog-order
// (monotonic Index), filtering by event_time is left to the caller
// via FilterExpression.
func (s *GRPCServer) scanCDCAlias(baseTable string, limit int) ([]types.Item, error) {
	if s.db == nil {
		return nil, nil
	}
	recs, err := s.db.ScanCDC(baseTable, 0, 0, limit)
	if err != nil {
		return nil, err
	}
	out := make([]types.Item, 0, len(recs))
	for _, rec := range recs {
		out = append(out, changeRecordToItem(rec))
	}
	return out, nil
}

// changeRecordToItem flattens a pebble.ChangeRecord into the row
// shape returned by a CDC alias scan. Nested key / item / oldItem /
// newItem maps land as `M` attributes so the planner / filter
// expressions can reach into them via dot notation when the parser
// learns about it; for v1 a caller filters by `event_time`, `op`,
// `index`, `event_name`.
func changeRecordToItem(rec pebble.ChangeRecord) types.Item {
	item := types.Item{
		"table":      {T: types.AttrS, S: rec.Table},
		"index":      {T: types.AttrN, N: strconv.FormatUint(rec.Index, 10)},
		"event_time": {T: types.AttrN, N: strconv.FormatInt(rec.UnixNano, 10)},
		"op":         {T: types.AttrS, S: string(rec.Op)},
	}
	if rec.EventName != "" {
		item["event_name"] = types.AttributeValue{T: types.AttrS, S: string(rec.EventName)}
	}
	if rec.SequenceNumber != "" {
		item["sequence_number"] = types.AttributeValue{T: types.AttrS, S: rec.SequenceNumber}
	}
	if rec.Key != nil {
		item["key"] = itemToMapAttr(rec.Key)
	}
	if rec.Item != nil {
		item["item"] = itemToMapAttr(rec.Item)
	}
	if rec.OldItem != nil {
		item["old_item"] = itemToMapAttr(rec.OldItem)
	}
	if rec.NewItem != nil {
		item["new_item"] = itemToMapAttr(rec.NewItem)
	}
	return item
}

// itemToMapAttr wraps a nested Item as an AttrM attribute so the
// CDC row can carry arbitrarily-shaped key / item payloads without
// flattening them into the top-level row schema.
func itemToMapAttr(in types.Item) types.AttributeValue {
	m := make(map[string]types.AttributeValue, len(in))
	for k, v := range in {
		m[k] = v
	}
	return types.AttributeValue{T: types.AttrM, M: m}
}
