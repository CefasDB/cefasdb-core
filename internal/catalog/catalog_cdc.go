package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	domain "github.com/CefasDb/cefasdb/internal/catalog/domain"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// cdcAliasBase strips the CDC suffix from name when present. Returns
// the base table name + true on match; ("", false) otherwise.
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

// describeUncached returns the descriptor for name without recursing
// through cdcAliasBase / MV fallback. Used by the CDC alias path to
// validate the underlying base table exists.
func (c *Catalog) describeUncached(name string) (types.TableDescriptor, error) {
	c.mu.RLock()
	td, ok := c.tables[name]
	c.mu.RUnlock()
	if ok {
		return domain.CloneTableDescriptor(td), nil
	}
	raw, err := c.db.Get(storage.KeyCatalog(name))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return types.TableDescriptor{}, types.ErrTableNotFound
		}
		return types.TableDescriptor{}, err
	}
	var fresh types.TableDescriptor
	if err := json.Unmarshal(raw, &fresh); err != nil {
		return types.TableDescriptor{}, fmt.Errorf("decode descriptor: %w", err)
	}
	_ = domain.NormalizeDescriptor(&fresh)
	c.mu.Lock()
	c.tables[fresh.Name] = domain.CloneTableDescriptor(fresh)
	c.mu.Unlock()
	return domain.CloneTableDescriptor(fresh), nil
}

// cdcSyntheticDescriptor builds the synthetic descriptor surface a
// CDC alias presents to Scan / Query. The PK is the base table name
// (literal) and the SK is the monotonic changelog index — enough to
// give the planner a key schema; the actual rows are decoded from
// the pebble changelog, not stored under this descriptor's keys.
func cdcSyntheticDescriptor(base types.TableDescriptor) types.TableDescriptor {
	return types.TableDescriptor{
		Name:      base.Name + types.CDCTableSuffix,
		KeySchema: types.KeySchema{PK: "table", SK: "index"},
	}
}
