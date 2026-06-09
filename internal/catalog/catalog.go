// Package catalog persists table schemas as JSON descriptors under
// cefas/catalog/<name>. It caches descriptors in memory after the first
// load so the request path doesn't hit Pebble for every operation.
package catalog

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

type Catalog struct {
	db *storage.DB

	mu     sync.RWMutex
	tables map[string]types.TableDescriptor
}

func New(db *storage.DB) (*Catalog, error) {
	c := &Catalog{db: db, tables: make(map[string]types.TableDescriptor)}
	if err := c.loadAll(); err != nil {
		return nil, fmt.Errorf("catalog load: %w", err)
	}
	return c, nil
}

func (c *Catalog) loadAll() error {
	lower, upper := storage.PrefixCatalog()
	it, err := c.db.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		var td types.TableDescriptor
		v := it.Value()
		// Pebble reuses value buffers between Next; decode immediately.
		if err := json.Unmarshal(v, &td); err != nil {
			return fmt.Errorf("decode descriptor at %s: %w", it.Key(), err)
		}
		c.tables[td.Name] = td
	}
	return it.Error()
}

// Create persists a new table. Returns ErrTableAlreadyExists if the name
// is taken.
func (c *Catalog) Create(td types.TableDescriptor) error {
	if td.Name == "" {
		return fmt.Errorf("table name required")
	}
	if td.KeySchema.PK == "" {
		return fmt.Errorf("KeySchema.PK required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[td.Name]; ok {
		return types.ErrTableAlreadyExists
	}
	b, err := json.Marshal(td)
	if err != nil {
		return fmt.Errorf("marshal descriptor: %w", err)
	}
	if err := c.db.Set(storage.KeyCatalog(td.Name), b); err != nil {
		return fmt.Errorf("persist descriptor: %w", err)
	}
	c.tables[td.Name] = td
	return nil
}

// Describe returns the descriptor for the given table.
func (c *Catalog) Describe(name string) (types.TableDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	td, ok := c.tables[name]
	if !ok {
		return types.TableDescriptor{}, types.ErrTableNotFound
	}
	return td, nil
}

// List returns descriptors of every known table.
func (c *Catalog) List() []types.TableDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]types.TableDescriptor, 0, len(c.tables))
	for _, td := range c.tables {
		out = append(out, td)
	}
	return out
}

// Drop removes a table descriptor. Items under the table are NOT erased
// here — call storage.DropTableItems separately if needed (Phase 2).
func (c *Catalog) Drop(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[name]; !ok {
		return types.ErrTableNotFound
	}
	if err := c.db.Delete(storage.KeyCatalog(name)); err != nil {
		return fmt.Errorf("delete descriptor: %w", err)
	}
	delete(c.tables, name)
	return nil
}
