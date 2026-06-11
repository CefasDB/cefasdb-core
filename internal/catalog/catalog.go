// Package catalog persists table schemas as JSON descriptors under
// cefas/catalog/<name>. It caches descriptors in memory after the first
// load so the request path doesn't hit Pebble for every operation.
package catalog

import (
	"encoding/json"
	"fmt"
	"strings"
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
		normalizeDescriptor(&td)
		c.tables[td.Name] = td
	}
	return it.Error()
}

// Reload drops the in-memory cache and re-reads every descriptor from
// Pebble. Useful in tests and after admin tools rewrite the catalog
// out-of-band.
func (c *Catalog) Reload() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables = make(map[string]types.TableDescriptor)
	return c.loadAll()
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
	if err := normalizeDescriptor(&td); err != nil {
		return err
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

// Describe returns the descriptor for the given table. Falls back to a
// Pebble Get on cache miss so followers see tables replicated through
// the Raft log without needing to be told to reload — the FSM applies
// the descriptor key, and the next Describe pulls it through.
func (c *Catalog) Describe(name string) (types.TableDescriptor, error) {
	c.mu.RLock()
	td, ok := c.tables[name]
	c.mu.RUnlock()
	if ok {
		return td, nil
	}
	raw, err := c.db.Get(storage.KeyCatalog(name))
	if err == storage.ErrNotFound {
		return types.TableDescriptor{}, types.ErrTableNotFound
	}
	if err != nil {
		return types.TableDescriptor{}, err
	}
	var fresh types.TableDescriptor
	if err := json.Unmarshal(raw, &fresh); err != nil {
		return types.TableDescriptor{}, fmt.Errorf("decode descriptor: %w", err)
	}
	if err := normalizeDescriptor(&fresh); err != nil {
		return types.TableDescriptor{}, err
	}
	c.mu.Lock()
	c.tables[fresh.Name] = fresh
	c.mu.Unlock()
	return fresh, nil
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

// UpdateTable persists an in-place mutation of an existing
// descriptor (TTL, future tags, etc.). Returns ErrTableNotFound when
// the name is unknown. The replacement keeps td.Name, KeySchema, and
// indexes — callers are responsible for not mutating fields the
// storage layer treats as immutable. Mirrors Create's pebble write so
// followers pick the change up through Reload on their next read.
func (c *Catalog) UpdateTable(td types.TableDescriptor) error {
	if td.Name == "" {
		return fmt.Errorf("table name required")
	}
	if err := normalizeDescriptor(&td); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[td.Name]; !ok {
		return types.ErrTableNotFound
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

func normalizeDescriptor(td *types.TableDescriptor) error {
	switch strings.ToLower(strings.TrimSpace(td.StorageClass)) {
	case "", types.StorageClassDisk:
		td.StorageClass = types.StorageClassDisk
	case types.StorageClassMemory:
		td.StorageClass = types.StorageClassMemory
	default:
		return fmt.Errorf("storageClass %q must be %q or %q", td.StorageClass, types.StorageClassDisk, types.StorageClassMemory)
	}
	for i := range td.AttributeDefinitions {
		td.AttributeDefinitions[i].Type = strings.ToUpper(strings.TrimSpace(td.AttributeDefinitions[i].Type))
		if td.AttributeDefinitions[i].Type == "V" && td.AttributeDefinitions[i].VectorDimensions <= 0 {
			return fmt.Errorf("attributeDefinitions[%d]: V requires vectorDimensions > 0", i)
		}
	}
	return nil
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
