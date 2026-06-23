// Package catalog persists table schemas as JSON descriptors under
// cefas/catalog/<name>. It caches descriptors in memory after the first
// load so the request path doesn't hit Pebble for every operation.
//
// Pure invariant logic (descriptor validation, stream ARN/label
// generation, deep cloning) lives in
// internal/catalog/domain — this file is the Pebble-backed adapter.
package catalog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog/domain"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type Catalog struct {
	db *pebble.DB

	mu            sync.RWMutex
	tables        map[string]types.TableDescriptor
	views         map[string]types.MaterializedViewDescriptor
	serviceLevels map[string]types.ServiceLevelDescriptor

	slUpdateMu        sync.RWMutex
	slUpdateListeners []func(name string)
}

// OnServiceLevelChanged registers fn to be invoked whenever a
// service-level descriptor is created, altered, or dropped. The
// callback receives the SL name. Used by the quota controller
// (#499) for hot reload: it invalidates its cached bucket so the
// next Begin call rebuilds from the fresh descriptor.
func (c *Catalog) OnServiceLevelChanged(fn func(name string)) {
	c.slUpdateMu.Lock()
	c.slUpdateListeners = append(c.slUpdateListeners, fn)
	c.slUpdateMu.Unlock()
}

func (c *Catalog) notifyServiceLevelChanged(name string) {
	c.slUpdateMu.RLock()
	listeners := make([]func(string), len(c.slUpdateListeners))
	copy(listeners, c.slUpdateListeners)
	c.slUpdateMu.RUnlock()
	for _, fn := range listeners {
		fn(name)
	}
}

func New(db *pebble.DB) (*Catalog, error) {
	c := &Catalog{
		db:            db,
		tables:        make(map[string]types.TableDescriptor),
		views:         make(map[string]types.MaterializedViewDescriptor),
		serviceLevels: make(map[string]types.ServiceLevelDescriptor),
	}
	if err := c.loadAll(); err != nil {
		return nil, fmt.Errorf("catalog load: %w", err)
	}
	if err := c.loadAllViews(); err != nil {
		return nil, fmt.Errorf("catalog view load: %w", err)
	}
	if err := c.loadAllServiceLevels(); err != nil {
		return nil, fmt.Errorf("catalog service-level load: %w", err)
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
		_ = domain.NormalizeDescriptor(&td)
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
	if err := domain.NormalizeDescriptor(&td); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[td.Name]; ok {
		return types.ErrTableAlreadyExists
	}
	streamDesc, err := c.newStreamDescriptor(td)
	if err != nil {
		return err
	}
	b, err := json.Marshal(td)
	if err != nil {
		return fmt.Errorf("marshal descriptor: %w", err)
	}
	batch := c.db.Batch()
	defer batch.Close()
	if err := batch.Set(storage.KeyCatalog(td.Name), b, nil); err != nil {
		return fmt.Errorf("batch descriptor: %w", err)
	}
	if streamDesc != nil {
		streamBytes, err := marshalStreamDescriptor(*streamDesc)
		if err != nil {
			return err
		}
		if err := batch.Set(storage.KeyStreamDescriptor(streamDesc.StreamArn), streamBytes, nil); err != nil {
			return fmt.Errorf("batch stream descriptor: %w", err)
		}
	}
	if err := c.db.CommitBatch(batch); err != nil {
		return fmt.Errorf("persist descriptors: %w", err)
	}
	c.tables[td.Name] = domain.CloneTableDescriptor(td)
	return nil
}

// Describe returns the descriptor for the given table. Falls back to a
// Pebble Get on cache miss so followers see tables replicated through
// the Raft log without needing to be told to reload — the FSM applies
// the descriptor key, and the next Describe pulls it through.
func (c *Catalog) Describe(name string) (types.TableDescriptor, error) {
	c.mu.RLock()
	td, ok := c.tables[name]
	if !ok {
		if mv, mvOk := c.views[name]; mvOk {
			c.mu.RUnlock()
			return mvToTableDescriptor(mv), nil
		}
	}
	c.mu.RUnlock()
	if ok {
		return domain.CloneTableDescriptor(td), nil
	}
	raw, err := c.db.Get(storage.KeyCatalog(name))
	if err == pebble.ErrNotFound {
		// Cross-shard MV cascade: peer nodes receive BatchWriteItem
		// with the MV's name as the table. The MV descriptor lives at
		// KeyMaterializedView, not KeyCatalog, so the table-key lookup
		// misses on cold caches. Fall through to the MV key before
		// declaring the table absent.
		mvRaw, mvErr := c.db.Get(storage.KeyMaterializedView(name))
		if mvErr == nil {
			var mv types.MaterializedViewDescriptor
			if jerr := json.Unmarshal(mvRaw, &mv); jerr != nil {
				return types.TableDescriptor{}, fmt.Errorf("decode view: %w", jerr)
			}
			_ = domain.NormalizeMVDescriptor(&mv)
			c.mu.Lock()
			c.views[mv.Name] = mv
			c.mu.Unlock()
			return mvToTableDescriptor(mv), nil
		}
		return types.TableDescriptor{}, types.ErrTableNotFound
	}
	if err != nil {
		return types.TableDescriptor{}, err
	}
	var fresh types.TableDescriptor
	if err := json.Unmarshal(raw, &fresh); err != nil {
		return types.TableDescriptor{}, fmt.Errorf("decode descriptor: %w", err)
	}
	if err := domain.NormalizeDescriptor(&fresh); err != nil {
		return types.TableDescriptor{}, err
	}
	c.mu.Lock()
	c.tables[fresh.Name] = domain.CloneTableDescriptor(fresh)
	c.mu.Unlock()
	return domain.CloneTableDescriptor(fresh), nil
}

// KeySchema returns the immutable primary key schema for a table
// without cloning the full descriptor on cache hits. Hot read paths
// only need PK/SK names, not the whole index/stream descriptor surface.
func (c *Catalog) KeySchema(name string) (types.KeySchema, error) {
	c.mu.RLock()
	td, ok := c.tables[name]
	c.mu.RUnlock()
	if ok {
		return td.KeySchema, nil
	}
	td, err := c.Describe(name)
	if err != nil {
		return types.KeySchema{}, err
	}
	return td.KeySchema, nil
}

// List returns descriptors of every known table.
func (c *Catalog) List() []types.TableDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]types.TableDescriptor, 0, len(c.tables))
	for _, td := range c.tables {
		out = append(out, domain.CloneTableDescriptor(td))
	}
	return out
}

// DescribeStream returns persisted metadata for one table stream ARN.
func (c *Catalog) DescribeStream(streamArn string) (types.StreamDescriptor, error) {
	raw, err := c.db.Get(storage.KeyStreamDescriptor(streamArn))
	if err == pebble.ErrNotFound {
		return types.StreamDescriptor{}, types.ErrStreamNotFound
	}
	if err != nil {
		return types.StreamDescriptor{}, err
	}
	var desc types.StreamDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return types.StreamDescriptor{}, fmt.Errorf("decode stream descriptor: %w", err)
	}
	domain.NormalizeStreamMetadata(&desc)
	return desc, nil
}

// ListStreams returns stream metadata, optionally filtered by table name.
func (c *Catalog) ListStreams(table string) ([]types.StreamDescriptor, error) {
	lower, upper := storage.PrefixStreamDescriptors()
	it, err := c.db.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []types.StreamDescriptor
	for valid := it.First(); valid; valid = it.Next() {
		var desc types.StreamDescriptor
		raw := append([]byte(nil), it.Value()...)
		if err := json.Unmarshal(raw, &desc); err != nil {
			return nil, fmt.Errorf("decode stream descriptor at %s: %w", it.Key(), err)
		}
		domain.NormalizeStreamMetadata(&desc)
		if table != "" && desc.TableName != table {
			continue
		}
		out = append(out, desc)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreationRequestDateTime == out[j].CreationRequestDateTime {
			return out[i].StreamArn < out[j].StreamArn
		}
		return out[i].CreationRequestDateTime < out[j].CreationRequestDateTime
	})
	return out, nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	existing, ok := c.tables[td.Name]
	if !ok {
		return types.ErrTableNotFound
	}
	if err := domain.NormalizeDescriptor(&td); err != nil {
		return err
	}
	if err := domain.ApplyStreamUpdateSemantics(existing, &td); err != nil {
		return err
	}
	oldStreamEnabled := domain.StreamEnabled(existing)
	newStreamEnabled := domain.StreamEnabled(td)
	var closedStream *types.StreamDescriptor
	if oldStreamEnabled && !newStreamEnabled {
		desc, err := c.closeStreamDescriptor(existing)
		if err != nil {
			return err
		}
		closedStream = desc
	}
	var newStream *types.StreamDescriptor
	if !oldStreamEnabled && newStreamEnabled {
		desc, err := c.newStreamDescriptor(td)
		if err != nil {
			return err
		}
		newStream = desc
	}
	b, err := json.Marshal(td)
	if err != nil {
		return fmt.Errorf("marshal descriptor: %w", err)
	}
	batch := c.db.Batch()
	defer batch.Close()
	if err := batch.Set(storage.KeyCatalog(td.Name), b, nil); err != nil {
		return fmt.Errorf("batch descriptor: %w", err)
	}
	for _, desc := range []*types.StreamDescriptor{closedStream, newStream} {
		if desc == nil {
			continue
		}
		streamBytes, err := marshalStreamDescriptor(*desc)
		if err != nil {
			return err
		}
		if err := batch.Set(storage.KeyStreamDescriptor(desc.StreamArn), streamBytes, nil); err != nil {
			return fmt.Errorf("batch stream descriptor: %w", err)
		}
	}
	if err := c.db.CommitBatch(batch); err != nil {
		return fmt.Errorf("persist descriptor: %w", err)
	}
	c.tables[td.Name] = domain.CloneTableDescriptor(td)
	return nil
}

func (c *Catalog) newStreamDescriptor(td types.TableDescriptor) (*types.StreamDescriptor, error) {
	if !domain.StreamEnabled(td) {
		return nil, nil
	}
	if td.LatestStreamArn == "" || td.LatestStreamLabel == "" {
		return nil, fmt.Errorf("stream metadata requires latest stream ARN and label")
	}
	changeIndex, err := c.db.CurrentChangeIndex()
	if err != nil {
		return nil, fmt.Errorf("load stream starting sequence: %w", err)
	}
	starting := strconv.FormatUint(changeIndex+1, 10)
	return &types.StreamDescriptor{
		StreamArn:               td.LatestStreamArn,
		StreamLabel:             td.LatestStreamLabel,
		TableName:               td.Name,
		StreamStatus:            types.StreamStatusEnabled,
		StreamViewType:          domain.StreamViewType(td),
		CreationRequestDateTime: time.Now().UnixNano(),
		KeySchema:               td.KeySchema,
		Shards: []types.StreamShardDescriptor{
			{
				ShardID: model.StreamShardIDSingle.String(),
				SequenceNumberRange: types.StreamSequenceNumberRange{
					StartingSequenceNumber: starting,
				},
			},
		},
	}, nil
}

func (c *Catalog) closeStreamDescriptor(td types.TableDescriptor) (*types.StreamDescriptor, error) {
	if td.LatestStreamArn == "" {
		return nil, nil
	}
	desc, err := c.DescribeStream(td.LatestStreamArn)
	if err == types.ErrStreamNotFound {
		desc = types.StreamDescriptor{
			StreamArn:               td.LatestStreamArn,
			StreamLabel:             td.LatestStreamLabel,
			TableName:               td.Name,
			StreamStatus:            types.StreamStatusEnabled,
			StreamViewType:          domain.StreamViewType(td),
			CreationRequestDateTime: time.Now().UnixNano(),
			KeySchema:               td.KeySchema,
			Shards: []types.StreamShardDescriptor{
				{
					ShardID: model.StreamShardIDSingle.String(),
					SequenceNumberRange: types.StreamSequenceNumberRange{
						StartingSequenceNumber: "1",
					},
				},
			},
		}
	} else if err != nil {
		return nil, err
	}
	changeIndex, err := c.db.CurrentChangeIndex()
	if err != nil {
		return nil, fmt.Errorf("load stream ending sequence: %w", err)
	}
	ending := strconv.FormatUint(changeIndex, 10)
	domain.NormalizeStreamMetadata(&desc)
	desc.StreamStatus = types.StreamStatusDisabled
	for i := range desc.Shards {
		if desc.Shards[i].SequenceNumberRange.EndingSequenceNumber == "" {
			desc.Shards[i].SequenceNumberRange.EndingSequenceNumber = ending
		}
	}
	return &desc, nil
}

func marshalStreamDescriptor(desc types.StreamDescriptor) ([]byte, error) {
	domain.NormalizeStreamMetadata(&desc)
	raw, err := json.Marshal(desc)
	if err != nil {
		return nil, fmt.Errorf("marshal stream descriptor: %w", err)
	}
	return raw, nil
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

func (c *Catalog) loadAllViews() error {
	lower, upper := storage.PrefixMaterializedViews()
	it, err := c.db.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		var mv types.MaterializedViewDescriptor
		v := it.Value()
		if err := json.Unmarshal(v, &mv); err != nil {
			return fmt.Errorf("decode mv at %s: %w", it.Key(), err)
		}
		_ = domain.NormalizeMVDescriptor(&mv)
		c.views[mv.Name] = mv
	}
	return it.Error()
}

func (c *Catalog) CreateView(mv types.MaterializedViewDescriptor) (types.MaterializedViewDescriptor, error) {
	if mv.Name == "" {
		return types.MaterializedViewDescriptor{}, fmt.Errorf("view name required")
	}
	if err := domain.NormalizeMVDescriptor(&mv); err != nil {
		return types.MaterializedViewDescriptor{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.views[mv.Name]; exists {
		return types.MaterializedViewDescriptor{}, types.ErrMVAlreadyExists
	}
	if _, exists := c.tables[mv.Name]; exists {
		return types.MaterializedViewDescriptor{}, fmt.Errorf("name %q clashes with an existing table", mv.Name)
	}
	base, ok := c.tables[mv.BaseTable]
	if !ok {
		return types.MaterializedViewDescriptor{}, fmt.Errorf("base table %q: %w", mv.BaseTable, types.ErrTableNotFound)
	}
	if mv.Status == "" {
		mv.Status = types.MVStatusBuilding
	}
	raw, err := json.Marshal(mv)
	if err != nil {
		return types.MaterializedViewDescriptor{}, fmt.Errorf("marshal view: %w", err)
	}
	batch := c.db.Batch()
	defer batch.Close()
	if err := batch.Set(storage.KeyMaterializedView(mv.Name), raw, nil); err != nil {
		return types.MaterializedViewDescriptor{}, fmt.Errorf("batch view: %w", err)
	}
	updatedBase := domain.CloneTableDescriptor(base)
	if !containsViewName(updatedBase.MaterializedViews, mv.Name) {
		updatedBase.MaterializedViews = append(updatedBase.MaterializedViews, mv.Name)
		baseRaw, err := json.Marshal(updatedBase)
		if err != nil {
			return types.MaterializedViewDescriptor{}, fmt.Errorf("marshal base: %w", err)
		}
		if err := batch.Set(storage.KeyCatalog(updatedBase.Name), baseRaw, nil); err != nil {
			return types.MaterializedViewDescriptor{}, fmt.Errorf("batch base: %w", err)
		}
	}
	if err := c.db.CommitBatch(batch); err != nil {
		return types.MaterializedViewDescriptor{}, fmt.Errorf("persist view: %w", err)
	}
	c.views[mv.Name] = mv
	c.tables[updatedBase.Name] = updatedBase
	return mv, nil
}

func (c *Catalog) DescribeView(name string) (types.MaterializedViewDescriptor, error) {
	c.mu.RLock()
	mv, ok := c.views[name]
	c.mu.RUnlock()
	if ok {
		return mv, nil
	}
	raw, err := c.db.Get(storage.KeyMaterializedView(name))
	if err != nil {
		return types.MaterializedViewDescriptor{}, types.ErrMVNotFound
	}
	var out types.MaterializedViewDescriptor
	if err := json.Unmarshal(raw, &out); err != nil {
		return types.MaterializedViewDescriptor{}, fmt.Errorf("decode view %s: %w", name, err)
	}
	_ = domain.NormalizeMVDescriptor(&out)
	c.mu.Lock()
	c.views[out.Name] = out
	c.mu.Unlock()
	return out, nil
}

func (c *Catalog) ListViews(baseTable string) []types.MaterializedViewDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]types.MaterializedViewDescriptor, 0, len(c.views))
	for _, mv := range c.views {
		if baseTable != "" && mv.BaseTable != baseTable {
			continue
		}
		out = append(out, mv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (c *Catalog) UpdateView(mv types.MaterializedViewDescriptor) error {
	if mv.Name == "" {
		return fmt.Errorf("view name required")
	}
	if err := domain.NormalizeMVDescriptor(&mv); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.views[mv.Name]; !ok {
		return types.ErrMVNotFound
	}
	raw, err := json.Marshal(mv)
	if err != nil {
		return fmt.Errorf("marshal view: %w", err)
	}
	if err := c.db.Set(storage.KeyMaterializedView(mv.Name), raw); err != nil {
		return fmt.Errorf("persist view: %w", err)
	}
	c.views[mv.Name] = mv
	return nil
}

func (c *Catalog) DropView(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	mv, ok := c.views[name]
	if !ok {
		return types.ErrMVNotFound
	}
	batch := c.db.Batch()
	defer batch.Close()
	if err := batch.Delete(storage.KeyMaterializedView(name), nil); err != nil {
		return fmt.Errorf("delete view: %w", err)
	}
	if base, baseOK := c.tables[mv.BaseTable]; baseOK {
		updated := domain.CloneTableDescriptor(base)
		updated.MaterializedViews = removeViewName(updated.MaterializedViews, name)
		baseRaw, err := json.Marshal(updated)
		if err != nil {
			return fmt.Errorf("marshal base after detach: %w", err)
		}
		if err := batch.Set(storage.KeyCatalog(updated.Name), baseRaw, nil); err != nil {
			return fmt.Errorf("batch detach: %w", err)
		}
		if err := c.db.CommitBatch(batch); err != nil {
			return fmt.Errorf("persist drop: %w", err)
		}
		c.tables[updated.Name] = updated
	} else {
		if err := c.db.CommitBatch(batch); err != nil {
			return fmt.Errorf("persist drop: %w", err)
		}
	}
	delete(c.views, name)
	return nil
}

func containsViewName(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func removeViewName(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v == x {
			continue
		}
		out = append(out, v)
	}
	return out
}

func mvToTableDescriptor(mv types.MaterializedViewDescriptor) types.TableDescriptor {
	return types.TableDescriptor{
		Name:      mv.Name,
		KeySchema: mv.KeySchema,
	}
}

// loadAllServiceLevels hydrates the in-memory map from pebble on open.
func (c *Catalog) loadAllServiceLevels() error {
	lower, upper := storage.PrefixServiceLevels()
	it, err := c.db.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		var sl types.ServiceLevelDescriptor
		if err := json.Unmarshal(it.Value(), &sl); err != nil {
			return fmt.Errorf("decode service-level at %s: %w", it.Key(), err)
		}
		c.serviceLevels[sl.Name] = sl
	}
	return it.Error()
}

// CreateServiceLevel persists a new service level. The name
// "default" is reserved and cannot be created or dropped — it is
// served synthetically from GetServiceLevel.
func (c *Catalog) CreateServiceLevel(sl types.ServiceLevelDescriptor) (types.ServiceLevelDescriptor, error) {
	if sl.Name == "" {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("service level name required")
	}
	if sl.Name == types.DefaultServiceLevelName {
		return types.ServiceLevelDescriptor{}, types.ErrServiceLevelReserved
	}
	if sl.Shares < 0 || sl.MaxInFlight < 0 || sl.MaxRowsPerSec < 0 || sl.MaxBytesPerSec < 0 {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("service level %q: shares + caps must be >= 0", sl.Name)
	}
	if sl.Shares == 0 {
		sl.Shares = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.serviceLevels[sl.Name]; exists {
		return types.ServiceLevelDescriptor{}, types.ErrServiceLevelExists
	}
	raw, err := json.Marshal(sl)
	if err != nil {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("marshal service-level: %w", err)
	}
	if err := c.db.Set(storage.KeyServiceLevel(sl.Name), raw); err != nil {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("persist service-level: %w", err)
	}
	c.serviceLevels[sl.Name] = sl
	c.notifyServiceLevelChanged(sl.Name)
	return sl, nil
}

// UpdateServiceLevel replaces the persisted descriptor. Name must
// match an existing record; default cannot be altered.
func (c *Catalog) UpdateServiceLevel(sl types.ServiceLevelDescriptor) (types.ServiceLevelDescriptor, error) {
	if sl.Name == "" {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("service level name required")
	}
	if sl.Name == types.DefaultServiceLevelName {
		return types.ServiceLevelDescriptor{}, types.ErrServiceLevelReserved
	}
	if sl.Shares < 0 || sl.MaxInFlight < 0 || sl.MaxRowsPerSec < 0 || sl.MaxBytesPerSec < 0 {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("service level %q: shares + caps must be >= 0", sl.Name)
	}
	if sl.Shares == 0 {
		sl.Shares = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.serviceLevels[sl.Name]; !exists {
		return types.ServiceLevelDescriptor{}, types.ErrServiceLevelNotFound
	}
	raw, err := json.Marshal(sl)
	if err != nil {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("marshal service-level: %w", err)
	}
	if err := c.db.Set(storage.KeyServiceLevel(sl.Name), raw); err != nil {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("persist service-level: %w", err)
	}
	c.serviceLevels[sl.Name] = sl
	c.notifyServiceLevelChanged(sl.Name)
	return sl, nil
}

// DropServiceLevel removes the persisted descriptor. The "default"
// name is reserved and rejected.
func (c *Catalog) DropServiceLevel(name string) error {
	if name == "" {
		return fmt.Errorf("service level name required")
	}
	if name == types.DefaultServiceLevelName {
		return types.ErrServiceLevelReserved
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.serviceLevels[name]; !exists {
		return types.ErrServiceLevelNotFound
	}
	if err := c.db.Delete(storage.KeyServiceLevel(name)); err != nil {
		return fmt.Errorf("delete service-level: %w", err)
	}
	delete(c.serviceLevels, name)
	c.notifyServiceLevelChanged(name)
	return nil
}

// PauseServiceLevel sets the SL's Paused flag and persists the
// descriptor. The default SL cannot be paused.
func (c *Catalog) PauseServiceLevel(name string) (types.ServiceLevelDescriptor, error) {
	return c.setServiceLevelPaused(name, true)
}

// ResumeServiceLevel clears the Paused flag.
func (c *Catalog) ResumeServiceLevel(name string) (types.ServiceLevelDescriptor, error) {
	return c.setServiceLevelPaused(name, false)
}

func (c *Catalog) setServiceLevelPaused(name string, paused bool) (types.ServiceLevelDescriptor, error) {
	if name == "" {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("service level name required")
	}
	if name == types.DefaultServiceLevelName {
		return types.ServiceLevelDescriptor{}, types.ErrServiceLevelReserved
	}
	c.mu.Lock()
	sl, ok := c.serviceLevels[name]
	if !ok {
		c.mu.Unlock()
		return types.ServiceLevelDescriptor{}, types.ErrServiceLevelNotFound
	}
	sl.Paused = paused
	raw, err := json.Marshal(sl)
	if err != nil {
		c.mu.Unlock()
		return types.ServiceLevelDescriptor{}, fmt.Errorf("marshal service-level: %w", err)
	}
	if err := c.db.Set(storage.KeyServiceLevel(name), raw); err != nil {
		c.mu.Unlock()
		return types.ServiceLevelDescriptor{}, fmt.Errorf("persist service-level: %w", err)
	}
	c.serviceLevels[name] = sl
	c.mu.Unlock()
	c.notifyServiceLevelChanged(name)
	return sl, nil
}

// GetServiceLevel returns the descriptor for name. The implicit
// "default" service level is served from a synthetic descriptor with
// shares=1 and no caps when no explicit record exists.
func (c *Catalog) GetServiceLevel(name string) (types.ServiceLevelDescriptor, error) {
	if name == "" {
		return types.ServiceLevelDescriptor{}, fmt.Errorf("service level name required")
	}
	c.mu.RLock()
	sl, ok := c.serviceLevels[name]
	c.mu.RUnlock()
	if ok {
		return sl, nil
	}
	if name == types.DefaultServiceLevelName {
		return types.ServiceLevelDescriptor{Name: types.DefaultServiceLevelName, Shares: 1}, nil
	}
	return types.ServiceLevelDescriptor{}, types.ErrServiceLevelNotFound
}

// ListServiceLevels returns every persisted service level plus the
// synthetic "default" entry. Order is unspecified.
func (c *Catalog) ListServiceLevels() []types.ServiceLevelDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]types.ServiceLevelDescriptor, 0, len(c.serviceLevels)+1)
	for _, sl := range c.serviceLevels {
		out = append(out, sl)
	}
	if _, ok := c.serviceLevels[types.DefaultServiceLevelName]; !ok {
		out = append(out, types.ServiceLevelDescriptor{Name: types.DefaultServiceLevelName, Shares: 1})
	}
	return out
}
