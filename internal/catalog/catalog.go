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

	"github.com/osvaldoandrade/cefas/internal/catalog/domain"
	"github.com/osvaldoandrade/cefas/internal/storage"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/internal/core/model"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

type Catalog struct {
	db *pebble.DB

	mu     sync.RWMutex
	tables map[string]types.TableDescriptor
}

func New(db *pebble.DB) (*Catalog, error) {
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
	c.mu.RUnlock()
	if ok {
		return domain.CloneTableDescriptor(td), nil
	}
	raw, err := c.db.Get(storage.KeyCatalog(name))
	if err == pebble.ErrNotFound {
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
