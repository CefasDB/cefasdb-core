// Package catalog persists table schemas as JSON descriptors under
// cefas/catalog/<name>. It caches descriptors in memory after the first
// load so the request path doesn't hit Pebble for every operation.
package catalog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

type Catalog struct {
	db *storage.DB

	mu     sync.RWMutex
	tables map[string]types.TableDescriptor
}

var (
	streamLabelMu           sync.Mutex
	lastStreamLabelUnixNano int64
)

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
	c.tables[td.Name] = cloneTableDescriptor(td)
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
		return cloneTableDescriptor(td), nil
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
	c.tables[fresh.Name] = cloneTableDescriptor(fresh)
	c.mu.Unlock()
	return cloneTableDescriptor(fresh), nil
}

// List returns descriptors of every known table.
func (c *Catalog) List() []types.TableDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]types.TableDescriptor, 0, len(c.tables))
	for _, td := range c.tables {
		out = append(out, cloneTableDescriptor(td))
	}
	return out
}

// DescribeStream returns persisted metadata for one table stream ARN.
func (c *Catalog) DescribeStream(streamArn string) (types.StreamDescriptor, error) {
	raw, err := c.db.Get(storage.KeyStreamDescriptor(streamArn))
	if err == storage.ErrNotFound {
		return types.StreamDescriptor{}, types.ErrStreamNotFound
	}
	if err != nil {
		return types.StreamDescriptor{}, err
	}
	var desc types.StreamDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return types.StreamDescriptor{}, fmt.Errorf("decode stream descriptor: %w", err)
	}
	normalizeStreamMetadata(&desc)
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
		normalizeStreamMetadata(&desc)
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
	if err := normalizeDescriptor(&td); err != nil {
		return err
	}
	if err := applyStreamUpdateSemantics(existing, &td); err != nil {
		return err
	}
	oldStreamEnabled := streamEnabled(existing)
	newStreamEnabled := streamEnabled(td)
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
	c.tables[td.Name] = cloneTableDescriptor(td)
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
	if err := normalizeStreamDescriptor(td); err != nil {
		return err
	}
	return nil
}

func normalizeStreamDescriptor(td *types.TableDescriptor) error {
	if td.StreamSpecification == nil || !td.StreamSpecification.StreamEnabled {
		td.StreamSpecification = nil
		td.LatestStreamArn = ""
		td.LatestStreamLabel = ""
		td.StreamStatus = ""
		return nil
	}
	view := types.NormalizeStreamViewType(td.StreamSpecification.StreamViewType)
	if view == "" {
		view = types.StreamViewTypeNewAndOldImages
	}
	if !types.IsValidStreamViewType(view) {
		return fmt.Errorf("streamViewType %q must be one of %q, %q, %q, %q",
			td.StreamSpecification.StreamViewType,
			types.StreamViewTypeKeysOnly,
			types.StreamViewTypeNewImage,
			types.StreamViewTypeOldImage,
			types.StreamViewTypeNewAndOldImages)
	}
	td.StreamSpecification = &types.StreamSpecification{
		StreamEnabled:  true,
		StreamViewType: view,
	}
	if td.LatestStreamLabel == "" {
		td.LatestStreamLabel = nextStreamLabel()
	}
	if td.LatestStreamArn == "" {
		td.LatestStreamArn = streamARN(td.Name, td.LatestStreamLabel)
	}
	if td.StreamStatus == "" {
		td.StreamStatus = types.StreamStatusEnabled
	}
	return nil
}

func streamEnabled(td types.TableDescriptor) bool {
	return td.StreamSpecification != nil && td.StreamSpecification.StreamEnabled
}

func streamViewType(td types.TableDescriptor) string {
	if td.StreamSpecification == nil {
		return ""
	}
	view := types.NormalizeStreamViewType(td.StreamSpecification.StreamViewType)
	if view == "" {
		return types.StreamViewTypeNewAndOldImages
	}
	return view
}

func applyStreamUpdateSemantics(existing types.TableDescriptor, td *types.TableDescriptor) error {
	if !streamEnabled(existing) || !streamEnabled(*td) {
		return nil
	}
	oldView := streamViewType(existing)
	newView := streamViewType(*td)
	if oldView != newView {
		return fmt.Errorf("streamViewType cannot be changed while stream is enabled; disable and re-enable the stream")
	}
	td.LatestStreamLabel = existing.LatestStreamLabel
	td.LatestStreamArn = existing.LatestStreamArn
	td.StreamStatus = types.StreamStatusEnabled
	return nil
}

func (c *Catalog) newStreamDescriptor(td types.TableDescriptor) (*types.StreamDescriptor, error) {
	if !streamEnabled(td) {
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
		StreamViewType:          streamViewType(td),
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
			StreamViewType:          streamViewType(td),
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
	normalizeStreamMetadata(&desc)
	desc.StreamStatus = types.StreamStatusDisabled
	for i := range desc.Shards {
		if desc.Shards[i].SequenceNumberRange.EndingSequenceNumber == "" {
			desc.Shards[i].SequenceNumberRange.EndingSequenceNumber = ending
		}
	}
	return &desc, nil
}

func marshalStreamDescriptor(desc types.StreamDescriptor) ([]byte, error) {
	normalizeStreamMetadata(&desc)
	raw, err := json.Marshal(desc)
	if err != nil {
		return nil, fmt.Errorf("marshal stream descriptor: %w", err)
	}
	return raw, nil
}

func normalizeStreamMetadata(desc *types.StreamDescriptor) {
	if desc.StreamStatus == "" {
		desc.StreamStatus = types.StreamStatusEnabled
	}
	if len(desc.Shards) == 0 {
		desc.Shards = []types.StreamShardDescriptor{
			{
				ShardID: model.StreamShardIDSingle.String(),
				SequenceNumberRange: types.StreamSequenceNumberRange{
					StartingSequenceNumber: "1",
				},
			},
		}
		return
	}
	for i := range desc.Shards {
		if desc.Shards[i].ShardID == "" {
			desc.Shards[i].ShardID = model.StreamShardIDSingle.String()
		}
		if desc.Shards[i].SequenceNumberRange.StartingSequenceNumber == "" {
			desc.Shards[i].SequenceNumberRange.StartingSequenceNumber = "1"
		}
	}
}

func nextStreamLabel() string {
	streamLabelMu.Lock()
	defer streamLabelMu.Unlock()
	now := time.Now().UTC().UnixNano()
	if now <= lastStreamLabelUnixNano {
		now = lastStreamLabelUnixNano + 1
	}
	lastStreamLabelUnixNano = now
	return time.Unix(0, now).UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func streamARN(table, label string) string {
	return fmt.Sprintf("arn:cefas:dynamodb:local:000000000000:table/%s/stream/%s", table, label)
}

func cloneTableDescriptor(td types.TableDescriptor) types.TableDescriptor {
	if td.AttributeDefinitions != nil {
		td.AttributeDefinitions = append([]types.AttributeDefinition(nil), td.AttributeDefinitions...)
	}
	if td.GSIs != nil {
		gsis := make([]types.GSIDescriptor, len(td.GSIs))
		for i, gsi := range td.GSIs {
			gsi.Projection = cloneIndexProjection(gsi.Projection)
			if gsi.Projected != nil {
				gsi.Projected = append([]string(nil), gsi.Projected...)
			}
			gsis[i] = gsi
		}
		td.GSIs = gsis
	}
	if td.LSIs != nil {
		lsis := make([]types.LSIDescriptor, len(td.LSIs))
		for i, lsi := range td.LSIs {
			lsi.Projection = cloneIndexProjection(lsi.Projection)
			lsis[i] = lsi
		}
		td.LSIs = lsis
	}
	if td.SpatialIndexes != nil {
		spatial := make([]types.SpatialIndexDescriptor, len(td.SpatialIndexes))
		for i, idx := range td.SpatialIndexes {
			if idx.Attributes != nil {
				idx.Attributes = append([]string(nil), idx.Attributes...)
			}
			if idx.Ranges != nil {
				idx.Ranges = append([]types.NumRange(nil), idx.Ranges...)
			}
			spatial[i] = idx
		}
		td.SpatialIndexes = spatial
	}
	if td.StreamSpecification != nil {
		spec := *td.StreamSpecification
		td.StreamSpecification = &spec
	}
	return td
}

func cloneIndexProjection(in types.IndexProjection) types.IndexProjection {
	if in.Include != nil {
		in.Include = append([]string(nil), in.Include...)
	}
	return in
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
