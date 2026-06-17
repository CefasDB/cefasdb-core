// Package roaring is a Roaring-bitmap cohort index plugin. Each
// cohort (named via index.Descriptor.Name) maps to a compressed
// uint32 bitmap; members add/remove cheaply and cohorts intersect /
// union without enumerating elements.
//
// Configuration:
//
//	{ "field": "<attribute>" }
//
// The attribute MUST be numeric and fit in uint32 — Roaring keys are
// 32-bit. Items whose field is missing or non-numeric are skipped on
// Build / Update / Delete.
//
// Backed by github.com/RoaringBitmap/roaring/v2.
package roaring

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	roar "github.com/RoaringBitmap/roaring/v2"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

type Config struct {
	Field string `json:"field"`
}

func parseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("roaring: parse config: %w", err)
		}
	}
	if c.Field == "" {
		return c, fmt.Errorf("roaring: config.field required")
	}
	return c, nil
}

// Cohort wraps the Roaring bitmap behind a mutex so the engine can
// share it across goroutines.
type Cohort struct {
	mu  sync.RWMutex
	cfg Config
	bm  *roar.Bitmap
}

func NewCohort(cfg Config) *Cohort { return &Cohort{cfg: cfg, bm: roar.New()} }

// Add inserts id into the cohort. Returns true iff id was new.
func (c *Cohort) Add(id uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bm.CheckedAdd(id)
}

// Remove evicts id; returns true iff id was present.
func (c *Cohort) Remove(id uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bm.CheckedRemove(id)
}

// Contains reports membership.
func (c *Cohort) Contains(id uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bm.Contains(id)
}

// Cardinality returns the exact (not estimated) member count.
func (c *Cohort) Cardinality() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bm.GetCardinality()
}

// Intersect returns a new Cohort containing members present in both.
// Caller-supplied config carries over from c.
func (c *Cohort) Intersect(other *Cohort) *Cohort {
	c.mu.RLock()
	other.mu.RLock()
	defer c.mu.RUnlock()
	defer other.mu.RUnlock()
	out := NewCohort(c.cfg)
	out.bm = roar.And(c.bm, other.bm)
	return out
}

// Union returns a new Cohort with every member from either.
func (c *Cohort) Union(other *Cohort) *Cohort {
	c.mu.RLock()
	other.mu.RLock()
	defer c.mu.RUnlock()
	defer other.mu.RUnlock()
	out := NewCohort(c.cfg)
	out.bm = roar.Or(c.bm, other.bm)
	return out
}

// Serialize emits the Roaring portable byte stream prefixed by a
// length-tagged JSON config.
func (c *Cohort) Serialize() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cfg, err := json.Marshal(c.cfg)
	if err != nil {
		return nil, err
	}
	bm, err := c.bm.ToBytes()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 4+len(cfg)+len(bm))
	if uint64(len(cfg)) > 0xffff_ffff {
		return nil, errors.New("roaring: config too large")
	}
	// little-endian length so it matches the Roaring serialization's
	// internal endianness.
	out[0] = byte(len(cfg))
	out[1] = byte(len(cfg) >> 8)
	out[2] = byte(len(cfg) >> 16)
	out[3] = byte(len(cfg) >> 24)
	copy(out[4:], cfg)
	copy(out[4+len(cfg):], bm)
	return out, nil
}

func Deserialize(buf []byte) (*Cohort, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("roaring: payload too short")
	}
	cfgLen := int(buf[0]) | int(buf[1])<<8 | int(buf[2])<<16 | int(buf[3])<<24
	if cfgLen < 0 || cfgLen > len(buf)-4 {
		return nil, fmt.Errorf("roaring: config length out of range")
	}
	cfg, err := parseConfig(buf[4 : 4+cfgLen])
	if err != nil {
		return nil, err
	}
	bm := roar.New()
	if _, err := bm.FromBuffer(buf[4+cfgLen:]); err != nil {
		return nil, err
	}
	return &Cohort{cfg: cfg, bm: bm}, nil
}

// ---------- plugin.IndexPlugin ----------

type Plugin struct {
	mu      sync.Mutex
	cohorts map[string]*Cohort
}

func NewPlugin() *Plugin { return &Plugin{cohorts: map[string]*Cohort{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "roaring",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Roaring-bitmap cohort index over uint32 attributes",
	}
}

func key(d index.Descriptor) string { return d.Table + "/" + d.Name }

func (p *Plugin) cohortFor(d index.Descriptor) (*Cohort, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.cohorts[key(d)]; ok {
		return c, nil
	}
	cfg, err := parseConfig(d.PluginConfig)
	if err != nil {
		return nil, err
	}
	c := NewCohort(cfg)
	p.cohorts[key(d)] = c
	return c, nil
}

func (p *Plugin) Build(d index.Descriptor, items func(yield func(model.Item) bool)) error {
	cfg, err := parseConfig(d.PluginConfig)
	if err != nil {
		return err
	}
	c := NewCohort(cfg)
	var inErr error
	items(func(it model.Item) bool {
		id, err := pickID(it, cfg.Field)
		if err != nil {
			inErr = err
			return false
		}
		if id != nil {
			c.Add(*id)
		}
		return true
	})
	if inErr != nil {
		return inErr
	}
	p.mu.Lock()
	p.cohorts[key(d)] = c
	p.mu.Unlock()
	return nil
}

func (p *Plugin) Update(d index.Descriptor, oldItem, newItem model.Item) error {
	c, err := p.cohortFor(d)
	if err != nil {
		return err
	}
	if oldItem != nil {
		id, err := pickID(oldItem, c.cfg.Field)
		if err != nil {
			return err
		}
		if id != nil {
			c.Remove(*id)
		}
	}
	if newItem != nil {
		id, err := pickID(newItem, c.cfg.Field)
		if err != nil {
			return err
		}
		if id != nil {
			c.Add(*id)
		}
	}
	return nil
}

func (p *Plugin) Delete(d index.Descriptor, keyItem model.Item) error {
	c, err := p.cohortFor(d)
	if err != nil {
		return err
	}
	if id, _ := pickID(keyItem, c.cfg.Field); id != nil {
		c.Remove(*id)
	}
	return nil
}

func (p *Plugin) Query(d index.Descriptor, req plugin.IndexQuery) (plugin.CandidateSet, error) {
	c, err := p.cohortFor(d)
	if err != nil {
		return nil, err
	}
	probe, err := pickProbeID(req)
	if err != nil {
		return nil, err
	}
	if probe == nil {
		// No probe → stream every cohort member as a candidate.
		return &cohortIter{cfg: c.cfg, iter: c.bm.Iterator()}, nil
	}
	if c.Contains(*probe) {
		return &cohortIter{cfg: c.cfg, single: probe}, nil
	}
	return &cohortIter{}, nil
}

func (p *Plugin) Estimate(d index.Descriptor, req plugin.IndexQuery) (int, error) {
	c, err := p.cohortFor(d)
	if err != nil {
		return 0, err
	}
	probe, err := pickProbeID(req)
	if err != nil {
		return 0, err
	}
	if probe == nil {
		return int(c.Cardinality()), nil
	}
	if c.Contains(*probe) {
		return 1, nil
	}
	return 0, nil
}

func pickID(it model.Item, field string) (*uint32, error) {
	av, ok := it[field]
	if !ok {
		return nil, nil
	}
	if av.T != model.AttrN {
		return nil, fmt.Errorf("roaring: attribute %q must be numeric (got %v)", field, av.T)
	}
	n, err := strconv.ParseUint(av.N, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("roaring: parse %q: %w", av.N, err)
	}
	v := uint32(n)
	return &v, nil
}

func pickProbeID(req plugin.IndexQuery) (*uint32, error) {
	if v, ok := req.Binds[":id"]; ok {
		return pickID(model.Item{"_": v}, "_")
	}
	if req.Predicate == "" {
		return nil, nil
	}
	n, err := strconv.ParseUint(req.Predicate, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("roaring: parse predicate %q: %w", req.Predicate, err)
	}
	v := uint32(n)
	return &v, nil
}

type cohortIter struct {
	cfg    Config
	single *uint32
	iter   roar.IntIterable
}

func (c *cohortIter) Next() (plugin.Candidate, bool) {
	if c.single != nil {
		out := plugin.Candidate{Key: model.Item{c.cfg.Field: numAttr(*c.single)}}
		c.single = nil
		return out, true
	}
	if c.iter == nil || !c.iter.HasNext() {
		return plugin.Candidate{}, false
	}
	id := c.iter.Next()
	return plugin.Candidate{Key: model.Item{c.cfg.Field: numAttr(id)}}, true
}
func (*cohortIter) Err() error   { return nil }
func (*cohortIter) Close() error { return nil }

func numAttr(v uint32) model.AttributeValue {
	return model.AttributeValue{T: model.AttrN, N: strconv.FormatUint(uint64(v), 10)}
}

func init() { plugin.Default.MustRegister(NewPlugin()) }
