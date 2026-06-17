// Package cbloom is a Counting Bloom Filter index plugin. Same
// false-positive contract as plain Bloom, but each bucket is a
// counter so Delete is correct (decrementing one slot per hash).
//
// Configuration:
//
//	{ "field": "<attribute>", "m": <buckets>, "k": <hashes>, "width": <bits-per-bucket> }
//
// `width` defaults to 4 when omitted; valid range is 1..16 (counters
// saturate, so size enough headroom to avoid clamping).
package cbloom

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/internal/hashfield"
)

type Config struct {
	Field string `json:"field"`
	M     int    `json:"m"`
	K     int    `json:"k"`
	Width int    `json:"width,omitempty"`
}

type Filter struct {
	mu       sync.RWMutex
	cfg      Config
	counters []uint16 // up to 16 bits per bucket; saturates at maxCount
	maxCount uint16
}

func New(raw []byte) (*Filter, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("cbloom: parse config: %w", err)
		}
	}
	if cfg.Field == "" {
		return nil, fmt.Errorf("cbloom: config.field required")
	}
	if cfg.M <= 0 || cfg.K <= 0 {
		return nil, fmt.Errorf("cbloom: config.m and config.k must be positive")
	}
	if cfg.Width == 0 {
		cfg.Width = 4
	}
	if cfg.Width < 1 || cfg.Width > 16 {
		return nil, fmt.Errorf("cbloom: width must be in [1,16]")
	}
	maxCount := uint16((1 << cfg.Width) - 1)
	return &Filter{cfg: cfg, counters: make([]uint16, cfg.M), maxCount: maxCount}, nil
}

func (f *Filter) positions(value []byte) [16]uint64 {
	var out [16]uint64
	h1, h2 := hashPair(value)
	for i := 0; i < f.cfg.K; i++ {
		out[i] = (h1 + uint64(i)*h2) % uint64(f.cfg.M)
	}
	return out
}

func (f *Filter) Add(value []byte) {
	pos := f.positions(value)
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < f.cfg.K; i++ {
		if f.counters[pos[i]] < f.maxCount {
			f.counters[pos[i]]++
		}
	}
}

// Remove decrements every counter touched by value. Counters that
// already saturated never decrement (so historical adds can keep them
// pinned); callers needing exact deletes must size `width` to avoid
// saturation.
func (f *Filter) Remove(value []byte) {
	pos := f.positions(value)
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < f.cfg.K; i++ {
		c := f.counters[pos[i]]
		if c == 0 || c == f.maxCount {
			continue
		}
		f.counters[pos[i]] = c - 1
	}
}

func (f *Filter) Contains(value []byte) bool {
	pos := f.positions(value)
	f.mu.RLock()
	defer f.mu.RUnlock()
	for i := 0; i < f.cfg.K; i++ {
		if f.counters[pos[i]] == 0 {
			return false
		}
	}
	return true
}

func (f *Filter) Serialize() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cfg, err := json.Marshal(f.cfg)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 4+len(cfg)+2*len(f.counters))
	binary.BigEndian.PutUint32(out, uint32(len(cfg)))
	copy(out[4:], cfg)
	for i, c := range f.counters {
		binary.BigEndian.PutUint16(out[4+len(cfg)+i*2:], c)
	}
	return out, nil
}

func Deserialize(buf []byte) (*Filter, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("cbloom: payload too short")
	}
	cfgLen := binary.BigEndian.Uint32(buf)
	if int(cfgLen) > len(buf)-4 {
		return nil, fmt.Errorf("cbloom: config length out of range")
	}
	var cfg Config
	if err := json.Unmarshal(buf[4:4+cfgLen], &cfg); err != nil {
		return nil, err
	}
	body := buf[4+cfgLen:]
	if len(body)%2 != 0 {
		return nil, fmt.Errorf("cbloom: counter length not multiple of 2")
	}
	counters := make([]uint16, len(body)/2)
	for i := range counters {
		counters[i] = binary.BigEndian.Uint16(body[i*2:])
	}
	if cfg.Width == 0 {
		cfg.Width = 4
	}
	return &Filter{cfg: cfg, counters: counters, maxCount: uint16((1 << cfg.Width) - 1)}, nil
}

// ---------- plugin.IndexPlugin ----------

type Plugin struct {
	mu      sync.Mutex
	filters map[string]*Filter
}

func NewPlugin() *Plugin { return &Plugin{filters: map[string]*Filter{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "cbloom",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Counting Bloom filter — Bloom semantics with delete support",
	}
}

func key(d index.Descriptor) string { return d.Table + "/" + d.Name }

func (p *Plugin) filterFor(d index.Descriptor) (*Filter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if f, ok := p.filters[key(d)]; ok {
		return f, nil
	}
	f, err := New(d.PluginConfig)
	if err != nil {
		return nil, err
	}
	p.filters[key(d)] = f
	return f, nil
}

func (p *Plugin) Build(d index.Descriptor, items func(yield func(model.Item) bool)) error {
	f, err := New(d.PluginConfig)
	if err != nil {
		return err
	}
	var inErr error
	items(func(it model.Item) bool {
		v, err := hashfield.Extract(it, f.cfg.Field)
		if err != nil {
			inErr = err
			return false
		}
		if v != nil {
			f.Add(v)
		}
		return true
	})
	if inErr != nil {
		return inErr
	}
	p.mu.Lock()
	p.filters[key(d)] = f
	p.mu.Unlock()
	return nil
}

func (p *Plugin) Update(d index.Descriptor, oldItem, newItem model.Item) error {
	f, err := p.filterFor(d)
	if err != nil {
		return err
	}
	if oldItem != nil {
		if v, _ := hashfield.Extract(oldItem, f.cfg.Field); v != nil {
			f.Remove(v)
		}
	}
	if newItem != nil {
		if v, _ := hashfield.Extract(newItem, f.cfg.Field); v != nil {
			f.Add(v)
		}
	}
	return nil
}

func (p *Plugin) Delete(d index.Descriptor, keyItem model.Item) error {
	f, err := p.filterFor(d)
	if err != nil {
		return err
	}
	if v, _ := hashfield.Extract(keyItem, f.cfg.Field); v != nil {
		f.Remove(v)
	}
	return nil
}

func (p *Plugin) Query(d index.Descriptor, req plugin.IndexQuery) (plugin.CandidateSet, error) {
	f, err := p.filterFor(d)
	if err != nil {
		return nil, err
	}
	probe, err := pickProbe(req)
	if err != nil {
		return nil, err
	}
	if f.Contains(probe) {
		return newSingleton(model.Item{f.cfg.Field: {T: model.AttrS, S: string(probe)}}), nil
	}
	return empty{}, nil
}

func (p *Plugin) Estimate(d index.Descriptor, req plugin.IndexQuery) (int, error) {
	cs, err := p.Query(d, req)
	if err != nil {
		return 0, err
	}
	defer cs.Close()
	if _, ok := cs.Next(); ok {
		return 1, nil
	}
	return 0, nil
}

// ---------- helpers ----------

func hashPair(value []byte) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write(value)
	a := h.Sum64()
	h.Reset()
	_, _ = h.Write([]byte{0xa5})
	_, _ = h.Write(value)
	return a, h.Sum64()
}

func pickProbe(req plugin.IndexQuery) ([]byte, error) {
	if v, ok := req.Binds[":value"]; ok {
		return hashfield.Extract(model.Item{"_": v}, "_")
	}
	return []byte(req.Predicate), nil
}

type singleton struct {
	item    model.Item
	emitted bool
}

func (s *singleton) Next() (plugin.Candidate, bool) {
	if s.emitted {
		return plugin.Candidate{}, false
	}
	s.emitted = true
	return plugin.Candidate{Key: s.item}, true
}
func (*singleton) Err() error   { return nil }
func (*singleton) Close() error { return nil }

func newSingleton(it model.Item) plugin.CandidateSet { return &singleton{item: it} }

type empty struct{}

func (empty) Next() (plugin.Candidate, bool) { return plugin.Candidate{}, false }
func (empty) Err() error                     { return nil }
func (empty) Close() error                   { return nil }

func init() { plugin.Default.MustRegister(NewPlugin()) }
