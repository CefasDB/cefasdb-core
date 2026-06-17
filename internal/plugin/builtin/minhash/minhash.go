// Package minhash is the MinHash-LSH similarity-index plugin. Each
// item's set-valued field is reduced to K MinHash signatures using K
// hash seeds; the signatures are partitioned into bands of `r`
// hashes each (numBands = K / r). Items are bucketed by the
// per-band hash of their signature slice; Query reproduces the same
// signatures and probes the matching buckets so candidates have
// guaranteed high estimated Jaccard.
//
// Configuration:
//
//	{ "field": "<attribute>", "k": <signatures>, "r": <hashes-per-band> }
//
// Defaults: k=128, r=8 → 16 bands.
package minhash

import (
	"encoding/json"
	"fmt"
	"hash/maphash"
	"sort"
	"sync"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/internal/pkid"
)

type Config struct {
	Field string `json:"field"`
	K     int    `json:"k,omitempty"`
	R     int    `json:"r,omitempty"`
}

type State struct {
	mu      sync.RWMutex
	cfg     Config
	ks      model.KeySchema
	seeds   []maphash.Seed
	bands   int
	items   map[string]model.Item // id → item
	sigs    map[string][]uint64   // id → K signatures
	buckets []map[uint64][]string // per-band hash → ids
}

func newState(cfg Config, ks model.KeySchema) *State {
	bands := cfg.K / cfg.R
	seeds := make([]maphash.Seed, cfg.K)
	for i := range seeds {
		// Same-process MakeSeed varies per call; that's fine for a
		// session, and serializing MinHash state across processes is
		// out of scope for v1.
		seeds[i] = maphash.MakeSeed()
	}
	buckets := make([]map[uint64][]string, bands)
	for i := range buckets {
		buckets[i] = map[uint64][]string{}
	}
	return &State{
		cfg:     cfg,
		ks:      ks,
		seeds:   seeds,
		bands:   bands,
		items:   map[string]model.Item{},
		sigs:    map[string][]uint64{},
		buckets: buckets,
	}
}

// signature computes the K MinHash signatures of values. Set inputs
// are deduplicated implicitly because MinHash of a set is invariant
// under multiplicity.
func (s *State) signature(values []string) []uint64 {
	sig := make([]uint64, s.cfg.K)
	const inf = ^uint64(0)
	for i := range sig {
		sig[i] = inf
	}
	for _, v := range values {
		for k := range sig {
			h := maphash.Bytes(s.seeds[k], []byte(v))
			if h < sig[k] {
				sig[k] = h
			}
		}
	}
	return sig
}

// foldBand folds r 64-bit signatures into a single 64-bit band hash
// via FNV-style mixing. Deterministic across calls + processes.
func foldBand(rs []uint64) uint64 {
	const off64 = 0xcbf29ce484222325
	const prime64 = 0x100000001b3
	h := uint64(off64)
	for _, v := range rs {
		h ^= v
		h *= prime64
	}
	return h
}

func (s *State) addLocked(id string, item model.Item) {
	values := extractValues(item, s.cfg.Field)
	if len(values) == 0 {
		return
	}
	sig := s.signature(values)
	s.sigs[id] = sig
	s.items[id] = item
	for b := 0; b < s.bands; b++ {
		bh := foldBand(sig[b*s.cfg.R : (b+1)*s.cfg.R])
		s.buckets[b][bh] = append(s.buckets[b][bh], id)
	}
}

func (s *State) removeLocked(id string) {
	sig, ok := s.sigs[id]
	if !ok {
		return
	}
	for b := 0; b < s.bands; b++ {
		bh := foldBand(sig[b*s.cfg.R : (b+1)*s.cfg.R])
		bucket := s.buckets[b][bh]
		for i, x := range bucket {
			if x == id {
				s.buckets[b][bh] = append(bucket[:i], bucket[i+1:]...)
				break
			}
		}
		if len(s.buckets[b][bh]) == 0 {
			delete(s.buckets[b], bh)
		}
	}
	delete(s.sigs, id)
	delete(s.items, id)
}

func extractValues(item model.Item, field string) []string {
	av, ok := item[field]
	if !ok {
		return nil
	}
	switch av.T {
	case model.AttrSS:
		out := make([]string, len(av.SS))
		copy(out, av.SS)
		return out
	case model.AttrL:
		out := make([]string, 0, len(av.L))
		for _, e := range av.L {
			if e.T == model.AttrS {
				out = append(out, e.S)
			}
		}
		return out
	case model.AttrS:
		// Treat as a single-element set, or shingle for trigram-style
		// fuzz? Single element matches the MinHash contract; the
		// trigram plugin (#143) covers fuzzy-text candidate gen.
		return []string{av.S}
	}
	return nil
}

// ---------- plugin.IndexPlugin ----------

type Plugin struct {
	mu     sync.Mutex
	states map[string]*State
}

func NewPlugin() *Plugin { return &Plugin{states: map[string]*State{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "minhash",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "MinHash-LSH similarity index over SS / L-of-S attributes",
	}
}

func parseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("minhash: parse config: %w", err)
		}
	}
	if c.Field == "" {
		return c, fmt.Errorf("minhash: config.field required")
	}
	if c.K == 0 {
		c.K = 128
	}
	if c.R == 0 {
		c.R = 8
	}
	if c.K%c.R != 0 {
		return c, fmt.Errorf("minhash: k=%d must be divisible by r=%d", c.K, c.R)
	}
	return c, nil
}

func key(d index.Descriptor) string { return d.Table + "/" + d.Name }

func (p *Plugin) stateFor(d index.Descriptor) (*State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.states[key(d)]; ok {
		return s, nil
	}
	cfg, err := parseConfig(d.PluginConfig)
	if err != nil {
		return nil, err
	}
	s := newState(cfg, d.KeySchema)
	p.states[key(d)] = s
	return s, nil
}

func (p *Plugin) Build(d index.Descriptor, items func(yield func(model.Item) bool)) error {
	cfg, err := parseConfig(d.PluginConfig)
	if err != nil {
		return err
	}
	fresh := newState(cfg, d.KeySchema)
	items(func(it model.Item) bool {
		id, ok := pkid.Of(it, d.KeySchema)
		if !ok {
			return true
		}
		fresh.addLocked(id, it)
		return true
	})
	p.mu.Lock()
	p.states[key(d)] = fresh
	p.mu.Unlock()
	return nil
}

func (p *Plugin) Update(d index.Descriptor, oldItem, newItem model.Item) error {
	s, err := p.stateFor(d)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldItem != nil {
		if id, ok := pkid.Of(oldItem, s.ks); ok {
			s.removeLocked(id)
		}
	}
	if newItem != nil {
		if id, ok := pkid.Of(newItem, s.ks); ok {
			s.addLocked(id, newItem)
		}
	}
	return nil
}

func (p *Plugin) Delete(d index.Descriptor, keyItem model.Item) error {
	s, err := p.stateFor(d)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := pkid.Of(keyItem, s.ks); ok {
		s.removeLocked(id)
	}
	return nil
}

func (p *Plugin) Query(d index.Descriptor, req plugin.IndexQuery) (plugin.CandidateSet, error) {
	s, err := p.stateFor(d)
	if err != nil {
		return nil, err
	}
	values, ok := probeValues(req)
	if !ok || len(values) == 0 {
		return &sliceSet{}, nil
	}
	s.mu.RLock()
	sig := s.signature(values)
	seen := map[string]struct{}{}
	for b := 0; b < s.bands; b++ {
		bh := foldBand(sig[b*s.cfg.R : (b+1)*s.cfg.R])
		for _, id := range s.buckets[b][bh] {
			seen[id] = struct{}{}
		}
	}
	out := make([]plugin.Candidate, 0, len(seen))
	for id := range seen {
		out = append(out, plugin.Candidate{Key: cloneItem(s.items[id])})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		ai, _ := pkid.Of(out[i].Key, s.ks)
		aj, _ := pkid.Of(out[j].Key, s.ks)
		return ai < aj
	})
	if req.Limit > 0 && req.Limit < len(out) {
		out = out[:req.Limit]
	}
	return &sliceSet{rows: out}, nil
}

func (p *Plugin) Estimate(d index.Descriptor, req plugin.IndexQuery) (int, error) {
	cs, err := p.Query(d, req)
	if err != nil {
		return 0, err
	}
	defer cs.Close()
	n := 0
	for {
		if _, ok := cs.Next(); !ok {
			break
		}
		n++
	}
	return n, nil
}

func probeValues(req plugin.IndexQuery) ([]string, bool) {
	if v, ok := req.Binds[":values"]; ok {
		if v.T == model.AttrSS {
			return v.SS, true
		}
	}
	if v, ok := req.Binds[":value"]; ok {
		if v.T == model.AttrS {
			return []string{v.S}, true
		}
	}
	if req.Predicate != "" {
		return []string{req.Predicate}, true
	}
	return nil, false
}

func cloneItem(in model.Item) model.Item {
	out := make(model.Item, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type sliceSet struct {
	rows []plugin.Candidate
	i    int
}

func (s *sliceSet) Next() (plugin.Candidate, bool) {
	if s.i >= len(s.rows) {
		return plugin.Candidate{}, false
	}
	out := s.rows[s.i]
	s.i++
	return out, true
}
func (*sliceSet) Err() error   { return nil }
func (*sliceSet) Close() error { return nil }

func init() { plugin.Default.MustRegister(NewPlugin()) }
