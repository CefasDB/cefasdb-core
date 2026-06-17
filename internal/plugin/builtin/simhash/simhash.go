// Package simhash is the SimHash near-duplicate-detection index. One
// 64-bit SimHash per item; items are bucketed by an `prefix_bits`-bit
// prefix of the hash. Query probes the matching bucket plus optional
// neighbors within a configured Hamming radius and returns the
// candidates; callers post-filter with the hamming distance plugin
// (#133) for exact verification.
//
// Configuration:
//
//	{ "field": "<attribute>", "prefix_bits": <0..32>, "max_radius": <0..16> }
//
// Defaults: prefix_bits=16, max_radius=3.
package simhash

import (
	"encoding/json"
	"fmt"
	"hash/maphash"
	"math/bits"
	"sort"
	"sync"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/internal/pkid"
)

type Config struct {
	Field      string `json:"field"`
	PrefixBits int    `json:"prefix_bits,omitempty"`
	MaxRadius  int    `json:"max_radius,omitempty"`
}

var simSeed = maphash.MakeSeed()

// SimHash computes the SimHash of value's tokens. Tokenisation is
// trigram-shingle so similar strings produce similar hashes.
func SimHash(value string) uint64 {
	tokens := shingle(value, 3)
	var counts [64]int
	for _, tok := range tokens {
		h := maphash.Bytes(simSeed, []byte(tok))
		for i := 0; i < 64; i++ {
			if h&(1<<i) != 0 {
				counts[i]++
			} else {
				counts[i]--
			}
		}
	}
	var out uint64
	for i := 0; i < 64; i++ {
		if counts[i] > 0 {
			out |= 1 << i
		}
	}
	return out
}

func shingle(s string, n int) []string {
	r := []rune(s)
	if len(r) < n {
		return []string{string(r)}
	}
	out := make([]string, 0, len(r)-n+1)
	for i := 0; i <= len(r)-n; i++ {
		out = append(out, string(r[i:i+n]))
	}
	return out
}

type State struct {
	mu       sync.RWMutex
	cfg      Config
	ks       model.KeySchema
	items    map[string]model.Item
	hashes   map[string]uint64   // id → SimHash
	prefixes map[uint32][]string // prefix bits → ids
}

func newState(cfg Config, ks model.KeySchema) *State {
	return &State{
		cfg:      cfg,
		ks:       ks,
		items:    map[string]model.Item{},
		hashes:   map[string]uint64{},
		prefixes: map[uint32][]string{},
	}
}

func (s *State) addLocked(id string, item model.Item) {
	v, ok := pkid.FieldString(item, s.cfg.Field)
	if !ok {
		return
	}
	h := SimHash(v)
	s.hashes[id] = h
	s.items[id] = item
	prefix := uint32(h >> (64 - s.cfg.PrefixBits))
	s.prefixes[prefix] = append(s.prefixes[prefix], id)
}

func (s *State) removeLocked(id string) {
	h, ok := s.hashes[id]
	if !ok {
		return
	}
	prefix := uint32(h >> (64 - s.cfg.PrefixBits))
	bucket := s.prefixes[prefix]
	for i, x := range bucket {
		if x == id {
			s.prefixes[prefix] = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
	if len(s.prefixes[prefix]) == 0 {
		delete(s.prefixes, prefix)
	}
	delete(s.hashes, id)
	delete(s.items, id)
}

// ---------- plugin.IndexPlugin ----------

type Plugin struct {
	mu     sync.Mutex
	states map[string]*State
}

func NewPlugin() *Plugin { return &Plugin{states: map[string]*State{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "simhash",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "SimHash near-duplicate index with bucketed Hamming probing",
	}
}

func parseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("simhash: parse config: %w", err)
		}
	}
	if c.Field == "" {
		return c, fmt.Errorf("simhash: config.field required")
	}
	if c.PrefixBits == 0 {
		c.PrefixBits = 16
	}
	if c.PrefixBits < 0 || c.PrefixBits > 32 {
		return c, fmt.Errorf("simhash: prefix_bits must be in [0,32]")
	}
	if c.MaxRadius == 0 {
		c.MaxRadius = 3
	}
	if c.MaxRadius < 0 || c.MaxRadius > 16 {
		return c, fmt.Errorf("simhash: max_radius must be in [0,16]")
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
	probe := pickProbe(req)
	if probe == "" {
		return &sliceSet{}, nil
	}
	qh := SimHash(probe)
	s.mu.RLock()
	defer s.mu.RUnlock()
	radius := s.cfg.MaxRadius
	out := make([]plugin.Candidate, 0)
	seen := map[string]struct{}{}
	for prefix, ids := range s.prefixes {
		// Quick reject: prefix differs by more bits than radius
		// permits — we already know hamming(full, full) ≥
		// hamming(prefix, prefix), so prefix mismatch above radius
		// can't yield a candidate within radius on the full hash.
		queryPrefix := uint32(qh >> (64 - s.cfg.PrefixBits))
		if bits.OnesCount32(prefix^queryPrefix) > radius {
			continue
		}
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				continue
			}
			if bits.OnesCount64(s.hashes[id]^qh) <= radius {
				seen[id] = struct{}{}
				out = append(out, plugin.Candidate{Key: cloneItem(s.items[id])})
			}
		}
	}
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

func pickProbe(req plugin.IndexQuery) string {
	if v, ok := req.Binds[":value"]; ok {
		if v.T == model.AttrS {
			return v.S
		}
	}
	return req.Predicate
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
