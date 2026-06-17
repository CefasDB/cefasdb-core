// Package vectorlsh is the random-projection LSH index for vector
// similarity. Each item's vector is hashed L times; each hash is `b`
// bits derived from the sign of the dot product with `b` random
// hyperplanes. Items bucket per (sketch_id, b-bit hash); Query
// hashes the query vector the same way and unions the matching
// buckets.
//
// Configuration:
//
//	{ "field": "<attribute>", "dim": <vector-dimension>,
//	  "sketches": <L>, "bits_per_sketch": <b> }
//
// Defaults: sketches=8, bits_per_sketch=12 (≈ 32k buckets per sketch).
package vectorlsh

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/internal/pkid"
	"github.com/CefasDb/cefasdb/pkg/plugin/internal/vecattr"
)

type Config struct {
	Field         string `json:"field"`
	Dim           int    `json:"dim"`
	Sketches      int    `json:"sketches,omitempty"`
	BitsPerSketch int    `json:"bits_per_sketch,omitempty"`
}

type State struct {
	mu      sync.RWMutex
	cfg     Config
	ks      model.KeySchema
	planes  [][][]float64 // sketches × bits × dim
	items   map[string]model.Item
	vectors map[string][]float64  // id → vector (for diagnostics)
	buckets []map[uint64][]string // per sketch: bucket → ids
}

func newState(cfg Config, ks model.KeySchema) *State {
	rng := rand.New(rand.NewSource(1)) // deterministic per state
	planes := make([][][]float64, cfg.Sketches)
	for s := 0; s < cfg.Sketches; s++ {
		planes[s] = make([][]float64, cfg.BitsPerSketch)
		for b := 0; b < cfg.BitsPerSketch; b++ {
			plane := make([]float64, cfg.Dim)
			for d := 0; d < cfg.Dim; d++ {
				plane[d] = rng.NormFloat64()
			}
			planes[s][b] = plane
		}
	}
	buckets := make([]map[uint64][]string, cfg.Sketches)
	for i := range buckets {
		buckets[i] = map[uint64][]string{}
	}
	return &State{
		cfg:     cfg,
		ks:      ks,
		planes:  planes,
		items:   map[string]model.Item{},
		vectors: map[string][]float64{},
		buckets: buckets,
	}
}

func (s *State) hash(v []float64) []uint64 {
	out := make([]uint64, s.cfg.Sketches)
	for k := 0; k < s.cfg.Sketches; k++ {
		var h uint64
		for b := 0; b < s.cfg.BitsPerSketch; b++ {
			dot := 0.0
			plane := s.planes[k][b]
			for i, x := range v {
				dot += x * plane[i]
			}
			if dot >= 0 {
				h |= 1 << b
			}
		}
		out[k] = h
	}
	return out
}

func (s *State) addLocked(id string, item model.Item, vec []float64) {
	s.items[id] = item
	s.vectors[id] = vec
	for k, h := range s.hash(vec) {
		s.buckets[k][h] = append(s.buckets[k][h], id)
	}
}

func (s *State) removeLocked(id string) {
	vec, ok := s.vectors[id]
	if !ok {
		return
	}
	for k, h := range s.hash(vec) {
		bucket := s.buckets[k][h]
		for i, x := range bucket {
			if x == id {
				s.buckets[k][h] = append(bucket[:i], bucket[i+1:]...)
				break
			}
		}
		if len(s.buckets[k][h]) == 0 {
			delete(s.buckets[k], h)
		}
	}
	delete(s.items, id)
	delete(s.vectors, id)
}

// ---------- plugin.IndexPlugin ----------

type Plugin struct {
	mu     sync.Mutex
	states map[string]*State
}

func NewPlugin() *Plugin { return &Plugin{states: map[string]*State{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "vectorlsh",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Random-projection LSH index for vector similarity (cosine)",
	}
}

func parseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("vectorlsh: parse config: %w", err)
		}
	}
	if c.Field == "" {
		return c, fmt.Errorf("vectorlsh: config.field required")
	}
	if c.Dim <= 0 {
		return c, fmt.Errorf("vectorlsh: config.dim must be positive")
	}
	if c.Sketches == 0 {
		c.Sketches = 8
	}
	if c.BitsPerSketch == 0 {
		c.BitsPerSketch = 12
	}
	if c.BitsPerSketch > 60 {
		return c, fmt.Errorf("vectorlsh: bits_per_sketch must be <= 60")
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
	var inErr error
	items(func(it model.Item) bool {
		id, ok := pkid.Of(it, d.KeySchema)
		if !ok {
			return true
		}
		v, ok := it[cfg.Field]
		if !ok {
			return true
		}
		vec, err := vecattr.AsFloats(v)
		if err != nil {
			inErr = err
			return false
		}
		if len(vec) != cfg.Dim {
			inErr = fmt.Errorf("vectorlsh: vector dim %d != config.dim %d", len(vec), cfg.Dim)
			return false
		}
		fresh.addLocked(id, it, vec)
		return true
	})
	if inErr != nil {
		return inErr
	}
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
		v, ok := newItem[s.cfg.Field]
		if !ok {
			return nil
		}
		vec, err := vecattr.AsFloats(v)
		if err != nil {
			return err
		}
		if len(vec) != s.cfg.Dim {
			return fmt.Errorf("vectorlsh: vector dim %d != config.dim %d", len(vec), s.cfg.Dim)
		}
		if id, ok := pkid.Of(newItem, s.ks); ok {
			s.addLocked(id, newItem, vec)
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
	vec, ok := probeVec(req)
	if !ok || len(vec) != s.cfg.Dim {
		return &sliceSet{}, nil
	}
	s.mu.RLock()
	seen := map[string]struct{}{}
	for k, h := range s.hash(vec) {
		for _, id := range s.buckets[k][h] {
			seen[id] = struct{}{}
		}
	}
	out := make([]plugin.Candidate, 0, len(seen))
	for id := range seen {
		out = append(out, plugin.Candidate{Key: cloneItem(s.items[id]), Score: cosineScore(vec, s.vectors[id])})
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		ai, _ := pkid.Of(out[i].Key, s.ks)
		aj, _ := pkid.Of(out[j].Key, s.ks)
		return ai < aj
	})
	if req.Limit > 0 && req.Limit < len(out) {
		out = out[:req.Limit]
	}
	return &sliceSet{rows: out}, nil
}

func cosineScore(a, b []float64) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	score := dot / (math.Sqrt(na) * math.Sqrt(nb))
	if score > 1 {
		return 1
	}
	if score < -1 {
		return -1
	}
	return score
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

func probeVec(req plugin.IndexQuery) ([]float64, bool) {
	v, ok := req.Binds[":vector"]
	if !ok {
		return nil, false
	}
	out, err := vecattr.AsFloats(v)
	if err != nil {
		return nil, false
	}
	return out, true
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
