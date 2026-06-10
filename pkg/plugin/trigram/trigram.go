// Package trigram is the trigram-shingle inverted-index plugin. Each
// item's configured field is shingled into overlapping 3-rune
// trigrams; per trigram the index keeps the set of item ids. Query
// shingles the candidate value the same way and intersects the
// posting lists.
//
// Configuration:
//
//	{ "field": "<attribute>", "min_overlap": <ratio> }
//
// `min_overlap` defaults to 0.5 — a candidate must share at least
// 50% of the query's trigrams. Lower it for fuzzier matches.
package trigram

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/internal/pkid"
)

type Config struct {
	Field      string  `json:"field"`
	MinOverlap float64 `json:"min_overlap,omitempty"`
}

type State struct {
	mu       sync.RWMutex
	cfg      Config
	ks       model.KeySchema
	postings map[string]map[string]struct{} // trigram → set of ids
	items    map[string]model.Item          // id → item (for Candidate output)
	tris     map[string][]string            // id → trigrams (so Update can remove old)
}

func newState(cfg Config, ks model.KeySchema) *State {
	return &State{
		cfg:      cfg,
		ks:       ks,
		postings: map[string]map[string]struct{}{},
		items:    map[string]model.Item{},
		tris:     map[string][]string{},
	}
}

// Shingle produces overlapping 3-rune trigrams. Strings shorter than
// 3 runes hash as a single shingle of the whole string so very short
// names still produce a non-empty index.
func Shingle(s string) []string {
	r := []rune(s)
	if len(r) < 3 {
		if len(r) == 0 {
			return nil
		}
		return []string{string(r)}
	}
	out := make([]string, 0, len(r)-2)
	for i := 0; i <= len(r)-3; i++ {
		out = append(out, string(r[i:i+3]))
	}
	return out
}

func (s *State) addLocked(id string, item model.Item) {
	v, ok := pkid.FieldString(item, s.cfg.Field)
	if !ok {
		return
	}
	tris := Shingle(v)
	s.tris[id] = tris
	s.items[id] = item
	for _, t := range tris {
		bucket, ok := s.postings[t]
		if !ok {
			bucket = map[string]struct{}{}
			s.postings[t] = bucket
		}
		bucket[id] = struct{}{}
	}
}

func (s *State) removeLocked(id string) {
	tris, ok := s.tris[id]
	if !ok {
		return
	}
	for _, t := range tris {
		if bucket, ok := s.postings[t]; ok {
			delete(bucket, id)
			if len(bucket) == 0 {
				delete(s.postings, t)
			}
		}
	}
	delete(s.tris, id)
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
		Name:        "trigram",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Trigram inverted-index for fuzzy text candidate generation",
	}
}

func parseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("trigram: parse config: %w", err)
		}
	}
	if c.Field == "" {
		return c, fmt.Errorf("trigram: config.field required")
	}
	if c.MinOverlap == 0 {
		c.MinOverlap = 0.5
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
	tris := Shingle(probe)
	if len(tris) == 0 {
		return &sliceSet{}, nil
	}
	threshold := int(float64(len(tris))*s.cfg.MinOverlap + 0.5)
	if threshold < 1 {
		threshold = 1
	}
	s.mu.RLock()
	counts := make(map[string]int, len(tris)*2)
	for _, t := range tris {
		if bucket, ok := s.postings[t]; ok {
			for id := range bucket {
				counts[id]++
			}
		}
	}
	out := make([]plugin.Candidate, 0, len(counts))
	for id, c := range counts {
		if c < threshold {
			continue
		}
		out = append(out, plugin.Candidate{Key: cloneItem(s.items[id]), Score: float64(c) / float64(len(tris))})
	}
	s.mu.RUnlock()
	// Best overlap first so the post-filter sees the most promising
	// candidates upfront.
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
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
		if s, ok := stringOf(v); ok {
			return s
		}
	}
	return req.Predicate
}

func stringOf(av model.AttributeValue) (string, bool) {
	switch av.T {
	case model.AttrS:
		return av.S, true
	case model.AttrN:
		return av.N, true
	}
	return "", false
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
