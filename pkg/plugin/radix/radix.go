// Package radix is the prefix-index plugin. Internally it keeps the
// indexed field values sorted; prefix queries are O(log n) via
// binary search for the lower / upper bound. The data model is
// equivalent to a radix tree at the API surface — same prefix
// semantics, simpler implementation, no per-node overhead.
//
// Configuration:
//
//	{ "field": "<attribute>" }
package radix

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/internal/pkid"
)

type Config struct {
	Field string `json:"field"`
}

type entry struct {
	value string
	id    string
	item  model.Item // kept so Query / Estimate can return rich keys
}

// State is the per-(table,index) index state.
type State struct {
	mu      sync.RWMutex
	cfg     Config
	ks      model.KeySchema
	entries []entry // sorted by entry.value
}

func newState(cfg Config, ks model.KeySchema) *State { return &State{cfg: cfg, ks: ks} }

func (s *State) replaceLocked(value, id string, item model.Item) {
	idx := sort.Search(len(s.entries), func(i int) bool {
		if s.entries[i].id == id {
			return true
		}
		return false
	})
	// Linear scan by id (alongside the sorted-by-value primary order)
	// because id != value; sorted lookup by value can't substitute.
	for i, e := range s.entries {
		if e.id == id {
			// remove old entry
			s.entries = append(s.entries[:i], s.entries[i+1:]...)
			break
		}
	}
	_ = idx
	// Insert at the right value-sorted position.
	pos := sort.Search(len(s.entries), func(i int) bool {
		return s.entries[i].value >= value
	})
	s.entries = append(s.entries, entry{})
	copy(s.entries[pos+1:], s.entries[pos:])
	s.entries[pos] = entry{value: value, id: id, item: cloneItem(item)}
}

func (s *State) removeLocked(id string) {
	for i, e := range s.entries {
		if e.id == id {
			s.entries = append(s.entries[:i], s.entries[i+1:]...)
			return
		}
	}
}

func cloneItem(in model.Item) model.Item {
	out := make(model.Item, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// PrefixRange returns the half-open slice indices [lo, hi) of every
// entry whose value starts with prefix.
func (s *State) PrefixRange(prefix string) (lo, hi int) {
	lo = sort.Search(len(s.entries), func(i int) bool { return s.entries[i].value >= prefix })
	// upper bound: first entry whose value > prefix + maxRune. Simpler:
	// find first entry that doesn't start with prefix at or after lo.
	hi = lo
	for hi < len(s.entries) && strings.HasPrefix(s.entries[hi].value, prefix) {
		hi++
	}
	return lo, hi
}

// ---------- plugin.IndexPlugin ----------

type Plugin struct {
	mu     sync.Mutex
	states map[string]*State
}

func NewPlugin() *Plugin { return &Plugin{states: map[string]*State{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "radix",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Sorted-prefix index — O(log n) prefix queries over a string attribute",
	}
}

func parseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("radix: parse config: %w", err)
		}
	}
	if c.Field == "" {
		return c, fmt.Errorf("radix: config.field required")
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
		v, ok := pkid.FieldString(it, cfg.Field)
		if !ok {
			return true // skip items missing the indexed field
		}
		id, ok := pkid.Of(it, d.KeySchema)
		if !ok {
			return true
		}
		fresh.entries = append(fresh.entries, entry{value: v, id: id, item: cloneItem(it)})
		return true
	})
	sort.Slice(fresh.entries, func(i, j int) bool { return fresh.entries[i].value < fresh.entries[j].value })
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
		v, ok := pkid.FieldString(newItem, s.cfg.Field)
		if !ok {
			return nil
		}
		id, ok := pkid.Of(newItem, s.ks)
		if !ok {
			return nil
		}
		s.replaceLocked(v, id, newItem)
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
	prefix := pickProbe(req)
	s.mu.RLock()
	lo, hi := s.PrefixRange(prefix)
	matches := make([]plugin.Candidate, hi-lo)
	for i := lo; i < hi; i++ {
		matches[i-lo] = plugin.Candidate{Key: cloneItem(s.entries[i].item)}
	}
	limit := req.Limit
	s.mu.RUnlock()
	if limit > 0 && limit < len(matches) {
		matches = matches[:limit]
	}
	return &sliceSet{rows: matches}, nil
}

func (p *Plugin) Estimate(d index.Descriptor, req plugin.IndexQuery) (int, error) {
	s, err := p.stateFor(d)
	if err != nil {
		return 0, err
	}
	prefix := pickProbe(req)
	s.mu.RLock()
	defer s.mu.RUnlock()
	lo, hi := s.PrefixRange(prefix)
	return hi - lo, nil
}

func pickProbe(req plugin.IndexQuery) string {
	if v, ok := req.Binds[":prefix"]; ok {
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
	case model.AttrB:
		return string(av.B), true
	}
	return "", false
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
