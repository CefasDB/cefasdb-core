// Package bloom is a Bloom-filter membership index plugin. It
// answers `does X exist in this set?` with a tunable false-positive
// rate; false negatives are impossible. Bloom cannot delete; for
// deletable membership see pkg/plugin/cbloom (Counting Bloom).
//
// Configuration (JSON, supplied via index.Descriptor.PluginConfig):
//
//	{ "field": "<attribute>", "m": <bits>, "k": <hashes> }
//
// `m` and `k` follow the standard sizing: for expected n items at
// false-positive rate p, set m ≈ -n·ln(p)/ln(2)² and k ≈ (m/n)·ln(2).
package bloom

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/internal/hashfield"
)

// Config is the per-index configuration. Field names the attribute the
// filter watches.
type Config struct {
	Field string `json:"field"`
	M     int    `json:"m"` // bit count
	K     int    `json:"k"` // hash functions
}

// Filter is the state one Bloom index keeps. Concurrent reads + one
// writer are safe; concurrent writers serialise on mu.
type Filter struct {
	mu   sync.RWMutex
	cfg  Config
	bits []uint64 // m/64 slots
}

// New constructs a fresh filter from a JSON config blob.
func New(raw []byte) (*Filter, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("bloom: parse config: %w", err)
		}
	}
	if cfg.Field == "" {
		return nil, fmt.Errorf("bloom: config.field required")
	}
	if cfg.M <= 0 || cfg.K <= 0 {
		return nil, fmt.Errorf("bloom: config.m and config.k must be positive")
	}
	return &Filter{cfg: cfg, bits: make([]uint64, (cfg.M+63)/64)}, nil
}

// Add records value in the filter.
func (f *Filter) Add(value []byte) {
	h1, h2 := hashes(value)
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < f.cfg.K; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(f.cfg.M)
		f.bits[pos>>6] |= 1 << (pos & 63)
	}
}

// Contains reports whether value MIGHT be in the set. False positives
// are possible; false negatives are not.
func (f *Filter) Contains(value []byte) bool {
	h1, h2 := hashes(value)
	f.mu.RLock()
	defer f.mu.RUnlock()
	for i := 0; i < f.cfg.K; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(f.cfg.M)
		if f.bits[pos>>6]&(1<<(pos&63)) == 0 {
			return false
		}
	}
	return true
}

// Population returns the number of set bits — useful for saturation
// estimates (saturation = pop / m; rising past 0.5 means it's time
// to rebuild with a larger m).
func (f *Filter) Population() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n := 0
	for _, w := range f.bits {
		n += popcount(w)
	}
	return n
}

// Serialize emits the on-disk binary form: a length-prefixed JSON
// config followed by the raw bit array.
func (f *Filter) Serialize() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cfg, err := json.Marshal(f.cfg)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 4+len(cfg)+8*len(f.bits))
	binary.BigEndian.PutUint32(out, uint32(len(cfg)))
	copy(out[4:], cfg)
	for i, w := range f.bits {
		binary.BigEndian.PutUint64(out[4+len(cfg)+i*8:], w)
	}
	return out, nil
}

// Deserialize rehydrates a Filter from Serialize's output.
func Deserialize(buf []byte) (*Filter, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("bloom: payload too short")
	}
	cfgLen := binary.BigEndian.Uint32(buf)
	if int(cfgLen) > len(buf)-4 {
		return nil, fmt.Errorf("bloom: config length out of range")
	}
	var cfg Config
	if err := json.Unmarshal(buf[4:4+cfgLen], &cfg); err != nil {
		return nil, err
	}
	body := buf[4+cfgLen:]
	if len(body)%8 != 0 {
		return nil, fmt.Errorf("bloom: bit-array length not a multiple of 8")
	}
	bits := make([]uint64, len(body)/8)
	for i := range bits {
		bits[i] = binary.BigEndian.Uint64(body[i*8:])
	}
	return &Filter{cfg: cfg, bits: bits}, nil
}

// ---------- plugin.IndexPlugin ----------

// Plugin is the plugin.IndexPlugin face of Bloom. v1 keeps one
// in-memory Filter per (table, index name) tuple; persistence is
// covered by Serialize/Deserialize which the engine snapshots
// alongside the catalog.
type Plugin struct {
	mu      sync.Mutex
	filters map[string]*Filter // key = "table/index"
}

// NewPlugin returns a fresh plugin instance — usually only the one
// pkg/plugin/builtins constructs at init time.
func NewPlugin() *Plugin { return &Plugin{filters: map[string]*Filter{}} }

// Manifest implements plugin.Plugin.
func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "bloom",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Bloom-filter membership index (false positives, no deletes)",
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

// Build replaces any existing filter for this descriptor by re-seeding
// from the supplied item stream.
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

// Update inserts the new value. Bloom cannot delete, so an old value's
// bits remain set — accept the false-positive cost (or use cbloom).
func (p *Plugin) Update(d index.Descriptor, oldItem, newItem model.Item) error {
	f, err := p.filterFor(d)
	if err != nil {
		return err
	}
	v, err := hashfield.Extract(newItem, f.cfg.Field)
	if err != nil {
		return err
	}
	if v != nil {
		f.Add(v)
	}
	return nil
}

// ErrDeleteUnsupported is returned by Delete on a plain Bloom — there
// is no way to remove a value from a Bloom filter. Use cbloom (Counting
// Bloom Filter) when deletes matter.
var ErrDeleteUnsupported = errors.New("bloom: plain Bloom filters cannot delete; use cbloom")

// Delete is a no-op on a plain Bloom filter; returns
// ErrDeleteUnsupported so the caller knows to rebuild for accurate
// membership when items leave the set.
func (p *Plugin) Delete(d index.Descriptor, key model.Item) error {
	return ErrDeleteUnsupported
}

// Query answers a membership check via Predicate (parsed as the raw
// candidate value when binds[":value"] is absent; otherwise binds
// take precedence so the planner can forward expression bindings).
func (p *Plugin) Query(d index.Descriptor, req plugin.IndexQuery) (plugin.CandidateSet, error) {
	f, err := p.filterFor(d)
	if err != nil {
		return nil, err
	}
	var probe []byte
	if v, ok := req.Binds[":value"]; ok {
		probe, err = encodeAttr(v)
	} else {
		probe = []byte(req.Predicate)
	}
	if err != nil {
		return nil, err
	}
	if f.Contains(probe) {
		return singletonCandidate(model.Item{f.cfg.Field: stringAttr(string(probe))}), nil
	}
	return emptyCandidate(), nil
}

// Estimate returns 1 (probably present) or 0 (definitely absent).
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

func hashes(value []byte) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write(value)
	h1 := h.Sum64()
	h.Reset()
	_, _ = h.Write([]byte{0x5a})
	_, _ = h.Write(value)
	h2 := h.Sum64()
	return h1, h2
}

func popcount(x uint64) int {
	// https://en.wikipedia.org/wiki/Hamming_weight ; portable variant.
	x = x - ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	x = (x + (x >> 4)) & 0x0f0f0f0f0f0f0f0f
	return int((x * 0x0101010101010101) >> 56)
}

func encodeAttr(av model.AttributeValue) ([]byte, error) {
	return hashfield.Extract(model.Item{"_": av}, "_")
}

func stringAttr(s string) model.AttributeValue {
	return model.AttributeValue{T: model.AttrS, S: s}
}

// singletonCandidate / emptyCandidate keep the CandidateSet contract
// shape simple for membership-style plugins.
type singletonSet struct {
	emitted bool
	item    model.Item
}

func (s *singletonSet) Next() (plugin.Candidate, bool) {
	if s.emitted {
		return plugin.Candidate{}, false
	}
	s.emitted = true
	return plugin.Candidate{Key: s.item}, true
}
func (s *singletonSet) Err() error   { return nil }
func (s *singletonSet) Close() error { return nil }

func singletonCandidate(it model.Item) plugin.CandidateSet { return &singletonSet{item: it} }

type emptySet struct{}

func (emptySet) Next() (plugin.Candidate, bool) { return plugin.Candidate{}, false }
func (emptySet) Err() error                     { return nil }
func (emptySet) Close() error                   { return nil }

func emptyCandidate() plugin.CandidateSet { return emptySet{} }

// init registers the built-in plugin against plugin.Default so the
// engine picks it up without any further wiring beyond a blank
// import.
func init() {
	plugin.Default.MustRegister(NewPlugin())
}
