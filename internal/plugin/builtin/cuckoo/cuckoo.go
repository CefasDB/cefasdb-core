// Package cuckoo implements a Cuckoo-filter membership index plugin
// with delete support and a typically lower false-positive rate than
// a Bloom of the same size. Each bucket stores up to four
// fingerprints; lookups compare against both candidate buckets;
// inserts kick-out on conflict (bounded by maxKicks).
//
// Configuration:
//
//	{ "field": "<attribute>", "buckets": <pow2>, "fingerprint_bits": <1..16> }
//
// `buckets` must be a power of two so the modulo collapses to a mask.
// `fingerprint_bits` defaults to 8 (false-positive rate ≈ 2·b/2^f).
package cuckoo

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/internal/hashfield"
)

const (
	bucketSlots = 4
	maxKicks    = 500
)

// ErrFull is returned by Add when no room remains after maxKicks
// evictions. Callers should rebuild with a larger bucket count.
var ErrFull = errors.New("cuckoo: filter full")

type Config struct {
	Field           string `json:"field"`
	Buckets         int    `json:"buckets"`
	FingerprintBits int    `json:"fingerprint_bits,omitempty"`
}

type Filter struct {
	mu          sync.RWMutex
	cfg         Config
	mask        uint64
	fpMask      uint16
	bucketTable [][bucketSlots]uint16
	rng         *rand.Rand
}

func New(raw []byte) (*Filter, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("cuckoo: parse config: %w", err)
		}
	}
	if cfg.Field == "" {
		return nil, fmt.Errorf("cuckoo: config.field required")
	}
	if cfg.Buckets <= 0 || cfg.Buckets&(cfg.Buckets-1) != 0 {
		return nil, fmt.Errorf("cuckoo: buckets must be a positive power of two")
	}
	if cfg.FingerprintBits == 0 {
		cfg.FingerprintBits = 8
	}
	if cfg.FingerprintBits < 1 || cfg.FingerprintBits > 16 {
		return nil, fmt.Errorf("cuckoo: fingerprint_bits must be in [1,16]")
	}
	return &Filter{
		cfg:         cfg,
		mask:        uint64(cfg.Buckets - 1),
		fpMask:      uint16((1 << cfg.FingerprintBits) - 1),
		bucketTable: make([][bucketSlots]uint16, cfg.Buckets),
		rng:         rand.New(rand.NewSource(1)),
	}, nil
}

// fingerprint hashes value to a non-zero w-bit fingerprint. Zero is
// reserved as the empty-slot sentinel.
func (f *Filter) fingerprint(value []byte) uint16 {
	h := fnv.New32a()
	_, _ = h.Write(value)
	fp := uint16(h.Sum32()) & f.fpMask
	if fp == 0 {
		fp = 1
	}
	return fp
}

func (f *Filter) bucketHash(value []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(value)
	return h.Sum64() & f.mask
}

func altBucket(b uint64, fp uint16, mask uint64) uint64 {
	h := fnv.New64a()
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], fp)
	_, _ = h.Write(buf[:])
	return (b ^ h.Sum64()) & mask
}

func (f *Filter) Add(value []byte) error {
	fp := f.fingerprint(value)
	b1 := f.bucketHash(value)
	b2 := altBucket(b1, fp, f.mask)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertInto(b1, fp) || f.insertInto(b2, fp) {
		return nil
	}
	// Kick-out path: pick one of the two candidate buckets at random,
	// evict a fingerprint, place fp, hash the evicted fingerprint to
	// its alternate bucket, repeat.
	b := b1
	if f.rng.Intn(2) == 1 {
		b = b2
	}
	for i := 0; i < maxKicks; i++ {
		slot := f.rng.Intn(bucketSlots)
		evicted := f.bucketTable[b][slot]
		f.bucketTable[b][slot] = fp
		fp = evicted
		b = altBucket(b, fp, f.mask)
		if f.insertInto(b, fp) {
			return nil
		}
	}
	return ErrFull
}

func (f *Filter) insertInto(b uint64, fp uint16) bool {
	for i := 0; i < bucketSlots; i++ {
		if f.bucketTable[b][i] == 0 {
			f.bucketTable[b][i] = fp
			return true
		}
	}
	return false
}

func (f *Filter) Contains(value []byte) bool {
	fp := f.fingerprint(value)
	b1 := f.bucketHash(value)
	b2 := altBucket(b1, fp, f.mask)
	f.mu.RLock()
	defer f.mu.RUnlock()
	return contains(f.bucketTable[b1], fp) || contains(f.bucketTable[b2], fp)
}

func (f *Filter) Remove(value []byte) bool {
	fp := f.fingerprint(value)
	b1 := f.bucketHash(value)
	b2 := altBucket(b1, fp, f.mask)
	f.mu.Lock()
	defer f.mu.Unlock()
	return removeOne(&f.bucketTable[b1], fp) || removeOne(&f.bucketTable[b2], fp)
}

func contains(b [bucketSlots]uint16, fp uint16) bool {
	for i := 0; i < bucketSlots; i++ {
		if b[i] == fp {
			return true
		}
	}
	return false
}

func removeOne(b *[bucketSlots]uint16, fp uint16) bool {
	for i := 0; i < bucketSlots; i++ {
		if b[i] == fp {
			b[i] = 0
			return true
		}
	}
	return false
}

// Serialize emits a length-prefixed JSON config followed by every
// bucket's 4 little-endian uint16 fingerprints.
func (f *Filter) Serialize() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	cfg, err := json.Marshal(f.cfg)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 4+len(cfg)+2*bucketSlots*len(f.bucketTable))
	binary.BigEndian.PutUint32(out, uint32(len(cfg)))
	copy(out[4:], cfg)
	off := 4 + len(cfg)
	for _, b := range f.bucketTable {
		for s := 0; s < bucketSlots; s++ {
			binary.BigEndian.PutUint16(out[off:], b[s])
			off += 2
		}
	}
	return out, nil
}

func Deserialize(buf []byte) (*Filter, error) {
	if len(buf) < 4 {
		return nil, fmt.Errorf("cuckoo: payload too short")
	}
	cfgLen := binary.BigEndian.Uint32(buf)
	if int(cfgLen) > len(buf)-4 {
		return nil, fmt.Errorf("cuckoo: config length out of range")
	}
	f, err := New(buf[4 : 4+cfgLen])
	if err != nil {
		return nil, err
	}
	body := buf[4+cfgLen:]
	if len(body) != 2*bucketSlots*len(f.bucketTable) {
		return nil, fmt.Errorf("cuckoo: body length mismatch")
	}
	off := 0
	for i := range f.bucketTable {
		for s := 0; s < bucketSlots; s++ {
			f.bucketTable[i][s] = binary.BigEndian.Uint16(body[off:])
			off += 2
		}
	}
	return f, nil
}

// ---------- plugin.IndexPlugin ----------

type Plugin struct {
	mu      sync.Mutex
	filters map[string]*Filter
}

func NewPlugin() *Plugin { return &Plugin{filters: map[string]*Filter{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "cuckoo",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Cuckoo-filter membership index with delete support",
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
			if err := f.Add(v); err != nil {
				inErr = err
				return false
			}
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
			return f.Add(v)
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
		return &singleton{item: model.Item{f.cfg.Field: {T: model.AttrS, S: string(probe)}}}, nil
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

type empty struct{}

func (empty) Next() (plugin.Candidate, bool) { return plugin.Candidate{}, false }
func (empty) Err() error                     { return nil }
func (empty) Close() error                   { return nil }

func init() { plugin.Default.MustRegister(NewPlugin()) }
