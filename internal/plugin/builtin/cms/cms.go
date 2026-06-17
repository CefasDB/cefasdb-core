// Package cms is a Count-Min Sketch frequency-estimator plugin. Per
// stream it keeps a depth×width counter matrix; the estimate of an
// item's frequency is the minimum across the depth hashes. Error is
// bounded by (ε, δ): width = ⌈e/ε⌉, depth = ⌈ln(1/δ)⌉.
package cms

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sync"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

// Sketch is the matrix backing one CMS stream.
type Sketch struct {
	mu       sync.RWMutex
	depth    int
	width    int
	counters [][]uint64
}

// New constructs a sketch sized for the (ε, δ) bounds. Both must lie
// in (0, 1).
func New(epsilon, delta float64) (*Sketch, error) {
	if epsilon <= 0 || epsilon >= 1 || delta <= 0 || delta >= 1 {
		return nil, fmt.Errorf("cms: epsilon and delta must lie in (0,1)")
	}
	width := int(math.Ceil(math.E / epsilon))
	depth := int(math.Ceil(math.Log(1 / delta)))
	c := make([][]uint64, depth)
	for i := range c {
		c[i] = make([]uint64, width)
	}
	return &Sketch{depth: depth, width: width, counters: c}, nil
}

// NewSized constructs a sketch with explicit depth + width — bypasses
// the (ε, δ) sizing for callers that already picked dimensions.
func NewSized(depth, width int) (*Sketch, error) {
	if depth <= 0 || width <= 0 {
		return nil, fmt.Errorf("cms: depth and width must be positive")
	}
	c := make([][]uint64, depth)
	for i := range c {
		c[i] = make([]uint64, width)
	}
	return &Sketch{depth: depth, width: width, counters: c}, nil
}

// Observe records one occurrence of value.
func (s *Sketch) Observe(value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for d := 0; d < s.depth; d++ {
		idx := hashWithSeed(value, uint32(d)) % uint32(s.width)
		s.counters[d][idx]++
	}
}

// Frequency returns the conservative estimate (min across hashes).
func (s *Sketch) Frequency(value []byte) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	min := uint64(math.MaxUint64)
	for d := 0; d < s.depth; d++ {
		idx := hashWithSeed(value, uint32(d)) % uint32(s.width)
		if c := s.counters[d][idx]; c < min {
			min = c
		}
	}
	return min
}

// Merge folds another sketch in; dimensions must match.
func (s *Sketch) Merge(other *Sketch) error {
	if s.depth != other.depth || s.width != other.width {
		return fmt.Errorf("cms: dimension mismatch (%dx%d vs %dx%d)", s.depth, s.width, other.depth, other.width)
	}
	s.mu.Lock()
	other.mu.RLock()
	defer s.mu.Unlock()
	defer other.mu.RUnlock()
	for d := 0; d < s.depth; d++ {
		for w := 0; w < s.width; w++ {
			s.counters[d][w] += other.counters[d][w]
		}
	}
	return nil
}

// Serialize emits 4-byte depth, 4-byte width, then row-major
// big-endian uint64 counters.
func (s *Sketch) Serialize() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]byte, 8+8*s.depth*s.width)
	binary.BigEndian.PutUint32(out, uint32(s.depth))
	binary.BigEndian.PutUint32(out[4:], uint32(s.width))
	off := 8
	for d := 0; d < s.depth; d++ {
		for w := 0; w < s.width; w++ {
			binary.BigEndian.PutUint64(out[off:], s.counters[d][w])
			off += 8
		}
	}
	return out
}

func Deserialize(buf []byte) (*Sketch, error) {
	if len(buf) < 8 {
		return nil, fmt.Errorf("cms: payload too short")
	}
	depth := int(binary.BigEndian.Uint32(buf))
	width := int(binary.BigEndian.Uint32(buf[4:]))
	expect := 8 + 8*depth*width
	if len(buf) != expect {
		return nil, fmt.Errorf("cms: payload size mismatch (have %d want %d)", len(buf), expect)
	}
	s, err := NewSized(depth, width)
	if err != nil {
		return nil, err
	}
	off := 8
	for d := 0; d < depth; d++ {
		for w := 0; w < width; w++ {
			s.counters[d][w] = binary.BigEndian.Uint64(buf[off:])
			off += 8
		}
	}
	return s, nil
}

func hashWithSeed(value []byte, seed uint32) uint32 {
	h := fnv.New32a()
	var sb [4]byte
	binary.BigEndian.PutUint32(sb[:], seed)
	_, _ = h.Write(sb[:])
	_, _ = h.Write(value)
	return h.Sum32()
}

// ---------- plugin.EstimatorPlugin ----------

// Default sketch sizing — ε = 0.001, δ = 0.001 → 2719 × 7 counters.
var defaultEpsilon, defaultDelta = 0.001, 0.001

type Plugin struct {
	mu      sync.Mutex
	streams map[string]*Sketch
}

func NewPlugin() *Plugin { return &Plugin{streams: map[string]*Sketch{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "cms",
		Kind:        plugin.KindEstimator,
		Version:     "1",
		Description: "Count-Min Sketch frequency estimator",
	}
}

func (p *Plugin) sketch(stream string) *Sketch {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.streams[stream]
	if !ok {
		s, _ = New(defaultEpsilon, defaultDelta)
		p.streams[stream] = s
	}
	return s
}

func (p *Plugin) Observe(stream string, value model.AttributeValue) error {
	bytes := attrBytes(value)
	if bytes == nil {
		return nil
	}
	p.sketch(stream).Observe(bytes)
	return nil
}

// Estimate returns the total observation count for the stream. For
// per-value frequency callers, the engine exposes Frequency via the
// CMS-specific accessor (kept off the EstimatorPlugin interface to
// preserve the shared shape).
func (p *Plugin) Estimate(stream string) (float64, error) {
	p.mu.Lock()
	s, ok := p.streams[stream]
	p.mu.Unlock()
	if !ok {
		return 0, nil
	}
	var sum uint64
	s.mu.RLock()
	for _, w := range s.counters[0] {
		sum += w
	}
	s.mu.RUnlock()
	return float64(sum), nil
}

// Frequency exposes the per-value estimate. Plugins downstream cast
// to the *Plugin type to reach this; alternatives include extending
// the EstimatorPlugin interface, but doing so would force every
// plugin (e.g. HLL) to implement Frequency too.
func (p *Plugin) Frequency(stream string, value model.AttributeValue) uint64 {
	p.mu.Lock()
	s, ok := p.streams[stream]
	p.mu.Unlock()
	if !ok {
		return 0
	}
	bytes := attrBytes(value)
	if bytes == nil {
		return 0
	}
	return s.Frequency(bytes)
}

func (p *Plugin) Merge(stream string, other []byte) error {
	in, err := Deserialize(other)
	if err != nil {
		return err
	}
	return p.sketch(stream).Merge(in)
}

func attrBytes(av model.AttributeValue) []byte {
	switch av.T {
	case model.AttrS:
		return []byte("s" + av.S)
	case model.AttrN:
		return []byte("n" + av.N)
	case model.AttrB:
		return append([]byte("b"), av.B...)
	}
	return nil
}

func init() { plugin.Default.MustRegister(NewPlugin()) }
