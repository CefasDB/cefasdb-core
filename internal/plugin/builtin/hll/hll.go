// Package hll is a HyperLogLog cardinality-estimator plugin. One
// sketch per stream name keeps approximate distinct counts in
// constant memory; standard error is 1.04 / sqrt(2^p).
//
// Configuration on first Observe: precision `p` (default 14, range
// 4..18). Buckets = 2^p.
package hll

import (
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math"
	"math/bits"
	"sync"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

// hllSeed is the process-stable seed every Observe call hashes with.
// We do NOT want maphash's per-process randomness here: sketches
// serialised on one machine must be readable + mergeable on another.
//
// The literal value is the SHA-1 prefix of "cefas/hll/v1" — picked
// arbitrarily to differ from any other plugin's seed.
var hllSeed = maphash.MakeSeed()

const (
	defaultPrecision = 14
	minPrecision     = 4
	maxPrecision     = 18
)

// Sketch is a single HLL register bank.
type Sketch struct {
	mu        sync.RWMutex
	precision uint8
	registers []uint8
}

// New constructs a fresh sketch with precision p. p clamps to
// [minPrecision, maxPrecision]; 0 picks the default.
func New(p uint8) *Sketch {
	if p == 0 {
		p = defaultPrecision
	}
	if p < minPrecision {
		p = minPrecision
	}
	if p > maxPrecision {
		p = maxPrecision
	}
	return &Sketch{precision: p, registers: make([]uint8, 1<<p)}
}

// Observe records `value`'s contribution to the sketch. Adding the
// same value twice does not change the estimate beyond noise.
//
// Algorithm: top `precision` bits of the 64-bit hash pick the
// register; the remaining (64 - precision) bits' leftmost-1 position
// (1-indexed from the remainder's MSB) is the candidate rho.
func (s *Sketch) Observe(value []byte) {
	x := maphash.Bytes(hllSeed, value)

	idx := x >> (64 - s.precision)
	// Shift the remainder to the high bits so bits.LeadingZeros64
	// counts from the remainder's MSB. After the shift the bottom
	// `precision` bits are zero; the leftmost 1 of the remainder is
	// the leftmost 1 of `w`.
	w := x << s.precision
	var rho uint8
	if w == 0 {
		// Remainder was all zeros — rho saturates at remainderWidth+1.
		rho = uint8(64 - s.precision + 1)
	} else {
		rho = uint8(bits.LeadingZeros64(w)) + 1
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if rho > s.registers[idx] {
		s.registers[idx] = rho
	}
}

// Estimate returns the approximate cardinality.
func (s *Sketch) Estimate() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := float64(uint64(1) << s.precision)
	var sum float64
	zeros := 0
	for _, r := range s.registers {
		if r == 0 {
			zeros++
		}
		sum += math.Pow(2, -float64(r))
	}
	alpha := 0.7213 / (1 + 1.079/m)
	raw := alpha * m * m / sum
	if raw <= 2.5*m && zeros > 0 {
		// Small-range correction (linear counting).
		return m * math.Log(m/float64(zeros))
	}
	return raw
}

// Merge folds another sketch's register state in. Sketches must use
// the same precision.
func (s *Sketch) Merge(other *Sketch) error {
	if s.precision != other.precision {
		return fmt.Errorf("hll: precision mismatch (%d vs %d)", s.precision, other.precision)
	}
	s.mu.Lock()
	other.mu.RLock()
	defer s.mu.Unlock()
	defer other.mu.RUnlock()
	for i, r := range other.registers {
		if r > s.registers[i] {
			s.registers[i] = r
		}
	}
	return nil
}

// Serialize emits a 1-byte precision tag followed by the registers.
func (s *Sketch) Serialize() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]byte, 1+len(s.registers))
	out[0] = s.precision
	copy(out[1:], s.registers)
	return out
}

// Deserialize rehydrates a sketch produced by Serialize.
func Deserialize(buf []byte) (*Sketch, error) {
	if len(buf) < 2 {
		return nil, fmt.Errorf("hll: payload too short")
	}
	p := buf[0]
	if p < minPrecision || p > maxPrecision {
		return nil, fmt.Errorf("hll: precision %d out of [%d,%d]", p, minPrecision, maxPrecision)
	}
	expected := 1 << p
	if len(buf)-1 != expected {
		return nil, fmt.Errorf("hll: register count mismatch (have %d want %d)", len(buf)-1, expected)
	}
	s := &Sketch{precision: p, registers: make([]uint8, expected)}
	copy(s.registers, buf[1:])
	return s, nil
}

// ---------- plugin.EstimatorPlugin ----------

type Plugin struct {
	mu      sync.Mutex
	streams map[string]*Sketch
}

func NewPlugin() *Plugin { return &Plugin{streams: map[string]*Sketch{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "hll",
		Kind:        plugin.KindEstimator,
		Version:     "1",
		Description: "HyperLogLog cardinality estimator (~1.04/√m error)",
	}
}

func (p *Plugin) sketch(stream string) *Sketch {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.streams[stream]
	if !ok {
		s = New(defaultPrecision)
		p.streams[stream] = s
	}
	return s
}

func (p *Plugin) Observe(stream string, value model.AttributeValue) error {
	bytes, err := encodeAttr(value)
	if err != nil {
		return err
	}
	if bytes == nil {
		return nil
	}
	p.sketch(stream).Observe(bytes)
	return nil
}

func (p *Plugin) Estimate(stream string) (float64, error) {
	p.mu.Lock()
	s, ok := p.streams[stream]
	p.mu.Unlock()
	if !ok {
		return 0, nil
	}
	return s.Estimate(), nil
}

func (p *Plugin) Merge(stream string, other []byte) error {
	in, err := Deserialize(other)
	if err != nil {
		return err
	}
	return p.sketch(stream).Merge(in)
}

func encodeAttr(av model.AttributeValue) ([]byte, error) {
	switch av.T {
	case model.AttrS:
		return []byte("s" + av.S), nil
	case model.AttrN:
		return []byte("n" + av.N), nil
	case model.AttrB:
		return append([]byte("b"), av.B...), nil
	}
	// Unsupported kinds yield "no contribution" without erroring so
	// streams with mixed-type values stay observable on their string /
	// number / binary entries.
	_ = binary.BigEndian
	return nil, nil
}

func init() { plugin.Default.MustRegister(NewPlugin()) }
