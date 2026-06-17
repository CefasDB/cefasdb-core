// Package geohash is the geohash index plugin. Each item's (lat,
// lon) attributes hash to a base-32 geohash string at the configured
// precision; items bucket by the hash. Query enumerates the center
// hash + 8 neighbors so candidates at hash-cell edges aren't lost,
// then returns the union of bucket contents. Callers post-filter
// with the haversine distance plugin (#141) for exact radius
// matching.
//
// Configuration:
//
//	{ "field": "<attribute>", "precision": <1..12> }
//
// Defaults: precision=7 (≈ 153m × 153m cells at the equator).
package geohash

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/internal/pkid"
)

const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

type Config struct {
	Field     string `json:"field"`
	Precision int    `json:"precision,omitempty"`
}

// Encode turns (lat, lon) at the given precision into a base-32
// geohash. Precision clamps to [1, 12].
func Encode(lat, lon float64, precision int) string {
	if precision < 1 {
		precision = 1
	}
	if precision > 12 {
		precision = 12
	}
	latMin, latMax := -90.0, 90.0
	lonMin, lonMax := -180.0, 180.0
	out := make([]byte, 0, precision)
	even := true
	var bit, ch int
	for len(out) < precision {
		if even {
			mid := (lonMin + lonMax) / 2
			if lon >= mid {
				ch = (ch << 1) | 1
				lonMin = mid
			} else {
				ch <<= 1
				lonMax = mid
			}
		} else {
			mid := (latMin + latMax) / 2
			if lat >= mid {
				ch = (ch << 1) | 1
				latMin = mid
			} else {
				ch <<= 1
				latMax = mid
			}
		}
		even = !even
		bit++
		if bit == 5 {
			out = append(out, base32[ch])
			bit = 0
			ch = 0
		}
	}
	return string(out)
}

// Decode returns the (lat, lon) center of the geohash cell.
func Decode(hash string) (lat, lon float64) {
	latMin, latMax := -90.0, 90.0
	lonMin, lonMax := -180.0, 180.0
	even := true
	for _, r := range hash {
		idx := indexOf(byte(r))
		if idx < 0 {
			break
		}
		for i := 4; i >= 0; i-- {
			bit := (idx >> i) & 1
			if even {
				mid := (lonMin + lonMax) / 2
				if bit == 1 {
					lonMin = mid
				} else {
					lonMax = mid
				}
			} else {
				mid := (latMin + latMax) / 2
				if bit == 1 {
					latMin = mid
				} else {
					latMax = mid
				}
			}
			even = !even
		}
	}
	return (latMin + latMax) / 2, (lonMin + lonMax) / 2
}

func indexOf(c byte) int {
	for i := 0; i < len(base32); i++ {
		if base32[i] == c {
			return i
		}
	}
	return -1
}

// Neighbors returns the 8 neighboring cells of hash at the same
// precision (N, NE, E, SE, S, SW, W, NW). Cells that fall off the
// pole edges return their wrap-around equivalent.
func Neighbors(hash string) []string {
	lat, lon := Decode(hash)
	step := cellSize(len(hash))
	dLat := step.lat
	dLon := step.lon
	out := make([]string, 0, 8)
	for _, off := range [][2]float64{
		{dLat, 0}, {dLat, dLon}, {0, dLon}, {-dLat, dLon},
		{-dLat, 0}, {-dLat, -dLon}, {0, -dLon}, {dLat, -dLon},
	} {
		la := lat + off[0]
		lo := lon + off[1]
		if la > 90 || la < -90 {
			continue
		}
		if lo > 180 {
			lo -= 360
		}
		if lo < -180 {
			lo += 360
		}
		out = append(out, Encode(la, lo, len(hash)))
	}
	return out
}

type cell struct{ lat, lon float64 }

// cellSize returns the approximate (latitudeSpan, longitudeSpan) of a
// geohash cell at given precision in degrees.
func cellSize(precision int) cell {
	latBits := precision * 5 / 2
	lonBits := precision*5 - latBits
	return cell{lat: 180 / pow2(latBits), lon: 360 / pow2(lonBits)}
}

func pow2(n int) float64 {
	x := 1.0
	for i := 0; i < n; i++ {
		x *= 2
	}
	return x
}

// ---------- plugin.IndexPlugin ----------

type State struct {
	mu      sync.RWMutex
	cfg     Config
	ks      model.KeySchema
	items   map[string]model.Item // id → item
	hashes  map[string]string     // id → geohash
	buckets map[string][]string   // geohash → ids
}

func newState(cfg Config, ks model.KeySchema) *State {
	return &State{
		cfg:     cfg,
		ks:      ks,
		items:   map[string]model.Item{},
		hashes:  map[string]string{},
		buckets: map[string][]string{},
	}
}

func (s *State) addLocked(id string, item model.Item) {
	lat, lon, ok := latLon(item, s.cfg.Field)
	if !ok {
		return
	}
	h := Encode(lat, lon, s.cfg.Precision)
	s.items[id] = item
	s.hashes[id] = h
	s.buckets[h] = append(s.buckets[h], id)
}

func (s *State) removeLocked(id string) {
	h, ok := s.hashes[id]
	if !ok {
		return
	}
	bucket := s.buckets[h]
	for i, x := range bucket {
		if x == id {
			s.buckets[h] = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}
	if len(s.buckets[h]) == 0 {
		delete(s.buckets, h)
	}
	delete(s.items, id)
	delete(s.hashes, id)
}

func latLon(item model.Item, field string) (float64, float64, bool) {
	av, ok := item[field]
	if !ok || av.T != model.AttrM {
		return 0, 0, false
	}
	lat, ok := numFrom(av.M, "lat")
	if !ok {
		return 0, 0, false
	}
	lon, ok := numFrom(av.M, "lon")
	if !ok {
		return 0, 0, false
	}
	return lat, lon, true
}

func numFrom(m map[string]model.AttributeValue, field string) (float64, bool) {
	v, ok := m[field]
	if !ok || v.T != model.AttrN {
		return 0, false
	}
	var n float64
	for i, r := range v.N {
		if i == 0 && r == '-' {
			continue
		}
		if r == '.' {
			break
		}
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	if _, err := fmtSscan(v.N, &n); err != nil {
		return 0, false
	}
	return n, true
}

// fmtSscan minimally parses a float without dragging in fmt's full
// scanner. Handles sign + decimal; sufficient for lat/lon strings.
func fmtSscan(s string, out *float64) (int, error) {
	var sign float64 = 1
	i := 0
	if i < len(s) && s[i] == '-' {
		sign = -1
		i++
	}
	var v float64
	frac := 1.0
	dot := false
	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			dot = true
			continue
		}
		if c < '0' || c > '9' {
			return i, fmt.Errorf("invalid digit %q", c)
		}
		d := float64(c - '0')
		if dot {
			frac /= 10
			v += d * frac
		} else {
			v = v*10 + d
		}
	}
	*out = sign * v
	return i, nil
}

type Plugin struct {
	mu     sync.Mutex
	states map[string]*State
}

func NewPlugin() *Plugin { return &Plugin{states: map[string]*State{}} }

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "geohash",
		Kind:        plugin.KindIndex,
		Version:     "1",
		Description: "Geohash spatial index over {lat,lon} attribute maps",
	}
}

func parseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("geohash: parse config: %w", err)
		}
	}
	if c.Field == "" {
		return c, fmt.Errorf("geohash: config.field required")
	}
	if c.Precision == 0 {
		c.Precision = 7
	}
	if c.Precision < 1 || c.Precision > 12 {
		return c, fmt.Errorf("geohash: precision must be in [1,12]")
	}
	return c, nil
}

func key(d index.Descriptor) string { return d.Table + "/" + d.Name }

func (p *Plugin) StateFor(d index.Descriptor) (*State, error) { return p.stateFor(d) }

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
	center, ok := req.Binds[":center"]
	if !ok {
		return &sliceSet{}, nil
	}
	if center.T != model.AttrM {
		return nil, fmt.Errorf("geohash: :center must be a map (got %v)", center.T)
	}
	lat, ok := numFrom(center.M, "lat")
	if !ok {
		return nil, fmt.Errorf("geohash: :center missing numeric lat")
	}
	lon, ok := numFrom(center.M, "lon")
	if !ok {
		return nil, fmt.Errorf("geohash: :center missing numeric lon")
	}
	centerHash := Encode(lat, lon, s.cfg.Precision)
	probes := append([]string{centerHash}, Neighbors(centerHash)...)
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := map[string]struct{}{}
	out := make([]plugin.Candidate, 0)
	for _, h := range probes {
		for _, id := range s.buckets[h] {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, plugin.Candidate{Key: cloneItem(s.items[id])})
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
