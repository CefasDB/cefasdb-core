// Package audience is the ads-workload AudiencePlugin. One plugin
// instance combines geo radius selection, Haversine post-filter,
// approximate-reach estimation (via HLL), TTL-bucketed dedup, and
// sliding-window frequency capping. The privacy-aware aggregator
// (#153) and campaign eligibility operator (#154) live alongside it
// because they compose the same primitives.
//
// The plugin keeps its dedup + freqcap state in memory in v1; the
// reaper-backed pebble store hooked through pkg/core/ttl is wired in
// follow-up work. The aggregator enforces a server-side
// min-group-size threshold so a downstream reporting command cannot
// extract small-cohort information.
package audience

import (
	"fmt"
	"sync"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/geohash"
	"github.com/osvaldoandrade/cefas/pkg/plugin/haversine"
	"github.com/osvaldoandrade/cefas/pkg/plugin/hll"
	"github.com/osvaldoandrade/cefas/pkg/plugin/internal/pkid"
)

// IndexBinding tells Select which geohash index to read. Callers set
// it once via Bind so subsequent Select / Estimate calls don't have
// to re-thread the descriptor.
type IndexBinding struct {
	Geohash index.Descriptor
}

// Plugin wires geo + dedup + freqcap + estimate into one
// AudiencePlugin face.
type Plugin struct {
	mu sync.Mutex

	geo  *geohash.Plugin
	hll  *hll.Plugin
	bind IndexBinding

	// dedup: scope/key → expiresAt
	dedupExp map[string]time.Time

	// freqcap sliding window: scope/key → list of timestamps.
	// Old entries get pruned on every check; for v1 the linear
	// scan is fine — replace with a circular buffer if it shows up
	// in benchmarks.
	freqHits map[string][]time.Time

	// now is overridable in tests so TTL behaviour is deterministic.
	now func() time.Time
}

// NewPlugin wires the audience plugin against the global plugin
// registry. Tests can supply a custom geohash / HLL instance instead.
func NewPlugin() *Plugin {
	p := &Plugin{
		dedupExp: map[string]time.Time{},
		freqHits: map[string][]time.Time{},
		now:      time.Now,
	}
	if raw, ok := plugin.Default.Lookup("geohash"); ok {
		p.geo, _ = raw.(*geohash.Plugin)
	}
	if raw, ok := plugin.Default.Lookup("hll"); ok {
		p.hll, _ = raw.(*hll.Plugin)
	}
	return p
}

// NewPluginWith is the test-friendly constructor — pass explicit
// dependencies + a clock so dedup/freqcap can be exercised without
// time.Sleep.
func NewPluginWith(geo *geohash.Plugin, h *hll.Plugin, now func() time.Time) *Plugin {
	if now == nil {
		now = time.Now
	}
	return &Plugin{
		geo:      geo,
		hll:      h,
		dedupExp: map[string]time.Time{},
		freqHits: map[string][]time.Time{},
		now:      now,
	}
}

// Bind installs the geohash descriptor every subsequent Select /
// Estimate uses to fetch candidates.
func (p *Plugin) Bind(b IndexBinding) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bind = b
}

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "audience",
		Kind:        plugin.KindAudience,
		Version:     "1",
		Description: "Geo radius selection + HLL estimate + dedup + freqcap for ads workloads",
	}
}

// Select returns a CandidateSet of items within `req.Radius` meters
// of (req.Lat, req.Lon). Composes geohash candidate generation with
// a Haversine post-filter so cell-boundary false positives drop out.
func (p *Plugin) Select(req plugin.AudienceRequest) (plugin.CandidateSet, error) {
	if p.geo == nil {
		return nil, fmt.Errorf("audience: geohash plugin not bound")
	}
	p.mu.Lock()
	bind := p.bind
	p.mu.Unlock()
	if bind.Geohash.Name == "" {
		return nil, fmt.Errorf("audience: IndexBinding not set; call Bind first")
	}
	if req.Radius <= 0 {
		return nil, fmt.Errorf("audience: radius must be positive")
	}
	st, err := p.geo.StateFor(bind.Geohash)
	if err != nil {
		return nil, err
	}
	field := geoField(bind.Geohash)
	if field == "" {
		return nil, fmt.Errorf("audience: geohash config missing field")
	}
	cs, err := p.geo.Query(bind.Geohash, plugin.IndexQuery{
		Binds: map[string]model.AttributeValue{
			":center": centerAttr(req.Lat, req.Lon),
		},
	})
	if err != nil {
		return nil, err
	}
	defer cs.Close()
	_ = st // silence unused — st is the holder of the cfg; field already plucked
	out := make([]plugin.Candidate, 0)
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		loc, ok := c.Key[field]
		if !ok || loc.T != model.AttrM {
			continue
		}
		lat, ok := numFrom(loc.M, "lat")
		if !ok {
			continue
		}
		lon, ok := numFrom(loc.M, "lon")
		if !ok {
			continue
		}
		d := haversine.Distance(req.Lat, req.Lon, lat, lon)
		if d > req.Radius {
			continue
		}
		out = append(out, plugin.Candidate{Key: c.Key, Score: d})
	}
	return &sliceSet{rows: out}, nil
}

// Estimate returns the approximate reach within radius. Observes
// every selected candidate's primary key into an HLL stream named
// `audience:<table>:<index>`; the estimate is read from the same
// stream after seeding.
//
// v1 re-walks Select on every call so the count reflects the live
// index; if that turns into a hot path, swap the streaming HLL
// observation for an incremental hook in Update.
func (p *Plugin) Estimate(req plugin.AudienceRequest) (int, error) {
	if p.hll == nil {
		return 0, fmt.Errorf("audience: hll plugin not bound")
	}
	cs, err := p.Select(req)
	if err != nil {
		return 0, err
	}
	defer cs.Close()
	p.mu.Lock()
	bind := p.bind
	p.mu.Unlock()
	stream := fmt.Sprintf("audience:%s:%s", bind.Geohash.Table, bind.Geohash.Name)
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		id, ok := pkid.Of(c.Key, bind.Geohash.KeySchema)
		if !ok {
			continue
		}
		_ = p.hll.Observe(stream, model.AttributeValue{T: model.AttrS, S: id})
	}
	est, err := p.hll.Estimate(stream)
	if err != nil {
		return 0, err
	}
	return int(est), nil
}

// Dedup records (scope, key) with a TTL. Returns (true, nil) when the
// key is new in the window — i.e. delivery is allowed — and (false,
// nil) on a duplicate inside the TTL.
func (p *Plugin) Dedup(scope, key string, ttl time.Duration) (bool, error) {
	if scope == "" || key == "" {
		return false, fmt.Errorf("audience: dedup needs scope + key")
	}
	if ttl <= 0 {
		return false, fmt.Errorf("audience: dedup ttl must be positive")
	}
	bucket := scope + "/" + key
	now := p.now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if exp, ok := p.dedupExp[bucket]; ok && exp.After(now) {
		return false, nil
	}
	p.dedupExp[bucket] = now.Add(ttl)
	return true, nil
}

// FreqCap records one hit against (scope, key) and reports whether
// the cumulative count inside `window` stayed at or below `limit`.
// Returns (true, nil) when the hit is allowed, (false, nil) when it
// would push past the cap.
func (p *Plugin) FreqCap(scope, key string, limit int, window time.Duration) (bool, error) {
	if scope == "" || key == "" {
		return false, fmt.Errorf("audience: freqcap needs scope + key")
	}
	if limit <= 0 || window <= 0 {
		return false, fmt.Errorf("audience: freqcap limit + window must be positive")
	}
	bucket := scope + "/" + key
	now := p.now()
	cutoff := now.Add(-window)
	p.mu.Lock()
	defer p.mu.Unlock()
	hits := p.freqHits[bucket]
	// Prune outside the window.
	keep := hits[:0]
	for _, t := range hits {
		if t.After(cutoff) {
			keep = append(keep, t)
		}
	}
	if len(keep) >= limit {
		p.freqHits[bucket] = keep
		return false, nil
	}
	keep = append(keep, now)
	p.freqHits[bucket] = keep
	return true, nil
}

// ---------- helpers ----------

func geoField(d index.Descriptor) string {
	// Parse only enough to surface "field"; full parsing lives in
	// pkg/plugin/geohash. Re-implementing the JSON pluck here avoids
	// exporting an internal helper for one short string.
	cfg := struct {
		Field string `json:"field"`
	}{}
	if len(d.PluginConfig) == 0 {
		return ""
	}
	if err := jsonUnmarshal(d.PluginConfig, &cfg); err != nil {
		return ""
	}
	return cfg.Field
}

func centerAttr(lat, lon float64) model.AttributeValue {
	return model.AttributeValue{T: model.AttrM, M: map[string]model.AttributeValue{
		"lat": {T: model.AttrN, N: formatFloat(lat)},
		"lon": {T: model.AttrN, N: formatFloat(lon)},
	}}
}

func numFrom(m map[string]model.AttributeValue, field string) (float64, bool) {
	v, ok := m[field]
	if !ok || v.T != model.AttrN {
		return 0, false
	}
	var n float64
	sign := 1.0
	i := 0
	s := v.N
	if i < len(s) && s[i] == '-' {
		sign = -1
		i++
	}
	frac := 1.0
	dot := false
	for ; i < len(s); i++ {
		c := s[i]
		if c == '.' {
			dot = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		d := float64(c - '0')
		if dot {
			frac /= 10
			n += d * frac
		} else {
			n = n*10 + d
		}
	}
	return sign * n, true
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
