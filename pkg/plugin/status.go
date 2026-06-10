package plugin

import "time"

// State is the lifecycle-state component of Status.
type State uint8

const (
	StateUnknown State = iota
	StateLoaded         // registered, not started
	StateRunning        // Start completed without error
	StateDisabled       // Disable() called; persisted data untouched
	StateFailed         // Start() or a runtime call returned an error
)

func (s State) String() string {
	switch s {
	case StateLoaded:
		return "loaded"
	case StateRunning:
		return "running"
	case StateDisabled:
		return "disabled"
	case StateFailed:
		return "failed"
	}
	return "unknown"
}

// Status is the snapshot the engine + CLI surface for a single plugin.
type Status struct {
	Name              string    `json:"name"`
	Kind              string    `json:"kind"`
	State             string    `json:"state"`
	LastError         string    `json:"lastError,omitempty"`
	LastErrorAtUnix   int64     `json:"lastErrorAtUnix,omitempty"`
	ItemsIndexed      int64     `json:"itemsIndexed,omitempty"`
	StartedAtUnix     int64     `json:"startedAtUnix,omitempty"`
}

// StatusProvider is an optional interface plugins implement when they
// want to surface real-time counters / error stats. Plugins that
// don't implement it report a synthesised Status reflecting only
// State + Manifest.
type StatusProvider interface {
	Status() Status
}

// Snapshot collects Status for every registered plugin. The engine
// fills in State from its own lifecycle bookkeeping; this helper just
// composes manifest + StatusProvider when one exists.
func Snapshot(r *Registry, state func(name string) State, lastErr func(name string) (string, time.Time)) []Status {
	plugs := r.List()
	out := make([]Status, 0, len(plugs))
	for _, p := range plugs {
		m := p.Manifest()
		s := Status{Name: m.Name, Kind: m.Kind.String()}
		if sp, ok := p.(StatusProvider); ok {
			s = sp.Status()
		}
		if state != nil {
			s.State = state(m.Name).String()
		}
		if lastErr != nil {
			msg, ts := lastErr(m.Name)
			s.LastError = msg
			if !ts.IsZero() {
				s.LastErrorAtUnix = ts.Unix()
			}
		}
		out = append(out, s)
	}
	return out
}
