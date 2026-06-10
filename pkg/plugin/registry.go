package plugin

import (
	"fmt"
	"sort"
	"sync"
)

// Registry is the canonical lookup point — built-in plugins register
// against it in init(); the engine + CLI list and dispatch through
// it. The zero value is not safe; use NewRegistry or the package-level
// Default registry.
type Registry struct {
	mu       sync.RWMutex
	byName   map[string]Plugin
	disabled map[string]struct{}
}

// NewRegistry returns an empty in-memory registry. Tests use it to
// stay isolated from the global Default.
func NewRegistry() *Registry {
	return &Registry{
		byName:   make(map[string]Plugin),
		disabled: make(map[string]struct{}),
	}
}

// Default is the package-level registry built-in plugins target via
// init(). Engines and the CLI consume Default; tests should use
// NewRegistry to stay isolated.
var Default = NewRegistry()

// Register installs p under its manifest name. Duplicate names error
// so that a build mistake can't silently shadow a built-in plugin.
func (r *Registry) Register(p Plugin) error {
	if p == nil {
		return fmt.Errorf("plugin registry: nil plugin")
	}
	m := p.Manifest()
	if err := m.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[m.Name]; dup {
		return fmt.Errorf("plugin registry: %q already registered", m.Name)
	}
	r.byName[m.Name] = p
	return nil
}

// MustRegister panics on error. Built-in plugins call this in init()
// where a duplicate is a programmer error.
func (r *Registry) MustRegister(p Plugin) {
	if err := r.Register(p); err != nil {
		panic(err)
	}
}

// Lookup returns the plugin registered under name. The bool reports
// whether the plugin is registered AND enabled — disabled plugins
// return (p, false).
func (r *Registry) Lookup(name string) (Plugin, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.byName[name]
	if !ok {
		return nil, false
	}
	if _, off := r.disabled[name]; off {
		return p, false
	}
	return p, true
}

// LookupByKind narrows to plugins of a specific kind, sorted by name.
// Useful for "show me all registered distance operators".
func (r *Registry) LookupByKind(kind Kind) []Plugin {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Plugin, 0)
	for _, p := range r.byName {
		if p.Manifest().Kind == kind {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Manifest().Name < out[j].Manifest().Name
	})
	return out
}

// List enumerates every registered plugin, sorted by name. Disabled
// plugins are still listed — callers checking enablement use Lookup.
func (r *Registry) List() []Plugin {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Plugin, 0, len(r.byName))
	for _, p := range r.byName {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Manifest().Name < out[j].Manifest().Name
	})
	return out
}

// Disable marks a plugin disabled. Persisted index data backed by the
// plugin is not touched — Disable is reversible and safe.
func (r *Registry) Disable(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byName[name]; !ok {
		return fmt.Errorf("plugin registry: %q not registered", name)
	}
	r.disabled[name] = struct{}{}
	return nil
}

// Enable lifts a previous Disable.
func (r *Registry) Enable(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byName[name]; !ok {
		return fmt.Errorf("plugin registry: %q not registered", name)
	}
	delete(r.disabled, name)
	return nil
}

// IsDisabled reports whether name is currently disabled.
func (r *Registry) IsDisabled(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, off := r.disabled[name]
	return off
}
