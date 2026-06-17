package query

import (
	"fmt"
	"sort"
	"sync"
)

// NewDistanceRegistry returns a thread-safe in-memory DistanceRegistry.
// The default registry built-in plugins register against lives in
// pkg/plugin/registry; this constructor is for tests and for engines
// that want an isolated registry per request.
func NewDistanceRegistry() DistanceRegistry {
	return &distanceRegistry{ops: make(map[string]DistanceOp)}
}

type distanceRegistry struct {
	mu  sync.RWMutex
	ops map[string]DistanceOp
}

func (r *distanceRegistry) Register(op DistanceOp) error {
	if op == nil {
		return fmt.Errorf("distance registry: nil operator")
	}
	name := op.Name()
	if name == "" {
		return fmt.Errorf("distance registry: operator has empty name")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.ops[name]; dup {
		return fmt.Errorf("distance registry: %q already registered", name)
	}
	r.ops[name] = op
	return nil
}

func (r *distanceRegistry) Lookup(name string) (DistanceOp, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	op, ok := r.ops[name]
	return op, ok
}

func (r *distanceRegistry) List() []DistanceOp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]DistanceOp, 0, len(r.ops))
	for _, op := range r.ops {
		out = append(out, op)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}
