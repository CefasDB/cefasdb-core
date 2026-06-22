package server

import (
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

// pluginIndexLocalState tracks which plugin index descriptors have
// already had their local slice Built into the in-process plugin
// state. Each entry is a sync.Once-style guard: the first caller
// runs Build against localIndexItemSourceFor; later callers wait on
// the same channel.
//
// Plugin state is per-process and in-memory: when a node owns a
// shard, the items it replicates need to be fed through the plugin's
// Build so subsequent Query / Audience / Estimate calls see them.
// CreateIndex / RebuildIndex (which always Build inline) call
// markPluginIndexLocalStateBuilt to record the work; lazy callers
// (typically query handlers) use ensurePluginIndexLocalState to
// trigger a Build on the first access on this node.
type pluginIndexLocalState struct {
	mu      sync.Mutex
	entries map[string]*pluginIndexLocalSlot
}

type pluginIndexLocalSlot struct {
	done chan struct{}
	err  error
}

var pluginIndexLocal = &pluginIndexLocalState{
	entries: map[string]*pluginIndexLocalSlot{},
}

// markPluginIndexLocalStateBuilt records that this node's slice of
// the plugin index has been Built. Subsequent
// ensurePluginIndexLocalState calls for the same descriptor return
// immediately without rerunning Build.
func (s *GRPCServer) markPluginIndexLocalStateBuilt(desc index.Descriptor) {
	key := indexKey(desc.Table, desc.Name)
	pluginIndexLocal.mu.Lock()
	defer pluginIndexLocal.mu.Unlock()
	slot, ok := pluginIndexLocal.entries[key]
	if !ok {
		slot = &pluginIndexLocalSlot{done: make(chan struct{})}
		pluginIndexLocal.entries[key] = slot
		close(slot.done)
		return
	}
	select {
	case <-slot.done:
	default:
		close(slot.done)
	}
}

// ensurePluginIndexLocalState guarantees that the local plugin state
// for desc has been Built at least once on this node. The first
// caller for a given descriptor runs the Build; concurrent callers
// wait on the same channel. On Build failure the slot is left in an
// errored state and reused — callers see the same error rather than
// retrying repeatedly into a broken plugin.
//
// Returns nil when the local state is ready (or has been marked
// ready by CreateIndex / RebuildIndex). Returns an error only when
// the lazy Build itself failed.
func (s *GRPCServer) ensurePluginIndexLocalState(desc index.Descriptor) error {
	key := indexKey(desc.Table, desc.Name)
	pluginIndexLocal.mu.Lock()
	slot, ok := pluginIndexLocal.entries[key]
	if ok {
		pluginIndexLocal.mu.Unlock()
		<-slot.done
		return slot.err
	}
	slot = &pluginIndexLocalSlot{done: make(chan struct{})}
	pluginIndexLocal.entries[key] = slot
	pluginIndexLocal.mu.Unlock()

	defer close(slot.done)
	slot.err = s.buildPluginIndexLocalSlice(desc)
	return slot.err
}

func (s *GRPCServer) buildPluginIndexLocalSlice(desc index.Descriptor) error {
	raw, ok := s.pluginRegistry().Lookup(desc.PluginName)
	if !ok {
		return status.Errorf(codes.FailedPrecondition,
			"plugin %q not registered", desc.PluginName)
	}
	ip, ok := raw.(plugin.IndexPlugin)
	if !ok {
		return status.Errorf(codes.InvalidArgument,
			"plugin %q is not an IndexPlugin", desc.PluginName)
	}
	source, _, err := s.localIndexItemSourceFor(desc.Table)
	if err != nil {
		return err
	}
	if err := ip.Build(desc, source); err != nil {
		return status.Errorf(codes.Internal, "plugin local build: %v", err)
	}
	return nil
}

// rehydratePluginIndexLocalStates kicks off background Builds for
// every plugin index descriptor known to the catalog. Called from
// hydratePluginIndexCatalog at server startup so a node coming back
// online repopulates its local slices without waiting for the first
// query.
func (s *GRPCServer) rehydratePluginIndexLocalStates() {
	descs, err := s.pluginIndexDescriptorsForTableAll()
	if err != nil || len(descs) == 0 {
		return
	}
	for _, desc := range descs {
		go func(d index.Descriptor) {
			_ = s.ensurePluginIndexLocalState(d)
		}(desc)
	}
}

func (s *GRPCServer) pluginIndexDescriptorsForTableAll() ([]index.Descriptor, error) {
	return s.db.ListPluginIndexDescriptors()
}
