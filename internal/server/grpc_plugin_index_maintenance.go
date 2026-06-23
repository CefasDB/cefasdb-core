package server

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/core/index"
	cefassql "github.com/CefasDb/cefasdb/internal/sql"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type pluginIndexItemGetter interface {
	GetItem(table string, ks types.KeySchema, key types.Item) (types.Item, error)
}

type pluginIndexDelta struct {
	oldItem   types.Item
	newItem   types.Item
	deleteKey types.Item
}

type pluginIndexWritePlan struct {
	descriptors []index.Descriptor
	deltas      []pluginIndexDelta
}

func (p pluginIndexWritePlan) empty() bool {
	return len(p.descriptors) == 0 || len(p.deltas) == 0
}

func mapWriteMutationErr(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return mapStorageErr(err)
}

func (s *GRPCServer) pluginIndexBuildDescriptors(td types.TableDescriptor) ([]index.Descriptor, error) {
	stored, err := s.pluginIndexDescriptorsForTable(td.Name)
	if err != nil {
		return nil, err
	}
	out := make([]index.Descriptor, 0, len(stored))
	for _, desc := range stored {
		if desc.PluginName == "" {
			continue
		}
		_, build, err := normalizePluginIndexDescriptor(desc, td)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "plugin index %s/%s descriptor: %v", desc.Table, desc.Name, err)
		}
		if build.PluginName != "" {
			out = append(out, build)
		}
	}
	return out, nil
}

func (s *GRPCServer) planPluginIndexPut(getter pluginIndexItemGetter, td types.TableDescriptor, item types.Item) (pluginIndexWritePlan, error) {
	descs, err := s.pluginIndexBuildDescriptors(td)
	if err != nil || len(descs) == 0 {
		return pluginIndexWritePlan{descriptors: descs}, err
	}
	var oldItem types.Item
	if s.pluginIndexNeedsOldItem(descs) {
		oldItem, err = readPluginIndexOldItem(getter, td, itemKeyOnly(item, td.KeySchema))
		if err != nil {
			return pluginIndexWritePlan{}, err
		}
	}
	return pluginIndexWritePlan{
		descriptors: descs,
		deltas:      []pluginIndexDelta{{oldItem: oldItem, newItem: clonePluginIndexItem(item)}},
	}, nil
}

func (s *GRPCServer) planPluginIndexDelete(getter pluginIndexItemGetter, td types.TableDescriptor, key types.Item) (pluginIndexWritePlan, error) {
	descs, err := s.pluginIndexBuildDescriptors(td)
	if err != nil || len(descs) == 0 {
		return pluginIndexWritePlan{descriptors: descs}, err
	}
	// Delete always needs the prior — the plugin sees the row leaving
	// and must update its state accordingly. Skip-the-read only
	// applies to puts, where the new image is the source of truth.
	oldItem, err := readPluginIndexOldItem(getter, td, itemKeyOnly(key, td.KeySchema))
	if err != nil {
		return pluginIndexWritePlan{}, err
	}
	return pluginIndexWritePlan{
		descriptors: descs,
		deltas:      []pluginIndexDelta{{oldItem: oldItem, deleteKey: clonePluginIndexItem(itemKeyOnly(key, td.KeySchema))}},
	}, nil
}

func (s *GRPCServer) planPluginIndexBatch(getter pluginIndexItemGetter, td types.TableDescriptor, ops []pebble.BatchOp) (pluginIndexWritePlan, error) {
	descs, err := s.pluginIndexBuildDescriptors(td)
	if err != nil || len(descs) == 0 || len(ops) == 0 {
		return pluginIndexWritePlan{descriptors: descs}, err
	}
	needsOld := s.pluginIndexNeedsOldItem(descs)
	deltas := make([]pluginIndexDelta, 0, len(ops))
	for i, op := range ops {
		switch op.Op {
		case pebble.BatchOpPut:
			key := itemKeyOnly(op.Item, td.KeySchema)
			var oldItem types.Item
			if needsOld {
				oldItem, err = readPluginIndexOldItem(getter, td, key)
				if err != nil {
					return pluginIndexWritePlan{}, fmt.Errorf("op %d plugin index prior: %w", i, err)
				}
			}
			deltas = append(deltas, pluginIndexDelta{oldItem: oldItem, newItem: clonePluginIndexItem(op.Item)})
		case pebble.BatchOpDelete:
			key := itemKeyOnly(op.Key, td.KeySchema)
			// Deletes always need the prior — see planPluginIndexDelete.
			oldItem, err := readPluginIndexOldItem(getter, td, key)
			if err != nil {
				return pluginIndexWritePlan{}, fmt.Errorf("op %d plugin index prior: %w", i, err)
			}
			deltas = append(deltas, pluginIndexDelta{oldItem: oldItem, deleteKey: clonePluginIndexItem(key)})
		default:
			return pluginIndexWritePlan{}, fmt.Errorf("op %d: unknown kind %d", i, op.Op)
		}
	}
	return pluginIndexWritePlan{descriptors: descs, deltas: deltas}, nil
}

// pluginIndexNeedsOldItem returns true when at least one descriptor's
// plugin manifest declares NeedsOldItem. The Put hot path can skip
// the prior-read entirely when this is false — bloom / HLL / geohash
// / ANN all return false today, leaving the optimisation on by
// default. A future plugin that needs the prior image (counter
// index, delta encoder) opts in via its Manifest.
func (s *GRPCServer) pluginIndexNeedsOldItem(descs []index.Descriptor) bool {
	reg := s.pluginRegistry()
	if reg == nil {
		// No registry attached — assume needs-old to keep the safe path.
		return true
	}
	for _, desc := range descs {
		raw, ok := reg.Lookup(desc.PluginName)
		if !ok {
			return true
		}
		ip, isIndex := raw.(plugin.IndexPlugin)
		if !isIndex {
			continue
		}
		if ip.Manifest().NeedsOldItem {
			return true
		}
	}
	return false
}

func (s *GRPCServer) pluginIndexMutationHook(td types.TableDescriptor) cefassql.MutationHook {
	return func(m cefassql.ItemMutation) error {
		plan := pluginIndexWritePlan{}
		var err error
		plan.descriptors, err = s.pluginIndexBuildDescriptors(td)
		if err != nil || len(plan.descriptors) == 0 {
			return err
		}
		plan.deltas = []pluginIndexDelta{{
			oldItem:   clonePluginIndexItem(m.OldItem),
			newItem:   clonePluginIndexItem(m.NewItem),
			deleteKey: clonePluginIndexItem(m.DeleteKey),
		}}
		return s.applyPluginIndexPlan(plan)
	}
}

func (s *GRPCServer) pluginIndexMutationHookForPlan(plan cefassql.Plan) cefassql.MutationHook {
	switch p := plan.(type) {
	case *cefassql.PlanPutItem:
		return s.pluginIndexMutationHook(p.Descriptor)
	case *cefassql.PlanUpdate:
		return s.pluginIndexMutationHook(p.Descriptor)
	case *cefassql.PlanDelete:
		return s.pluginIndexMutationHook(p.Descriptor)
	default:
		return nil
	}
}

// applyPluginIndexPlan runs every plugin-backed index's deltas. The
// previous serial loop blocked the write path for
// sum(D × N × per-plugin update cost). Plugins are independent (each
// owns its own state and lock), so descriptors run in parallel via
// errgroup. Deltas inside one descriptor stay serial because many
// plugins keep a single writer-lock internally; touching them
// concurrently would either serialise or break the plugin.
//
// Fast path: only one descriptor → run inline, no goroutine cost.
func (s *GRPCServer) applyPluginIndexPlan(plan pluginIndexWritePlan) error {
	if plan.empty() {
		return nil
	}
	// Resolve every plugin up front so a missing or typed-mismatched
	// plugin fails before any work is dispatched.
	type bound struct {
		desc index.Descriptor
		idx  plugin.IndexPlugin
	}
	resolved := make([]bound, 0, len(plan.descriptors))
	for _, desc := range plan.descriptors {
		raw, ok := s.pluginRegistry().Lookup(desc.PluginName)
		if !ok {
			return status.Errorf(codes.FailedPrecondition, "plugin index %s/%s: plugin %q not registered", desc.Table, desc.Name, desc.PluginName)
		}
		idx, ok := raw.(plugin.IndexPlugin)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "plugin index %s/%s: plugin %q is not an IndexPlugin", desc.Table, desc.Name, desc.PluginName)
		}
		resolved = append(resolved, bound{desc: desc, idx: idx})
	}

	if len(resolved) == 1 {
		return s.applyPluginIndexDescriptorDeltas(resolved[0].desc, resolved[0].idx, plan.deltas)
	}

	var g errgroup.Group
	for _, b := range resolved {
		b := b
		g.Go(func() error {
			return s.applyPluginIndexDescriptorDeltas(b.desc, b.idx, plan.deltas)
		})
	}
	return g.Wait()
}

func (s *GRPCServer) applyPluginIndexDescriptorDeltas(desc index.Descriptor, idx plugin.IndexPlugin, deltas []pluginIndexDelta) error {
	for _, delta := range deltas {
		if delta.newItem == nil {
			key := delta.oldItem
			if key == nil {
				key = delta.deleteKey
			}
			if key == nil {
				continue
			}
			started := time.Now()
			err := idx.Delete(desc, clonePluginIndexItem(key))
			s.observePluginIndexMutation(desc.Table, "delete", started, err)
			if err != nil {
				return status.Errorf(codes.Internal, "plugin index %s/%s delete: %v", desc.Table, desc.Name, err)
			}
			continue
		}
		started := time.Now()
		err := idx.Update(desc, clonePluginIndexItem(delta.oldItem), clonePluginIndexItem(delta.newItem))
		s.observePluginIndexMutation(desc.Table, "update", started, err)
		if err != nil {
			return status.Errorf(codes.Internal, "plugin index %s/%s update: %v", desc.Table, desc.Name, err)
		}
	}
	return nil
}

func (s *GRPCServer) observePluginIndexMutation(table, op string, started time.Time, err error) {
	if s.metrics == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "err"
	}
	s.metrics.Observe("plugin_index_"+op, table, outcome, time.Since(started).Seconds())
}

func readPluginIndexOldItem(getter pluginIndexItemGetter, td types.TableDescriptor, key types.Item) (types.Item, error) {
	oldItem, err := getter.GetItem(td.Name, td.KeySchema, key)
	if errors.Is(err, types.ErrItemNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return clonePluginIndexItem(oldItem), nil
}

func itemKeyOnly(item types.Item, ks types.KeySchema) types.Item {
	if item == nil {
		return nil
	}
	out := types.Item{ks.PK: item[ks.PK]}
	if ks.SK != "" {
		out[ks.SK] = item[ks.SK]
	}
	return out
}

func clonePluginIndexItem(in types.Item) types.Item {
	if in == nil {
		return nil
	}
	out := make(types.Item, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
