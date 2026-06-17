package server

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/internal/core/index"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	cefassql "github.com/osvaldoandrade/cefas/internal/sql"
	"github.com/osvaldoandrade/cefas/pkg/types"
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
	oldItem, err := readPluginIndexOldItem(getter, td, itemKeyOnly(item, td.KeySchema))
	if err != nil {
		return pluginIndexWritePlan{}, err
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
	deltas := make([]pluginIndexDelta, 0, len(ops))
	for i, op := range ops {
		switch op.Op {
		case pebble.BatchOpPut:
			key := itemKeyOnly(op.Item, td.KeySchema)
			oldItem, err := readPluginIndexOldItem(getter, td, key)
			if err != nil {
				return pluginIndexWritePlan{}, fmt.Errorf("op %d plugin index prior: %w", i, err)
			}
			deltas = append(deltas, pluginIndexDelta{oldItem: oldItem, newItem: clonePluginIndexItem(op.Item)})
		case pebble.BatchOpDelete:
			key := itemKeyOnly(op.Key, td.KeySchema)
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

func (s *GRPCServer) applyPluginIndexPlan(plan pluginIndexWritePlan) error {
	if plan.empty() {
		return nil
	}
	for _, desc := range plan.descriptors {
		raw, ok := s.pluginRegistry().Lookup(desc.PluginName)
		if !ok {
			return status.Errorf(codes.FailedPrecondition, "plugin index %s/%s: plugin %q not registered", desc.Table, desc.Name, desc.PluginName)
		}
		idx, ok := raw.(plugin.IndexPlugin)
		if !ok {
			return status.Errorf(codes.InvalidArgument, "plugin index %s/%s: plugin %q is not an IndexPlugin", desc.Table, desc.Name, desc.PluginName)
		}
		for _, delta := range plan.deltas {
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
