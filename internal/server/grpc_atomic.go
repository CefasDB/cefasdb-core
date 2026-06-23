// Handler for the CefasAtomic service (issue #242). Lives in pkg/api
// so it reuses the same GRPCServer wiring as the rest of the surface,
// but registers under its own gRPC service name to keep the proto
// strictly append-only (the existing Cefas service is unchanged).
//
// AtomicUpdate delegates to internal/pebble.DB.AtomicUpdate, which
// performs the read-modify-write under a per-key mutex inside one
// pebble.Batch — see internal/storage/atomic.go for the contract.
package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// AtomicServer implements cefaspb.CefasAtomicServer over the same
// storage / catalog instances the main GRPCServer uses. Construct via
// NewAtomicServer; register with RegisterCefasAtomicServer alongside
// the existing Cefas service on the same *grpc.Server.
type AtomicServer struct {
	cefaspb.UnimplementedCefasAtomicServer
	core *GRPCServer
}

// NewAtomicServer wraps an existing GRPCServer so we share the storage,
// catalog, cluster manager, and metrics handles. The returned server
// is safe to register on the same *grpc.Server as the core Cefas
// service — the protoc-generated service names are distinct.
func NewAtomicServer(core *GRPCServer) *AtomicServer {
	return &AtomicServer{core: core}
}

// RegisterAtomic is a convenience that mirrors cefaspb.RegisterCefasServer
// so callers can wire both services in one line.
func RegisterAtomic(g *grpc.Server, core *GRPCServer) {
	cefaspb.RegisterCefasAtomicServer(g, NewAtomicServer(core))
}

// AtomicUpdate performs a server-side read-modify-write against the
// row identified by req.Key. See AtomicAction's per-kind docstring on
// the proto for the supported mutators. The post-image is always
// returned so callers never need a follow-up GetItem.
func (s *AtomicServer) AtomicUpdate(ctx context.Context, req *cefaspb.AtomicUpdateRequest) (*cefaspb.AtomicUpdateResponse, error) {
	started := time.Now()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemWrite, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemWrite)); err != nil {
		return nil, err
	}
	td, err := s.core.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	key, err := pbToItem(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("key: %v", err))
	}
	binds, err := pbToItem(req.GetBinds())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("binds: %v", err))
	}
	actions, err := pbToAtomicActions(req.GetActions())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pkBytes, err := pkBytesFromItem(key, td.KeySchema)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	targets, err := s.core.writeTargetsForPK(pkBytes)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	defer targets.Release()

	res, err := targets.primary.AtomicUpdate(td, key, pebble.AtomicOptions{
		Condition: req.GetCondition(),
		Binds:     binds,
		RequestID: req.GetRequestId(),
		Actions:   actions,
	})
	if err != nil {
		if errors.Is(err, storage.ErrConditionFailed) {
			return nil, status.Error(codes.FailedPrecondition, "condition failed")
		}
		if errors.Is(err, pebble.ErrAtomicUnsupported) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, mapStorageErr(err)
	}
	// Mirror the resulting item to dual-write targets so split-shard
	// readers see the post-image consistently with non-atomic writes.
	if err := targets.MirrorPutItem(td, res.Item); err != nil {
		return nil, mapStorageErr(err)
	}
	pluginPlan := pluginIndexWritePlan{
		deltas: []pluginIndexDelta{{
			oldItem: clonePluginIndexItem(res.OldItem),
			newItem: clonePluginIndexItem(res.Item),
		}},
	}
	pluginPlan.descriptors, err = s.core.pluginIndexBuildDescriptors(td)
	if err != nil {
		return nil, mapWriteMutationErr(err)
	}
	if err := s.core.applyPluginIndexPlan(pluginPlan); err != nil {
		return nil, mapWriteMutationErr(err)
	}

	returned := make([]*cefaspb.AttributeValue, len(res.Returned))
	for i := range res.Returned {
		returned[i] = attrToPB(res.Returned[i])
	}
	resp := &cefaspb.AtomicUpdateResponse{
		Item:           itemToPB(res.Item),
		ReturnedValues: returned,
		Created:        res.Created,
	}
	s.core.observeRangeMetric(rangeMetricWrite, pkBytes, estimatedItemBytes(res.Item), started)
	return resp, nil
}

func pbToAtomicActions(in []*cefaspb.AtomicAction) ([]pebble.AtomicAction, error) {
	out := make([]pebble.AtomicAction, 0, len(in))
	for i, a := range in {
		if a == nil {
			return nil, fmt.Errorf("action %d: nil", i)
		}
		kind, err := pbAtomicKind(a.GetKind())
		if err != nil {
			return nil, fmt.Errorf("action %d: %w", i, err)
		}
		var val types.AttributeValue
		if a.GetValue() != nil {
			val, err = pbToAttr(a.GetValue())
			if err != nil {
				return nil, fmt.Errorf("action %d value: %w", i, err)
			}
		}
		out = append(out, pebble.AtomicAction{
			Kind:       kind,
			Attribute:  a.GetAttribute(),
			Value:      val,
			Expression: a.GetExpression(),
		})
	}
	return out, nil
}

func pbAtomicKind(k cefaspb.AtomicActionKind) (pebble.AtomicActionKind, error) {
	switch k {
	case cefaspb.AtomicActionKind_ATOMIC_SET:
		return pebble.AtomicActionSet, nil
	case cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN:
		return pebble.AtomicActionIncrReturn, nil
	case cefaspb.AtomicActionKind_ATOMIC_ADD_RETURN:
		return pebble.AtomicActionAddReturn, nil
	case cefaspb.AtomicActionKind_ATOMIC_APPLY:
		return pebble.AtomicActionApply, nil
	}
	return 0, fmt.Errorf("unsupported AtomicActionKind %v", k)
}
