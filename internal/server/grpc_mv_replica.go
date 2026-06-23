package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// BatchWriteMV is the receiver side of the cross-shard MV cascade.
// It writes the cascade ops directly to this node's local pebble
// store for the routed shard, bypassing raft entirely.
//
// MVs are explicitly RF=1: only this node holds the rows for shards
// it owns. If the owner is lost the view is rebuilt via
// RefreshMaterializedView; followers do not carry an authoritative
// copy. Skipping consensus per cascade bucket recovers the ~5ms
// raft-Apply tail that dominated the cascade in the post-#535 bench.
func (s *GRPCServer) BatchWriteMV(ctx context.Context, req *cefaspb.BatchWriteMVRequest) (*cefaspb.BatchWriteMVResponse, error) {
	if s.cat == nil {
		return nil, status.Error(codes.FailedPrecondition, "catalog not attached")
	}
	mv, err := s.cat.DescribeView(req.GetView())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "view %s: %v", req.GetView(), err)
	}
	mvTD := mvSyntheticTableDescriptor(mv)

	ops := make([]pebble.BatchOp, 0, len(req.GetOps()))
	for i, raw := range req.GetOps() {
		switch raw.GetKind() {
		case cefaspb.BatchWriteOp_KIND_PUT:
			item, err := pbToItem(raw.GetItem())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "op %d: %v", i, err)
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpPut, Item: item})
		case cefaspb.BatchWriteOp_KIND_DELETE:
			key, err := pbToItem(raw.GetKey())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "op %d: %v", i, err)
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpDelete, Key: key})
		default:
			return nil, status.Errorf(codes.InvalidArgument, "op %d: unknown kind", i)
		}
	}

	if s.manager == nil {
		// Single-node fallback: only one pebble.DB.
		if err := s.db.BatchWriteItemLocal(mvTD, ops); err != nil {
			return nil, status.Errorf(codes.Internal, "mv %s: %v", mv.Name, err)
		}
		return &cefaspb.BatchWriteMVResponse{}, nil
	}

	router := s.manager.Router()
	buckets := make(map[uint32][]pebble.BatchOp, 4)
	for _, op := range ops {
		probe := op.Item
		if op.Op == pebble.BatchOpDelete {
			probe = op.Key
		}
		pkBytes, err := pkBytesFromItem(probe, mv.KeySchema)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "mv %s pk: %v", mv.Name, err)
		}
		shardID, err := router.ShardForPK(pkBytes)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "mv %s shard: %v", mv.Name, err)
		}
		buckets[shardID] = append(buckets[shardID], op)
	}

	for shardID, group := range buckets {
		sh, ok := s.manager.Shard(shardID)
		if !ok || sh == nil || sh.Storage == nil {
			return nil, status.Errorf(codes.Unavailable, "mv %s shard %d not local", mv.Name, shardID)
		}
		if err := sh.Storage.BatchWriteItemLocal(mvTD, group); err != nil {
			return nil, status.Errorf(codes.Internal, "mv %s shard %d: %v", mv.Name, shardID, err)
		}
	}
	return &cefaspb.BatchWriteMVResponse{}, nil
}

// AtomicUpdateMV is the receiver side for aggregate-MV counter
// maintenance. The coordinator has already routed by the MV key; this
// method accepts only shards hosted by the receiving node.
func (s *GRPCServer) AtomicUpdateMV(ctx context.Context, req *cefaspb.AtomicUpdateMVRequest) (*cefaspb.AtomicUpdateMVResponse, error) {
	if s.cat == nil {
		return nil, status.Error(codes.FailedPrecondition, "catalog not attached")
	}
	mv, err := s.cat.DescribeView(req.GetView())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "view %s: %v", req.GetView(), err)
	}
	mvTD := mvSyntheticTableDescriptor(mv)
	key, err := pbToItem(req.GetKey())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "key: %v", err)
	}
	actions, err := pbToAtomicActions(req.GetActions())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if s.manager == nil {
		if _, err := s.db.AtomicUpdate(mvTD, key, pebble.AtomicOptions{Actions: actions}); err != nil {
			return nil, status.Errorf(codes.Internal, "mv %s: %v", mv.Name, err)
		}
		return &cefaspb.AtomicUpdateMVResponse{}, nil
	}

	pkBytes, err := pkBytesFromItem(key, mv.KeySchema)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "mv %s pk: %v", mv.Name, err)
	}
	shardID, err := s.manager.Router().ShardForPK(pkBytes)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "mv %s shard: %v", mv.Name, err)
	}
	sh, ok := s.manager.Shard(shardID)
	if !ok || sh == nil || sh.Storage == nil {
		return nil, status.Errorf(codes.Unavailable, "mv %s shard %d not local", mv.Name, shardID)
	}
	if _, err := sh.Storage.AtomicUpdate(mvTD, key, pebble.AtomicOptions{Actions: actions}); err != nil {
		return nil, status.Errorf(codes.Internal, "mv %s shard %d: %v", mv.Name, shardID, err)
	}
	return &cefaspb.AtomicUpdateMVResponse{}, nil
}
