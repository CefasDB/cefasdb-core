package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// BatchWriteGI is the receiver side of the cross-shard GI cascade.
// Mirrors BatchWriteMV from #537 — writes pointer rows directly to
// this node's local pebble store, bypassing raft (GSI is RF=1 by
// design per ADR 0005 §3).
func (s *GRPCServer) BatchWriteGI(ctx context.Context, req *cefaspb.BatchWriteGIRequest) (*cefaspb.BatchWriteGIResponse, error) {
	if s.cat == nil {
		return nil, status.Error(codes.FailedPrecondition, "catalog not attached")
	}
	gi, err := s.cat.DescribeGlobalIndex(req.GetIndex())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "index %s: %v", req.GetIndex(), err)
	}
	base, err := s.cat.Describe(gi.BaseTable)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "index %s base %s: %v", req.GetIndex(), gi.BaseTable, err)
	}
	giTD := giSyntheticTableDescriptor(gi, base.KeySchema.PK)

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
		if err := s.db.BatchWriteItemLocal(giTD, ops); err != nil {
			return nil, status.Errorf(codes.Internal, "gi %s: %v", gi.Name, err)
		}
		return &cefaspb.BatchWriteGIResponse{}, nil
	}

	router := s.manager.Router()
	buckets := make(map[uint32][]pebble.BatchOp, 4)
	for _, op := range ops {
		probe := op.Item
		if op.Op == pebble.BatchOpDelete {
			probe = op.Key
		}
		pkBytes, err := pkBytesFromItem(probe, giTD.KeySchema)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "gi %s pk: %v", gi.Name, err)
		}
		shardID, err := router.ShardForPK(pkBytes)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "gi %s shard: %v", gi.Name, err)
		}
		buckets[shardID] = append(buckets[shardID], op)
	}

	for shardID, group := range buckets {
		sh, ok := s.manager.Shard(shardID)
		if !ok || sh == nil || sh.Storage == nil {
			return nil, status.Errorf(codes.Unavailable, "gi %s shard %d not local", gi.Name, shardID)
		}
		if err := sh.Storage.BatchWriteItemLocal(giTD, group); err != nil {
			return nil, status.Errorf(codes.Internal, "gi %s shard %d: %v", gi.Name, shardID, err)
		}
	}
	return &cefaspb.BatchWriteGIResponse{}, nil
}
