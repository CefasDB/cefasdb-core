package server

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/storage"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// ScanShard streams every primary item from the requested logical
// shard, served straight from this node's local pebble.DB. It is the
// cluster-internal counterpart of the public Scan RPC: a coordinator
// that needs items from a shard it does not host calls this on a peer
// that does.
//
// The handler refuses requests for shards the node does not replicate,
// returning UNAVAILABLE so the caller can try a different peer. It
// uses Iter rather than ScanTable so memory stays bounded regardless
// of how many items the shard holds.
func (s *GRPCServer) ScanShard(req *cefaspb.ScanShardRequest, stream cefaspb.Replica_ScanShardServer) error {
	if s.manager == nil {
		return status.Error(codes.FailedPrecondition, "cluster: manager not attached")
	}
	if req.GetTable() == "" {
		return status.Error(codes.InvalidArgument, "table is required")
	}

	sh, ok := s.manager.Shard(req.GetShardId())
	if !ok {
		return status.Errorf(codes.NotFound, "cluster: shard %d not in placement", req.GetShardId())
	}
	if !(sh.IsLocalVoter || sh.IsLocalNonVoter) {
		return status.Errorf(codes.Unavailable,
			"cluster: node has no local replica for shard %d (voters=%v nonVoters=%v)",
			req.GetShardId(), sh.Voters, sh.NonVoters)
	}
	if sh.Storage == nil {
		return status.Errorf(codes.Unavailable, "cluster: shard %d has no storage", req.GetShardId())
	}

	lower, upper := storage.PrefixPrimaryAll(req.GetTable())
	it, err := sh.Storage.Iter(lower, upper)
	if err != nil {
		return mapStorageErr(err)
	}
	defer it.Close()

	ctx := stream.Context()
	for valid := it.First(); valid; valid = it.Next() {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		v := it.Value()
		cp := make([]byte, len(v))
		copy(cp, v)
		item, err := storage.DecodeItem(cp)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("decode at key %q: %v", it.Key(), err))
		}
		if err := stream.Send(&cefaspb.Item{Attributes: itemToPB(item)}); err != nil {
			return err
		}
	}
	if err := it.Error(); err != nil {
		return mapStorageErr(err)
	}
	return nil
}
