package api

import (
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

const (
	rangeMetricRead  = "read"
	rangeMetricWrite = "write"
)

func (s *Server) observeRangeMetric(op string, pkBytes []byte, approxBytes uint64, started time.Time) {
	if s == nil || s.metrics == nil {
		return
	}
	shardID, token := rangeMetricRoute(s.manager, pkBytes)
	s.metrics.ObserveRangeOperation(shardID, token, op, approxBytes, time.Since(started))
}

func (s *GRPCServer) observeRangeMetric(op string, pkBytes []byte, approxBytes uint64, started time.Time) {
	if s == nil || s.metrics == nil {
		return
	}
	shardID, token := rangeMetricRoute(s.manager, pkBytes)
	s.metrics.ObserveRangeOperation(shardID, token, op, approxBytes, time.Since(started))
}

// rangeMetricRoute returns the routing decision for a metric
// observation. nil manager is the single-node default, where every
// operation maps to shard 0; UnroutedShardID fires only when the
// router rejects the token (genuine routing miss) so the dashboard
// distinguishes "no cluster" from "cluster with a routing gap".
func rangeMetricRoute(mgr *cluster.Manager, pkBytes []byte) (model.ShardID, uint64) {
	if mgr == nil {
		return model.MustShardID(0), xxhash.Sum64(pkBytes)
	}
	router := mgr.Router()
	token := router.TokenForPK(pkBytes)
	id, err := router.ShardForUint64(token)
	if err != nil {
		return model.UnroutedShardID, token
	}
	shardID, err := model.NewShardID(id)
	if err != nil {
		return model.UnroutedShardID, token
	}
	return shardID, token
}

func estimatedItemBytes(item types.Item) uint64 {
	var n uint64
	for name, value := range item {
		n += uint64(len(name)) + estimatedAttrBytes(value)
	}
	return n
}

func estimatedItemsBytes(items []types.Item) uint64 {
	var n uint64
	for _, item := range items {
		n += estimatedItemBytes(item)
	}
	return n
}

func estimatedAttrBytes(v types.AttributeValue) uint64 {
	switch v.T {
	case types.AttrS, types.AttrN:
		return uint64(len(v.S) + len(v.N))
	case types.AttrB:
		return uint64(len(v.B))
	case types.AttrBOOL, types.AttrNull:
		return 1
	case types.AttrSS:
		var n uint64
		for _, s := range v.SS {
			n += uint64(len(s))
		}
		return n
	case types.AttrNS:
		var n uint64
		for _, s := range v.NS {
			n += uint64(len(s))
		}
		return n
	case types.AttrBS:
		var n uint64
		for _, b := range v.BS {
			n += uint64(len(b))
		}
		return n
	case types.AttrL:
		var n uint64
		for _, item := range v.L {
			n += estimatedAttrBytes(item)
		}
		return n
	case types.AttrM:
		var n uint64
		for name, item := range v.M {
			n += uint64(len(name)) + estimatedAttrBytes(item)
		}
		return n
	default:
		return 0
	}
}
