package api

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	cquery "github.com/osvaldoandrade/cefas/pkg/core/query"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

const annVectorBind = ":vector"

type annIndexRef struct {
	build index.Descriptor
	cfg   annConfig
}

type annTopKResult struct {
	rows           []cquery.TopKResult
	candidateCount int
}

var exactTopKScanFallbackHook func(table, field, explicit string)

func (s *GRPCServer) findANNDescriptor(table, field string, target model.AttributeValue) (index.Descriptor, annConfig, bool, error) {
	descs, err := s.pluginIndexDescriptorsForTable(table)
	if err != nil {
		return index.Descriptor{}, annConfig{}, false, err
	}
	matches := make([]index.Descriptor, 0, len(descs))
	for _, desc := range descs {
		if desc.Table == table && strings.EqualFold(desc.PluginName, "ann") {
			matches = append(matches, desc)
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Name < matches[j].Name })

	for _, desc := range matches {
		cfg, err := parseANNConfig(desc.PluginConfig)
		if err != nil {
			return index.Descriptor{}, annConfig{}, false, err
		}
		if cfg.Field != field {
			continue
		}
		if cfg.Metric == "" {
			cfg.Metric = "cosine"
		}
		if cfg.Dim > 0 {
			if got := attrVectorDim(target); got > 0 && got != cfg.Dim {
				return index.Descriptor{}, annConfig{}, false, fmt.Errorf("ann: target dim %d != index dim %d", got, cfg.Dim)
			}
		}
		return desc, cfg, true, nil
	}
	return index.Descriptor{}, annConfig{}, false, nil
}

func (s *GRPCServer) findANNIndex(table, field string, target model.AttributeValue) (annIndexRef, bool, error) {
	stored, cfg, ok, err := s.findANNDescriptor(table, field, target)
	if err != nil || !ok {
		return annIndexRef{}, ok, err
	}
	td, err := s.cat.Describe(table)
	if err != nil {
		return annIndexRef{}, false, mapStorageErr(err)
	}
	_, build, err := normalizePluginIndexDescriptor(stored, td)
	if err != nil {
		return annIndexRef{}, false, err
	}
	return annIndexRef{build: build, cfg: cfg}, true, nil
}

func (s *GRPCServer) indexedANNTopK(table, field string, target model.AttributeValue, limit int, explicitMetric string) (annTopKResult, bool, error) {
	if limit <= 0 {
		return annTopKResult{}, false, status.Error(codes.InvalidArgument, "ann limit must be > 0")
	}
	ref, ok, err := s.findANNIndex(table, field, target)
	if err != nil || !ok {
		return annTopKResult{}, ok, err
	}
	if explicit := strings.TrimSpace(explicitMetric); explicit != "" && !strings.EqualFold(explicit, ref.cfg.Metric) {
		return annTopKResult{}, true, status.Errorf(codes.InvalidArgument,
			"distance_operator %q does not match ann index metric %q", explicit, ref.cfg.Metric)
	}

	idxPlug, ok := s.pluginRegistry().Lookup(ref.build.PluginName)
	if !ok {
		return annTopKResult{}, true, status.Errorf(codes.FailedPrecondition, "plugin %q not registered", ref.build.PluginName)
	}
	idx, ok := idxPlug.(plugin.IndexPlugin)
	if !ok {
		return annTopKResult{}, true, status.Errorf(codes.InvalidArgument, "plugin %q is not an IndexPlugin", ref.build.PluginName)
	}
	distPlug, ok := s.pluginRegistry().Lookup(ref.cfg.Metric)
	if !ok {
		return annTopKResult{}, true, status.Errorf(codes.NotFound, "distance plugin %q not registered", ref.cfg.Metric)
	}
	dist, ok := distPlug.(plugin.DistancePlugin)
	if !ok {
		return annTopKResult{}, true, status.Errorf(codes.InvalidArgument, "plugin %q is not a DistancePlugin", ref.cfg.Metric)
	}

	cs, err := idx.Query(ref.build, plugin.IndexQuery{
		Binds: map[string]model.AttributeValue{
			annVectorBind: target,
		},
		Limit: limit,
	})
	if err != nil {
		return annTopKResult{}, true, status.Errorf(codes.Internal, "ann query: %v", err)
	}
	defer cs.Close()

	eng, err := cquery.NewTopK(dist, field, target, limit)
	if err != nil {
		return annTopKResult{}, true, status.Error(codes.InvalidArgument, err.Error())
	}
	count := 0
	for {
		cand, ok := cs.Next()
		if !ok {
			break
		}
		count++
		if err := eng.Observe(cand.Key); err != nil {
			return annTopKResult{}, true, status.Errorf(codes.InvalidArgument, "observe: %v", err)
		}
	}
	if err := cs.Err(); err != nil {
		return annTopKResult{}, true, status.Errorf(codes.Internal, "ann candidates: %v", err)
	}
	return annTopKResult{
		rows:           eng.Result(),
		candidateCount: count,
	}, true, nil
}

func (s *GRPCServer) exactScanTopK(table, field string, target model.AttributeValue, limit int, dist cquery.DistanceOp, explicit string) ([]cquery.TopKResult, int, error) {
	if exactTopKScanFallbackHook != nil {
		exactTopKScanFallbackHook(table, field, explicit)
	}
	if s.manager == nil {
		return localExactScanTopK(s.db, table, field, target, limit, dist)
	}
	td, err := s.cat.Describe(table)
	if err != nil {
		return nil, 0, mapStorageErr(err)
	}
	eng, err := cquery.NewTopK(dist, field, target, limit)
	if err != nil {
		return nil, 0, status.Error(codes.InvalidArgument, err.Error())
	}
	seen := make(map[string]struct{})
	scanned := 0
	stores := s.scatterReadStores()
	if len(stores) == 0 {
		return nil, 0, status.Error(codes.Unavailable, "no readable shards available")
	}
	for _, db := range stores {
		rows, localScanned, err := localExactScanTopK(db, table, field, target, limit, dist)
		if err != nil {
			return nil, 0, err
		}
		scanned += localScanned
		for _, row := range rows {
			id, err := primaryIdentity(row.Item, td.KeySchema)
			if err != nil {
				return nil, 0, status.Errorf(codes.Internal, "topk identity: %v", err)
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			if err := eng.Observe(row.Item); err != nil {
				return nil, 0, status.Errorf(codes.InvalidArgument, "observe: %v", err)
			}
		}
	}
	return eng.Result(), scanned, nil
}

func localExactScanTopK(db *storage.DB, table, field string, target model.AttributeValue, limit int, dist cquery.DistanceOp) ([]cquery.TopKResult, int, error) {
	eng, err := cquery.NewTopK(dist, field, target, limit)
	if err != nil {
		return nil, 0, status.Error(codes.InvalidArgument, err.Error())
	}
	items, err := db.ScanTable(table, 0)
	if err != nil {
		return nil, 0, mapStorageErr(err)
	}
	for _, it := range items {
		if err := eng.Observe(it); err != nil {
			return nil, 0, status.Errorf(codes.InvalidArgument, "observe: %v", err)
		}
	}
	return eng.Result(), len(items), nil
}

func (s *GRPCServer) scatterReadStores() []*storage.DB {
	if s.manager == nil {
		return []*storage.DB{s.db}
	}
	out := make([]*storage.DB, 0)
	seen := map[*storage.DB]struct{}{}
	for _, sh := range s.manager.Shards() {
		if !scatterReadableShard(sh) || sh.Storage == nil {
			continue
		}
		if _, ok := seen[sh.Storage]; ok {
			continue
		}
		out = append(out, sh.Storage)
		seen[sh.Storage] = struct{}{}
	}
	return out
}

func scatterReadableShard(sh *cluster.Shard) bool {
	if sh == nil {
		return false
	}
	switch sh.State {
	case "", cluster.ShardStateActive, cluster.ShardStateSplitting, cluster.ShardStateMoving, cluster.ShardStateReadOnly:
		return true
	default:
		return false
	}
}

func topKRowsToPB(rows []cquery.TopKResult) []*cefaspb.TopKRow {
	out := make([]*cefaspb.TopKRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, &cefaspb.TopKRow{
			Item:     &cefaspb.Item{Attributes: itemToPB(r.Item)},
			Distance: r.Distance,
		})
	}
	return out
}
