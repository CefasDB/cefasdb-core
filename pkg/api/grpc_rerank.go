// MMR Rerank handler (issue #244). The operator + tests live in
// pkg/core/query/mmr; this file is the gRPC seam that wires the
// engine into the existing GRPCServer surface, including resolving
// the default distance operator from a registered ANN index, the
// same way TopK does.
package api

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	cquery "github.com/osvaldoandrade/cefas/pkg/core/query"
	"github.com/osvaldoandrade/cefas/pkg/core/query/mmr"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

// defaultRerankField is what we read off each candidate when the
// caller leaves Field blank. Matches the canonical "embedding" name
// the TopK examples use.
const defaultRerankField = "embedding"

// Rerank applies the MMR diversification operator to a candidate
// set and returns the selected slate. The handler stays stateless:
// the candidates the client passes in are the only input rows. That
// keeps the seam clean for downstream callers that compose Rerank
// after a TopK / SQL ANN query without the server having to remember
// where the candidates came from.
func (s *GRPCServer) Rerank(ctx context.Context, req *cefaspb.RerankRequest) (*cefaspb.RerankResponse, error) {
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	if req.GetTargetSize() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "target_size must be > 0")
	}
	if req.GetLambda() < 0 || req.GetLambda() > 1 {
		return nil, status.Errorf(codes.InvalidArgument, "lambda %.3f out of [0,1]", req.GetLambda())
	}
	if len(req.GetCandidates()) == 0 {
		return &cefaspb.RerankResponse{DistanceOperator: req.GetDistanceOperator()}, nil
	}
	field := req.GetField()
	if field == "" {
		field = defaultRerankField
	}

	cands, err := pbToRerankCandidates(req.GetCandidates(), field)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	dist, opName, err := s.resolveRerankDistance(req.GetTable(), field, req.GetDistanceOperator(), cands)
	if err != nil {
		return nil, err
	}

	slate, err := mmr.Rerank(mmr.Request{
		Candidates: cands,
		Sim:        mmr.SimilarityFromDistance(dist, field),
		Lambda:     req.GetLambda(),
		N:          int(req.GetTargetSize()),
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &cefaspb.RerankResponse{
		Slate:            rerankCandidatesToPB(slate),
		DistanceOperator: opName,
	}, nil
}

// resolveRerankDistance picks the similarity metric to use. Order:
//  1. explicit `distance_operator` on the request,
//  2. metric of the ANN index registered for table+field,
//  3. "cosine" as the catch-all default.
func (s *GRPCServer) resolveRerankDistance(table, field, explicit string, cands []mmr.Candidate) (cquery.DistanceOp, string, error) {
	name := explicit
	if name == "" && table != "" {
		// Probe with the first candidate's vector so findANNConfig
		// can validate dim against the registered index, mirroring
		// TopK's behaviour.
		var probe model.AttributeValue
		if len(cands) > 0 {
			probe = cands[0].Vector
		}
		cfg, ok, err := findANNConfig(table, field, probe)
		if err != nil {
			return nil, "", status.Error(codes.InvalidArgument, err.Error())
		}
		if ok {
			name = cfg.Metric
		}
	}
	if name == "" {
		name = "cosine"
	}
	plug, ok := s.pluginRegistry().Lookup(name)
	if !ok {
		return nil, "", status.Errorf(codes.NotFound, "distance plugin %q not registered", name)
	}
	dp, ok := plug.(plugin.DistancePlugin)
	if !ok {
		return nil, "", status.Errorf(codes.InvalidArgument, "plugin %q is not a DistancePlugin", name)
	}
	return dp, name, nil
}

func pbToRerankCandidates(in []*cefaspb.RerankCandidate, field string) ([]mmr.Candidate, error) {
	out := make([]mmr.Candidate, 0, len(in))
	for i, c := range in {
		if c == nil || c.GetItem() == nil {
			return nil, fmt.Errorf("candidate %d: missing item", i)
		}
		item, err := pbToItem(c.GetItem().GetAttributes())
		if err != nil {
			return nil, fmt.Errorf("candidate %d: %w", i, err)
		}
		out = append(out, mmr.Candidate{
			Item:     item,
			Distance: c.GetDistance(),
			Vector:   item[field],
		})
	}
	return out, nil
}

func rerankCandidatesToPB(in []mmr.Candidate) []*cefaspb.RerankCandidate {
	out := make([]*cefaspb.RerankCandidate, 0, len(in))
	for _, c := range in {
		out = append(out, &cefaspb.RerankCandidate{
			Item:     &cefaspb.Item{Attributes: itemToPB(c.Item)},
			Distance: c.Distance,
		})
	}
	return out
}
