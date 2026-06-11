// MMR rerank client surface (issue #244). Lives in its own file so
// the implementation does not touch client.go.
package client

import (
	"context"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// RerankCandidate is one input row to Rerank. Distance is the
// upstream relevance distance (smaller-is-better, the TopK
// convention). The server rank-normalises it internally so the
// scoring rule is invariant to the distance operator's scale.
type RerankCandidate struct {
	Item     types.Item
	Distance float64
}

// RerankRequest packs the MMR inputs the client sends over the wire.
// Field defaults to "embedding" when empty. DistanceOperator
// defaults to the metric of the ANN index registered for table+field
// (when one exists) and falls back to "cosine".
type RerankRequest struct {
	Table            string
	Field            string
	DistanceOperator string
	Lambda           float64
	TargetSize       int
	Candidates       []RerankCandidate
}

// RerankResponse carries the selected slate plus the resolved
// distance operator name (useful when the caller relied on the
// default).
type RerankResponse struct {
	Slate            []RerankCandidate
	DistanceOperator string
}

// Rerank applies the MMR diversification operator server-side and
// returns the selected slate.
func (c *Client) Rerank(ctx context.Context, req RerankRequest) (RerankResponse, error) {
	pbCands := make([]*cefaspb.RerankCandidate, 0, len(req.Candidates))
	for _, c := range req.Candidates {
		pbCands = append(pbCands, &cefaspb.RerankCandidate{
			Item:     &cefaspb.Item{Attributes: itemAttrMap(c.Item)},
			Distance: c.Distance,
		})
	}
	resp, err := c.stub.Rerank(c.withAuth(ctx), &cefaspb.RerankRequest{
		Table:            req.Table,
		Field:            req.Field,
		DistanceOperator: req.DistanceOperator,
		Lambda:           req.Lambda,
		TargetSize:       int32(req.TargetSize),
		Candidates:       pbCands,
	})
	if err != nil {
		return RerankResponse{}, err
	}
	out := RerankResponse{DistanceOperator: resp.GetDistanceOperator()}
	out.Slate = make([]RerankCandidate, 0, len(resp.GetSlate()))
	for _, s := range resp.GetSlate() {
		out.Slate = append(out.Slate, RerankCandidate{
			Item:     itemFromPB(s.GetItem().GetAttributes()),
			Distance: s.GetDistance(),
		})
	}
	return out, nil
}
