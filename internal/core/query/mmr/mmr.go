package mmr

import (
	"errors"
	"fmt"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/core/query"
)

// Candidate is one entry the MMR engine ranks. Score is the upstream
// relevance score; lower-is-better in the TopK convention, so the
// engine flips its sign before applying the MMR rule. The optional
// Vector is the value the similarity function compares — when the
// caller already extracted the embedding (typical when MMR runs on
// the output of a TopK over a vector index), supplying it here avoids
// a second attribute lookup inside the inner loop.
type Candidate struct {
	Item     model.Item
	Distance float64
	Vector   model.AttributeValue
}

// SimilarityFunc returns a similarity score between two candidates
// in [0, 1]. Implementations are free to derive that from a distance
// (e.g. cosine similarity = 1 − cosine distance) or to project the
// item to whatever metric space they want.
type SimilarityFunc func(a, b Candidate) (float64, error)

// SimilarityFromDistance adapts a query.DistanceOp to a SimilarityFunc.
// It pulls each candidate's value from `attr` (or the Candidate.Vector
// field if already extracted) and returns `1 − distance` clamped to
// [0, 1]. Distance operators on cefas already produce comparable
// outputs in that range for cosine and the Jaccard family; the clamp
// keeps λ=0 well-behaved for ops that may exceed it (Euclidean on
// normalised vectors, for instance).
func SimilarityFromDistance(op query.DistanceOp, attr string) SimilarityFunc {
	return func(a, b Candidate) (float64, error) {
		av := a.Vector
		if !attrPresent(av) {
			av = a.Item[attr]
		}
		bv := b.Vector
		if !attrPresent(bv) {
			bv = b.Item[attr]
		}
		d, err := op.Eval(av, bv)
		if err != nil {
			return 0, err
		}
		s := 1 - d
		if s < 0 {
			s = 0
		}
		if s > 1 {
			s = 1
		}
		return s, nil
	}
}

func attrPresent(av model.AttributeValue) bool {
	// model.AttributeValue is a struct (alias to types.AttributeValue);
	// any non-zero AttrType means the caller supplied a value.
	return av.T != 0 || len(av.Vec) > 0 || len(av.L) > 0 || len(av.NS) > 0 || av.S != "" || av.N != "" || len(av.B) > 0
}

// Request bundles the MMR inputs.
type Request struct {
	// Candidates is the ranked input list. The engine treats the input
	// order as the relevance ranking when no Distance is set; with
	// Distances, smaller-is-better (the TopK convention).
	Candidates []Candidate
	// Sim is the pairwise similarity function. Required.
	Sim SimilarityFunc
	// Lambda is the relevance weight in [0, 1]. λ=1 reproduces the
	// input order; λ=0 maximises diversity after the seed pick.
	Lambda float64
	// N is the target slate size. Capped at len(Candidates).
	N int
}

// Rerank applies MMR to req and returns the selected slate in pick
// order. Errors surface invalid parameters and propagate the
// similarity function's own errors.
func Rerank(req Request) ([]Candidate, error) {
	if req.Sim == nil {
		return nil, errors.New("mmr: similarity function required")
	}
	if req.Lambda < 0 || req.Lambda > 1 {
		return nil, fmt.Errorf("mmr: lambda %.3f out of [0,1]", req.Lambda)
	}
	if req.N <= 0 {
		return nil, fmt.Errorf("mmr: target slate size %d must be > 0", req.N)
	}
	if len(req.Candidates) == 0 {
		return nil, nil
	}
	n := req.N
	if n > len(req.Candidates) {
		n = len(req.Candidates)
	}

	// Convert distances to per-candidate relevance in [0, 1]. We use
	// rank-normalised relevance — the input order is authoritative —
	// so the scoring rule is invariant to the distance operator's
	// absolute scale. The first-ranked candidate gets relevance 1.0
	// and the last gets 1 / len(candidates).
	rel := make([]float64, len(req.Candidates))
	denom := float64(len(req.Candidates))
	for i := range req.Candidates {
		rel[i] = float64(len(req.Candidates)-i) / denom
	}

	picked := make([]Candidate, 0, n)
	used := make([]bool, len(req.Candidates))
	// maxSim[i] caches max similarity from candidate i to the picked
	// set; we update it incrementally as picks land.
	maxSim := make([]float64, len(req.Candidates))

	for step := 0; step < n; step++ {
		bestIdx := -1
		bestScore := 0.0
		for i := range req.Candidates {
			if used[i] {
				continue
			}
			score := req.Lambda*rel[i] - (1-req.Lambda)*maxSim[i]
			if bestIdx == -1 || score > bestScore {
				bestIdx, bestScore = i, score
			}
		}
		if bestIdx == -1 {
			break
		}
		used[bestIdx] = true
		picked = append(picked, req.Candidates[bestIdx])
		// Refresh maxSim against the new pick.
		newPick := req.Candidates[bestIdx]
		for i := range req.Candidates {
			if used[i] {
				continue
			}
			s, err := req.Sim(req.Candidates[i], newPick)
			if err != nil {
				return nil, fmt.Errorf("mmr: similarity at step %d: %w", step, err)
			}
			if s > maxSim[i] {
				maxSim[i] = s
			}
		}
	}
	return picked, nil
}
