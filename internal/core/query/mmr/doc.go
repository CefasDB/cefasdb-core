// Package mmr owns the Maximal Marginal Relevance diversification
// post-rank for Top-K candidate sets.
//
// MMR (Carbonell & Goldstein, 1998) balances per-candidate
// relevance against intra-slate similarity so a near-duplicate
// heavy TopK answer collapses to a more diverse slate the
// application surfaces to the user. The scoring rule is
//
//	score(c) = λ · relevance(c) − (1 − λ) · max_{p ∈ picked} sim(c, p).
//
// The package is deliberately model-free and deterministic:
// callers pass a similarity function (a query.DistanceOp via the
// SimilarityFromDistance adapter, or any func) and a ranked
// candidate list. The engine never touches the storage layer.
//
// Import-direction rule: mmr imports only pkg/core/model and
// pkg/core/query; never internal/ and never the engine packages.
//
// The package boundary:
//
//   - Candidate: one ranked entry the engine considers.
//   - Request: the MMR inputs (candidates, similarity, λ, slate
//     size).
//   - SimilarityFunc / SimilarityFromDistance: the pairwise
//     similarity contract and the DistanceOp-to-similarity
//     adapter.
//   - Rerank: applies MMR to a Request and returns the selected
//     slate in pick order.
package mmr
