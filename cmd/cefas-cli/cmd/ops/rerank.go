// CLI command for the MMR rerank RPC (issue #244). Mirrors the
// shape of `top-k`: read a DynamoDB-JSON candidate set off disk (or
// inline), call Rerank server-side, render the resulting slate. The
// SQL `DIVERSIFY BY` surface is deferred to a follow-up — this
// command is the v1 user-facing entrypoint to MMR.
package ops

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/fileloader"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
)

// rerankCandidateWire is the on-disk shape the --candidates flag
// expects: an array of {Item: <ddb-json item>, Distance: <float>}
// objects, mirroring the top-k response so the round-trip is a no-op.
type rerankCandidateWire struct {
	Item     map[string]ddbjson.Attribute `json:"Item"`
	Distance float64                      `json:"Distance"`
}

func registerRerank(root *cobra.Command) {
	var (
		table, field, distanceOp, candidatesArg string
		lambda                                  float64
		targetSize                              int
	)
	c := &cobra.Command{
		Use:   "rerank",
		Short: "Apply the MMR diversification operator to a candidate set",
		Long: `Reranks a candidate set with Maximal Marginal Relevance (MMR), the
canonical "balance relevance against intra-slate similarity"
operator. The candidate set is typically the output of a top-k call
over an ANN-indexed embedding column; rerank trims it to a diverse
slate the application surfaces to the user.

The scoring rule is

  score(c) = λ · relevance(c) − (1 − λ) · max_{p ∈ picked} sim(c, p)

λ=1 reproduces the input ranking, λ=0 maximises diversity after the
first (highest-relevance) pick. --metric defaults to the metric of
the ANN index registered for table+field (when one exists) and
falls back to "cosine".

Example (vector retrieval → MMR → slate):
  # 1. Pull candidates with top-k:
  cefas top-k --table Docs \
              --by "cosine(embedding, :q)" \
              --k 100 \
              --query @query-vec.json > candidates.json

  # 2. Diversify down to a slate of 10:
  cefas rerank --table Docs \
               --field embedding \
               --lambda 0.5 \
               --slate 10 \
               --candidates @candidates.json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || candidatesArg == "" || targetSize <= 0 {
				return fmt.Errorf("--table, --candidates, and --slate > 0 are required")
			}
			if lambda < 0 || lambda > 1 {
				return fmt.Errorf("--lambda must be in [0, 1]")
			}
			raw, err := fileloader.Load(candidatesArg)
			if err != nil {
				return err
			}
			cands, err := parseRerankCandidates(raw)
			if err != nil {
				return fmt.Errorf("--candidates: %w", err)
			}

			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			resp, err := cli.Rerank(ctx, client.RerankRequest{
				Table:            table,
				Field:            field,
				DistanceOperator: distanceOp,
				Lambda:           lambda,
				TargetSize:       targetSize,
				Candidates:       cands,
			})
			if err != nil {
				return fmt.Errorf("rerank: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			slate := make([]map[string]any, 0, len(resp.Slate))
			for _, r := range resp.Slate {
				slate = append(slate, map[string]any{
					"Item":     ddbjson.EncodeItem(r.Item),
					"Distance": r.Distance,
				})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Slate":            slate,
				"DistanceOperator": resp.DistanceOperator,
				"Lambda":           lambda,
				"Field":            field,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Source table the candidates came from (required for distance defaulting)")
	f.StringVar(&field, "field", "embedding", "Attribute the similarity function compares")
	f.StringVar(&distanceOp, "metric", "", "Distance/similarity operator (default: ANN index metric, else cosine)")
	f.Float64Var(&lambda, "lambda", 0.5, "Relevance vs diversity trade-off in [0, 1]")
	f.IntVar(&targetSize, "slate", 0, "Target slate size (required, > 0)")
	f.StringVar(&candidatesArg, "candidates", "", "Candidate set: '@path/to/file.json' or inline JSON array")
	root.AddCommand(c)
}

func parseRerankCandidates(raw []byte) ([]client.RerankCandidate, error) {
	var wire []rerankCandidateWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	out := make([]client.RerankCandidate, 0, len(wire))
	for i, w := range wire {
		if len(w.Item) == 0 {
			return nil, fmt.Errorf("candidate %d: empty item", i)
		}
		item, err := ddbjson.DecodeItem(w.Item)
		if err != nil {
			return nil, fmt.Errorf("candidate %d: %w", i, err)
		}
		out = append(out, client.RerankCandidate{Item: item, Distance: w.Distance})
	}
	return out, nil
}
