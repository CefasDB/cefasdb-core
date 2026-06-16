package ops

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/fileloader"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
)

func registerRecommend(root *cobra.Command) {
	var (
		table, by, queryArg, filter string
		dedupScope, dedupKey        string
		freqScope, freqKey          string
		candidateLimit, limit       int
		freqLimit                   int
		dedupTTL, freqWindow        int64
		lambda                      float64
		disableDiversify            bool
	)
	c := &cobra.Command{
		Use:   "recommend",
		Short: "Run retrieve/filter/MMR/cap recommendation pipeline",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || by == "" || queryArg == "" || limit <= 0 {
				return fmt.Errorf("--table, --by, --query, and --limit > 0 are required")
			}
			m := byExprRegex.FindStringSubmatch(by)
			if m == nil {
				return fmt.Errorf("--by must look like 'op(field, :bind)'")
			}
			op, field := m[1], m[2]
			if strings.EqualFold(op, "ann") {
				op = ""
			}
			raw, err := fileloader.Load(queryArg)
			if err != nil {
				return err
			}
			var attr ddbjson.Attribute
			if err := json.Unmarshal(raw, &attr); err != nil {
				return fmt.Errorf("--query: %w", err)
			}
			target, err := attr.ToAttr()
			if err != nil {
				return fmt.Errorf("--query: %w", err)
			}

			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			resp, err := cli.Recommend(ctx, client.RecommendRequest{
				Table:                table,
				Field:                field,
				DistanceOperator:     op,
				Target:               target,
				CandidateLimit:       candidateLimit,
				FilterExpression:     filter,
				Limit:                limit,
				MMRLambda:            lambda,
				DisableDiversify:     disableDiversify,
				DedupScope:           dedupScope,
				DedupKeyField:        dedupKey,
				DedupTTLSeconds:      dedupTTL,
				FreqCapScope:         freqScope,
				FreqCapKeyField:      freqKey,
				FreqCapLimit:         freqLimit,
				FreqCapWindowSeconds: freqWindow,
			})
			if err != nil {
				return fmt.Errorf("recommend: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			rows := make([]map[string]any, 0, len(resp.Rows))
			for _, row := range resp.Rows {
				rows = append(rows, map[string]any{
					"Item":     ddbjson.EncodeItem(row.Item),
					"Distance": row.Distance,
					"Reason":   row.Reason,
				})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Rows":        rows,
				"Stages":      pipelineStagesForOutput(resp.Stages),
				"ReasonCodes": resp.ReasonCodes,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&by, "by", "", "Distance expression: 'op(field, :bind)' (required)")
	f.StringVar(&queryArg, "query", "", "DynamoDB-JSON attribute value for the distance op")
	f.IntVar(&candidateLimit, "candidate-limit", 0, "Retrieval fan-out before filter/diversify (default: max(limit*4,100))")
	f.StringVar(&filter, "filter", "", "Optional SQL predicate applied after retrieval")
	f.IntVar(&limit, "limit", 0, "Final recommendation count (required)")
	f.Float64Var(&lambda, "lambda", 0.5, "MMR relevance/diversity trade-off in [0,1]")
	f.BoolVar(&disableDiversify, "no-diversify", false, "Disable MMR diversification")
	f.StringVar(&dedupScope, "dedup-scope", "", "Dedup scope (default: table)")
	f.StringVar(&dedupKey, "dedup-key", "", "Item field used as dedup key")
	f.Int64Var(&dedupTTL, "dedup-ttl-seconds", 0, "Dedup TTL in seconds")
	f.StringVar(&freqScope, "freqcap-scope", "", "Frequency-cap scope (default: table)")
	f.StringVar(&freqKey, "freqcap-key", "", "Item field used as frequency-cap key")
	f.IntVar(&freqLimit, "freqcap-limit", 0, "Frequency-cap limit")
	f.Int64Var(&freqWindow, "freqcap-window-seconds", 0, "Frequency-cap window in seconds")
	root.AddCommand(c)
}

func pipelineStagesForOutput(stages []client.PipelineStageTiming) []map[string]any {
	out := make([]map[string]any, 0, len(stages))
	for _, s := range stages {
		out = append(out, map[string]any{
			"Stage":       s.Stage,
			"InputCount":  s.InputCount,
			"OutputCount": s.OutputCount,
			"ElapsedMS":   s.ElapsedMS,
			"ReasonCodes": s.ReasonCodes,
		})
	}
	return out
}
