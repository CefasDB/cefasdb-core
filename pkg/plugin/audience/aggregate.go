package audience

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

// AggregateSpec describes one aggregation. GroupBy names the
// attributes that build the group key; Metrics name the attributes
// to sum per group. MinGroupSize is the privacy floor — groups with
// fewer than MinGroupSize members are dropped before the result
// returns; if any group would be dropped the entire call errors so
// the caller can't infer per-group counts from missing keys.
type AggregateSpec struct {
	GroupBy      []string
	Metrics      []string
	MinGroupSize int
}

// AggregateResult is one row of the output.
type AggregateResult struct {
	GroupKey map[string]string
	Counts   map[string]float64
	Members  int
}

// ErrMinGroupSize is returned when MinGroupSize > 0 and at least one
// group falls below the threshold. Callers see a single typed error
// rather than a partial result.
var ErrMinGroupSize = fmt.Errorf("audience: aggregate produced a group below min-group-size")

// Aggregate runs spec against items. v1 streams the slice directly;
// the server-side path will call this from a Scan iterator (Epic 7
// CLI wiring).
//
// Privacy contract: the function returns nothing if a single group
// would have leaked. It never silently drops the small group.
func Aggregate(items []model.Item, spec AggregateSpec) ([]AggregateResult, error) {
	if len(spec.GroupBy) == 0 {
		return nil, fmt.Errorf("audience: aggregate needs at least one group-by attribute")
	}
	type bucket struct {
		members int
		sums    map[string]float64
		keys    map[string]string
	}
	buckets := map[string]*bucket{}
	for _, it := range items {
		key := groupKey(it, spec.GroupBy)
		b, ok := buckets[key.id]
		if !ok {
			b = &bucket{sums: map[string]float64{}, keys: key.fields}
			buckets[key.id] = b
		}
		b.members++
		for _, m := range spec.Metrics {
			b.sums[m] += metricValue(it, m)
		}
	}
	// Privacy gate.
	for _, b := range buckets {
		if spec.MinGroupSize > 0 && b.members < spec.MinGroupSize {
			return nil, fmt.Errorf("%w (group %v has %d members, threshold %d)",
				ErrMinGroupSize, b.keys, b.members, spec.MinGroupSize)
		}
	}
	out := make([]AggregateResult, 0, len(buckets))
	for _, b := range buckets {
		out = append(out, AggregateResult{
			GroupKey: cloneStrMap(b.keys),
			Counts:   cloneFloatMap(b.sums),
			Members:  b.members,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return lessKey(out[i].GroupKey, out[j].GroupKey, spec.GroupBy)
	})
	return out, nil
}

type groupKeyResult struct {
	id     string
	fields map[string]string
}

func groupKey(it model.Item, groupBy []string) groupKeyResult {
	fields := make(map[string]string, len(groupBy))
	parts := make([]string, 0, len(groupBy))
	for _, k := range groupBy {
		v := stringOf(it[k])
		fields[k] = v
		parts = append(parts, v)
	}
	return groupKeyResult{id: joinNul(parts), fields: fields}
}

func metricValue(it model.Item, attr string) float64 {
	v, ok := it[attr]
	if !ok || v.T != model.AttrN {
		return 0
	}
	f, err := strconv.ParseFloat(v.N, 64)
	if err != nil {
		return 0
	}
	return f
}

func stringOf(av model.AttributeValue) string {
	switch av.T {
	case model.AttrS:
		return av.S
	case model.AttrN:
		return av.N
	}
	return ""
}

func joinNul(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "\x00"
		}
		out += p
	}
	return out
}

func cloneStrMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneFloatMap(in map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func lessKey(a, b map[string]string, order []string) bool {
	for _, k := range order {
		if a[k] < b[k] {
			return true
		}
		if a[k] > b[k] {
			return false
		}
	}
	return false
}
