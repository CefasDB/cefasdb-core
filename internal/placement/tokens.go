// Package cluster: token-segment arithmetic.
//
// Split off from planner.go to isolate the big-int range math the
// plan strategies use to subtract/cover token ranges. None of this
// touches Manager state — it is pure value-level computation kept in
// its own file so the planner stays readable.
package placement

import (
	"fmt"
	"math/big"
	"sort"
)

func midpointToken(r TokenRange) uint64 {
	start := new(big.Int).SetUint64(r.Start)
	end := new(big.Int).SetUint64(r.End)
	if r.Start == r.End || r.Start > r.End {
		end.Add(end, bigTokenSpace)
	}
	mid := new(big.Int).Add(start, end)
	mid.Div(mid, big.NewInt(2))
	if mid.Cmp(bigTokenSpace) >= 0 {
		mid.Sub(mid, bigTokenSpace)
	}
	return mid.Uint64()
}

func TokenStrictlyInside(r TokenRange, token uint64) bool {
	if token == r.Start || token == r.End {
		return false
	}
	return r.Contains(token)
}

func SplitRange(r TokenRange, token uint64) (TokenRange, TokenRange) {
	return TokenRange{Start: r.Start, End: token}, TokenRange{Start: token, End: r.End}
}

func SubtractTokenRanges(ranges []TokenRange, remove TokenRange) ([]TokenRange, error) {
	if len(ranges) == 0 {
		return nil, fmt.Errorf("source has no ranges")
	}
	owners := make([]tokenSegment, 0, len(ranges)*2)
	for _, rng := range ranges {
		owners = append(owners, tokenRangeSegments(rng)...)
	}
	sortTokenSegments(owners)
	removeSegs := tokenRangeSegments(remove)
	sortTokenSegments(removeSegs)
	for _, seg := range removeSegs {
		if !segmentCoveredBySegments(seg, owners) {
			return nil, fmt.Errorf("range [%d,%d) is not fully owned", remove.Start, remove.End)
		}
	}

	remaining := cloneTokenSegments(owners)
	for _, rm := range removeSegs {
		next := make([]tokenSegment, 0, len(remaining)+1)
		for _, seg := range remaining {
			next = append(next, subtractTokenSegment(seg, rm)...)
		}
		remaining = next
	}
	sortTokenSegments(remaining)
	remaining = mergeAdjacentTokenSegments(remaining)
	return tokenSegmentsToRanges(remaining), nil
}

func subtractTokenSegment(seg, remove tokenSegment) []tokenSegment {
	if remove.end.Cmp(seg.start) <= 0 || remove.start.Cmp(seg.end) >= 0 {
		return []tokenSegment{cloneTokenSegment(seg)}
	}
	var out []tokenSegment
	if remove.start.Cmp(seg.start) > 0 {
		out = append(out, tokenSegment{
			start: new(big.Int).Set(seg.start),
			end:   minBig(remove.start, seg.end),
		})
	}
	if remove.end.Cmp(seg.end) < 0 {
		out = append(out, tokenSegment{
			start: maxBig(remove.end, seg.start),
			end:   new(big.Int).Set(seg.end),
		})
	}
	return out
}

func segmentCoveredBySegments(seg tokenSegment, owners []tokenSegment) bool {
	coveredUntil := new(big.Int).Set(seg.start)
	for _, owner := range owners {
		if owner.end.Cmp(coveredUntil) <= 0 {
			continue
		}
		if owner.start.Cmp(coveredUntil) > 0 {
			return false
		}
		if owner.end.Cmp(seg.end) >= 0 {
			return true
		}
		coveredUntil.Set(owner.end)
	}
	return false
}

func tokenSegmentsToRanges(segs []tokenSegment) []TokenRange {
	out := make([]TokenRange, 0, len(segs))
	for _, seg := range segs {
		if seg.start.Cmp(seg.end) == 0 {
			continue
		}
		out = append(out, TokenRange{Start: bigTokenToUint64(seg.start), End: bigTokenToUint64(seg.end)})
	}
	return out
}

func bigTokenToUint64(v *big.Int) uint64 {
	if v.Cmp(bigTokenSpace) == 0 {
		return 0
	}
	return v.Uint64()
}

func sortTokenSegments(segs []tokenSegment) {
	sort.Slice(segs, func(i, j int) bool { return segs[i].start.Cmp(segs[j].start) < 0 })
}

func mergeAdjacentTokenSegments(segs []tokenSegment) []tokenSegment {
	if len(segs) <= 1 {
		return segs
	}
	out := make([]tokenSegment, 0, len(segs))
	current := cloneTokenSegment(segs[0])
	for _, seg := range segs[1:] {
		if current.end.Cmp(seg.start) == 0 {
			current.end = new(big.Int).Set(seg.end)
			continue
		}
		out = append(out, current)
		current = cloneTokenSegment(seg)
	}
	out = append(out, current)
	return out
}

func cloneTokenSegments(in []tokenSegment) []tokenSegment {
	out := make([]tokenSegment, 0, len(in))
	for _, seg := range in {
		out = append(out, cloneTokenSegment(seg))
	}
	return out
}

func cloneTokenSegment(seg tokenSegment) tokenSegment {
	return tokenSegment{start: new(big.Int).Set(seg.start), end: new(big.Int).Set(seg.end)}
}

func minBig(a, b *big.Int) *big.Int {
	if a.Cmp(b) <= 0 {
		return new(big.Int).Set(a)
	}
	return new(big.Int).Set(b)
}

func maxBig(a, b *big.Int) *big.Int {
	if a.Cmp(b) >= 0 {
		return new(big.Int).Set(a)
	}
	return new(big.Int).Set(b)
}
