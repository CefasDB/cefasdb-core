package query

import (
	"container/heap"
	"fmt"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

// TopKResult is one row in a Top-K answer.
type TopKResult struct {
	Item     model.Item
	Distance float64
}

// TopKEngine ranks a stream of items against a distance operator and
// keeps the K with the smallest distance. v1 streams from the caller;
// future engines can plug in candidate-set generators (Trigram, LSH,
// MinHash) so the engine doesn't have to see every row.
type TopKEngine struct {
	op         DistanceOp
	target     model.AttributeValue
	attr       string
	k          int
	heap       topKHeap
	heapPushed int
}

// NewTopK constructs an engine that ranks items by op.Eval(item[attr],
// target) and keeps the K rows with smallest distance.
func NewTopK(op DistanceOp, attr string, target model.AttributeValue, k int) (*TopKEngine, error) {
	if op == nil {
		return nil, fmt.Errorf("topk: nil operator")
	}
	if attr == "" {
		return nil, fmt.Errorf("topk: empty attribute name")
	}
	if k <= 0 {
		return nil, fmt.Errorf("topk: k must be > 0")
	}
	return &TopKEngine{op: op, attr: attr, target: target, k: k}, nil
}

// Observe scores `item` against the configured operator + target. The
// engine drops items whose distance exceeds the current K-th best so
// it never holds more than K rows.
func (e *TopKEngine) Observe(item model.Item) error {
	v, ok := item[e.attr]
	if !ok {
		return nil // skip items missing the ranking attribute
	}
	d, err := e.op.Eval(v, e.target)
	if err != nil {
		return err
	}
	if len(e.heap) < e.k {
		heap.Push(&e.heap, TopKResult{Item: item, Distance: d})
		e.heapPushed++
		return nil
	}
	// heap[0] is the worst (largest distance) of the kept rows; only
	// admit items strictly better than that.
	if d < e.heap[0].Distance {
		e.heap[0] = TopKResult{Item: item, Distance: d}
		heap.Fix(&e.heap, 0)
	}
	return nil
}

// Result returns the kept rows in ascending distance order.
func (e *TopKEngine) Result() []TopKResult {
	// Pop one at a time from the max-heap to produce reverse-sorted
	// output, then reverse to get ascending order.
	out := make([]TopKResult, len(e.heap))
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&e.heap).(TopKResult)
	}
	return out
}

// topKHeap is a max-heap on Distance (the worst kept row is the root,
// so we can cheaply drop it when a better one arrives).
type topKHeap []TopKResult

func (h topKHeap) Len() int           { return len(h) }
func (h topKHeap) Less(i, j int) bool { return h[i].Distance > h[j].Distance }
func (h topKHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *topKHeap) Push(x any)        { *h = append(*h, x.(TopKResult)) }
func (h *topKHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
