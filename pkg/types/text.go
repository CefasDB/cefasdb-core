package types

// TextQuery is a partition-bound native text query. Scalar lower bounds let a
// caller enforce a shorter visibility policy without changing stored expiry.
type TextQuery struct {
	Table       string
	IndexName   string
	PKValue     AttributeValue
	SKLow       AttributeValue
	SKHigh      AttributeValue
	Search      string
	Phrase      string
	Fields      map[string]AttributeValue
	MinValues   map[string]AttributeValue
	MaxValues   map[string]AttributeValue
	Descending  bool
	Limit       int
	Cursor      string
	Consistency string
}

type TextQueryResult struct {
	Items      []Item
	NextCursor string
}

type TextAggregate struct {
	TextQuery
	TimeField       string
	IntervalSeconds int
	GroupBy         string
	ErrorField      string
	ErrorValues     []string
}

type TextBucket struct {
	Timestamp string            `json:"timestamp"`
	Count     uint64            `json:"count"`
	Errors    uint64            `json:"errors"`
	Groups    map[string]uint64 `json:"groups,omitempty"`
}

type TextAggregateResult struct {
	Buckets []TextBucket `json:"buckets"`
}
