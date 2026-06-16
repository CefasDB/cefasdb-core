package model

import (
	"fmt"
	"strconv"
	"strings"
)

// Phase 5a (#313) introduces value-object IDs to kill the primitive
// obsession that has crept across pkg/api / internal/cluster /
// internal/metrics. Every ID type here is a struct (not a type
// alias) so callers must go through the constructor and so the
// compiler catches accidental string/uint32 misuse.
//
// Each VO implements encoding.TextMarshaler / TextUnmarshaler with
// the same wire form the bare primitive used to produce, so this
// PR is additive: introducing the types does not change any HTTP or
// gRPC payload. The next slices (5b..e) migrate signatures and
// eventually flip the wire shape under the v2 namespace.

// ShardID identifies a placement shard. The catalog assigns
// contiguous IDs from 0; the constructor accepts any uint32 in
// that range but rejects the zero-as-sentinel pattern that some
// metric labels used to mean "unrouted" — use the dedicated
// UnroutedShardID for that case.
type ShardID struct{ v uint32 }

// UnroutedShardID is the sentinel returned for diagnostic paths
// that record a request the router could not place (see
// pkg/api/range_metrics.go). IsUnrouted reports true.
var UnroutedShardID = ShardID{v: 0xffff_ffff}

// NewShardID validates and constructs a ShardID. uint32(0) is
// allowed — shard 0 is the legacy single-shard default.
// MaxUint32 is reserved for UnroutedShardID; callers asking for it
// directly get an error so the sentinel can't be confused with a
// legitimate shard.
func NewShardID(v uint32) (ShardID, error) {
	if v == UnroutedShardID.v {
		return ShardID{}, fmt.Errorf("model: shard id %d is reserved for the unrouted sentinel", v)
	}
	return ShardID{v: v}, nil
}

// MustShardID is the test/fixture form. Panics on the same
// invariant NewShardID rejects.
func MustShardID(v uint32) ShardID {
	id, err := NewShardID(v)
	if err != nil {
		panic(err)
	}
	return id
}

// Uint32 unwraps to the raw value. Use only at the persistence /
// wire boundary; intra-process code should pass ShardID around.
func (s ShardID) Uint32() uint32 { return s.v }

// IsUnrouted reports whether s is the UnroutedShardID sentinel.
func (s ShardID) IsUnrouted() bool { return s == UnroutedShardID }

// String returns the canonical decimal form, or "unrouted" for
// the sentinel. Stable across versions — the metric labels in
// internal/metrics depend on it.
func (s ShardID) String() string {
	if s.IsUnrouted() {
		return "unrouted"
	}
	return strconv.FormatUint(uint64(s.v), 10)
}

// MarshalText satisfies encoding.TextMarshaler so JSON / proto
// codecs emit the same string the caller used to produce by
// hand. The wire form does not change in 5a; phase 5d flips it
// under the /v2 namespace.
func (s ShardID) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// UnmarshalText parses what MarshalText emitted.
func (s *ShardID) UnmarshalText(b []byte) error {
	str := string(b)
	if str == "unrouted" {
		*s = UnroutedShardID
		return nil
	}
	v, err := strconv.ParseUint(str, 10, 32)
	if err != nil {
		return fmt.Errorf("model: shard id %q: %w", str, err)
	}
	id, err := NewShardID(uint32(v))
	if err != nil {
		return err
	}
	*s = id
	return nil
}

// NodeID identifies a cluster member (raft peer). Free-form
// string per the existing catalog contract; the constructor
// enforces the minimal hygiene the legacy code already assumed
// (non-empty, no leading/trailing whitespace).
type NodeID struct{ v string }

// NewNodeID validates and constructs a NodeID.
func NewNodeID(v string) (NodeID, error) {
	if v == "" {
		return NodeID{}, fmt.Errorf("model: node id cannot be empty")
	}
	if strings.TrimSpace(v) != v {
		return NodeID{}, fmt.Errorf("model: node id %q has leading or trailing whitespace", v)
	}
	return NodeID{v: v}, nil
}

// MustNodeID is the test/fixture form.
func MustNodeID(v string) NodeID {
	id, err := NewNodeID(v)
	if err != nil {
		panic(err)
	}
	return id
}

// String returns the raw identifier.
func (n NodeID) String() string { return n.v }

// MarshalText / UnmarshalText preserve the legacy string wire form.
func (n NodeID) MarshalText() ([]byte, error) { return []byte(n.v), nil }

// UnmarshalText parses the legacy string wire form into a NodeID.
func (n *NodeID) UnmarshalText(b []byte) error {
	id, err := NewNodeID(string(b))
	if err != nil {
		return err
	}
	*n = id
	return nil
}

// StreamShardID identifies a DynamoDB-style stream shard inside a
// stream ARN. The legacy single-shard convention uses the literal
// "shardId-000000000000" — the constructor accepts that or any
// future "shardId-NNNNNNNNNNNN" pattern with 12 decimal digits.
type StreamShardID struct{ v string }

const streamShardIDPrefix = "shardId-"

// StreamShardIDSingle is the canonical single-shard identifier the
// legacy wire format uses everywhere.
var StreamShardIDSingle = StreamShardID{v: streamShardIDPrefix + "000000000000"}

// NewStreamShardID validates and constructs a StreamShardID.
func NewStreamShardID(v string) (StreamShardID, error) {
	if !strings.HasPrefix(v, streamShardIDPrefix) {
		return StreamShardID{}, fmt.Errorf("model: stream shard id %q must start with %q", v, streamShardIDPrefix)
	}
	tail := strings.TrimPrefix(v, streamShardIDPrefix)
	if len(tail) != 12 {
		return StreamShardID{}, fmt.Errorf("model: stream shard id %q must have a 12-digit suffix, got %d", v, len(tail))
	}
	for _, r := range tail {
		if r < '0' || r > '9' {
			return StreamShardID{}, fmt.Errorf("model: stream shard id %q has non-digit suffix %q", v, tail)
		}
	}
	return StreamShardID{v: v}, nil
}

// MustStreamShardID is the test/fixture form.
func MustStreamShardID(v string) StreamShardID {
	id, err := NewStreamShardID(v)
	if err != nil {
		panic(err)
	}
	return id
}

// String returns the canonical form.
func (s StreamShardID) String() string { return s.v }

// MarshalText / UnmarshalText preserve the legacy string wire form.
func (s StreamShardID) MarshalText() ([]byte, error) { return []byte(s.v), nil }

// UnmarshalText parses the legacy string wire form into a StreamShardID.
func (s *StreamShardID) UnmarshalText(b []byte) error {
	id, err := NewStreamShardID(string(b))
	if err != nil {
		return err
	}
	*s = id
	return nil
}

// TableID identifies a user-defined table by name. The constructor
// enforces what catalog.Create already requires: non-empty, no
// leading/trailing whitespace, no internal slashes (which would
// conflict with the storage key encoding).
type TableID struct{ v string }

// NewTableID validates and constructs a TableID.
func NewTableID(v string) (TableID, error) {
	if v == "" {
		return TableID{}, fmt.Errorf("model: table id cannot be empty")
	}
	if strings.TrimSpace(v) != v {
		return TableID{}, fmt.Errorf("model: table id %q has leading or trailing whitespace", v)
	}
	if strings.ContainsRune(v, '/') {
		return TableID{}, fmt.Errorf("model: table id %q cannot contain '/'", v)
	}
	return TableID{v: v}, nil
}

// MustTableID is the test/fixture form.
func MustTableID(v string) TableID {
	id, err := NewTableID(v)
	if err != nil {
		panic(err)
	}
	return id
}

// String returns the raw table name.
func (t TableID) String() string { return t.v }

// MarshalText / UnmarshalText preserve the legacy string wire form.
func (t TableID) MarshalText() ([]byte, error) { return []byte(t.v), nil }

// UnmarshalText parses the legacy string wire form into a TableID.
func (t *TableID) UnmarshalText(b []byte) error {
	id, err := NewTableID(string(b))
	if err != nil {
		return err
	}
	*t = id
	return nil
}
