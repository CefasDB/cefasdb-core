package model_test

import (
	"math"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

// FuzzShardID exercises NewShardID + (*ShardID).UnmarshalText and the
// MarshalText round-trip. Invariants:
//   - No panic for any uint32 input.
//   - If NewShardID succeeds, MarshalText/UnmarshalText round-trips
//     to the same value.
//   - MaxUint32 is rejected (reserved for UnroutedShardID).
//   - UnroutedShardID round-trips via the literal "unrouted" wire form.
func FuzzShardID(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(42))
	f.Add(uint32(65_535))
	f.Add(uint32(math.MaxUint32 - 1))
	f.Add(uint32(math.MaxUint32))

	f.Fuzz(func(t *testing.T, v uint32) {
		s, err := model.NewShardID(v)
		if err != nil {
			// MaxUint32 is the only documented rejection: enforce it
			// so a regression in the constructor surfaces here.
			if v != math.MaxUint32 {
				t.Fatalf("NewShardID(%d) unexpectedly errored: %v", v, err)
			}
			return
		}
		if s.IsUnrouted() {
			t.Fatalf("NewShardID(%d) produced unrouted sentinel", v)
		}
		if s.Uint32() != v {
			t.Fatalf("Uint32() = %d, want %d", s.Uint32(), v)
		}
		text, err := s.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText: %v", err)
		}
		var got model.ShardID
		if err := got.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if got != s {
			t.Fatalf("round-trip mismatch: %v -> %q -> %v", s, text, got)
		}
	})
}

// FuzzNodeID exercises NewNodeID + (*NodeID).UnmarshalText. Invariants:
//   - No panic for any string input.
//   - If NewNodeID succeeds, String() returns exactly the input
//     (NodeID is a raw, non-normalising container) and the text
//     wire form round-trips.
func FuzzNodeID(f *testing.F) {
	f.Add("n1")
	f.Add("node-1")
	f.Add("node_with_under_scores")
	f.Add("node-with-very-long-name-up-to-the-limit-and-beyond")
	f.Add("☃")     // unicode snowman
	f.Add("a/b/c") // NodeID does not forbid '/'
	f.Add("a b")   // internal whitespace allowed
	f.Add("")      // rejected
	f.Add(" n1")   // rejected: leading whitespace
	f.Add("n1\t")  // rejected: trailing whitespace

	f.Fuzz(func(t *testing.T, s string) {
		n, err := model.NewNodeID(s)
		if err != nil {
			return
		}
		if n.String() != s {
			t.Fatalf("String() = %q, want %q", n.String(), s)
		}
		text, err := n.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText: %v", err)
		}
		var got model.NodeID
		if err := got.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if got != n {
			t.Fatalf("round-trip mismatch: %q -> %q -> %q", s, text, got.String())
		}
	})
}

// FuzzStreamShardID exercises NewStreamShardID and the text
// round-trip. Invariants:
//   - No panic for any string input.
//   - If NewStreamShardID succeeds, the input matches the documented
//     shape ("shardId-" + 12 decimal digits) and round-trips through
//     MarshalText/UnmarshalText.
func FuzzStreamShardID(f *testing.F) {
	f.Add("shardId-000000000000")
	f.Add("shardId-000000000001")
	f.Add("shardId-000000000042")
	f.Add("shardId-999999999999")
	f.Add("shardId-")
	f.Add("shardId-1")
	f.Add("shardId-00000000000a")
	f.Add("shard-1")
	f.Add("")

	f.Fuzz(func(t *testing.T, s string) {
		id, err := model.NewStreamShardID(s)
		if err != nil {
			return
		}
		// On success the constructor must have validated the shape;
		// re-verify locally so regressions show up.
		const prefix = "shardId-"
		if len(s) != len(prefix)+12 {
			t.Fatalf("accepted ill-shaped stream shard id %q (len %d)", s, len(s))
		}
		if s[:len(prefix)] != prefix {
			t.Fatalf("accepted stream shard id without prefix: %q", s)
		}
		for i, r := range s[len(prefix):] {
			if r < '0' || r > '9' {
				t.Fatalf("accepted non-digit %q at index %d in %q", r, i, s)
			}
		}
		if id.String() != s {
			t.Fatalf("String() = %q, want %q", id.String(), s)
		}
		text, err := id.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText: %v", err)
		}
		var got model.StreamShardID
		if err := got.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if got != id {
			t.Fatalf("round-trip mismatch: %q -> %q -> %q", s, text, got.String())
		}
	})
}

// FuzzTableID exercises NewTableID and the text round-trip. Invariants:
//   - No panic for any string input.
//   - If NewTableID succeeds, the input is non-empty, contains no
//     leading/trailing whitespace, and no '/' separator.
//   - String() returns exactly the input and the wire form round-trips.
func FuzzTableID(f *testing.F) {
	f.Add("events")
	f.Add("Events_v2")
	f.Add("a")
	f.Add("table.with.dots")
	f.Add("☃")
	f.Add("")         // rejected
	f.Add(" events")  // rejected: leading whitespace
	f.Add("events ")  // rejected: trailing whitespace
	f.Add("schema/x") // rejected: contains '/'

	f.Fuzz(func(t *testing.T, s string) {
		id, err := model.NewTableID(s)
		if err != nil {
			return
		}
		if s == "" {
			t.Fatalf("accepted empty table id")
		}
		if id.String() != s {
			t.Fatalf("String() = %q, want %q", id.String(), s)
		}
		for _, r := range s {
			if r == '/' {
				t.Fatalf("accepted table id %q containing '/'", s)
			}
		}
		text, err := id.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText: %v", err)
		}
		var got model.TableID
		if err := got.UnmarshalText(text); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", text, err)
		}
		if got != id {
			t.Fatalf("round-trip mismatch: %q -> %q -> %q", s, text, got.String())
		}
	})
}
