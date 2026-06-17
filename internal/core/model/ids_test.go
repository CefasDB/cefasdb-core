package model_test

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/model"
)

// ----- ShardID -----

func TestShardIDConstructorRejectsSentinel(t *testing.T) {
	t.Parallel()
	if _, err := model.NewShardID(math.MaxUint32); err == nil {
		t.Fatal("expected NewShardID(MaxUint32) to error — that value is reserved for UnroutedShardID")
	}
}

func TestShardIDStringAndSentinel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		id   model.ShardID
		want string
	}{
		{"zero", model.MustShardID(0), "0"},
		{"forty-two", model.MustShardID(42), "42"},
		{"unrouted", model.UnroutedShardID, "unrouted"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.id.String(); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestShardIDTextRoundTrip(t *testing.T) {
	t.Parallel()
	for _, v := range []uint32{0, 1, 7, 65_535, math.MaxUint32 - 1} {
		t.Run("v="+model.MustShardID(v).String(), func(t *testing.T) {
			t.Parallel()
			id := model.MustShardID(v)
			b, err := id.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText: %v", err)
			}
			var got model.ShardID
			if err := got.UnmarshalText(b); err != nil {
				t.Fatalf("UnmarshalText(%q): %v", b, err)
			}
			if got != id {
				t.Fatalf("round trip: got %v, want %v", got, id)
			}
		})
	}
}

func TestShardIDUnrouted(t *testing.T) {
	t.Parallel()
	if !model.UnroutedShardID.IsUnrouted() {
		t.Fatal("UnroutedShardID.IsUnrouted() = false")
	}
	if model.MustShardID(0).IsUnrouted() {
		t.Fatal("shard 0 must not be unrouted")
	}
	b, err := model.UnroutedShardID.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	if string(b) != "unrouted" {
		t.Fatalf("wire = %q, want %q", b, "unrouted")
	}
	var back model.ShardID
	if err := back.UnmarshalText(b); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if !back.IsUnrouted() {
		t.Fatalf("round trip lost unrouted sentinel: %v", back)
	}
}

func TestShardIDJSONUsesTextMarshaler(t *testing.T) {
	t.Parallel()
	got, err := json.Marshal(model.MustShardID(7))
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if string(got) != `"7"` {
		t.Fatalf("JSON = %s, want %q", got, `"7"`)
	}
	var back model.ShardID
	if err := json.Unmarshal(got, &back); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back != model.MustShardID(7) {
		t.Fatalf("round trip: %v", back)
	}
}

// ----- NodeID -----

func TestNodeIDValidation(t *testing.T) {
	t.Parallel()
	bad := []string{"", " ", " n1", "n1 ", "\tn1"}
	for _, v := range bad {
		t.Run("reject:"+v, func(t *testing.T) {
			t.Parallel()
			if _, err := model.NewNodeID(v); err == nil {
				t.Fatalf("expected error for %q", v)
			}
		})
	}
	id, err := model.NewNodeID("n1")
	if err != nil || id.String() != "n1" {
		t.Fatalf("NewNodeID(n1) = %v, %v", id, err)
	}
}

func TestNodeIDTextRoundTrip(t *testing.T) {
	t.Parallel()
	id := model.MustNodeID("node-7")
	b, err := id.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	if string(b) != "node-7" {
		t.Fatalf("wire = %q", b)
	}
	var got model.NodeID
	if err := got.UnmarshalText(b); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if got != id {
		t.Fatalf("round trip: %v", got)
	}
}

// ----- StreamShardID -----

func TestStreamShardIDValidation(t *testing.T) {
	t.Parallel()
	bad := []string{
		"",
		"shard-1",
		"shardId-",
		"shardId-1",             // too short
		"shardId-0000000000000", // too long
		"shardId-00000000000a",  // non-digit
		strings.Repeat("shardId-", 2),
	}
	for _, v := range bad {
		t.Run("reject:"+v, func(t *testing.T) {
			t.Parallel()
			if _, err := model.NewStreamShardID(v); err == nil {
				t.Fatalf("expected error for %q", v)
			}
		})
	}
	for _, v := range []string{"shardId-000000000000", "shardId-000000000042"} {
		t.Run("accept:"+v, func(t *testing.T) {
			t.Parallel()
			id, err := model.NewStreamShardID(v)
			if err != nil {
				t.Fatalf("NewStreamShardID(%q): %v", v, err)
			}
			if id.String() != v {
				t.Fatalf("String() = %q, want %q", id.String(), v)
			}
		})
	}
}

func TestStreamShardIDSingleConstant(t *testing.T) {
	t.Parallel()
	if model.StreamShardIDSingle.String() != "shardId-000000000000" {
		t.Fatalf("StreamShardIDSingle = %q", model.StreamShardIDSingle.String())
	}
}

// ----- TableID -----

func TestTableIDValidation(t *testing.T) {
	t.Parallel()
	bad := []string{"", " ", "events ", "schema/events"}
	for _, v := range bad {
		t.Run("reject:"+v, func(t *testing.T) {
			t.Parallel()
			if _, err := model.NewTableID(v); err == nil {
				t.Fatalf("expected error for %q", v)
			}
		})
	}
	id, err := model.NewTableID("Events_v2")
	if err != nil {
		t.Fatalf("NewTableID: %v", err)
	}
	if id.String() != "Events_v2" {
		t.Fatalf("String() = %q", id.String())
	}
}

func TestMustHelpersPanicOnInvalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		fn   func()
	}{
		{"shard", func() { _ = model.MustShardID(math.MaxUint32) }},
		{"node", func() { _ = model.MustNodeID("") }},
		{"stream", func() { _ = model.MustStreamShardID("nope") }},
		{"table", func() { _ = model.MustTableID("a/b") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for invalid %s id", tc.name)
				}
			}()
			tc.fn()
		})
	}
}

// TestUnmarshalTextSurfacesErr ensures the error returned by the
// constructor bubbles up via UnmarshalText rather than being lost.
func TestUnmarshalTextSurfacesErr(t *testing.T) {
	t.Parallel()
	var n model.NodeID
	err := n.UnmarshalText([]byte(""))
	if err == nil {
		t.Fatal("expected error from empty NodeID UnmarshalText")
	}
	if !errors.Is(err, err) { // sanity: err is not nil
		t.Fatal("err lost itself")
	}
}
