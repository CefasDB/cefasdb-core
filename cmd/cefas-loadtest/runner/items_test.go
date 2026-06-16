package runner

import (
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestMakeItemKeyRoundTrip(t *testing.T) {
	t.Parallel()
	const users int64 = 100
	for _, id := range []int64{0, 1, 42, 999, 1_000_000} {
		item := makeItem(id, users, "payload")
		key := keyFor(id, users)
		if item["pk"].S != key["pk"].S {
			t.Fatalf("pk mismatch for id=%d: item=%q key=%q", id, item["pk"].S, key["pk"].S)
		}
		if item["sk"].S != key["sk"].S {
			t.Fatalf("sk mismatch for id=%d: item=%q key=%q", id, item["sk"].S, key["sk"].S)
		}
		if item["payload"].T != types.AttrS {
			t.Fatalf("payload attr type for id=%d: got %v", id, item["payload"].T)
		}
		if item["payload"].S != "payload" {
			t.Fatalf("payload value for id=%d: got %q", id, item["payload"].S)
		}
		if item["active"].T != types.AttrBOOL {
			t.Fatalf("active attr type for id=%d: got %v", id, item["active"].T)
		}
		if item["active"].BOOL != (id%2 == 0) {
			t.Fatalf("active value for id=%d: got %v", id, item["active"].BOOL)
		}
	}
}

func TestCityForCycles(t *testing.T) {
	t.Parallel()
	want := []string{"Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba"}
	for i, w := range want {
		if got := cityFor(int64(i)); got != w {
			t.Fatalf("cityFor(%d) = %q, want %q", i, got, w)
		}
	}
	// Wraps modulo 4.
	if cityFor(4) != cityFor(0) {
		t.Fatal("cityFor should be modulo 4")
	}
}

func TestPermuteStaysInRange(t *testing.T) {
	t.Parallel()
	const modulo int64 = 1000
	seen := make(map[int64]struct{})
	for seq := int64(0); seq < 5_000; seq++ {
		x := permute(seq, modulo)
		if x < 0 || x >= modulo {
			t.Fatalf("permute(%d, %d) = %d out of range", seq, modulo, x)
		}
		seen[x] = struct{}{}
	}
	// With 5_000 trials over 1_000 slots the spread should be wide.
	if len(seen) < 900 {
		t.Fatalf("permute coverage too low: %d distinct values", len(seen))
	}
}

func TestPermuteModuloEdges(t *testing.T) {
	t.Parallel()
	if permute(0, 0) != 0 {
		t.Fatal("permute with modulo=0 should return 0")
	}
	if permute(42, 1) != 0 {
		t.Fatal("permute with modulo=1 should return 0")
	}
}
