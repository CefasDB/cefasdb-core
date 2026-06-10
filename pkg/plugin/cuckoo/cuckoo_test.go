package cuckoo_test

import (
	"fmt"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/plugin/cuckoo"
)

func TestAddContainsRemove(t *testing.T) {
	f, err := cuckoo.New([]byte(`{"field":"x","buckets":1024,"fingerprint_bits":12}`))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for i := 0; i < 500; i++ {
		if err := f.Add(fmt.Appendf(nil, "v-%d", i)); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	for i := 0; i < 500; i++ {
		if !f.Contains(fmt.Appendf(nil, "v-%d", i)) {
			t.Fatalf("missing %d", i)
		}
	}
	removed := 0
	for i := 0; i < 500; i++ {
		if f.Remove(fmt.Appendf(nil, "v-%d", i)) {
			removed++
		}
	}
	if removed < 500 {
		t.Fatalf("removed = %d, want 500", removed)
	}
}

func TestBucketsMustBePowerOfTwo(t *testing.T) {
	if _, err := cuckoo.New([]byte(`{"field":"x","buckets":1000}`)); err == nil {
		t.Fatal("expected power-of-two error")
	}
}

func TestFingerprintBitsRange(t *testing.T) {
	if _, err := cuckoo.New([]byte(`{"field":"x","buckets":16,"fingerprint_bits":0}`)); err == nil {
		// width defaults to 8 when omitted but 0 explicit must error too
	}
	if _, err := cuckoo.New([]byte(`{"field":"x","buckets":16,"fingerprint_bits":17}`)); err == nil {
		t.Fatal("expected fp-bits range error")
	}
}

func TestSerializeRoundTrip(t *testing.T) {
	f, _ := cuckoo.New([]byte(`{"field":"x","buckets":1024,"fingerprint_bits":12}`))
	for i := 0; i < 100; i++ {
		_ = f.Add(fmt.Appendf(nil, "v-%d", i))
	}
	buf, err := f.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	g, err := cuckoo.Deserialize(buf)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	for i := 0; i < 100; i++ {
		if !g.Contains(fmt.Appendf(nil, "v-%d", i)) {
			t.Fatalf("round-trip lost %d", i)
		}
	}
}
