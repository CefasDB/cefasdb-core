package pebble

import (
	"testing"

	"github.com/CefasDb/cefasdb/internal/storage"
)

func TestItemCacheBoundedAndCopiesValues(t *testing.T) {
	cache := newItemCache(2)
	cache.set([]byte("k1"), []byte("v1"))
	cache.set([]byte("k2"), []byte("v2"))

	got, ok := cache.get([]byte("k1"))
	if !ok || string(got) != "v1" {
		t.Fatalf("k1 = %q, %v", got, ok)
	}
	got[0] = 'x'
	got, ok = cache.get([]byte("k1"))
	if !ok || string(got) != "v1" {
		t.Fatalf("cache value mutated through returned slice: %q, %v", got, ok)
	}

	cache.set([]byte("k3"), []byte("v3"))
	if _, ok := cache.get([]byte("k1")); ok {
		t.Fatal("expected oldest key to be evicted")
	}
	if got, ok := cache.get([]byte("k2")); !ok || string(got) != "v2" {
		t.Fatalf("k2 = %q, %v", got, ok)
	}
	if got, ok := cache.get([]byte("k3")); !ok || string(got) != "v3" {
		t.Fatalf("k3 = %q, %v", got, ok)
	}
}

func TestItemCacheInvalidatesByTable(t *testing.T) {
	cache := newItemCache(4)
	k1 := storage.KeyPrimary("t1", []byte("pk1"), nil)
	k2 := storage.KeyPrimary("t2", []byte("pk2"), nil)
	cache.set(k1, []byte("v1"))
	cache.set(k2, []byte("v2"))

	cache.deleteKeysForTables([][]byte{storage.KeyPrimary("t3", []byte("pk3"), nil)}, []string{"t3"})
	if got, ok := cache.get(k1); !ok || string(got) != "v1" {
		t.Fatalf("t1 cache after unrelated table invalidation = %q, %v", got, ok)
	}
	if got, ok := cache.get(k2); !ok || string(got) != "v2" {
		t.Fatalf("t2 cache after unrelated table invalidation = %q, %v", got, ok)
	}

	cache.deleteKeysForTables([][]byte{k1}, []string{"t1"})
	if _, ok := cache.get(k1); ok {
		t.Fatal("expected t1 key to be invalidated")
	}
	if cache.hasAnyTable([]string{"t1"}) {
		t.Fatal("expected t1 table count to be removed")
	}
	if got, ok := cache.get(k2); !ok || string(got) != "v2" {
		t.Fatalf("t2 cache after t1 invalidation = %q, %v", got, ok)
	}
}
