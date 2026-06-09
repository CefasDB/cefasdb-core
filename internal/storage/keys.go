// Package storage implements the cefas storage engine on top of Pebble.
//
// Key layout — all data keys live under the "cefas/" namespace so the
// underlying Pebble directory can be shared with Raft state (under
// "raft/", phase 4) without prefix collisions. Numeric components that
// need lexicographic ordering are big-endian fixed-width bytes (8 bytes
// for hash buckets, scores, etc.).
//
//	cefas/catalog/<table>                                   → JSON TableDescriptor
//	cefas/t/<table>/p/<pk_hash8>/<sk_bytes>                 → encoded Item
//	cefas/t/<table>/gsi/<idx>/<gsi_pk>/<gsi_sk>/<id>        → pointer (Phase 2)
//	cefas/t/<table>/geo/<idx>/<geohash>/<id>                → pointer (Phase 3)
//	cefas/t/<table>/zorder/<idx>/<morton_be>/<id>           → pointer (Phase 3)
//
// pk_hash8 = first 8 bytes of xxhash64(serialized PK). The hash keeps the
// primary lookup O(1) and uniformly distributes load across multi-Raft
// shards (Phase 5). SK is appended raw so range scans within a single PK
// bucket sort by SK natively.
package storage

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

const (
	Namespace = "cefas/"
	pCatalog  = Namespace + "catalog/"
	pTables   = Namespace + "t/"

	segPrimary = "/p/"
	segGSI     = "/gsi/"
	segGeo     = "/geo/"
	segZorder  = "/zorder/"
)

// KeyCatalog returns the catalog descriptor key for a table.
func KeyCatalog(table string) []byte {
	return []byte(pCatalog + table)
}

// PrefixCatalog covers every table descriptor for catalog scans.
func PrefixCatalog() (lower, upper []byte) {
	p := []byte(pCatalog)
	return p, prefixUpper(p)
}

// KeyPrimary returns the storage key for a single item.
//
//	cefas/t/<table>/p/<pk_hash8>/<sk_bytes>
//
// pkSerialized is the canonical byte representation of the PK attribute
// (see attrCanonicalBytes in item_codec.go). skBytes may be nil (no sort
// key) — in that case the key terminates after pk_hash8.
func KeyPrimary(table string, pkSerialized, skBytes []byte) []byte {
	base := tableBase(table) + segPrimary
	h := pkHash8(pkSerialized)
	k := make([]byte, 0, len(base)+8+len(skBytes))
	k = append(k, base...)
	k = append(k, h...)
	k = append(k, skBytes...)
	return k
}

// PrefixPrimaryAll returns the [lower, upper) bound covering every item
// in a table — used for Scan.
func PrefixPrimaryAll(table string) (lower, upper []byte) {
	p := []byte(tableBase(table) + segPrimary)
	return p, prefixUpper(p)
}

// PrefixPrimaryByPK returns the [lower, upper) bound covering every SK
// under a single PK — used for Query without an SK condition.
func PrefixPrimaryByPK(table string, pkSerialized []byte) (lower, upper []byte) {
	base := tableBase(table) + segPrimary
	h := pkHash8(pkSerialized)
	p := make([]byte, 0, len(base)+8)
	p = append(p, base...)
	p = append(p, h...)
	return p, prefixUpper(p)
}

// RangePrimaryBySK returns the [lower, upper) bound for a Query that
// constrains SK to [skLow, skHigh) inside a single PK. skLow / skHigh are
// the raw serialized SK byte forms. skHigh may be nil to mean "open
// upper bound within this PK" (i.e. equivalent to PrefixPrimaryByPK).
func RangePrimaryBySK(table string, pkSerialized, skLow, skHigh []byte) (lower, upper []byte) {
	base := tableBase(table) + segPrimary
	h := pkHash8(pkSerialized)

	lower = make([]byte, 0, len(base)+8+len(skLow))
	lower = append(lower, base...)
	lower = append(lower, h...)
	lower = append(lower, skLow...)

	if skHigh == nil {
		// Open upper within the bucket.
		p := make([]byte, 0, len(base)+8)
		p = append(p, base...)
		p = append(p, h...)
		upper = prefixUpper(p)
	} else {
		upper = make([]byte, 0, len(base)+8+len(skHigh))
		upper = append(upper, base...)
		upper = append(upper, h...)
		upper = append(upper, skHigh...)
	}
	return lower, upper
}

func tableBase(table string) string { return pTables + table }

func pkHash8(serialized []byte) []byte {
	h := xxhash.Sum64(serialized)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], h)
	return b[:]
}

// prefixUpper returns the smallest key strictly greater than every key
// starting with p. The cefas namespace ends with ASCII-safe bytes, so
// incrementing the last byte gives a clean exclusive upper bound. Same
// pattern as codeq/internal/repository/pebble/keys.go.
func prefixUpper(p []byte) []byte {
	u := make([]byte, len(p))
	copy(u, p)
	for i := len(u) - 1; i >= 0; i-- {
		if u[i] < 0xff {
			u[i]++
			return u[:i+1]
		}
	}
	return nil
}

// be8 encodes a uint64 in big-endian — used by future score-keyed indexes.
func be8(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}
