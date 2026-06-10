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
	"fmt"

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

// ---------- GSI keys ----------
//
// Layout for a single GSI entry:
//
//	cefas/t/<table>/gsi/<idx>/<gsi_pk_hash8><gsi_sk_bytes><pk_hash8>
//
// gsi_pk_hash8 is a fixed-width hash of the GSI partition value, so
// equality lookups on the GSI PK are a single iterator with a static
// prefix. The GSI SK bytes follow directly so range scans within a
// partition sort lexicographically — exactly the same trick we play on
// the primary table. The 8-byte trailing pk_hash8 disambiguates entries
// whose (gsi_pk, gsi_sk) collide on different primary items, which is
// legal because the team may project a non-unique attribute as GSI SK.
//
// The pointer value stores the primary key reference so a GSI iterator
// can resolve back to the underlying item with one Get per row. The
// reference layout is documented in EncodeGSIPointer / DecodeGSIPointer.

// KeyGSI builds a GSI entry key. gsiPK / gsiSK are the canonical bytes
// of the indexed attributes; primaryPK / primarySK are the bytes that
// identify the underlying item (used here only to compute the unique
// disambiguator suffix).
func KeyGSI(table, idxName string, gsiPK, gsiSK, primaryPK, primarySK []byte) []byte {
	base := tableBase(table) + segGSI + idxName + "/"
	gph := pkHash8(gsiPK)
	suffix := primaryDisambiguator(primaryPK, primarySK)
	k := make([]byte, 0, len(base)+8+len(gsiSK)+8)
	k = append(k, base...)
	k = append(k, gph...)
	k = append(k, gsiSK...)
	k = append(k, suffix...)
	return k
}

// PrefixGSIByPK returns [lower, upper) covering every entry for a
// single GSI partition (every SK under one gsi_pk).
func PrefixGSIByPK(table, idxName string, gsiPK []byte) (lower, upper []byte) {
	base := tableBase(table) + segGSI + idxName + "/"
	gph := pkHash8(gsiPK)
	p := make([]byte, 0, len(base)+8)
	p = append(p, base...)
	p = append(p, gph...)
	return p, prefixUpper(p)
}

// RangeGSIBySK constrains the GSI scan to entries where gsi_sk falls in
// [skLow, skHigh) within a single gsi_pk. Either bound may be nil for
// open-ended ranges on that side.
func RangeGSIBySK(table, idxName string, gsiPK, skLow, skHigh []byte) (lower, upper []byte) {
	base := tableBase(table) + segGSI + idxName + "/"
	gph := pkHash8(gsiPK)

	lower = make([]byte, 0, len(base)+8+len(skLow))
	lower = append(lower, base...)
	lower = append(lower, gph...)
	lower = append(lower, skLow...)

	if skHigh == nil {
		p := make([]byte, 0, len(base)+8)
		p = append(p, base...)
		p = append(p, gph...)
		upper = prefixUpper(p)
	} else {
		upper = make([]byte, 0, len(base)+8+len(skHigh))
		upper = append(upper, base...)
		upper = append(upper, gph...)
		upper = append(upper, skHigh...)
	}
	return lower, upper
}

// primaryDisambiguator hashes the full primary key (PK + SK) into 8
// bytes. The pair is stable per item, so two GSI entries for the same
// (gsi_pk, gsi_sk) belonging to different items get distinct suffixes
// and the same item's old GSI entry can be located by recomputing the
// hash at update time.
func primaryDisambiguator(primaryPK, primarySK []byte) []byte {
	h := xxhash.New()
	_, _ = h.Write(primaryPK)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(primarySK)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], h.Sum64())
	return b[:]
}

// EncodeGSIPointer packs the primary key reference stored as the GSI
// entry value. Reader extracts (pkBytes, skBytes) and rebuilds the
// primary key with KeyPrimary.
func EncodeGSIPointer(primaryPK, primarySK []byte) []byte {
	out := make([]byte, 0, 4+len(primaryPK)+len(primarySK))
	out = appendUvarint(out, uint64(len(primaryPK)))
	out = append(out, primaryPK...)
	out = appendUvarint(out, uint64(len(primarySK)))
	out = append(out, primarySK...)
	return out
}

// DecodeGSIPointer reverses EncodeGSIPointer. Returns the primary PK
// and SK byte forms used to compute the primary item key.
func DecodeGSIPointer(data []byte) (primaryPK, primarySK []byte, err error) {
	n, rest, err := readUvarint(data)
	if err != nil {
		return nil, nil, fmt.Errorf("gsi pointer pk len: %w", err)
	}
	if uint64(len(rest)) < n {
		return nil, nil, fmt.Errorf("gsi pointer pk: short")
	}
	primaryPK = append([]byte(nil), rest[:n]...)
	rest = rest[n:]
	n, rest, err = readUvarint(rest)
	if err != nil {
		return nil, nil, fmt.Errorf("gsi pointer sk len: %w", err)
	}
	if uint64(len(rest)) < n {
		return nil, nil, fmt.Errorf("gsi pointer sk: short")
	}
	primarySK = append([]byte(nil), rest[:n]...)
	return primaryPK, primarySK, nil
}
