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
	"bytes"
	"encoding/binary"
	"fmt"
	"net/url"

	"github.com/cespare/xxhash/v2"
)

const (
	Namespace = "cefas/"
	pCatalog  = Namespace + "catalog/"
	pTables   = Namespace + "t/"
	pAdmin    = Namespace + "admin/"
	pInternal = Namespace + "internal/"

	pPluginIndex = pInternal + "plugin-index/"

	segPrimary = "/p/"
	segGSI     = "/gsi/"
	segLSI     = "/lsi/"
	segGeo     = "/geo/"
	segZorder  = "/zorder/"
	segTTL     = "/ttl/"
)

var changeCounterKey = []byte(pAdmin + "change/counter")

func KeyChangeLog(index uint64) []byte {
	base := []byte(pAdmin + "change/log/")
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], index)
	out := make([]byte, 0, len(base)+len(b))
	out = append(out, base...)
	out = append(out, b[:]...)
	return out
}

func PrefixChangeLog() (lower, upper []byte) {
	p := []byte(pAdmin + "change/log/")
	return p, prefixUpper(p)
}

// KeyCatalog returns the catalog descriptor key for a table.
func KeyCatalog(table string) []byte {
	return []byte(pCatalog + table)
}

// PrefixCatalog covers every table descriptor for catalog scans.
func PrefixCatalog() (lower, upper []byte) {
	p := []byte(pCatalog)
	return p, prefixUpper(p)
}

// KeyPluginIndexDescriptor stores a plugin-backed index descriptor.
func KeyPluginIndexDescriptor(table, name string) []byte {
	return []byte(pPluginIndex + escapeKeySegment(table) + "/" + escapeKeySegment(name))
}

// PrefixPluginIndexDescriptors covers every plugin index descriptor.
func PrefixPluginIndexDescriptors() (lower, upper []byte) {
	p := []byte(pPluginIndex)
	return p, prefixUpper(p)
}

// PrefixPluginIndexTableDescriptors covers plugin index descriptors for one table.
func PrefixPluginIndexTableDescriptors(table string) (lower, upper []byte) {
	p := []byte(pPluginIndex + escapeKeySegment(table) + "/")
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

// PrefixTables returns the [lower, upper) bound covering every table-scoped
// data key, across all tables.
func PrefixTables() (lower, upper []byte) {
	p := []byte(pTables)
	return p, prefixUpper(p)
}

// PrefixTable returns the [lower, upper) bound covering every key that belongs
// to a table, including primary rows and secondary index entries.
func PrefixTable(table string) (lower, upper []byte) {
	p := []byte(tableBase(table) + "/")
	return p, prefixUpper(p)
}

// PrimaryTokenFromKey extracts the uint64 partition token from a primary-row
// storage key. It returns false for catalog, index, TTL, and malformed keys.
func PrimaryTokenFromKey(key []byte) (uint64, bool) {
	if !bytes.HasPrefix(key, []byte(pTables)) {
		return 0, false
	}
	rest := key[len(pTables):]
	pos := bytes.Index(rest, []byte(segPrimary))
	if pos < 0 {
		return 0, false
	}
	tokenStart := len(pTables) + pos + len(segPrimary)
	if len(key) < tokenStart+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[tokenStart : tokenStart+8]), true
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

func escapeKeySegment(s string) string { return url.PathEscape(s) }

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

// ---------- LSI keys ----------
//
// LSI rows live in the primary partition's hash bucket so writes
// never leave the local shard. The disambiguator suffix is the
// item's primary SK bytes (the partition is already pinned by
// pk_hash8, so we don't need the primaryDisambiguator hash).
//
//	cefas/t/<table>/lsi/<idx>/<primary_pk_hash8><lsi_sk_bytes><primary_sk_bytes>

// KeyLSI builds a local-secondary-index entry key.
func KeyLSI(table, idxName string, primaryPK, lsiSK, primarySK []byte) []byte {
	base := tableBase(table) + segLSI + idxName + "/"
	pkh := pkHash8(primaryPK)
	k := make([]byte, 0, len(base)+8+len(lsiSK)+len(primarySK))
	k = append(k, base...)
	k = append(k, pkh...)
	k = append(k, lsiSK...)
	k = append(k, primarySK...)
	return k
}

// PrefixLSIByPK returns [lower, upper) covering every LSI entry for a
// single primary partition.
func PrefixLSIByPK(table, idxName string, primaryPK []byte) (lower, upper []byte) {
	base := tableBase(table) + segLSI + idxName + "/"
	pkh := pkHash8(primaryPK)
	p := make([]byte, 0, len(base)+8)
	p = append(p, base...)
	p = append(p, pkh...)
	return p, prefixUpper(p)
}

// RangeLSIBySK constrains the LSI scan to entries whose lsi_sk falls
// in [skLow, skHigh) inside a single primary partition. Either bound
// may be nil for an open-ended range.
func RangeLSIBySK(table, idxName string, primaryPK, skLow, skHigh []byte) (lower, upper []byte) {
	base := tableBase(table) + segLSI + idxName + "/"
	pkh := pkHash8(primaryPK)

	lower = make([]byte, 0, len(base)+8+len(skLow))
	lower = append(lower, base...)
	lower = append(lower, pkh...)
	lower = append(lower, skLow...)

	if skHigh == nil {
		p := make([]byte, 0, len(base)+8)
		p = append(p, base...)
		p = append(p, pkh...)
		upper = prefixUpper(p)
	} else {
		upper = make([]byte, 0, len(base)+8+len(skHigh))
		upper = append(upper, base...)
		upper = append(upper, pkh...)
		upper = append(upper, skHigh...)
	}
	return lower, upper
}

// ---------- TTL keys ----------
//
// TTL pointers live under a per-table prefix sorted by expire time.
// The reaper iterates them in ascending order and deletes both the
// pointer and the primary item once expire ≤ now.
//
//	cefas/t/<table>/ttl/<expire_be8>/<pk_hash8><sk_bytes>

// KeyTTL builds a TTL index entry for an item.
func KeyTTL(table string, expireUnix uint64, primaryPK, primarySK []byte) []byte {
	base := tableBase(table) + segTTL
	pkh := pkHash8(primaryPK)
	k := make([]byte, 0, len(base)+8+8+len(primarySK))
	k = append(k, base...)
	k = append(k, be8(expireUnix)...)
	k = append(k, pkh...)
	k = append(k, primarySK...)
	return k
}

// PrefixTTLBefore returns [lower, upper) covering every TTL entry
// whose expire time is strictly less than `before`. Used by the
// reaper to sweep expired rows.
func PrefixTTLBefore(table string, before uint64) (lower, upper []byte) {
	base := tableBase(table) + segTTL
	lower = []byte(base)
	upper = make([]byte, 0, len(base)+8)
	upper = append(upper, base...)
	upper = append(upper, be8(before)...)
	return lower, upper
}

// ParseTTLKey returns the primary key (pkHash8) and primarySK bytes
// embedded in a TTL entry key. The pk_hash8 is enough to identify
// which storage shard owns the row; the SK is the row's primary SK.
func ParseTTLKey(table string, key []byte) (pkHash, primarySK []byte, ok bool) {
	prefix := tableBase(table) + segTTL
	if len(key) < len(prefix)+8+8 || string(key[:len(prefix)]) != prefix {
		return nil, nil, false
	}
	rest := key[len(prefix)+8:] // skip the expire_be8
	pkHash = rest[:8]
	primarySK = rest[8:]
	return pkHash, primarySK, true
}

// ---------- spatial keys ----------
//
// Layouts:
//
//	cefas/t/<table>/geo/<idx>/<geohash_chars><pk_hash8>
//	cefas/t/<table>/zorder/<idx>/<morton_bytes><pk_hash8>
//
// Pointer value uses the same EncodeGSIPointer codec so the reader
// only needs to know how to resolve a (primary_pk, primary_sk) ref.
// The 8-byte pk_hash8 suffix mirrors the GSI disambiguator — two
// items whose coordinates round to the same cell produce distinct
// keys and remain individually addressable.

// KeyGeo builds a geohash-index entry key.
func KeyGeo(table, idxName, geohash string, primaryPK, primarySK []byte) []byte {
	base := tableBase(table) + segGeo + idxName + "/"
	suffix := primaryDisambiguator(primaryPK, primarySK)
	k := make([]byte, 0, len(base)+len(geohash)+8)
	k = append(k, base...)
	k = append(k, geohash...)
	k = append(k, suffix...)
	return k
}

// PrefixGeoCell returns [lower, upper) covering every entry whose
// geohash starts with `cellPrefix`. Used by the cover-set scan.
func PrefixGeoCell(table, idxName, cellPrefix string) (lower, upper []byte) {
	base := tableBase(table) + segGeo + idxName + "/"
	p := make([]byte, 0, len(base)+len(cellPrefix))
	p = append(p, base...)
	p = append(p, cellPrefix...)
	return p, prefixUpper(p)
}

// KeyZorder builds a Z-order-index entry key. mortonBytes is the
// fixed-width interleaved key produced by spatial.EncodeMorton.
func KeyZorder(table, idxName string, mortonBytes []byte, primaryPK, primarySK []byte) []byte {
	base := tableBase(table) + segZorder + idxName + "/"
	suffix := primaryDisambiguator(primaryPK, primarySK)
	k := make([]byte, 0, len(base)+len(mortonBytes)+8)
	k = append(k, base...)
	k = append(k, mortonBytes...)
	k = append(k, suffix...)
	return k
}

// RangeZorder returns [lower, upper) for a Morton range scan from
// `low` (inclusive) to `high` (inclusive). The caller produced low /
// high via spatial.MortonRange.
func RangeZorder(table, idxName string, low, high []byte) (lower, upper []byte) {
	base := tableBase(table) + segZorder + idxName + "/"
	lower = make([]byte, 0, len(base)+len(low))
	lower = append(lower, base...)
	lower = append(lower, low...)

	// Inclusive upper: append a 0xff disambiguator suffix so any
	// concrete entry whose Morton equals `high` sorts before upper.
	upper = make([]byte, 0, len(base)+len(high)+8)
	upper = append(upper, base...)
	upper = append(upper, high...)
	upper = append(upper, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...)
	// Strictly speaking we need one byte past the largest valid
	// disambiguator — use prefixUpper of (high + 0xff*8).
	upper = prefixUpper(upper)
	return lower, upper
}

// Index pointer markers. KEYS_ONLY pointers omit the marker for
// wire-compat with pre-projection entries. INCLUDE/ALL pointers
// lead with one of these reserved bytes which can never appear as
// the first byte of a varint length prefix (>= 0xFD).
const (
	projMarkerInclude byte = 0xFE
	projMarkerAll     byte = 0xFD
)

// EncodeProjectedPointer encodes a pointer that also carries
// projected attributes. mode is the Projection.Mode string from the
// descriptor; projected is the subset of the item that should travel
// with the pointer (INCLUDE: only the listed names; ALL: every
// attribute on the item).
func EncodeProjectedPointer(primaryPK, primarySK []byte, mode string, projected []byte) []byte {
	marker := byte(0)
	switch mode {
	case "INCLUDE":
		marker = projMarkerInclude
	case "ALL":
		marker = projMarkerAll
	default:
		// KEYS_ONLY → original layout, no marker.
		return EncodeGSIPointer(primaryPK, primarySK)
	}
	out := make([]byte, 0, 1+4+len(primaryPK)+len(primarySK)+len(projected))
	out = append(out, marker)
	out = appendUvarint(out, uint64(len(primaryPK)))
	out = append(out, primaryPK...)
	out = appendUvarint(out, uint64(len(primarySK)))
	out = append(out, primarySK...)
	out = appendUvarint(out, uint64(len(projected)))
	out = append(out, projected...)
	return out
}

// DecodeProjectedPointer reverses EncodeProjectedPointer. mode is
// inferred from the first byte: missing → "KEYS_ONLY".
// projectedBytes is the raw item bytes the caller can pass through
// DecodeItem to materialise the projected attributes (nil for
// KEYS_ONLY).
func DecodeProjectedPointer(data []byte) (primaryPK, primarySK, projectedBytes []byte, mode string, err error) {
	if len(data) == 0 {
		return nil, nil, nil, "", fmt.Errorf("projected pointer: empty")
	}
	switch data[0] {
	case projMarkerInclude:
		mode = "INCLUDE"
	case projMarkerAll:
		mode = "ALL"
	default:
		mode = "KEYS_ONLY"
		pk, sk, err := DecodeGSIPointer(data)
		return pk, sk, nil, mode, err
	}
	rest := data[1:]
	n, rest, err := readUvarint(rest)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("projected pointer pk len: %w", err)
	}
	if uint64(len(rest)) < n {
		return nil, nil, nil, "", fmt.Errorf("projected pointer pk: short")
	}
	primaryPK = append([]byte(nil), rest[:n]...)
	rest = rest[n:]
	n, rest, err = readUvarint(rest)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("projected pointer sk len: %w", err)
	}
	if uint64(len(rest)) < n {
		return nil, nil, nil, "", fmt.Errorf("projected pointer sk: short")
	}
	primarySK = append([]byte(nil), rest[:n]...)
	rest = rest[n:]
	n, rest, err = readUvarint(rest)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("projected pointer item len: %w", err)
	}
	if uint64(len(rest)) < n {
		return nil, nil, nil, "", fmt.Errorf("projected pointer item: short")
	}
	projectedBytes = append([]byte(nil), rest[:n]...)
	return primaryPK, primarySK, projectedBytes, mode, nil
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
