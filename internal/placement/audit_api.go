package placement

import (
	"bytes"

	"github.com/osvaldoandrade/cefas/internal/storage"
)

// AuditCollector is the public-facing wrapper around the internal
// issue collector. The internal collector type stays unexported (its
// invariants are private to the placement package); this wrapper lets
// the cluster Manager — which runs the storage-walking part of the
// audit in a sibling package — record issues using the same caps and
// truncation rules as AuditPlacementCatalog.
type AuditCollector struct {
	inner *placementAuditCollector
}

// NewAuditCollector returns a collector pre-seeded with issues already
// found by AuditPlacementCatalog. maxIssues = 0 picks the default.
func NewAuditCollector(maxIssues int, prior []PlacementAuditIssue, truncated bool) *AuditCollector {
	return &AuditCollector{inner: &placementAuditCollector{
		max:       auditMaxIssues(maxIssues),
		issues:    append([]PlacementAuditIssue(nil), prior...),
		truncated: truncated,
	}}
}

// Add records an issue. Returns false when the collector is full.
func (c *AuditCollector) Add(issue PlacementAuditIssue) bool {
	return c.inner.add(issue)
}

// MarkTruncated flags the collector as truncated without adding an
// issue. Used when the caller stops scanning before hitting the cap.
func (c *AuditCollector) MarkTruncated() {
	c.inner.truncated = true
}

// Truncated reports whether the collector hit its issue cap.
func (c *AuditCollector) Truncated() bool { return c.inner.truncated }

// Issues returns the issues collected so far.
func (c *AuditCollector) Issues() []PlacementAuditIssue { return c.inner.issues }

// FinishAuditReport finalises the report after the Manager has folded
// in the per-shard storage scan results. Calls report.finish under the
// hood.
func FinishAuditReport(r *PlacementAuditReport, req PlacementAuditRequest) {
	r.finish(req)
}

// AuditMaxPrimaryKeysPerShard returns the audited cap (or the package
// default when v <= 0).
func AuditMaxPrimaryKeysPerShard(v int) int {
	return auditMaxPrimaryKeysPerShard(v)
}

// AuditCatalogTableFromKey extracts the table name from a catalog-key
// blob (`<ns>catalog/<name>`). Returns ("", false) when key does not
// match the expected prefix.
func AuditCatalogTableFromKey(key []byte) (string, bool) {
	return auditCatalogTableFromKey(key)
}

// AuditPrimaryTableFromKey extracts the table name from a primary-row
// key (`<ns>t/<table>/p/...`).
func AuditPrimaryTableFromKey(key []byte) (string, bool) {
	prefix := []byte(storage.Namespace + "t/")
	if !bytes.HasPrefix(key, prefix) {
		return "", false
	}
	rest := key[len(prefix):]
	pos := bytes.Index(rest, []byte("/p/"))
	if pos <= 0 {
		return "", false
	}
	return string(rest[:pos]), true
}

// ActiveOwnersForToken returns the shard IDs that own the given
// token under cat's active placement.
func ActiveOwnersForToken(cat PlacementCatalog, token uint64) []uint32 {
	return activeOwnersForToken(cat, token)
}

// ContainsUint32 returns true when v appears in in.
func ContainsUint32(in []uint32, v uint32) bool {
	return containsUint32(in, v)
}

// U32ptr returns the address of a uint32 literal — handy for building
// PlacementAuditIssue values inline.
func U32ptr(v uint32) *uint32 { return u32ptr(v) }

// U64ptr returns the address of a uint64 literal.
func U64ptr(v uint64) *uint64 { return u64ptr(v) }
