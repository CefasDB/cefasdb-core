// Package query is a deprecated public-API shim that re-exports
// the symbols third-party plugin code depends on from
// internal/core/query.
//
// Deprecated: import "github.com/CefasDb/cefasdb/internal/core/query"
// directly. This shim exists so plugin authors who imported
// query.DistanceOp keep building during a migration window. It will
// be removed in a future minor release.
package query

import (
	internalquery "github.com/CefasDb/cefasdb/internal/core/query"
)

// DistanceOp aliases internal/core/query.DistanceOp. Plugin
// implementations (cosine, euclidean, haversine, ...) declare
// themselves against this interface.
//
// Deprecated: use internal/core/query.DistanceOp.
type DistanceOp = internalquery.DistanceOp
