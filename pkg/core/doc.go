// Package core hosts the deprecated public-API shims for the types
// third-party plugin code used to import from pkg/core/*.
//
// Deprecated: every sub-package here is a thin alias layer to the
// canonical implementation at internal/core/<same name>. The shims
// exist so external plugin authors who pinned to pkg/core/{index,
// model,query} keep compiling for a migration window. New code
// should import internal/core/<X> directly. The shims will be
// removed in a future minor release.
//
// Migration map:
//
//	pkg/core/index → internal/core/index
//	pkg/core/model → internal/core/model
//	pkg/core/query → internal/core/query
//
// Sub-packages that were never on the third-party plugin surface
// (condition, query/mmr, stream, ttl) moved directly to internal/
// without a shim.
//
// The "no engine imports" invariant for pkg/core/ is still pinned
// by the coregraph test, with the shim files explicitly exempted —
// they violate the rule by design, but only as transitional
// scaffolding.
package core
