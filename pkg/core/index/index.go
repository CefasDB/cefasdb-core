// Package index is a deprecated public-API shim that re-exports
// every symbol from internal/core/index.
//
// Deprecated: import "github.com/osvaldoandrade/cefas/internal/core/index"
// directly. This shim exists so external plugin authors who imported
// the public path before the PR 5 carve-out keep building during a
// migration window. It will be removed in a future minor release.
package index

import (
	internalindex "github.com/osvaldoandrade/cefas/internal/core/index"
)

// Descriptor aliases internal/core/index.Descriptor.
//
// Deprecated: use internal/core/index.Descriptor.
type Descriptor = internalindex.Descriptor

// Lifecycle aliases internal/core/index.Lifecycle.
//
// Deprecated: use internal/core/index.Lifecycle.
type Lifecycle = internalindex.Lifecycle
