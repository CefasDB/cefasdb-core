// Package builtins blank-imports every in-tree plugin so a single
// import of this package wires them into plugin.Default. The server
// imports this; tests that need an isolated registry skip it and use
// pkg/plugin.NewRegistry().
//
// Adding a new built-in plugin: drop another blank import below.
package builtins

import (
	// Index plugins (Epic 3).
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/bloom"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/cbloom"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/cms"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/cuckoo"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/hll"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/roaring"

	// Distance plugins (Epic 4).
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/cosine"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/damerau"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/euclidean"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/hamming"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/haversine"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/jaccard"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/jarowinkler"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/levenshtein"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/manhattan"

	// Search index plugins (Epic 5).
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/minhash"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/radix"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/simhash"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/trigram"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/vectorlsh"

	// Geo + ads (Epic 6). audience must come after geohash + hll so
	// its init() can look them up from plugin.Default at registration.
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/audience"
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/geohash"

	// Bandit operators (issue #246).
	_ "github.com/CefasDb/cefasdb/internal/plugin/builtin/bandit"
)
