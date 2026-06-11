// Package builtins blank-imports every in-tree plugin so a single
// import of this package wires them into plugin.Default. The server
// imports this; tests that need an isolated registry skip it and use
// pkg/plugin.NewRegistry().
//
// Adding a new built-in plugin: drop another blank import below.
package builtins

import (
	// Index plugins (Epic 3).
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/bloom"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/cbloom"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/cms"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/cuckoo"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/hll"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/roaring"

	// Distance plugins (Epic 4).
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/cosine"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/damerau"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/euclidean"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/hamming"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/haversine"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/jaccard"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/jarowinkler"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/levenshtein"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/manhattan"

	// Search index plugins (Epic 5).
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/minhash"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/radix"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/simhash"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/trigram"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/vectorlsh"

	// Geo + ads (Epic 6). audience must come after geohash + hll so
	// its init() can look them up from plugin.Default at registration.
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/audience"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/geohash"

	// Bandit operators (issue #246).
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/bandit"
)
