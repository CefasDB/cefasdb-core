// Package builtins blank-imports every in-tree plugin so a single
// import of this package wires them into plugin.Default. The server
// imports this; tests that need an isolated registry skip it and use
// pkg/plugin.NewRegistry().
//
// Adding a new built-in plugin: drop another blank import below.
package builtins

import (
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/bloom"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/cbloom"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/cms"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/cuckoo"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/hll"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/roaring"
)
