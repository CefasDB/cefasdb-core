// Architectural guard for the plugin SDK. After the PR 7 package
// restructure, pkg/plugin is the contract third-party plugin authors
// write against — it legitimately references types under
// internal/core/* (Descriptor, Lifecycle, DistanceOp, KeySchema
// aliases) because those types ARE the plugin contract. What still
// must never leak in is the engine surface: internal/server,
// internal/sql, internal/storage, internal/cluster — anything that
// would tie plugin code to a specific cefasdb embedding.
package plugin_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestPluginHasNoEngineImports(t *testing.T) {
	abs, err := filepath.Abs(filepath.Join("..", "plugin"))
	if err != nil {
		t.Fatalf("abs: %v", err)
	}

	// Engine packages a plugin must never reach into. internal/core
	// is intentionally absent — it is the shared kernel the plugin
	// contract sits on. pkg/client is forbidden so plugins do not
	// embed the Go SDK call paths.
	forbidden := []string{
		"github.com/CefasDb/cefasdb/internal/server",
		"github.com/CefasDb/cefasdb/internal/sql",
		"github.com/CefasDb/cefasdb/internal/storage",
		"github.com/CefasDb/cefasdb/internal/cluster",
		"github.com/CefasDb/cefasdb/internal/placement",
		"github.com/CefasDb/cefasdb/internal/routing",
		"github.com/CefasDb/cefasdb/internal/replication",
		"github.com/CefasDb/cefasdb/internal/catalog",
		"github.com/CefasDb/cefasdb/internal/rebalance",
		"github.com/CefasDb/cefasdb/internal/bootstrap",
		"github.com/CefasDb/cefasdb/internal/metrics",
		"github.com/CefasDb/cefasdb/internal/config",
		"github.com/CefasDb/cefasdb/internal/compat",
		"github.com/CefasDb/cefasdb/pkg/client",
	}

	fset := token.NewFileSet()
	visited := 0
	walkErr := filepath.WalkDir(abs, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		visited++
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			val, _ := strconv.Unquote(imp.Path.Value)
			for _, bad := range forbidden {
				if strings.HasPrefix(val, bad) {
					t.Errorf("%s imports forbidden package %s", path, val)
				}
			}
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk: %v", walkErr)
	}
	if visited == 0 {
		t.Fatal("walked no .go files under pkg/plugin")
	}
}
