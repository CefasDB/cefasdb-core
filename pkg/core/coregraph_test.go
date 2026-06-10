// Test the load-bearing invariant of the plugin architecture: nothing
// under pkg/core may import an internal/* package or any of cefas's
// engine packages (pkg/api, pkg/sql, pkg/client). If this test fires,
// a future change has introduced a coupling that would force plugins
// to depend on engine internals.
package core_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestCoreHasNoEngineImports(t *testing.T) {
	root := filepath.Join("..", "core") // walk pkg/core/...
	// Resolve to an absolute path so relative go/parser positions
	// don't depend on the test's working directory layout.
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatalf("abs: %v", err)
	}

	forbidden := []string{
		"github.com/osvaldoandrade/cefas/internal/",
		"github.com/osvaldoandrade/cefas/pkg/api",
		"github.com/osvaldoandrade/cefas/pkg/sql",
		"github.com/osvaldoandrade/cefas/pkg/client",
	}

	fset := token.NewFileSet()
	visited := 0
	err = filepath.WalkDir(abs, func(path string, d fs.DirEntry, walkErr error) error {
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
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if visited == 0 {
		t.Fatal("walked no .go files under pkg/core — fixture path wrong?")
	}
}
