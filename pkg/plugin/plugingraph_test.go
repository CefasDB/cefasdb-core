// Mirror of pkg/core/coregraph_test.go for pkg/plugin: nothing under
// pkg/plugin may import an engine internal (internal/*, pkg/api,
// pkg/sql, pkg/client). Plugins must stay portable across cefas
// versions and embeddings.
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
	forbidden := []string{
		"github.com/osvaldoandrade/cefas/internal/",
		"github.com/osvaldoandrade/cefas/internal/api",
		"github.com/osvaldoandrade/cefas/pkg/sql",
		"github.com/osvaldoandrade/cefas/pkg/client",
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
