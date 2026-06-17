package fileloader_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
)

func TestLoadInline(t *testing.T) {
	b, err := fileloader.Load(`{"k":"v"}`)
	if err != nil || string(b) != `{"k":"v"}` {
		t.Fatalf("inline: %q %v", b, err)
	}
}

func TestLoadFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.json")
	os.WriteFile(path, []byte(`{"x":1}`), 0o644)
	b, err := fileloader.Load("file://" + path)
	if err != nil || string(b) != `{"x":1}` {
		t.Fatalf("file://: %q %v", b, err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := fileloader.Load("file:///does/not/exist"); err == nil {
		t.Fatalf("expected error on missing file")
	}
}
