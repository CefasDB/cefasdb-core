package rf3acceptance_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

func FuzzDecodeBundle(f *testing.F) {
	valid := readValidBundle(f)
	f.Add(valid.root, valid.workload, valid.topology)
	f.Add([]byte{}, valid.workload, valid.topology)
	f.Add([]byte("{"), []byte("[]"), []byte("null"))

	invalidDirectory := filepath.Join("testdata", "invalid", "v1")
	entries, err := os.ReadDir(invalidDirectory)
	if err != nil {
		f.Fatalf("read invalid corpus: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		input := valid
		data := readFixture(f, filepath.Join(invalidDirectory, entry.Name()))
		switch {
		case strings.HasPrefix(entry.Name(), "root-"):
			input.root = data
		case strings.HasPrefix(entry.Name(), "workload-"):
			input.workload = data
		case strings.HasPrefix(entry.Name(), "topology-"):
			input.topology = data
		default:
			f.Fatalf("invalid corpus file %q does not identify a document role", entry.Name())
		}
		f.Add(input.root, input.workload, input.topology)
	}

	f.Fuzz(func(t *testing.T, rootData, workloadData, topologyData []byte) {
		got, err := decodeBundle(t, bundleBytes{
			root:     rootData,
			workload: workloadData,
			topology: topologyData,
		})
		if err != nil {
			if !reflect.DeepEqual(got, rf3acceptance.Bundle{}) {
				t.Fatalf("DecodeBundle() returned a partial bundle: %#v", got)
			}
			return
		}
		if reflect.DeepEqual(got, rf3acceptance.Bundle{}) {
			t.Fatal("DecodeBundle() succeeded with Bundle{}")
		}
	})
}
