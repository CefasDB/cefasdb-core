package rf3environment_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3environment"
)

func FuzzDecodeEnvironment(f *testing.F) {
	acceptance := loadAcceptance(f)
	contract := loadEnvironmentContract(f)
	f.Add(uint8(0), readEnvironmentFixture(f, "valid", "v1", "manifest.json"))
	f.Add(uint8(1), readEnvironmentFixture(f, "valid", "v1", "receipt-create.json"))
	f.Add(uint8(1), readEnvironmentFixture(f, "valid", "v1", "receipt-recreate.json"))
	f.Add(uint8(0), []byte("{"))
	f.Add(uint8(1), []byte("[]"))

	invalidDirectory := filepath.Join("testdata", "invalid", "v1")
	entries, err := os.ReadDir(invalidDirectory)
	if err != nil {
		f.Fatalf("read invalid corpus: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data := readFile(f, filepath.Join(invalidDirectory, entry.Name()))
		kind := uint8(0)
		if strings.HasPrefix(entry.Name(), "receipt-") {
			kind = 1
		}
		f.Add(kind, data)
	}

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		if kind%2 == 0 {
			got, err := decodeManifest(t, data, acceptance)
			if err != nil {
				if !reflect.DeepEqual(got, rf3environment.EnvironmentManifestV1{}) {
					t.Fatalf("DecodeManifestV1() returned partial value: %#v", got)
				}
				return
			}
			if reflect.DeepEqual(got, rf3environment.EnvironmentManifestV1{}) {
				t.Fatal("DecodeManifestV1() succeeded with zero value")
			}
			return
		}

		got, err := decodeReceipt(t, data, "receipts/fuzz-v1.json", contract)
		if err != nil {
			if !reflect.DeepEqual(got, rf3environment.ObservedEnvironmentReceiptV1{}) {
				t.Fatalf("DecodeReceiptV1() returned partial value: %#v", got)
			}
			return
		}
		if reflect.DeepEqual(got, rf3environment.ObservedEnvironmentReceiptV1{}) {
			t.Fatal("DecodeReceiptV1() succeeded with zero value")
		}
	})
}
