package contractbuild

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3environment"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const rf3EnvironmentManifestName = "environment.json"

func TestLoadRF3EnvironmentFiles(t *testing.T) {
	t.Parallel()

	corpus := readRF3Corpus(t)
	manifest := readRF3EnvironmentFixture(t, "valid", "v1", "manifest.json")
	directory := t.TempDir()
	rootPath := writeRF3File(t, directory, rf3RootName, corpus.root)
	workloadPath := writeRF3File(t, directory, rf3WorkloadName, corpus.workload)
	topologyPath := writeRF3File(t, directory, rf3TopologyName, corpus.topology)
	manifestPath := writeRF3File(t, directory, rf3EnvironmentManifestName, manifest)

	got, err := LoadRF3EnvironmentFiles(
		context.Background(),
		rootPath,
		workloadPath,
		topologyPath,
		manifestPath,
	)
	if err != nil {
		t.Fatalf("LoadRF3EnvironmentFiles() unexpected error: %v", err)
	}
	assertRF3NonzeroEnvironmentContract(t, got)

	t.Run("manifest path error is preserved", func(t *testing.T) {
		missingPath := filepath.Join(directory, "missing-environment.json")
		loaded, err := LoadRF3EnvironmentFiles(
			context.Background(),
			rootPath,
			workloadPath,
			topologyPath,
			missingPath,
		)
		assertRF3RejectedEnvironmentContract(t, loaded, err)
		assertRF3PathError(t, err, missingPath, "environment manifest")
	})

	t.Run("manifest outside acceptance root is rejected", func(t *testing.T) {
		otherDirectory := t.TempDir()
		outsidePath := writeRF3File(t, otherDirectory, rf3EnvironmentManifestName, manifest)
		loaded, err := LoadRF3EnvironmentFiles(
			context.Background(),
			rootPath,
			workloadPath,
			topologyPath,
			outsidePath,
		)
		assertRF3RejectedEnvironmentContract(t, loaded, err)
	})
}

func TestLoadRF3EnvironmentFilesHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := LoadRF3EnvironmentFiles(ctx, "root.json", "workload.json", "topology.json", "environment.json")
	assertRF3RejectedEnvironmentContract(t, got, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestLoadRF3EnvironmentManifestBoundsAndClosesInput(t *testing.T) {
	t.Parallel()

	acceptance, err := decodeRF3Corpus(t, readRF3Corpus(t))
	if err != nil {
		t.Fatalf("DecodeBundle() fixture error: %v", err)
	}
	tests := []struct {
		name      string
		data      []byte
		readerErr error
		closeErr  error
	}{
		{
			name: "success",
			data: readRF3EnvironmentFixture(t, "valid", "v1", "manifest.json"),
		},
		{
			name: "bounded read",
			data: bytes.Repeat([]byte{'x'}, maxRF3EnvironmentFileBytes+1),
		},
		{
			name:      "read failure",
			readerErr: errors.New("synthetic read failure"),
		},
		{
			name:     "close failure",
			data:     readRF3EnvironmentFixture(t, "valid", "v1", "manifest.json"),
			closeErr: errors.New("synthetic close failure"),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var reader io.Reader = bytes.NewReader(test.data)
			if test.readerErr != nil {
				reader = errorReader{err: test.readerErr}
			}
			tracked := &trackingReadCloser{reader: reader, closeErr: test.closeErr}
			got, err := loadRF3EnvironmentManifest(
				context.Background(),
				acceptance,
				rf3RootName,
				rf3EnvironmentManifestName,
				func(path string) (io.ReadCloser, error) {
					if path != rf3EnvironmentManifestName {
						t.Fatalf("open path = %q", path)
					}
					return tracked, nil
				},
			)
			if test.name == "success" {
				if err != nil {
					t.Fatalf("loadRF3EnvironmentManifest() unexpected error: %v", err)
				}
				assertRF3NonzeroEnvironmentContract(t, got)
			} else {
				assertRF3RejectedEnvironmentContract(t, got, err)
			}
			if tracked.closeCalls != 1 {
				t.Fatalf("Close() calls = %d, want 1", tracked.closeCalls)
			}
			if test.readerErr != nil && !errors.Is(err, test.readerErr) {
				t.Fatalf("error = %v, want read failure", err)
			}
			if test.closeErr != nil && !errors.Is(err, test.closeErr) {
				t.Fatalf("error = %v, want close failure", err)
			}
		})
	}
}

func TestRF3EnvironmentSchemaAndSemanticValidationAgree(t *testing.T) {
	t.Parallel()

	schemaPath := filepath.Join("..", "..", "contracts", "rf3-environment", "schema-v1.json")
	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	schemaValue, err := decodeStrictRF3JSON(schemaData)
	if err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	const schemaURL = "https://cefasdb.com/contracts/rf3-environment/schema-v1.json"
	if err := compiler.AddResource(schemaURL, schemaValue); err != nil {
		t.Fatalf("register schema: %v", err)
	}
	compiled, err := compiler.Compile(schemaURL)
	if err != nil {
		t.Fatalf("compile schema: %v", err)
	}
	assertRF3EnvironmentObjectsClosed(t, schemaValue, "$")

	acceptance, err := decodeRF3Corpus(t, readRF3Corpus(t))
	if err != nil {
		t.Fatalf("DecodeBundle() fixture error: %v", err)
	}
	manifestData := readRF3EnvironmentFixture(t, "valid", "v1", "manifest.json")
	manifestSource := mustRF3Source(t, rf3EnvironmentManifestName, manifestData)
	manifest, err := rf3environment.DecodeManifestV1(manifestSource, acceptance)
	if err != nil {
		t.Fatalf("DecodeManifestV1() fixture error: %v", err)
	}
	contract := manifest.Contract()

	validDirectory := filepath.Join("..", "core", "rf3environment", "testdata", "valid", "v1")
	validEntries, err := os.ReadDir(validDirectory)
	if err != nil {
		t.Fatalf("read valid corpus: %v", err)
	}
	for _, entry := range validEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data := readRF3File(t, filepath.Join(validDirectory, entry.Name()))
		value, err := decodeStrictRF3JSON(data)
		if err != nil {
			t.Fatalf("strictly decode valid %s: %v", entry.Name(), err)
		}
		if err := compiled.Validate(value); err != nil {
			t.Fatalf("schema rejected valid %s: %v", entry.Name(), err)
		}
		if strings.HasPrefix(entry.Name(), "receipt-") {
			source := mustRF3Source(t, filepath.ToSlash(filepath.Join("receipts", entry.Name())), data)
			receipt, err := rf3environment.DecodeReceiptV1(source, contract)
			if err != nil || reflect.DeepEqual(receipt, rf3environment.ObservedEnvironmentReceiptV1{}) {
				t.Fatalf("semantic decoder rejected valid %s: %#v, %v", entry.Name(), receipt, err)
			}
		}
	}

	invalidDirectory := filepath.Join("..", "core", "rf3environment", "testdata", "invalid", "v1")
	invalidEntries, err := os.ReadDir(invalidDirectory)
	if err != nil {
		t.Fatalf("read invalid corpus: %v", err)
	}
	for _, entry := range invalidEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		entry := entry
		t.Run(entry.Name(), func(t *testing.T) {
			data := readRF3File(t, filepath.Join(invalidDirectory, entry.Name()))
			if value, decodeErr := decodeStrictRF3JSON(data); decodeErr == nil {
				if err := compiled.Validate(value); err == nil {
					t.Fatal("schema tooling unexpectedly accepted invalid fixture")
				}
			}
			if strings.HasPrefix(entry.Name(), "manifest-") {
				source := mustRF3Source(t, rf3EnvironmentManifestName, data)
				got, err := rf3environment.DecodeManifestV1(source, acceptance)
				if err == nil || !reflect.DeepEqual(got, rf3environment.EnvironmentManifestV1{}) {
					t.Fatalf("DecodeManifestV1() = %#v, %v", got, err)
				}
				return
			}
			source := mustRF3Source(t, filepath.ToSlash(filepath.Join("receipts", entry.Name())), data)
			got, err := rf3environment.DecodeReceiptV1(source, contract)
			if err == nil || !reflect.DeepEqual(got, rf3environment.ObservedEnvironmentReceiptV1{}) {
				t.Fatalf("DecodeReceiptV1() = %#v, %v", got, err)
			}
		})
	}
}

func readRF3EnvironmentFixture(t testing.TB, parts ...string) []byte {
	t.Helper()
	path := filepath.Join(append(
		[]string{"..", "core", "rf3environment", "testdata"},
		parts...,
	)...)
	return readRF3File(t, path)
}

func assertRF3RejectedEnvironmentContract(
	t testing.TB,
	got rf3environment.EnvironmentContractV1,
	err error,
) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(got, rf3environment.EnvironmentContractV1{}) {
		t.Fatalf("environment contract = %#v, %v; want zero plus error", got, err)
	}
}

func assertRF3NonzeroEnvironmentContract(
	t testing.TB,
	got rf3environment.EnvironmentContractV1,
) {
	t.Helper()
	if reflect.DeepEqual(got, rf3environment.EnvironmentContractV1{}) {
		t.Fatal("environment contract is zero")
	}
}

func assertRF3EnvironmentObjectsClosed(t *testing.T, value any, location string) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		if typed["type"] == "object" && typed["additionalProperties"] != false {
			t.Fatalf("%s is an open object schema", location)
		}
		for key, child := range typed {
			assertRF3EnvironmentObjectsClosed(t, child, location+"."+key)
		}
	case []any:
		for index, child := range typed {
			assertRF3EnvironmentObjectsClosed(t, child, location+"[]")
			_ = index
		}
	}
}
