package contractbuild

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

const (
	rf3RootName     = "root.json"
	rf3WorkloadName = "workload.json"
	rf3TopologyName = "topology.json"
)

type rf3Corpus struct {
	root     []byte
	workload []byte
	topology []byte
}

type trackingReadCloser struct {
	reader     io.Reader
	closeErr   error
	closeCalls int
}

func (reader *trackingReadCloser) Read(data []byte) (int, error) {
	return reader.reader.Read(data)
}

func (reader *trackingReadCloser) Close() error {
	reader.closeCalls++
	return reader.closeErr
}

type errorReader struct {
	err error
}

func (reader errorReader) Read([]byte) (int, error) {
	return 0, reader.err
}

type cancelAfterFirstRead struct {
	reader io.Reader
	cancel context.CancelFunc
	read   bool
}

func (reader *cancelAfterFirstRead) Read(data []byte) (int, error) {
	if !reader.read {
		reader.read = true
		reader.cancel()
		if len(data) > 1 {
			data = data[:1]
		}
	}
	return reader.reader.Read(data)
}

func (corpus rf3Corpus) forPath(t *testing.T, path string) []byte {
	t.Helper()

	switch path {
	case rf3RootName:
		return corpus.root
	case rf3WorkloadName:
		return corpus.workload
	case rf3TopologyName:
		return corpus.topology
	default:
		t.Fatalf("unexpected path %q", path)
		return nil
	}
}

func (corpus rf3Corpus) documents() map[string][]byte {
	return map[string][]byte{
		rf3RootName:     corpus.root,
		rf3WorkloadName: corpus.workload,
		rf3TopologyName: corpus.topology,
	}
}

func readRF3Corpus(t testing.TB) rf3Corpus {
	t.Helper()

	directory := filepath.Join("..", "core", "rf3acceptance", "testdata", "valid", "v1")
	return rf3Corpus{
		root:     readRF3File(t, filepath.Join(directory, rf3RootName)),
		workload: readRF3File(t, filepath.Join(directory, rf3WorkloadName)),
		topology: readRF3File(t, filepath.Join(directory, rf3TopologyName)),
	}
}

func readRF3File(t testing.TB, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func writeRF3File(t *testing.T, directory, name string, data []byte) string {
	t.Helper()

	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func decodeRF3Corpus(t testing.TB, corpus rf3Corpus) (rf3acceptance.Bundle, error) {
	t.Helper()

	root := mustRF3Source(t, rf3RootName, corpus.root)
	workload := mustRF3Source(t, rf3WorkloadName, corpus.workload)
	topology := mustRF3Source(t, rf3TopologyName, corpus.topology)
	return rf3acceptance.DecodeBundle(root, workload, topology)
}

func mustRF3Source(t testing.TB, path string, data []byte) rf3acceptance.Source {
	t.Helper()

	source, err := rf3acceptance.NewSource(path, data)
	if err != nil {
		t.Fatalf("NewSource(%q) fixture error: %v", path, err)
	}
	return source
}

func decodeStrictRF3JSON(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := consumeRF3JSONValue(decoder); err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("trailing JSON token %v", token)
	}

	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func consumeRF3JSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}

	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key has type %T", keyToken)
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeRF3JSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := consumeRF3JSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected opening delimiter %q", delimiter)
	}

	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != matchingRF3Delimiter(delimiter) {
		return fmt.Errorf("closing delimiter %q does not match %q", closing, delimiter)
	}
	return nil
}

func matchingRF3Delimiter(open json.Delim) json.Delim {
	if open == '{' {
		return '}'
	}
	return ']'
}

func assertRF3PathError(t *testing.T, err error, wantPath, role string) {
	t.Helper()

	var pathError *os.PathError
	if !errors.As(err, &pathError) {
		t.Fatalf("error %T does not preserve *os.PathError: %v", err, err)
	}
	if pathError.Path != wantPath {
		t.Fatalf("path error path = %q, want %q", pathError.Path, wantPath)
	}
	if !strings.Contains(err.Error(), role) {
		t.Fatalf("error %q does not identify %s input", err, role)
	}
}

func assertEveryRF3InputClosed(t *testing.T, opened map[string]*trackingReadCloser) {
	t.Helper()

	if len(opened) == 0 {
		t.Fatal("loader opened no inputs")
	}
	for path, reader := range opened {
		if reader.closeCalls != 1 {
			t.Errorf("%s close calls = %d, want 1", path, reader.closeCalls)
		}
	}
}

func assertRF3RejectedBundle(t testing.TB, got rf3acceptance.Bundle, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("operation unexpectedly succeeded")
	}
	if !reflect.DeepEqual(got, rf3acceptance.Bundle{}) {
		t.Fatalf("operation returned a partial bundle: %#v", got)
	}
}

func assertRF3NonzeroBundle(t testing.TB, got rf3acceptance.Bundle) {
	t.Helper()

	if reflect.DeepEqual(got, rf3acceptance.Bundle{}) {
		t.Fatal("operation returned Bundle{}")
	}
}
