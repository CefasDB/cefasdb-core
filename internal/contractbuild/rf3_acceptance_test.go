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

	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestLoadPublishedRF3AcceptanceBundle(t *testing.T) {
	t.Parallel()

	directory := filepath.Join("..", "..", "contracts", "rf3-acceptance")
	rootPath := filepath.Join(directory, "v1.json")
	workloadPath := filepath.Join(directory, "workload-v1.json")
	topologyPath := filepath.Join(directory, "topology-v1.json")

	got, err := LoadRF3AcceptanceFiles(context.Background(), rootPath, workloadPath, topologyPath)
	if err != nil {
		t.Fatalf("LoadRF3AcceptanceFiles() unexpected error: %v", err)
	}
	assertRF3NonzeroBundle(t, got)

	if value := got.Acceptance().ID().String(); value != "rf3.acceptance.rf3-p1.v1" {
		t.Fatalf("acceptance ID = %q, want rf3.acceptance.rf3-p1.v1", value)
	}
	if value := got.Workload().ID().String(); value != "rf3.workload.synthetic-mixed-one-kops.v1" {
		t.Fatalf("workload ID = %q, want rf3.workload.synthetic-mixed-one-kops.v1", value)
	}
	if value := got.Topology().ID().String(); value != "rf3.topology.three-node-standard.v1" {
		t.Fatalf("topology ID = %q, want rf3.topology.three-node-standard.v1", value)
	}

	digests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "acceptance",
			got:  got.AcceptanceDigest().String(),
			want: "sha256:10004b1885b3cbbe5d75cd9431d48449d5d6305d72c925448aba13ed9ab09853",
		},
		{
			name: "workload",
			got:  got.WorkloadDigest().String(),
			want: "sha256:d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66",
		},
		{
			name: "topology",
			got:  got.TopologyDigest().String(),
			want: "sha256:33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78",
		},
	}
	for _, digest := range digests {
		if digest.got != digest.want {
			t.Errorf("%s digest = %q, want %q", digest.name, digest.got, digest.want)
		}
	}

	wantWorkloadNames := []string{"write", "point-read", "scan", "mixed"}
	if names := got.Workload().ProfileNames(); !reflect.DeepEqual(names, wantWorkloadNames) {
		t.Fatalf("workload names = %v, want %v", names, wantWorkloadNames)
	}

	scenarios := got.Acceptance().Scenarios()
	scenarioNames := make([]string, len(scenarios))
	for index, scenario := range scenarios {
		scenarioNames[index] = scenario.Name()
	}
	wantScenarioNames := []string{
		"healthy-baseline",
		"pod-kill",
		"node-drain",
		"node-shutdown",
		"orphan-process",
		"disk-pressure",
		"network-partition",
		"two-node-failure",
	}
	if !reflect.DeepEqual(scenarioNames, wantScenarioNames) {
		t.Fatalf("scenario names = %v, want %v", scenarioNames, wantScenarioNames)
	}

	wantFailureDomains := []string{"rf3q-zone-a", "rf3q-zone-b", "rf3q-zone-c"}
	if domains := got.Topology().FailureDomains(); !reflect.DeepEqual(domains, wantFailureDomains) {
		t.Fatalf("failure domains = %v, want %v", domains, wantFailureDomains)
	}
	if storageClass := got.Topology().Volume().Class; storageClass != "cefasdb-rf3-durable-rwo-v1" {
		t.Fatalf("storage class = %q, want cefasdb-rf3-durable-rwo-v1", storageClass)
	}
}

func TestLoadRF3AcceptanceFiles(t *testing.T) {
	t.Parallel()

	corpus := readRF3Corpus(t)
	directory := t.TempDir()
	rootPath := writeRF3File(t, directory, rf3RootName, corpus.root)
	workloadPath := writeRF3File(t, directory, rf3WorkloadName, corpus.workload)
	topologyPath := writeRF3File(t, directory, rf3TopologyName, corpus.topology)

	got, err := LoadRF3AcceptanceFiles(
		context.Background(),
		rootPath,
		workloadPath,
		topologyPath,
	)
	if err != nil {
		t.Fatalf("LoadRF3AcceptanceFiles() unexpected error: %v", err)
	}
	assertRF3NonzeroBundle(t, got)

	tests := []struct {
		name         string
		rootPath     string
		workloadPath string
		topologyPath string
		missingPath  string
		role         string
	}{
		{
			name:         "root path error is preserved",
			rootPath:     filepath.Join(directory, "missing-root.json"),
			workloadPath: workloadPath,
			topologyPath: topologyPath,
			missingPath:  filepath.Join(directory, "missing-root.json"),
			role:         "root",
		},
		{
			name:         "workload path error is preserved",
			rootPath:     rootPath,
			workloadPath: filepath.Join(directory, "missing-workload.json"),
			topologyPath: topologyPath,
			missingPath:  filepath.Join(directory, "missing-workload.json"),
			role:         "workload",
		},
		{
			name:         "topology path error is preserved",
			rootPath:     rootPath,
			workloadPath: workloadPath,
			topologyPath: filepath.Join(directory, "missing-topology.json"),
			missingPath:  filepath.Join(directory, "missing-topology.json"),
			role:         "topology",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			loaded, err := LoadRF3AcceptanceFiles(
				context.Background(),
				test.rootPath,
				test.workloadPath,
				test.topologyPath,
			)
			assertRF3RejectedBundle(t, loaded, err)
			assertRF3PathError(t, err, test.missingPath, test.role)
		})
	}

	t.Run("profile path substitution is rejected", func(t *testing.T) {
		substitute := writeRF3File(t, directory, "substitute-workload.json", corpus.workload)
		loaded, err := LoadRF3AcceptanceFiles(
			context.Background(),
			rootPath,
			substitute,
			topologyPath,
		)
		assertRF3RejectedBundle(t, loaded, err)
	})

	t.Run("profile role substitution is rejected", func(t *testing.T) {
		loaded, err := LoadRF3AcceptanceFiles(
			context.Background(),
			rootPath,
			topologyPath,
			workloadPath,
		)
		assertRF3RejectedBundle(t, loaded, err)
	})
}

func TestLoadRF3AcceptanceFilesHonorsCancellation(t *testing.T) {
	t.Parallel()

	corpus := readRF3Corpus(t)
	directory := t.TempDir()
	rootPath := writeRF3File(t, directory, rf3RootName, corpus.root)
	workloadPath := writeRF3File(t, directory, rf3WorkloadName, corpus.workload)
	topologyPath := writeRF3File(t, directory, rf3TopologyName, corpus.topology)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := LoadRF3AcceptanceFiles(ctx, rootPath, workloadPath, topologyPath)
	assertRF3RejectedBundle(t, got, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestLoadRF3AcceptanceFilesBoundsEveryRead(t *testing.T) {
	t.Parallel()

	corpus := readRF3Corpus(t)
	opened := make(map[string]*trackingReadCloser)
	openFile := func(path string) (io.ReadCloser, error) {
		data := corpus.forPath(t, path)
		if path == rf3RootName {
			data = bytes.Repeat([]byte{'x'}, maxRF3AcceptanceFileBytes+1)
		}
		reader := &trackingReadCloser{reader: bytes.NewReader(data)}
		opened[path] = reader
		return reader, nil
	}

	got, err := loadRF3AcceptanceFiles(
		context.Background(),
		rf3AcceptancePaths{
			root:     rf3RootName,
			workload: rf3WorkloadName,
			topology: rf3TopologyName,
		},
		openFile,
	)
	assertRF3RejectedBundle(t, got, err)
	if !strings.Contains(err.Error(), rf3RootName) {
		t.Fatalf("bounded read error %q does not preserve root path", err)
	}
	assertEveryRF3InputClosed(t, opened)
}

func TestLoadRF3AcceptanceFilesClosesEveryInput(t *testing.T) {
	corpus := readRF3Corpus(t)
	opened := make(map[string]*trackingReadCloser)
	openFile := func(path string) (io.ReadCloser, error) {
		reader := &trackingReadCloser{reader: bytes.NewReader(corpus.forPath(t, path))}
		opened[path] = reader
		return reader, nil
	}

	got, err := loadRF3AcceptanceFiles(
		context.Background(),
		rf3AcceptancePaths{
			root:     rf3RootName,
			workload: rf3WorkloadName,
			topology: rf3TopologyName,
		},
		openFile,
	)
	if err != nil {
		t.Fatalf("loadRF3AcceptanceFiles() unexpected error: %v", err)
	}
	assertRF3NonzeroBundle(t, got)
	assertEveryRF3InputClosed(t, opened)
}

func TestLoadRF3AcceptanceFilesClosesInputsOnFailure(t *testing.T) {
	t.Run("open failure", func(t *testing.T) {
		corpus := readRF3Corpus(t)
		openFailure := errors.New("synthetic open failure")
		opened := make(map[string]*trackingReadCloser)
		openFile := func(path string) (io.ReadCloser, error) {
			if path == rf3WorkloadName {
				return nil, openFailure
			}
			tracked := &trackingReadCloser{reader: bytes.NewReader(corpus.forPath(t, path))}
			opened[path] = tracked
			return tracked, nil
		}

		got, err := loadRF3AcceptanceFiles(
			context.Background(),
			rf3AcceptancePaths{
				root:     rf3RootName,
				workload: rf3WorkloadName,
				topology: rf3TopologyName,
			},
			openFile,
		)
		assertRF3RejectedBundle(t, got, err)
		if !errors.Is(err, openFailure) {
			t.Fatalf("error = %v, want synthetic open failure", err)
		}
		assertEveryRF3InputClosed(t, opened)
	})

	t.Run("read failure", func(t *testing.T) {
		corpus := readRF3Corpus(t)
		readFailure := errors.New("synthetic read failure")
		opened := make(map[string]*trackingReadCloser)
		openFile := func(path string) (io.ReadCloser, error) {
			var reader io.Reader = bytes.NewReader(corpus.forPath(t, path))
			if path == rf3WorkloadName {
				reader = errorReader{err: readFailure}
			}
			tracked := &trackingReadCloser{reader: reader}
			opened[path] = tracked
			return tracked, nil
		}

		got, err := loadRF3AcceptanceFiles(
			context.Background(),
			rf3AcceptancePaths{
				root:     rf3RootName,
				workload: rf3WorkloadName,
				topology: rf3TopologyName,
			},
			openFile,
		)
		assertRF3RejectedBundle(t, got, err)
		if !errors.Is(err, readFailure) {
			t.Fatalf("error = %v, want synthetic read failure", err)
		}
		assertEveryRF3InputClosed(t, opened)
	})

	t.Run("close failure", func(t *testing.T) {
		corpus := readRF3Corpus(t)
		closeFailure := errors.New("synthetic close failure")
		opened := make(map[string]*trackingReadCloser)
		openFile := func(path string) (io.ReadCloser, error) {
			tracked := &trackingReadCloser{reader: bytes.NewReader(corpus.forPath(t, path))}
			if path == rf3WorkloadName {
				tracked.closeErr = closeFailure
			}
			opened[path] = tracked
			return tracked, nil
		}

		got, err := loadRF3AcceptanceFiles(
			context.Background(),
			rf3AcceptancePaths{
				root:     rf3RootName,
				workload: rf3WorkloadName,
				topology: rf3TopologyName,
			},
			openFile,
		)
		assertRF3RejectedBundle(t, got, err)
		if !errors.Is(err, closeFailure) {
			t.Fatalf("error = %v, want synthetic close failure", err)
		}
		if !strings.Contains(err.Error(), rf3WorkloadName) {
			t.Fatalf("close error %q does not preserve workload path", err)
		}
		assertEveryRF3InputClosed(t, opened)
	})
}

func TestLoadRF3AcceptanceFilesCancelsDuringRead(t *testing.T) {
	corpus := readRF3Corpus(t)
	ctx, cancel := context.WithCancel(context.Background())
	opened := make(map[string]*trackingReadCloser)
	openFile := func(path string) (io.ReadCloser, error) {
		var reader io.Reader = bytes.NewReader(corpus.forPath(t, path))
		if path == rf3RootName {
			reader = &cancelAfterFirstRead{
				reader: bytes.NewReader(corpus.root),
				cancel: cancel,
			}
		}
		tracked := &trackingReadCloser{reader: reader}
		opened[path] = tracked
		return tracked, nil
	}

	got, err := loadRF3AcceptanceFiles(
		ctx,
		rf3AcceptancePaths{
			root:     rf3RootName,
			workload: rf3WorkloadName,
			topology: rf3TopologyName,
		},
		openFile,
	)
	assertRF3RejectedBundle(t, got, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	assertEveryRF3InputClosed(t, opened)
}

func TestRF3AcceptanceSchemaAndSemanticValidationAgree(t *testing.T) {
	t.Parallel()

	schemaPath := filepath.Join("..", "..", "contracts", "rf3-acceptance", "schema-v1.json")
	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	schemaValue, err := decodeStrictRF3JSON(schemaData)
	if err != nil {
		t.Fatalf("decode schema: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	const schemaURL = "https://cefasdb.com/contracts/rf3-acceptance/schema-v1.json"
	if err := compiler.AddResource(schemaURL, schemaValue); err != nil {
		t.Fatalf("register schema resource: %v", err)
	}
	compiled, err := compiler.Compile(schemaURL)
	if err != nil {
		t.Fatalf("compile draft 2020-12 schema: %v", err)
	}

	publishedDirectory := filepath.Join("..", "..", "contracts", "rf3-acceptance")
	publishedRootPath := filepath.Join(publishedDirectory, "v1.json")
	publishedWorkloadPath := filepath.Join(publishedDirectory, "workload-v1.json")
	publishedTopologyPath := filepath.Join(publishedDirectory, "topology-v1.json")
	publishedDocuments := []struct {
		name string
		path string
	}{
		{name: "v1.json", path: publishedRootPath},
		{name: "workload-v1.json", path: publishedWorkloadPath},
		{name: "topology-v1.json", path: publishedTopologyPath},
	}
	for _, document := range publishedDocuments {
		data, err := os.ReadFile(document.path)
		if err != nil {
			t.Fatalf("read published %s: %v", document.name, err)
		}
		value, err := decodeStrictRF3JSON(data)
		if err != nil {
			t.Fatalf("strictly decode published %s: %v", document.name, err)
		}
		if err := compiled.Validate(value); err != nil {
			t.Fatalf("schema rejected published %s: %v", document.name, err)
		}
	}
	publishedBundle, err := LoadRF3AcceptanceFiles(
		context.Background(), publishedRootPath, publishedWorkloadPath, publishedTopologyPath,
	)
	if err != nil {
		t.Fatalf("semantic loader rejected published bundle: %v", err)
	}
	assertRF3NonzeroBundle(t, publishedBundle)

	valid := readRF3Corpus(t)
	for name, data := range valid.documents() {
		value, err := decodeStrictRF3JSON(data)
		if err != nil {
			t.Fatalf("strictly decode valid %s: %v", name, err)
		}
		if err := compiled.Validate(value); err != nil {
			t.Fatalf("schema rejected valid %s: %v", name, err)
		}
	}
	got, err := decodeRF3Corpus(t, valid)
	if err != nil {
		t.Fatalf("semantic decoder rejected valid corpus: %v", err)
	}
	assertRF3NonzeroBundle(t, got)

	invalidDirectory := filepath.Join("..", "core", "rf3acceptance", "testdata", "invalid", "v1")
	entries, err := os.ReadDir(invalidDirectory)
	if err != nil {
		t.Fatalf("read invalid corpus: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		entry := entry
		t.Run(entry.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(invalidDirectory, entry.Name()))
			if err != nil {
				t.Fatalf("read invalid fixture: %v", err)
			}
			if value, decodeErr := decodeStrictRF3JSON(data); decodeErr == nil {
				if err := compiled.Validate(value); err == nil {
					t.Fatal("schema tooling unexpectedly accepted invalid fixture")
				}
			}

			input := valid
			switch {
			case strings.HasPrefix(entry.Name(), "root-"):
				input.root = data
			case strings.HasPrefix(entry.Name(), "workload-"):
				input.workload = data
			case strings.HasPrefix(entry.Name(), "topology-"):
				input.topology = data
			default:
				t.Fatalf("fixture %q does not identify a document role", entry.Name())
			}
			got, err := decodeRF3Corpus(t, input)
			assertRF3RejectedBundle(t, got, err)
		})
	}
}
