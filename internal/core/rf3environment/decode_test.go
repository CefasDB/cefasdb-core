package rf3environment_test

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
	"github.com/CefasDb/cefasdb/internal/core/rf3environment"
)

const (
	manifestPath        = "environment.json"
	createReceiptPath   = "receipts/create-v1.json"
	recreateReceiptPath = "receipts/recreate-v1.json"
)

func TestDecodeManifestV1AcceptsExactFixture(t *testing.T) {
	t.Parallel()

	acceptance := loadAcceptance(t)
	manifestData := readEnvironmentFixture(t, "valid", "v1", "manifest.json")
	manifest, err := decodeManifest(t, manifestData, acceptance)
	if err != nil {
		t.Fatalf("DecodeManifestV1() unexpected error: %v", err)
	}
	if reflect.DeepEqual(manifest, rf3environment.EnvironmentManifestV1{}) {
		t.Fatal("DecodeManifestV1() returned a zero manifest")
	}
	if manifest.Identity().ID != "rf3.environment.synthetic.v1" ||
		manifest.P1Binding().AcceptanceContractID != acceptance.Acceptance().ID().String() ||
		!manifest.P1Binding().AcceptanceDigest.Equal(acceptance.AcceptanceDigest()) {
		t.Fatalf("manifest identity or RF3-P1 binding is wrong: %#v", manifest.Identity())
	}
	specification := manifest.Specification()
	if specification.Topology.NodeCount != 3 ||
		specification.Topology.Replicas != 3 ||
		specification.Storage.VolumeReusePolicy != "forbid-prior-cycle" {
		t.Fatalf("manifest specification = %#v", specification)
	}
}

func TestDecodeManifestV1RejectsEveryInvalidPartition(t *testing.T) {
	t.Parallel()

	acceptance := loadAcceptance(t)
	valid := readEnvironmentFixture(t, "valid", "v1", "manifest.json")
	tests := []struct {
		name   string
		mutate func(*testing.T, []byte) []byte
	}{
		{"unknown field", mutateDocument(func(document map[string]any) {
			document["unexpected"] = true
		})},
		{"missing field", mutateDocument(func(document map[string]any) {
			delete(document, "storage")
		})},
		{"unsupported schema", mutateDocument(func(document map[string]any) {
			document["schemaVersion"] = "1.1"
		})},
		{"production context", mutateDocument(func(document map[string]any) {
			object(document, "provider")["kubernetesContext"] = "production-cluster-v1"
		})},
		{"mutable chart version", mutateDocument(func(document map[string]any) {
			object(document, "deployment")["chartVersion"] = "latest"
		})},
		{"mutable chart version with extension", mutateDocument(func(document map[string]any) {
			object(document, "deployment")["chartVersion"] = "1.2.latest"
		})},
		{"mismatched p1 digest", mutateDocument(func(document map[string]any) {
			object(document, "p1")["topologyDigest"] = digest("9")
		})},
		{"duplicate failure domain", mutateDocument(func(document map[string]any) {
			topology := object(document, "topology")
			domains := array(topology, "failureDomains")
			domains[2] = domains[1]
		})},
		{"missing node", mutateDocument(func(document map[string]any) {
			topology := object(document, "topology")
			topology["nodes"] = array(topology, "nodes")[:2]
		})},
		{"storage mismatch", mutateDocument(func(document map[string]any) {
			object(document, "storage")["accessMode"] = "read-write-many"
		})},
		{"broad rbac verb", mutateDocument(func(document map[string]any) {
			object(document, "admission")["rbacVerbs"] = []any{"get", "list", "create", "update", "patch"}
		})},
		{"secret shaped value", mutateDocument(func(document map[string]any) {
			object(document, "admission")["credentialAudience"] = "Bearer synthetic-value"
		})},
		{"duplicate key", func(_ *testing.T, input []byte) []byte {
			return bytes.Replace(
				input,
				[]byte(`  "schemaVersion": "1.0",`),
				[]byte("  \"schemaVersion\": \"1.0\",\n  \"schemaVersion\": \"1.0\","),
				1,
			)
		}},
		{"trailing document", func(_ *testing.T, input []byte) []byte {
			return append(append([]byte(nil), input...), []byte("{}\n")...)
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeManifest(t, test.mutate(t, valid), acceptance)
			assertRejectedManifest(t, got, err)
		})
	}
}

func TestDecodeReceiptV1AcceptsCreateAndRecreate(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	create := decodeReceiptFixture(t, contract, "receipt-create.json", createReceiptPath)
	recreate := decodeReceiptFixture(t, contract, "receipt-recreate.json", recreateReceiptPath)
	if create.Identity().Cycle != "create" || recreate.Identity().Cycle != "recreate" {
		t.Fatalf("receipt cycles = %q, %q", create.Identity().Cycle, recreate.Identity().Cycle)
	}
	if len(create.Observation().Nodes) != 3 || len(create.Observation().Volumes) != 3 {
		t.Fatalf("create observation = %#v", create.Observation())
	}

	comparison, err := rf3environment.CompareReceiptsV1(create, recreate)
	if err != nil {
		t.Fatalf("CompareReceiptsV1() unexpected error: %v", err)
	}
	if !comparison.Passed() || len(comparison.ReusedPVCUIDs()) != 0 ||
		len(comparison.ReusedPVUIDs()) != 0 {
		t.Fatalf("comparison = %#v", comparison)
	}
}

func TestDecodeReceiptV1RejectsEveryInvalidPartition(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	valid := readEnvironmentFixture(t, "valid", "v1", "receipt-create.json")
	tests := []struct {
		name   string
		mutate func(*testing.T, []byte) []byte
	}{
		{"unknown field", mutateDocument(func(document map[string]any) {
			document["unexpected"] = true
		})},
		{"unsupported schema", mutateDocument(func(document map[string]any) {
			document["schemaVersion"] = "2.0"
		})},
		{"mismatched manifest digest", mutateDocument(func(document map[string]any) {
			document["manifestDigest"] = digest("8")
		})},
		{"mutable provider version", mutateDocument(func(document map[string]any) {
			object(document, "provider")["providerVersion"] = "latest"
		})},
		{"replica colocation", mutateDocument(func(document map[string]any) {
			replicas := array(document, "replicas")
			second := replicas[1].(map[string]any)
			second["nodeId"] = "synthetic-node-a"
			second["failureDomain"] = "synthetic-fd-a"
		})},
		{"volume policy mismatch", mutateDocument(func(document map[string]any) {
			first := array(document, "volumes")[0].(map[string]any)
			first["reclaimPolicy"] = "retain"
		})},
		{"expired credential", mutateDocument(func(document map[string]any) {
			object(document, "admission")["credentialExpiresAt"] = "2026-01-04T00:04:00Z"
		})},
		{"doctor incomplete", mutateDocument(func(document map[string]any) {
			object(document, "doctor")["complete"] = false
		})},
		{"baseline failed", mutateDocument(func(document map[string]any) {
			object(document, "baseline")["passed"] = false
		})},
		{"artifact reference mismatch", mutateDocument(func(document map[string]any) {
			object(document, "doctor", "artifact")["digest"] = digest("a")
		})},
		{"cleanup incomplete", mutateDocument(func(document map[string]any) {
			object(document, "cleanup")["residualVolumes"] = float64(1)
		})},
		{"secret shaped value", mutateDocument(func(document map[string]any) {
			object(document, "admission")["holderIdentity"] = "Bearer synthetic-value"
		})},
		{"duplicate key", func(_ *testing.T, input []byte) []byte {
			return bytes.Replace(
				input,
				[]byte(`  "schemaVersion": "1.0",`),
				[]byte("  \"schemaVersion\": \"1.0\",\n  \"schemaVersion\": \"1.0\","),
				1,
			)
		}},
		{"trailing document", func(_ *testing.T, input []byte) []byte {
			return append(append([]byte(nil), input...), []byte("{}\n")...)
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeReceipt(t, test.mutate(t, valid), createReceiptPath, contract)
			assertRejectedReceipt(t, got, err)
		})
	}
}

func TestCompareReceiptsV1DetectsForbiddenVolumeReuse(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	create := decodeReceiptFixture(t, contract, "receipt-create.json", createReceiptPath)
	recreateData := readEnvironmentFixture(t, "valid", "v1", "receipt-recreate.json")
	recreateData = mutateDocument(func(document map[string]any) {
		replica := array(document, "replicas")[0].(map[string]any)
		volume := array(document, "volumes")[0].(map[string]any)
		replica["pvcUid"] = "pvc-uid-create-0"
		replica["pvUid"] = "pv-uid-create-0"
		volume["pvcUid"] = "pvc-uid-create-0"
		volume["pvUid"] = "pv-uid-create-0"
	})(t, recreateData)
	recreate, err := decodeReceipt(t, recreateData, recreateReceiptPath, contract)
	if err != nil {
		t.Fatalf("DecodeReceiptV1(reuse fixture): %v", err)
	}

	comparison, err := rf3environment.CompareReceiptsV1(create, recreate)
	if err != nil {
		t.Fatalf("CompareReceiptsV1() unexpected error: %v", err)
	}
	if comparison.Passed() ||
		!reflect.DeepEqual(comparison.ReusedPVCUIDs(), []string{"pvc-uid-create-0"}) ||
		!reflect.DeepEqual(comparison.ReusedPVUIDs(), []string{"pv-uid-create-0"}) {
		t.Fatalf("comparison did not preserve reuse evidence: %#v", comparison)
	}
}

func TestCompareReceiptsV1RejectsWrongCycleOrder(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	create := decodeReceiptFixture(t, contract, "receipt-create.json", createReceiptPath)
	got, err := rf3environment.CompareReceiptsV1(create, create)
	if err == nil || !reflect.DeepEqual(got, rf3environment.ReceiptComparisonV1{}) {
		t.Fatalf("CompareReceiptsV1(create, create) = %#v, %v", got, err)
	}
}

func TestReceiptDecodingIsInvariantToObservationOrdering(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	createData := readEnvironmentFixture(t, "valid", "v1", "receipt-create.json")
	property := func(seed int64) bool {
		mutated := mutateDocument(func(document map[string]any) {
			random := rand.New(rand.NewSource(seed))
			for _, field := range []string{"nodes", "replicas", "volumes", "artifacts"} {
				values := array(document, field)
				random.Shuffle(len(values), func(left, right int) {
					values[left], values[right] = values[right], values[left]
				})
			}
		})(t, createData)
		receipt, err := decodeReceipt(t, mutated, createReceiptPath, contract)
		return err == nil && len(receipt.Observation().Replicas) == 3
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("observation-order property failed: %v", err)
	}
}

func loadEnvironmentContract(t testing.TB) rf3environment.EnvironmentContractV1 {
	t.Helper()

	acceptance := loadAcceptance(t)
	data := readEnvironmentFixture(t, "valid", "v1", "manifest.json")
	manifest, err := decodeManifest(t, data, acceptance)
	if err != nil {
		t.Fatalf("DecodeManifestV1() fixture error: %v", err)
	}
	return manifest.Contract()
}

func loadAcceptance(t testing.TB) rf3acceptance.Bundle {
	t.Helper()

	directory := filepath.Join("..", "rf3acceptance", "testdata", "valid", "v1")
	root := mustSource(t, "root.json", readFile(t, filepath.Join(directory, "root.json")))
	workload := mustSource(t, "workload.json", readFile(t, filepath.Join(directory, "workload.json")))
	topology := mustSource(t, "topology.json", readFile(t, filepath.Join(directory, "topology.json")))
	bundle, err := rf3acceptance.DecodeBundle(root, workload, topology)
	if err != nil {
		t.Fatalf("DecodeBundle() fixture error: %v", err)
	}
	return bundle
}

func decodeManifest(
	t testing.TB,
	data []byte,
	acceptance rf3acceptance.Bundle,
) (rf3environment.EnvironmentManifestV1, error) {
	t.Helper()
	return rf3environment.DecodeManifestV1(mustSource(t, manifestPath, data), acceptance)
}

func decodeReceiptFixture(
	t testing.TB,
	contract rf3environment.EnvironmentContractV1,
	name string,
	sourcePath string,
) rf3environment.ObservedEnvironmentReceiptV1 {
	t.Helper()

	receipt, err := decodeReceipt(
		t,
		readEnvironmentFixture(t, "valid", "v1", name),
		sourcePath,
		contract,
	)
	if err != nil {
		t.Fatalf("DecodeReceiptV1(%s) fixture error: %v", name, err)
	}
	return receipt
}

func decodeReceipt(
	t testing.TB,
	data []byte,
	sourcePath string,
	contract rf3environment.EnvironmentContractV1,
) (rf3environment.ObservedEnvironmentReceiptV1, error) {
	t.Helper()
	return rf3environment.DecodeReceiptV1(mustSource(t, sourcePath, data), contract)
}

func mustSource(t testing.TB, sourcePath string, data []byte) rf3acceptance.Source {
	t.Helper()

	source, err := rf3acceptance.NewSource(sourcePath, data)
	if err != nil {
		t.Fatalf("NewSource(%q): %v", sourcePath, err)
	}
	return source
}

func readEnvironmentFixture(t testing.TB, parts ...string) []byte {
	t.Helper()
	return readFile(t, filepath.Join(append([]string{"testdata"}, parts...)...))
}

func readFile(t testing.TB, filePath string) []byte {
	t.Helper()

	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read %s: %v", filePath, err)
	}
	return data
}

func mutateDocument(mutate func(map[string]any)) func(*testing.T, []byte) []byte {
	return func(t *testing.T, data []byte) []byte {
		t.Helper()

		var document map[string]any
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatalf("decode fixture: %v", err)
		}
		mutate(document)
		encoded, err := json.MarshalIndent(document, "", "  ")
		if err != nil {
			t.Fatalf("encode fixture: %v", err)
		}
		return append(encoded, '\n')
	}
}

func object(parent map[string]any, fields ...string) map[string]any {
	current := parent
	for _, field := range fields {
		current = current[field].(map[string]any)
	}
	return current
}

func array(parent map[string]any, field string) []any {
	return parent[field].([]any)
}

func digest(character string) string {
	return "sha256:" + string(bytes.Repeat([]byte(character), 64))
}

func assertRejectedManifest(
	t testing.TB,
	got rf3environment.EnvironmentManifestV1,
	err error,
) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(got, rf3environment.EnvironmentManifestV1{}) {
		t.Fatalf("DecodeManifestV1() = %#v, %v; want zero plus error", got, err)
	}
}

func assertRejectedReceipt(
	t testing.TB,
	got rf3environment.ObservedEnvironmentReceiptV1,
	err error,
) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(got, rf3environment.ObservedEnvironmentReceiptV1{}) {
		t.Fatalf("DecodeReceiptV1() = %#v, %v; want zero plus error", got, err)
	}
}
