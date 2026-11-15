package rf3acceptance_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

func TestNewSourceAcceptsCanonicalRelativePaths(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"root.json",
		"profiles/workload-v1.json",
		"profiles/topology-v1.json",
	} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			got, err := rf3acceptance.NewSource(path, []byte("{}"))
			if err != nil {
				t.Fatalf("NewSource(%q) unexpected error: %v", path, err)
			}
			if reflect.DeepEqual(got, rf3acceptance.Source{}) {
				t.Fatalf("NewSource(%q) returned Source{}", path)
			}
		})
	}
}

func TestNewSourceRejectsNonCanonicalOrMutablePaths(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"",
		"/root.json",
		"./root.json",
		"profiles/../root.json",
		"profiles//root.json",
		`profiles\root.json`,
		"latest",
		"current",
		"stable",
	} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			got, err := rf3acceptance.NewSource(path, []byte("{}"))
			if err == nil {
				t.Fatalf("NewSource(%q) unexpectedly succeeded", path)
			}
			if !reflect.DeepEqual(got, rf3acceptance.Source{}) {
				t.Fatalf("NewSource(%q) returned a partial source: %#v", path, got)
			}
		})
	}
}

func TestNewSourceDefensivelyCopiesExactBytes(t *testing.T) {
	t.Parallel()

	input := readValidBundle(t)
	root := mustSource(t, rootSourcePath, input.root)
	workload := mustSource(t, workloadSourcePath, input.workload)
	topology := mustSource(t, topologySourcePath, input.topology)

	overwrite(input.root)
	overwrite(input.workload)
	overwrite(input.topology)

	got, err := rf3acceptance.DecodeBundle(root, workload, topology)
	if err != nil {
		t.Fatalf("DecodeBundle() after caller mutation: %v", err)
	}
	assertNonzeroBundle(t, got)
}

func TestTopologyProfileReadViewsExposeValidatedValues(t *testing.T) {
	t.Parallel()

	input := readValidBundle(t)
	bundle, err := decodeBundle(t, input)
	if err != nil {
		t.Fatalf("DecodeBundle() unexpected error: %v", err)
	}

	topology := bundle.Topology()
	if topology.Environment() != "non-production" ||
		topology.NodeCount() != 3 ||
		topology.Replicas() != 3 ||
		topology.ReplicationFactor() != 3 ||
		topology.Shards() != 24 ||
		!topology.AntiColocation() {
		t.Fatalf("topology read views do not expose the validated fixture: %#v", topology)
	}
	volume := topology.Volume()
	if !volume.Durable || volume.Class != "synthetic-durable" ||
		volume.AccessMode != "read-write-once" ||
		volume.Size != (rf3acceptance.TopologyQuantityV1{Value: 20, Unit: "gibibytes"}) {
		t.Fatalf("Volume() = %#v", volume)
	}
	resources := topology.ResourcesPerNode()
	if resources.CPU.Value != 2 || resources.CPU.Unit != "cores" ||
		resources.Memory.Value != 4 || resources.Memory.Unit != "gibibytes" ||
		resources.Storage.Value != 20 || resources.Storage.Unit != "gibibytes" ||
		resources.Network.Value != 1000 || resources.Network.Unit != "megabits-per-second" {
		t.Fatalf("ResourcesPerNode() = %#v", resources)
	}

	domains := topology.FailureDomains()
	domains[0] = "mutated"
	if topology.FailureDomains()[0] != "synthetic-fd-a" {
		t.Fatal("FailureDomains() exposed mutable topology state")
	}
}

func TestBundleDigestsBindExactSourceBytes(t *testing.T) {
	t.Parallel()

	input := readValidBundle(t)
	bundle, err := decodeBundle(t, input)
	if err != nil {
		t.Fatalf("DecodeBundle() unexpected error: %v", err)
	}

	if bundle.AcceptanceDigest().String() != digestOf(input.root) ||
		bundle.WorkloadDigest().String() != digestOf(input.workload) ||
		bundle.TopologyDigest().String() != digestOf(input.topology) {
		t.Fatal("bundle digest views do not bind the exact input bytes")
	}
}

func overwrite(data []byte) {
	for index := range data {
		data[index] = 'x'
	}
}

func rootCase(name string, mutate func(*testing.T, map[string]any)) invalidCase {
	return invalidCase{
		name: name,
		apply: func(t *testing.T, input bundleBytes) bundleBytes {
			input.root = mutateJSONObject(t, input.root, mutate)
			return input
		},
	}
}

func workloadCase(name string, rebind bool, mutate func(*testing.T, map[string]any)) invalidCase {
	return invalidCase{
		name: name,
		apply: func(t *testing.T, input bundleBytes) bundleBytes {
			input.workload = mutateJSONObject(t, input.workload, mutate)
			if rebind {
				input = bindProfileIdentities(t, input)
			}
			return input
		},
	}
}

func topologyCase(name string, rebind bool, mutate func(*testing.T, map[string]any)) invalidCase {
	return invalidCase{
		name: name,
		apply: func(t *testing.T, input bundleBytes) bundleBytes {
			input.topology = mutateJSONObject(t, input.topology, mutate)
			if rebind {
				input = bindProfileIdentities(t, input)
			}
			return input
		},
	}
}

func rawRootCase(name string, mutate func([]byte) []byte) invalidCase {
	return invalidCase{
		name: name,
		apply: func(_ *testing.T, input bundleBytes) bundleBytes {
			input.root = mutate(input.root)
			return input
		},
	}
}

func rawWorkloadCase(name string, rebind bool, mutate func([]byte) []byte) invalidCase {
	return invalidCase{
		name: name,
		apply: func(t *testing.T, input bundleBytes) bundleBytes {
			input.workload = mutate(input.workload)
			if rebind {
				input = bindProfileDigests(t, input)
			}
			return input
		},
	}
}

func rawTopologyCase(name string, rebind bool, mutate func([]byte) []byte) invalidCase {
	return invalidCase{
		name: name,
		apply: func(t *testing.T, input bundleBytes) bundleBytes {
			input.topology = mutate(input.topology)
			if rebind {
				input = bindProfileDigests(t, input)
			}
			return input
		},
	}
}

func bindProfileIdentities(t *testing.T, input bundleBytes) bundleBytes {
	t.Helper()

	workload := decodeJSONObject(t, input.workload)
	topology := decodeJSONObject(t, input.topology)
	root := decodeJSONObject(t, input.root)

	workloadID := stringAt(t, workload, "profileId")
	topologyID := stringAt(t, topology, "profileId")
	workloadDigest := digestOf(input.workload)
	topologyDigest := digestOf(input.topology)

	references := objectAt(t, root, "references")
	workloadReference := objectAt(t, references, "workload")
	workloadReference["profileId"] = workloadID
	workloadReference["digest"] = workloadDigest
	topologyReference := objectAt(t, references, "topology")
	topologyReference["profileId"] = topologyID
	topologyReference["digest"] = topologyDigest

	for _, value := range arrayAt(t, root, "approvals") {
		approval := objectValue(t, value, "approval")
		approval["workloadProfileId"] = workloadID
		approval["workloadDigest"] = workloadDigest
		approval["topologyProfileId"] = topologyID
		approval["topologyDigest"] = topologyDigest
	}
	input.root = encodeJSONObject(t, root)
	return input
}

func bindProfileDigests(t *testing.T, input bundleBytes) bundleBytes {
	t.Helper()

	root := decodeJSONObject(t, input.root)
	setBoundDigest(t, root, "workload", digestOf(input.workload))
	setBoundDigest(t, root, "topology", digestOf(input.topology))
	input.root = encodeJSONObject(t, root)
	return input
}

func setBoundDigest(t *testing.T, root map[string]any, profile string, digest string) {
	t.Helper()

	reference := objectAt(t, objectAt(t, root, "references"), profile)
	reference["digest"] = digest
	field := profile + "Digest"
	for _, value := range arrayAt(t, root, "approvals") {
		objectValue(t, value, "approval")[field] = digest
	}
}

func approvalAt(t *testing.T, root map[string]any, role string) map[string]any {
	t.Helper()

	for _, value := range arrayAt(t, root, "approvals") {
		approval := objectValue(t, value, "approval")
		if approval["role"] == role {
			return approval
		}
	}
	t.Fatalf("approval role %q not found", role)
	return nil
}

func profileAt(t *testing.T, workload map[string]any, name string) map[string]any {
	t.Helper()

	for _, value := range arrayAt(t, workload, "profiles") {
		profile := objectValue(t, value, "profile")
		if profile["name"] == name {
			return profile
		}
	}
	t.Fatalf("profile %q not found", name)
	return nil
}

func scenarioAt(t *testing.T, root map[string]any, name string) map[string]any {
	t.Helper()

	for _, value := range arrayAt(t, root, "scenarios") {
		scenario := objectValue(t, value, "scenario")
		if scenario["name"] == name {
			return scenario
		}
	}
	t.Fatalf("scenario %q not found", name)
	return nil
}

func quantityAt(t *testing.T, parent map[string]any, field string) map[string]any {
	t.Helper()
	return objectAt(t, parent, field)
}

func provenanceAt(t *testing.T, quantity map[string]any) map[string]any {
	t.Helper()
	return objectAt(t, quantity, "provenance")
}

func objectAt(t *testing.T, parent map[string]any, fields ...string) map[string]any {
	t.Helper()

	current := parent
	for _, field := range fields {
		value, ok := current[field]
		if !ok {
			t.Fatalf("field %q is missing", field)
		}
		current = objectValue(t, value, field)
	}
	return current
}

func arrayAt(t *testing.T, parent map[string]any, field string) []any {
	t.Helper()

	value, ok := parent[field]
	if !ok {
		t.Fatalf("field %q is missing", field)
	}
	array, ok := value.([]any)
	if !ok {
		t.Fatalf("field %q = %T, want array", field, value)
	}
	return array
}

func stringAt(t *testing.T, parent map[string]any, field string) string {
	t.Helper()

	value, ok := parent[field].(string)
	if !ok {
		t.Fatalf("field %q = %T, want string", field, parent[field])
	}
	return value
}

func objectValue(t *testing.T, value any, label string) map[string]any {
	t.Helper()

	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want object", label, value)
	}
	return object
}

func removeNamedObject(
	t *testing.T,
	values []any,
	field string,
	name string,
) []any {
	t.Helper()

	filtered := make([]any, 0, len(values))
	for _, value := range values {
		object := objectValue(t, value, field)
		if object[field] != name {
			filtered = append(filtered, value)
		}
	}
	if len(filtered) != len(values)-1 {
		t.Fatalf("removed %d values named %q, want exactly one", len(values)-len(filtered), name)
	}
	return filtered
}

func cloneJSONValue(t *testing.T, value any) any {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal clone: %v", err)
	}
	var cloned any
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		t.Fatalf("unmarshal clone: %v", err)
	}
	return cloned
}

func mutateJSONObject(
	t *testing.T,
	input []byte,
	mutate func(*testing.T, map[string]any),
) []byte {
	t.Helper()

	document := decodeJSONObject(t, input)
	mutate(t, document)
	return encodeJSONObject(t, document)
}

func decodeJSONObject(t *testing.T, input []byte) map[string]any {
	t.Helper()

	var document map[string]any
	if err := json.Unmarshal(input, &document); err != nil {
		t.Fatalf("decode JSON fixture: %v", err)
	}
	return document
}

func encodeJSONObject(t *testing.T, document map[string]any) []byte {
	t.Helper()

	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatalf("encode JSON fixture: %v", err)
	}
	return append(encoded, '\n')
}

func duplicateSchemaVersion(input []byte) []byte {
	needle := []byte(`  "schemaVersion": "1.0",`)
	replacement := []byte(`  "schemaVersion": "1.0",
  "schemaVersion": "1.0",`)
	return bytes.Replace(input, needle, replacement, 1)
}

func appendTrailingDocument(input []byte) []byte {
	result := append([]byte(nil), input...)
	return append(result, []byte("{}\n")...)
}

func digestOf(input []byte) string {
	sum := sha256.Sum256(input)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func zeroDigest() string {
	return "sha256:" + string(bytes.Repeat([]byte{'0'}, sha256.Size*2))
}

func readValidBundle(t testing.TB) bundleBytes {
	t.Helper()

	directory := filepath.Join("testdata", "valid", "v1")
	return bundleBytes{
		root:     readFixture(t, filepath.Join(directory, rootSourcePath)),
		workload: readFixture(t, filepath.Join(directory, workloadSourcePath)),
		topology: readFixture(t, filepath.Join(directory, topologySourcePath)),
	}
}

func readFixture(t testing.TB, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return data
}

func decodeBundle(t testing.TB, input bundleBytes) (rf3acceptance.Bundle, error) {
	t.Helper()

	return rf3acceptance.DecodeBundle(
		mustSource(t, rootSourcePath, input.root),
		mustSource(t, workloadSourcePath, input.workload),
		mustSource(t, topologySourcePath, input.topology),
	)
}

func mustSource(t testing.TB, path string, data []byte) rf3acceptance.Source {
	t.Helper()

	source, err := rf3acceptance.NewSource(path, data)
	if err != nil {
		t.Fatalf("NewSource(%q) fixture error: %v", path, err)
	}
	return source
}

func assertRejectedBundle(t testing.TB, got rf3acceptance.Bundle, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("DecodeBundle() unexpectedly succeeded")
	}
	if !reflect.DeepEqual(got, rf3acceptance.Bundle{}) {
		t.Fatalf("DecodeBundle() returned a partial bundle: %#v", got)
	}
}

func assertNonzeroBundle(t testing.TB, got rf3acceptance.Bundle) {
	t.Helper()

	if reflect.DeepEqual(got, rf3acceptance.Bundle{}) {
		t.Fatal("DecodeBundle() returned Bundle{} for valid input")
	}
}
