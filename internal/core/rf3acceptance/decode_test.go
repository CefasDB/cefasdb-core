package rf3acceptance_test

import (
	"reflect"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

const (
	rootSourcePath     = "root.json"
	workloadSourcePath = "workload.json"
	topologySourcePath = "topology.json"
)

type bundleBytes struct {
	root     []byte
	workload []byte
	topology []byte
}

type invalidCase struct {
	name  string
	apply func(*testing.T, bundleBytes) bundleBytes
}

func TestDecodeBundleAcceptsExactSyntheticBundle(t *testing.T) {
	t.Parallel()

	input := readValidBundle(t)
	first, err := decodeBundle(t, input)
	if err != nil {
		t.Fatalf("DecodeBundle() unexpected error: %v", err)
	}
	assertNonzeroBundle(t, first)

	second, err := decodeBundle(t, input)
	if err != nil {
		t.Fatalf("DecodeBundle() repeat unexpected error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("DecodeBundle() is not deterministic:\nfirst:  %#v\nsecond: %#v", first, second)
	}
}

func TestDecodeBundleRejectsEveryInvalidPartition(t *testing.T) {
	t.Parallel()

	tests := append([]invalidCase{}, invalidDocumentCases()...)
	tests = append(tests, invalidIdentityCases()...)
	tests = append(tests, invalidApprovalCases()...)
	tests = append(tests, invalidProvenanceCases()...)
	tests = append(tests, invalidMembershipCases()...)
	tests = append(tests, invalidTopologyCases()...)
	tests = append(tests, invalidSafeRejectionCases()...)

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			input := test.apply(t, readValidBundle(t))
			got, err := decodeBundle(t, input)
			assertRejectedBundle(t, got, err)
		})
	}
}

func TestDecodeBundleRequiresEverySource(t *testing.T) {
	t.Parallel()

	input := readValidBundle(t)
	root := mustSource(t, rootSourcePath, input.root)
	workload := mustSource(t, workloadSourcePath, input.workload)
	topology := mustSource(t, topologySourcePath, input.topology)

	tests := []struct {
		name     string
		root     rf3acceptance.Source
		workload rf3acceptance.Source
		topology rf3acceptance.Source
	}{
		{name: "missing root", workload: workload, topology: topology},
		{name: "missing workload", root: root, topology: topology},
		{name: "missing topology", root: root, workload: workload},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := rf3acceptance.DecodeBundle(test.root, test.workload, test.topology)
			assertRejectedBundle(t, got, err)
		})
	}
}

func invalidDocumentCases() []invalidCase {
	return []invalidCase{
		rootCase("unknown root field", func(_ *testing.T, document map[string]any) {
			document["unexpected"] = true
		}),
		workloadCase("unknown workload field", true, func(_ *testing.T, document map[string]any) {
			document["unexpected"] = true
		}),
		topologyCase("unknown topology field", true, func(_ *testing.T, document map[string]any) {
			document["unexpected"] = true
		}),
		rootCase("missing root field", func(_ *testing.T, document map[string]any) {
			delete(document, "defaultRule")
		}),
		workloadCase("missing workload field", true, func(_ *testing.T, document map[string]any) {
			delete(document, "profiles")
		}),
		topologyCase("missing topology field", true, func(_ *testing.T, document map[string]any) {
			delete(document, "failureDomains")
		}),
		rawRootCase("duplicate root key", func(input []byte) []byte {
			return duplicateSchemaVersion(input)
		}),
		rawWorkloadCase("duplicate workload key", true, func(input []byte) []byte {
			return duplicateSchemaVersion(input)
		}),
		rawTopologyCase("duplicate topology key", true, func(input []byte) []byte {
			return duplicateSchemaVersion(input)
		}),
		rawRootCase("trailing root document", appendTrailingDocument),
		rawWorkloadCase("trailing workload document", true, appendTrailingDocument),
		rawTopologyCase("trailing topology document", true, appendTrailingDocument),
		rootCase("unsupported schema version", func(_ *testing.T, document map[string]any) {
			document["schemaVersion"] = "1.1"
		}),
		rootCase("invalid root enum", func(t *testing.T, document map[string]any) {
			objectAt(t, document, "capacityMethod")["status"] = "accepted"
		}),
		workloadCase("invalid workload enum", true, func(t *testing.T, document map[string]any) {
			profileAt(t, document, "scan")["consistency"] = "linearizable"
		}),
		topologyCase("invalid topology enum", true, func(_ *testing.T, document map[string]any) {
			document["environment"] = "production"
		}),
		rootCase("error rate above ppm boundary", func(t *testing.T, document map[string]any) {
			quantityAt(t, objectAt(t, document, "defaultRule"), "maxErrorRate")["value"] = float64(1_000_001)
		}),
		workloadCase("zero dataset item boundary", true, func(t *testing.T, document map[string]any) {
			quantityAt(t, objectAt(t, document, "dataset"), "itemCount")["value"] = float64(0)
		}),
		topologyCase("zero node boundary", true, func(t *testing.T, document map[string]any) {
			quantityAt(t, document, "nodeCount")["value"] = float64(0)
		}),
	}
}

func invalidIdentityCases() []invalidCase {
	return []invalidCase{
		rootCase("mutable latest path alias", func(t *testing.T, document map[string]any) {
			objectAt(t, objectAt(t, document, "references"), "workload")["path"] = "latest"
		}),
		rootCase("mutable current profile alias", func(t *testing.T, document map[string]any) {
			objectAt(t, objectAt(t, document, "references"), "workload")["profileId"] = "current"
		}),
		rootCase("stale workload digest", func(t *testing.T, document map[string]any) {
			setBoundDigest(t, document, "workload", zeroDigest())
		}),
		rootCase("swapped workload and topology digests", func(t *testing.T, document map[string]any) {
			references := objectAt(t, document, "references")
			workloadDigest := stringAt(t, objectAt(t, references, "workload"), "digest")
			topologyDigest := stringAt(t, objectAt(t, references, "topology"), "digest")
			setBoundDigest(t, document, "workload", topologyDigest)
			setBoundDigest(t, document, "topology", workloadDigest)
		}),
		rawWorkloadCase("formatting change invalidates exact bytes", false, func(input []byte) []byte {
			return append([]byte(" "), input...)
		}),
	}
}

func invalidApprovalCases() []invalidCase {
	var cases []invalidCase
	for _, role := range []string{"product-owner", "database-operator", "platform-owner"} {
		role := role
		cases = append(cases, rootCase(role+" approval is missing", func(t *testing.T, document map[string]any) {
			document["approvals"] = removeNamedObject(t, arrayAt(t, document, "approvals"), "role", role)
		}))
		cases = append(cases, rootCase(role+" approval is duplicated", func(t *testing.T, document map[string]any) {
			approvals := arrayAt(t, document, "approvals")
			document["approvals"] = append(approvals, cloneJSONValue(t, approvalAt(t, document, role)))
		}))
	}
	return append(cases,
		rootCase("approval is not bound to workload", func(t *testing.T, document map[string]any) {
			approvalAt(t, document, "product-owner")["workloadDigest"] = zeroDigest()
		}),
		rootCase("approval predates referenced bytes", func(t *testing.T, document map[string]any) {
			approvalAt(t, document, "platform-owner")["approvedAt"] = "2025-12-31T23:59:59Z"
		}),
		rootCase("approval is bound to another contract", func(t *testing.T, document map[string]any) {
			approvalAt(t, document, "database-operator")["contractId"] = "rf3.acceptance.synthetic.v2"
		}),
	)
}

func invalidProvenanceCases() []invalidCase {
	return []invalidCase{
		workloadCase("measured value has no evidence", true, func(t *testing.T, document map[string]any) {
			provenance := provenanceAt(t, quantityAt(t, objectAt(t, document, "dataset"), "itemCount"))
			delete(provenance, "evidence")
		}),
		rootCase("proposed target has no owner", func(t *testing.T, document map[string]any) {
			provenance := provenanceAt(t, quantityAt(t, objectAt(t, document, "defaultRule"), "observationWindow"))
			delete(provenance, "owner")
		}),
		rootCase("proposed target has no rationale", func(t *testing.T, document map[string]any) {
			provenance := provenanceAt(t, quantityAt(t, objectAt(t, document, "defaultRule"), "observationWindow"))
			delete(provenance, "rationale")
		}),
		rootCase("proposed target carries measured evidence", func(t *testing.T, document map[string]any) {
			provenance := provenanceAt(t, quantityAt(t, objectAt(t, document, "defaultRule"), "observationWindow"))
			provenance["evidence"] = "synthetic://unexpected"
		}),
	}
}

func invalidMembershipCases() []invalidCase {
	return []invalidCase{
		workloadCase("missing scan profile", true, func(t *testing.T, document map[string]any) {
			document["profiles"] = removeNamedObject(t, arrayAt(t, document, "profiles"), "name", "scan")
		}),
		workloadCase("extra workload profile", true, func(t *testing.T, document map[string]any) {
			profiles := arrayAt(t, document, "profiles")
			extra := cloneJSONValue(t, profiles[1]).(map[string]any)
			extra["name"] = "bulk-read"
			document["profiles"] = append(profiles, extra)
		}),
		workloadCase("duplicate workload profile", true, func(t *testing.T, document map[string]any) {
			profiles := arrayAt(t, document, "profiles")
			document["profiles"] = append(profiles, cloneJSONValue(t, profiles[0]))
		}),
		rootCase("missing network partition scenario", func(t *testing.T, document map[string]any) {
			document["scenarios"] = removeNamedObject(
				t,
				arrayAt(t, document, "scenarios"),
				"name",
				"network-partition",
			)
		}),
		rootCase("extra resilience scenario", func(t *testing.T, document map[string]any) {
			scenarios := arrayAt(t, document, "scenarios")
			extra := cloneJSONValue(t, scenarios[1]).(map[string]any)
			extra["name"] = "rolling-restart"
			document["scenarios"] = append(scenarios, extra)
		}),
		rootCase("duplicate resilience scenario", func(t *testing.T, document map[string]any) {
			scenarios := arrayAt(t, document, "scenarios")
			document["scenarios"] = append(scenarios, cloneJSONValue(t, scenarios[0]))
		}),
	}
}

func invalidTopologyCases() []invalidCase {
	return []invalidCase{
		topologyCase("replication factor is not three", true, func(t *testing.T, document map[string]any) {
			quantityAt(t, document, "replicationFactor")["value"] = float64(2)
		}),
		topologyCase("failure domains are not distinct", true, func(t *testing.T, document map[string]any) {
			domains := arrayAt(t, document, "failureDomains")
			domains[2] = domains[1]
		}),
		topologyCase("durable volumes are disabled", true, func(t *testing.T, document map[string]any) {
			objectAt(t, document, "volume")["durable"] = false
		}),
		topologyCase("anti colocation is disabled", true, func(_ *testing.T, document map[string]any) {
			document["antiColocation"] = false
		}),
		topologyCase("resource unit is missing", true, func(t *testing.T, document map[string]any) {
			delete(quantityAt(t, objectAt(t, document, "resourcesPerNode"), "network"), "unit")
		}),
	}
}

func invalidSafeRejectionCases() []invalidCase {
	return []invalidCase{
		rootCase("two node failure does not require safe rejection", func(t *testing.T, document map[string]any) {
			scenarioAt(t, document, "two-node-failure")["requireSafeRejection"] = false
		}),
		rootCase("two node failure acknowledges a no quorum write", func(t *testing.T, document map[string]any) {
			scenario := scenarioAt(t, document, "two-node-failure")
			quantityAt(t, scenario, "maxAcknowledgedWritesDuringNoQuorum")["value"] = float64(1)
		}),
		rootCase("two node failure observes no rejected write", func(t *testing.T, document map[string]any) {
			scenario := scenarioAt(t, document, "two-node-failure")
			quantityAt(t, scenario, "minimumRejectedWritesDuringNoQuorum")["value"] = float64(0)
		}),
		rootCase("two node failure omits safe rejection counter", func(t *testing.T, document map[string]any) {
			delete(scenarioAt(t, document, "two-node-failure"), "minimumRejectedWritesDuringNoQuorum")
		}),
		rootCase("two node failure omits recovered baseline", func(t *testing.T, document map[string]any) {
			scenarioAt(t, document, "two-node-failure")["requireBaselineAfterRecovery"] = false
		}),
	}
}
