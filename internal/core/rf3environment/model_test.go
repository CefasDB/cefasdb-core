package rf3environment_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/core/rf3environment"
)

func TestEnvironmentManifestV1ReturnsDefensiveViews(t *testing.T) {
	t.Parallel()

	manifest := loadEnvironmentContract(t).Manifest()
	specification := manifest.Specification()
	specification.Topology.FailureDomains[0] = "mutated"
	specification.Topology.Nodes[0].NodeID = "mutated"
	if manifest.Specification().Topology.FailureDomains[0] != "synthetic-fd-a" ||
		manifest.Specification().Topology.Nodes[0].NodeID != "synthetic-node-a" {
		t.Fatal("Specification() exposed mutable manifest state")
	}

	admission := manifest.Admission()
	admission.RBACVerbs[0] = "delete"
	admission.RBACResources[0] = "secrets"
	if manifest.Admission().RBACVerbs[0] != "get" ||
		manifest.Admission().RBACResources[0] != "leases.coordination.k8s.io" {
		t.Fatal("Admission() exposed mutable manifest state")
	}
}

func TestObservedEnvironmentReceiptV1ReturnsDefensiveViews(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	receipt := decodeReceiptFixture(t, contract, "receipt-create.json", createReceiptPath)
	observation := receipt.Observation()
	observation.Nodes[0].NodeUID = "mutated"
	observation.Replicas[0].PVCUID = "mutated"
	observation.Volumes[0].PVUID = "mutated"
	if receipt.Observation().Nodes[0].NodeUID != "node-uid-create-a" ||
		receipt.Observation().Replicas[0].PVCUID != "pvc-uid-create-0" ||
		receipt.Observation().Volumes[0].PVUID != "pv-uid-create-0" {
		t.Fatal("Observation() exposed mutable receipt state")
	}

	evidence := receipt.Evidence()
	evidence.Artifacts[0].Path = "mutated"
	if receipt.Evidence().Artifacts[0].Path != "create/doctor.json" {
		t.Fatal("Evidence() exposed mutable artifact state")
	}
}

func TestReceiptComparisonV1ReturnsDefensiveViews(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	create := decodeReceiptFixture(t, contract, "receipt-create.json", createReceiptPath)
	recreate := decodeReceiptFixture(t, contract, "receipt-recreate.json", recreateReceiptPath)
	comparison, err := rf3environment.CompareReceiptsV1(create, recreate)
	if err != nil {
		t.Fatalf("CompareReceiptsV1() unexpected error: %v", err)
	}
	cycles := comparison.Cycles()
	cycles.CreateNodeUIDs[0] = "mutated"
	if comparison.Cycles().CreateNodeUIDs[0] == "mutated" {
		t.Fatal("Cycles() exposed mutable comparison state")
	}
}

func TestNewRecoveryReceiptV1EnforcesRecoveryProof(t *testing.T) {
	t.Parallel()

	contract := loadEnvironmentContract(t)
	create := decodeReceiptFixture(t, contract, "receipt-create.json", createReceiptPath)
	evidence := create.Evidence().Artifacts[:1]
	valid := rf3environment.RecoveryReceiptInputV1{
		ID:                   "rf3.environment.recovery.synthetic.v1",
		EnvironmentID:        contract.Manifest().Identity().ID,
		Reason:               rf3environment.QuarantineCleanupFailed,
		RecoveredAt:          time.Date(2026, 1, 4, 2, 0, 0, 0, time.UTC),
		OldRunTerminated:     true,
		CredentialsRevoked:   true,
		ProviderQuiescent:    true,
		EnvironmentDestroyed: true,
		Evidence:             evidence,
	}
	receipt, err := rf3environment.NewRecoveryReceiptV1(valid)
	if err != nil {
		t.Fatalf("NewRecoveryReceiptV1() unexpected error: %v", err)
	}
	if reflect.DeepEqual(receipt, rf3environment.RecoveryReceiptV1{}) {
		t.Fatal("NewRecoveryReceiptV1() returned zero")
	}
	proof := receipt.Proof()
	proof.Evidence[0].Path = "mutated"
	if receipt.Proof().Evidence[0].Path == "mutated" {
		t.Fatal("Proof() exposed mutable recovery evidence")
	}

	tests := []struct {
		name   string
		mutate func(*rf3environment.RecoveryReceiptInputV1)
	}{
		{"unsupported reason", func(input *rf3environment.RecoveryReceiptInputV1) {
			input.Reason = rf3environment.QuarantineReason("unsupported")
		}},
		{"old run active", func(input *rf3environment.RecoveryReceiptInputV1) {
			input.OldRunTerminated = false
		}},
		{"credential not revoked", func(input *rf3environment.RecoveryReceiptInputV1) {
			input.CredentialsRevoked = false
		}},
		{"provider not quiescent", func(input *rf3environment.RecoveryReceiptInputV1) {
			input.ProviderQuiescent = false
		}},
		{"both terminal proofs set", func(input *rf3environment.RecoveryReceiptInputV1) {
			input.BaselineRestored = true
		}},
		{"evidence missing", func(input *rf3environment.RecoveryReceiptInputV1) {
			input.Evidence = nil
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			input := valid
			test.mutate(&input)
			got, err := rf3environment.NewRecoveryReceiptV1(input)
			if err == nil || !reflect.DeepEqual(got, rf3environment.RecoveryReceiptV1{}) {
				t.Fatalf("NewRecoveryReceiptV1() = %#v, %v; want zero plus error", got, err)
			}
		})
	}
}
