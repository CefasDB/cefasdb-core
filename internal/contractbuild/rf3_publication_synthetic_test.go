package contractbuild

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

const (
	syntheticRF3ManifestLocator = "codetask/synthetic/manifest.json"
	syntheticRF3PolicyLocator   = "codetask/synthetic/policy.json"
	syntheticRF3PlatformLocator = "codetask/synthetic/platform.json"
	syntheticRF3RunID           = "RUN-SYNTHETIC-RF3"
	syntheticRF3InitiativeID    = "INIT-SYNTHETIC-RF3"
	syntheticRF3StepID          = "synthetic-publication"
)

func TestLoadRF3PublishedAcceptanceFilesRejectsProtectedSemanticDrift(t *testing.T) {
	t.Parallel()

	corpus := readRF3Corpus(t)
	directory := t.TempDir()
	rootPath := writeRF3File(t, directory, rf3RootName, corpus.root)
	workloadPath := writeRF3File(t, directory, rf3WorkloadName, corpus.workload)
	topologyPath := writeRF3File(t, directory, rf3TopologyName, corpus.topology)
	publicationPath, verifier := writeSyntheticRF3Publication(
		t,
		directory,
		rootPath,
		workloadPath,
		topologyPath,
	)
	tamperSyntheticRF3ApprovalDecision(t, publicationPath, verifier)

	got, err := LoadRF3PublishedAcceptanceFiles(
		context.Background(),
		RF3PublishedFiles{
			RootPath:        rootPath,
			WorkloadPath:    workloadPath,
			TopologyPath:    topologyPath,
			PublicationPath: publicationPath,
		},
		verifier,
	)
	if err == nil || !reflect.DeepEqual(got, RF3PublishedAcceptanceV1{}) {
		t.Fatalf("published acceptance = %#v, %v; want zero plus error", got, err)
	}
	if !strings.Contains(err.Error(), "decision does not match") {
		t.Fatalf("error = %v, want protected decision mismatch", err)
	}
}

func writeSyntheticRF3Publication(
	t testing.TB,
	directory string,
	rootPath string,
	workloadPath string,
	topologyPath string,
) (string, *rf3EvidenceVerifier) {
	t.Helper()
	bundle, err := LoadRF3AcceptanceFiles(
		context.Background(),
		rootPath,
		workloadPath,
		topologyPath,
	)
	if err != nil {
		t.Fatalf("load synthetic acceptance: %v", err)
	}
	var acceptance RF3AcceptanceDocumentV1
	rootData, err := os.ReadFile(rootPath)
	if err != nil {
		t.Fatalf("read synthetic root: %v", err)
	}
	if err := decodeStrictRF3Document(rootData, &acceptance); err != nil {
		t.Fatalf("decode synthetic root: %v", err)
	}
	publication, documents := syntheticRF3Publication(t, bundle, acceptance)
	data := marshalRF3ProtectedDocument(t, publication)
	path := filepath.Join(directory, "publication-v1.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write synthetic publication: %v", err)
	}
	return path, &rf3EvidenceVerifier{remaining: documents}
}

func syntheticRF3Publication(
	t testing.TB,
	bundle rf3acceptance.Bundle,
	acceptance RF3AcceptanceDocumentV1,
) (map[string]any, map[string][]byte) {
	t.Helper()
	platform := syntheticRF3PlatformInput(bundle, acceptance)
	platformDocument := marshalRF3ProtectedDocument(t, platform)
	platformDigest := syntheticRF3Digest(t, syntheticRF3PlatformLocator, platformDocument)

	policy := syntheticRF3DecisionPolicy(t, bundle, acceptance, platformDigest)
	policyDocument := marshalRF3ProtectedDocument(t, policy)
	policyDigest := syntheticRF3Digest(t, syntheticRF3PolicyLocator, policyDocument)

	manifest := syntheticRF3ReviewManifest(policy)
	manifestDocument := marshalRF3ProtectedDocument(t, manifest)
	manifestDigest := syntheticRF3Digest(t, syntheticRF3ManifestLocator, manifestDocument)

	approvalMaps, approvalDocuments := syntheticRF3Approvals(
		t,
		acceptance,
		bundle.AcceptanceDigest().String(),
		policy,
		policyDigest,
		manifestDigest,
	)
	documents := map[string][]byte{
		syntheticRF3ManifestLocator: manifestDocument,
		syntheticRF3PolicyLocator:   policyDocument,
		syntheticRF3PlatformLocator: platformDocument,
	}
	for locator, document := range approvalDocuments {
		documents[locator] = document
	}

	publication := map[string]any{
		"documentType":  rf3PublicationDocumentType,
		"schemaVersion": "1.0",
		"contractId":    bundle.Acceptance().ID().String(),
		"bundle": map[string]any{
			"root": map[string]any{
				"path":   filepath.Base(rf3RootName),
				"digest": bundle.AcceptanceDigest().String(),
			},
			"workload": map[string]any{
				"path":      filepath.Base(rf3WorkloadName),
				"profileId": bundle.Workload().ID().String(),
				"digest":    bundle.WorkloadDigest().String(),
			},
			"topology": map[string]any{
				"path":      filepath.Base(rf3TopologyName),
				"profileId": bundle.Topology().ID().String(),
				"digest":    bundle.TopologyDigest().String(),
			},
		},
		"review": map[string]any{
			"manifest": map[string]any{
				"evidence": syntheticRF3ManifestLocator,
				"digest":   manifestDigest,
			},
			"decisionPolicy": map[string]any{
				"policyId": rf3DecisionPolicyID,
				"evidence": syntheticRF3PolicyLocator,
				"digest":   policyDigest,
			},
		},
		"platformInputs": map[string]any{
			"evidence":       syntheticRF3PlatformLocator,
			"digest":         platformDigest,
			"failureDomains": bundle.Topology().FailureDomains(),
			"storageClass":   bundle.Topology().Volume().Class,
		},
		"attemptSemantics": map[string]any{
			"rootField":        rf3AttemptRootField,
			"encodedUnit":      rf3AttemptEncodedUnit,
			"denominator":      rf3AttemptDenominator,
			"writeAttempt":     rf3AttemptWrite,
			"pointReadAttempt": rf3AttemptPointRead,
			"failedBatch":      rf3AttemptFailedBatch,
		},
		"approvals": approvalMaps,
	}
	return publication, documents
}

func syntheticRF3PlatformInput(
	bundle rf3acceptance.Bundle,
	acceptance RF3AcceptanceDocumentV1,
) RF3PlatformInputDecisionV1 {
	issuer := acceptance.Approvals[2].Approver
	return RF3PlatformInputDecisionV1{
		Kind:                rf3ConfigurationApprovalKind,
		RunID:               syntheticRF3RunID,
		TaskID:              rf3ApprovalTaskID,
		InitiativeID:        syntheticRF3InitiativeID,
		StepID:              syntheticRF3StepID,
		ProposalSHA256:      strings.Repeat("1", 64),
		ManifestPath:        "codetask/synthetic/platform-manifest.json",
		ManifestSHA256:      strings.Repeat("2", 64),
		CandidatePath:       "codetask/synthetic/platform-candidate.json",
		CandidateSHA256:     strings.Repeat("3", 64),
		ConfigurationPath:   "codetask/synthetic/configuration.md",
		ConfigurationSHA256: strings.Repeat("4", 64),
		InputSetSHA256:      strings.Repeat("5", 64),
		TaskScopeSHA256:     strings.Repeat("6", 64),
		ProposalRevision:    1,
		Values: RF3PlatformInputValuesV1{
			FailureDomains: bundle.Topology().FailureDomains(), StorageClass: bundle.Topology().Volume().Class}, Scope: rf3ConfigurationInputScope,
		IssuerRole:      "platform-owner",
		Issuer:          issuer,
		Organization:    "CefasDB",
		OrganizationID:  1,
		AuthoritySource: "codetask/synthetic/platform.json#ownerRegistration",
		OwnerRegistration: RF3OwnerRegistrationV1{
			Scope:                  "this-input-review",
			Team:                   "team-cefasdb",
			Assignee:               issuer,
			RegisteredBy:           issuer,
			OrganizationMembership: "https://example.test/membership",
			RegistrySHA256:         strings.Repeat("7", 64),
		},
		Method: "direct",
		At:     "2026-01-02T00:00:00Z",
	}
}

func syntheticRF3DecisionPolicy(
	t testing.TB,
	bundle rf3acceptance.Bundle,
	acceptance RF3AcceptanceDocumentV1,
	platformDigest string,
) RF3DecisionPolicyV1 {
	t.Helper()
	platformDigestHex, err := rf3DigestHex(platformDigest)
	if err != nil {
		t.Fatalf("synthetic platform digest: %v", err)
	}
	workloadDigest, err := rf3DigestHex(acceptance.References.Workload.Digest)
	if err != nil {
		t.Fatalf("synthetic workload digest: %v", err)
	}
	topologyDigest, err := rf3DigestHex(acceptance.References.Topology.Digest)
	if err != nil {
		t.Fatalf("synthetic topology digest: %v", err)
	}
	return RF3DecisionPolicyV1{
		DocumentType:   rf3DecisionPolicyDocumentType,
		SchemaVersion:  "1.0",
		PolicyID:       rf3DecisionPolicyID,
		TaskID:         rf3ApprovalTaskID,
		ContractID:     acceptance.ContractID,
		SourceRevision: strings.Repeat("a", 40),
		Proposal: RF3DecisionPolicyProposalV1{
			Path:            "codetask/synthetic/proposal.md",
			SHA256:          strings.Repeat("8", 64),
			SelectionSHA256: strings.Repeat("9", 64),
			Revision:        1,
		},
		SelectedPreparation: RF3DocumentBindingV1{
			Path:   "codetask/synthetic/preparation.md",
			SHA256: strings.Repeat("b", 64),
		},
		ConfigurationInputApproval: RF3ConfigurationInputBindingV1{
			Path:   syntheticRF3PlatformLocator,
			SHA256: platformDigestHex,
			Scope:  rf3ConfigurationInputScope,
		},
		Profiles: RF3DecisionPolicyProfilesV1{
			Workload: RF3DecisionPolicyProfileV1{
				ProfileID: bundle.Workload().ID().String(),
				Path:      "contracts/rf3-acceptance/" + acceptance.References.Workload.Path,
				SHA256:    workloadDigest,
			},
			Topology: RF3DecisionPolicyProfileV1{
				ProfileID: bundle.Topology().ID().String(),
				Path:      "contracts/rf3-acceptance/" + acceptance.References.Topology.Path,
				SHA256:    topologyDigest,
			},
		},
		Decisions: RF3DecisionPolicyDecisionsV1{
			WorkloadAndServingTargets: "synthetic-exact-values",
			SchemaV1Applicability:     "synthetic-schema-v1",
			Variance: RF3DecisionPolicyVarianceV1{
				Method:         acceptance.Variance.Method,
				Population:     acceptance.Variance.Population,
				Aggregation:    acceptance.Variance.Aggregation,
				Metrics:        []string{"throughput", "p99", "recovery"},
				Formula:        "ceil((max-min)/median*1000000)",
				MinimumRuns:    acceptance.Variance.MinimumRuns.Value,
				TolerancePPM:   acceptance.Variance.Tolerance.Value,
				IncompleteWhen: []string{"missing-or-incomparable-set", "zero-median"},
			},
			Capacity: RF3DecisionPolicyCapacityV1{
				Method:                       acceptance.CapacityMethod.Method,
				PeakUnit:                     acceptance.CapacityMethod.PeakUnit,
				AcceptedPeak:                 100,
				FirstProbe:                   300,
				MaximumFloorRoundedMidpoints: 7,
				ObservationWindowMSPerPoint: acceptance.
					CapacityMethod.ObservationWindow.Value,
				SaturationSignal:            acceptance.CapacityMethod.SaturationSignal,
				Rounding:                    acceptance.CapacityMethod.Rounding,
				HeadroomPPM:                 acceptance.CapacityMethod.Headroom.Value,
				HeadroomFormula:             "1-(acceptedPeak/safeCapacity)",
				RequiredRoundedSafeCapacity: 143,
				PassRule:                    "tested-passing-low-gte-143",
				IncompleteWhen:              "synthetic bracket remains unresolved",
			},
		},
		RequiredRoles: []string{"product-owner", "database-operator", "platform-owner"},
		ApprovalTiming: RF3DecisionPolicyTimingV1{
			ExternalMinimum:    "2026-01-01T00:00:00Z",
			ProfilesFrozenAt:   "2026-01-02T00:00:00Z",
			EffectiveNotBefore: "2026-01-02T00:00:00Z",
		},
	}
}

func syntheticRF3ReviewManifest(policy RF3DecisionPolicyV1) RF3ReviewManifestV1 {
	documents := []RF3ReviewManifestDocumentV1{
		{
			Path:   "codetask/synthetic/workload.json",
			Name:   "workload.json",
			SHA256: strings.Repeat("1", 64),
		},
		{
			Path:   "codetask/synthetic/topology.json",
			Name:   "topology.json",
			SHA256: strings.Repeat("2", 64),
		},
		{
			Path:   "codetask/synthetic/policy-review.json",
			Name:   "policy.json",
			SHA256: strings.Repeat("3", 64),
		},
	}
	return RF3ReviewManifestV1{
		Status:         "proposed",
		ProposalSHA256: policy.SelectedPreparation.SHA256,
		Documents:      documents,
		Decisions: []string{
			"product owner decision",
			"database operator decision",
			"platform owner decision",
		},
	}
}
