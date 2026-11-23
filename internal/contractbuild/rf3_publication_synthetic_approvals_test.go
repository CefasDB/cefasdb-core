package contractbuild

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

func syntheticRF3Approvals(
	t testing.TB,
	acceptance RF3AcceptanceDocumentV1,
	rootDigest string,
	policy RF3DecisionPolicyV1,
	policyDigest string,
	manifestDigest string,
) ([]map[string]any, map[string][]byte) {
	t.Helper()
	result := make([]map[string]any, len(acceptance.Approvals))
	documents := make(map[string][]byte, len(acceptance.Approvals))
	for index, embedded := range acceptance.Approvals {
		locator := "codetask/synthetic/" + embedded.Role + ".json"
		approval := syntheticRF3Approval(
			t,
			embedded,
			policy,
			policyDigest,
			manifestDigest,
		)
		document := marshalRF3ProtectedDocument(t, approval)
		documents[locator] = document
		result[index] = syntheticRF3PublicationApproval(
			t,
			embedded,
			rootDigest,
			locator,
			document,
			approval.AuthorityRecord,
		)
	}
	return result, documents
}

func syntheticRF3Approval(
	t testing.TB,
	embedded RF3ApprovalV1,
	policy RF3DecisionPolicyV1,
	policyDigest string,
	manifestDigest string,
) RF3ProtectedApprovalV1 {
	t.Helper()
	policyDigestHex, err := rf3DigestHex(policyDigest)
	if err != nil {
		t.Fatalf("synthetic policy digest: %v", err)
	}
	manifestDigestHex, err := rf3DigestHex(manifestDigest)
	if err != nil {
		t.Fatalf("synthetic manifest digest: %v", err)
	}
	workloadDigest, err := rf3DigestHex(embedded.WorkloadDigest)
	if err != nil {
		t.Fatalf("synthetic approval workload digest: %v", err)
	}
	topologyDigest, err := rf3DigestHex(embedded.TopologyDigest)
	if err != nil {
		t.Fatalf("synthetic approval topology digest: %v", err)
	}
	decisionIDs, err := expectedRF3DecisionIDs(embedded.Role)
	if err != nil {
		t.Fatalf("synthetic approval decisions: %v", err)
	}
	authority := RF3AuthorityRecordV1{
		IssuerID:               uint64(100 + len(embedded.Role)),
		OrganizationID:         1,
		Repository:             "CefasDB/cefasdb-core",
		RepositoryID:           1,
		InitiativeID:           syntheticRF3InitiativeID,
		TaskID:                 rf3ApprovalTaskID,
		RegistrySHA256:         strings.Repeat("c", 64),
		Role:                   embedded.Role,
		Team:                   "team-cefasdb",
		Registration:           "direct-final-review-only",
		OrganizationMembership: "https://example.test/membership",
		OrganizationTeams:      "https://example.test/teams",
		TeamInventorySHA256:    strings.Repeat("d", 64),
		Method:                 "direct-role-registration",
		Organization:           "CefasDB",
		Scope:                  "this-final-profile-review",
		RegisteredBy:           embedded.Approver,
		ManifestSHA256:         manifestDigestHex,
	}
	return RF3ProtectedApprovalV1{
		Kind:                 rf3ProfileApprovalKind,
		SchemaVersion:        "1.0",
		Decision:             rf3ApprovalDecision,
		Approver:             embedded.Approver,
		ApprovedAt:           embedded.ApprovedAt,
		ContractID:           embedded.ContractID,
		WorkloadProfileID:    embedded.WorkloadProfileID,
		WorkloadDigest:       workloadDigest,
		TopologyProfileID:    embedded.TopologyProfileID,
		TopologyDigest:       topologyDigest,
		DecisionPolicyID:     policy.PolicyID,
		DecisionPolicyPath:   syntheticRF3PolicyLocator,
		DecisionPolicyDigest: policyDigestHex,
		DecisionPolicyRecordID: "ledgerdb:codetask_documents/" +
			syntheticRF3PolicyLocator +
			"#sha256=" +
			policyDigestHex,
		Role:                  embedded.Role,
		ApplicableDecisionIDs: decisionIDs,
		RunID:                 syntheticRF3RunID,
		TaskID:                rf3ApprovalTaskID,
		InitiativeID:          syntheticRF3InitiativeID,
		StepID:                syntheticRF3StepID,
		TaskScopeSHA256:       strings.Repeat("e", 64),
		ManifestSHA256:        manifestDigestHex,
		ProposalSHA256:        policy.SelectedPreparation.SHA256,
		AuthoritySource:       "codetask/synthetic/initiative.yaml",
		AuthorityRecord:       authority,
	}
}

func syntheticRF3PublicationApproval(
	t testing.TB,
	embedded RF3ApprovalV1,
	rootDigest string,
	locator string,
	document []byte,
	authority RF3AuthorityRecordV1,
) map[string]any {
	t.Helper()
	authorityDocument := marshalRF3ProtectedDocument(t, authority)
	return map[string]any{
		"role":             embedded.Role,
		"approver":         embedded.Approver,
		"approvedAt":       embedded.ApprovedAt,
		"evidence":         locator,
		"digest":           syntheticRF3Digest(t, locator, document),
		"authorityPointer": rf3AuthorityRecordPointer,
		"authorityDigest": syntheticRF3Digest(
			t,
			"authority-record.json",
			authorityDocument,
		),
		"contractId":        embedded.ContractID,
		"rootDigest":        rootDigest,
		"workloadProfileId": embedded.WorkloadProfileID,
		"workloadDigest":    embedded.WorkloadDigest,
		"topologyProfileId": embedded.TopologyProfileID,
		"topologyDigest":    embedded.TopologyDigest,
	}
}

func tamperSyntheticRF3ApprovalDecision(
	t testing.TB,
	publicationPath string,
	verifier *rf3EvidenceVerifier,
) {
	t.Helper()
	locator := "codetask/synthetic/product-owner.json"
	original := string(verifier.remaining[locator])
	document := []byte(strings.Replace(
		original,
		`"decision": "approve"`,
		`"decision": "reject"`,
		1,
	))
	if string(document) == original {
		t.Fatal("synthetic approval decision was not replaced")
	}
	verifier.remaining[locator] = document

	data, err := os.ReadFile(publicationPath)
	if err != nil {
		t.Fatalf("read synthetic publication: %v", err)
	}
	var publication map[string]any
	if err := json.Unmarshal(data, &publication); err != nil {
		t.Fatalf("decode synthetic publication: %v", err)
	}
	approvals := publication["approvals"].([]any)
	product := approvals[0].(map[string]any)
	product["digest"] = syntheticRF3Digest(t, locator, document)
	if err := os.WriteFile(
		publicationPath,
		marshalRF3ProtectedDocument(t, publication),
		0o600,
	); err != nil {
		t.Fatalf("write tampered synthetic publication: %v", err)
	}
}

func marshalRF3ProtectedDocument(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("encode protected document: %v", err)
	}
	return append(data, '\n')
}
func syntheticRF3Digest(t testing.TB, locator string, document []byte) string {
	t.Helper()
	source, err := rf3acceptance.NewSource(locator, document)
	if err != nil {
		t.Fatalf("construct protected source %q: %v", locator, err)
	}
	return source.Digest().String()
}
