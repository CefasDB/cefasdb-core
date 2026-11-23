package contractbuild

import (
	"fmt"
	"strings"
	"time"
)

func validateRF3ReviewManifest(
	binding RF3ProtectedEvidence,
	manifest RF3ReviewManifestV1,
	policy RF3DecisionPolicyV1,
) error {
	if manifest.Status == "" ||
		manifest.ProposalSHA256 != policy.SelectedPreparation.SHA256 ||
		len(manifest.Documents) == 0 ||
		len(manifest.Decisions) != len(policy.RequiredRoles) {
		return fmt.Errorf("review manifest does not match the decision policy")
	}
	seen := make(map[string]struct{}, len(manifest.Documents))
	for _, document := range manifest.Documents {
		if document.Name == "" {
			return fmt.Errorf("review manifest document name is empty")
		}
		if _, err := rf3PrefixedDigest(document.SHA256); err != nil {
			return fmt.Errorf("review manifest document %q digest: %w", document.Name, err)
		}
		if err := validateRF3EvidencePathAndDigest(
			document.Path,
			"sha256:"+document.SHA256,
		); err != nil {
			return fmt.Errorf("review manifest document %q: %w", document.Name, err)
		}
		if _, duplicate := seen[document.Path]; duplicate {
			return fmt.Errorf("review manifest document %q is duplicated", document.Path)
		}
		seen[document.Path] = struct{}{}
	}
	for _, decision := range manifest.Decisions {
		if strings.TrimSpace(decision) == "" {
			return fmt.Errorf("review manifest contains an empty decision")
		}
	}
	if binding.Kind != rf3ReviewManifestEvidenceKind {
		return fmt.Errorf("review manifest evidence kind is invalid")
	}
	return nil
}

func validateRF3PlatformInputDecision(
	binding RF3ProtectedEvidence,
	platform RF3PlatformInputDecisionV1,
	policy RF3DecisionPolicyV1,
	published RF3PublishedAcceptanceV1,
) error {
	if platform.Kind != rf3ConfigurationApprovalKind ||
		platform.TaskID != policy.TaskID ||
		platform.ProposalRevision != policy.Proposal.Revision ||
		platform.Scope != policy.ConfigurationInputApproval.Scope ||
		platform.IssuerRole != "platform-owner" {
		return fmt.Errorf("platform input decision identity is invalid")
	}
	expected := binding.Expectation
	if !reflectRF3Strings(platform.Values.FailureDomains, expected.FailureDomains) ||
		platform.Values.StorageClass != expected.StorageClass ||
		!reflectRF3Strings(
			platform.Values.FailureDomains,
			published.Publication.PlatformInputs.FailureDomains,
		) ||
		platform.Values.StorageClass != published.Publication.PlatformInputs.StorageClass {
		return fmt.Errorf("platform input decision does not match the loaded topology")
	}
	if platform.RunID == "" ||
		platform.InitiativeID == "" ||
		platform.StepID == "" ||
		platform.Issuer == "" ||
		platform.Organization == "" ||
		platform.OrganizationID == 0 ||
		platform.AuthoritySource == "" ||
		platform.Method == "" {
		return fmt.Errorf("platform input decision provenance is incomplete")
	}
	if _, err := parseRF3Timestamp("platform input approved at", platform.At); err != nil {
		return err
	}
	if err := validateRF3PlatformInputHashes(platform); err != nil {
		return err
	}
	registration := platform.OwnerRegistration
	if registration.Scope == "" ||
		registration.Team == "" ||
		registration.Assignee != platform.Issuer ||
		registration.RegisteredBy != platform.Issuer ||
		registration.OrganizationMembership == "" {
		return fmt.Errorf("platform input owner registration is invalid")
	}
	if err := validateRF3RawSHA256(registration.RegistrySHA256); err != nil {
		return fmt.Errorf("platform input owner registry digest: %w", err)
	}
	return nil
}

func validateRF3PlatformInputHashes(platform RF3PlatformInputDecisionV1) error {
	values := map[string]string{
		"proposal":      platform.ProposalSHA256,
		"manifest":      platform.ManifestSHA256,
		"candidate":     platform.CandidateSHA256,
		"configuration": platform.ConfigurationSHA256,
		"input set":     platform.InputSetSHA256,
		"task scope":    platform.TaskScopeSHA256,
	}
	if platform.ManifestPath == "" ||
		platform.CandidatePath == "" ||
		platform.ConfigurationPath == "" {
		return fmt.Errorf("platform input evidence paths are incomplete")
	}
	for label, digest := range values {
		if err := validateRF3RawSHA256(digest); err != nil {
			return fmt.Errorf("platform input %s digest: %w", label, err)
		}
	}
	return nil
}

func validateRF3ProtectedApprovalSet(
	evidence []rf3VerifiedProtectedEvidence,
	approvals []RF3ProtectedApprovalV1,
	policy RF3DecisionPolicyV1,
	platform RF3PlatformInputDecisionV1,
	manifest RF3ProtectedEvidence,
) error {
	if len(evidence) != len(approvals) ||
		len(approvals) != len(policy.RequiredRoles) {
		return fmt.Errorf("protected approval count is invalid")
	}
	roles := make([]string, len(approvals))
	var platformApprover string
	for index := range approvals {
		if err := validateRF3ProtectedApproval(
			evidence[index].binding,
			approvals[index],
			policy,
			manifest,
		); err != nil {
			return fmt.Errorf("protected approval[%d]: %w", index, err)
		}
		roles[index] = approvals[index].Role
		if approvals[index].Role == "platform-owner" {
			platformApprover = approvals[index].Approver
		}
	}
	if !reflectRF3Strings(roles, policy.RequiredRoles) {
		return fmt.Errorf("protected approval roles do not match decision policy")
	}
	if platformApprover == "" || platformApprover != platform.Issuer {
		return fmt.Errorf("platform input issuer does not match platform owner approval")
	}
	if err := validateRF3ApprovalSetIdentity(approvals); err != nil {
		return err
	}
	return nil
}

func validateRF3ProtectedApproval(
	binding RF3ProtectedEvidence,
	approval RF3ProtectedApprovalV1,
	policy RF3DecisionPolicyV1,
	manifest RF3ProtectedEvidence,
) error {
	expected := binding.Expectation
	if approval.Kind != rf3ProfileApprovalKind ||
		approval.SchemaVersion != "1.0" ||
		approval.Decision != expected.Decision ||
		approval.Role != binding.Role ||
		approval.Approver != expected.Approver ||
		approval.ApprovedAt != expected.ApprovedAt ||
		approval.ContractID != expected.ContractID {
		return fmt.Errorf("approval identity or decision does not match publication")
	}
	if err := validateRF3ProtectedApprovalProfiles(approval, expected); err != nil {
		return err
	}
	policyDigest, err := rf3DigestHex(policy.Digest)
	if err != nil {
		return fmt.Errorf("approval decision policy digest: %w", err)
	}
	if approval.DecisionPolicyID != expected.PolicyID ||
		approval.DecisionPolicyID != policy.PolicyID ||
		approval.DecisionPolicyPath != policy.Evidence ||
		approval.DecisionPolicyDigest != policyDigest {
		return fmt.Errorf("approval decision policy binding is invalid")
	}
	decisionIDs, err := expectedRF3DecisionIDs(approval.Role)
	if err != nil {
		return err
	}
	if !reflectRF3Strings(approval.ApplicableDecisionIDs, decisionIDs) ||
		!reflectRF3Strings(approval.ApplicableDecisionIDs, expected.ApplicableDecisionIDs) {
		return fmt.Errorf("approval applicable decision ids are invalid")
	}
	manifestDigest, err := rf3DigestHex(manifest.Digest)
	if err != nil {
		return err
	}
	if approval.ManifestSHA256 != manifestDigest ||
		approval.ManifestSHA256 != digestExpectationHex(expected.ReviewManifestDigest) ||
		approval.ProposalSHA256 != policy.SelectedPreparation.SHA256 ||
		approval.TaskID != policy.TaskID {
		return fmt.Errorf("approval review binding is invalid")
	}
	if err := validateRF3ProtectedApprovalProvenance(approval, policy, manifestDigest); err != nil {
		return err
	}
	return nil
}

func validateRF3ProtectedApprovalProfiles(
	approval RF3ProtectedApprovalV1,
	expected RF3ProtectedEvidenceExpectation,
) error {
	workloadDigest, err := rf3PrefixedDigest(approval.WorkloadDigest)
	if err != nil {
		return fmt.Errorf("approval workload digest: %w", err)
	}
	topologyDigest, err := rf3PrefixedDigest(approval.TopologyDigest)
	if err != nil {
		return fmt.Errorf("approval topology digest: %w", err)
	}
	if approval.WorkloadProfileID != expected.WorkloadProfileID ||
		workloadDigest != expected.WorkloadDigest ||
		approval.TopologyProfileID != expected.TopologyProfileID ||
		topologyDigest != expected.TopologyDigest {
		return fmt.Errorf("approval profile binding does not match publication")
	}
	return nil
}

func validateRF3ProtectedApprovalProvenance(
	approval RF3ProtectedApprovalV1,
	policy RF3DecisionPolicyV1,
	manifestDigest string,
) error {
	if approval.RunID == "" ||
		approval.InitiativeID == "" ||
		approval.StepID == "" ||
		approval.TaskScopeSHA256 == "" ||
		approval.AuthoritySource == "" {
		return fmt.Errorf("approval provenance is incomplete")
	}
	if err := validateRF3RawSHA256(approval.TaskScopeSHA256); err != nil {
		return fmt.Errorf("approval task scope digest: %w", err)
	}
	approvedAt, err := parseRF3Timestamp("approval timestamp", approval.ApprovedAt)
	if err != nil {
		return err
	}
	effective, err := parseRF3Timestamp(
		"decision policy effective not before",
		policy.ApprovalTiming.EffectiveNotBefore,
	)
	if err != nil {
		return err
	}
	if approvedAt.Before(effective) {
		return fmt.Errorf("approval predates frozen-profile authorization")
	}
	expectedRecordID := "ledgerdb:codetask_documents/" +
		approval.DecisionPolicyPath +
		"#sha256=" +
		approval.DecisionPolicyDigest
	if approval.DecisionPolicyRecordID != expectedRecordID {
		return fmt.Errorf("approval decision policy record id is invalid")
	}
	return validateRF3AuthorityRecord(approval, manifestDigest)
}

func validateRF3AuthorityRecord(
	approval RF3ProtectedApprovalV1,
	manifestDigest string,
) error {
	authority := approval.AuthorityRecord
	if authority.IssuerID == 0 ||
		authority.OrganizationID == 0 ||
		authority.Repository != "CefasDB/cefasdb-core" ||
		authority.RepositoryID == 0 ||
		authority.InitiativeID != approval.InitiativeID ||
		authority.TaskID != approval.TaskID ||
		authority.Role != approval.Role ||
		authority.Team == "" ||
		authority.Registration == "" ||
		authority.OrganizationMembership == "" ||
		authority.OrganizationTeams == "" ||
		authority.Method == "" ||
		authority.Organization == "" ||
		authority.Scope == "" ||
		authority.RegisteredBy != approval.Approver ||
		authority.ManifestSHA256 != manifestDigest {
		return fmt.Errorf("approval authority record does not match role decision")
	}
	for label, digest := range map[string]string{
		"registry":       authority.RegistrySHA256,
		"team inventory": authority.TeamInventorySHA256,
	} {
		if err := validateRF3RawSHA256(digest); err != nil {
			return fmt.Errorf("approval authority %s digest: %w", label, err)
		}
	}
	return nil
}

func validateRF3ApprovalSetIdentity(approvals []RF3ProtectedApprovalV1) error {
	if len(approvals) == 0 {
		return fmt.Errorf("protected approvals are empty")
	}
	first := approvals[0]
	for _, approval := range approvals[1:] {
		if approval.RunID != first.RunID ||
			approval.InitiativeID != first.InitiativeID ||
			approval.StepID != first.StepID ||
			approval.TaskScopeSHA256 != first.TaskScopeSHA256 ||
			approval.ManifestSHA256 != first.ManifestSHA256 ||
			approval.ProposalSHA256 != first.ProposalSHA256 {
			return fmt.Errorf("protected approvals do not share one review identity")
		}
	}
	return nil
}

func applyRF3DecisionPolicy(
	acceptance *RF3AcceptanceDocumentV1,
	policy RF3DecisionPolicyV1,
) {
	variance := policy.Decisions.Variance
	acceptance.Variance.Metrics = append([]string(nil), variance.Metrics...)
	acceptance.Variance.Formula = variance.Formula
	acceptance.Variance.IncompleteWhen = append(
		[]string(nil),
		variance.IncompleteWhen...,
	)
	capacity := policy.Decisions.Capacity
	acceptance.CapacityMethod.AcceptedPeak = capacity.AcceptedPeak
	acceptance.CapacityMethod.FirstProbe = capacity.FirstProbe
	acceptance.CapacityMethod.MaximumFloorRoundedMidpoints =
		capacity.MaximumFloorRoundedMidpoints
	acceptance.CapacityMethod.HeadroomFormula = capacity.HeadroomFormula
	acceptance.CapacityMethod.RequiredRoundedSafeCapacity =
		capacity.RequiredRoundedSafeCapacity
	acceptance.CapacityMethod.PassRule = capacity.PassRule
	acceptance.CapacityMethod.IncompleteWhen = capacity.IncompleteWhen
}

func validateRF3StringSet(label string, values []string) error {
	if len(values) == 0 {
		return fmt.Errorf("%s is empty", label)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s contains an empty value", label)
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("%s contains duplicate %q", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateRF3RawSHA256(digest string) error {
	_, err := rf3PrefixedDigest(digest)
	return err
}

func parseRF3Timestamp(label string, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q: %w", label, value, err)
	}
	return parsed, nil
}

func digestExpectationHex(digest string) string {
	return strings.TrimPrefix(digest, "sha256:")
}
