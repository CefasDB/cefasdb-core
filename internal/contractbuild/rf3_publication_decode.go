package contractbuild

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

func decodeRF3PublishedDocuments(
	sources [3]rf3acceptance.Source,
	publicationSource rf3acceptance.Source,
	bundle rf3acceptance.Bundle,
) (RF3PublishedAcceptanceV1, []RF3ProtectedEvidence, error) {
	var acceptance RF3AcceptanceDocumentV1
	if err := decodeStrictRF3Document(sources[0].Bytes(), &acceptance); err != nil {
		return RF3PublishedAcceptanceV1{}, nil, fmt.Errorf("decode acceptance projection: %w", err)
	}
	var workload RF3WorkloadDocumentV1
	if err := decodeStrictRF3Document(sources[1].Bytes(), &workload); err != nil {
		return RF3PublishedAcceptanceV1{}, nil, fmt.Errorf("decode workload projection: %w", err)
	}
	var publication RF3PublicationV1
	if err := decodeStrictRF3Document(publicationSource.Bytes(), &publication); err != nil {
		return RF3PublishedAcceptanceV1{}, nil, fmt.Errorf("decode publication: %w", err)
	}
	publication.Digest = publicationSource.Digest().String()
	evidence, err := validateRF3Publication(publication, acceptance, bundle, sources)
	if err != nil {
		return RF3PublishedAcceptanceV1{}, nil, err
	}
	return RF3PublishedAcceptanceV1{
		Bundle:      bundle,
		Publication: publication,
		Acceptance:  acceptance,
		Workload:    workload,
	}, evidence, nil
}

func decodeStrictRF3Document(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing json.RawMessage
	switch err := decoder.Decode(&trailing); {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return fmt.Errorf("decode trailing JSON: %w", err)
	default:
		return fmt.Errorf("trailing JSON document")
	}
}

func validateRF3Publication(
	publication RF3PublicationV1,
	acceptance RF3AcceptanceDocumentV1,
	bundle rf3acceptance.Bundle,
	sources [3]rf3acceptance.Source,
) ([]RF3ProtectedEvidence, error) {
	if publication.DocumentType != rf3PublicationDocumentType ||
		publication.SchemaVersion != "1.0" ||
		publication.ContractID != bundle.Acceptance().ID().String() {
		return nil, fmt.Errorf("publication identity is invalid")
	}
	if err := validateRF3PublishedArtifacts(publication, bundle, sources); err != nil {
		return nil, err
	}
	if publication.Review.DecisionPolicy.PolicyID != rf3DecisionPolicyID {
		return nil, fmt.Errorf("publication decision policy id is invalid")
	}
	if err := validateRF3AttemptSemantics(publication.AttemptSemantics); err != nil {
		return nil, err
	}
	if err := validateRF3PlatformInputs(publication.PlatformInputs, bundle); err != nil {
		return nil, err
	}
	return buildRF3ProtectedEvidence(publication, acceptance)
}

func validateRF3PublishedArtifacts(
	publication RF3PublicationV1,
	bundle rf3acceptance.Bundle,
	sources [3]rf3acceptance.Source,
) error {
	documents := []struct {
		label     string
		artifact  RF3PublicationArtifactV1
		source    rf3acceptance.Source
		profileID string
	}{
		{"root", publication.Bundle.Root, sources[0], ""},
		{"workload", publication.Bundle.Workload, sources[1], bundle.Workload().ID().String()},
		{"topology", publication.Bundle.Topology, sources[2], bundle.Topology().ID().String()},
	}
	for _, document := range documents {
		if document.artifact.Path != document.source.Path() ||
			document.artifact.ProfileID != document.profileID ||
			document.artifact.Digest != document.source.Digest().String() {
			return fmt.Errorf("publication %s artifact binding is invalid", document.label)
		}
	}
	return nil
}

func validateRF3AttemptSemantics(semantics RF3AttemptSemanticsV1) error {
	if semantics.RootField != rf3AttemptRootField ||
		semantics.EncodedUnit != rf3AttemptEncodedUnit ||
		semantics.Denominator != rf3AttemptDenominator ||
		semantics.WriteAttempt != rf3AttemptWrite ||
		semantics.PointReadAttempt != rf3AttemptPointRead ||
		semantics.FailedBatch != rf3AttemptFailedBatch {
		return fmt.Errorf("publication attempt semantics are invalid")
	}
	return nil
}

func validateRF3PlatformInputs(inputs RF3PlatformInputsV1,
	bundle rf3acceptance.Bundle) error {
	if !reflectRF3Strings(inputs.FailureDomains, bundle.Topology().FailureDomains()) ||
		inputs.StorageClass != bundle.Topology().Volume().Class {
		return fmt.Errorf("publication platform inputs do not match topology")
	}
	return nil
}

func buildRF3ProtectedEvidence(
	publication RF3PublicationV1,
	acceptance RF3AcceptanceDocumentV1,
) ([]RF3ProtectedEvidence, error) {
	result := []RF3ProtectedEvidence{
		{
			Kind:    rf3ReviewManifestEvidenceKind,
			Locator: publication.Review.Manifest.Evidence,
			Digest:  publication.Review.Manifest.Digest,
		},
		{
			Kind:    rf3DecisionPolicyEvidenceKind,
			Locator: publication.Review.DecisionPolicy.Evidence,
			Digest:  publication.Review.DecisionPolicy.Digest,
			Expectation: RF3ProtectedEvidenceExpectation{
				PolicyID:          publication.Review.DecisionPolicy.PolicyID,
				ContractID:        acceptance.ContractID,
				WorkloadProfileID: acceptance.References.Workload.ProfileID,
				WorkloadDigest:    acceptance.References.Workload.Digest,
				TopologyProfileID: acceptance.References.Topology.ProfileID,
				TopologyDigest:    acceptance.References.Topology.Digest,
			},
		},
		{
			Kind:    rf3PlatformInputsEvidenceKind,
			Locator: publication.PlatformInputs.Evidence,
			Digest:  publication.PlatformInputs.Digest,
			Expectation: RF3ProtectedEvidenceExpectation{
				FailureDomains: append([]string(nil), publication.PlatformInputs.FailureDomains...),
				StorageClass:   publication.PlatformInputs.StorageClass,
			},
		},
	}
	approvals, err := buildRF3ApprovalEvidence(publication, acceptance)
	if err != nil {
		return nil, err
	}
	result = append(result, approvals...)
	if err := validateRF3EvidenceBindings(result); err != nil {
		return nil, err
	}
	return result, nil
}

func buildRF3ApprovalEvidence(
	publication RF3PublicationV1,
	acceptance RF3AcceptanceDocumentV1,
) ([]RF3ProtectedEvidence, error) {
	if len(publication.Approvals) != len(acceptance.Approvals) {
		return nil, fmt.Errorf("publication approval count is invalid")
	}
	result := make([]RF3ProtectedEvidence, len(publication.Approvals))
	for index := range publication.Approvals {
		if err := validateRF3Approval(
			publication.Approvals[index],
			acceptance.Approvals[index],
			publication.Bundle.Root.Digest,
		); err != nil {
			return nil, fmt.Errorf("publication approval[%d]: %w", index, err)
		}
		decisionIDs, err := expectedRF3DecisionIDs(publication.Approvals[index].Role)
		if err != nil {
			return nil, fmt.Errorf("publication approval[%d]: %w", index, err)
		}
		result[index] = RF3ProtectedEvidence{
			Kind:             rf3RoleApprovalEvidenceKind,
			Role:             publication.Approvals[index].Role,
			Locator:          publication.Approvals[index].Evidence,
			Digest:           publication.Approvals[index].Digest,
			AuthorityPointer: publication.Approvals[index].AuthorityPointer,
			AuthorityDigest:  publication.Approvals[index].AuthorityDigest,
			Expectation: RF3ProtectedEvidenceExpectation{
				Decision:              rf3ApprovalDecision,
				PolicyID:              publication.Review.DecisionPolicy.PolicyID,
				Approver:              publication.Approvals[index].Approver,
				ApprovedAt:            publication.Approvals[index].ApprovedAt,
				ContractID:            publication.Approvals[index].ContractID,
				WorkloadProfileID:     publication.Approvals[index].WorkloadProfileID,
				WorkloadDigest:        publication.Approvals[index].WorkloadDigest,
				TopologyProfileID:     publication.Approvals[index].TopologyProfileID,
				TopologyDigest:        publication.Approvals[index].TopologyDigest,
				ApplicableDecisionIDs: decisionIDs,
				ReviewManifestDigest:  publication.Review.Manifest.Digest,
			},
		}
	}
	return result, nil
}

func validateRF3Approval(
	publication RF3PublicationApprovalV1,
	acceptance RF3ApprovalV1,
	rootDigest string,
) error {
	if publication.Role != acceptance.Role ||
		publication.Approver != acceptance.Approver ||
		publication.ApprovedAt != acceptance.ApprovedAt ||
		publication.ContractID != acceptance.ContractID ||
		publication.RootDigest != rootDigest {
		return fmt.Errorf("identity or root binding does not match acceptance root")
	}
	if publication.WorkloadProfileID != acceptance.WorkloadProfileID ||
		publication.WorkloadDigest != acceptance.WorkloadDigest ||
		publication.TopologyProfileID != acceptance.TopologyProfileID ||
		publication.TopologyDigest != acceptance.TopologyDigest {
		return fmt.Errorf("profile binding does not match acceptance root")
	}
	return nil
}

func reflectRF3Strings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
