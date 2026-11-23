package contractbuild

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

type rf3VerifiedProtectedEvidence struct {
	binding  RF3ProtectedEvidence
	document []byte
}

func verifyRF3ProtectedEvidence(
	ctx context.Context,
	verifier RF3ProtectedEvidenceVerifier,
	evidence []RF3ProtectedEvidence,
	published *RF3PublishedAcceptanceV1,
) error {
	if published == nil {
		return fmt.Errorf("verify RF3 protected evidence: published contract is required")
	}
	verified := make(map[string]rf3VerifiedProtectedEvidence, len(evidence))
	for _, binding := range evidence {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("verify RF3 protected evidence: %w", err)
		}
		document, err := verifier.VerifyRF3ProtectedEvidence(ctx, binding)
		if err != nil {
			return fmt.Errorf(
				"verify %s evidence %q: %w",
				binding.Kind,
				binding.Locator,
				err,
			)
		}
		if err := verifyRF3ProtectedDocument(binding, document); err != nil {
			return fmt.Errorf(
				"verify %s evidence %q: %w",
				binding.Kind,
				binding.Locator,
				err,
			)
		}
		key := rf3ProtectedEvidenceKey(binding)
		if _, duplicate := verified[key]; duplicate {
			return fmt.Errorf("verify RF3 protected evidence: duplicate %q", key)
		}
		verified[key] = rf3VerifiedProtectedEvidence{
			binding:  binding,
			document: append([]byte(nil), document...),
		}
	}
	authorization, err := decodeRF3Authorization(*published, verified)
	if err != nil {
		return err
	}
	published.Authorization = authorization
	applyRF3DecisionPolicy(&published.Acceptance, authorization.DecisionPolicy)
	return nil
}

func verifyRF3ProtectedDocument(binding RF3ProtectedEvidence, document []byte) error {
	if len(document) == 0 {
		return fmt.Errorf("resolver returned an empty document")
	}
	source, err := rf3acceptance.NewSource(binding.Locator, document)
	if err != nil {
		return err
	}
	if source.Digest().String() != binding.Digest {
		return fmt.Errorf(
			"document digest = %q, want %q",
			source.Digest().String(),
			binding.Digest,
		)
	}
	if binding.AuthorityPointer == "" {
		if binding.AuthorityDigest != "" {
			return fmt.Errorf("authority digest is present without a pointer")
		}
		return nil
	}
	if binding.AuthorityPointer != rf3AuthorityRecordPointer {
		return fmt.Errorf("authority pointer must be %q", rf3AuthorityRecordPointer)
	}
	authority, err := rf3AuthorityRecordBytes(document)
	if err != nil {
		return err
	}
	authoritySource, err := rf3acceptance.NewSource("authority-record.json", authority)
	if err != nil {
		return err
	}
	if authoritySource.Digest().String() != binding.AuthorityDigest {
		return fmt.Errorf(
			"authority digest = %q, want %q",
			authoritySource.Digest().String(),
			binding.AuthorityDigest,
		)
	}
	return nil
}

func rf3AuthorityRecordBytes(document []byte) ([]byte, error) {
	var envelope struct {
		AuthorityRecord json.RawMessage `json:"authorityRecord"`
	}
	if err := json.Unmarshal(document, &envelope); err != nil {
		return nil, fmt.Errorf("decode authority record: %w", err)
	}
	if len(envelope.AuthorityRecord) == 0 {
		return nil, fmt.Errorf("authority record is missing")
	}
	var result bytes.Buffer
	if err := json.Indent(
		&result,
		bytes.TrimSpace(envelope.AuthorityRecord),
		"",
		"  ",
	); err != nil {
		return nil, fmt.Errorf("format authority record: %w", err)
	}
	result.WriteByte('\n')
	return result.Bytes(), nil
}

func decodeRF3Authorization(
	published RF3PublishedAcceptanceV1,
	verified map[string]rf3VerifiedProtectedEvidence,
) (RF3AuthorizationV1, error) {
	manifestEvidence, err := requireRF3ProtectedEvidence(
		verified,
		rf3ReviewManifestEvidenceKind,
		"",
	)
	if err != nil {
		return RF3AuthorizationV1{}, err
	}
	policyEvidence, err := requireRF3ProtectedEvidence(
		verified,
		rf3DecisionPolicyEvidenceKind,
		"",
	)
	if err != nil {
		return RF3AuthorizationV1{}, err
	}
	platformEvidence, err := requireRF3ProtectedEvidence(
		verified,
		rf3PlatformInputsEvidenceKind,
		"",
	)
	if err != nil {
		return RF3AuthorizationV1{}, err
	}

	var manifest RF3ReviewManifestV1
	if err := decodeRF3ProtectedDocument(manifestEvidence, &manifest); err != nil {
		return RF3AuthorizationV1{}, err
	}
	var policy RF3DecisionPolicyV1
	if err := decodeRF3ProtectedDocument(policyEvidence, &policy); err != nil {
		return RF3AuthorizationV1{}, err
	}
	policy.Evidence = policyEvidence.binding.Locator
	policy.Digest = policyEvidence.binding.Digest
	var platform RF3PlatformInputDecisionV1
	if err := decodeRF3ProtectedDocument(platformEvidence, &platform); err != nil {
		return RF3AuthorizationV1{}, err
	}

	approvals := make([]RF3ProtectedApprovalV1, len(published.Publication.Approvals))
	approvalEvidence := make([]rf3VerifiedProtectedEvidence, len(approvals))
	for index, publicationApproval := range published.Publication.Approvals {
		record, err := requireRF3ProtectedEvidence(
			verified,
			rf3RoleApprovalEvidenceKind,
			publicationApproval.Role,
		)
		if err != nil {
			return RF3AuthorizationV1{}, err
		}
		if err := decodeRF3ProtectedDocument(record, &approvals[index]); err != nil {
			return RF3AuthorizationV1{}, err
		}
		approvalEvidence[index] = record
	}

	if len(verified) != rf3ProtectedEvidenceCount {
		return RF3AuthorizationV1{}, fmt.Errorf(
			"protected evidence count = %d, want %d",
			len(verified),
			rf3ProtectedEvidenceCount,
		)
	}
	if err := validateRF3DecisionPolicy(policyEvidence.binding, policy, published); err != nil {
		return RF3AuthorizationV1{}, err
	}
	if err := validateRF3ReviewManifest(manifestEvidence.binding, manifest, policy); err != nil {
		return RF3AuthorizationV1{}, err
	}
	if err := validateRF3PlatformInputDecision(
		platformEvidence.binding,
		platform,
		policy,
		published,
	); err != nil {
		return RF3AuthorizationV1{}, err
	}
	if err := validateRF3ProtectedApprovalSet(
		approvalEvidence,
		approvals,
		policy,
		platform,
		manifestEvidence.binding,
	); err != nil {
		return RF3AuthorizationV1{}, err
	}
	return RF3AuthorizationV1{
		ReviewManifest: manifest,
		DecisionPolicy: policy,
		PlatformInputs: platform,
		Approvals:      approvals,
	}, nil
}

func decodeRF3ProtectedDocument(
	evidence rf3VerifiedProtectedEvidence,
	target any,
) error {
	if err := decodeStrictRF3Document(evidence.document, target); err != nil {
		return fmt.Errorf(
			"decode %s evidence %q: %w",
			evidence.binding.Kind,
			evidence.binding.Locator,
			err,
		)
	}
	return nil
}

func requireRF3ProtectedEvidence(
	verified map[string]rf3VerifiedProtectedEvidence,
	kind string,
	role string,
) (rf3VerifiedProtectedEvidence, error) {
	key := rf3ProtectedEvidenceKey(RF3ProtectedEvidence{Kind: kind, Role: role})
	record, ok := verified[key]
	if !ok {
		return rf3VerifiedProtectedEvidence{}, fmt.Errorf(
			"protected evidence %q is missing",
			key,
		)
	}
	return record, nil
}

func rf3ProtectedEvidenceKey(binding RF3ProtectedEvidence) string {
	if binding.Role == "" {
		return binding.Kind
	}
	return binding.Kind + ":" + binding.Role
}

func validateRF3EvidenceBindings(evidence []RF3ProtectedEvidence) error {
	if len(evidence) != rf3ProtectedEvidenceCount {
		return fmt.Errorf(
			"protected evidence count = %d, want %d",
			len(evidence),
			rf3ProtectedEvidenceCount,
		)
	}
	required := map[string]struct{}{
		rf3ReviewManifestEvidenceKind:                      {},
		rf3DecisionPolicyEvidenceKind:                      {},
		rf3PlatformInputsEvidenceKind:                      {},
		rf3RoleApprovalEvidenceKind + ":product-owner":     {},
		rf3RoleApprovalEvidenceKind + ":database-operator": {},
		rf3RoleApprovalEvidenceKind + ":platform-owner":    {},
	}
	locators := make(map[string]struct{}, len(evidence))
	for _, binding := range evidence {
		if err := validateRF3EvidencePathAndDigest(binding.Locator, binding.Digest); err != nil {
			return fmt.Errorf("%s evidence: %w", binding.Kind, err)
		}
		key := rf3ProtectedEvidenceKey(binding)
		if _, ok := required[key]; !ok {
			return fmt.Errorf("protected evidence kind %q is invalid", key)
		}
		delete(required, key)
		if _, duplicate := locators[binding.Locator]; duplicate {
			return fmt.Errorf("protected evidence locator %q is duplicated", binding.Locator)
		}
		locators[binding.Locator] = struct{}{}
		if binding.Kind == rf3RoleApprovalEvidenceKind {
			if binding.AuthorityPointer != rf3AuthorityRecordPointer {
				return fmt.Errorf(
					"%s authority pointer must be %q",
					binding.Role,
					rf3AuthorityRecordPointer,
				)
			}
			if _, err := rf3acceptance.NewDigest(binding.AuthorityDigest); err != nil {
				return fmt.Errorf("%s authority digest: %w", binding.Role, err)
			}
			continue
		}
		if binding.Role != "" ||
			binding.AuthorityPointer != "" ||
			binding.AuthorityDigest != "" {
			return fmt.Errorf("%s evidence has unexpected role authority fields", binding.Kind)
		}
	}
	if len(required) != 0 {
		return fmt.Errorf("protected evidence set is incomplete")
	}
	return nil
}

func validateRF3EvidencePathAndDigest(locator, digest string) error {
	if _, err := rf3acceptance.NewSource(locator, []byte("{}")); err != nil {
		return err
	}
	if _, err := rf3acceptance.NewDigest(digest); err != nil {
		return err
	}
	return nil
}

func expectedRF3DecisionIDs(role string) ([]string, error) {
	var result []string
	switch role {
	case "product-owner":
		result = []string{
			"workload-and-serving-targets",
			"variance-tolerance",
			"headroom-target",
			"schema-v1-applicability",
		}
	case "database-operator":
		result = []string{
			"execution-controls",
			"scenario-and-recovery-semantics",
			"variance-method",
			"capacity-search",
			"schema-v1-applicability",
		}
	case "platform-owner":
		result = []string{
			"topology-resources",
			"failure-domains",
			"durable-storage-class",
			"schema-v1-applicability",
		}
	default:
		return nil, fmt.Errorf("unsupported approval role %q", role)
	}
	return result, nil
}

func rf3DigestHex(digest string) (string, error) {
	if _, err := rf3acceptance.NewDigest(digest); err != nil {
		return "", err
	}
	return strings.TrimPrefix(digest, "sha256:"), nil
}

func rf3PrefixedDigest(digest string) (string, error) {
	candidate := "sha256:" + digest
	if _, err := rf3acceptance.NewDigest(candidate); err != nil {
		return "", err
	}
	return candidate, nil
}
