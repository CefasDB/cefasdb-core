package model

import (
	"bytes"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

type (
	contractFields RecoveryContract
	contractWire   struct {
		contractFields
		ApprovedAt *string `json:"approvedAt"`
		Warning    *string `json:"warning"`
	}
)

func decodeContractWire(raw []byte) (contractWire, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire contractWire
	if err := decoder.Decode(&wire); err != nil {
		return contractWire{}, validationError("INVALID_CONTRACT", "INVALID_JSON", "$")
	}
	var root map[string]json.RawMessage
	_ = json.Unmarshal(raw, &root)
	if _, warningPresent := root["warning"]; warningPresent {
		if _, present := root["approvedAt"]; present || len(wire.ApprovedBy) != 0 {
			return contractWire{}, validationError("INVALID_CONTRACT", "INVALID_WARNING", "warning")
		}
	}
	return wire, nil
}

// LoadContract reads, hash-verifies, and validates one recovery contract.
func LoadContract(path string) (RecoveryContract, ContractRef, error) {
	// #nosec G304 -- selecting the contract file is this API's explicit caller-controlled operation.
	file, err := os.Open(path)
	if err != nil {
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "READ_FAILED", "path")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "READ_FAILED", "path")
	}
	if info.Size() > maxContractBytes {
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "INPUT_TOO_LARGE", "$")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxContractBytes+1))
	if err != nil {
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "READ_FAILED", "path")
	}
	if len(raw) > maxContractBytes {
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "INPUT_TOO_LARGE", "$")
	}
	_, digest, err := canonicalizeContractJSON(raw)
	if err != nil {
		return RecoveryContract{}, ContractRef{}, err
	}
	wire, err := decodeContractWire(raw)
	if err != nil {
		return RecoveryContract{}, ContractRef{}, err
	}
	switch {
	case wire.SHA256 == "":
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "MISSING_FIELD", "sha256")
	case !validDigest(wire.SHA256):
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "INVALID_HASH", "sha256")
	case wire.SHA256 != digest:
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "HASH_MISMATCH", "sha256")
	case wire.ApprovedAt == nil || *wire.ApprovedAt == "":
		return RecoveryContract{}, ContractRef{}, validationError("UNAPPROVED_CONTRACT", "MISSING_APPROVED_AT", "approvedAt")
	case len(wire.ApprovedBy) == 0:
		return RecoveryContract{}, ContractRef{}, validationError("UNAPPROVED_CONTRACT", "EMPTY_APPROVED_BY", "approvedBy")
	}
	approvedAt, parseErr := time.Parse(time.RFC3339Nano, *wire.ApprovedAt)
	if parseErr != nil {
		return RecoveryContract{}, ContractRef{}, validationError("INVALID_CONTRACT", "INVALID_APPROVED_AT", "approvedAt")
	}
	contract := wire.toContract(approvedAt)
	if err := contract.Validate(); err != nil {
		return RecoveryContract{}, ContractRef{}, err
	}
	ref := ContractRef{ID: contract.ContractID, Revision: contract.Revision, SHA256: contract.SHA256}
	return contract, ref, nil
}

func (wire contractWire) toContract(approvedAt time.Time) RecoveryContract {
	contract := RecoveryContract(wire.contractFields)
	contract.ApprovedAt = approvedAt
	contract.approvedAtRaw = *wire.ApprovedAt
	contract.ApprovedBy = append([]string(nil), wire.ApprovedBy...)
	contract.RequiredChecks = append([]string(nil), wire.RequiredChecks...)
	contract.ManifestVersions = append([]uint32(nil), wire.ManifestVersions...)
	return contract
}

// MarshalJSON preserves the exact approvedAt spelling that LoadContract hash-verified.
func (contract RecoveryContract) MarshalJSON() ([]byte, error) {
	type wireContract RecoveryContract
	if contract.approvedAtRaw == "" {
		return json.Marshal(wireContract(contract))
	}
	approvedAt, err := time.Parse(time.RFC3339Nano, contract.approvedAtRaw)
	if err != nil || !approvedAt.Equal(contract.ApprovedAt) {
		return json.Marshal(wireContract(contract))
	}
	return json.Marshal(struct {
		wireContract
		ApprovedAt string `json:"approvedAt"`
	}{
		wireContract: wireContract(contract),
		ApprovedAt:   contract.approvedAtRaw,
	})
}

func canonicalContractDigest(contract RecoveryContract) (string, error) {
	raw, err := json.Marshal(contract)
	if err != nil {
		return "", validationError("INVALID_CONTRACT", "INVALID_APPROVED_AT", "approvedAt")
	}
	_, digest, err := canonicalizeContractJSON(raw)
	return digest, err
}

// Validate enforces every RecoveryContract invariant in deterministic order.
func (contract RecoveryContract) Validate() error {
	switch {
	case !validDigest(contract.SHA256):
		return validationError("INVALID_CONTRACT", "INVALID_HASH", "sha256")
	case placeholderDigest(contract.SHA256):
		return validationError("INVALID_CONTRACT", "PLACEHOLDER_VALUE", "sha256")
	case contract.ApprovedAt.IsZero():
		return validationError("UNAPPROVED_CONTRACT", "MISSING_APPROVED_AT", "approvedAt")
	case len(contract.ApprovedBy) == 0:
		return validationError("UNAPPROVED_CONTRACT", "EMPTY_APPROVED_BY", "approvedBy")
	}
	previous := ""
	for _, approver := range contract.ApprovedBy {
		if !canonicalIdentifier(approver) || placeholder(approver) || previous != "" && approver <= previous {
			return validationError("INVALID_CONTRACT", "INVALID_APPROVER_SET", "approvedBy")
		}
		previous = approver
	}
	for _, item := range []struct{ field, value string }{
		{"contractId", contract.ContractID},
		{"scopeId", contract.ScopeID},
		{"repositoryPolicyId", contract.RepositoryPolicyID},
		{"runbook.revision", contract.Runbook.Revision},
	} {
		if !canonicalIdentifier(item.value) {
			return validationError("INVALID_CONTRACT", "INVALID_IDENTIFIER", item.field)
		}
		if placeholder(item.value) {
			return validationError("INVALID_CONTRACT", "PLACEHOLDER_VALUE", item.field)
		}
	}
	switch {
	case contract.Revision == 0:
		return validationError("INVALID_CONTRACT", "INVALID_REVISION", "revision")
	case contract.ScopeKind != ScopeKindEnvironment && contract.ScopeKind != ScopeKindTenant:
		return validationError("INVALID_CONTRACT", "INVALID_ENUM", "scopeKind")
	case contract.CanonicalPath != CanonicalPathClusterBackupSetV1 &&
		contract.CanonicalPath != CanonicalPathDirectoryArchiveV1 &&
		contract.CanonicalPath != CanonicalPathTableRestoreV1:
		return validationError("INVALID_CONTRACT", "INVALID_ENUM", "canonicalPath")
	case contract.SupportedVersions.RecoveryBranch != RecoveryBranchRollback &&
		contract.SupportedVersions.RecoveryBranch != RecoveryBranchForward:
		return validationError("INVALID_CONTRACT", "INVALID_ENUM", "supportedVersions.recoveryBranch")
	}
	if field := firstZero([]uintRequirement{
		{"objectives.rpoSeconds", contract.Objectives.RPOSeconds},
		{"objectives.rtoSeconds", contract.Objectives.RTOSeconds},
		{"objectives.preflightSeconds", contract.Objectives.PreflightSeconds},
	}); field != "" {
		return validationError("INVALID_CONTRACT", "NON_POSITIVE_OBJECTIVE", field)
	}
	for _, item := range []struct{ field, got, want string }{
		{"measurement.declarationEvent", contract.Measurement.DeclarationEvent, "recovery-declared-at"},
		{"measurement.recoveryPointEvent", contract.Measurement.RecoveryPointEvent, "complete-cross-shard-recovery-point-at"},
		{"measurement.readyEvent", contract.Measurement.ReadyEvent, "ready-and-reconciled-at"},
		{"measurement.clockSource", contract.Measurement.ClockSource, "utc"},
	} {
		if item.got != item.want {
			return validationError("INVALID_CONTRACT", "INVALID_MEASUREMENT", item.field)
		}
	}
	if field := firstZero([]uintRequirement{
		{"workload.logicalBytes", contract.Workload.LogicalBytes},
		{"workload.shards", uint64(contract.Workload.Shards)},
		{"workload.replicationFactor", uint64(contract.Workload.ReplicationFactor)},
		{"workload.writesPerSecond", contract.Workload.WritesPerSecond},
		{"workload.readsPerSecond", contract.Workload.ReadsPerSecond},
	}); field != "" {
		return validationError("INVALID_CONTRACT", "NON_POSITIVE_WORKLOAD", field)
	}
	if field := firstZero([]uintRequirement{
		{"retention.minimumAgeSeconds", contract.Retention.MinimumAgeSeconds},
		{"retention.minimumQualifiedSets", uint64(contract.Retention.MinimumQualifiedSets)},
		{"retention.evidenceSeconds", contract.Retention.EvidenceSeconds},
	}); field != "" {
		return validationError("INVALID_CONTRACT", "NON_POSITIVE_RETENTION", field)
	}
	if contract.Retention.EvidenceSeconds < contract.Retention.MinimumAgeSeconds {
		return validationError("INVALID_CONTRACT", "INVALID_RETENTION", "retention.evidenceSeconds")
	}
	required := [...]string{"REC-01", "REC-02", "REC-03", "REC-04", "REC-05"}
	if len(contract.RequiredChecks) != len(required) {
		return validationError("INVALID_CONTRACT", "INVALID_CHECK_SET", "requiredChecks")
	}
	for index := range required {
		if contract.RequiredChecks[index] != required[index] {
			return validationError("INVALID_CONTRACT", "INVALID_CHECK_SET", "requiredChecks")
		}
	}
	if len(contract.ManifestVersions) == 0 {
		return validationError("INVALID_CONTRACT", "INVALID_MANIFEST_VERSION", "manifestVersions")
	}
	for index, version := range contract.ManifestVersions {
		if version > 1 || index > 0 && version <= contract.ManifestVersions[index-1] {
			return validationError("INVALID_CONTRACT", "INVALID_MANIFEST_VERSION", "manifestVersions")
		}
	}
	if err := validateReleaseAt(contract.SupportedVersions.Oldest, "supportedVersions.oldest"); err != nil {
		return err
	}
	if err := validateReleaseAt(contract.SupportedVersions.Candidate, "supportedVersions.candidate"); err != nil {
		return err
	}
	if contract.SupportedVersions.Oldest == contract.SupportedVersions.Candidate {
		return validationError("INVALID_CONTRACT", "IDENTICAL_RELEASES", "supportedVersions")
	}
	if !validDigest(contract.Runbook.SHA256) || placeholderDigest(contract.Runbook.SHA256) {
		return validationError("INVALID_CONTRACT", "INVALID_RUNBOOK_HASH", "runbook.sha256")
	}
	if contract.Lease.DurationSeconds == 0 || contract.Lease.RenewBeforeSeconds == 0 ||
		contract.Lease.RenewBeforeSeconds >= contract.Lease.DurationSeconds {
		return validationError("INVALID_CONTRACT", "INVALID_LEASE", "lease")
	}
	expectedDigest, err := canonicalContractDigest(contract)
	if err != nil {
		return err
	}
	if contract.SHA256 != expectedDigest {
		return validationError("INVALID_CONTRACT", "HASH_MISMATCH", "sha256")
	}
	return nil
}

// Validate rejects mutable or malformed release identities.
func (release ReleaseIdentity) Validate() error {
	switch {
	case release.ImageDigest == "":
		return validationError("MUTABLE_RELEASE_IDENTITY", "MISSING_IMAGE_DIGEST", "imageDigest")
	case !validDigest(release.ImageDigest):
		return validationError("INVALID_RELEASE_IDENTITY", "MALFORMED_IMAGE_DIGEST", "imageDigest")
	case placeholderDigest(release.ImageDigest):
		return validationError("INVALID_RELEASE_IDENTITY", "PLACEHOLDER_VALUE", "imageDigest")
	case strings.TrimSpace(release.Version) != release.Version || release.Version == "" || placeholder(release.Version):
		return validationError("INVALID_RELEASE_IDENTITY", "MUTABLE_VERSION", "version")
	case !commitPattern.MatchString(release.SourceCommit):
		return validationError("INVALID_RELEASE_IDENTITY", "MALFORMED_SOURCE_COMMIT", "sourceCommit")
	case placeholderCommit(release.SourceCommit):
		return validationError("INVALID_RELEASE_IDENTITY", "PLACEHOLDER_VALUE", "sourceCommit")
	case !validProvenance(release.ProvenanceURI):
		return validationError("INVALID_RELEASE_IDENTITY", "INVALID_PROVENANCE_URI", "provenanceURI")
	default:
		return nil
	}
}

type uintRequirement struct {
	field string
	value uint64
}

func firstZero(requirements []uintRequirement) string {
	for _, requirement := range requirements {
		if requirement.value == 0 {
			return requirement.field
		}
	}
	return ""
}

func validateReleaseAt(release ReleaseIdentity, prefix string) error {
	err := release.Validate()
	if err == nil {
		return nil
	}
	failure := err.(*ValidationError)
	return validationError(failure.Code, failure.Reason, prefix+"."+failure.Field)
}

var (
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	commitPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	identifierPattern = regexp.MustCompile(`^[a-z](?:[a-z0-9]|[-./@_][a-z0-9])*$`)
)

func validDigest(value string) bool { return digestPattern.MatchString(value) }
func placeholderDigest(value string) bool {
	return validDigest(value) && repeatedHex(strings.TrimPrefix(value, "sha256:"))
}

func placeholderCommit(value string) bool {
	return commitPattern.MatchString(value) && repeatedHex(value)
}

func repeatedHex(value string) bool {
	if value == "" {
		return false
	}
	return strings.Trim(value, value[:1]) == ""
}

func canonicalIdentifier(value string) bool {
	return len(value) <= 192 && identifierPattern.MatchString(value)
}

func placeholder(value string) bool {
	for _, token := range strings.FieldsFunc(strings.ToLower(value), func(char rune) bool {
		return (char < 'a' || char > 'z') && (char < '0' || char > '9')
	}) {
		switch token {
		case "unknown", "tbd", "todo", "placeholder", "example", "proposed", "draft", "pending",
			"unset", "temporary", "dummy", "fake", "sample", "changeme", "latest", "current",
			"stable", "head", "main", "master", "nightly", "snapshot", "edge":
			return true
		}
	}
	return false
}

func validProvenance(value string) bool {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.Contains(value, "#") {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" || host == "invalid" || strings.HasSuffix(host, ".invalid") {
		return false
	}
	for _, reserved := range []string{"example.com", "example.net", "example.org"} {
		if host == reserved || strings.HasSuffix(host, "."+reserved) {
			return false
		}
	}
	return true
}
