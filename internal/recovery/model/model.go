// Package model defines immutable recovery governance contracts.
package model

import "time"

type (
	ScopeKind      string
	CanonicalPath  string
	RecoveryBranch string
)

const (
	ScopeKindEnvironment            ScopeKind      = "environment"
	ScopeKindTenant                 ScopeKind      = "tenant"
	CanonicalPathClusterBackupSetV1 CanonicalPath  = "cluster-backup-set-v1"
	CanonicalPathDirectoryArchiveV1 CanonicalPath  = "directory-archive-v1"
	CanonicalPathTableRestoreV1     CanonicalPath  = "table-restore-v1"
	RecoveryBranchRollback          RecoveryBranch = "rollback"
	RecoveryBranchForward           RecoveryBranch = "forward"
)

// RecoveryContract is the complete REC-GOV aggregate.
type RecoveryContract struct {
	ContractID         string                `json:"contractId"`
	Revision           uint64                `json:"revision"`
	SHA256             string                `json:"sha256"`
	ApprovedAt         time.Time             `json:"approvedAt"`
	ApprovedBy         []string              `json:"approvedBy"`
	ScopeKind          ScopeKind             `json:"scopeKind"`
	ScopeID            string                `json:"scopeId"`
	CanonicalPath      CanonicalPath         `json:"canonicalPath"`
	Objectives         RecoveryObjectives    `json:"objectives"`
	Measurement        MeasurementBoundaries `json:"measurement"`
	Workload           Workload              `json:"workload"`
	RequiredChecks     []string              `json:"requiredChecks"`
	Retention          RetentionPolicy       `json:"retention"`
	ManifestVersions   []uint32              `json:"manifestVersions"`
	SupportedVersions  SupportedVersions     `json:"supportedVersions"`
	Lease              LeasePolicy           `json:"lease"`
	RepositoryPolicyID string                `json:"repositoryPolicyId"`
	Runbook            RunbookIdentity       `json:"runbook"`

	// approvedAtRaw preserves the hash-bound wire spelling loaded from disk.
	approvedAtRaw string
}

// ReleaseIdentity binds a version to immutable build evidence.
type ReleaseIdentity struct {
	Version       string `json:"version"`
	ImageDigest   string `json:"imageDigest"`
	SourceCommit  string `json:"sourceCommit"`
	ProvenanceURI string `json:"provenanceURI"`
}

// ContractRef identifies one validated contract revision.
type ContractRef struct {
	ID       string `json:"id"`
	Revision uint64 `json:"revision"`
	SHA256   string `json:"sha256"`
}
type RecoveryObjectives struct {
	RPOSeconds       uint64 `json:"rpoSeconds"`
	RTOSeconds       uint64 `json:"rtoSeconds"`
	PreflightSeconds uint64 `json:"preflightSeconds"`
}
type MeasurementBoundaries struct {
	DeclarationEvent   string `json:"declarationEvent"`
	RecoveryPointEvent string `json:"recoveryPointEvent"`
	ReadyEvent         string `json:"readyEvent"`
	ClockSource        string `json:"clockSource"`
}
type Workload struct {
	LogicalBytes      uint64 `json:"logicalBytes"`
	Shards            uint32 `json:"shards"`
	ReplicationFactor uint32 `json:"replicationFactor"`
	WritesPerSecond   uint64 `json:"writesPerSecond"`
	ReadsPerSecond    uint64 `json:"readsPerSecond"`
}
type RetentionPolicy struct {
	MinimumAgeSeconds    uint64 `json:"minimumAgeSeconds"`
	MinimumQualifiedSets uint32 `json:"minimumQualifiedSets"`
	EvidenceSeconds      uint64 `json:"evidenceSeconds"`
}
type SupportedVersions struct {
	Oldest         ReleaseIdentity `json:"oldest"`
	Candidate      ReleaseIdentity `json:"candidate"`
	RecoveryBranch RecoveryBranch  `json:"recoveryBranch"`
}
type LeasePolicy struct {
	DurationSeconds    uint64 `json:"durationSeconds"`
	RenewBeforeSeconds uint64 `json:"renewBeforeSeconds"`
}
type RunbookIdentity struct {
	Revision string `json:"revision"`
	SHA256   string `json:"sha256"`
}

// ValidationError is a stable machine-readable contract failure.
type ValidationError struct {
	Code   string
	Reason string
	Field  string
}

func (err *ValidationError) Error() string {
	return err.Code + ": " + err.Reason + " (" + err.Field + ")"
}

func validationError(code, reason, field string) error {
	return &ValidationError{Code: code, Reason: reason, Field: field}
}
