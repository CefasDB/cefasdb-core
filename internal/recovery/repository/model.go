// Package repository defines provider-neutral immutable BackupSet persistence.
package repository

import (
	"errors"
	"io"
	"time"

	recoverymodel "github.com/CefasDb/cefasdb/internal/recovery/model"
)

const (
	ManifestSchemaVersion uint32 = 1

	MediaTypeShard    = "application/vnd.cefasdb.shard+tar+zstd"
	MediaTypeCatalog  = "application/vnd.cefasdb.catalog+json"
	MediaTypeManifest = "application/vnd.cefasdb.backup-manifest.v1+json"
)

type LifecycleState string

const (
	LifecycleReserved    LifecycleState = "reserved"
	LifecycleUploading   LifecycleState = "uploading"
	LifecyclePublished   LifecycleState = "published"
	LifecycleDeleting    LifecycleState = "deleting"
	LifecycleQuarantined LifecycleState = "quarantined"
	LifecycleDeleted     LifecycleState = "deleted"
)

type ErrorCode string

const (
	CodeReservationConflict      ErrorCode = "RESERVATION_CONFLICT"
	CodeRepositoryDenied         ErrorCode = "REPOSITORY_DENIED"
	CodePartialPublication       ErrorCode = "PARTIAL_PUBLICATION"
	CodeDigestMismatch           ErrorCode = "DIGEST_MISMATCH"
	CodeCleanupIncomplete        ErrorCode = "CLEANUP_INCOMPLETE"
	CodeIncompleteShardCoverage  ErrorCode = "INCOMPLETE_SHARD_COVERAGE"
	CodePlacementChanged         ErrorCode = "PLACEMENT_CHANGED"
	CodeBackupIDConflict         ErrorCode = "BACKUP_ID_CONFLICT"
	CodePinConflict              ErrorCode = "PIN_CONFLICT"
	CodeUnsafeRetention          ErrorCode = "UNSAFE_RETENTION"
	CodeDeletePartial            ErrorCode = "DELETE_PARTIAL"
	CodeLastQualifiedSet         ErrorCode = "LAST_QUALIFIED_SET"
)

// RepositoryError is a stable machine-readable repository failure.
type RepositoryError struct {
	Code      ErrorCode
	Operation string
	ScopeID   string
	BackupID  string
	Reason    string
	Cause     error
}

func (err *RepositoryError) Error() string {
	if err == nil {
		return ""
	}
	message := string(err.Code)
	if err.Operation != "" {
		message += ": " + err.Operation
	}
	if err.Reason != "" {
		message += ": " + err.Reason
	}
	return message
}

func (err *RepositoryError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

func (err *RepositoryError) Is(target error) bool {
	other, ok := target.(*RepositoryError)
	return ok && (other.Code == "" || err.Code == other.Code)
}

// IsCode reports whether err contains the requested stable repository code.
func IsCode(err error, code ErrorCode) bool {
	var failure *RepositoryError
	return errors.As(err, &failure) && failure.Code == code
}

type ScheduleWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type RecoveryPoint struct {
	BarrierID           string       `json:"barrierId"`
	SourceEnvironmentID string       `json:"sourceEnvironmentId"`
	PlacementEpoch      uint64       `json:"placementEpoch"`
	AdmittedAt          time.Time    `json:"admittedAt"`
	RecoveryPointAt     time.Time    `json:"recoveryPointAt"`
	Shards              []ShardPoint `json:"shards"`
}

type ShardPoint struct {
	ShardID         uint32 `json:"shardId"`
	PlacementEpoch  uint64 `json:"placementEpoch"`
	ReplicaID       string `json:"replicaId"`
	RaftAppliedIndex uint64 `json:"raftAppliedIndex"`
	ChangeIndex      uint64 `json:"changeIndex"`
}

type ObjectRef struct {
	ObjectID  string `json:"objectId"`
	SHA256    string `json:"sha256"`
	SizeBytes uint64 `json:"sizeBytes"`
	MediaType string `json:"mediaType"`
}

type TableStat struct {
	Table   string `json:"table"`
	Rows    uint64 `json:"rows"`
	FNV1A64 string `json:"fnv1a64"`
}

type ShardArtifact struct {
	ShardID        uint32      `json:"shardId"`
	PlacementEpoch uint64      `json:"placementEpoch"`
	ReplicaID      string      `json:"replicaId"`
	Object         ObjectRef   `json:"object"`
	RowStats       []TableStat `json:"rowStats"`
}

// BackupSet is the immutable BACKUP-SET-V1 aggregate.
type BackupSet struct {
	SchemaVersion       uint32                        `json:"schemaVersion"`
	ScopeID              string                        `json:"scopeId"`
	BackupID             string                        `json:"backupId"`
	ContractRef          recoverymodel.ContractRef     `json:"contractRef"`
	SourceEnvironmentID  string                        `json:"sourceEnvironmentId"`
	SourceRelease        recoverymodel.ReleaseIdentity `json:"sourceRelease"`
	ScheduleWindow       ScheduleWindow                `json:"scheduleWindow"`
	RecoveryPoint        RecoveryPoint                 `json:"recoveryPoint"`
	ExpectedShardIDs     []uint32                      `json:"expectedShardIds"`
	Artifacts            []ShardArtifact               `json:"artifacts"`
	CatalogObject        ObjectRef                     `json:"catalogObject"`
	PublishedAt          time.Time                     `json:"publishedAt"`
	ManifestSHA256       string                        `json:"manifestSha256,omitempty"`
}

type Reservation struct {
	ScopeID             string                    `json:"scopeId"`
	BackupID            string                    `json:"backupId"`
	SourceEnvironmentID string                    `json:"sourceEnvironmentId"`
	ContractRef         recoverymodel.ContractRef `json:"contractRef"`
	ScheduleWindow      ScheduleWindow            `json:"scheduleWindow"`
}

type ReservationResult struct {
	AttemptID string         `json:"attemptId"`
	ScopeID   string         `json:"scopeId"`
	BackupID  string         `json:"backupId"`
	State     LifecycleState `json:"state"`
	Created   bool           `json:"created"`
}

type ShardUpload struct {
	AttemptID string
	ShardID   uint32
	SHA256    string
	SizeBytes uint64
	MediaType string
	Body      io.Reader
}

type CatalogUpload struct {
	AttemptID string
	SHA256    string
	SizeBytes uint64
	MediaType string
	Body      io.Reader
}

type ManifestRef struct {
	ScopeID        string    `json:"scopeId"`
	BackupID       string    `json:"backupId"`
	ObjectID       string    `json:"objectId"`
	ManifestSHA256 string    `json:"manifestSha256"`
	ObjectSHA256   string    `json:"objectSha256"`
	SizeBytes      uint64    `json:"sizeBytes"`
	MediaType      string    `json:"mediaType"`
	PublishedAt    time.Time `json:"publishedAt"`
}

type ListResult struct {
	Items         []ManifestRef `json:"items"`
	NextPageToken string        `json:"nextPageToken,omitempty"`
}

type CleanupReport struct {
	AttemptID   string         `json:"attemptId"`
	ScopeID     string         `json:"scopeId"`
	BackupID    string         `json:"backupId"`
	State       LifecycleState `json:"state"`
	DeletedKeys []string       `json:"deletedKeys"`
	ResidualKeys []string      `json:"residualKeys"`
	Reason      string         `json:"reason,omitempty"`
}

type Pin struct {
	ScopeID   string    `json:"scopeId"`
	BackupID  string    `json:"backupId"`
	RunID     string    `json:"runId"`
	ExpiresAt time.Time `json:"expiresAt"`
	CreatedAt time.Time `json:"createdAt"`
}

type Tombstone struct {
	ScopeID        string    `json:"scopeId"`
	BackupID       string    `json:"backupId"`
	Reason         string    `json:"reason"`
	ManifestSHA256 string    `json:"manifestSha256,omitempty"`
	DeletedAt      time.Time `json:"deletedAt"`
}

type QualifiedSetRef struct {
	BackupID      string    `json:"backupId"`
	EvidenceSHA256 string   `json:"evidenceSha256"`
	QualifiedAt   time.Time `json:"qualifiedAt"`
}

// RetentionPolicy is a hash-bound qualification snapshot plus validated
// retention thresholds from the active RecoveryContract.
type RetentionPolicy struct {
	ScopeID               string            `json:"scopeId"`
	MinimumAgeSeconds     uint64            `json:"minimumAgeSeconds"`
	MinimumQualifiedSets  uint32            `json:"minimumQualifiedSets"`
	EvaluatedAt           time.Time         `json:"evaluatedAt"`
	QualifiedSets         []QualifiedSetRef `json:"qualifiedSets"`
}

type RetentionResult struct {
	ScopeID                  string   `json:"scopeId"`
	EvaluatedAt              time.Time `json:"evaluatedAt"`
	DryRun                   bool     `json:"dryRun"`
	WouldDelete              []string `json:"wouldDelete"`
	Deleted                  []string `json:"deleted"`
	SkippedPinned           []string `json:"skippedPinned"`
	PreservedLastQualified  []string `json:"preservedLastQualified"`
	Quarantined             []string `json:"quarantined"`
}
