// Package evidence defines immutable terminal recovery evidence.
package evidence

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/CefasDb/cefasdb/internal/recovery/model"
)

const (
	SchemaVersion     uint32 = 1
	MaxCanonicalBytes        = 1 << 20
	MaxFindings              = 1000
)

type QualificationStatus string

const (
	StatusPass QualificationStatus = "PASS"
	StatusNoGo QualificationStatus = "NO_GO"
)

// FindingReason identifies a reviewed summary that cannot carry row or credential text.
type FindingReason string

const (
	FindingReasonCheckFailed     FindingReason = "CHECK_FAILED"
	FindingReasonCheckMissing    FindingReason = "CHECK_MISSING"
	FindingReasonEvidenceMissing FindingReason = "EVIDENCE_MISSING"
	FindingReasonObjectiveMiss   FindingReason = "OBJECTIVE_EXCEEDED"
	FindingReasonMutationUnknown FindingReason = "MUTATION_OUTCOME_UNKNOWN"
	FindingReasonOperatorBlocked FindingReason = "OPERATOR_BLOCKED"
)

type Finding struct {
	CheckID     string        `json:"checkId"`
	Owner       string        `json:"owner"`
	Reason      FindingReason `json:"reason"`
	EvidenceRef string        `json:"evidenceRef"`
}

type QualificationRecord struct {
	SchemaVersion           uint32                `json:"schemaVersion"`
	ScopeID                 string                `json:"scopeId"`
	RunID                   string                `json:"runId"`
	RunRevision             uint64                `json:"runRevision"`
	ContractRef             model.ContractRef     `json:"contractRef"`
	BackupSetRef            string                `json:"backupSetRef"`
	SourceRelease           model.ReleaseIdentity `json:"sourceRelease"`
	CandidateRelease        model.ReleaseIdentity `json:"candidateRelease"`
	TargetEnvironmentID     string                `json:"targetEnvironmentId"`
	TargetProofRef          string                `json:"targetProofRef"`
	RestoreRunRef           string                `json:"restoreRunRef"`
	ReconciliationReportRef string                `json:"reconciliationReportRef"`
	UpgradeRunRef           string                `json:"upgradeRunRef"`
	Status                  QualificationStatus   `json:"status"`
	TargetMutated           bool                  `json:"targetMutated"`
	MeasuredRPOSeconds      uint64                `json:"measuredRpoSeconds"`
	MeasuredRTOSeconds      uint64                `json:"measuredRtoSeconds"`
	OperatorID              string                `json:"operatorId"`
	RunbookRevision         string                `json:"runbookRevision"`
	RunbookSHA256           string                `json:"runbookSha256"`
	DiagnosticsRef          string                `json:"diagnosticsRef"`
	Findings                []Finding             `json:"findings"`
	StartedAt               time.Time             `json:"startedAt"`
	CompletedAt             time.Time             `json:"completedAt"`
	SHA256                  string                `json:"sha256"`
}

type EvidenceRef struct {
	Key       string    `json:"key"`
	SHA256    string    `json:"sha256"`
	CreatedAt time.Time `json:"createdAt"`
}
type EvidencePage struct {
	Items         []QualificationRecord `json:"items"`
	NextPageToken string                `json:"nextPageToken,omitempty"`
}

type ErrorCode string

const (
	CodeEvidenceConflict        ErrorCode = "EVIDENCE_CONFLICT"
	CodeEvidenceNotDurable      ErrorCode = "EVIDENCE_NOT_DURABLE"
	CodeScopeDenied             ErrorCode = "SCOPE_DENIED"
	CodeEvidenceRedactionFailed ErrorCode = "EVIDENCE_REDACTION_FAILED"
	CodeInvalidPageToken        ErrorCode = "INVALID_PAGE_TOKEN"
	CodeInvalidPageSize         ErrorCode = "INVALID_PAGE_SIZE"
	CodeEvidenceNotFound        ErrorCode = "EVIDENCE_NOT_FOUND"
	CodeInvalidEvidence         ErrorCode = "INVALID_EVIDENCE"
	CodeEvidenceHashMismatch    ErrorCode = "EVIDENCE_HASH_MISMATCH"
)

type Error struct {
	Code      ErrorCode
	Operation string
	Field     string
	Cause     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	s := string(e.Code)
	if e.Operation != "" {
		s += ": " + e.Operation
	}
	if e.Field != "" {
		s += ": " + e.Field
	}
	return s
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && (other.Code == "" || e.Code == other.Code)
}

var (
	ErrEvidenceConflict        = &Error{Code: CodeEvidenceConflict}
	ErrEvidenceNotDurable      = &Error{Code: CodeEvidenceNotDurable}
	ErrScopeDenied             = &Error{Code: CodeScopeDenied}
	ErrEvidenceRedactionFailed = &Error{Code: CodeEvidenceRedactionFailed}
	ErrInvalidPageToken        = &Error{Code: CodeInvalidPageToken}
	ErrInvalidPageSize         = &Error{Code: CodeInvalidPageSize}
	ErrEvidenceNotFound        = &Error{Code: CodeEvidenceNotFound}
	ErrInvalidEvidence         = &Error{Code: CodeInvalidEvidence}
	ErrEvidenceHashMismatch    = &Error{Code: CodeEvidenceHashMismatch}
)

func IsCode(err error, code ErrorCode) bool {
	var failure *Error
	return errors.As(err, &failure) && failure.Code == code
}

func fail(code ErrorCode, op, field string) error {
	return &Error{Code: code, Operation: op, Field: field}
}

type EvidenceStore interface {
	Durable() bool
	PutTerminal(context.Context, QualificationRecord) (EvidenceRef, error)
	Get(context.Context, string, string) (QualificationRecord, error)
	List(context.Context, string, int, string) (EvidencePage, error)
}

var (
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._@/-]{0,191})$`)
	scopeIDPattern = regexp.MustCompile(`^[a-z](?:[a-z0-9]|[-./@_][a-z0-9])*$`)
	digestPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	refPattern     = regexp.MustCompile(`^[^\x00-\x20\x7f]+$`)
)

func index(i int) string { return strconv.Itoa(i) }
func validID(v string) bool {
	return idPattern.MatchString(v) && v != "." && v != ".." && !strings.Contains(v, "..")
}

func validScopeID(v string) bool { return len(v) <= 192 && scopeIDPattern.MatchString(v) }

func validFindingReason(reason FindingReason) bool {
	switch reason {
	case FindingReasonCheckFailed,
		FindingReasonCheckMissing,
		FindingReasonEvidenceMissing,
		FindingReasonObjectiveMiss,
		FindingReasonMutationUnknown,
		FindingReasonOperatorBlocked:
		return true
	default:
		return false
	}
}

func validRef(v string) bool {
	return len(v) <= 4096 && utf8.ValidString(v) && refPattern.MatchString(v)
}

func validTime(v time.Time) bool {
	if v.IsZero() {
		return false
	}
	_, offset := v.Zone()
	return offset == 0
}

func clone(record QualificationRecord) QualificationRecord {
	out := record
	out.StartedAt, out.CompletedAt = record.StartedAt.Round(0), record.CompletedAt.Round(0)
	if record.Findings != nil {
		out.Findings = append([]Finding{}, record.Findings...)
	}
	return out
}

func validateShape(r QualificationRecord) error {
	bad := func(field string) error { return fail(CodeInvalidEvidence, "PutTerminal", field) }
	switch {
	case r.SchemaVersion != SchemaVersion:
		return bad("schemaVersion")
	case !validScopeID(r.ScopeID):
		return bad("scopeId")
	case !validID(r.RunID):
		return bad("runId")
	case r.RunRevision == 0:
		return bad("runRevision")
	case !validID(r.ContractRef.ID) || r.ContractRef.Revision == 0 || !digestPattern.MatchString(r.ContractRef.SHA256):
		return bad("contractRef")
	case !validRef(r.BackupSetRef):
		return bad("backupSetRef")
	case !validID(r.TargetEnvironmentID):
		return bad("targetEnvironmentId")
	case !validRef(r.TargetProofRef):
		return bad("targetProofRef")
	case !validRef(r.RestoreRunRef):
		return bad("restoreRunRef")
	case !validRef(r.ReconciliationReportRef):
		return bad("reconciliationReportRef")
	case !validRef(r.UpgradeRunRef):
		return bad("upgradeRunRef")
	case r.Status != StatusPass && r.Status != StatusNoGo:
		return bad("status")
	case r.Status == StatusPass && len(r.Findings) != 0:
		return bad("findings")
	case r.Status == StatusNoGo && len(r.Findings) == 0:
		return bad("findings")
	case !validID(r.OperatorID):
		return bad("operatorId")
	case !validID(r.RunbookRevision):
		return bad("runbookRevision")
	case !digestPattern.MatchString(r.RunbookSHA256):
		return bad("runbookSha256")
	case !validRef(r.DiagnosticsRef):
		return bad("diagnosticsRef")
	case len(r.Findings) > MaxFindings:
		return bad("findings")
	case !validTime(r.StartedAt):
		return bad("startedAt")
	case !validTime(r.CompletedAt) || r.CompletedAt.Before(r.StartedAt):
		return bad("completedAt")
	}
	for i, finding := range r.Findings {
		field := "findings[" + index(i) + "]"
		if !validID(finding.CheckID) {
			return bad(field + ".checkId")
		}
		if !validID(finding.Owner) {
			return bad(field + ".owner")
		}
		if finding.Reason == "" || !utf8.ValidString(string(finding.Reason)) {
			return bad(field + ".reason")
		}
		if !validRef(finding.EvidenceRef) {
			return bad(field + ".evidenceRef")
		}
	}
	return nil
}

func canonicalQualificationRecord(record QualificationRecord) ([]byte, string, error) {
	if !allStringsUTF8(record) {
		return nil, "", fail(CodeInvalidEvidence, "canonicalQualificationRecord", "$")
	}
	raw, err := json.Marshal(clone(record))
	if err != nil {
		return nil, "", &Error{Code: CodeInvalidEvidence, Operation: "canonicalQualificationRecord", Field: "$", Cause: err}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var root map[string]any
	if err = decoder.Decode(&root); err != nil {
		return nil, "", &Error{Code: CodeInvalidEvidence, Operation: "canonicalQualificationRecord", Field: "$", Cause: err}
	}
	delete(root, "sha256")
	canonical, err := json.Marshal(root)
	if err != nil {
		return nil, "", &Error{Code: CodeInvalidEvidence, Operation: "canonicalQualificationRecord", Field: "$", Cause: err}
	}
	canonical = append(canonical, '\n')
	if len(canonical) > MaxCanonicalBytes {
		return nil, "", fail(CodeInvalidEvidence, "canonicalQualificationRecord", "$")
	}
	sum := sha256.Sum256(canonical)
	return canonical, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func allStringsUTF8(r QualificationRecord) bool {
	values := []string{
		r.ScopeID, r.RunID, r.ContractRef.ID, r.ContractRef.SHA256, r.BackupSetRef,
		r.SourceRelease.Version, r.SourceRelease.ImageDigest, r.SourceRelease.SourceCommit, r.SourceRelease.ProvenanceURI,
		r.CandidateRelease.Version, r.CandidateRelease.ImageDigest, r.CandidateRelease.SourceCommit, r.CandidateRelease.ProvenanceURI,
		r.TargetEnvironmentID, r.TargetProofRef, r.RestoreRunRef, r.ReconciliationReportRef, r.UpgradeRunRef,
		string(r.Status), r.OperatorID, r.RunbookRevision, r.RunbookSHA256, r.DiagnosticsRef,
	}
	for _, finding := range r.Findings {
		values = append(values, finding.CheckID, finding.Owner, string(finding.Reason), finding.EvidenceRef)
	}
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

func prepare(record QualificationRecord) (QualificationRecord, []byte, string, error) {
	if err := validateShape(record); err != nil {
		return QualificationRecord{}, nil, "", err
	}
	if err := record.SourceRelease.Validate(); err != nil {
		return QualificationRecord{}, nil, "", err
	}
	if err := record.CandidateRelease.Validate(); err != nil {
		return QualificationRecord{}, nil, "", err
	}
	if err := validateRedaction(record); err != nil {
		return QualificationRecord{}, nil, "", err
	}
	canonical, digest, err := canonicalQualificationRecord(record)
	if err != nil {
		return QualificationRecord{}, nil, "", err
	}
	if !digestPattern.MatchString(record.SHA256) || record.SHA256 != digest {
		return QualificationRecord{}, nil, "", fail(CodeEvidenceHashMismatch, "PutTerminal", "sha256")
	}
	return clone(record), canonical, digest, nil
}

// CopyRecord returns a deep copy safe for storage or caller mutation.
func CopyRecord(record QualificationRecord) QualificationRecord { return clone(record) }

// CanonicalRecord returns the hash-excluding canonical bytes and digest.
func CanonicalRecord(record QualificationRecord) ([]byte, string, error) {
	return canonicalQualificationRecord(record)
}

// PrepareRecord validates a terminal record and returns its canonical identity.
func PrepareRecord(record QualificationRecord) (QualificationRecord, []byte, string, error) {
	return prepare(record)
}

func validatePageSize(size int) error {
	if size < 1 || size > 100 {
		return fail(CodeInvalidPageSize, "List", "pageSize")
	}
	return nil
}

func encodePageToken(scope, after string, key []byte, version byte) string {
	body := []byte{version, byte(len(scope))}
	body = append(body, scope...)
	body = append(body, byte(len(after)))
	body = append(body, after...)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(body)
	return base64.RawURLEncoding.EncodeToString(append(body, mac.Sum(nil)...))
}

func decodePageToken(token, scope string, key []byte) (string, error) {
	if token == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) < 35 {
		return "", fail(CodeInvalidPageToken, "List", "pageToken")
	}
	body, tag := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(body)
	if !hmac.Equal(tag, mac.Sum(nil)) || body[0] != 1 {
		return "", fail(CodeInvalidPageToken, "List", "pageToken")
	}
	n := int(body[1])
	if len(body) < 3+n || string(body[2:2+n]) != scope {
		return "", fail(CodeInvalidPageToken, "List", "pageToken")
	}
	m := int(body[2+n])
	if len(body) != 3+n+m {
		return "", fail(CodeInvalidPageToken, "List", "pageToken")
	}
	after := string(body[3+n:])
	if !validID(after) {
		return "", fail(CodeInvalidPageToken, "List", "pageToken")
	}
	return after, nil
}

// EncodePageToken creates a scope-bound token; version is exposed for adapter conformance tests.
func EncodePageToken(scope, after string, key []byte, version byte) string {
	return encodePageToken(scope, after, key, version)
}

// Paginate applies deterministic scoped pagination to a copied snapshot.
func Paginate(records map[string]QualificationRecord, scope string, size int, token string, key []byte) (EvidencePage, error) {
	if err := validatePageSize(size); err != nil {
		return EvidencePage{}, err
	}
	after, err := decodePageToken(token, scope, key)
	if err != nil {
		return EvidencePage{}, err
	}
	ids := make([]string, 0, len(records))
	for id, record := range records {
		if record.ScopeID == scope && record.RunID == id && id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := EvidencePage{Items: make([]QualificationRecord, 0, size)}
	for _, id := range ids {
		if len(out.Items) == size {
			out.NextPageToken = encodePageToken(scope, out.Items[len(out.Items)-1].RunID, key, 1)
			break
		}
		out.Items = append(out.Items, clone(records[id]))
	}
	return out, nil
}
