// Package state defines the provider-neutral recovery run-state port.
package state

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxCanonicalJSONBytes = 1 << 20
	maxCanonicalJSONDepth = 64
)

type StepStatus string

const (
	StepStatusNotStarted StepStatus = "not_started"
	StepStatusStarted    StepStatus = "started"
	StepStatusSucceeded  StepStatus = "succeeded"
	StepStatusFailed     StepStatus = "failed"
	StepStatusUnknown    StepStatus = "unknown"
)

type StepState struct {
	StepID        string     `json:"stepId"`
	RequestSHA256 string     `json:"requestSha256"`
	Status        StepStatus `json:"status"`
	ExternalRef   string     `json:"externalRef,omitempty"`
	Attempt       uint32     `json:"attempt"`
}

type RecoveryRun struct {
	RunID         string               `json:"runId"`
	PayloadSHA256 string               `json:"payloadSha256"`
	ScopeID       string               `json:"scopeId"`
	Revision      uint64               `json:"revision"`
	State         string               `json:"state"`
	StepStates    map[string]StepState `json:"stepStates"`
	CreatedAt     time.Time            `json:"createdAt"`
	UpdatedAt     time.Time            `json:"updatedAt"`
}

type RunPage struct {
	Items         []RecoveryRun `json:"items"`
	NextPageToken string        `json:"nextPageToken,omitempty"`
}

type ErrorCode string

const (
	CodeRunPayloadConflict ErrorCode = "RUN_PAYLOAD_CONFLICT"
	CodeRevisionConflict   ErrorCode = "REVISION_CONFLICT"
	CodeScopeDenied        ErrorCode = "SCOPE_DENIED"
	CodeInvalidPageToken   ErrorCode = "INVALID_PAGE_TOKEN"
	CodeInvalidPageSize    ErrorCode = "INVALID_PAGE_SIZE"
	CodeRunNotFound        ErrorCode = "RUN_NOT_FOUND"
	CodeInvalidRun         ErrorCode = "INVALID_RUN"
)

type Error struct {
	Code      ErrorCode
	Operation string
	Field     string
	Revision  uint64
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
	ErrRunPayloadConflict = &Error{Code: CodeRunPayloadConflict}
	ErrRevisionConflict   = &Error{Code: CodeRevisionConflict}
	ErrScopeDenied        = &Error{Code: CodeScopeDenied}
	ErrInvalidPageToken   = &Error{Code: CodeInvalidPageToken}
	ErrInvalidPageSize    = &Error{Code: CodeInvalidPageSize}
	ErrRunNotFound        = &Error{Code: CodeRunNotFound}
	ErrInvalidRun         = &Error{Code: CodeInvalidRun}
)

func IsCode(err error, code ErrorCode) bool {
	var failure *Error
	return errors.As(err, &failure) && failure.Code == code
}

func fail(code ErrorCode, op, field string) error {
	return &Error{Code: code, Operation: op, Field: field}
}

func conflict(code ErrorCode, op string, revision uint64) error {
	return &Error{Code: code, Operation: op, Revision: revision}
}

type RunStore interface {
	Durable() bool
	Create(context.Context, RecoveryRun) (RecoveryRun, error)
	Get(context.Context, string, string) (RecoveryRun, error)
	CompareAndSwap(context.Context, string, string, uint64, RecoveryRun) (RecoveryRun, error)
	List(context.Context, string, int, string) (RunPage, error)
}

var (
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._@/-]{0,191})$`)
	scopeIDPattern = regexp.MustCompile(`^[a-z](?:[a-z0-9]|[-./@_][a-z0-9])*$`)
	digestPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

func validID(v string) bool {
	return idPattern.MatchString(v) && v != "." && v != ".." && !strings.Contains(v, "..")
}

func validScopeID(v string) bool { return len(v) <= 192 && scopeIDPattern.MatchString(v) }

func validTime(v time.Time) bool {
	if v.IsZero() {
		return false
	}
	_, offset := v.Zone()
	return offset == 0
}

func clone(run RecoveryRun) RecoveryRun {
	out := run
	out.CreatedAt, out.UpdatedAt = run.CreatedAt.Round(0).UTC(), run.UpdatedAt.Round(0).UTC()
	if run.StepStates != nil {
		out.StepStates = make(map[string]StepState, len(run.StepStates))
		for k, v := range run.StepStates {
			out.StepStates[k] = v
		}
	}
	return out
}
func equal(a, b RecoveryRun) bool { return reflect.DeepEqual(clone(a), clone(b)) }

func validate(run RecoveryRun) error {
	bad := func(field string) error { return fail(CodeInvalidRun, "validate", field) }
	switch {
	case !validScopeID(run.ScopeID):
		return bad("scopeId")
	case !validID(run.RunID):
		return bad("runId")
	case !digestPattern.MatchString(run.PayloadSHA256):
		return bad("payloadSha256")
	case run.Revision == 0:
		return bad("revision")
	case run.State == "" || len(run.State) > 4096 || !utf8.ValidString(run.State):
		return bad("state")
	case !validTime(run.CreatedAt):
		return bad("createdAt")
	case !validTime(run.UpdatedAt) || run.UpdatedAt.Before(run.CreatedAt):
		return bad("updatedAt")
	}
	for key, step := range run.StepStates {
		if !validID(key) || step.StepID != key {
			return bad("stepStates")
		}
		if !digestPattern.MatchString(step.RequestSHA256) {
			return bad("stepStates." + key + ".requestSha256")
		}
		switch step.Status {
		case StepStatusNotStarted:
			if step.Attempt != 0 || step.ExternalRef != "" {
				return bad("stepStates." + key)
			}
		case StepStatusStarted, StepStatusSucceeded, StepStatusFailed, StepStatusUnknown:
			if step.Attempt == 0 {
				return bad("stepStates." + key + ".attempt")
			}
		default:
			return bad("stepStates." + key + ".status")
		}
		if len(step.ExternalRef) > 4096 || !utf8.ValidString(step.ExternalRef) {
			return bad("stepStates." + key + ".externalRef")
		}
	}
	return nil
}

func scanJSONObject(decoder *json.Decoder, depth int) error {
	keys := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return ErrInvalidRun
		}
		if _, duplicate := keys[key]; duplicate {
			return ErrInvalidRun
		}
		keys[key] = struct{}{}
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err := decoder.Token()
	return err
}

func scanJSONArray(decoder *json.Decoder, depth int) error {
	for decoder.More() {
		if err := scanJSONValue(decoder, depth+1); err != nil {
			return err
		}
	}
	_, err := decoder.Token()
	return err
}

func scanJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxCanonicalJSONDepth {
		return ErrInvalidRun
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		return scanJSONObject(decoder, depth)
	case '[':
		return scanJSONArray(decoder, depth)
	default:
		return ErrInvalidRun
	}
}

func canonicalJSON(raw []byte, operation string) ([]byte, string, error) {
	if len(raw) == 0 || len(raw) > maxCanonicalJSONBytes || !utf8.Valid(raw) {
		return nil, "", fail(CodeInvalidRun, operation, "$")
	}
	scanner := json.NewDecoder(bytes.NewReader(raw))
	scanner.UseNumber()
	if err := scanJSONValue(scanner, 0); err != nil {
		return nil, "", &Error{Code: CodeInvalidRun, Operation: operation, Field: "$", Cause: err}
	}
	if _, err := scanner.Token(); !errors.Is(err, io.EOF) {
		return nil, "", &Error{Code: CodeInvalidRun, Operation: operation, Field: "$", Cause: err}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, "", &Error{Code: CodeInvalidRun, Operation: operation, Field: "$", Cause: err}
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, "", &Error{Code: CodeInvalidRun, Operation: operation, Field: "$", Cause: err}
	}
	canonical = append(canonical, '\n')
	if len(canonical) > maxCanonicalJSONBytes {
		return nil, "", fail(CodeInvalidRun, operation, "$")
	}
	sum := sha256.Sum256(canonical)
	return canonical, "sha256:" + hex.EncodeToString(sum[:]), nil
}

// CanonicalPayload canonicalizes one JSON run payload and returns its SHA-256 identity.
func CanonicalPayload(raw []byte) ([]byte, string, error) {
	return canonicalJSON(raw, "CanonicalPayload")
}

// CanonicalRequest canonicalizes one JSON step request and returns its SHA-256 identity.
func CanonicalRequest(raw []byte) ([]byte, string, error) {
	return canonicalJSON(raw, "CanonicalRequest")
}

func validateAdvance(current, next RecoveryRun, expected uint64) error {
	if current.Revision != expected || expected == ^uint64(0) || next.Revision != expected+1 {
		return conflict(CodeRevisionConflict, "CompareAndSwap", current.Revision)
	}
	if next.ScopeID != current.ScopeID || next.RunID != current.RunID ||
		next.PayloadSHA256 != current.PayloadSHA256 || !next.CreatedAt.Equal(current.CreatedAt) ||
		next.UpdatedAt.Before(current.UpdatedAt) {
		return conflict(CodeRevisionConflict, "CompareAndSwap", current.Revision)
	}
	if err := validate(next); err != nil {
		return err
	}
	for key, old := range current.StepStates {
		if n, ok := next.StepStates[key]; !ok || n.StepID != old.StepID || n.RequestSHA256 != old.RequestSHA256 || n.Attempt < old.Attempt {
			return conflict(CodeRevisionConflict, "CompareAndSwap", current.Revision)
		}
	}
	return nil
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

// CopyRun returns a deep copy safe for storage or caller mutation.
func CopyRun(run RecoveryRun) RecoveryRun { return clone(run) }

// SameRun compares normalized run values.
func SameRun(a, b RecoveryRun) bool { return equal(a, b) }

// ValidateRun validates a complete run value.
func ValidateRun(run RecoveryRun) error { return validate(run) }

// ValidateAdvance validates the stable-identity and monotonic CAS contract.
func ValidateAdvance(current, next RecoveryRun, expected uint64) error {
	return validateAdvance(current, next, expected)
}

// EncodePageToken creates a scope-bound token; version is exposed for adapter conformance tests.
func EncodePageToken(scope, after string, key []byte, version byte) string {
	return encodePageToken(scope, after, key, version)
}

// Paginate applies deterministic scoped pagination to a copied snapshot.
func Paginate(runs map[string]RecoveryRun, scope string, size int, token string, key []byte) (RunPage, error) {
	return page(runs, scope, size, token, key)
}

func page(runs map[string]RecoveryRun, scope string, size int, token string, key []byte) (RunPage, error) {
	if err := validatePageSize(size); err != nil {
		return RunPage{}, err
	}
	after, err := decodePageToken(token, scope, key)
	if err != nil {
		return RunPage{}, err
	}
	ids := make([]string, 0, len(runs))
	for id, run := range runs {
		if run.ScopeID == scope && run.RunID == id && id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := RunPage{Items: make([]RecoveryRun, 0, size)}
	for _, id := range ids {
		if len(out.Items) == size {
			out.NextPageToken = encodePageToken(scope, out.Items[len(out.Items)-1].RunID, key, 1)
			break
		}
		out.Items = append(out.Items, clone(runs[id]))
	}
	return out, nil
}
