package repository

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxManifestBytes = 1 << 20
	maxMetadataBytes = 1 << 20
	maxJSONDepth = 64
	streamChunkBytes = 64 << 10
	maxRetentionSeconds uint64 = (1<<63 - 1) / 1_000_000_000
)

var (
	digestPattern = regexp.MustCompile("^sha256:[0-9a-f]{64}$")
	fnvPattern = regexp.MustCompile("^[0-9a-f]{16}$")
	idPattern = regexp.MustCompile("^[A-Za-z0-9](?:[A-Za-z0-9._@-]{0,191})$")
	manifestJSONKeys = map[string]string{
		"$": "|schemaVersion|scopeId|backupId|contractRef|sourceEnvironmentId|sourceRelease|scheduleWindow|recoveryPoint|expectedShardIds|artifacts|catalogObject|publishedAt|manifestSha256|",
		"$.contractRef": "|id|revision|sha256|",
		"$.sourceRelease": "|version|imageDigest|sourceCommit|provenanceURI|",
		"$.scheduleWindow": "|start|end|",
		"$.recoveryPoint": "|barrierId|sourceEnvironmentId|placementEpoch|admittedAt|recoveryPointAt|shards|",
		"$.recoveryPoint.shards[]": "|shardId|placementEpoch|replicaId|raftAppliedIndex|changeIndex|",
		"$.artifacts[]": "|shardId|placementEpoch|replicaId|object|rowStats|",
		"$.artifacts[].object": "|objectId|sha256|sizeBytes|mediaType|",
		"$.artifacts[].rowStats[]": "|table|rows|fnv1a64|",
		"$.catalogObject": "|objectId|sha256|sizeBytes|mediaType|",
	}
)

func repositoryFailure(code ErrorCode, operation, scopeID, backupID, reason string, cause error) error {
	return &RepositoryError{Code: code, Operation: operation, ScopeID: scopeID, BackupID: backupID, Reason: reason, Cause: cause}
}

func fail(code ErrorCode, operation, scopeID, backupID, reason string) error {
	return repositoryFailure(code, operation, scopeID, backupID, reason, nil)
}

func validIdentifier(value string) bool {
	return idPattern.MatchString(value) && value != "." && value != ".." && !strings.Contains(value, "..")
}
func validDigest(value string) bool { return digestPattern.MatchString(value) }
func utcTime(value time.Time) bool {
	if value.IsZero() { return false }
	_, offset := value.Zone()
	return offset == 0
}
func normalizeTime(value time.Time) time.Time { return value.UTC().Round(0) }

func validateScheduleWindow(operation, scopeID, backupID string, window ScheduleWindow) error {
	if !utcTime(window.Start) || !utcTime(window.End) || !window.Start.Before(window.End) {
		return fail(CodeRepositoryDenied, operation, scopeID, backupID, "INVALID_SCHEDULE_WINDOW")
	}
	return nil
}

func validateContractRef(operation, scopeID, backupID, id string, revision uint64, digest string) error {
	if !validIdentifier(id) || revision == 0 || !validDigest(digest) {
		return fail(CodeRepositoryDenied, operation, scopeID, backupID, "INVALID_CONTRACT_REF")
	}
	return nil
}

func validateReservation(input Reservation) (Reservation, error) {
	const op = "Reserve"
	switch {
	case !validIdentifier(input.ScopeID):
		return Reservation{}, fail(CodeRepositoryDenied, op, input.ScopeID, input.BackupID, "INVALID_SCOPE")
	case !validIdentifier(input.BackupID):
		return Reservation{}, fail(CodeRepositoryDenied, op, input.ScopeID, input.BackupID, "INVALID_BACKUP_ID")
	case !validIdentifier(input.SourceEnvironmentID):
		return Reservation{}, fail(CodeRepositoryDenied, op, input.ScopeID, input.BackupID, "INVALID_SOURCE_ENVIRONMENT")
	}
	if err := validateContractRef(op, input.ScopeID, input.BackupID, input.ContractRef.ID, input.ContractRef.Revision, input.ContractRef.SHA256); err != nil { return Reservation{}, err }
	if err := validateScheduleWindow(op, input.ScopeID, input.BackupID, input.ScheduleWindow); err != nil { return Reservation{}, err }
	input.ScheduleWindow.Start, input.ScheduleWindow.End = normalizeTime(input.ScheduleWindow.Start), normalizeTime(input.ScheduleWindow.End)
	return input, nil
}

func reservationIdentity(reservation Reservation) (string, string, error) {
	normalized, err := validateReservation(reservation)
	if err != nil { return "", "", err }
	identity := struct {
		ScopeID string `json:"scopeId"`
		SourceEnvironmentID string `json:"sourceEnvironmentId"`
		ScheduleWindow ScheduleWindow `json:"scheduleWindow"`
	}{normalized.ScopeID, normalized.SourceEnvironmentID, normalized.ScheduleWindow}
	raw, err := json.Marshal(identity)
	if err != nil { return "", "", repositoryFailure(CodeRepositoryDenied, "Reserve", normalized.ScopeID, normalized.BackupID, "INVALID_RESERVATION", err) }
	sum := sha256.Sum256(raw)
	key := "reservations/" + hex.EncodeToString(sum[:])
	token, err := encodeOpaqueToken("attempt", normalized.ScopeID, normalized.BackupID, key)
	return key, token, err
}

func backupRecordKey(backupID string) string { return "backups/" + backupID }
func manifestObjectKey(scopeID, backupID string) string {
	return "scopes/" + scopeID + "/backups/" + backupID + "/manifest.v1.json"
}
func shardObjectKey(scopeID, backupID string, shardID uint32, digest string) string {
	return "scopes/" + scopeID + "/backups/" + backupID + "/shards/" + strconv.FormatUint(uint64(shardID), 10) + "/" + strings.TrimPrefix(digest, "sha256:") + ".tar.zst"
}
func catalogObjectKey(scopeID, backupID, digest string) string {
	return "scopes/" + scopeID + "/backups/" + backupID + "/catalog/" + strings.TrimPrefix(digest, "sha256:")
}

func canonicalBackupSet(set BackupSet) (BackupSet, []byte, string, error) {
	if err := validateBackupSet(set); err != nil { return BackupSet{}, nil, "", err }
	normalized := cloneBackupSet(set)
	normalized.ScheduleWindow.Start, normalized.ScheduleWindow.End = normalizeTime(normalized.ScheduleWindow.Start), normalizeTime(normalized.ScheduleWindow.End)
	normalized.RecoveryPoint.AdmittedAt = normalizeTime(normalized.RecoveryPoint.AdmittedAt)
	normalized.RecoveryPoint.RecoveryPointAt = normalizeTime(normalized.RecoveryPoint.RecoveryPointAt)
	normalized.PublishedAt = normalizeTime(normalized.PublishedAt)
	sort.Slice(normalized.ExpectedShardIDs, func(i, j int) bool { return normalized.ExpectedShardIDs[i] < normalized.ExpectedShardIDs[j] })
	sort.Slice(normalized.RecoveryPoint.Shards, func(i, j int) bool { return normalized.RecoveryPoint.Shards[i].ShardID < normalized.RecoveryPoint.Shards[j].ShardID })
	sort.Slice(normalized.Artifacts, func(i, j int) bool { return normalized.Artifacts[i].ShardID < normalized.Artifacts[j].ShardID })
	for i := range normalized.Artifacts {
		sort.Slice(normalized.Artifacts[i].RowStats, func(a, b int) bool {
			left, right := normalized.Artifacts[i].RowStats[a], normalized.Artifacts[i].RowStats[b]
			if left.Table != right.Table { return left.Table < right.Table }
			if left.Rows != right.Rows { return left.Rows < right.Rows }
			return left.FNV1A64 < right.FNV1A64
		})
	}
	provided := normalized.ManifestSHA256
	normalized.ManifestSHA256 = ""
	canonical, err := json.Marshal(normalized)
	if err != nil { return BackupSet{}, nil, "", repositoryFailure(CodeRepositoryDenied, "canonicalBackupSet", set.ScopeID, set.BackupID, "MANIFEST_ENCODING_FAILED", err) }
	canonical = append(canonical, '\n')
	if len(canonical) > maxManifestBytes { return BackupSet{}, nil, "", fail(CodeRepositoryDenied, "canonicalBackupSet", set.ScopeID, set.BackupID, "MANIFEST_TOO_LARGE") }
	sum := sha256.Sum256(canonical)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if provided != "" && provided != digest { return BackupSet{}, nil, "", fail(CodeDigestMismatch, "canonicalBackupSet", set.ScopeID, set.BackupID, "MANIFEST_DIGEST_MISMATCH") }
	normalized.ManifestSHA256 = digest
	return normalized, canonical, digest, nil
}

func storedManifestBytes(set BackupSet) ([]byte, error) {
	normalized, _, _, err := canonicalBackupSet(set)
	if err != nil { return nil, err }
	raw, err := json.Marshal(normalized)
	if err != nil { return nil, repositoryFailure(CodeRepositoryDenied, "storedManifestBytes", set.ScopeID, set.BackupID, "MANIFEST_ENCODING_FAILED", err) }
	raw = append(raw, '\n')
	if len(raw) > maxManifestBytes { return nil, fail(CodeRepositoryDenied, "storedManifestBytes", set.ScopeID, set.BackupID, "MANIFEST_TOO_LARGE") }
	return raw, nil
}

func decodeBackupSet(raw []byte) (BackupSet, error) {
	switch {
	case len(raw) > maxManifestBytes:
		return BackupSet{}, fail(CodeRepositoryDenied, "decodeBackupSet", "", "", "MANIFEST_TOO_LARGE")
	case !utf8.Valid(raw):
		return BackupSet{}, fail(CodeRepositoryDenied, "decodeBackupSet", "", "", "INVALID_UTF8")
	case bytes.IndexByte(raw, '\r') >= 0:
		return BackupSet{}, fail(CodeRepositoryDenied, "decodeBackupSet", "", "", "INVALID_LINE_ENDING")
	}
	if err := scanManifestJSON(raw); err != nil { return BackupSet{}, err }
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var set BackupSet
	if err := decoder.Decode(&set); err != nil { return BackupSet{}, repositoryFailure(CodeRepositoryDenied, "decodeBackupSet", "", "", "INVALID_JSON", err) }
	if err := requireJSONEOF(decoder); err != nil { return BackupSet{}, err }
	normalized, _, _, err := canonicalBackupSet(set)
	return normalized, err
}

func requireJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) { return fail(CodeRepositoryDenied, "decodeBackupSet", "", "", "TRAILING_JSON") }
	return nil
}

func cloneBackupSet(set BackupSet) BackupSet {
	clone := set
	clone.ExpectedShardIDs = append([]uint32(nil), set.ExpectedShardIDs...)
	clone.RecoveryPoint.Shards = append([]ShardPoint(nil), set.RecoveryPoint.Shards...)
	clone.Artifacts = make([]ShardArtifact, len(set.Artifacts))
	for i := range set.Artifacts {
		clone.Artifacts[i] = set.Artifacts[i]
		clone.Artifacts[i].RowStats = append([]TableStat(nil), set.Artifacts[i].RowStats...)
	}
	return clone
}

func validateBackupSet(set BackupSet) error {
	const op = "validateBackupSet"
	bad := func(code ErrorCode, reason string) error { return fail(code, op, set.ScopeID, set.BackupID, reason) }
	switch {
	case set.SchemaVersion != ManifestSchemaVersion:
		return bad(CodeRepositoryDenied, "UNSUPPORTED_MANIFEST_VERSION")
	case !validIdentifier(set.ScopeID):
		return bad(CodeRepositoryDenied, "INVALID_SCOPE")
	case !validIdentifier(set.BackupID):
		return bad(CodeRepositoryDenied, "INVALID_BACKUP_ID")
	case !validIdentifier(set.SourceEnvironmentID):
		return bad(CodeRepositoryDenied, "INVALID_SOURCE_ENVIRONMENT")
	case !validIdentifier(set.RecoveryPoint.BarrierID):
		return bad(CodeRepositoryDenied, "MISSING_BARRIER")
	case set.RecoveryPoint.SourceEnvironmentID != set.SourceEnvironmentID:
		return bad(CodeRepositoryDenied, "SOURCE_ENVIRONMENT_MISMATCH")
	case set.RecoveryPoint.PlacementEpoch == 0:
		return bad(CodePlacementChanged, "INVALID_PLACEMENT_EPOCH")
	case !utcTime(set.RecoveryPoint.AdmittedAt) || !utcTime(set.RecoveryPoint.RecoveryPointAt) || set.RecoveryPoint.RecoveryPointAt.Before(set.RecoveryPoint.AdmittedAt):
		return bad(CodeRepositoryDenied, "INVALID_RECOVERY_POINT_TIME")
	case !utcTime(set.PublishedAt) || set.PublishedAt.Before(set.RecoveryPoint.RecoveryPointAt):
		return bad(CodeRepositoryDenied, "INVALID_PUBLISHED_AT")
	}
	if err := validateContractRef(op, set.ScopeID, set.BackupID, set.ContractRef.ID, set.ContractRef.Revision, set.ContractRef.SHA256); err != nil { return err }
	if err := set.SourceRelease.Validate(); err != nil { return repositoryFailure(CodeRepositoryDenied, op, set.ScopeID, set.BackupID, "INVALID_SOURCE_RELEASE", err) }
	if err := validateScheduleWindow(op, set.ScopeID, set.BackupID, set.ScheduleWindow); err != nil { return err }
	if len(set.ExpectedShardIDs) == 0 { return bad(CodeIncompleteShardCoverage, "EMPTY_EXPECTED_SHARDS") }

	expected := make(map[uint32]struct{}, len(set.ExpectedShardIDs))
	for _, id := range set.ExpectedShardIDs {
		if _, exists := expected[id]; exists { return bad(CodeIncompleteShardCoverage, "DUPLICATE_EXPECTED_SHARD") }
		expected[id] = struct{}{}
	}
	points := make(map[uint32]ShardPoint, len(set.RecoveryPoint.Shards))
	for _, point := range set.RecoveryPoint.Shards {
		if _, ok := expected[point.ShardID]; !ok { return bad(CodeIncompleteShardCoverage, "UNEXPECTED_RECOVERY_POINT_SHARD") }
		if _, exists := points[point.ShardID]; exists { return bad(CodeIncompleteShardCoverage, "DUPLICATE_RECOVERY_POINT_SHARD") }
		if point.PlacementEpoch != set.RecoveryPoint.PlacementEpoch || !validIdentifier(point.ReplicaID) { return bad(CodePlacementChanged, "RECOVERY_POINT_PLACEMENT_MISMATCH") }
		points[point.ShardID] = point
	}
	if len(points) != len(expected) { return bad(CodeIncompleteShardCoverage, "MISSING_RECOVERY_POINT_SHARD") }

	artifacts := make(map[uint32]struct{}, len(set.Artifacts))
	for _, artifact := range set.Artifacts {
		point, ok := points[artifact.ShardID]
		if !ok { return bad(CodeIncompleteShardCoverage, "UNEXPECTED_ARTIFACT_SHARD") }
		if _, exists := artifacts[artifact.ShardID]; exists { return bad(CodeIncompleteShardCoverage, "DUPLICATE_ARTIFACT_SHARD") }
		if artifact.PlacementEpoch != set.RecoveryPoint.PlacementEpoch || artifact.ReplicaID != point.ReplicaID { return bad(CodePlacementChanged, "ARTIFACT_PLACEMENT_MISMATCH") }
		key := shardObjectKey(set.ScopeID, set.BackupID, artifact.ShardID, artifact.Object.SHA256)
		if err := validateObjectRef(op, set.ScopeID, set.BackupID, artifact.Object, key, MediaTypeShard); err != nil { return err }
		tables := make(map[string]struct{}, len(artifact.RowStats))
		for _, stat := range artifact.RowStats {
			if !validIdentifier(stat.Table) || !fnvPattern.MatchString(stat.FNV1A64) { return bad(CodeRepositoryDenied, "INVALID_ROW_STAT") }
			if _, exists := tables[stat.Table]; exists { return bad(CodeRepositoryDenied, "DUPLICATE_ROW_STAT") }
			tables[stat.Table] = struct{}{}
		}
		artifacts[artifact.ShardID] = struct{}{}
	}
	if len(artifacts) != len(expected) { return bad(CodeIncompleteShardCoverage, "MISSING_ARTIFACT_SHARD") }
	catalogKey := catalogObjectKey(set.ScopeID, set.BackupID, set.CatalogObject.SHA256)
	if err := validateObjectRef(op, set.ScopeID, set.BackupID, set.CatalogObject, catalogKey, MediaTypeCatalog); err != nil { return err }
	if set.ManifestSHA256 != "" && !validDigest(set.ManifestSHA256) { return bad(CodeDigestMismatch, "INVALID_MANIFEST_DIGEST") }
	return nil
}

func validateObjectRef(operation, scopeID, backupID string, ref ObjectRef, expectedKey, expectedMediaType string) error {
	switch {
	case !validDigest(ref.SHA256):
		return fail(CodeDigestMismatch, operation, scopeID, backupID, "INVALID_OBJECT_DIGEST")
	case ref.SizeBytes == 0:
		return fail(CodeDigestMismatch, operation, scopeID, backupID, "INVALID_OBJECT_SIZE")
	case ref.ObjectID != expectedKey:
		return fail(CodeRepositoryDenied, operation, scopeID, backupID, "OBJECT_KEY_MISMATCH")
	case ref.MediaType != expectedMediaType:
		return fail(CodeRepositoryDenied, operation, scopeID, backupID, "OBJECT_MEDIA_TYPE_MISMATCH")
	default:
		return nil
	}
}

func manifestJSONError(reason string, cause error) error {
	return repositoryFailure(CodeRepositoryDenied, "decodeBackupSet", "", "", reason, cause)
}
func scanManifestJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil { return manifestJSONError("INVALID_JSON", err) }
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' { return manifestJSONError("TOP_LEVEL_NOT_OBJECT", nil) }
	if err := scanManifestObject(decoder, "$", 1); err != nil { return err }
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) { return manifestJSONError("TRAILING_JSON", nil) }
	return nil
}
func scanManifestValue(decoder *json.Decoder, path string, depth int) error {
	if depth > maxJSONDepth { return manifestJSONError("JSON_TOO_DEEP", nil) }
	token, err := decoder.Token()
	if err != nil { return manifestJSONError("INVALID_JSON", err) }
	switch value := token.(type) {
	case json.Delim:
		if value == '{' { return scanManifestObject(decoder, path, depth) }
		if value == '[' { return scanManifestArray(decoder, path, depth) }
		return manifestJSONError("INVALID_JSON", nil)
	case json.Number:
		if !integerJSON(value.String()) { return manifestJSONError("NON_INTEGER_NUMBER", nil) }
	}
	return nil
}
func scanManifestObject(decoder *json.Decoder, path string, depth int) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil { return manifestJSONError("INVALID_JSON", err) }
		key, ok := token.(string)
		if !ok { return manifestJSONError("INVALID_JSON", nil) }
		if _, exists := seen[key]; exists { return manifestJSONError("DUPLICATE_FIELD", nil) }
		allowed, known := manifestJSONKeys[path]
		if !known || !strings.Contains(allowed, "|"+key+"|") { return manifestJSONError("UNKNOWN_FIELD", nil) }
		seen[key] = struct{}{}
		if err := scanManifestValue(decoder, path+"."+key, depth+1); err != nil { return err }
	}
	token, err := decoder.Token()
	if err != nil || token != json.Delim('}') { return manifestJSONError("INVALID_JSON", err) }
	return nil
}
func scanManifestArray(decoder *json.Decoder, path string, depth int) error {
	for decoder.More() {
		if err := scanManifestValue(decoder, path+"[]", depth+1); err != nil { return err }
	}
	token, err := decoder.Token()
	if err != nil || token != json.Delim(']') { return manifestJSONError("INVALID_JSON", err) }
	return nil
}
func integerJSON(value string) bool {
	value = strings.TrimPrefix(value, "-")
	if value == "" { return false }
	for _, char := range value {
		if char < '0' || char > '9' { return false }
	}
	return true
}

type opaqueToken struct {
	Kind string `json:"kind"`
	ScopeID string `json:"scopeId"`
	BackupID string `json:"backupId,omitempty"`
	Cursor string `json:"cursor"`
	Checksum string `json:"checksum"`
}
func encodeOpaqueToken(kind, scopeID, backupID, cursor string) (string, error) {
	token := opaqueToken{Kind: kind, ScopeID: scopeID, BackupID: backupID, Cursor: cursor}
	token.Checksum = tokenChecksum(token)
	raw, err := json.Marshal(token)
	if err != nil { return "", repositoryFailure(CodeRepositoryDenied, "encodeToken", scopeID, backupID, "TOKEN_ENCODING_FAILED", err) }
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
func decodeOpaqueToken(value, kind string) (opaqueToken, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) == 0 || len(raw) > 4096 { return opaqueToken{}, repositoryFailure(CodeRepositoryDenied, "decodeToken", "", "", "INVALID_PAGE_TOKEN", err) }
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var token opaqueToken
	if err := decoder.Decode(&token); err != nil { return opaqueToken{}, repositoryFailure(CodeRepositoryDenied, "decodeToken", "", "", "INVALID_PAGE_TOKEN", err) }
	eofErr := requireJSONEOF(decoder)
	if eofErr != nil || token.Kind != kind || !validIdentifier(token.ScopeID) || token.Cursor == "" || token.Checksum != tokenChecksum(token) {
		return opaqueToken{}, repositoryFailure(CodeRepositoryDenied, "decodeToken", token.ScopeID, token.BackupID, "INVALID_PAGE_TOKEN", eofErr)
	}
	return token, nil
}
func tokenChecksum(token opaqueToken) string {
	sum := sha256.Sum256([]byte(token.Kind + "\x00" + token.ScopeID + "\x00" + token.BackupID + "\x00" + token.Cursor))
	return hex.EncodeToString(sum[:])
}

func validateRetentionPolicy(input RetentionPolicy) (RetentionPolicy, error) {
	const op = "ApplyRetention"
	bad := func(backupID, reason string) (RetentionPolicy, error) {
		return RetentionPolicy{}, fail(CodeUnsafeRetention, op, input.ScopeID, backupID, reason)
	}
	switch {
	case !validIdentifier(input.ScopeID):
		return bad("", "INVALID_SCOPE")
	case input.MinimumAgeSeconds == 0 || input.MinimumAgeSeconds > maxRetentionSeconds || input.MinimumQualifiedSets == 0:
		return bad("", "INVALID_RETENTION_THRESHOLD")
	case !utcTime(input.EvaluatedAt):
		return bad("", "INVALID_EVALUATED_AT")
	case len(input.QualifiedSets) == 0:
		return bad("", "MISSING_QUALIFICATION_SNAPSHOT")
	}
	normalized := input
	normalized.EvaluatedAt = normalizeTime(input.EvaluatedAt)
	normalized.QualifiedSets = append([]QualifiedSetRef(nil), input.QualifiedSets...)
	seen := make(map[string]struct{}, len(normalized.QualifiedSets))
	for i := range normalized.QualifiedSets {
		ref := &normalized.QualifiedSets[i]
		switch {
		case !validIdentifier(ref.BackupID):
			return bad(ref.BackupID, "INVALID_QUALIFIED_BACKUP")
		case !validDigest(ref.EvidenceSHA256):
			return bad(ref.BackupID, "INVALID_EVIDENCE_DIGEST")
		case !utcTime(ref.QualifiedAt) || ref.QualifiedAt.After(normalized.EvaluatedAt):
			return bad(ref.BackupID, "INVALID_QUALIFIED_AT")
		}
		if _, exists := seen[ref.BackupID]; exists { return bad(ref.BackupID, "DUPLICATE_QUALIFIED_SET") }
		seen[ref.BackupID] = struct{}{}
		ref.QualifiedAt = normalizeTime(ref.QualifiedAt)
	}
	return normalized, nil
}
