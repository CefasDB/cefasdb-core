package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"sort"
	"time"
)

type CaptureRepository interface {
	Reserve(context.Context, Reservation) (ReservationResult, error)
	PutShard(context.Context, string, string, ShardUpload) (ObjectRef, error)
	PutCatalog(context.Context, string, string, CatalogUpload) (ObjectRef, error)
	Publish(context.Context, BackupSet) (ManifestRef, error)
	DeleteUnpublished(context.Context, string) (CleanupReport, error)
}

type ReadRepository interface {
	Read(context.Context, string, string) (BackupSet, error)
	List(context.Context, string, int, string) (ListResult, error)
}

type RetentionRepository interface {
	Tombstone(context.Context, string, string, string) (Tombstone, error)
	Pin(context.Context, string, string, string, time.Time) (Pin, error)
	Unpin(context.Context, string, string, string) error
	ApplyRetention(context.Context, RetentionPolicy, bool) (RetentionResult, error)
}

type Repository interface {
	CaptureRepository
	ReadRepository
	RetentionRepository
}

type repository struct {
	blobs    BlobStore
	metadata MetadataStore
	now      func() time.Time
}

type plannedObject struct {
	Kind      string    `json:"kind"`
	ShardID   uint32    `json:"shardId,omitempty"`
	Ref       ObjectRef `json:"ref"`
	Committed bool      `json:"committed"`
}

type backupRecord struct {
	Version          uint32          `json:"version"`
	ScopeID          string          `json:"scopeId"`
	BackupID         string          `json:"backupId"`
	Reservation      Reservation     `json:"reservation"`
	AttemptID        string          `json:"attemptId"`
	State            LifecycleState  `json:"state"`
	Planned          []plannedObject `json:"planned"`
	Manifest         *ManifestRef    `json:"manifest,omitempty"`
	Pins             []Pin           `json:"pins"`
	Tombstone        *Tombstone      `json:"tombstone,omitempty"`
	QuarantineReason string          `json:"quarantineReason,omitempty"`
	ResidualKeys     []string        `json:"residualKeys"`
	CreatedAt        time.Time       `json:"createdAt"`
	UpdatedAt        time.Time       `json:"updatedAt"`
}

type reservationIndex struct {
	Reservation Reservation `json:"reservation"`
	AttemptID   string      `json:"attemptId"`
}

func New(blobs BlobStore, metadata MetadataStore, now func() time.Time) (Repository, error) {
	if blobs == nil || metadata == nil || now == nil {
		return nil, repositoryFailure(CodeRepositoryDenied, "New", "", "", "MISSING_BACKEND_OR_CLOCK", nil)
	}
	return &repository{blobs: blobs, metadata: metadata, now: now}, nil
}

func (r *repository) Reserve(ctx context.Context, input Reservation) (ReservationResult, error) {
	if err := contextErr(ctx); err != nil {
		return ReservationResult{}, err
	}
	reservation, err := validateReservation(input)
	if err != nil {
		return ReservationResult{}, err
	}
	indexKey, attemptID, err := reservationIdentity(reservation)
	if err != nil {
		return ReservationResult{}, err
	}
	recordKey := backupRecordKey(reservation.BackupID)
	value, getErr := r.metadata.Get(ctx, reservation.ScopeID, recordKey)
	created := false
	var record backupRecord
	switch {
	case getErr == nil:
		record, err = decodeRecord(value.Bytes)
	case errors.Is(getErr, ErrBackendNotFound):
		now := normalizeTime(r.now())
		record = backupRecord{Version: 1, ScopeID: reservation.ScopeID, BackupID: reservation.BackupID, Reservation: reservation, AttemptID: attemptID, State: LifecycleReserved, CreatedAt: now, UpdatedAt: now}
		value, err = r.casRecord(ctx, recordKey, "", record, CodeReservationConflict, "Reserve")
		if err != nil && errors.Is(getErrCode(err), ErrBackendConflict) {
			value, getErr = r.metadata.Get(ctx, reservation.ScopeID, recordKey)
			if getErr == nil {
				record, err = decodeRecord(value.Bytes)
			}
		} else if err == nil {
			created = true
		}
	default:
		err = translateBackend(getErr, CodeRepositoryDenied, "Reserve", reservation.ScopeID, reservation.BackupID)
	}
	if err != nil {
		return ReservationResult{}, err
	}
	if record.Reservation != reservation || record.AttemptID != attemptID {
		return ReservationResult{}, repositoryFailure(CodeReservationConflict, "Reserve", reservation.ScopeID, reservation.BackupID, "BACKUP_ID_ALREADY_RESERVED", nil)
	}
	if record.State == LifecycleDeleted || record.State == LifecycleQuarantined {
		return ReservationResult{}, repositoryFailure(CodeReservationConflict, "Reserve", reservation.ScopeID, reservation.BackupID, "TERMINAL_BACKUP_ID", nil)
	}
	index := reservationIndex{Reservation: reservation, AttemptID: attemptID}
	indexBytes, _ := json.Marshal(index)
	_, err = r.casBytes(ctx, reservation.ScopeID, indexKey, "", indexBytes)
	if err != nil {
		existing, loadErr := r.metadata.Get(ctx, reservation.ScopeID, indexKey)
		if loadErr == nil && bytes.Equal(existing.Bytes, indexBytes) {
			err = nil
		}
	}
	if err != nil {
		if created {
			record.State = LifecycleQuarantined
			record.QuarantineReason = "RESERVATION_CONFLICT"
			record.UpdatedAt = normalizeTime(r.now())
			_, _ = r.casRecord(ctx, recordKey, value.Revision, record, CodeReservationConflict, "Reserve")
		}
		return ReservationResult{}, repositoryFailure(CodeReservationConflict, "Reserve", reservation.ScopeID, reservation.BackupID, "SCHEDULE_WINDOW_ALREADY_RESERVED", err)
	}
	return ReservationResult{AttemptID: attemptID, ScopeID: reservation.ScopeID, BackupID: reservation.BackupID, State: record.State, Created: created}, nil
}

func (r *repository) PutShard(ctx context.Context, scopeID, backupID string, upload ShardUpload) (ObjectRef, error) {
	return r.putObject(ctx, scopeID, backupID, upload.AttemptID, "shard", upload.ShardID, upload.SHA256, upload.SizeBytes, upload.MediaType, upload.Body)
}

func (r *repository) PutCatalog(ctx context.Context, scopeID, backupID string, upload CatalogUpload) (ObjectRef, error) {
	return r.putObject(ctx, scopeID, backupID, upload.AttemptID, "catalog", 0, upload.SHA256, upload.SizeBytes, upload.MediaType, upload.Body)
}

func (r *repository) putObject(ctx context.Context, scopeID, backupID, attemptID, kind string, shardID uint32, digest string, size uint64, mediaType string, body io.Reader) (ObjectRef, error) {
	const operation = "PutObject"
	if err := contextErr(ctx); err != nil {
		return ObjectRef{}, err
	}
	expectedMedia := MediaTypeCatalog
	key := catalogObjectKey(scopeID, backupID, digest)
	if kind == "shard" {
		expectedMedia = MediaTypeShard
		key = shardObjectKey(scopeID, backupID, shardID, digest)
	}
	if !validIdentifier(scopeID) || !validIdentifier(backupID) || !validDigest(digest) || size == 0 || mediaType != expectedMedia || body == nil {
		return ObjectRef{}, repositoryFailure(CodeRepositoryDenied, operation, scopeID, backupID, "INVALID_UPLOAD", nil)
	}
	token, err := decodeOpaqueToken(attemptID, "attempt")
	if err != nil || token.ScopeID != scopeID || token.BackupID != backupID {
		return ObjectRef{}, repositoryFailure(CodeRepositoryDenied, operation, scopeID, backupID, "ATTEMPT_SCOPE_MISMATCH", err)
	}
	record, value, err := r.loadRecord(ctx, scopeID, backupID, operation)
	if err != nil {
		return ObjectRef{}, err
	}
	ref := ObjectRef{ObjectID: key, SHA256: digest, SizeBytes: size, MediaType: mediaType}
	index := plannedIndex(record.Planned, kind, shardID)
	if index >= 0 {
		if record.Planned[index].Ref != ref {
			return ObjectRef{}, repositoryFailure(CodeDigestMismatch, operation, scopeID, backupID, "UPLOAD_IDENTITY_CHANGED", nil)
		}
		if record.Planned[index].Committed {
			return ref, nil
		}
	} else {
		if record.AttemptID != attemptID || (record.State != LifecycleReserved && record.State != LifecycleUploading) {
			return ObjectRef{}, repositoryFailure(CodeRepositoryDenied, operation, scopeID, backupID, "ATTEMPT_NOT_ACTIVE", nil)
		}
		record.Planned = append(record.Planned, plannedObject{Kind: kind, ShardID: shardID, Ref: ref})
		sortPlanned(record.Planned)
		record.State = LifecycleUploading
		record.UpdatedAt = normalizeTime(r.now())
		value, err = r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodePartialPublication, operation)
		if err != nil {
			return ObjectRef{}, err
		}
	}
	validator := newValidatingReader(body, digest, size)
	reconciled := false
	stored, putErr := r.blobs.PutIfAbsent(ctx, scopeID, key, BlobMetadata{SHA256: digest, SizeBytes: size, MediaType: mediaType}, validator)
	if putErr != nil {
		if errors.Is(putErr, ErrBackendConflict) || errors.Is(putErr, ErrBackendAmbiguous) {
			if verifyErr := r.verifyObject(ctx, scopeID, backupID, ref); verifyErr == nil {
				putErr, reconciled = nil, true
				stored = BlobMetadata{SHA256: digest, SizeBytes: size, MediaType: mediaType}
			}
		}
	}
	if putErr != nil {
		return ObjectRef{}, translateBackend(putErr, CodePartialPublication, operation, scopeID, backupID)
	}
	if stored != (BlobMetadata{SHA256: digest, SizeBytes: size, MediaType: mediaType}) || (!reconciled && !validator.complete) {
		return ObjectRef{}, repositoryFailure(CodeDigestMismatch, operation, scopeID, backupID, "BACKEND_METADATA_MISMATCH", nil)
	}
	record, value, err = r.loadRecord(ctx, scopeID, backupID, operation)
	if err != nil {
		return ObjectRef{}, err
	}
	index = plannedIndex(record.Planned, kind, shardID)
	if index < 0 || record.Planned[index].Ref != ref {
		return ObjectRef{}, repositoryFailure(CodePartialPublication, operation, scopeID, backupID, "PLANNED_UPLOAD_LOST", nil)
	}
	record.Planned[index].Committed = true
	record.State = LifecycleUploading
	record.UpdatedAt = normalizeTime(r.now())
	if _, err = r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodePartialPublication, operation); err != nil {
		return ObjectRef{}, err
	}
	return ref, nil
}

func (r *repository) Publish(ctx context.Context, input BackupSet) (ManifestRef, error) {
	if err := contextErr(ctx); err != nil {
		return ManifestRef{}, err
	}
	set, _, digest, err := canonicalBackupSet(input)
	if err != nil {
		return ManifestRef{}, err
	}
	record, value, err := r.loadRecord(ctx, set.ScopeID, set.BackupID, "Publish")
	if err != nil {
		return ManifestRef{}, err
	}
	if record.State == LifecyclePublished && record.Manifest != nil {
		if record.Manifest.ManifestSHA256 == digest {
			return *record.Manifest, nil
		}
		return ManifestRef{}, repositoryFailure(CodeBackupIDConflict, "Publish", set.ScopeID, set.BackupID, "MANIFEST_ALREADY_PUBLISHED", nil)
	}
	if (record.State != LifecycleReserved && record.State != LifecycleUploading) || !reservationMatchesSet(record.Reservation, set) {
		return ManifestRef{}, repositoryFailure(CodeRepositoryDenied, "Publish", set.ScopeID, set.BackupID, "RESERVATION_MISMATCH", nil)
	}
	if err := matchCommittedObjects(record, set); err != nil {
		return ManifestRef{}, err
	}
	for _, artifact := range set.Artifacts {
		if err := r.verifyObject(ctx, set.ScopeID, set.BackupID, artifact.Object); err != nil {
			return ManifestRef{}, err
		}
	}
	if err := r.verifyObject(ctx, set.ScopeID, set.BackupID, set.CatalogObject); err != nil {
		return ManifestRef{}, err
	}
	manifestBytes, err := storedManifestBytes(set)
	if err != nil {
		return ManifestRef{}, err
	}
	rawSum := sha256.Sum256(manifestBytes)
	rawDigest := "sha256:" + hex.EncodeToString(rawSum[:])
	objectRef := ObjectRef{ObjectID: manifestObjectKey(set.ScopeID, set.BackupID), SHA256: rawDigest, SizeBytes: uint64(len(manifestBytes)), MediaType: MediaTypeManifest}
	manifestRef := ManifestRef{ScopeID: set.ScopeID, BackupID: set.BackupID, ObjectID: objectRef.ObjectID, ManifestSHA256: digest, ObjectSHA256: rawDigest, SizeBytes: objectRef.SizeBytes, MediaType: MediaTypeManifest, PublishedAt: set.PublishedAt}
	index := plannedIndex(record.Planned, "manifest", 0)
	if index < 0 {
		record.Planned = append(record.Planned, plannedObject{Kind: "manifest", Ref: objectRef})
		sortPlanned(record.Planned)
		record.UpdatedAt = normalizeTime(r.now())
		value, err = r.casRecord(ctx, backupRecordKey(set.BackupID), value.Revision, record, CodePartialPublication, "Publish")
		if err != nil {
			reloaded, reloadedValue, reloadErr := r.loadRecord(ctx, set.ScopeID, set.BackupID, "Publish")
			if reloadErr == nil && reloaded.State == LifecyclePublished && reloaded.Manifest != nil && reloaded.Manifest.ManifestSHA256 == digest {
				return *reloaded.Manifest, nil
			}
			reloadedIndex := plannedIndex(reloaded.Planned, "manifest", 0)
			if reloadErr != nil || reloadedIndex < 0 || reloaded.Planned[reloadedIndex].Ref != objectRef {
				return ManifestRef{}, err
			}
			record, value = reloaded, reloadedValue
		}
	} else if record.Planned[index].Ref != objectRef {
		return ManifestRef{}, repositoryFailure(CodeBackupIDConflict, "Publish", set.ScopeID, set.BackupID, "MANIFEST_OBJECT_CONFLICT", nil)
	}
	validator := newValidatingReader(bytes.NewReader(manifestBytes), rawDigest, uint64(len(manifestBytes)))
	reconciled := false
	stored, putErr := r.blobs.PutIfAbsent(ctx, set.ScopeID, objectRef.ObjectID, BlobMetadata{SHA256: rawDigest, SizeBytes: objectRef.SizeBytes, MediaType: MediaTypeManifest}, validator)
	if putErr != nil && (errors.Is(putErr, ErrBackendConflict) || errors.Is(putErr, ErrBackendAmbiguous)) {
		if verifyErr := r.verifyObject(ctx, set.ScopeID, set.BackupID, objectRef); verifyErr == nil {
			putErr, reconciled = nil, true
			stored = BlobMetadata{SHA256: rawDigest, SizeBytes: objectRef.SizeBytes, MediaType: MediaTypeManifest}
		}
	}
	if putErr != nil {
		return ManifestRef{}, translateBackend(putErr, CodePartialPublication, "Publish", set.ScopeID, set.BackupID)
	}
	if stored.SHA256 != rawDigest || stored.SizeBytes != objectRef.SizeBytes || stored.MediaType != MediaTypeManifest || (!reconciled && !validator.complete) {
		return ManifestRef{}, repositoryFailure(CodeDigestMismatch, "Publish", set.ScopeID, set.BackupID, "MANIFEST_METADATA_MISMATCH", nil)
	}
	record, value, err = r.loadRecord(ctx, set.ScopeID, set.BackupID, "Publish")
	if err != nil {
		return ManifestRef{}, err
	}
	if record.State == LifecyclePublished && record.Manifest != nil && record.Manifest.ManifestSHA256 == digest {
		return *record.Manifest, nil
	}
	index = plannedIndex(record.Planned, "manifest", 0)
	if index < 0 || record.Planned[index].Ref != objectRef {
		return ManifestRef{}, repositoryFailure(CodePartialPublication, "Publish", set.ScopeID, set.BackupID, "MANIFEST_PLAN_LOST", nil)
	}
	record.Planned[index].Committed = true
	record.State = LifecyclePublished
	record.Manifest = &manifestRef
	record.UpdatedAt = normalizeTime(r.now())
	_, err = r.casRecord(ctx, backupRecordKey(set.BackupID), value.Revision, record, CodePartialPublication, "Publish")
	if err != nil {
		reloaded, _, reloadErr := r.loadRecord(ctx, set.ScopeID, set.BackupID, "Publish")
		if reloadErr == nil && reloaded.State == LifecyclePublished && reloaded.Manifest != nil && reloaded.Manifest.ManifestSHA256 == digest {
			return *reloaded.Manifest, nil
		}
		return ManifestRef{}, err
	}
	return manifestRef, nil
}

func (r *repository) Read(ctx context.Context, scopeID, backupID string) (BackupSet, error) {
	if err := contextErr(ctx); err != nil {
		return BackupSet{}, err
	}
	record, _, err := r.loadRecord(ctx, scopeID, backupID, "Read")
	if err != nil || record.State != LifecyclePublished || record.Manifest == nil {
		if err != nil {
			return BackupSet{}, err
		}
		return BackupSet{}, repositoryFailure(CodeRepositoryDenied, "Read", scopeID, backupID, "BACKUP_NOT_PUBLISHED", nil)
	}
	raw, err := r.readManifest(ctx, *record.Manifest)
	if err != nil {
		return BackupSet{}, err
	}
	set, err := decodeBackupSet(raw)
	if err != nil {
		return BackupSet{}, err
	}
	if set.ScopeID != scopeID || set.BackupID != backupID || set.ManifestSHA256 != record.Manifest.ManifestSHA256 || manifestObjectKey(scopeID, backupID) != record.Manifest.ObjectID {
		return BackupSet{}, repositoryFailure(CodeRepositoryDenied, "Read", scopeID, backupID, "MANIFEST_SCOPE_OR_ID_MISMATCH", nil)
	}
	for _, artifact := range set.Artifacts {
		if err := r.verifyObject(ctx, scopeID, backupID, artifact.Object); err != nil {
			return BackupSet{}, err
		}
	}
	if err := r.verifyObject(ctx, scopeID, backupID, set.CatalogObject); err != nil {
		return BackupSet{}, err
	}
	final, _, err := r.loadRecord(ctx, scopeID, backupID, "Read")
	if err != nil || final.State != LifecyclePublished || final.Manifest == nil || final.Manifest.ManifestSHA256 != set.ManifestSHA256 {
		if err != nil {
			return BackupSet{}, err
		}
		return BackupSet{}, repositoryFailure(CodeRepositoryDenied, "Read", scopeID, backupID, "BACKUP_NO_LONGER_PUBLISHED", nil)
	}
	return set, nil
}

func (r *repository) List(ctx context.Context, scopeID string, pageSize int, pageToken string) (ListResult, error) {
	if err := contextErr(ctx); err != nil {
		return ListResult{}, err
	}
	if !validIdentifier(scopeID) || pageSize < 1 || pageSize > 100 {
		return ListResult{}, repositoryFailure(CodeRepositoryDenied, "List", scopeID, "", "INVALID_PAGE_REQUEST", nil)
	}
	cursor := ""
	if pageToken != "" {
		token, err := decodeOpaqueToken(pageToken, "page")
		if err != nil || token.ScopeID != scopeID || token.BackupID != "" || !validIdentifier(token.Cursor) {
			return ListResult{}, repositoryFailure(CodeRepositoryDenied, "List", scopeID, "", "INVALID_PAGE_TOKEN", err)
		}
		cursor = token.Cursor
	}
	records, err := r.listRecords(ctx, scopeID)
	if err != nil {
		return ListResult{}, err
	}
	refs := make([]ManifestRef, 0, len(records))
	for _, record := range records {
		if record.State == LifecyclePublished && record.Manifest != nil {
			if record.Manifest.ScopeID != scopeID || record.Manifest.BackupID != record.BackupID {
				return ListResult{}, repositoryFailure(CodeRepositoryDenied, "List", scopeID, record.BackupID, "MANIFEST_REF_SCOPE_MISMATCH", nil)
			}
			refs = append(refs, *record.Manifest)
		}
	}
	sort.Slice(refs, func(left, right int) bool { return refs[left].BackupID < refs[right].BackupID })
	start := sort.Search(len(refs), func(index int) bool { return refs[index].BackupID > cursor })
	end := start + pageSize
	if end > len(refs) {
		end = len(refs)
	}
	result := ListResult{Items: append([]ManifestRef(nil), refs[start:end]...)}
	if end < len(refs) {
		result.NextPageToken, err = encodeOpaqueToken("page", scopeID, "", refs[end-1].BackupID)
		if err != nil {
			return ListResult{}, err
		}
	}
	return result, nil
}

func (r *repository) DeleteUnpublished(ctx context.Context, attemptID string) (CleanupReport, error) {
	if err := contextErr(ctx); err != nil {
		return CleanupReport{}, err
	}
	token, err := decodeOpaqueToken(attemptID, "attempt")
	if err != nil {
		return CleanupReport{}, err
	}
	record, value, err := r.loadRecord(ctx, token.ScopeID, token.BackupID, "DeleteUnpublished")
	if err != nil {
		return CleanupReport{}, err
	}
	report := CleanupReport{AttemptID: attemptID, ScopeID: token.ScopeID, BackupID: token.BackupID, State: record.State}
	if record.AttemptID != attemptID || record.State == LifecyclePublished || record.Manifest != nil && record.State == LifecycleDeleting {
		return report, repositoryFailure(CodeRepositoryDenied, "DeleteUnpublished", token.ScopeID, token.BackupID, "BACKUP_NOT_CLEANABLE", nil)
	}
	if record.State == LifecycleDeleted {
		return report, nil
	}
	if record.State != LifecycleDeleting {
		if record.State != LifecycleReserved && record.State != LifecycleUploading && record.State != LifecycleQuarantined {
			return report, repositoryFailure(CodeRepositoryDenied, "DeleteUnpublished", token.ScopeID, token.BackupID, "ILLEGAL_STATE_TRANSITION", nil)
		}
		record.State, record.UpdatedAt = LifecycleDeleting, normalizeTime(r.now())
		value, err = r.casRecord(ctx, backupRecordKey(token.BackupID), value.Revision, record, CodeCleanupIncomplete, "DeleteUnpublished")
		if err != nil {
			reloaded, _, reloadErr := r.loadRecord(ctx, token.ScopeID, token.BackupID, "DeleteUnpublished")
			if reloadErr == nil && reloaded.State == LifecyclePublished {
				return report, repositoryFailure(CodeRepositoryDenied, "DeleteUnpublished", token.ScopeID, token.BackupID, "BACKUP_PUBLISHED", nil)
			}
			return report, err
		}
		report.State = LifecycleDeleting
	}
	for _, object := range deletionOrder(record.Planned) {
		if err := contextErr(ctx); err != nil {
			return report, err
		}
		deleteErr := r.deleteObject(ctx, token.ScopeID, object.Ref.ObjectID)
		if errors.Is(deleteErr, context.Canceled) || errors.Is(deleteErr, context.DeadlineExceeded) { return report, deleteErr }
		if deleteErr == nil {
			report.DeletedKeys = append(report.DeletedKeys, object.Ref.ObjectID)
		} else {
			report.ResidualKeys = append(report.ResidualKeys, object.Ref.ObjectID)
		}
	}
	if len(report.ResidualKeys) != 0 {
		record.State = LifecycleQuarantined
		record.QuarantineReason = "CLEANUP_INCOMPLETE"
		record.ResidualKeys = append([]string(nil), report.ResidualKeys...)
		record.UpdatedAt = normalizeTime(r.now())
		report.State, report.Reason = record.State, record.QuarantineReason
		_, casErr := r.casRecord(ctx, backupRecordKey(token.BackupID), value.Revision, record, CodeCleanupIncomplete, "DeleteUnpublished")
		return report, repositoryFailure(CodeCleanupIncomplete, "DeleteUnpublished", token.ScopeID, token.BackupID, "RESIDUAL_OBJECTS", casErr)
	}
	tombstone := Tombstone{ScopeID: token.ScopeID, BackupID: token.BackupID, Reason: "unpublished-cleanup", DeletedAt: normalizeTime(r.now())}
	record.State, record.Tombstone = LifecycleDeleted, &tombstone
	record.ResidualKeys, record.QuarantineReason = nil, ""
	record.UpdatedAt = tombstone.DeletedAt
	report.State = LifecycleDeleted
	if _, err := r.casRecord(ctx, backupRecordKey(token.BackupID), value.Revision, record, CodeCleanupIncomplete, "DeleteUnpublished"); err != nil {
		return report, err
	}
	return report, nil
}

func (r *repository) Tombstone(ctx context.Context, scopeID, backupID, reason string) (Tombstone, error) {
	if err := contextErr(ctx); err != nil {
		return Tombstone{}, err
	}
	if reason == "" || len(reason) > 1024 {
		return Tombstone{}, repositoryFailure(CodeRepositoryDenied, "Tombstone", scopeID, backupID, "INVALID_REASON", nil)
	}
	record, value, err := r.loadRecord(ctx, scopeID, backupID, "Tombstone")
	if err != nil {
		return Tombstone{}, err
	}
	if record.State == LifecycleDeleted && record.Tombstone != nil {
		return *record.Tombstone, nil
	}
	if record.State != LifecycleDeleting && record.State != LifecycleQuarantined {
		return Tombstone{}, repositoryFailure(CodeRepositoryDenied, "Tombstone", scopeID, backupID, "ILLEGAL_STATE_TRANSITION", nil)
	}
	for _, object := range record.Planned {
		reader, _, openErr := r.blobs.Open(ctx, scopeID, object.Ref.ObjectID)
		if openErr == nil {
			_ = reader.Close()
			return Tombstone{}, repositoryFailure(CodeDeletePartial, "Tombstone", scopeID, backupID, "OBJECT_STILL_PRESENT", nil)
		}
		if !errors.Is(openErr, ErrBackendNotFound) {
			return Tombstone{}, translateBackend(openErr, CodeDeletePartial, "Tombstone", scopeID, backupID)
		}
	}
	tombstone := Tombstone{ScopeID: scopeID, BackupID: backupID, Reason: reason, DeletedAt: normalizeTime(r.now())}
	if record.Manifest != nil {
		tombstone.ManifestSHA256 = record.Manifest.ManifestSHA256
	}
	record.State, record.Tombstone = LifecycleDeleted, &tombstone
	record.ResidualKeys, record.QuarantineReason = nil, ""
	record.UpdatedAt = tombstone.DeletedAt
	if _, err := r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodeDeletePartial, "Tombstone"); err != nil {
		return Tombstone{}, err
	}
	return tombstone, nil
}

func (r *repository) Pin(ctx context.Context, scopeID, backupID, runID string, expiresAt time.Time) (Pin, error) {
	if err := contextErr(ctx); err != nil {
		return Pin{}, err
	}
	now := normalizeTime(r.now())
	if !validIdentifier(runID) || !utcTime(expiresAt) || !expiresAt.After(now) {
		return Pin{}, repositoryFailure(CodePinConflict, "Pin", scopeID, backupID, "INVALID_PIN", nil)
	}
	record, value, err := r.loadRecord(ctx, scopeID, backupID, "Pin")
	if err != nil {
		return Pin{}, err
	}
	if record.State != LifecyclePublished {
		return Pin{}, repositoryFailure(CodePinConflict, "Pin", scopeID, backupID, "BACKUP_NOT_PUBLISHED", nil)
	}
	expiresAt = normalizeTime(expiresAt)
	for _, pin := range record.Pins {
		if pin.RunID == runID {
			if pin.ExpiresAt.Equal(expiresAt) {
				return pin, nil
			}
			return Pin{}, repositoryFailure(CodePinConflict, "Pin", scopeID, backupID, "RUN_PIN_CONFLICT", nil)
		}
	}
	pin := Pin{ScopeID: scopeID, BackupID: backupID, RunID: runID, ExpiresAt: expiresAt, CreatedAt: now}
	record.Pins = append(record.Pins, pin)
	sort.Slice(record.Pins, func(left, right int) bool { return record.Pins[left].RunID < record.Pins[right].RunID })
	record.UpdatedAt = now
	if _, err := r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodePinConflict, "Pin"); err != nil {
		reloaded, _, loadErr := r.loadRecord(ctx, scopeID, backupID, "Pin")
		if loadErr == nil {
			for _, existing := range reloaded.Pins {
				if existing.RunID == runID && existing.ExpiresAt.Equal(expiresAt) { return existing, nil }
			}
		}
		return Pin{}, err
	}
	return pin, nil
}

func (r *repository) Unpin(ctx context.Context, scopeID, backupID, runID string) error {
	if err := contextErr(ctx); err != nil {
		return err
	}
	if !validIdentifier(runID) {
		return repositoryFailure(CodePinConflict, "Unpin", scopeID, backupID, "INVALID_RUN_ID", nil)
	}
	record, value, err := r.loadRecord(ctx, scopeID, backupID, "Unpin")
	if err != nil {
		return err
	}
	if record.State != LifecyclePublished {
		return repositoryFailure(CodePinConflict, "Unpin", scopeID, backupID, "BACKUP_NOT_PUBLISHED", nil)
	}
	index := -1
	for candidate := range record.Pins {
		if record.Pins[candidate].RunID == runID {
			index = candidate
			break
		}
	}
	if index < 0 {
		return nil
	}
	record.Pins = append(record.Pins[:index], record.Pins[index+1:]...)
	record.UpdatedAt = normalizeTime(r.now())
	_, err = r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodePinConflict, "Unpin")
	if err != nil {
		reloaded, _, loadErr := r.loadRecord(ctx, scopeID, backupID, "Unpin")
		if loadErr == nil {
			for _, pin := range reloaded.Pins { if pin.RunID == runID { return err } }
			return nil
		}
	}
	return err
}

func (r *repository) ApplyRetention(ctx context.Context, input RetentionPolicy, dryRun bool) (RetentionResult, error) {
	if err := contextErr(ctx); err != nil {
		return RetentionResult{}, err
	}
	policy, err := validateRetentionPolicy(input)
	if err != nil {
		return RetentionResult{}, err
	}
	if policy.EvaluatedAt.After(normalizeTime(r.now())) {
		return RetentionResult{}, repositoryFailure(CodeUnsafeRetention, "ApplyRetention", policy.ScopeID, "", "EVALUATED_AT_IN_FUTURE", nil)
	}
	records, err := r.listRecords(ctx, policy.ScopeID)
	if err != nil {
		return RetentionResult{}, err
	}
	published := make(map[string]backupRecord)
	for _, record := range records {
		if record.State == LifecyclePublished && record.Manifest != nil {
			published[record.BackupID] = record
		}
	}
	qualified := append([]QualifiedSetRef(nil), policy.QualifiedSets...)
	for _, ref := range qualified {
		if _, ok := published[ref.BackupID]; !ok {
			return RetentionResult{}, repositoryFailure(CodeUnsafeRetention, "ApplyRetention", policy.ScopeID, ref.BackupID, "QUALIFIED_SET_NOT_PUBLISHED", nil)
		}
	}
	if len(qualified) < int(policy.MinimumQualifiedSets) {
		return RetentionResult{}, repositoryFailure(CodeLastQualifiedSet, "ApplyRetention", policy.ScopeID, "", "INSUFFICIENT_QUALIFIED_SETS", nil)
	}
	sort.Slice(qualified, func(left, right int) bool {
		if qualified[left].QualifiedAt.Equal(qualified[right].QualifiedAt) { return qualified[left].BackupID < qualified[right].BackupID }
		return qualified[left].QualifiedAt.Before(qualified[right].QualifiedAt)
	})
	protected := make(map[string]struct{}, policy.MinimumQualifiedSets)
	for index := len(qualified) - int(policy.MinimumQualifiedSets); index < len(qualified); index++ {
		protected[qualified[index].BackupID] = struct{}{}
	}
	ordered := make([]backupRecord, 0, len(published))
	for _, record := range published {
		ordered = append(ordered, record)
	}
	sort.Slice(ordered, func(left, right int) bool {
		if ordered[left].Manifest.PublishedAt.Equal(ordered[right].Manifest.PublishedAt) {
			return ordered[left].BackupID < ordered[right].BackupID
		}
		return ordered[left].Manifest.PublishedAt.Before(ordered[right].Manifest.PublishedAt)
	})
	result := RetentionResult{ScopeID: policy.ScopeID, EvaluatedAt: policy.EvaluatedAt, DryRun: dryRun}
	for _, record := range ordered {
		_, keep := protected[record.BackupID]
		pinned := activePin(record.Pins, policy.EvaluatedAt)
		if pinned {
			result.SkippedPinned = append(result.SkippedPinned, record.BackupID)
		}
		if keep {
			result.PreservedLastQualified = append(result.PreservedLastQualified, record.BackupID)
		}
		eligibleAt := record.Manifest.PublishedAt.Add(time.Duration(policy.MinimumAgeSeconds) * time.Second)
		if !pinned && !keep && !eligibleAt.After(policy.EvaluatedAt) {
			result.WouldDelete = append(result.WouldDelete, record.BackupID)
		}
	}
	if dryRun {
		return result, nil
	}
	for _, backupID := range result.WouldDelete {
		record, value, loadErr := r.loadRecord(ctx, policy.ScopeID, backupID, "ApplyRetention")
		if loadErr != nil {
			return result, loadErr
		}
		if record.State != LifecyclePublished || record.Manifest == nil || activePin(record.Pins, policy.EvaluatedAt) {
			continue
		}
		if _, keep := protected[backupID]; keep {
			return result, repositoryFailure(CodeLastQualifiedSet, "ApplyRetention", policy.ScopeID, backupID, "QUALIFIED_SET_RECHECK_FAILED", nil)
		}
		record.State, record.UpdatedAt = LifecycleDeleting, normalizeTime(r.now())
		value, err = r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodeDeletePartial, "ApplyRetention")
		if err != nil {
			reloaded, _, reloadErr := r.loadRecord(ctx, policy.ScopeID, backupID, "ApplyRetention")
			if reloadErr == nil && activePin(reloaded.Pins, policy.EvaluatedAt) {
				result.SkippedPinned = appendUnique(result.SkippedPinned, backupID)
				continue
			}
			return result, err
		}
		residual := make([]string, 0)
		for _, object := range deletionOrder(record.Planned) {
			deleteErr := r.deleteObject(ctx, policy.ScopeID, object.Ref.ObjectID)
			if errors.Is(deleteErr, context.Canceled) || errors.Is(deleteErr, context.DeadlineExceeded) { return result, deleteErr }
			if deleteErr != nil { residual = append(residual, object.Ref.ObjectID) }
		}
		if len(residual) != 0 {
			record.State, record.QuarantineReason, record.ResidualKeys = LifecycleQuarantined, "DELETE_PARTIAL", residual
			record.UpdatedAt = normalizeTime(r.now())
			_, casErr := r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodeDeletePartial, "ApplyRetention")
			result.Quarantined = append(result.Quarantined, backupID)
			return result, repositoryFailure(CodeDeletePartial, "ApplyRetention", policy.ScopeID, backupID, "RESIDUAL_OBJECTS", casErr)
		}
		tombstone := Tombstone{ScopeID: policy.ScopeID, BackupID: backupID, Reason: "retention", ManifestSHA256: record.Manifest.ManifestSHA256, DeletedAt: normalizeTime(r.now())}
		record.State, record.Tombstone, record.UpdatedAt = LifecycleDeleted, &tombstone, tombstone.DeletedAt
		record.ResidualKeys, record.QuarantineReason = nil, ""
		if _, err := r.casRecord(ctx, backupRecordKey(backupID), value.Revision, record, CodeDeletePartial, "ApplyRetention"); err != nil {
			return result, err
		}
		result.Deleted = append(result.Deleted, backupID)
	}
	return result, nil
}

func (r *repository) loadRecord(ctx context.Context, scopeID, backupID, operation string) (backupRecord, MetadataValue, error) {
	if !validIdentifier(scopeID) || !validIdentifier(backupID) {
		return backupRecord{}, MetadataValue{}, repositoryFailure(CodeRepositoryDenied, operation, scopeID, backupID, "INVALID_SCOPE_OR_BACKUP_ID", nil)
	}
	value, err := r.metadata.Get(ctx, scopeID, backupRecordKey(backupID))
	if err != nil {
		return backupRecord{}, MetadataValue{}, translateBackend(err, CodeRepositoryDenied, operation, scopeID, backupID)
	}
	record, err := decodeRecord(value.Bytes)
	if err != nil || record.ScopeID != scopeID || record.BackupID != backupID {
		return backupRecord{}, MetadataValue{}, repositoryFailure(CodeRepositoryDenied, operation, scopeID, backupID, "INVALID_METADATA_RECORD", err)
	}
	return record, value, nil
}

func (r *repository) casRecord(ctx context.Context, key, expected string, record backupRecord, fallback ErrorCode, operation string) (MetadataValue, error) {
	raw, err := json.Marshal(record)
	if err != nil || len(raw) > maxMetadataBytes {
		return MetadataValue{}, repositoryFailure(CodeRepositoryDenied, operation, record.ScopeID, record.BackupID, "METADATA_ENCODING_FAILED", err)
	}
	value, err := r.casBytes(ctx, record.ScopeID, key, expected, raw)
	if err != nil {
		return MetadataValue{}, translateBackend(err, fallback, operation, record.ScopeID, record.BackupID)
	}
	return value, nil
}

func (r *repository) casBytes(ctx context.Context, scopeID, key, expected string, raw []byte) (MetadataValue, error) {
	value, err := r.metadata.CompareAndSwap(ctx, scopeID, key, expected, append([]byte(nil), raw...))
	if err == nil {
		return value, nil
	}
	if errors.Is(err, ErrBackendAmbiguous) {
		current, getErr := r.metadata.Get(ctx, scopeID, key)
		if getErr == nil && bytes.Equal(current.Bytes, raw) {
			return current, nil
		}
	}
	return MetadataValue{}, err
}

func decodeRecord(raw []byte) (backupRecord, error) {
	if len(raw) == 0 || len(raw) > maxMetadataBytes {
		return backupRecord{}, errors.New("invalid metadata size")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var record backupRecord
	if err := decoder.Decode(&record); err != nil {
		return backupRecord{}, err
	}
	if err := requireJSONEOF(decoder); err != nil || record.Version != 1 {
		return backupRecord{}, errors.New("invalid metadata record")
	}
	return record, nil
}

func (r *repository) listRecords(ctx context.Context, scopeID string) ([]backupRecord, error) {
	records := make([]backupRecord, 0)
	token := ""
	seenTokens := make(map[string]struct{})
	for {
		page, err := r.metadata.List(ctx, scopeID, "backups/", 100, token)
		if err != nil {
			return nil, translateBackend(err, CodeRepositoryDenied, "List", scopeID, "")
		}
		for _, entry := range page.Entries {
			record, decodeErr := decodeRecord(entry.Value.Bytes)
			if decodeErr != nil || entry.Key != backupRecordKey(record.BackupID) || record.ScopeID != scopeID {
				return nil, repositoryFailure(CodeRepositoryDenied, "List", scopeID, "", "INVALID_METADATA_RECORD", decodeErr)
			}
			records = append(records, record)
		}
		if page.NextPageToken == "" {
			break
		}
		if _, repeated := seenTokens[page.NextPageToken]; repeated {
			return nil, repositoryFailure(CodeRepositoryDenied, "List", scopeID, "", "REPEATED_BACKEND_PAGE_TOKEN", nil)
		}
		seenTokens[page.NextPageToken], token = struct{}{}, page.NextPageToken
	}
	return records, nil
}

func (r *repository) deleteObject(ctx context.Context, scopeID, key string) error {
	err := r.blobs.Delete(ctx, scopeID, key)
	if err == nil || errors.Is(err, ErrBackendNotFound) { return nil }
	if !errors.Is(err, ErrBackendAmbiguous) { return err }
	reader, _, openErr := r.blobs.Open(ctx, scopeID, key)
	if errors.Is(openErr, ErrBackendNotFound) { return nil }
	if openErr != nil { return openErr }
	_ = reader.Close()
	return err
}

func (r *repository) verifyObject(ctx context.Context, scopeID, backupID string, ref ObjectRef) error {
	reader, metadata, err := r.blobs.Open(ctx, scopeID, ref.ObjectID)
	if err != nil {
		return translateBackend(err, CodeDigestMismatch, "verifyObject", scopeID, backupID)
	}
	defer reader.Close()
	if metadata.SHA256 != ref.SHA256 || metadata.SizeBytes != ref.SizeBytes || metadata.MediaType != ref.MediaType {
		return repositoryFailure(CodeDigestMismatch, "verifyObject", scopeID, backupID, "OBJECT_METADATA_MISMATCH", nil)
	}
	hasher := sha256.New()
	buffer := make([]byte, streamChunkBytes)
	var count uint64
	for {
		if err := contextErr(ctx); err != nil {
			return err
		}
		read, readErr := reader.Read(buffer)
		if read > 0 {
			count += uint64(read)
			_, _ = hasher.Write(buffer[:read])
			if count > ref.SizeBytes {
				return repositoryFailure(CodeDigestMismatch, "verifyObject", scopeID, backupID, "OBJECT_SIZE_MISMATCH", nil)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return repositoryFailure(CodeDigestMismatch, "verifyObject", scopeID, backupID, "OBJECT_READ_FAILED", readErr)
		}
		if read == 0 {
			return repositoryFailure(CodeDigestMismatch, "verifyObject", scopeID, backupID, "OBJECT_READER_STALLED", nil)
		}
	}
	digest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if count != ref.SizeBytes || digest != ref.SHA256 {
		return repositoryFailure(CodeDigestMismatch, "verifyObject", scopeID, backupID, "OBJECT_DIGEST_MISMATCH", nil)
	}
	return nil
}

func (r *repository) readManifest(ctx context.Context, ref ManifestRef) ([]byte, error) {
	reader, metadata, err := r.blobs.Open(ctx, ref.ScopeID, ref.ObjectID)
	if err != nil {
		return nil, translateBackend(err, CodeDigestMismatch, "Read", ref.ScopeID, ref.BackupID)
	}
	defer reader.Close()
	if ref.MediaType != MediaTypeManifest || metadata.SHA256 != ref.ObjectSHA256 || metadata.SizeBytes != ref.SizeBytes || metadata.MediaType != MediaTypeManifest || ref.SizeBytes > maxManifestBytes {
		return nil, repositoryFailure(CodeDigestMismatch, "Read", ref.ScopeID, ref.BackupID, "MANIFEST_METADATA_MISMATCH", nil)
	}
	raw := make([]byte, 0, ref.SizeBytes)
	buffer := make([]byte, 32<<10)
	for uint64(len(raw)) <= ref.SizeBytes {
		read, readErr := reader.Read(buffer)
		raw = append(raw, buffer[:read]...)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil || read == 0 {
			return nil, repositoryFailure(CodeDigestMismatch, "Read", ref.ScopeID, ref.BackupID, "MANIFEST_READ_FAILED", readErr)
		}
	}
	sum := sha256.Sum256(raw)
	if uint64(len(raw)) != ref.SizeBytes || "sha256:"+hex.EncodeToString(sum[:]) != ref.ObjectSHA256 {
		return nil, repositoryFailure(CodeDigestMismatch, "Read", ref.ScopeID, ref.BackupID, "MANIFEST_DIGEST_MISMATCH", nil)
	}
	return raw, nil
}

func matchCommittedObjects(record backupRecord, set BackupSet) error {
	dataObjects := 0
	for _, object := range record.Planned {
		if object.Kind != "manifest" { dataObjects++ }
	}
	if dataObjects != len(set.Artifacts)+1 {
		return repositoryFailure(CodePartialPublication, "Publish", set.ScopeID, set.BackupID, "UPLOAD_SET_MISMATCH", nil)
	}
	for _, artifact := range set.Artifacts {
		index := plannedIndex(record.Planned, "shard", artifact.ShardID)
		if index < 0 || !record.Planned[index].Committed || record.Planned[index].Ref != artifact.Object {
			return repositoryFailure(CodePartialPublication, "Publish", set.ScopeID, set.BackupID, "SHARD_NOT_COMMITTED", nil)
		}
	}
	index := plannedIndex(record.Planned, "catalog", 0)
	if index < 0 || !record.Planned[index].Committed || record.Planned[index].Ref != set.CatalogObject {
		return repositoryFailure(CodePartialPublication, "Publish", set.ScopeID, set.BackupID, "CATALOG_NOT_COMMITTED", nil)
	}
	return nil
}

func reservationMatchesSet(reservation Reservation, set BackupSet) bool {
	return reservation.ScopeID == set.ScopeID &&
		reservation.BackupID == set.BackupID &&
		reservation.SourceEnvironmentID == set.SourceEnvironmentID &&
		reservation.ContractRef == set.ContractRef &&
		reservation.ScheduleWindow.Start.Equal(set.ScheduleWindow.Start) &&
		reservation.ScheduleWindow.End.Equal(set.ScheduleWindow.End)
}

func plannedIndex(objects []plannedObject, kind string, shardID uint32) int {
	for index := range objects {
		if objects[index].Kind == kind && objects[index].ShardID == shardID {
			return index
		}
	}
	return -1
}

func sortPlanned(objects []plannedObject) {
	order := func(kind string) int {
		switch kind {
		case "shard":
			return 0
		case "catalog":
			return 1
		default:
			return 2
		}
	}
	sort.Slice(objects, func(left, right int) bool {
		if order(objects[left].Kind) != order(objects[right].Kind) {
			return order(objects[left].Kind) < order(objects[right].Kind)
		}
		return objects[left].ShardID < objects[right].ShardID
	})
}

func deletionOrder(objects []plannedObject) []plannedObject {
	ordered := append([]plannedObject(nil), objects...)
	sortPlanned(ordered)
	return ordered
}

func activePin(pins []Pin, at time.Time) bool {
	for _, pin := range pins {
		if pin.ExpiresAt.After(at) {
			return true
		}
	}
	return false
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func contextErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func translateBackend(err error, fallback ErrorCode, operation, scopeID, backupID string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code, reason := fallback, "BACKEND_FAILURE"
	switch {
	case errors.Is(err, ErrBackendDenied), errors.Is(err, ErrBackendInvalidToken), errors.Is(err, ErrBackendNotFound):
		code, reason = CodeRepositoryDenied, "BACKEND_DENIED"
	case errors.Is(err, ErrBackendDigestMismatch):
		code, reason = CodeDigestMismatch, "BACKEND_DIGEST_MISMATCH"
	case errors.Is(err, ErrBackendConflict):
		reason = "CAS_CONFLICT"
	case errors.Is(err, ErrBackendAmbiguous):
		reason = "AMBIGUOUS_ACKNOWLEDGEMENT"
	}
	return repositoryFailure(code, operation, scopeID, backupID, reason, err)
}

func getErrCode(err error) error {
	var failure *RepositoryError
	if errors.As(err, &failure) {
		return failure.Cause
	}
	return err
}

type validatingReader struct {
	source    io.Reader
	hasher    hash.Hash
	expected  string
	size      uint64
	bytesRead uint64
	complete  bool
}

func newValidatingReader(source io.Reader, digest string, size uint64) *validatingReader {
	return &validatingReader{source: source, hasher: sha256.New(), expected: digest, size: size}
}

func (reader *validatingReader) Read(buffer []byte) (int, error) {
	if len(buffer) > streamChunkBytes {
		buffer = buffer[:streamChunkBytes]
	}
	read, err := reader.source.Read(buffer)
	if read > 0 {
		reader.bytesRead += uint64(read)
		_, _ = reader.hasher.Write(buffer[:read])
	}
	if reader.bytesRead > reader.size {
		return read, ErrBackendDigestMismatch
	}
	if errors.Is(err, io.EOF) {
		reader.complete = true
		digest := "sha256:" + hex.EncodeToString(reader.hasher.Sum(nil))
		if reader.bytesRead != reader.size || digest != reader.expected {
			return read, ErrBackendDigestMismatch
		}
	}
	return read, err
}
