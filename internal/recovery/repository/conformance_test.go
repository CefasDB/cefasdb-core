package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	recoverymodel "github.com/CefasDb/cefasdb/internal/recovery/model"
)

type memoryBlob struct {
	path string
	meta BlobMetadata
}
type memoryBlobStore struct {
	mu                 sync.Mutex
	values             map[string]memoryBlob
	dir                string
	failPutBefore      map[string]bool
	ambiguousPut       map[string]bool
	ambiguousDelete    map[string]bool
	failDelete         map[string]bool
	afterPut           map[string]func()
	cancelDelete       context.CancelFunc
	cancelAfterDeletes int
	deletes            int
}

func newMemoryBlobStore(t testing.TB) *memoryBlobStore {
	t.Helper()
	return &memoryBlobStore{
		values:          map[string]memoryBlob{},
		dir:             t.TempDir(),
		failPutBefore:   map[string]bool{},
		ambiguousPut:    map[string]bool{},
		ambiguousDelete: map[string]bool{},
		failDelete:      map[string]bool{},
		afterPut:        map[string]func(){},
	}
}
func scoped(scope, key string) string { return scope + "\x00" + key }
func (store *memoryBlobStore) PutIfAbsent(ctx context.Context, scope, key string, meta BlobMetadata, body io.Reader) (BlobMetadata, error) {
	if err := ctx.Err(); err != nil {
		return BlobMetadata{}, err
	}
	if !strings.HasPrefix(key, "scopes/"+scope+"/") {
		return BlobMetadata{}, ErrBackendDenied
	}
	id := scoped(scope, key)
	store.mu.Lock()
	failBefore := store.failPutBefore[id]
	store.mu.Unlock()
	if failBefore {
		return BlobMetadata{}, errors.New("injected put failure")
	}
	temporary, err := os.CreateTemp(store.dir, "upload-*")
	if err != nil {
		return BlobMetadata{}, err
	}
	path := temporary.Name()
	keep := false
	defer func() {
		_ = temporary.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()
	hasher := sha256.New()
	written, err := io.CopyBuffer(io.MultiWriter(temporary, hasher), body, make([]byte, streamChunkBytes))
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return BlobMetadata{}, err
	}
	digest := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if written < 0 || uint64(written) != meta.SizeBytes || digest != meta.SHA256 {
		return BlobMetadata{}, ErrBackendDigestMismatch
	}
	store.mu.Lock()
	if existing, ok := store.values[id]; ok {
		store.mu.Unlock()
		if existing.meta != meta {
			return BlobMetadata{}, ErrBackendConflict
		}
		return existing.meta, nil
	}
	store.values[id] = memoryBlob{path: path, meta: meta}
	keep = true
	hook := store.afterPut[id]
	delete(store.afterPut, id)
	ambiguous := store.ambiguousPut[id]
	if ambiguous {
		delete(store.ambiguousPut, id)
	}
	store.mu.Unlock()
	if hook != nil {
		hook()
	}
	if ambiguous {
		return BlobMetadata{}, ErrBackendAmbiguous
	}
	return meta, nil
}
func (store *memoryBlobStore) Open(ctx context.Context, scope, key string) (io.ReadCloser, BlobMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, BlobMetadata{}, err
	}
	store.mu.Lock()
	value, ok := store.values[scoped(scope, key)]
	if !ok {
		store.mu.Unlock()
		return nil, BlobMetadata{}, ErrBackendNotFound
	}
	file, err := os.Open(value.path)
	store.mu.Unlock()
	if errors.Is(err, os.ErrNotExist) {
		return nil, BlobMetadata{}, ErrBackendNotFound
	}
	if err != nil {
		return nil, BlobMetadata{}, err
	}
	return file, value.meta, nil
}
func (store *memoryBlobStore) Delete(ctx context.Context, scope, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id := scoped(scope, key)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.cancelDelete != nil && store.deletes >= store.cancelAfterDeletes {
		cancel := store.cancelDelete
		store.cancelDelete = nil
		cancel()
		return ctx.Err()
	}
	if store.failDelete[id] {
		return errors.New("injected delete failure")
	}
	value, ok := store.values[id]
	if !ok {
		return ErrBackendNotFound
	}
	if err := os.Remove(value.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	delete(store.values, id)
	store.deletes++
	if store.ambiguousDelete[id] {
		delete(store.ambiguousDelete, id)
		return ErrBackendAmbiguous
	}
	return nil
}
func (store *memoryBlobStore) tamper(scope, key string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	value := store.values[scoped(scope, key)]
	file, err := os.OpenFile(value.path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	first := []byte{0}
	if _, err := file.ReadAt(first, 0); err != nil {
		return err
	}
	first[0] ^= 0xff
	if _, err := file.WriteAt(first, 0); err != nil {
		return err
	}
	return nil
}
func (store *memoryBlobStore) deleteCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.deletes
}

type memoryMetadata struct {
	data     []byte
	revision string
}
type memoryMetadataStore struct {
	mu         sync.Mutex
	values     map[string]memoryMetadata
	next       uint64
	failBefore map[string]bool
	ambiguous  map[string]bool
}

func newMemoryMetadataStore() *memoryMetadataStore {
	return &memoryMetadataStore{values: map[string]memoryMetadata{}, failBefore: map[string]bool{}, ambiguous: map[string]bool{}}
}
func (store *memoryMetadataStore) Get(ctx context.Context, scope, key string) (MetadataValue, error) {
	if err := ctx.Err(); err != nil {
		return MetadataValue{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	value, ok := store.values[scoped(scope, key)]
	if !ok {
		return MetadataValue{}, ErrBackendNotFound
	}
	return MetadataValue{Bytes: append([]byte(nil), value.data...), Revision: value.revision}, nil
}
func (store *memoryMetadataStore) CompareAndSwap(ctx context.Context, scope, key, expected string, next []byte) (MetadataValue, error) {
	if err := ctx.Err(); err != nil {
		return MetadataValue{}, err
	}
	id := scoped(scope, key)
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failBefore[id] {
		return MetadataValue{}, errors.New("injected CAS failure")
	}
	current, exists := store.values[id]
	if expected == "" && exists || expected != "" && (!exists || current.revision != expected) {
		return MetadataValue{}, ErrBackendConflict
	}
	store.next++
	value := memoryMetadata{data: append([]byte(nil), next...), revision: strconv.FormatUint(store.next, 10)}
	store.values[id] = value
	result := MetadataValue{Bytes: append([]byte(nil), value.data...), Revision: value.revision}
	if store.ambiguous[id] {
		delete(store.ambiguous, id)
		return MetadataValue{}, ErrBackendAmbiguous
	}
	return result, nil
}
func (store *memoryMetadataStore) List(ctx context.Context, scope, prefix string, pageSize int, token string) (MetadataPage, error) {
	if err := ctx.Err(); err != nil {
		return MetadataPage{}, err
	}
	start := 0
	var err error
	if token != "" {
		start, err = strconv.Atoi(token)
		if err != nil {
			return MetadataPage{}, ErrBackendInvalidToken
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	keys := make([]string, 0)
	scopePrefix := scope + "\x00" + prefix
	for key := range store.values {
		if strings.HasPrefix(key, scopePrefix) {
			keys = append(keys, strings.TrimPrefix(key, scope+"\x00"))
		}
	}
	sort.Strings(keys)
	if start < 0 || start > len(keys) {
		return MetadataPage{}, ErrBackendInvalidToken
	}
	end := start + pageSize
	if end > len(keys) {
		end = len(keys)
	}
	page := MetadataPage{}
	for _, key := range keys[start:end] {
		value := store.values[scoped(scope, key)]
		page.Entries = append(page.Entries, MetadataEntry{Key: key, Value: MetadataValue{Bytes: append([]byte(nil), value.data...), Revision: value.revision}})
	}
	if end < len(keys) {
		page.NextPageToken = strconv.Itoa(end)
	}
	return page, nil
}
func (store *memoryMetadataStore) ambiguousNext(scope, key string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.ambiguous[scoped(scope, key)] = true
}

type boundedReader struct {
	data   []byte
	offset int
}

type zeroReader struct {
	remaining uint64
	maxRead   int
}

func (reader *zeroReader) Read(buffer []byte) (int, error) {
	if reader.remaining == 0 {
		return 0, io.EOF
	}
	read := len(buffer)
	if uint64(read) > reader.remaining {
		read = int(reader.remaining)
	}
	clear(buffer[:read])
	reader.remaining -= uint64(read)
	if read > reader.maxRead {
		reader.maxRead = read
	}
	return read, nil
}
func (reader *boundedReader) Read(buffer []byte) (int, error) {
	if len(buffer) > streamChunkBytes {
		return 0, errors.New("read buffer exceeded 64 KiB")
	}
	if reader.offset == len(reader.data) {
		return 0, io.EOF
	}
	read := copy(buffer, reader.data[reader.offset:])
	reader.offset += read
	return read, nil
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func digestZeroes(size uint64) string {
	hasher := sha256.New()
	reader := &zeroReader{remaining: size}
	_, _ = io.CopyBuffer(hasher, reader, make([]byte, streamChunkBytes))
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil))
}
func shardData(backup string, shard uint32) []byte {
	return []byte(fmt.Sprintf("shard:%s:%d", backup, shard))
}
func catalogData(backup string) []byte { return []byte("catalog:" + backup) }

func fixtureSet(backup string, publishedAt time.Time, shardCount int) BackupSet {
	scope := "scope-a"
	set := BackupSet{
		SchemaVersion:       ManifestSchemaVersion,
		ScopeID:             scope,
		BackupID:            backup,
		ContractRef:         recoverymodel.ContractRef{ID: "recovery-v1", Revision: 1, SHA256: digestBytes([]byte("contract-v1"))},
		SourceEnvironmentID: "source-a",
		SourceRelease: recoverymodel.ReleaseIdentity{
			Version:       "1.2.3",
			ImageDigest:   digestBytes([]byte("release-1.2.3")),
			SourceCommit:  "0123456789abcdef0123456789abcdef01234567",
			ProvenanceURI: "https://codecompany.com.br/provenance/1.2.3",
		},
		ScheduleWindow: ScheduleWindow{Start: publishedAt.Add(-2 * time.Hour), End: publishedAt.Add(-time.Hour)},
		RecoveryPoint:  RecoveryPoint{BarrierID: "barrier-" + backup, SourceEnvironmentID: "source-a", PlacementEpoch: 7, AdmittedAt: publishedAt.Add(-10 * time.Minute), RecoveryPointAt: publishedAt.Add(-9 * time.Minute)},
		PublishedAt:    publishedAt,
	}
	for shard := 0; shard < shardCount; shard++ {
		id := uint32(shard)
		replica := fmt.Sprintf("replica-%d", shard)
		data := shardData(backup, id)
		ref := ObjectRef{ObjectID: shardObjectKey(scope, backup, id, digestBytes(data)), SHA256: digestBytes(data), SizeBytes: uint64(len(data)), MediaType: MediaTypeShard}
		set.ExpectedShardIDs = append(set.ExpectedShardIDs, id)
		set.RecoveryPoint.Shards = append(set.RecoveryPoint.Shards, ShardPoint{ShardID: id, PlacementEpoch: 7, ReplicaID: replica, RaftAppliedIndex: uint64(shard), ChangeIndex: uint64(shard)})
		set.Artifacts = append(set.Artifacts, ShardArtifact{ShardID: id, PlacementEpoch: 7, ReplicaID: replica, Object: ref, RowStats: []TableStat{{Table: "table-a", Rows: uint64(shard), FNV1A64: "0123456789abcdef"}}})
	}
	catalog := catalogData(backup)
	set.CatalogObject = ObjectRef{ObjectID: catalogObjectKey(scope, backup, digestBytes(catalog)), SHA256: digestBytes(catalog), SizeBytes: uint64(len(catalog)), MediaType: MediaTypeCatalog}
	return set
}
func mustRepository(t testing.TB, blobs BlobStore, metadata MetadataStore, now *time.Time) Repository {
	t.Helper()
	repo, err := New(blobs, metadata, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	return repo
}
func prepareAttempt(t testing.TB, repo Repository, set BackupSet, uploadShards int) ReservationResult {
	t.Helper()
	reservation := Reservation{ScopeID: set.ScopeID, BackupID: set.BackupID, SourceEnvironmentID: set.SourceEnvironmentID, ContractRef: set.ContractRef, ScheduleWindow: set.ScheduleWindow}
	result, err := repo.Reserve(context.Background(), reservation)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < uploadShards; index++ {
		artifact := set.Artifacts[index]
		data := shardData(set.BackupID, artifact.ShardID)
		ref, putErr := repo.PutShard(context.Background(), set.ScopeID, set.BackupID, ShardUpload{AttemptID: result.AttemptID, ShardID: artifact.ShardID, SHA256: artifact.Object.SHA256, SizeBytes: uint64(len(data)), MediaType: MediaTypeShard, Body: &boundedReader{data: data}})
		if putErr != nil {
			t.Fatal(putErr)
		}
		if ref != artifact.Object {
			t.Fatalf("unexpected shard ref: %#v", ref)
		}
	}
	catalog := catalogData(set.BackupID)
	if _, err := repo.PutCatalog(context.Background(), set.ScopeID, set.BackupID, CatalogUpload{AttemptID: result.AttemptID, SHA256: set.CatalogObject.SHA256, SizeBytes: uint64(len(catalog)), MediaType: MediaTypeCatalog, Body: &boundedReader{data: catalog}}); err != nil {
		t.Fatal(err)
	}
	return result
}
func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func retentionPolicy(set BackupSet, evidence string, qualifiedAt, evaluatedAt time.Time) RetentionPolicy {
	return RetentionPolicy{
		ScopeID:              set.ScopeID,
		ContractRef:          set.ContractRef,
		MinimumAgeSeconds:    1,
		MinimumQualifiedSets: 1,
		EvaluatedAt:          evaluatedAt,
		QualifiedSets: []QualifiedSetRef{{
			BackupID:       set.BackupID,
			ContractRef:    set.ContractRef,
			EvidenceSHA256: digestBytes([]byte(evidence)),
			QualifiedAt:    qualifiedAt,
		}},
	}
}

func TestConformance_CompleteTwelveShardPublish(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-12", now.Add(-time.Minute), 12)
	blobs.mu.Lock()
	blobs.ambiguousPut[scoped(set.ScopeID, set.Artifacts[0].Object.ObjectID)] = true
	blobs.ambiguousPut[scoped(set.ScopeID, manifestObjectKey(set.ScopeID, set.BackupID))] = true
	blobs.mu.Unlock()
	prepareAttempt(t, repo, set, 12)
	metadata.ambiguousNext(set.ScopeID, backupRecordKey(set.BackupID))
	ref, err := repo.Publish(context.Background(), set)
	if err != nil {
		t.Fatal(err)
	}
	read, err := repo.Read(context.Background(), set.ScopeID, set.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	if read.ManifestSHA256 != ref.ManifestSHA256 {
		t.Fatalf("digest mismatch: %s != %s", read.ManifestSHA256, ref.ManifestSHA256)
	}
	listed, err := repo.List(context.Background(), set.ScopeID, 100, "")
	if err != nil || len(listed.Items) != 1 || listed.Items[0].BackupID != set.BackupID {
		t.Fatalf("unexpected list: %#v, %v", listed, err)
	}
}

func TestConformance_PartialPublish(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-partial", now.Add(-time.Minute), 12)
	reservation := prepareAttempt(t, repo, set, 11)
	set.Artifacts = set.Artifacts[:11]
	if _, err := repo.Publish(context.Background(), set); !IsCode(err, CodeIncompleteShardCoverage) {
		t.Fatalf("expected coverage error, got %v", err)
	}
	listed, err := repo.List(context.Background(), set.ScopeID, 10, "")
	if err != nil || len(listed.Items) != 0 {
		t.Fatalf("partial set became visible: %#v, %v", listed, err)
	}
	blobs.mu.Lock()
	blobs.ambiguousDelete[scoped(set.ScopeID, set.CatalogObject.ObjectID)] = true
	blobs.mu.Unlock()
	report, err := repo.DeleteUnpublished(context.Background(), reservation.AttemptID)
	if err != nil || report.State != LifecycleDeleted {
		t.Fatalf("cleanup failed: %#v, %v", report, err)
	}
}

func TestConformance_UploadFailureStaysUnpublished(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-put-failure", now.Add(-time.Minute), 1)
	reservation, err := repo.Reserve(context.Background(), Reservation{ScopeID: set.ScopeID, BackupID: set.BackupID, SourceEnvironmentID: set.SourceEnvironmentID, ContractRef: set.ContractRef, ScheduleWindow: set.ScheduleWindow})
	if err != nil {
		t.Fatal(err)
	}
	artifact := set.Artifacts[0]
	data := shardData(set.BackupID, artifact.ShardID)
	blobs.mu.Lock()
	blobs.failPutBefore[scoped(set.ScopeID, artifact.Object.ObjectID)] = true
	blobs.mu.Unlock()
	_, err = repo.PutShard(context.Background(), set.ScopeID, set.BackupID, ShardUpload{AttemptID: reservation.AttemptID, ShardID: artifact.ShardID, SHA256: artifact.Object.SHA256, SizeBytes: uint64(len(data)), MediaType: MediaTypeShard, Body: &boundedReader{data: data}})
	if !IsCode(err, CodePartialPublication) {
		t.Fatalf("expected partial publication, got %v", err)
	}
	listed, listErr := repo.List(context.Background(), set.ScopeID, 10, "")
	if listErr != nil || len(listed.Items) != 0 {
		t.Fatalf("failed upload became visible: %#v, %v", listed, listErr)
	}
	if _, cleanupErr := repo.DeleteUnpublished(context.Background(), reservation.AttemptID); cleanupErr != nil {
		t.Fatalf("failed upload cleanup: %v", cleanupErr)
	}
}

func TestConformance_CleanupWinsUploadRace(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-upload-cleanup-race", now.Add(-time.Minute), 1)
	reservation, err := repo.Reserve(context.Background(), Reservation{
		ScopeID:             set.ScopeID,
		BackupID:            set.BackupID,
		SourceEnvironmentID: set.SourceEnvironmentID,
		ContractRef:         set.ContractRef,
		ScheduleWindow:      set.ScheduleWindow,
	})
	if err != nil {
		t.Fatal(err)
	}

	var cleanup CleanupReport
	var cleanupErr error
	object := set.Artifacts[0]
	blobs.mu.Lock()
	blobs.afterPut[scoped(set.ScopeID, object.Object.ObjectID)] = func() {
		cleanup, cleanupErr = repo.DeleteUnpublished(context.Background(), reservation.AttemptID)
	}
	blobs.mu.Unlock()
	data := shardData(set.BackupID, object.ShardID)
	_, err = repo.PutShard(context.Background(), set.ScopeID, set.BackupID, ShardUpload{
		AttemptID: reservation.AttemptID,
		ShardID:   object.ShardID,
		SHA256:    object.Object.SHA256,
		SizeBytes: object.Object.SizeBytes,
		MediaType: MediaTypeShard,
		Body:      &boundedReader{data: data},
	})
	if !IsCode(err, CodePartialPublication) {
		t.Fatalf("stale upload succeeded: %v", err)
	}
	if cleanupErr != nil || cleanup.State != LifecycleDeleted {
		t.Fatalf("cleanup failed: %#v, %v", cleanup, cleanupErr)
	}
	value, err := metadata.Get(context.Background(), set.ScopeID, backupRecordKey(set.BackupID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeRecord(value.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != LifecycleDeleted {
		t.Fatalf("upload rolled lifecycle back to %s", record.State)
	}
	if _, _, err := blobs.Open(context.Background(), set.ScopeID, object.Object.ObjectID); !errors.Is(err, ErrBackendNotFound) {
		t.Fatalf("stale upload object remained: %v", err)
	}
}

func TestConformance_CleanupWinsPublishRace(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-publish-cleanup-race", now.Add(-time.Minute), 1)
	reservation := prepareAttempt(t, repo, set, 1)
	manifestKey := manifestObjectKey(set.ScopeID, set.BackupID)
	var cleanup CleanupReport
	var cleanupErr error
	blobs.mu.Lock()
	blobs.afterPut[scoped(set.ScopeID, manifestKey)] = func() {
		cleanup, cleanupErr = repo.DeleteUnpublished(context.Background(), reservation.AttemptID)
	}
	blobs.mu.Unlock()
	if _, err := repo.Publish(context.Background(), set); !IsCode(err, CodePartialPublication) {
		t.Fatalf("stale publish succeeded: %v", err)
	}
	if cleanupErr != nil || cleanup.State != LifecycleDeleted {
		t.Fatalf("cleanup failed: %#v, %v", cleanup, cleanupErr)
	}
	value, err := metadata.Get(context.Background(), set.ScopeID, backupRecordKey(set.BackupID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeRecord(value.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != LifecycleDeleted || record.Manifest != nil {
		t.Fatalf("publish rolled lifecycle back: %#v", record)
	}
	listed, err := repo.List(context.Background(), set.ScopeID, 10, "")
	if err != nil || len(listed.Items) != 0 {
		t.Fatalf("stale manifest became visible: %#v, %v", listed, err)
	}
	if _, _, err := blobs.Open(context.Background(), set.ScopeID, manifestKey); !errors.Is(err, ErrBackendNotFound) {
		t.Fatalf("stale manifest remained: %v", err)
	}
}

func TestConformance_LargeUploadUsesBoundedMemory(t *testing.T) {
	const objectSize = uint64(32 << 20)
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-large-stream", now.Add(-time.Minute), 1)
	digest := digestZeroes(objectSize)
	set.Artifacts[0].Object = ObjectRef{
		ObjectID:  shardObjectKey(set.ScopeID, set.BackupID, 0, digest),
		SHA256:    digest,
		SizeBytes: objectSize,
		MediaType: MediaTypeShard,
	}
	reservation, err := repo.Reserve(context.Background(), Reservation{
		ScopeID:             set.ScopeID,
		BackupID:            set.BackupID,
		SourceEnvironmentID: set.SourceEnvironmentID,
		ContractRef:         set.ContractRef,
		ScheduleWindow:      set.ScheduleWindow,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := &zeroReader{remaining: objectSize}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = repo.PutShard(context.Background(), set.ScopeID, set.BackupID, ShardUpload{
		AttemptID: reservation.AttemptID,
		ShardID:   0,
		SHA256:    digest,
		SizeBytes: objectSize,
		MediaType: MediaTypeShard,
		Body:      body,
	})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	if body.maxRead > streamChunkBytes {
		t.Fatalf("read buffer = %d, want <= %d", body.maxRead, streamChunkBytes)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= objectSize/2 {
		t.Fatalf("streaming upload allocated %d bytes for a %d-byte object", allocated, objectSize)
	}
	if _, err := repo.DeleteUnpublished(context.Background(), reservation.AttemptID); err != nil {
		t.Fatal(err)
	}
}

func TestConformance_TamperAndScope(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-tamper", now.Add(-time.Minute), 2)
	prepareAttempt(t, repo, set, 2)
	if _, err := repo.Publish(context.Background(), set); err != nil {
		t.Fatal(err)
	}
	if err := blobs.tamper(set.ScopeID, set.Artifacts[0].Object.ObjectID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Read(context.Background(), set.ScopeID, set.BackupID); !IsCode(err, CodeDigestMismatch) {
		t.Fatalf("expected digest error, got %v", err)
	}
	if _, err := repo.Read(context.Background(), "scope-b", set.BackupID); !IsCode(err, CodeRepositoryDenied) {
		t.Fatalf("expected scope denial, got %v", err)
	}
	manifestSet := fixtureSet("backup-manifest-tamper", now.Add(-2*time.Minute), 1)
	prepareAttempt(t, repo, manifestSet, 1)
	if _, err := repo.Publish(context.Background(), manifestSet); err != nil {
		t.Fatal(err)
	}
	if err := blobs.tamper(manifestSet.ScopeID, manifestObjectKey(manifestSet.ScopeID, manifestSet.BackupID)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Read(context.Background(), manifestSet.ScopeID, manifestSet.BackupID); !IsCode(err, CodeDigestMismatch) {
		t.Fatalf("expected manifest digest error, got %v", err)
	}
}

func TestConformance_PinnedRetentionSurvivesRestart(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-pinned", now.Add(-2*time.Hour), 2)
	prepareAttempt(t, repo, set, 2)
	if _, err := repo.Publish(context.Background(), set); err != nil {
		t.Fatal(err)
	}
	expires := now.Add(time.Hour)
	if _, err := repo.Pin(context.Background(), set.ScopeID, set.BackupID, "run-1", expires); err != nil {
		t.Fatal(err)
	}
	restarted := mustRepository(t, blobs, metadata, &now)
	if _, err := restarted.Pin(context.Background(), set.ScopeID, set.BackupID, "run-1", expires); err != nil {
		t.Fatalf("pin did not survive restart: %v", err)
	}
	if _, err := restarted.Pin(context.Background(), set.ScopeID, set.BackupID, "run-1", expires.Add(time.Minute)); !IsCode(err, CodePinConflict) {
		t.Fatalf("expected pin conflict, got %v", err)
	}
	policy := retentionPolicy(set, "evidence", now.Add(-time.Minute), now)
	for _, dryRun := range []bool{true, false} {
		result, err := restarted.ApplyRetention(context.Background(), policy, dryRun)
		if err != nil || !contains(result.SkippedPinned, set.BackupID) || !contains(result.PreservedLastQualified, set.BackupID) || len(result.Deleted) != 0 {
			t.Fatalf("unsafe retention result: %#v, %v", result, err)
		}
	}
	if blobs.deleteCount() != 0 {
		t.Fatal("retention deleted a protected object")
	}
}

func TestConformance_RetentionQualificationIsContractBound(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	active := fixtureSet("backup-active-contract", now.Add(-4*time.Hour), 1)
	other := fixtureSet("backup-other-contract", now.Add(-2*time.Hour), 1)
	other.ContractRef = recoverymodel.ContractRef{
		ID:       "recovery-v2",
		Revision: 2,
		SHA256:   digestBytes([]byte("contract-v2")),
	}
	for _, set := range []BackupSet{active, other} {
		prepareAttempt(t, repo, set, 1)
		if _, err := repo.Publish(context.Background(), set); err != nil {
			t.Fatal(err)
		}
	}
	policy := retentionPolicy(active, "active-contract-evidence", now.Add(-time.Hour), now)
	result, err := repo.ApplyRetention(context.Background(), policy, true)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(result.PreservedLastQualified, active.BackupID) || contains(result.PreservedLastQualified, other.BackupID) {
		t.Fatalf("wrong contract protected: %#v", result)
	}
	if !contains(result.WouldDelete, other.BackupID) || contains(result.WouldDelete, active.BackupID) {
		t.Fatalf("active contract backup was not preserved: %#v", result)
	}

	mismatched := retentionPolicy(active, "mismatched-contract-evidence", now.Add(-time.Hour), now)
	mismatched.QualifiedSets[0].BackupID = other.BackupID
	if _, err := repo.ApplyRetention(context.Background(), mismatched, true); !IsCode(err, CodeUnsafeRetention) {
		t.Fatalf("cross-contract qualification accepted: %v", err)
	}
}

func TestConformance_CleanupQuarantineAndTombstone(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-cleanup", now.Add(-time.Minute), 2)
	reservation := prepareAttempt(t, repo, set, 1)
	failedKey := scoped(set.ScopeID, set.Artifacts[0].Object.ObjectID)
	blobs.mu.Lock()
	blobs.failDelete[failedKey] = true
	blobs.mu.Unlock()
	report, err := repo.DeleteUnpublished(context.Background(), reservation.AttemptID)
	if !IsCode(err, CodeCleanupIncomplete) || report.State != LifecycleQuarantined || len(report.ResidualKeys) != 1 {
		t.Fatalf("expected quarantine: %#v, %v", report, err)
	}
	blobs.mu.Lock()
	delete(blobs.failDelete, failedKey)
	blobs.mu.Unlock()
	report, err = repo.DeleteUnpublished(context.Background(), reservation.AttemptID)
	if err != nil || report.State != LifecycleDeleted {
		t.Fatalf("cleanup retry failed: %#v, %v", report, err)
	}
	if _, err := repo.Reserve(context.Background(), Reservation{ScopeID: set.ScopeID, BackupID: set.BackupID, SourceEnvironmentID: set.SourceEnvironmentID, ContractRef: set.ContractRef, ScheduleWindow: set.ScheduleWindow}); !IsCode(err, CodeReservationConflict) {
		t.Fatalf("terminal id reused: %v", err)
	}
}

func TestConformance_CleanupCancellationQuarantines(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	set := fixtureSet("backup-cleanup-canceled", now.Add(-time.Minute), 1)
	reservation := prepareAttempt(t, repo, set, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blobs.mu.Lock()
	blobs.cancelDelete = cancel
	blobs.cancelAfterDeletes = 1
	blobs.mu.Unlock()
	report, err := repo.DeleteUnpublished(ctx, reservation.AttemptID)
	if !IsCode(err, CodeCleanupIncomplete) || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected durable cancellation quarantine: %#v, %v", report, err)
	}
	if report.State != LifecycleQuarantined || report.Reason != "DELETE_CANCELED" || len(report.ResidualKeys) != 1 || report.ResidualKeys[0] != set.CatalogObject.ObjectID {
		t.Fatalf("unexpected cleanup report: %#v", report)
	}
	value, err := metadata.Get(context.Background(), set.ScopeID, backupRecordKey(set.BackupID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeRecord(value.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != LifecycleQuarantined || record.QuarantineReason != report.Reason || len(record.ResidualKeys) != 1 || record.ResidualKeys[0] != set.CatalogObject.ObjectID {
		t.Fatalf("cancellation quarantine was not durable: %#v", record)
	}
}

func TestConformance_RetentionDeletePartial(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	oldSet := fixtureSet("backup-retention-old", now.Add(-3*time.Hour), 1)
	newSet := fixtureSet("backup-retention-new", now.Add(-2*time.Hour), 1)
	for _, set := range []BackupSet{oldSet, newSet} {
		prepareAttempt(t, repo, set, 1)
		if _, err := repo.Publish(context.Background(), set); err != nil {
			t.Fatal(err)
		}
	}
	failedKey := scoped(oldSet.ScopeID, oldSet.Artifacts[0].Object.ObjectID)
	blobs.mu.Lock()
	blobs.failDelete[failedKey] = true
	blobs.mu.Unlock()
	policy := retentionPolicy(newSet, "retention-evidence", now.Add(-time.Hour), now)
	result, err := repo.ApplyRetention(context.Background(), policy, false)
	if !IsCode(err, CodeDeletePartial) || !contains(result.Quarantined, oldSet.BackupID) {
		t.Fatalf("expected partial deletion quarantine: %#v, %v", result, err)
	}
	listed, err := repo.List(context.Background(), oldSet.ScopeID, 10, "")
	if err != nil || len(listed.Items) != 1 || listed.Items[0].BackupID != newSet.BackupID {
		t.Fatalf("quarantined backup visible: %#v, %v", listed, err)
	}
}

func TestConformance_RetentionCancellationQuarantines(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	oldSet := fixtureSet("backup-retention-canceled", now.Add(-3*time.Hour), 1)
	newSet := fixtureSet("backup-retention-current", now.Add(-2*time.Hour), 1)
	for _, set := range []BackupSet{oldSet, newSet} {
		prepareAttempt(t, repo, set, 1)
		if _, err := repo.Publish(context.Background(), set); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blobs.mu.Lock()
	blobs.cancelDelete = cancel
	blobs.cancelAfterDeletes = 1
	blobs.mu.Unlock()
	policy := retentionPolicy(newSet, "retention-cancel-evidence", now.Add(-time.Hour), now)
	result, err := repo.ApplyRetention(ctx, policy, false)
	if !IsCode(err, CodeDeletePartial) || !errors.Is(err, context.Canceled) || !contains(result.Quarantined, oldSet.BackupID) {
		t.Fatalf("expected cancellation quarantine: %#v, %v", result, err)
	}
	value, err := metadata.Get(context.Background(), oldSet.ScopeID, backupRecordKey(oldSet.BackupID))
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeRecord(value.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	manifestKey := manifestObjectKey(oldSet.ScopeID, oldSet.BackupID)
	if record.State != LifecycleQuarantined || record.QuarantineReason != "DELETE_CANCELED" || len(record.ResidualKeys) != 2 || !contains(record.ResidualKeys, oldSet.CatalogObject.ObjectID) || !contains(record.ResidualKeys, manifestKey) {
		t.Fatalf("cancellation state was not durable: %#v", record)
	}
	if _, _, openErr := blobs.Open(context.Background(), oldSet.ScopeID, oldSet.Artifacts[0].Object.ObjectID); !errors.Is(openErr, ErrBackendNotFound) {
		t.Fatalf("first deletion did not complete before cancellation: %v", openErr)
	}
}

func TestConformance_Pagination(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	blobs, metadata := newMemoryBlobStore(t), newMemoryMetadataStore()
	repo := mustRepository(t, blobs, metadata, &now)
	for index, id := range []string{"backup-a", "backup-b"} {
		set := fixtureSet(id, now.Add(time.Duration(index-3)*time.Hour), 1)
		prepareAttempt(t, repo, set, 1)
		if _, err := repo.Publish(context.Background(), set); err != nil {
			t.Fatal(err)
		}
	}
	first, err := repo.List(context.Background(), "scope-a", 1, "")
	if err != nil || len(first.Items) != 1 || first.NextPageToken == "" {
		t.Fatalf("bad first page: %#v, %v", first, err)
	}
	second, err := repo.List(context.Background(), "scope-a", 1, first.NextPageToken)
	if err != nil || len(second.Items) != 1 || second.Items[0].BackupID <= first.Items[0].BackupID {
		t.Fatalf("bad second page: %#v, %v", second, err)
	}
	if _, err := repo.List(context.Background(), "scope-b", 1, first.NextPageToken); !IsCode(err, CodeRepositoryDenied) {
		t.Fatalf("cross-scope token accepted: %v", err)
	}
	for _, size := range []int{0, 101} {
		if _, err := repo.List(context.Background(), "scope-a", size, ""); !IsCode(err, CodeRepositoryDenied) {
			t.Fatalf("page size %d accepted", size)
		}
	}
}

func TestCanonicalBackupSet_StrictAndStable(t *testing.T) {
	base := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	set := fixtureSet("backup-canon", base, 2)
	set.Artifacts[0].RowStats = append(set.Artifacts[0].RowStats, TableStat{Table: "table-b", Rows: 2, FNV1A64: "fedcba9876543210"})
	_, _, first, err := canonicalBackupSet(set)
	if err != nil {
		t.Fatal(err)
	}
	reordered := cloneBackupSet(set)
	sort.Slice(reordered.ExpectedShardIDs, func(i, j int) bool { return reordered.ExpectedShardIDs[i] > reordered.ExpectedShardIDs[j] })
	sort.Slice(reordered.RecoveryPoint.Shards, func(i, j int) bool {
		return reordered.RecoveryPoint.Shards[i].ShardID > reordered.RecoveryPoint.Shards[j].ShardID
	})
	sort.Slice(reordered.Artifacts, func(i, j int) bool { return reordered.Artifacts[i].ShardID > reordered.Artifacts[j].ShardID })
	sort.Slice(reordered.Artifacts[1].RowStats, func(i, j int) bool {
		return reordered.Artifacts[1].RowStats[i].Table > reordered.Artifacts[1].RowStats[j].Table
	})
	_, _, second, err := canonicalBackupSet(reordered)
	if err != nil || first != second {
		t.Fatalf("canonical digest changed: %s %s %v", first, second, err)
	}
	raw, err := storedManifestBytes(set)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := bytes.Replace(raw, []byte(`"backupId":"backup-canon"`), []byte(`"backupId":"backup-canon","backupId":"backup-canon"`), 1)
	cases := [][]byte{
		bytes.Replace(raw, []byte("{"), []byte(`{"unknown":1,`), 1),
		duplicate,
		append(append([]byte(nil), raw...), []byte("{}")...),
		bytes.ReplaceAll(raw, []byte("\n"), []byte("\r\n")),
		{0xff, 0xfe},
		append(append([]byte(nil), raw...), bytes.Repeat([]byte(" "), maxManifestBytes)...),
	}
	for index, candidate := range cases {
		if _, err := decodeBackupSet(candidate); err == nil {
			t.Fatalf("strict case %d accepted", index)
		}
	}
}

func TestConformance_ConcurrentReservation(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	repo := mustRepository(t, newMemoryBlobStore(t), newMemoryMetadataStore(), &now)
	set := fixtureSet("backup-race", now, 1)
	input := Reservation{ScopeID: set.ScopeID, BackupID: set.BackupID, SourceEnvironmentID: set.SourceEnvironmentID, ContractRef: set.ContractRef, ScheduleWindow: set.ScheduleWindow}
	const workers = 8
	var wait sync.WaitGroup
	results := make(chan ReservationResult, workers)
	failures := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := repo.Reserve(context.Background(), input)
			if err != nil {
				failures <- err
				return
			}
			results <- result
		}()
	}
	wait.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Errorf("concurrent reserve failed: %v", err)
	}
	attempt := ""
	for result := range results {
		if attempt == "" {
			attempt = result.AttemptID
		}
		if result.AttemptID != attempt {
			t.Fatalf("multiple attempts: %s != %s", result.AttemptID, attempt)
		}
	}
	conflict := input
	conflict.BackupID = "backup-race-other"
	if _, err := repo.Reserve(context.Background(), conflict); !IsCode(err, CodeReservationConflict) {
		t.Fatalf("expected schedule reservation conflict, got %v", err)
	}
}

func TestConformance_ConcurrentPublish(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	repo := mustRepository(t, newMemoryBlobStore(t), newMemoryMetadataStore(), &now)
	set := fixtureSet("backup-publish-race", now.Add(-time.Minute), 2)
	prepareAttempt(t, repo, set, 2)
	const workers = 4
	var wait sync.WaitGroup
	refs := make(chan ManifestRef, workers)
	failures := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ref, err := repo.Publish(context.Background(), set)
			if err != nil {
				failures <- err
				return
			}
			refs <- ref
		}()
	}
	wait.Wait()
	close(refs)
	close(failures)
	for err := range failures {
		t.Errorf("concurrent publish failed: %v", err)
	}
	digest := ""
	for ref := range refs {
		if digest == "" {
			digest = ref.ManifestSHA256
		}
		if ref.ManifestSHA256 != digest {
			t.Fatalf("multiple manifests: %s != %s", ref.ManifestSHA256, digest)
		}
	}
	listed, err := repo.List(context.Background(), set.ScopeID, 10, "")
	if err != nil || len(listed.Items) != 1 {
		t.Fatalf("unexpected published visibility: %#v, %v", listed, err)
	}
}

func FuzzCanonicalBackupSet(f *testing.F) {
	base := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	seed, err := storedManifestBytes(fixtureSet("backup-fuzz", base, 1))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		set, err := decodeBackupSet(data)
		if err != nil {
			return
		}
		_, first, digest, err := canonicalBackupSet(set)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeBackupSet(data)
		if err != nil {
			t.Fatal(err)
		}
		_, second, secondDigest, err := canonicalBackupSet(decoded)
		if err != nil || digest != secondDigest || !bytes.Equal(first, second) {
			t.Fatalf("unstable canonical round trip: %v", err)
		}
	})
}
