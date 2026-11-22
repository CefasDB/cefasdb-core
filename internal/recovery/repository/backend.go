package repository

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrBackendNotFound means the scoped key is absent.
	ErrBackendNotFound = errors.New("repository backend: not found")
	// ErrBackendConflict means a create-only write or CAS lost to another value.
	ErrBackendConflict = errors.New("repository backend: conflict")
	// ErrBackendDenied means the backend rejected the supplied scope or operation.
	ErrBackendDenied = errors.New("repository backend: denied")
	// ErrBackendAmbiguous means the operation may have committed before acknowledgement.
	ErrBackendAmbiguous = errors.New("repository backend: ambiguous acknowledgement")
	// ErrBackendDigestMismatch means streamed bytes did not match declared metadata.
	ErrBackendDigestMismatch = errors.New("repository backend: digest mismatch")
	// ErrBackendInvalidToken means a backend pagination token is malformed or stale.
	ErrBackendInvalidToken = errors.New("repository backend: invalid page token")
)

type BlobMetadata struct {
	SHA256    string `json:"sha256"`
	SizeBytes uint64 `json:"sizeBytes"`
	MediaType string `json:"mediaType"`
}

// BlobStore is a create-only, scope-authorized streaming object port.
//
// PutIfAbsent must consume body without buffering the complete object, commit
// only when its bytes match metadata, and make an identical retry idempotent.
// Every method must honor ctx and must not authorize a key from scopeID alone.
type BlobStore interface {
	PutIfAbsent(
		ctx context.Context,
		scopeID string,
		key string,
		metadata BlobMetadata,
		body io.Reader,
	) (BlobMetadata, error)
	Open(ctx context.Context, scopeID string, key string) (io.ReadCloser, BlobMetadata, error)
	Delete(ctx context.Context, scopeID string, key string) error
}

type MetadataValue struct {
	Bytes    []byte `json:"bytes"`
	Revision string `json:"revision"`
}

type MetadataEntry struct {
	Key   string        `json:"key"`
	Value MetadataValue `json:"value"`
}

type MetadataPage struct {
	Entries       []MetadataEntry `json:"entries"`
	NextPageToken string          `json:"nextPageToken,omitempty"`
}

// MetadataStore is a scope-authorized durable CAS and pagination port.
//
// Empty expectedRevision means create-if-absent. A successful
// CompareAndSwap is durable before acknowledgement. Revisions and page tokens
// are opaque, and List must never return values from another scope.
type MetadataStore interface {
	Get(ctx context.Context, scopeID string, key string) (MetadataValue, error)
	CompareAndSwap(
		ctx context.Context,
		scopeID string,
		key string,
		expectedRevision string,
		nextBytes []byte,
	) (MetadataValue, error)
	List(
		ctx context.Context,
		scopeID string,
		prefix string,
		pageSize int,
		pageToken string,
	) (MetadataPage, error)
}
