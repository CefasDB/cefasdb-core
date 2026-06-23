// Package types contains the public data model for cefas: items,
// attribute values, table descriptors. The shape mirrors DynamoDB closely
// enough that anyone familiar with the latter will feel at home, but the
// wire format and SDK are independent.
package types

import (
	"errors"
	"strings"
)

// AttrType is the kind tag on an AttributeValue. Same letter codes as
// DynamoDB so requests are diff-readable side-by-side.
type AttrType uint8

const (
	AttrNull AttrType = iota
	AttrS             // string
	AttrN             // number (decimal, kept as string for arbitrary precision)
	AttrB             // binary
	AttrBOOL
	AttrSS  // string set
	AttrNS  // number set
	AttrBS  // binary set
	AttrL   // list
	AttrM   // map
	AttrVec // native numeric vector
)

// AttributeValue is the polymorphic attribute carrier. Only one field is
// meaningful per instance; T disambiguates. We keep it as a flat struct
// (not interface) because every item write/read touches a fistful of
// attributes and the per-allocation cost of interface boxing adds up on
// hot paths.
type AttributeValue struct {
	T    AttrType
	S    string
	N    string // canonical decimal text
	B    []byte
	BOOL bool
	SS   []string
	NS   []string
	BS   [][]byte
	L    []AttributeValue
	M    map[string]AttributeValue
	Vec  []float64
}

// Item is the unit of storage: a flat map of attribute name to value. The
// table's KeySchema designates which attributes form the primary key.
type Item map[string]AttributeValue

// KeySchema describes the primary key of a table.
//   - PK is the hash key attribute name; required.
//   - SK is the sort key attribute name; empty means "no sort key" (item
//     keyed solely by PK).
type KeySchema struct {
	PK string `json:"pk"`
	SK string `json:"sk,omitempty"`
}

// GSIDescriptor describes a global secondary index (Phase 2). Only the
// shape is defined here so callers can prepare schemas; the writer/index
// code lands later.
type GSIDescriptor struct {
	Name       string          `json:"name"`
	KeySchema  KeySchema       `json:"keySchema"`
	Projection IndexProjection `json:"projection,omitempty"`
	// Projected is the legacy field — kept for backward compat with
	// existing descriptors. New code uses Projection.
	Projected []string `json:"projected,omitempty"`
}

// LSIDescriptor describes a local secondary index — an alternate sort
// key co-located with the primary partition. Cheaper than a GSI
// because writes never leave the primary partition's hash bucket, so
// the planner needs no cross-shard coordination.
//
// LSI key layout (built by storage.KeyLSI):
//
//	cefas/t/<table>/lsi/<idx>/<primary_pk_hash8><lsi_sk_bytes><primary_sk_bytes>
type LSIDescriptor struct {
	Name string `json:"name"`
	// SK is the alternate sort-key attribute name. The partition key
	// is implicitly the table's primary PK, so we do not store a
	// KeySchema struct.
	SK         string          `json:"sk"`
	Projection IndexProjection `json:"projection,omitempty"`
}

// IndexProjection controls which item attributes the index stores in
// its value payload, mirroring DynamoDB's projection modes.
//
//   - "KEYS_ONLY" (zero value) — index value carries only the primary
//     key reference. Readers must do one Get per row to materialise
//     attributes.
//   - "INCLUDE"   — primary key reference + the listed attribute names.
//     Readers can satisfy queries that touch only those attributes
//     without a dereference.
//   - "ALL"       — full denormalised item in the index value.
//     Readers never dereference but writes amplify by O(item size).
type IndexProjection struct {
	Mode    string   `json:"mode,omitempty"`    // "KEYS_ONLY" | "INCLUDE" | "ALL"
	Include []string `json:"include,omitempty"` // INCLUDE only; ignored otherwise
}

// SpatialIndexDescriptor describes either a geohash or a Z-order index
// over numeric attributes (Phase 3).
type SpatialIndexDescriptor struct {
	Name string `json:"name"`
	// Kind is "geohash" or "zorder".
	Kind string `json:"kind"`
	// Attributes that feed the index. For geohash: [lat, lon]. For
	// zorder: any number of numeric attributes (typically 2-4).
	Attributes []string `json:"attributes"`
	// Precision: geohash char length (1-12). Ignored for zorder
	// (always 32 bits per dim).
	Precision int `json:"precision,omitempty"`
	// Ranges, when set, declare the [lo, hi] bounds of each Z-order
	// dimension in attribute order. Required for "zorder" kind;
	// ignored for geohash. Values outside the range get clamped at
	// encode time.
	Ranges []NumRange `json:"ranges,omitempty"`
}

// AttributeDefinition captures optional table-level attribute metadata.
// Key attributes only need their scalar type. Vector attributes use
// Type="V" and VectorDimensions > 0 so write paths can fail dimension
// mismatches before data lands in storage.
type AttributeDefinition struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	VectorDimensions int    `json:"vectorDimensions,omitempty"`
}

const (
	AttributeTypeString  = "S"
	AttributeTypeNumber  = "N"
	AttributeTypeBinary  = "B"
	AttributeTypeVector  = "V"
	AttributeTypeCounter = "COUNTER"
)

func NormalizeAttributeType(t string) string {
	return strings.ToUpper(strings.TrimSpace(t))
}

func IsCounterAttributeType(t string) bool {
	return NormalizeAttributeType(t) == AttributeTypeCounter
}

const (
	StorageClassDisk   = "disk"
	StorageClassMemory = "memory"
)

const (
	StreamViewTypeKeysOnly        = "KEYS_ONLY"
	StreamViewTypeNewImage        = "NEW_IMAGE"
	StreamViewTypeOldImage        = "OLD_IMAGE"
	StreamViewTypeNewAndOldImages = "NEW_AND_OLD_IMAGES"
	// DELTA_IMAGE emits only the columns that changed on an
	// UpdateItem. INSERT keeps the full new image (no diff
	// available); DELETE keeps key only (no "changed columns"
	// semantics). Designed for bandwidth-sensitive replication
	// and audit subscribers that already hold the prior state
	// upstream (#522).
	StreamViewTypeDeltaImage = "DELTA_IMAGE"

	StreamStatusEnabling  = "ENABLING"
	StreamStatusEnabled   = "ENABLED"
	StreamStatusDisabling = "DISABLING"
	StreamStatusDisabled  = "DISABLED"

	// StreamShardIDSingle is the canonical single-shard identifier
	// for DynamoDB-compatible streams.
	//
	// This package keeps it as a bare string because pkg/types sits
	// at the bottom of the import graph (pkg/core/model imports
	// pkg/types, not the other way round). The phase-5 value-object
	// migration introduced model.StreamShardIDSingle of type
	// model.StreamShardID as the new source of truth; new callers
	// should prefer the VO and call .String() only when they need
	// to hand the wire form to a string-shaped field. Both values
	// resolve to the same canonical "shardId-000000000000" literal.
	StreamShardIDSingle = "shardId-000000000000"
)

// StreamSpecification mirrors DynamoDB's table-level stream settings.
type StreamSpecification struct {
	StreamEnabled  bool   `json:"streamEnabled"`
	StreamViewType string `json:"streamViewType,omitempty"`
	// RetentionSeconds overrides the cluster-wide retention for
	// this table's stream (#521). Zero means "inherit cluster
	// default"; non-zero must be within [MinStreamRetentionSeconds,
	// MaxStreamRetentionSeconds].
	RetentionSeconds int64 `json:"retentionSeconds,omitempty"`
}

// Stream retention bounds for the per-table override. Loose enough
// to accept "1 minute" through "90 days"; the cluster-wide default
// stays whatever the pebble StreamRetentionOptions set at boot.
const (
	MinStreamRetentionSeconds int64 = 60
	MaxStreamRetentionSeconds int64 = 90 * 24 * 60 * 60
)

// StreamSequenceNumberRange mirrors DynamoDB's per-shard sequence range.
// Active shards omit EndingSequenceNumber; closed shards retain it.
type StreamSequenceNumberRange struct {
	StartingSequenceNumber string `json:"startingSequenceNumber,omitempty"`
	EndingSequenceNumber   string `json:"endingSequenceNumber,omitempty"`
}

// StreamShardDescriptor is the public shard model for table streams.
// CefasDB V1 uses one open shard per active table stream.
type StreamShardDescriptor struct {
	ShardID             string                    `json:"shardId"`
	SequenceNumberRange StreamSequenceNumberRange `json:"sequenceNumberRange"`
}

// StreamDescriptor is the persisted metadata returned by future
// ListStreams and DescribeStream APIs.
type StreamDescriptor struct {
	StreamArn               string                  `json:"streamArn"`
	StreamLabel             string                  `json:"streamLabel"`
	TableName               string                  `json:"tableName"`
	StreamStatus            string                  `json:"streamStatus"`
	StreamViewType          string                  `json:"streamViewType"`
	CreationRequestDateTime int64                   `json:"creationRequestDateTime"`
	KeySchema               KeySchema               `json:"keySchema"`
	Shards                  []StreamShardDescriptor `json:"shards,omitempty"`
}

func NormalizeStreamViewType(view string) string {
	return strings.ToUpper(strings.TrimSpace(view))
}

func IsValidStreamViewType(view string) bool {
	switch NormalizeStreamViewType(view) {
	case StreamViewTypeKeysOnly,
		StreamViewTypeNewImage,
		StreamViewTypeOldImage,
		StreamViewTypeNewAndOldImages,
		StreamViewTypeDeltaImage:
		return true
	default:
		return false
	}
}

// NumRange bounds a single numeric dimension for Z-order encoding.
type NumRange struct {
	Lo float64 `json:"lo"`
	Hi float64 `json:"hi"`
}

// TableDescriptor is the persisted schema. Stored under cefas/catalog/<name>.
type TableDescriptor struct {
	Name                 string                   `json:"name"`
	KeySchema            KeySchema                `json:"keySchema"`
	AttributeDefinitions []AttributeDefinition    `json:"attributeDefinitions,omitempty"`
	GSIs                 []GSIDescriptor          `json:"gsis,omitempty"`
	LSIs                 []LSIDescriptor          `json:"lsis,omitempty"`
	SpatialIndexes       []SpatialIndexDescriptor `json:"spatialIndexes,omitempty"`
	StorageClass         string                   `json:"storageClass,omitempty"`
	MemoryFootprintBytes int64                    `json:"memoryFootprintBytes,omitempty"`
	// TTLAttribute, when non-empty, names a numeric attribute whose
	// value (Unix epoch seconds) marks the row's expiration. The
	// background reaper sweeps expired rows lazily; reads of an
	// expired row are still served until the reaper passes.
	TTLAttribute        string               `json:"ttlAttribute,omitempty"`
	StreamSpecification *StreamSpecification `json:"streamSpecification,omitempty"`
	LatestStreamArn     string               `json:"latestStreamArn,omitempty"`
	LatestStreamLabel   string               `json:"latestStreamLabel,omitempty"`
	StreamStatus        string               `json:"streamStatus,omitempty"`
	// MaterializedViews names the views whose base is this table.
	MaterializedViews []string `json:"materializedViews,omitempty"`
	// GlobalIndexes names the ScyllaDB-style global indexes attached
	// to this base table (epic #509). The write hook in #511 reads
	// this list to fan out per-mutation index updates.
	GlobalIndexes []string `json:"globalIndexes,omitempty"`
}

// RefreshMode mirrors cefaspb.RefreshPolicy_Mode.
type RefreshMode string

const (
	RefreshModeUnspecified RefreshMode = ""
	RefreshModeEager       RefreshMode = "eager"
	RefreshModeScheduled   RefreshMode = "scheduled"
	RefreshModeOnDemand    RefreshMode = "on_demand"
	// RefreshModeFast applies the delta since the last refresh from
	// the base table's changelog. Unlike SCHEDULED (complete rescan)
	// it costs O(B) per tick where B is the number of base mutations
	// in the interval, not O(|base|).
	RefreshModeFast RefreshMode = "fast"
)

// RefreshPolicy decides when a materialized view is reconciled
// with its base. IntervalSeconds is only meaningful when Mode is
// RefreshModeScheduled or RefreshModeFast.
type RefreshPolicy struct {
	Mode            RefreshMode `json:"mode"`
	IntervalSeconds int64       `json:"intervalSeconds,omitempty"`
}

const (
	MVStatusBuilding = "building"
	MVStatusActive   = "active"
	MVStatusPaused   = "paused"
	MVStatusFailed   = "failed"
)

// MaterializedViewDescriptor is the persisted shape of a materialized
// view. Stored under cefas/internal/mv/<name>.
type MaterializedViewDescriptor struct {
	Name                string        `json:"name"`
	BaseTable           string        `json:"baseTable"`
	KeySchema           KeySchema     `json:"keySchema"`
	ProjectedAttributes []string      `json:"projectedAttributes,omitempty"`
	RefreshPolicy       RefreshPolicy `json:"refreshPolicy"`
	Status              string        `json:"status"`
	LastRefreshAtUnix   int64         `json:"lastRefreshAtUnix,omitempty"`
}

// Errors surfaced by the public API. Server code maps these to HTTP /
// gRPC status codes at the boundary.
var (
	ErrTableNotFound              = errors.New("cefas: table not found")
	ErrTableAlreadyExists         = errors.New("cefas: table already exists")
	ErrItemNotFound               = errors.New("cefas: item not found")
	ErrMissingKey                 = errors.New("cefas: item missing key attribute")
	ErrInvalidKeyType             = errors.New("cefas: key attribute must be S, N, or B")
	ErrSpatialNotFound            = errors.New("cefas: spatial index not found")
	ErrInvalidSpatial             = errors.New("cefas: invalid spatial index descriptor")
	ErrStreamNotFound             = errors.New("cefas: stream not found")
	ErrStreamShardNotFound        = errors.New("cefas: stream shard not found")
	ErrStreamIteratorInvalid      = errors.New("cefas: stream iterator invalid")
	ErrStreamIteratorExpired      = errors.New("cefas: stream iterator expired")
	ErrStreamTrimmed              = errors.New("cefas: stream sequence has been trimmed")
	ErrMVNotFound                 = errors.New("cefas: materialized view not found")
	ErrMVAlreadyExists            = errors.New("cefas: materialized view already exists")
	ErrServiceLevelNotFound       = errors.New("cefas: service level not found")
	ErrServiceLevelExists         = errors.New("cefas: service level already exists")
	ErrServiceLevelReserved       = errors.New("cefas: service level name is reserved")
	ErrGlobalIndexNotFound        = errors.New("cefas: global index not found")
	ErrGlobalIndexExists          = errors.New("cefas: global index already exists")
	ErrInvalidAttributeDefinition = errors.New("cefas: invalid attribute definition")
)

// DefaultServiceLevelName is the implicit service level every caller
// falls back to when no explicit SL is resolved. Cannot be dropped.
const DefaultServiceLevelName = "default"

// CDCTableSuffix is the synthetic-table suffix that exposes a base
// table's changelog as a queryable stream (#523). A Scan/Query
// against "<base>_cdc" walks the underlying pebble changelog and
// decodes each ChangeRecord into a row.
const CDCTableSuffix = "_cdc"

// GlobalIndexDescriptor describes a ScyllaDB-style global secondary
// index. Unlike the native DynamoDB-shaped GSI (#475), the index has
// its own partitioning by the IndexedColumn's value — queries hit
// exactly one shard and writes cross-shard cascade.
//
// Phase 1 of #509 ships the descriptor only; Phase 2 (#511) wires
// the write hook, Phase 3 (#512) the read routing, Phase 4 (#513)
// backfill.
type GlobalIndexDescriptor struct {
	Name              string   `json:"name"`
	BaseTable         string   `json:"baseTable"`
	IndexedColumn     string   `json:"indexedColumn"`
	ProjectedColumns  []string `json:"projectedColumns,omitempty"`
	Status            string   `json:"status,omitempty"`
	Shards            int      `json:"shards,omitempty"`
	ReplicationFactor int      `json:"replicationFactor,omitempty"`
	Paused            bool     `json:"paused,omitempty"`
}

// GlobalIndexStatus constants mirror the materialized-view status
// model (#488) so operators reading the catalog see the same shape.
const (
	GlobalIndexStatusBuilding = "building"
	GlobalIndexStatusActive   = "active"
	GlobalIndexStatusFailed   = "failed"
	GlobalIndexStatusPaused   = "paused"
)

// ServiceLevelDescriptor is the catalog object the workload
// prioritization scheduler reads to size per-SL lanes. Shares is the
// relative weight (1+); caps are advisory hints — Phase 4 (#499)
// wires the rate limits, Phase 3 (#498) wires shares-based DRR.
type ServiceLevelDescriptor struct {
	Name           string `json:"name"`
	Shares         int    `json:"shares,omitempty"`
	MaxInFlight    int    `json:"maxInFlight,omitempty"`
	MaxRowsPerSec  int64  `json:"maxRowsPerSec,omitempty"`
	MaxBytesPerSec int64  `json:"maxBytesPerSec,omitempty"`
	// Paused, when true, causes the quota controller to reject every
	// admission for this SL with codes.Unavailable. Resume restores
	// the previous shares / caps without rewriting the descriptor.
	Paused bool `json:"paused,omitempty"`
}
