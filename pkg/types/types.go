// Package types contains the public data model for cefas: items,
// attribute values, table descriptors. The shape mirrors DynamoDB closely
// enough that anyone familiar with the latter will feel at home, but the
// wire format and SDK are independent.
package types

import "errors"

// AttrType is the kind tag on an AttributeValue. Same letter codes as
// DynamoDB so requests are diff-readable side-by-side.
type AttrType uint8

const (
	AttrNull AttrType = iota
	AttrS             // string
	AttrN             // number (decimal, kept as string for arbitrary precision)
	AttrB             // binary
	AttrBOOL
	AttrSS // string set
	AttrNS // number set
	AttrBS // binary set
	AttrL  // list
	AttrM  // map
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
	Name      string    `json:"name"`
	KeySchema KeySchema `json:"keySchema"`
	Projected []string  `json:"projected,omitempty"`
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

// NumRange bounds a single numeric dimension for Z-order encoding.
type NumRange struct {
	Lo float64 `json:"lo"`
	Hi float64 `json:"hi"`
}

// TableDescriptor is the persisted schema. Stored under cefas/catalog/<name>.
type TableDescriptor struct {
	Name           string                   `json:"name"`
	KeySchema      KeySchema                `json:"keySchema"`
	GSIs           []GSIDescriptor          `json:"gsis,omitempty"`
	SpatialIndexes []SpatialIndexDescriptor `json:"spatialIndexes,omitempty"`
}

// Errors surfaced by the public API. Server code maps these to HTTP /
// gRPC status codes at the boundary.
var (
	ErrTableNotFound      = errors.New("cefas: table not found")
	ErrTableAlreadyExists = errors.New("cefas: table already exists")
	ErrItemNotFound       = errors.New("cefas: item not found")
	ErrMissingKey         = errors.New("cefas: item missing key attribute")
	ErrInvalidKeyType     = errors.New("cefas: key attribute must be S, N, or B")
	ErrSpatialNotFound    = errors.New("cefas: spatial index not found")
	ErrInvalidSpatial     = errors.New("cefas: invalid spatial index descriptor")
)
