package model

import "github.com/osvaldoandrade/cefas/pkg/types"

// Item is a row's attribute map.
type Item = types.Item

// AttributeValue carries a single attribute's typed value.
type AttributeValue = types.AttributeValue

// AttrType enumerates the supported attribute kinds (S / N / B / BOOL
// / NULL / SS / NS / BS / L / M / V).
type AttrType = types.AttrType

// KeySchema names the primary partition (PK) and optional sort (SK)
// attributes.
type KeySchema = types.KeySchema

// TableDescriptor describes a table's schema as persisted in the
// catalog (name, key schema, secondary indexes, TTL attribute).
type TableDescriptor = types.TableDescriptor

// Re-export the well-known attribute-kind constants so plugins can
// switch on av.T without importing pkg/types.
const (
	AttrS    = types.AttrS
	AttrN    = types.AttrN
	AttrB    = types.AttrB
	AttrBOOL = types.AttrBOOL
	AttrNull = types.AttrNull
	AttrSS   = types.AttrSS
	AttrNS   = types.AttrNS
	AttrBS   = types.AttrBS
	AttrL    = types.AttrL
	AttrM    = types.AttrM
	AttrVec  = types.AttrVec
)
